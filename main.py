"""Telegram Group Bridge - Multi-Session Worker."""

import asyncio
import html
import os
import tempfile
import time
from typing import Dict, List, Set, Tuple

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from dotenv import load_dotenv
from loguru import logger
from telethon import TelegramClient, events
from telethon.errors import RPCError, UserAlreadyParticipantError
from telethon import functions
from telethon.tl.functions.channels import CreateForumTopicRequest
from telethon.tl.types import InputPeerChannel, MessageMediaWebPage, PeerChannel
from telethon.sessions import StringSession

from src.database import (
    BridgeEntry,
    TopicRule,
    TopicProposal,
    UserSession,
    create_topic_proposal,
    create_topic_mapping,
    get_active_bridges,
    get_active_topic_rules,
    get_route_reload_token,
    get_session,
    get_topic_proposal_by_source,
    get_topic_mapping,
    init_db,
    mark_topic_proposal_notified,
)
from src.parser import contains_keywords, process_message

RECONNECT_DELAY = 5
ROUTING_RELOAD_DELAY = 5
SOURCE_POLL_DELAY = 5
GENERAL_TOPIC_ID = 1
MEDIA_MAX_SIZE_BYTES = 15 * 1024 * 1024
MEDIA_TTL_SECONDS = 60 * 60
MEDIA_CLEANUP_KEEP_FILES = 200
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MEDIA_TEMP_DIR = os.path.join(BASE_DIR, ".runtime", "media-tmp")

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
notification_bot = (
    Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    if BOT_TOKEN
    else None
)


def parse_keywords(keywords_str: str) -> List[str]:
    if not keywords_str:
        return []
    return [k.strip().lower() for k in keywords_str.split(",") if k.strip()]


class SessionWorker:
    """Worker for a single session that can handle multiple bridges."""

    def __init__(self, session: UserSession):
        self.session = session
        self.client: TelegramClient | None = None
        self.sender_client: TelegramClient | None = None
        self.bot_username: str | None = None
        self.bot_target_access: Dict[int, bool] = {}
        self.bridges: Dict[int, BridgeEntry] = {}
        self.direct_bridges: Dict[int, List[BridgeEntry]] = {}
        self.forum_bridges: Dict[int, List[BridgeEntry]] = {}
        self.topic_rules: Dict[tuple[int, int], List[tuple[TopicRule, BridgeEntry]]] = {}
        self.source_baselines: Dict[int, int] = {}
        self.processed_messages: Set[Tuple[int, int]] = set()
        self.running = False
        self.route_reload_token = 0.0

    async def start(self) -> None:
        """Start the worker and connect to Telegram."""
        self.client = TelegramClient(
            StringSession(self.session.session_string),
            self.session.api_id,
            self.session.api_hash,
        )

        while True:
            try:
                await self._connect_and_run()
            except Exception:
                logger.exception(f"Session {self.session.session_id}: Connection error. Reconnecting...")
                await asyncio.sleep(RECONNECT_DELAY)

    async def _connect_and_run(self) -> None:
        logger.info(f"Session {self.session.session_id}: Connecting to Telegram...")
        await self.client.connect()

        if not await self.client.is_user_authorized():
            logger.error(f"Session {self.session.session_id}: Not authorized, skipping")
            return
        os.makedirs(MEDIA_TEMP_DIR, exist_ok=True)
        self._cleanup_temp_media_dir()
        await self._init_sender_client()

        await self._load_bridges()
        await self._ensure_source_baselines()
        self.route_reload_token = get_route_reload_token()
        self._setup_handlers()
        await self._scan_for_new_forum_topics()

        if self.bridges:
            logger.info(f"Session {self.session.session_id}: Listening {len(self.bridges)} sources")
            self.running = True
            # Run client and periodic bridge reload concurrently
            await asyncio.gather(
                self.client.run_until_disconnected(),
                self._periodic_reload_bridges(),
                self._poll_sources(),
                return_exceptions=True,
            )
        else:
            logger.info(f"Session {self.session.session_id}: No bridges, disconnecting")
            await self.client.disconnect()
            await asyncio.sleep(30)

    async def _init_sender_client(self) -> None:
        """Initialize sender client. Prefer bot identity when BOT_TOKEN is set."""
        if self.sender_client and self.sender_client is not self.client:
            await self.sender_client.disconnect()

        if not BOT_TOKEN:
            self.sender_client = self.client
            logger.warning("BOT_TOKEN is not set, sending via user account")
            return

        bot_client = TelegramClient(
            StringSession(),
            self.session.api_id,
            self.session.api_hash,
        )
        await bot_client.connect()
        await bot_client.start(bot_token=BOT_TOKEN)
        self.sender_client = bot_client
        bot_me = await bot_client.get_me()
        self.bot_username = getattr(bot_me, "username", None)
        logger.info(
            f"Session {self.session.session_id}: Sender initialized as bot "
            f"@{self.bot_username or 'unknown'}"
        )

    def _get_sender_client(self) -> TelegramClient:
        return self.sender_client or self.client

    async def _resolve_target_peer(self, target_id: int):
        """Resolve target entity using user session cache for stable access_hash."""
        sender = self._get_sender_client()
        if sender is self.client:
            return await self.client.get_input_entity(target_id)

        if not await self._ensure_bot_target_access(target_id):
            raise ValueError(
                f"Bot has no access to target {target_id}. "
                "Add bot to target and grant send permissions."
            )
        return await sender.get_input_entity(target_id)

    async def _ensure_bot_target_access(self, target_id: int) -> bool:
        if target_id in self.bot_target_access:
            return self.bot_target_access[target_id]

        sender = self._get_sender_client()
        if sender is self.client:
            self.bot_target_access[target_id] = True
            return True

        # Fast path: bot already has entity in cache/access.
        try:
            await sender.get_input_entity(target_id)
            self.bot_target_access[target_id] = True
            return True
        except Exception:
            pass

        if not self.bot_username:
            self.bot_target_access[target_id] = False
            return False

        # Attempt to invite bot from user session for channels/supergroups/forums.
        try:
            channel_entity = await self.client.get_input_entity(target_id)
            await self.client(
                functions.channels.InviteToChannelRequest(
                    channel=channel_entity,
                    users=[self.bot_username],
                )
            )
        except UserAlreadyParticipantError:
            pass
        except Exception as e:
            logger.warning(
                f"Session {self.session.session_id}: Cannot auto-add bot to {target_id}: {e}"
            )

        try:
            await sender.get_input_entity(target_id)
            self.bot_target_access[target_id] = True
            return True
        except Exception as e:
            self.bot_target_access[target_id] = False
            logger.warning(
                f"Session {self.session.session_id}: Bot still has no access to {target_id}: {e}"
            )
            return False

    async def _send_text(
        self,
        target_id: int,
        text: str,
        entities=None,
        reply_to: int | None = None,
    ) -> None:
        sender = self._get_sender_client()
        target_peer = await self._resolve_target_peer(target_id)
        kwargs = {}
        if reply_to:
            kwargs["reply_to"] = reply_to
        if entities:
            kwargs["formatting_entities"] = entities
        # Keep links clickable but hide webpage preview banners.
        kwargs["link_preview"] = False
        await sender.send_message(target_peer, text, **kwargs)

    async def _send_media(
        self,
        target_id: int,
        message,
        caption: str | None = None,
        caption_entities=None,
        reply_to: int | None = None,
    ) -> None:
        sender = self._get_sender_client()
        target_peer = await self._resolve_target_peer(target_id)
        file_info = getattr(message, "file", None)
        media_size = getattr(file_info, "size", None)
        if media_size and media_size > MEDIA_MAX_SIZE_BYTES:
            logger.info(
                f"Session {self.session.session_id}: Skip media >15MB, target={target_id}, "
                f"message_id={getattr(message, 'id', 'unknown')}, size={media_size}"
            )
            if caption:
                await self._send_text(target_id, caption, entities=caption_entities, reply_to=reply_to)
            return

        self._cleanup_temp_media_dir()
        temp_file_path = await self.client.download_media(message, file=MEDIA_TEMP_DIR)
        if not temp_file_path:
            # Retry with explicit temp path for edge cases where directory download returns None.
            suffix = ""
            if file_info and getattr(file_info, "ext", None):
                suffix = file_info.ext
            fd, manual_path = tempfile.mkstemp(prefix="mirror-", suffix=suffix, dir=MEDIA_TEMP_DIR)
            os.close(fd)
            try:
                retry_result = await self.client.download_media(message, file=manual_path)
                if isinstance(retry_result, str) and os.path.exists(retry_result):
                    temp_file_path = retry_result
                elif os.path.exists(manual_path) and os.path.getsize(manual_path) > 0:
                    temp_file_path = manual_path
                else:
                    os.remove(manual_path)
            except Exception as e:
                if os.path.exists(manual_path):
                    os.remove(manual_path)
                logger.warning(
                    f"Session {self.session.session_id}: Media download retry failed: {e}"
                )

        if not temp_file_path:
            logger.warning(
                f"Session {self.session.session_id}: Media download returned empty result, "
                f"target={target_id}, message_id={getattr(message, 'id', 'unknown')}"
            )
            if caption:
                await self._send_text(target_id, caption, entities=caption_entities, reply_to=reply_to)
            return

        try:
            kwargs = {}
            if reply_to:
                kwargs["reply_to"] = reply_to
            if getattr(message, "voice", False):
                kwargs["voice_note"] = True
            if getattr(message, "video_note", False):
                kwargs["video_note"] = True
            if getattr(message, "video", False):
                kwargs["supports_streaming"] = True

            # Stickers do not support captions.
            if not getattr(message, "sticker", False):
                kwargs["caption"] = caption
                if caption_entities:
                    kwargs["formatting_entities"] = caption_entities

            await sender.send_file(target_peer, file=temp_file_path, **kwargs)
            logger.info(
                f"Session {self.session.session_id}: Media sent by bot to {target_id}, "
                f"message_id={getattr(message, 'id', 'unknown')}, file={temp_file_path}"
            )
        except Exception as e:
            logger.warning(
                f"Session {self.session.session_id}: Failed to send media to {target_id}: {e}"
            )
            if caption:
                await self._send_text(target_id, caption, entities=caption_entities, reply_to=reply_to)
        finally:
            if isinstance(temp_file_path, str) and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except Exception:
                    logger.debug(f"Failed to remove temp media file: {temp_file_path}")

    def _cleanup_temp_media_dir(self) -> None:
        try:
            if not os.path.isdir(MEDIA_TEMP_DIR):
                return
            now = time.time()
            files = []
            for name in os.listdir(MEDIA_TEMP_DIR):
                path = os.path.join(MEDIA_TEMP_DIR, name)
                if not os.path.isfile(path):
                    continue
                try:
                    mtime = os.path.getmtime(path)
                    files.append((mtime, path))
                    if now - mtime > MEDIA_TTL_SECONDS:
                        os.remove(path)
                except Exception:
                    continue

            # Keep directory bounded even when TTL hasn't elapsed yet.
            files = [(mtime, path) for mtime, path in files if os.path.exists(path)]
            if len(files) > MEDIA_CLEANUP_KEEP_FILES:
                files.sort()  # oldest first
                for _, path in files[: len(files) - MEDIA_CLEANUP_KEEP_FILES]:
                    try:
                        os.remove(path)
                    except Exception:
                        pass
        except Exception as e:
            logger.debug(f"Temp media cleanup failed: {e}")

    async def _periodic_reload_bridges(self) -> None:
        """Reload bridges every 30 seconds to catch new/deleted bridges."""
        while self.running:
            await asyncio.sleep(ROUTING_RELOAD_DELAY)
            if not self.running:
                break
            await self._maybe_reload_routes(force=False)
            await self._scan_for_new_forum_topics()

    async def _maybe_reload_routes(self, force: bool = False) -> None:
        current_token = get_route_reload_token()
        if not force and current_token <= self.route_reload_token:
            return
        old_signature = (
            len(self.bridges),
            sum(len(items) for items in self.direct_bridges.values()),
            sum(len(items) for items in self.topic_rules.values()),
        )
        await self._load_bridges()
        self.route_reload_token = current_token
        new_signature = (
            len(self.bridges),
            sum(len(items) for items in self.direct_bridges.values()),
            sum(len(items) for items in self.topic_rules.values()),
        )
        if old_signature != new_signature:
            logger.info(
                f"Session {self.session.session_id}: Routing reloaded: {old_signature} -> {new_signature}"
            )
            logger.info(f"  Current sources: {list(self.direct_bridges.keys())}")
        await self._ensure_source_baselines()

    async def _load_bridges(self) -> None:
        """Load active bridges for this session."""
        all_bridges = get_active_bridges()
        session_bridges = [b for b in all_bridges if b.session_id == self.session.session_id]
        self.bridges = {b.id: b for b in session_bridges}
        self.direct_bridges = {}
        self.forum_bridges = {}
        for bridge in session_bridges:
            if bridge.source_type in {"channel", "chat"} and bridge.target_type != "forum":
                self.direct_bridges.setdefault(bridge.source_id, []).append(bridge)
            elif bridge.source_type == "forum":
                self.forum_bridges.setdefault(bridge.source_id, []).append(bridge)

        self.topic_rules = {}
        active_rules = get_active_topic_rules()
        for rule in active_rules:
            bridge = self.bridges.get(rule.bridge_id)
            if not bridge:
                continue
            key = (rule.source_chat_id, rule.source_thread_id)
            self.topic_rules.setdefault(key, []).append((rule, bridge))

    async def _ensure_source_baselines(self) -> None:
        all_source_ids = (
            set(self.direct_bridges.keys())
            | set(self.forum_bridges.keys())
            | {key[0] for key in self.topic_rules.keys()}
        )
        for source_chat_id in all_source_ids:
            if source_chat_id in self.source_baselines:
                continue
            try:
                entity = await self._resolve_source_entity(source_chat_id)
                last_message = await self.client.get_messages(entity, limit=1)
                self.source_baselines[source_chat_id] = last_message[0].id if last_message else 0
            except Exception as e:
                logger.debug(f"Failed to initialize source baseline for {source_chat_id}: {e}")
                self.source_baselines[source_chat_id] = 0

    async def _poll_sources(self) -> None:
        while self.running:
            await asyncio.sleep(SOURCE_POLL_DELAY)
            if not self.running:
                break
            await self._maybe_reload_routes(force=False)
            all_source_ids = (
                set(self.direct_bridges.keys())
                | set(self.forum_bridges.keys())
                | {key[0] for key in self.topic_rules.keys()}
            )
            for source_chat_id in all_source_ids:
                await self._poll_source_chat(source_chat_id)

    async def _poll_source_chat(self, source_chat_id: int) -> None:
        try:
            entity = await self._resolve_source_entity(source_chat_id)
            last_seen = self.source_baselines.get(source_chat_id, 0)
            messages = await self.client.get_messages(entity, limit=10, min_id=last_seen)
            if not messages:
                return

            for message in reversed(messages):
                if getattr(message, "out", False):
                    continue
                message_key = (source_chat_id, message.id)
                if message_key in self.processed_messages:
                    continue
                self.source_baselines[source_chat_id] = max(self.source_baselines.get(source_chat_id, 0), message.id)

                class PolledEvent:
                    def __init__(self, client, message):
                        self.client = client
                        self.message = message
                        self.chat_id = message.chat_id
                        self.raw_text = message.raw_text or ""
                        self.sender_id = message.sender_id

                    async def get_sender(self):
                        return await self.client.get_entity(self.message.sender_id) if self.message.sender_id else None

                    async def get_input_chat(self):
                        return await self.client.get_input_entity(self.chat_id)

                await self._process_message(PolledEvent(self.client, message))
        except Exception as e:
            logger.debug(f"Polling failed for source {source_chat_id}: {e}")

    def _setup_handlers(self) -> None:
        """Set up message handlers for all source chats."""
        if not self.bridges:
            return

        source_ids = sorted(set(self.direct_bridges.keys()) | set(self.forum_bridges.keys()))
        logger.info(f"Session {self.session.session_id}: Setting up handlers for {len(source_ids)} chats: {source_ids}")

        # Debug handler - catch ALL messages first
        @self.client.on(events.NewMessage)
        async def debug_all_messages(event: events.NewMessage.Event) -> None:
            await self._maybe_reload_routes(force=False)
            chat_id = event.chat_id
            msg_text = event.raw_text[:50] if event.raw_text else "[no text]"
            
            # Full event debugging
            logger.info(f"DEBUG Session {self.session.session_id}: Message from {chat_id}: {msg_text}")
            logger.info(f"  Message ID: {event.message.id}")
            logger.info(f"  Is Forum: {getattr(event.message, 'is_forum', False)}")
            logger.info(f"  Reply to: {event.message.reply_to}")
            if event.message.reply_to:
                logger.info(f"    Reply to msg ID: {event.message.reply_to.reply_to_msg_id}")
            source_chat_id = self._normalize_chat_id(chat_id)
            source_thread_id = self._get_event_source_thread_id(event)
            logger.info(f"  Direct bridge keys: {list(self.direct_bridges.keys())}")
            logger.info(f"  Forum bridge keys: {list(self.forum_bridges.keys())}")
            logger.info(f"  Topic rule keys: {list(self.topic_rules.keys())}")
            should_process = bool(self.direct_bridges.get(source_chat_id) or self.forum_bridges.get(source_chat_id))
            if not should_process:
                should_process = bool(
                    self.topic_rules.get((source_chat_id, source_thread_id))
                    or (source_thread_id == 0 and self.topic_rules.get((source_chat_id, GENERAL_TOPIC_ID)))
                )

            if should_process:
                logger.info(
                    f"  Will process with source_chat={source_chat_id}, source_thread={source_thread_id}"
                )
                await self._process_message(event)
            else:
                logger.info(f"  Skipped - no bridge match")

        # Original filtered handler (backup)
        # @self.client.on(events.NewMessage(chats=source_ids))
        # async def handle_message(event: events.NewMessage.Event) -> None:
        #     await self._process_message(event)

    async def _get_sender_status(self, event: events.NewMessage.Event, sender) -> str | None:
        """Resolve sender role inside the source chat."""
        try:
            chat = await event.get_input_chat()
            permissions = await self.client.get_permissions(chat, sender)
            if permissions and getattr(permissions, "is_creator", False):
                return "owner"
            if permissions and getattr(permissions, "is_admin", False):
                return "admin"
        except Exception as e:
            logger.debug(f"Failed to resolve sender status: {e}")
        return None

    async def _build_sender_header(self, event: events.NewMessage.Event) -> str:
        """Build sender header."""
        sender = await event.get_sender()
        sender_id = getattr(sender, "id", None) or event.sender_id or 0
        sender_ref = f"@{sender.username}" if getattr(sender, "username", None) else str(sender_id)
        sender_name = (
            getattr(sender, "title", None)
            or " ".join(
                part
                for part in [getattr(sender, "first_name", None), getattr(sender, "last_name", None)]
                if part
            ).strip()
            or (f"@{sender.username}" if getattr(sender, "username", None) else None)
            or "Unknown"
        )
        status = await self._get_sender_status(event, sender or sender_id)
        header = f"👤 {sender_name}({sender_ref})"
        if status:
            header = f"{header} {status}"
        return header

    @staticmethod
    def _normalize_chat_id(chat_id: int | None) -> int:
        if not chat_id:
            return 0
        return int(str(chat_id).replace("-100", "").replace("-", ""))

    @staticmethod
    def _get_event_source_thread_id(event: events.NewMessage.Event) -> int:
        message = event.message
        if not message.reply_to:
            return 0

        reply = message.reply_to
        if not getattr(reply, "forum_topic", False):
            return 0

        for attr_name in ("reply_to_top_id", "top_msg_id", "reply_to_msg_id"):
            value = getattr(reply, attr_name, None)
            if value:
                return value
        return 0

    async def _resolve_source_entity(self, source_chat_id: int):
        try:
            return await self.client.get_entity(-source_chat_id)
        except Exception:
            return await self.client.get_entity(int(f"-100{source_chat_id}"))

    async def _get_source_forum_topics(self, source_chat_id: int) -> list[tuple[int, str]]:
        try:
            channel_id = int(str(source_chat_id).replace("-100", ""))
            entity = await self.client.get_input_entity(PeerChannel(channel_id))
            result = await self.client(
                functions.channels.GetForumTopicsRequest(
                    channel=entity,
                    offset_date=None,
                    offset_id=0,
                    offset_topic=0,
                    limit=100,
                    q=None,
                )
            )
            topics = []
            for topic in result.topics:
                title = getattr(topic, "title", None) or f"Topic {topic.id}"
                topics.append((topic.id, title))
            return topics
        except Exception as e:
            logger.debug(f"Failed to get source forum topics for {source_chat_id}: {e}")
            return []

    async def _get_source_topic_title(self, source_id: int, topic_id: int) -> str | None:
        """Get exact topic title from source forum."""
        try:
            from telethon import functions

            channel_id = int(str(source_id).replace("-100", ""))
            entity = await self.client.get_input_entity(PeerChannel(channel_id))

            result = await self.client(
                functions.channels.GetForumTopicsByIDRequest(
                    channel=entity,
                    topics=[topic_id],
                )
            )

            if result.topics:
                return getattr(result.topics[0], "title", None)
            return None
        except Exception as e:
            logger.debug(f"Failed to get topic title: {e}")
            return None

    async def _get_or_create_target_topic(
        self, bridge: BridgeEntry, source_thread_id: int, source_id: int
    ) -> int:
        """Get or create target topic for a source topic. Returns target_thread_id."""
        # Check if we already have a mapping
        mapping = get_topic_mapping(bridge.id, source_thread_id)
        if mapping:
            return mapping[0]

        # Need to create a new topic in target forum
        try:
            # Get real topic title from source
            topic_title = await self._get_source_topic_title(source_id, source_thread_id)
            title = topic_title or f"Topic {source_thread_id}"
            
            # Get channel entity with proper access_hash
            channel_id = int(str(bridge.target_id).replace("-100", ""))
            entity = await self.client.get_entity(PeerChannel(channel_id))
            
            result = await self.client(
                CreateForumTopicRequest(
                    channel=entity,
                    title=title,
                )
            )
            # The new topic ID is in the updates
            target_thread_id = result.updates[0].id if result.updates else 1
            logger.info(f"Created target topic '{title}' with ID {target_thread_id}")
            
            # Save mapping
            create_topic_mapping(bridge.id, source_thread_id, target_thread_id, title)
            return target_thread_id
        except Exception as e:
            logger.exception(f"Failed to create topic: {e}")
            return 1  # Fallback to General

    @staticmethod
    def _topic_proposal_keyboard(proposal_id: int) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Подключить ветку",
                        callback_data=f"topic_proposal_accept:{proposal_id}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="Скрыть",
                        callback_data=f"topic_proposal_dismiss:{proposal_id}",
                    )
                ],
            ]
        )

    async def _notify_topic_proposal(self, proposal: TopicProposal) -> None:
        if not notification_bot:
            logger.warning("BOT_TOKEN is missing, cannot notify about topic proposals")
            return

        text = (
            "<b>Найдена новая ветка</b>\n\n"
            f"Форум-источник: <b>{html.escape(proposal.bridge_source_title)}</b>\n"
            f"Новая ветка: <b>{html.escape(proposal.source_title)}</b>\n"
            f"Зеркало: <b>{html.escape(proposal.bridge_target_title)}</b>\n\n"
            "Подключить эту ветку в зеркало?"
        )
        await notification_bot.send_message(
            proposal.user_id,
            text,
            reply_markup=self._topic_proposal_keyboard(proposal.id),
        )
        mark_topic_proposal_notified(proposal.id)

    async def _handle_unknown_forum_topic(
        self,
        source_chat_id: int,
        source_thread_id: int,
    ) -> None:
        if source_thread_id <= GENERAL_TOPIC_ID:
            return

        forum_bridges = self.forum_bridges.get(source_chat_id, [])
        if not forum_bridges:
            return

        source_title = await self._get_source_topic_title(source_chat_id, source_thread_id)
        topic_title = source_title or f"Topic {source_thread_id}"

        for bridge in forum_bridges:
            proposal = get_topic_proposal_by_source(bridge.id, source_chat_id, source_thread_id)
            if proposal:
                if proposal.status == "pending" and not proposal.notified_at:
                    try:
                        await self._notify_topic_proposal(proposal)
                    except Exception as e:
                        logger.exception(
                            f"Session {self.session.session_id}: Failed to retry topic proposal notification: {e}"
                        )
                continue

            proposal, created = create_topic_proposal(
                bridge_id=bridge.id,
                source_chat_id=source_chat_id,
                source_thread_id=source_thread_id,
                source_title=topic_title,
            )
            if created and proposal.status == "pending":
                try:
                    await self._notify_topic_proposal(proposal)
                    logger.info(
                        f"Session {self.session.session_id}: Proposed new topic '{topic_title}' "
                        f"for bridge {bridge.id}"
                    )
                except Exception as e:
                    logger.exception(
                        f"Session {self.session.session_id}: Failed to notify about topic proposal: {e}"
                    )

    async def _scan_for_new_forum_topics(self) -> None:
        for source_chat_id, forum_bridges in self.forum_bridges.items():
            topics = await self._get_source_forum_topics(source_chat_id)
            if not topics:
                continue

            for topic_id, topic_title in topics:
                if topic_id <= GENERAL_TOPIC_ID:
                    continue

                has_rule = bool(self.topic_rules.get((source_chat_id, topic_id)))
                if has_rule:
                    continue

                for bridge in forum_bridges:
                    if get_topic_proposal_by_source(bridge.id, source_chat_id, topic_id):
                        continue

                    proposal, created = create_topic_proposal(
                        bridge_id=bridge.id,
                        source_chat_id=source_chat_id,
                        source_thread_id=topic_id,
                        source_title=topic_title,
                    )
                    if not created or proposal.status != "pending":
                        continue

                    try:
                        await self._notify_topic_proposal(proposal)
                        logger.info(
                            f"Session {self.session.session_id}: Proposed forum topic by scan "
                            f"'{topic_title}' for bridge {bridge.id}"
                        )
                    except Exception as e:
                        logger.exception(
                            f"Session {self.session.session_id}: Failed to notify scanned topic proposal: {e}"
                        )

    async def _process_message(self, event: events.NewMessage.Event) -> None:
        """Process incoming message and forward if matches filters."""
        try:
            source_id = event.chat_id
            msg_id = event.message.id
            source_chat_id = self._normalize_chat_id(source_id)
            source_thread_id = self._get_event_source_thread_id(event)
            message_key = (source_chat_id, msg_id)
            if message_key in self.processed_messages:
                logger.info(f"Session {self.session.session_id}: Skip duplicate message {message_key}")
                return
            self.processed_messages.add(message_key)
            self.source_baselines[source_chat_id] = max(self.source_baselines.get(source_chat_id, 0), msg_id)
            
            logger.info(f"Session {self.session.session_id}: Got message {msg_id} from chat {source_id}")
            logger.info(f"  Normalized source_chat={source_chat_id}, source_thread={source_thread_id}")

            incoming_text = event.raw_text or ""
            source_text = event.message.message or incoming_text or ""
            is_webpage_preview = isinstance(event.message.media, MessageMediaWebPage)
            has_media = event.message.media is not None and not is_webpage_preview
            entities = getattr(event.message, "entities", None) or []
            has_text = bool(source_text.strip())
            has_entities = bool(has_text and entities)

            logger.info(
                f"  Text length: {len(source_text)}, has_media: {has_media}, "
                f"is_webpage_preview: {is_webpage_preview}, entities: {len(entities)}, "
                f"has_entities: {has_entities}"
            )

            if not has_text and not has_media:
                logger.info(f"Session {self.session.session_id}: Skipped empty message from {source_id}")
                return

            sender_header = await self._build_sender_header(event)
            body_plain = process_message(incoming_text)
            body_with_entities = source_text
            direct_targets = self.direct_bridges.get(source_chat_id, [])
            topic_targets = list(self.topic_rules.get((source_chat_id, source_thread_id), []))
            if source_thread_id == 0:
                topic_targets.extend(self.topic_rules.get((source_chat_id, GENERAL_TOPIC_ID), []))
            routed = False

            for bridge in direct_targets:
                keywords = parse_keywords(bridge.keywords)
                logger.info(f"  Direct bridge {bridge.id} keywords: {keywords}")
                if has_text and keywords and not contains_keywords(source_text, keywords):
                    logger.info(f"  Direct bridge {bridge.id} skipped by keywords")
                    continue

                if has_media:
                    await self._send_text(bridge.target_id, sender_header)
                    await self._send_media(
                        bridge.target_id,
                        event.message,
                        caption=body_with_entities if has_text else None,
                        caption_entities=entities if has_entities and has_text else None,
                    )
                else:
                    if has_entities and has_text:
                        await self._send_text(bridge.target_id, sender_header)
                        await self._send_text(
                            bridge.target_id,
                            body_with_entities,
                            entities=entities,
                        )
                    else:
                        outgoing_text = f"{sender_header}\n\n{body_plain}" if body_plain else sender_header
                        await self._send_text(bridge.target_id, outgoing_text)
                logger.info(f"Session {self.session.session_id}: ✅ Forwarded via direct bridge {bridge.id}")
                routed = True

            for rule, bridge in topic_targets:
                keywords = parse_keywords(bridge.keywords)
                logger.info(f"  Topic rule {rule.id} keywords: {keywords}")
                if has_text and keywords and not contains_keywords(source_text, keywords):
                    logger.info(f"  Topic rule {rule.id} skipped by keywords")
                    continue

                include_header = bool(getattr(rule, "header_enabled", True))
                if has_media:
                    if include_header:
                        await self._send_text(
                            rule.target_chat_id,
                            sender_header,
                            reply_to=rule.target_thread_id,
                        )
                    await self._send_media(
                        rule.target_chat_id,
                        event.message,
                        caption=body_with_entities if has_text else None,
                        caption_entities=entities if has_entities and has_text else None,
                        reply_to=rule.target_thread_id,
                    )
                else:
                    if has_entities and has_text:
                        if include_header:
                            await self._send_text(
                                rule.target_chat_id,
                                sender_header,
                                reply_to=rule.target_thread_id,
                            )
                            await self._send_text(
                                rule.target_chat_id,
                                body_with_entities,
                                entities=entities,
                                reply_to=rule.target_thread_id,
                            )
                        else:
                            await self._send_text(
                                rule.target_chat_id,
                                body_with_entities,
                                entities=entities,
                                reply_to=rule.target_thread_id,
                            )
                    else:
                        if include_header:
                            outgoing_text = (
                                f"{sender_header}\n\n{body_plain}" if body_plain else sender_header
                            )
                        else:
                            outgoing_text = body_plain
                        await self._send_text(
                            rule.target_chat_id,
                            outgoing_text,
                            reply_to=rule.target_thread_id,
                        )
                logger.info(
                    f"Session {self.session.session_id}: ✅ Forwarded via topic rule {rule.id} "
                    f"to thread {rule.target_thread_id}"
                )
                routed = True

            if not routed:
                await self._handle_unknown_forum_topic(source_chat_id, source_thread_id)
                logger.info(
                    f"Session {self.session.session_id}: No routing rule matched for "
                    f"source_chat={source_chat_id}, source_thread={source_thread_id}"
                )

        except RPCError as e:
            logger.exception(f"Session {self.session.session_id}: Telegram API error: {e}")
        except Exception as e:
            logger.exception(f"Session {self.session.session_id}: Error processing message: {e}")

    async def stop(self) -> None:
        """Stop the worker and disconnect."""
        if self.sender_client and self.sender_client is not self.client:
            await self.sender_client.disconnect()
        if self.client:
            await self.client.disconnect()
        self.running = False


class BridgeManager:
    """Manages multiple session workers."""

    def __init__(self):
        self.workers: Dict[int, SessionWorker] = {}
        self.tasks: Dict[int, asyncio.Task] = {}

    async def run(self) -> None:
        """Main loop: periodically check for new sessions and manage workers."""
        init_db()

        logger.add(
            "logs/bridge.log",
            rotation="10 MB",
            retention="7 days",
            level="INFO",
        )
        logger.add(
            lambda msg: print(msg, end=""),
            level="INFO",
            colorize=True,
        )

        logger.info("Bridge Manager started")

        while True:
            try:
                await self._sync_workers()
            except Exception:
                logger.exception("Manager error")
            await asyncio.sleep(30)  # Check every 30 seconds

    async def _sync_workers(self) -> None:
        """Sync active workers with database state."""
        from src.database import get_user_sessions

        # Get all bridges and group by session
        bridges = get_active_bridges()
        active_session_ids = set(b.session_id for b in bridges)

        # Get session info for each
        sessions_info = {}
        for session_id in active_session_ids:
            session = get_session(session_id)
            if session:
                sessions_info[session_id] = session

        # Add new workers
        for session_id, session in sessions_info.items():
            if session_id not in self.workers:
                worker = SessionWorker(session)
                self.workers[session_id] = worker
                self.tasks[session_id] = asyncio.create_task(worker.start())
                logger.info(f"Started worker for session {session_id} (user {session.user_id})")

        # Remove workers with no bridges
        for session_id in list(self.workers.keys()):
            if session_id not in active_session_ids:
                worker = self.workers.pop(session_id)
                await worker.stop()
                if session_id in self.tasks:
                    self.tasks[session_id].cancel()
                    del self.tasks[session_id]
                logger.info(f"Stopped worker for session {session_id}")


async def main() -> None:
    manager = BridgeManager()
    await manager.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bridge stopped by user.")
