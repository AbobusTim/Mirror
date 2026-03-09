"""Telegram Group Bridge - Multi-Session Worker."""

import asyncio
import html
import os
from typing import Dict, List, Set, Tuple

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from dotenv import load_dotenv
from loguru import logger
from telethon import TelegramClient, events
from telethon.errors import RPCError
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
                    await self.client.send_message(bridge.target_id, sender_header)
                    file_kw = {}
                    if has_entities and has_text:
                        file_kw["formatting_entities"] = entities
                    await self.client.send_file(
                        bridge.target_id,
                        file=event.message.media,
                        caption=body_with_entities if has_text else None,
                        **file_kw,
                    )
                else:
                    if has_entities and has_text:
                        await self.client.send_message(bridge.target_id, sender_header)
                        await self.client.send_message(
                            bridge.target_id,
                            body_with_entities,
                            formatting_entities=entities,
                        )
                    else:
                        outgoing_text = f"{sender_header}\n\n{body_plain}" if body_plain else sender_header
                        await self.client.send_message(bridge.target_id, outgoing_text)
                logger.info(f"Session {self.session.session_id}: ✅ Forwarded via direct bridge {bridge.id}")
                routed = True

            for rule, bridge in topic_targets:
                keywords = parse_keywords(bridge.keywords)
                logger.info(f"  Topic rule {rule.id} keywords: {keywords}")
                if has_text and keywords and not contains_keywords(source_text, keywords):
                    logger.info(f"  Topic rule {rule.id} skipped by keywords")
                    continue

                send_kwargs = {"reply_to": rule.target_thread_id}
                if has_media:
                    await self.client.send_message(rule.target_chat_id, sender_header, **send_kwargs)
                    file_kw = {"reply_to": rule.target_thread_id}
                    if has_entities and has_text:
                        file_kw["formatting_entities"] = entities
                    await self.client.send_file(
                        rule.target_chat_id,
                        file=event.message.media,
                        caption=body_with_entities if has_text else None,
                        **file_kw,
                    )
                else:
                    if has_entities and has_text:
                        await self.client.send_message(rule.target_chat_id, sender_header, **send_kwargs)
                        await self.client.send_message(
                            rule.target_chat_id,
                            body_with_entities,
                            formatting_entities=entities,
                            **send_kwargs,
                        )
                    else:
                        outgoing_text = f"{sender_header}\n\n{body_plain}" if body_plain else sender_header
                        await self.client.send_message(rule.target_chat_id, outgoing_text, **send_kwargs)
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
