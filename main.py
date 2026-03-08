"""Telegram Group Bridge - Multi-Session Worker."""

import asyncio
from typing import Dict, List

from loguru import logger
from telethon import TelegramClient, events
from telethon.errors import RPCError
from telethon.sessions import StringSession

from src.database import (
    BridgeEntry,
    UserSession,
    get_active_bridges,
    get_session,
    init_db,
)
from src.parser import contains_keywords, process_message

RECONNECT_DELAY = 5


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
        self.running = False

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
        self._setup_handlers()

        if self.bridges:
            logger.info(f"Session {self.session.session_id}: Listening {len(self.bridges)} sources")
            self.running = True
            await self.client.run_until_disconnected()
        else:
            logger.info(f"Session {self.session.session_id}: No bridges, disconnecting")
            await self.client.disconnect()
            await asyncio.sleep(30)

    async def _load_bridges(self) -> None:
        """Load active bridges for this session."""
        all_bridges = get_active_bridges()
        session_bridges = [b for b in all_bridges if b.session_id == self.session.session_id]
        self.bridges = {b.source_id: b for b in session_bridges}

    def _setup_handlers(self) -> None:
        """Set up message handlers for all source chats."""
        if not self.bridges:
            return

        source_ids = list(self.bridges.keys())
        logger.info(f"Session {self.session.session_id}: Setting up handlers for {len(source_ids)} chats: {source_ids}")

        # Debug handler - catch ALL messages first
        @self.client.on(events.NewMessage)
        async def debug_all_messages(event: events.NewMessage.Event) -> None:
            chat_id = event.chat_id
            msg_text = event.raw_text[:50] if event.raw_text else "[no text]"
            logger.info(f"DEBUG Session {self.session.session_id}: Message from {chat_id}: {msg_text}")
            # Only process if matches our bridges
            if chat_id in self.bridges or (str(chat_id).startswith("-100") and int(str(chat_id).replace("-100", "")) in self.bridges):
                await self._process_message(event)

        # Original filtered handler (backup)
        # @self.client.on(events.NewMessage(chats=source_ids))
        # async def handle_message(event: events.NewMessage.Event) -> None:
        #     await self._process_message(event)

    async def _process_message(self, event: events.NewMessage.Event) -> None:
        """Process incoming message and forward if matches filters."""
        try:
            source_id = event.chat_id
            msg_id = event.message.id
            
            logger.info(f"Session {self.session.session_id}: Got message {msg_id} from chat {source_id}")
            logger.info(f"  Available bridges: {list(self.bridges.keys())}")
            
            # Handle both formats: with and without -100 prefix
            bridge = self.bridges.get(source_id)
            if not bridge and source_id < 0:
                # Try without -100 prefix (e.g., -1001234567890 -> 1234567890)
                alt_id = int(str(source_id).replace("-100", "").replace("-", ""))
                bridge = self.bridges.get(alt_id)
                logger.info(f"  Trying alt_id {alt_id}: {bridge is not None}")
            if not bridge and source_id > 0:
                # Try with -100 prefix
                alt_id = int(f"-100{source_id}")
                bridge = self.bridges.get(alt_id)
                logger.info(f"  Trying alt_id {alt_id}: {bridge is not None}")

            if not bridge:
                logger.warning(f"Session {self.session.session_id}: No bridge found for {source_id}")
                return

            logger.info(f"  Found bridge: {bridge.source_title} -> {bridge.target_title}")

            incoming_text = event.raw_text or ""
            has_media = event.message.media is not None

            logger.info(f"  Text length: {len(incoming_text)}, has_media: {has_media}")

            if not incoming_text.strip() and not has_media:
                logger.info(f"Session {self.session.session_id}: Skipped empty message from {source_id}")
                return

            # Check keywords filter
            keywords = parse_keywords(bridge.keywords)
            logger.info(f"  Keywords filter: {keywords}")
            
            if incoming_text.strip() and keywords:
                if not contains_keywords(incoming_text, keywords):
                    logger.info(f"Session {self.session.session_id}: Skipped message - no keyword match")
                    return
                else:
                    logger.info(f"  Keyword match found!")

            outgoing_text = process_message(incoming_text)
            logger.info(f"  Sending to target {bridge.target_id}: {outgoing_text[:50]}...")

            # Send message to target
            if has_media:
                await self.client.send_file(
                    bridge.target_id,
                    file=event.message.media,
                    caption=outgoing_text if incoming_text.strip() else None,
                )
            else:
                await self.client.send_message(bridge.target_id, outgoing_text)

            logger.info(
                f"Session {self.session.session_id}: ✅ Forwarded from {bridge.source_title} to {bridge.target_title}"
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
