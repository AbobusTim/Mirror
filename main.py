"""Telegram Group Bridge - Multi-User Worker."""

import asyncio
from typing import Dict, List

from loguru import logger
from telethon import TelegramClient, events
from telethon.errors import RPCError
from telethon.sessions import StringSession

from src.database import BridgeEntry, get_active_bridges, init_db
from src.parser import contains_keywords, process_message

RECONNECT_DELAY = 5


def parse_keywords(keywords_str: str) -> List[str]:
    if not keywords_str:
        return []
    return [k.strip().lower() for k in keywords_str.split(",") if k.strip()]


class UserWorker:
    """Worker for a single user with their own Telegram session."""

    def __init__(self, user_id: int, api_id: int, api_hash: str, session_string: str):
        self.user_id = user_id
        self.api_id = api_id
        self.api_hash = api_hash
        self.session_string = session_string
        self.client: TelegramClient | None = None
        self.bridges: Dict[int, BridgeEntry] = {}
        self.running = False

    async def start(self) -> None:
        """Start the worker and connect to Telegram."""
        self.client = TelegramClient(
            StringSession(self.session_string),
            self.api_id,
            self.api_hash,
        )

        while True:
            try:
                await self._connect_and_run()
            except Exception:
                logger.exception(f"User {self.user_id}: Connection error. Reconnecting...")
                await asyncio.sleep(RECONNECT_DELAY)

    async def _connect_and_run(self) -> None:
        logger.info(f"User {self.user_id}: Connecting to Telegram...")
        await self.client.connect()

        if not await self.client.is_user_authorized():
            logger.error(f"User {self.user_id}: Not authorized, skipping")
            return

        await self._load_bridges()
        self._setup_handlers()

        if self.bridges:
            logger.info(f"User {self.user_id}: Listening {len(self.bridges)} sources")
            self.running = True
            await self.client.run_until_disconnected()
        else:
            logger.info(f"User {self.user_id}: No bridges, disconnecting")
            await self.client.disconnect()
            # Retry after delay to check for new bridges
            await asyncio.sleep(30)

    async def _load_bridges(self) -> None:
        """Load active bridges for this user."""
        all_bridges = get_active_bridges()
        user_bridges = [b for b in all_bridges if b.user_id == self.user_id]
        self.bridges = {b.source_id: b for b in user_bridges}

    def _setup_handlers(self) -> None:
        """Set up message handlers for all source chats."""
        if not self.bridges:
            return

        source_ids = list(self.bridges.keys())

        @self.client.on(events.NewMessage(chats=source_ids))
        async def handle_message(event: events.NewMessage.Event) -> None:
            await self._process_message(event)

    async def _process_message(self, event: events.NewMessage.Event) -> None:
        """Process incoming message and forward if matches filters."""
        try:
            source_id = event.chat_id
            bridge = self.bridges.get(source_id)

            if not bridge:
                return

            incoming_text = event.raw_text or ""
            has_media = event.message.media is not None

            if not incoming_text.strip() and not has_media:
                logger.debug(f"User {self.user_id}: Skipped empty message from {source_id}")
                return

            # Check keywords filter
            keywords = parse_keywords(bridge.keywords)
            if incoming_text.strip() and keywords:
                if not contains_keywords(incoming_text, keywords):
                    logger.debug(f"User {self.user_id}: Skipped message from {source_id}: no keyword match")
                    return

            outgoing_text = process_message(incoming_text)

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
                f"User {self.user_id}: Forwarded from {bridge.source_title} to {bridge.target_title} "
                f"(media={has_media})"
            )

        except RPCError:
            logger.exception(f"User {self.user_id}: Telegram API error")
        except Exception:
            logger.exception(f"User {self.user_id}: Error processing message")

    async def stop(self) -> None:
        """Stop the worker and disconnect."""
        if self.client:
            await self.client.disconnect()
        self.running = False


class BridgeManager:
    """Manages multiple user workers."""

    def __init__(self):
        self.workers: Dict[int, UserWorker] = {}
        self.tasks: Dict[int, asyncio.Task] = {}

    async def run(self) -> None:
        """Main loop: periodically check for new users and manage workers."""
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
        from src.database import get_user_credentials

        # Get all bridges and group by user
        bridges = get_active_bridges()
        active_user_ids = set(b.user_id for b in bridges)

        # Add new workers
        for user_id in active_user_ids:
            if user_id not in self.workers:
                creds = get_user_credentials(user_id)
                if creds and creds.session_string:
                    worker = UserWorker(
                        user_id=user_id,
                        api_id=creds.api_id,
                        api_hash=creds.api_hash,
                        session_string=creds.session_string,
                    )
                    self.workers[user_id] = worker
                    self.tasks[user_id] = asyncio.create_task(worker.start())
                    logger.info(f"Started worker for user {user_id}")

        # Remove workers with no bridges
        for user_id in list(self.workers.keys()):
            if user_id not in active_user_ids:
                worker = self.workers.pop(user_id)
                await worker.stop()
                if user_id in self.tasks:
                    self.tasks[user_id].cancel()
                    del self.tasks[user_id]
                logger.info(f"Stopped worker for user {user_id}")


async def main() -> None:
    manager = BridgeManager()
    await manager.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bridge stopped by user.")
