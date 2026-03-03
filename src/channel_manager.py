import logging

from telethon import TelegramClient
from telethon.tl.functions.channels import CreateChannelRequest
from telethon.tl.functions.messages import CreateChatRequest
from telethon.tl.types import InputPeerChannel, InputPeerChat

logger = logging.getLogger(__name__)


class ChannelManager:
    def __init__(self, client: TelegramClient):
        self.client = client

    async def create_target_for_source(
        self, source_title: str, source_type: str
    ) -> tuple[int, int, str] | None:
        """Create target channel/chat. Returns (internal_id, target_id, title) or None."""
        try:
            if source_type == "channel":
                return await self._create_channel(source_title)
            else:
                return await self._create_chat(source_title)
        except Exception as e:
            logger.exception(f"Failed to create target for {source_title}")
            return None

    async def _create_channel(self, source_title: str) -> tuple[int, int, str]:
        title = f"MIRROR: {source_title}"
        about = f"MIRROR of {source_title}"

        logger.info(f"Creating channel: {title}")
        result = await self.client(CreateChannelRequest(
            title=title,
            about=about,
            megagroup=False,
            broadcast=True,
        ))

        channel = result.chats[0]
        target_id = int(f"-100{channel.id}")
        logger.info(f"Created channel: id={channel.id}, target_id={target_id}")
        return channel.id, target_id, title

    async def _create_chat(self, source_title: str) -> tuple[int, int, str]:
        title = f"MIRROR: {source_title}"
        me = await self.client.get_me()

        if not me:
            raise RuntimeError("Cannot get current user")

        logger.info(f"Creating chat: {title}")
        result = await self.client(CreateChatRequest(
            users=[me],
            title=title,
        ))

        chat = result.chats[0]
        target_id = -chat.id  # Regular chat ID format
        logger.info(f"Created chat: id={chat.id}, target_id={target_id}")
        return chat.id, target_id, title

    async def resolve_source(self, link_or_id: str) -> tuple[int, str, str] | None:
        """Resolve source link/ID to (source_id, source_type, title)."""
        try:
            if link_or_id.startswith("https://t.me/") or link_or_id.startswith("@"):
                entity = await self.client.get_entity(link_or_id)
            else:
                entity = await self.client.get_entity(int(link_or_id))

            source_id = int(entity.id)

            # Determine type
            if hasattr(entity, "broadcast") and entity.broadcast:
                source_type = "channel"
            elif hasattr(entity, "megagroup") and entity.megagroup:
                source_type = "chat"
            elif hasattr(entity, "participants_count"):
                source_type = "chat"
            else:
                source_type = "channel"

            title = getattr(entity, "title", None) or getattr(entity, "first_name", "Unknown")
            logger.info(f"Resolved source: {link_or_id} -> {title} ({source_type}, id={source_id})")
            return source_id, source_type, title

        except Exception as e:
            logger.exception(f"Failed to resolve source: {link_or_id}")
            return None
