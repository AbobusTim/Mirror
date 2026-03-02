from telethon import TelegramClient
from telethon.tl.functions.channels import CreateChannelRequest
from telethon.tl.functions.messages import CreateChatRequest
from telethon.tl.types import InputPeerChannel, InputPeerChat


class ChannelManager:
    def __init__(self, client: TelegramClient):
        self.client = client

    async def create_target_for_source(
        self, source_title: str, source_type: str
    ) -> tuple[int, str]:
        if source_type == "channel":
            return await self._create_channel(source_title)
        else:
            return await self._create_chat(source_title)

    async def _create_channel(self, source_title: str) -> tuple[int, int, str]:
        title = f"MIRROR: {source_title}"
        about = f"MIRROR of {source_title}"

        result = await self.client(CreateChannelRequest(
            title=title,
            about=about,
            megagroup=False,
            broadcast=True,
        ))

        channel = result.chats[0]
        return channel.id, int(f"-100{channel.id}"), title

    async def _create_chat(self, source_title: str) -> tuple[int, int, str]:
        title = f"MIRROR: {source_title}"
        me = await self.client.get_me()

        result = await self.client(CreateChatRequest(
            users=[me],
            title=title,
        ))

        chat = result.chats[0]
        return chat.id, -chat.id, title

    async def resolve_source(self, link_or_id: str) -> tuple[int, str, str] | None:
        try:
            if link_or_id.startswith("https://t.me/") or link_or_id.startswith("@"):
                entity = await self.client.get_entity(link_or_id)
            else:
                entity = await self.client.get_entity(int(link_or_id))

            source_id = int(entity.id)

            if hasattr(entity, "broadcast") and entity.broadcast:
                source_type = "channel"
            elif hasattr(entity, "megagroup") and entity.megagroup:
                source_type = "chat"
            elif hasattr(entity, "participants_count"):
                source_type = "chat"
            else:
                source_type = "channel"

            return source_id, source_type, entity.title

        except Exception:
            return None
