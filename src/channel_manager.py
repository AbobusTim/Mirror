import logging
import asyncio

from telethon import TelegramClient
from telethon.tl.functions.channels import CreateChannelRequest, ToggleForumRequest
from telethon.tl.types import InputChannel, PeerChannel

logger = logging.getLogger(__name__)


class ChannelManager:
    def __init__(self, client: TelegramClient):
        self.client = client

    async def create_target_for_source(
        self, source_title: str, target_type: str
    ) -> tuple[int, int, str] | None:
        """Create target channel/chat/forum. Returns (internal_id, target_id, title) or None."""
        try:
            if target_type == "channel":
                return await self._create_channel(source_title)
            elif target_type == "forum":
                return await self._create_forum(source_title)
            else:
                return await self._create_megagroup(source_title)
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

    async def _create_megagroup(self, source_title: str) -> tuple[int, int, str]:
        """Create a megagroup (supergroup) for regular chats."""
        title = f"MIRROR: {source_title}"
        about = f"MIRROR of {source_title}"

        logger.info(f"Creating megagroup: {title}")
        result = await self.client(CreateChannelRequest(
            title=title,
            about=about,
            megagroup=True,
            broadcast=False,
        ))

        channel = result.chats[0]
        target_id = int(f"-100{channel.id}")
        logger.info(f"Created megagroup: id={channel.id}, target_id={target_id}")
        return channel.id, target_id, title

    async def _create_forum(self, source_title: str) -> tuple[int, int, str]:
        """Create a megagroup with forum topics enabled."""
        title = f"MIRROR: {source_title}"
        about = f"MIRROR of {source_title}"

        logger.info(f"Creating forum: {title}")
        # First create a megagroup
        result = await self.client(CreateChannelRequest(
            title=title,
            about=about,
            megagroup=True,
            broadcast=False,
        ))

        channel = result.chats[0]
        logger.info(f"Created megagroup for forum: id={channel.id}, access_hash={getattr(channel, 'access_hash', 'N/A')}")

        # Get full channel info to ensure we have correct access_hash
        try:
            from telethon.tl.functions.channels import GetChannelsRequest
            
            # Refresh channel entity to get proper access_hash
            full_result = await self.client(GetChannelsRequest(id=[InputChannel(channel.id, channel.access_hash)]))
            fresh_channel = full_result.chats[0]
            logger.info(f"Refreshed channel: id={fresh_channel.id}, access_hash={fresh_channel.access_hash}")
            
            # Enable forum topics - use InputChannel (not InputPeerChannel)
            toggle_result = await self.client(ToggleForumRequest(
                channel=InputChannel(fresh_channel.id, fresh_channel.access_hash),
                enabled=True
            ))
            logger.info(f"ToggleForum result: chats={len(getattr(toggle_result, 'chats', []))}, updates={getattr(toggle_result, 'updates', None)}")
        except Exception as e:
            logger.exception(f"Failed to enable forum for channel {channel.id}: {e}")
            # Return as regular megagroup if forum toggle fails

        target_id = int(f"-100{channel.id}")
        logger.info(f"Created forum: id={channel.id}, target_id={target_id}")
        return channel.id, target_id, title

    async def resolve_source(self, link_or_id: str) -> tuple[int, str, str] | None:
        """Resolve source link/ID to (source_id, source_type, title)."""
        try:
            if link_or_id.startswith("https://t.me/") or link_or_id.startswith("@"):
                entity = await self.client.get_entity(link_or_id)
            else:
                numeric_id = int(link_or_id)
                try:
                    entity = await self.client.get_entity(numeric_id)
                except Exception:
                    # Telegram Web hashes sometimes expose a plain negative chat id,
                    # while some copied ids use the Bot API-style -100 prefix.
                    if str(numeric_id).startswith("-100"):
                        fallback_id = -int(str(numeric_id)[4:])
                    elif numeric_id < 0:
                        fallback_id = int(f"-100{abs(numeric_id)}")
                    else:
                        raise
                    entity = await self.client.get_entity(fallback_id)

            source_id = int(entity.id)

            # Determine type - check for forum first
            if hasattr(entity, "broadcast") and entity.broadcast:
                source_type = "channel"
            elif hasattr(entity, "forum") and entity.forum:
                source_type = "forum"
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

    async def get_forum_topics(self, source_id: int) -> list[tuple[int, str]]:
        """Get all forum topics from source. Returns list of (topic_id, topic_title)."""
        try:
            from telethon import functions

            channel_id = int(str(source_id).replace("-100", ""))
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
                logger.info(f"Found topic: ID={topic.id}, title='{title}'")

            logger.info(f"Total topics found: {len(topics)}")
            return topics

        except Exception as e:
            logger.exception(f"Failed to get forum topics for {source_id}: {e}")
            return []

    async def create_target_topic(self, target_id: int, source_topic_id: int, title: str) -> int | None:
        """Create a topic in target forum. Returns target_thread_id or None."""
        from telethon import functions

        channel_id = int(str(target_id).replace("-100", ""))
        last_error = None

        # Telegram can return transient errors just after forum creation/toggle.
        for attempt in range(1, 6):
            try:
                entity = await self.client.get_input_entity(PeerChannel(channel_id))
                result = await self.client(functions.channels.CreateForumTopicRequest(
                    channel=entity,
                    title=title,
                ))
                target_thread_id = result.updates[0].id if result.updates else 1
                logger.info(f"Created target topic '{title}' with ID {target_thread_id} (attempt {attempt})")
                return target_thread_id
            except Exception as e:
                last_error = e
                logger.warning(
                    f"Create topic attempt {attempt}/5 failed for target {target_id}, title '{title}': {e}"
                )
                if attempt < 5:
                    await asyncio.sleep(1.5 * attempt)

        logger.exception(f"Failed to create topic '{title}' in target {target_id}: {last_error}")
        return None
