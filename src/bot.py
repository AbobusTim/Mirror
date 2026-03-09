"""MIRROR Bot - Full button navigation with session management."""

import asyncio
import html
import os
from typing import Any
from urllib.parse import quote_plus

from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from dotenv import load_dotenv
from loguru import logger
from telethon import TelegramClient
from telethon.errors import (
    ApiIdInvalidError,
    FloodWaitError,
    PhoneNumberBannedError,
    PhoneNumberFloodError,
    PhoneNumberInvalidError,
    RpcCallFailError,
)
from telethon.sessions import StringSession

from src.channel_manager import ChannelManager
from src.database import (
    TopicRule,
    add_bridge,
    add_topic_rule,
    create_session,
    create_topic_mapping,
    delete_bridge,
    delete_session,
    delete_topic_rule,
    get_active_bridges,
    get_all_bridges,
    get_first_session,
    get_session,
    get_topic_proposal,
    get_topic_rule,
    get_topic_rules_for_bridge,
    get_user_bridges,
    get_user_sessions,
    has_any_session,
    init_db,
    migrate_old_user_data,
    toggle_topic_rule,
    toggle_bridge,
    update_topic_proposal_status,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is required in .env")

EMOJI = {
    "welcome": "🪞",
    "success": "✨",
    "error": "❌",
    "loading": "⏳",
    "deleted": "🗑",
    "mirror": "🪞",
    "channel": "📢",
    "chat": "💬",
    "forum": "🧵",
    "active": "🟢",
    "inactive": "🔴",
    "key": "🔑",
    "all": "📄",
    "account": "👤",
    "back": "⬅️",
    "home": "🏠",
    "add": "➕",
    "settings": "⚙️",
}

# States
class MenuStates(StatesGroup):
    main = State()
    session_menu = State()
    session_detail = State()
    bridge_menu = State()
    bridge_detail = State()
    creating_bridge = State()
    selecting_session = State()


class QrStates(StatesGroup):
    waiting_for_api_id = State()
    waiting_for_api_hash = State()


class BridgeCreateStates(StatesGroup):
    selecting_account = State()
    selecting_type = State()
    entering_source = State()
    selecting_topics = State()
    selecting_filter = State()
    entering_keywords = State()


class TopicEditorStates(StatesGroup):
    selecting_source_topics = State()
    entering_external_title = State()
    entering_external_source = State()
    selecting_external_forum_topic = State()


bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher(storage=MemoryStorage())
active_qr_sessions: dict[int, dict[str, Any]] = {}
GENERAL_TOPIC_ID = 1


def safe_html(value: Any) -> str:
    return html.escape(str(value))


# ==================== KEYBOARDS ====================

def main_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📲 Подключить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton(text="👤 Мои аккаунты", callback_data="my_accounts")],
        [InlineKeyboardButton(text="🪞 Мои зеркала", callback_data="my_bridges_menu")],
        [InlineKeyboardButton(text="✨ Создать зеркало", callback_data="create_bridge")],
        [InlineKeyboardButton(text="ℹ️ О боте", callback_data="help")],
    ])


def back_keyboard(callback_data="main_menu"):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data=callback_data)],
    ])


def session_list_keyboard(sessions, with_create_bridge=False):
    buttons = []
    for s in sessions:
        text = f"{s.label} (+{s.phone})"
        callback = f"session_detail:{s.session_id}"
        if with_create_bridge:
            callback = f"select_session_for_bridge:{s.session_id}"
        buttons.append([InlineKeyboardButton(text=text, callback_data=callback)])
    buttons.append([InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def session_detail_keyboard(session_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{EMOJI['mirror']} Зеркала аккаунта", callback_data=f"session_bridges:{session_id}")],
        [InlineKeyboardButton(text=f"{EMOJI['add']} Создать зеркало", callback_data=f"create_bridge_with_session:{session_id}")],
        [InlineKeyboardButton(text="📋 Session string", callback_data=f"get_session_string:{session_id}")],
        [InlineKeyboardButton(text=f"{EMOJI['deleted']} Удалить аккаунт", callback_data=f"delete_session:{session_id}")],
        [InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data="my_accounts")],
    ])


def bridge_list_keyboard(bridges, sessions_map, back_to="my_bridges_menu"):
    buttons = []
    for b in bridges:
        status = EMOJI['active'] if b.is_active else EMOJI['inactive']
        emoji = {
            "channel": EMOJI["channel"],
            "chat": EMOJI["chat"],
            "forum": EMOJI["forum"],
        }.get(b.source_type, EMOJI["mirror"])
        session_info = sessions_map.get(b.session_id, "")
        text = f"{status} {emoji} {b.source_title[:20]}"
        if session_info:
            text += f" ({session_info.label[:15]})"
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"bridge_detail:{b.id}")])
    buttons.append([InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data=back_to)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def bridge_detail_keyboard(bridge_id, is_active, is_forum=False):
    toggle_text = "🔴 Отключить зеркало" if is_active else "🟢 Включить зеркало"
    rows = [[InlineKeyboardButton(text=toggle_text, callback_data=f"toggle_bridge:{bridge_id}")]]
    if is_forum:
        rows.append([InlineKeyboardButton(text=f"{EMOJI['forum']} Управление ветками", callback_data=f"topic_editor:{bridge_id}")])
    rows.extend([
        [InlineKeyboardButton(text=f"{EMOJI['deleted']} Удалить зеркало", callback_data=f"delete_bridge:{bridge_id}")],
        [InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data="my_bridges_menu")],
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def filter_type_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{EMOJI['all']} Без фильтра", callback_data="filter_all")],
        [InlineKeyboardButton(text=f"{EMOJI['key']} Фильтр по словам", callback_data="filter_keywords")],
        [InlineKeyboardButton(text=f"{EMOJI['back']} Отменить", callback_data="cancel_bridge")],
    ])


def bridge_type_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{EMOJI['channel']} Канал", callback_data="bridge_type:channel")],
        [InlineKeyboardButton(text=f"{EMOJI['chat']} Группа или чат", callback_data="bridge_type:chat")],
        [InlineKeyboardButton(text=f"{EMOJI['forum']} Форум", callback_data="bridge_type:forum")],
        [InlineKeyboardButton(text=f"{EMOJI['back']} Отменить", callback_data="main_menu")],
    ])


def topic_selection_keyboard(
    topics: list[tuple[int, str]],
    selected_ids: set[int],
    toggle_prefix: str,
    confirm_callback: str,
    cancel_callback: str,
    locked_ids: set[int] | None = None,
) -> InlineKeyboardMarkup:
    locked_ids = locked_ids or set()
    rows = []
    for topic_id, title in topics:
        mark = "✅" if topic_id in selected_ids else "⬜"
        suffix = " • обязательно" if topic_id in locked_ids else ""
        rows.append(
            [InlineKeyboardButton(text=f"{mark} {title}{suffix}", callback_data=f"{toggle_prefix}:{topic_id}")]
        )
    rows.extend(
        [
            [InlineKeyboardButton(text="Сохранить выбор", callback_data=confirm_callback)],
            [InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data=cancel_callback)],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def topic_editor_keyboard(bridge_id: int, rules: list[TopicRule]) -> InlineKeyboardMarkup:
    rows = []
    for rule in rules:
        status = EMOJI["active"] if rule.is_active else EMOJI["inactive"]
        source_kind = "внешний" if rule.is_external else "форум"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{status} {rule.target_title[:24]} ({source_kind})",
                    callback_data=f"topic_rule_detail:{bridge_id}:{rule.id}",
                )
            ]
        )
    rows.extend(
        [
            [InlineKeyboardButton(text=f"{EMOJI['add']} Добавить ветки из форума", callback_data=f"topic_add_source:{bridge_id}")],
            [InlineKeyboardButton(text=f"{EMOJI['add']} Подключить внешний источник", callback_data=f"topic_add_external:{bridge_id}")],
            [InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data=f"bridge_detail:{bridge_id}")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def topic_rule_detail_keyboard(bridge_id: int, rule_id: int, is_active: bool) -> InlineKeyboardMarkup:
    toggle_text = "🔴 Отключить ветку" if is_active else "🟢 Включить ветку"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=toggle_text, callback_data=f"toggle_topic_rule:{bridge_id}:{rule_id}")],
            [InlineKeyboardButton(text=f"{EMOJI['deleted']} Удалить ветку", callback_data=f"delete_topic_rule:{bridge_id}:{rule_id}")],
            [InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data=f"topic_editor:{bridge_id}")],
        ]
    )


def normalize_source_input(source_input: str) -> str:
    source_input = source_input.strip()
    if not source_input:
        return source_input

    if source_input.startswith("@"):
        return source_input

    if source_input.startswith("-100"):
        return source_input

    from urllib.parse import parse_qs, urlparse

    candidate = source_input
    if candidate.startswith(("t.me/", "telegram.me/", "web.telegram.org/")):
        candidate = f"https://{candidate}"
    elif candidate.startswith("tg://"):
        parsed = urlparse(candidate)
        params = parse_qs(parsed.query)

        domain = params.get("domain")
        if domain:
            return f"@{domain[0]}"

        channel = params.get("channel") or params.get("channel_id") or params.get("chat_id")
        if channel:
            try:
                return f"-100{abs(int(channel[0]))}"
            except ValueError:
                return source_input

        username = params.get("username")
        if username:
            return f"@{username[0]}"

        return source_input

    parsed = urlparse(candidate)
    netloc = parsed.netloc.lower()

    if "web.telegram.org" in netloc and parsed.fragment:
        fragment = parsed.fragment.lstrip("/")
        fragment_path = fragment.split("?", 1)[0]
        if fragment_path.startswith("-"):
            try:
                return str(int(fragment_path))
            except ValueError:
                return source_input

    if any(host in netloc for host in ("t.me", "telegram.me")):
        path = [part for part in parsed.path.strip("/").split("/") if part]
        if len(path) >= 2 and path[0] == "c":
            try:
                return f"-100{int(path[1])}"
            except (ValueError, IndexError):
                return source_input
        if path and path[0] not in {"s", "share", "joinchat", "+", "login"}:
            return f"@{path[0]}"

    if "web.telegram.org" in netloc and parsed.path:
        path = [part for part in parsed.path.strip("/").split("/") if part]
        if path and path[-1].startswith("-"):
            try:
                return f"-100{abs(int(path[-1]))}"
            except ValueError:
                return source_input

    return source_input


def ensure_general_topic_selected(topics: list[tuple[int, str]], selected_ids: set[int]) -> set[int]:
    if any(topic_id == GENERAL_TOPIC_ID for topic_id, _ in topics):
        selected_ids.add(GENERAL_TOPIC_ID)
    return selected_ids


async def ensure_bridge_topic_rule(
    manager: ChannelManager,
    bridge,
    source_chat_id: int,
    source_thread_id: int,
    source_title: str,
    *,
    target_title: str | None = None,
    is_external: bool = False,
) -> int | None:
    final_target_title = target_title or source_title

    if (
        not is_external
        and source_chat_id == bridge.source_id
        and source_thread_id == GENERAL_TOPIC_ID
    ):
        add_topic_rule(
            bridge_id=bridge.id,
            source_chat_id=source_chat_id,
            source_type="topic",
            source_thread_id=GENERAL_TOPIC_ID,
            source_title=source_title,
            target_chat_id=bridge.target_id,
            target_thread_id=GENERAL_TOPIC_ID,
            target_title=final_target_title,
        )
        return GENERAL_TOPIC_ID

    target_topic_id = await manager.create_target_topic(
        bridge.target_id,
        source_thread_id,
        final_target_title,
    )
    if not target_topic_id:
        return None

    if not is_external and source_chat_id == bridge.source_id and source_thread_id != GENERAL_TOPIC_ID:
        create_topic_mapping(bridge.id, source_thread_id, target_topic_id, final_target_title)

    add_topic_rule(
        bridge_id=bridge.id,
        source_chat_id=source_chat_id,
        source_type="topic" if source_thread_id else bridge.source_type,
        source_thread_id=source_thread_id,
        source_title=source_title,
        target_chat_id=bridge.target_id,
        target_thread_id=target_topic_id,
        target_title=final_target_title,
        is_external=is_external,
    )
    return target_topic_id


def build_client(session) -> TelegramClient:
    return TelegramClient(
        StringSession(session.session_string),
        session.api_id,
        session.api_hash,
    )


# ==================== QR AUTH ====================

async def cleanup_qr_session(user_id: int) -> None:
    entry = active_qr_sessions.pop(user_id, None)
    if not entry:
        return
    task = entry.get("task")
    if task and not task.done():
        task.cancel()
    client = entry.get("client")
    if client:
        try:
            await client.disconnect()
        except Exception:
            pass


async def wait_qr_approval(
    user_id: int, api_id: int, api_hash: str, client: TelegramClient, qr_login
) -> None:
    try:
        await qr_login.wait(timeout=180)
        me = await client.get_me()
        session_string = client.session.save()
        phone = me.phone or "unknown"
        
        session_id = create_session(user_id, api_id, api_hash, session_string, phone, f"+{phone}")
        
        full_name = f"{me.first_name or ''} {me.last_name or ''}".strip()
        await bot.send_message(
            user_id,
            f"{EMOJI['success']} <b>Аккаунт добавлен!</b>\n\n"
            f"<b>ID:</b> <code>{session_id}</code>\n"
            f"<b>Имя:</b> {full_name}\n"
            f"<b>Телефон:</b> +{phone}\n\n"
            f"Теперь можно создавать зеркала",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(),
        )
    except asyncio.TimeoutError:
        await bot.send_message(
            user_id,
            f"{EMOJI['error']} Время ожидания истекло (3 мин).\n\nПопробуйте: /start",
            reply_markup=main_menu_keyboard(),
        )
    except Exception as e:
        await bot.send_message(
            user_id,
            f"{EMOJI['error']} Ошибка: {e}\n\n/start",
            reply_markup=main_menu_keyboard(),
        )
    finally:
        await cleanup_qr_session(user_id)


# ==================== HANDLERS ====================

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext) -> None:
    await state.clear()
    user_id = message.from_user.id
    migrate_old_user_data(user_id)
    
    sessions = get_user_sessions(user_id)
    
    if not sessions:
        await message.answer(
            f"{EMOJI['welcome']} <b>MIRROR Bot</b>\n\n"
            "Бот помогает быстро собирать зеркала для каналов, чатов и форумов.\n\n"
            "Что умеет:\n"
            "• копировать каналы, группы и форумы\n"
            "• зеркалить отдельные ветки форума\n"
            "• подключать внешние источники в нужную ветку\n"
            "• фильтровать сообщения по ключевым словам\n\n"
            "Для начала подключите Telegram-аккаунт.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📲 Подключить аккаунт", callback_data="add_account")],
            ]),
        )
        return
    
    await message.answer(
        f"{EMOJI['welcome']} <b>MIRROR Bot</b>\n\n"
        "Управляйте зеркалами каналов, чатов и форумов в одном месте.\n\n"
        f"👤 Подключено аккаунтов: <b>{len(sessions)}</b>\n"
        "Выберите действие ниже.",
        reply_markup=main_menu_keyboard(),
    )


@dp.callback_query(F.data == "main_menu")
async def cb_main_menu(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()  # Immediate response
    await state.clear()
    user_id = callback.from_user.id
    sessions = get_user_sessions(user_id)
    
    text = f"{EMOJI['welcome']} <b>MIRROR Bot</b>\n\n"
    if sessions:
        text += f"👤 Подключено аккаунтов: <b>{len(sessions)}</b>\n"
        text += "🪞 Здесь можно создавать и настраивать зеркала.\n\n"
    else:
        text += "Подключите аккаунт, чтобы начать работу.\n\n"
    text += "Выберите действие ниже."
    
    await callback.message.edit_text(text, reply_markup=main_menu_keyboard())


# -------------------- ACCOUNTS --------------------

@dp.callback_query(F.data == "add_account")
async def cb_add_account(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await cleanup_qr_session(callback.from_user.id)
    await state.set_state(QrStates.waiting_for_api_id)
    await callback.message.edit_text(
        f"{EMOJI['account']} <b>Добавление аккаунта</b>\n\n"
        "Шаг 1 из 2. Введите API_ID\n"
        "<i>Например: 12345678</i>",
        reply_markup=back_keyboard(),
    )


@dp.message(QrStates.waiting_for_api_id)
async def process_api_id(message: types.Message, state: FSMContext) -> None:
    try:
        api_id = int(message.text.strip())
    except ValueError:
        await message.answer(f"{EMOJI['error']} API_ID должен быть числом")
        return
    await state.update_data(api_id=api_id)
    await state.set_state(QrStates.waiting_for_api_hash)
    await message.answer(
        "Шаг 2 из 2. Введите API_HASH\n"
        "<i>Строка из букв и цифр</i>",
        reply_markup=back_keyboard(),
    )


@dp.message(QrStates.waiting_for_api_hash)
async def process_api_hash(message: types.Message, state: FSMContext) -> None:
    api_hash = message.text.strip()
    if len(api_hash) < 10:
        await message.answer(f"{EMOJI['error']} Слишком короткий")
        return

    data = await state.get_data()
    api_id = data.get("api_id")
    user_id = message.from_user.id
    
    loading = await message.answer(f"{EMOJI['loading']} Создаю QR...")

    client = TelegramClient(StringSession(), api_id, api_hash)
    try:
        await client.connect()
        qr_login = await client.qr_login()
        task = asyncio.create_task(
            wait_qr_approval(user_id, api_id, api_hash, client, qr_login)
        )
        active_qr_sessions[user_id] = {"client": client, "task": task}
        
        qr_image_url = (
            "https://api.qrserver.com/v1/create-qr-code/?size=520x520&data="
            + quote_plus(qr_login.url)
        )
        await loading.delete()
        await message.answer_photo(
            photo=qr_image_url,
            caption=(
                "📱 <b>Сканируйте QR в Telegram</b>\n\n"
                "<b>Настройки → Устройства → Подключить</b>\n\n"
                f"Или ссылка: <code>{qr_login.url}</code>\n\n"
                "Ожидание 3 минуты..."
            ),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="main_menu")]
            ]),
        )
        await state.clear()
    except Exception as e:
        await loading.edit_text(f"{EMOJI['error']} Ошибка: {e}")
        await client.disconnect()


@dp.callback_query(F.data == "my_accounts")
async def cb_my_accounts(callback: types.CallbackQuery) -> None:
    await callback.answer()
    user_id = callback.from_user.id
    sessions = get_user_sessions(user_id)
    
    if not sessions:
        await callback.message.edit_text(
            f"{EMOJI['error']} Нет аккаунтов\n\n"
            f"Добавьте первый:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"{EMOJI['add']} Добавить", callback_data="add_account")],
                [InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data="main_menu")],
            ]),
        )
        return
    
    text = f"{EMOJI['account']} <b>Ваши аккаунты:</b>\n\n"
    for s in sessions:
        text += f"• <code>{s.session_id}</code>: +{s.phone}\n"
    
    await callback.message.edit_text(
        text,
        reply_markup=session_list_keyboard(sessions),
    )


@dp.callback_query(F.data.startswith("session_detail:"))
async def cb_session_detail(callback: types.CallbackQuery) -> None:
    await callback.answer()
    session_id = int(callback.data.split(":")[1])
    session = get_session(session_id)
    
    if not session:
        await callback.answer("Аккаунт не найден!")
        return
    
    bridges = get_user_bridges(callback.from_user.id)
    session_bridges = [b for b in bridges if b.session_id == session_id]
    
    text = (
        f"{EMOJI['account']} <b>Аккаунт {session_id}</b>\n\n"
        f"Название: {session.label}\n"
        f"Телефон: +{session.phone}\n"
        f"Зеркал: {len(session_bridges)}\n\n"
        f"Выберите действие:"
    )
    
    await callback.message.edit_text(text, reply_markup=session_detail_keyboard(session_id))


@dp.callback_query(F.data.startswith("get_session_string:"))
async def cb_get_session_string(callback: types.CallbackQuery) -> None:
    await callback.answer()
    session_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    
    session = get_session(session_id)
    if not session or session.user_id != user_id:
        await callback.answer("Аккаунт не найден!", show_alert=True)
        return
    
    # Send session string as separate message
    await callback.message.answer(
        f"📋 <b>Session String для аккаунта {session.label}</b>\n\n"
        f"<code>{session.session_string}</code>\n\n"
        f"Скопируйте эту строку для вашего агента.",
        parse_mode=ParseMode.HTML,
    )


@dp.callback_query(F.data.startswith("delete_session:"))
async def cb_delete_session(callback: types.CallbackQuery) -> None:
    await callback.answer()
    session_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    
    # Check for active bridges
    bridges = get_user_bridges(user_id)
    active = [b for b in bridges if b.session_id == session_id and b.is_active]
    
    if active:
        await callback.answer(f"Нельзя удалить! {len(active)} активных зеркал", show_alert=True)
        return
    
    if delete_session(session_id, user_id):
        await callback.answer("Аккаунт удален")
        await cb_my_accounts(callback)
    else:
        await callback.answer("Не удалось удалить", show_alert=True)


# -------------------- BRIDGES --------------------

@dp.callback_query(F.data == "my_bridges_menu")
async def cb_my_bridges_menu(callback: types.CallbackQuery) -> None:
    """Show bridge type selection menu."""
    await callback.answer()
    user_id = callback.from_user.id
    bridges = get_user_bridges(user_id)
    
    if not bridges:
        await callback.message.edit_text(
            f"{EMOJI['mirror']} Нет зеркал\n\n"
            f"Создайте первое зеркало:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"{EMOJI['add']} Создать зеркало", callback_data="create_bridge")],
                [InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data="main_menu")],
            ]),
        )
        return
    
    channels_count = sum(1 for b in bridges if b.source_type == "channel")
    chats_count = sum(1 for b in bridges if b.source_type == "chat")
    forums_count = sum(1 for b in bridges if b.source_type == "forum")
    
    await callback.message.edit_text(
        f"{EMOJI['mirror']} <b>Мои зеркала</b>\n\n"
        f"📺 Каналы: {channels_count}\n"
        f"💬 Чаты: {chats_count}\n"
        f"🧵 Форумы: {forums_count}\n"
        f"📋 Всего: {len(bridges)}\n\n"
        f"Выберите раздел:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"📺 Каналы ({channels_count})", callback_data="my_bridges:channel")],
            [InlineKeyboardButton(text=f"💬 Чаты ({chats_count})", callback_data="my_bridges:chat")],
            [InlineKeyboardButton(text=f"🧵 Форумы ({forums_count})", callback_data="my_bridges:forum")],
            [InlineKeyboardButton(text=f"📋 Все зеркала", callback_data="my_bridges:all")],
            [InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data="main_menu")],
        ]),
    )


@dp.callback_query(F.data.startswith("my_bridges:"))
async def cb_my_bridges(callback: types.CallbackQuery) -> None:
    await callback.answer()
    user_id = callback.from_user.id
    bridges = get_user_bridges(user_id)
    sessions = {s.session_id: s for s in get_user_sessions(user_id)}
    
    filter_type = callback.data.split(":")[1]  # channel, chat, or all
    
    if filter_type == "channel":
        bridges = [b for b in bridges if b.source_type == "channel"]
        title = "📺 <b>Каналы:</b>"
    elif filter_type == "chat":
        bridges = [b for b in bridges if b.source_type == "chat"]
        title = "💬 <b>Чаты:</b>"
    elif filter_type == "forum":
        bridges = [b for b in bridges if b.source_type == "forum"]
        title = "🧵 <b>Форумы:</b>"
    else:
        title = "📋 <b>Все зеркала:</b>"
    
    if not bridges:
        await callback.message.edit_text(
            f"{EMOJI['error']} Нет зеркал в этом разделе\n\n"
            f"Создайте первое:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"{EMOJI['add']} Создать", callback_data="create_bridge")],
                [InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data="my_bridges_menu")],
            ]),
        )
        return
    
    await callback.message.edit_text(
        f"{EMOJI['mirror']} {title}\n\nНажмите для управления:",
        reply_markup=bridge_list_keyboard(bridges, sessions, back_to="my_bridges_menu"),
    )


@dp.callback_query(F.data.startswith("bridge_detail:"))
async def cb_bridge_detail(callback: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    bridge_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    bridges = get_user_bridges(user_id)
    bridge = next((b for b in bridges if b.id == bridge_id), None)
    
    if not bridge:
        await callback.answer("Зеркало не найдено", show_alert=True)
        return
    
    sessions = {s.session_id: s for s in get_user_sessions(user_id)}
    session = sessions.get(bridge.session_id)
    
    status = "🟢 Активно" if bridge.is_active else "🔴 Выключено"
    keywords_info = f"{EMOJI['key']} {bridge.keywords}" if bridge.keywords else f"{EMOJI['all']} без ограничений"
    rule_count = len(get_topic_rules_for_bridge(bridge.id, active_only=False)) if bridge.source_type == "forum" else 0
    
    text = (
        f"{EMOJI['mirror']} <b>Зеркало {bridge_id}</b>\n\n"
        f"Статус: {status}\n"
        f"Тип: {bridge.source_type}\n"
        f"Источник: {bridge.source_title}\n"
        f"Цель: {bridge.target_title}\n"
        f"Фильтр: {keywords_info}\n"
    )
    if bridge.source_type == "forum":
        text += f"Веток подключено: {rule_count}\n"
    if session:
        text += f"\nАккаунт: {safe_html(session.label)}"
    
    await callback.message.edit_text(
        text,
        reply_markup=bridge_detail_keyboard(bridge_id, bridge.is_active, is_forum=bridge.source_type == "forum"),
    )


@dp.callback_query(F.data.startswith("toggle_bridge:"))
async def cb_toggle_bridge(callback: types.CallbackQuery, state: FSMContext) -> None:
    bridge_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    
    bridges = get_user_bridges(user_id)
    bridge = next((b for b in bridges if b.id == bridge_id), None)
    
    if not bridge:
        await callback.answer("Зеркало не найдено", show_alert=True)
        return
    
    new_status = not bridge.is_active
    toggle_bridge(bridge_id, new_status)
    await callback.answer("Настройки зеркала обновлены")
    await cb_bridge_detail(callback, state)


@dp.callback_query(F.data.startswith("delete_bridge:"))
async def cb_delete_bridge(callback: types.CallbackQuery) -> None:
    bridge_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    
    if delete_bridge(bridge_id):
        await callback.answer("Зеркало удалено")
        await cb_my_bridges(callback)
    else:
        await callback.answer("Не удалось удалить", show_alert=True)


@dp.callback_query(F.data.startswith("topic_editor:"))
async def cb_topic_editor(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.clear()
    bridge_id = int(callback.data.split(":")[1])
    bridge = next((b for b in get_user_bridges(callback.from_user.id) if b.id == bridge_id), None)
    if not bridge or bridge.source_type != "forum":
        await callback.answer("Форум не найден", show_alert=True)
        return

    rules = get_topic_rules_for_bridge(bridge_id)
    await callback.message.edit_text(
        f"{EMOJI['forum']} <b>Редактор веток</b>\n\n"
        f"Форум: <b>{safe_html(bridge.target_title)}</b>\n"
        f"Активных правил: {len(rules)}",
        reply_markup=topic_editor_keyboard(bridge_id, rules),
    )


@dp.callback_query(F.data.startswith("topic_rule_detail:"))
async def cb_topic_rule_detail(callback: types.CallbackQuery) -> None:
    await callback.answer()
    _, bridge_id_str, rule_id_str = callback.data.split(":")
    bridge_id = int(bridge_id_str)
    rule = get_topic_rule(int(rule_id_str))
    if not rule:
        await callback.answer("Ветка не найдена", show_alert=True)
        return

    status = "🟢 Активна" if rule.is_active else "🔴 Выключена"
    source_kind = "Внешний источник" if rule.is_external else "Источник форума"
    await callback.message.edit_text(
        f"{EMOJI['forum']} <b>Ветка</b>\n\n"
        f"Статус: {status}\n"
        f"Тип: {source_kind}\n"
        f"Источник: {rule.source_title}\n"
        f"Цель: {rule.target_title}",
        reply_markup=topic_rule_detail_keyboard(bridge_id, rule.id, rule.is_active),
    )


@dp.callback_query(F.data.startswith("toggle_topic_rule:"))
async def cb_toggle_topic_rule(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    _, bridge_id_str, rule_id_str = callback.data.split(":")
    rule = get_topic_rule(int(rule_id_str))
    if not rule:
        await callback.answer("Ветка не найдена", show_alert=True)
        return
    toggle_topic_rule(rule.id, not rule.is_active)
    updated_rule = get_topic_rule(int(rule_id_str))
    if not updated_rule:
        await callback.answer("Ветка не найдена", show_alert=True)
        return
    status = "🟢 Активна" if updated_rule.is_active else "🔴 Выключена"
    source_kind = "Внешний источник" if updated_rule.is_external else "Источник форума"
    await callback.message.edit_text(
        f"{EMOJI['forum']} <b>Ветка</b>\n\n"
        f"Статус: {status}\n"
        f"Тип: {source_kind}\n"
        f"Источник: {updated_rule.source_title}\n"
        f"Цель: {updated_rule.target_title}",
        reply_markup=topic_rule_detail_keyboard(int(bridge_id_str), updated_rule.id, updated_rule.is_active),
    )


@dp.callback_query(F.data.startswith("delete_topic_rule:"))
async def cb_delete_topic_rule(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    _, bridge_id_str, rule_id_str = callback.data.split(":")
    delete_topic_rule(int(rule_id_str))
    bridge_id = int(bridge_id_str)
    bridge = next((b for b in get_user_bridges(callback.from_user.id) if b.id == bridge_id), None)
    rules = get_topic_rules_for_bridge(bridge_id)
    await state.clear()
    if not bridge or bridge.source_type != "forum":
        await callback.answer("Форум не найден", show_alert=True)
        return
    await callback.message.edit_text(
        f"{EMOJI['forum']} <b>Редактор веток</b>\n\n"
        f"Форум: <b>{safe_html(bridge.target_title)}</b>\n"
        f"Активных правил: {len(rules)}",
        reply_markup=topic_editor_keyboard(bridge_id, rules),
    )


@dp.callback_query(F.data.startswith("topic_proposal_accept:"))
async def cb_topic_proposal_accept(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    proposal_id = int(callback.data.split(":")[1])
    proposal = get_topic_proposal(proposal_id)
    if not proposal or proposal.user_id != callback.from_user.id:
        await callback.answer("Предложение не найдено", show_alert=True)
        return
    if proposal.status != "pending":
        await callback.answer("Это предложение уже обработано", show_alert=True)
        return

    bridge = next((b for b in get_user_bridges(callback.from_user.id) if b.id == proposal.bridge_id), None)
    if not bridge:
        await callback.answer("Зеркало не найдено", show_alert=True)
        return

    session = get_session(bridge.session_id)
    if not session:
        await callback.answer("Аккаунт не найден", show_alert=True)
        return

    await state.clear()
    client = build_client(session)
    try:
        await client.connect()
        manager = ChannelManager(client)
        target_topic_id = await ensure_bridge_topic_rule(
            manager,
            bridge,
            proposal.source_chat_id,
            proposal.source_thread_id,
            proposal.source_title,
        )
    finally:
        await client.disconnect()

    if not target_topic_id:
        await callback.answer("Не удалось подключить ветку", show_alert=True)
        return

    update_topic_proposal_status(proposal.id, "accepted")
    await callback.message.edit_text(
        f"{EMOJI['success']} <b>Ветка подключена</b>\n\n"
        f"Форум: <b>{safe_html(proposal.bridge_target_title)}</b>\n"
        f"Ветка: <b>{safe_html(proposal.source_title)}</b>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=f"{EMOJI['forum']} Открыть редактор веток",
                        callback_data=f"topic_editor:{proposal.bridge_id}",
                    )
                ]
            ]
        ),
    )


@dp.callback_query(F.data.startswith("topic_proposal_dismiss:"))
async def cb_topic_proposal_dismiss(callback: types.CallbackQuery) -> None:
    await callback.answer()
    proposal_id = int(callback.data.split(":")[1])
    proposal = get_topic_proposal(proposal_id)
    if not proposal or proposal.user_id != callback.from_user.id:
        await callback.answer("Предложение не найдено", show_alert=True)
        return
    if proposal.status != "pending":
        await callback.answer("Это предложение уже обработано", show_alert=True)
        return

    update_topic_proposal_status(proposal.id, "dismissed")
    await callback.message.edit_text(
        f"{EMOJI['forum']} <b>Предложение скрыто</b>\n\n"
        f"Ветка <b>{safe_html(proposal.source_title)}</b> не будет подключена автоматически.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=f"{EMOJI['forum']} Открыть редактор веток",
                        callback_data=f"topic_editor:{proposal.bridge_id}",
                    )
                ]
            ]
        ),
    )


@dp.callback_query(F.data.startswith("topic_add_source:"))
async def cb_topic_add_source(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    bridge_id = int(callback.data.split(":")[1])
    bridge = next((b for b in get_user_bridges(callback.from_user.id) if b.id == bridge_id), None)
    if not bridge:
        await callback.answer("Зеркало не найдено", show_alert=True)
        return

    session = get_session(bridge.session_id)
    loading = await callback.message.edit_text(f"{EMOJI['loading']} Читаю доступные ветки...")
    client = build_client(session)
    try:
        await client.connect()
        manager = ChannelManager(client)
        topics = await manager.get_forum_topics(bridge.source_id)
    finally:
        await client.disconnect()

    existing = {
        rule.source_thread_id
        for rule in get_topic_rules_for_bridge(bridge_id)
        if not rule.is_external and rule.source_chat_id == bridge.source_id
    }
    available = [topic for topic in topics if topic[0] not in existing]
    if not available:
        await loading.edit_text(
            f"{EMOJI['forum']} Новых веток для подключения пока нет.",
            reply_markup=topic_editor_keyboard(bridge_id, get_topic_rules_for_bridge(bridge_id)),
        )
        return

    await state.set_state(TopicEditorStates.selecting_source_topics)
    initial_selected = ensure_general_topic_selected(available, set())
    await state.update_data(
        editor_bridge_id=bridge_id,
        available_topics=available,
        selected_topic_ids=list(initial_selected),
    )
    await loading.edit_text(
        f"{EMOJI['forum']} <b>Добавление веток</b>\n\n"
        f"Отметьте ветки, которые нужно подключить:",
        reply_markup=topic_selection_keyboard(
            available,
            initial_selected,
            "editor_topic_toggle",
            "editor_topics_confirm",
            f"topic_editor:{bridge_id}",
            {GENERAL_TOPIC_ID},
        ),
    )


@dp.callback_query(F.data.startswith("editor_topic_toggle:"))
async def cb_editor_topic_toggle(callback: types.CallbackQuery, state: FSMContext) -> None:
    topic_id = int(callback.data.split(":")[1])
    data = await state.get_data()
    topics = data.get("available_topics", [])
    selected_ids = set(data.get("selected_topic_ids", []))
    if topic_id == GENERAL_TOPIC_ID:
        await callback.answer("Ветку General нельзя отключить")
        selected_ids = ensure_general_topic_selected(topics, selected_ids)
    elif topic_id in selected_ids:
        await callback.answer()
        selected_ids.remove(topic_id)
    else:
        await callback.answer()
        selected_ids.add(topic_id)
    selected_ids = ensure_general_topic_selected(topics, selected_ids)
    await state.update_data(selected_topic_ids=list(selected_ids))
    bridge_id = data.get("editor_bridge_id")
    await callback.message.edit_reply_markup(
        reply_markup=topic_selection_keyboard(
            topics,
            selected_ids,
            "editor_topic_toggle",
            "editor_topics_confirm",
            f"topic_editor:{bridge_id}",
            {GENERAL_TOPIC_ID},
        )
    )


@dp.callback_query(F.data == "editor_topics_confirm")
async def cb_editor_topics_confirm(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    data = await state.get_data()
    bridge_id = data.get("editor_bridge_id")
    available_topics = data.get("available_topics", [])
    selected_ids = ensure_general_topic_selected(available_topics, set(data.get("selected_topic_ids", [])))
    topics = [topic for topic in data.get("available_topics", []) if topic[0] in selected_ids]
    if not topics:
        await callback.answer("Выберите хотя бы одну ветку", show_alert=True)
        return

    user_bridge = None
    for b in get_user_bridges(callback.from_user.id):
        if b.id == bridge_id:
            user_bridge = b
            break
    if not user_bridge:
        await callback.answer("Зеркало не найдено", show_alert=True)
        return

    session = get_session(user_bridge.session_id)
    client = build_client(session)
    try:
        await client.connect()
        manager = ChannelManager(client)
        for topic_id, topic_title in topics:
            await ensure_bridge_topic_rule(
                manager,
                user_bridge,
                user_bridge.source_id,
                topic_id,
                topic_title,
            )
    finally:
        await client.disconnect()

    await state.clear()
    rules = get_topic_rules_for_bridge(bridge_id)
    await callback.message.edit_text(
        f"{EMOJI['forum']} <b>Редактор веток</b>\n\n"
        f"Форум: <b>{safe_html(user_bridge.target_title)}</b>\n"
        f"Активных правил: {len(rules)}",
        reply_markup=topic_editor_keyboard(bridge_id, rules),
    )


@dp.callback_query(F.data.startswith("topic_add_external:"))
async def cb_topic_add_external(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    bridge_id = int(callback.data.split(":")[1])
    await state.set_state(TopicEditorStates.entering_external_title)
    await state.update_data(editor_bridge_id=bridge_id)
    await callback.message.edit_text(
        f"{EMOJI['forum']} <b>Новая внешняя ветка</b>\n\n"
        f"Введите название для новой ветки:",
        reply_markup=back_keyboard(f"topic_editor:{bridge_id}"),
    )


@dp.message(TopicEditorStates.entering_external_title)
async def process_external_title(message: types.Message, state: FSMContext) -> None:
    title = message.text.strip()
    data = await state.get_data()
    await state.update_data(external_target_title=title)
    await state.set_state(TopicEditorStates.entering_external_source)
    await message.answer(
        f"{EMOJI['forum']} <b>Новая внешняя ветка</b>\n\n"
        f"Название: <b>{safe_html(title)}</b>\n\n"
        f"Теперь отправьте ссылку, @username или ID источника.",
        reply_markup=back_keyboard(f"topic_editor:{data.get('editor_bridge_id')}"),
    )


@dp.message(TopicEditorStates.entering_external_source)
async def process_external_source(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    bridge_id = data.get("editor_bridge_id")
    bridge = next((b for b in get_user_bridges(message.from_user.id) if b.id == bridge_id), None)
    if not bridge:
        await message.answer(f"{EMOJI['error']} Форум не найден", reply_markup=main_menu_keyboard())
        await state.clear()
        return

    session = get_session(bridge.session_id)
    client = build_client(session)
    source_input = normalize_source_input(message.text)
    loading = await message.answer(f"{EMOJI['loading']} Проверяю источник и доступ...")
    try:
        await client.connect()
        manager = ChannelManager(client)
        resolved = await manager.resolve_source(source_input)
        if not resolved:
            await loading.edit_text(
                f"{EMOJI['error']} Не удалось найти источник или получить к нему доступ.",
                reply_markup=topic_editor_keyboard(bridge_id, get_topic_rules_for_bridge(bridge_id)),
            )
            await state.clear()
            return
        source_id, source_type, source_title = resolved
        await state.update_data(external_source_id=source_id, external_source_type=source_type, external_source_title=source_title)

        if source_type == "forum":
            topics = await manager.get_forum_topics(source_id)
            await state.update_data(available_topics=topics, selected_topic_ids=[])
            await state.set_state(TopicEditorStates.selecting_external_forum_topic)
            await loading.edit_text(
                f"{EMOJI['forum']} <b>Источник — форум</b>\n\n"
                f"Источник: <b>{safe_html(source_title)}</b>\n"
                f"Выберите одну ветку, которую нужно подключить:",
                reply_markup=topic_selection_keyboard(
                    topics,
                    set(),
                    "external_topic_toggle",
                    "external_topic_confirm",
                    f"topic_editor:{bridge_id}",
                ),
            )
            return

        target_title = data.get("external_target_title")
        target_topic_id = await manager.create_target_topic(bridge.target_id, 0, target_title)
        if target_topic_id:
            add_topic_rule(
                bridge_id=bridge_id,
                source_chat_id=source_id,
                source_type=source_type,
                source_thread_id=0,
                source_title=source_title,
                target_chat_id=bridge.target_id,
                target_thread_id=target_topic_id,
                target_title=target_title,
                is_external=True,
            )
        await state.clear()
        await loading.edit_text(
            f"{EMOJI['success']} Ветка <b>{target_title}</b> подключена.",
            reply_markup=topic_editor_keyboard(bridge_id, get_topic_rules_for_bridge(bridge_id)),
        )
    finally:
        await client.disconnect()


@dp.callback_query(F.data.startswith("external_topic_toggle:"))
async def cb_external_topic_toggle(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    topic_id = int(callback.data.split(":")[1])
    data = await state.get_data()
    topics = data.get("available_topics", [])
    selected_ids = {topic_id}
    await state.update_data(selected_topic_ids=list(selected_ids))
    bridge_id = data.get("editor_bridge_id")
    await callback.message.edit_reply_markup(
        reply_markup=topic_selection_keyboard(
            topics,
            selected_ids,
            "external_topic_toggle",
            "external_topic_confirm",
            f"topic_editor:{bridge_id}",
        )
    )


@dp.callback_query(F.data == "external_topic_confirm")
async def cb_external_topic_confirm(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    data = await state.get_data()
    bridge_id = data.get("editor_bridge_id")
    selected_ids = set(data.get("selected_topic_ids", []))
    if len(selected_ids) != 1:
        await callback.answer("Выберите одну ветку", show_alert=True)
        return

    bridge = next((b for b in get_user_bridges(callback.from_user.id) if b.id == bridge_id), None)
    if not bridge:
        await callback.answer("Форум не найден", show_alert=True)
        return

    topic_id = next(iter(selected_ids))
    topics_map = {tid: title for tid, title in data.get("available_topics", [])}
    source_title = topics_map.get(topic_id, f"Topic {topic_id}")
    target_title = data.get("external_target_title")
    session = get_session(bridge.session_id)
    client = build_client(session)
    try:
        await client.connect()
        manager = ChannelManager(client)
        await ensure_bridge_topic_rule(
            manager,
            bridge,
            data.get("external_source_id"),
            topic_id,
            source_title,
            target_title=target_title,
            is_external=True,
        )
    finally:
        await client.disconnect()

    await state.clear()
    rules = get_topic_rules_for_bridge(bridge_id)
    await callback.message.edit_text(
        f"{EMOJI['forum']} <b>Редактор веток</b>\n\n"
        f"Форум: <b>{safe_html(bridge.target_title)}</b>\n"
        f"Активных правил: {len(rules)}",
        reply_markup=topic_editor_keyboard(bridge_id, rules),
    )


# -------------------- CREATE BRIDGE --------------------

@dp.callback_query(F.data == "create_bridge")
async def cb_create_bridge(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    user_id = callback.from_user.id
    sessions = get_user_sessions(user_id)
    
    if not sessions:
        await callback.answer("Сначала подключите хотя бы один аккаунт", show_alert=True)
        return
    
    if len(sessions) == 1:
        # Auto-select if only one session
        await state.update_data(session_id=sessions[0].session_id)
        await state.set_state(BridgeCreateStates.selecting_type)
        await callback.message.edit_text(
            f"{EMOJI['mirror']} <b>Создание зеркала</b>\n\n"
            f"Аккаунт: <b>{safe_html(sessions[0].label)}</b>\n\n"
            f"Шаг 2 из 4. Выберите тип источника:",
            reply_markup=bridge_type_keyboard(),
        )
        return
    
    # Multiple sessions - let user choose
    await state.set_state(BridgeCreateStates.selecting_account)
    await callback.message.edit_text(
        f"{EMOJI['mirror']} <b>Создание зеркала</b>\n\n"
        f"Шаг 1 из 4. Выберите аккаунт:",
        reply_markup=session_list_keyboard(sessions, with_create_bridge=True),
    )


@dp.callback_query(F.data.startswith("select_session_for_bridge:"))
async def cb_select_session_for_bridge(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    session_id = int(callback.data.split(":")[1])
    await state.update_data(session_id=session_id)
    session = get_session(session_id)

    await state.set_state(BridgeCreateStates.selecting_type)
    await callback.message.edit_text(
        f"{EMOJI['mirror']} <b>Создание зеркала</b>\n\n"
        f"Аккаунт: <b>{safe_html(session.label)}</b>\n\n"
        f"Шаг 2 из 4. Выберите тип источника:",
        reply_markup=bridge_type_keyboard(),
    )


@dp.callback_query(F.data.startswith("create_bridge_with_session:"))
async def cb_create_bridge_with_session(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    session_id = int(callback.data.split(":")[1])
    await state.update_data(session_id=session_id)
    session = get_session(session_id)

    await state.set_state(BridgeCreateStates.selecting_type)
    await callback.message.edit_text(
        f"{EMOJI['mirror']} <b>Создание зеркала</b>\n\n"
        f"Аккаунт: <b>{safe_html(session.label)}</b>\n\n"
        f"Шаг 2 из 4. Выберите тип источника:",
        reply_markup=bridge_type_keyboard(),
    )


@dp.message(BridgeCreateStates.entering_source)
async def process_bridge_source(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    source_input = normalize_source_input(message.text)
    selected_source_type = data.get("source_type", "channel")
    session_id = data.get("session_id")

    await state.update_data(source_input=source_input)

    if selected_source_type != "forum":
        await state.set_state(BridgeCreateStates.selecting_filter)
        await message.answer(
            f"{EMOJI['mirror']} <b>Создание зеркала</b>\n\n"
            f"Шаг 4 из 4. Выберите режим фильтрации:",
            reply_markup=filter_type_keyboard(),
        )
        return

    session = get_session(session_id)
    if not session:
        await message.answer(f"{EMOJI['error']} Аккаунт не найден", reply_markup=main_menu_keyboard())
        await state.clear()
        return

    loading = await message.answer(f"{EMOJI['loading']} Читаю ветки форума...")
    client = build_client(session)
    try:
        await client.connect()
        manager = ChannelManager(client)
        resolved = await manager.resolve_source(source_input)
        if not resolved:
            await loading.edit_text(
                f"{EMOJI['error']} Не удалось найти источник. Проверьте ссылку, ID и доступ.",
                reply_markup=main_menu_keyboard(),
            )
            await state.clear()
            return

        source_id, resolved_type, source_title = resolved
        if resolved_type != "forum":
            await loading.edit_text(
                f"{EMOJI['error']} Вы выбрали режим форума, но источник не является форумом.",
                reply_markup=main_menu_keyboard(),
            )
            await state.clear()
            return

        topics = await manager.get_forum_topics(source_id)
        if not topics:
            await loading.edit_text(
                f"{EMOJI['error']} Не удалось получить список веток форума.",
                reply_markup=main_menu_keyboard(),
            )
            await state.clear()
            return

        selected_ids = {topic_id for topic_id, _ in topics}
        selected_ids = ensure_general_topic_selected(topics, selected_ids)
        await state.update_data(
            source_id=source_id,
            source_title=source_title,
            resolved_source_type=resolved_type,
            available_topics=topics,
            selected_topic_ids=list(selected_ids),
        )
        await state.set_state(BridgeCreateStates.selecting_topics)
        await loading.edit_text(
            f"{EMOJI['forum']} <b>Выбор веток</b>\n\n"
            f"Источник: <b>{safe_html(source_title)}</b>\n"
            f"Отметьте ветки, которые нужно подключить:",
            reply_markup=topic_selection_keyboard(
                topics,
                selected_ids,
                "create_topic_toggle",
                "create_topics_confirm",
                "cancel_bridge",
                {GENERAL_TOPIC_ID},
            ),
        )
    finally:
        await client.disconnect()


@dp.callback_query(F.data == "filter_all")
async def cb_filter_all(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.update_data(keywords="")
    await create_bridge(callback.message, state)


@dp.callback_query(F.data == "filter_keywords")
async def cb_filter_keywords(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.set_state(BridgeCreateStates.entering_keywords)
    await callback.message.edit_text(
        f"{EMOJI['key']} <b>Фильтр по словам</b>\n\n"
        f"Введите ключевые слова через запятую.\n\n"
        f"<i>Например: btc, eth, новости</i>",
        reply_markup=back_keyboard("cancel_bridge"),
    )


@dp.message(BridgeCreateStates.entering_keywords)
async def process_keywords(message: types.Message, state: FSMContext) -> None:
    keywords = message.text.strip()
    await state.update_data(keywords=keywords)
    await create_bridge(message, state)


async def create_bridge(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    source_input = data.get("source_input")
    keywords = data.get("keywords", "")
    session_id = data.get("session_id")
    selected_source_type = data.get("source_type", "channel")  # User-selected type
    user_id = message.chat.id if hasattr(message, 'chat') else message.from_user.id
    
    if not all([source_input, session_id]):
        await message.answer(f"{EMOJI['error']} Не хватает данных для создания зеркала.")
        await state.clear()
        return
    
    session = get_session(session_id)
    if not session:
        await message.answer(f"{EMOJI['error']} Аккаунт не найден.")
        await state.clear()
        return
    
    loading = await message.answer(f"{EMOJI['loading']} Создаю зеркало...")
    
    client = build_client(session)
    
    try:
        await client.connect()
        manager = ChannelManager(client)
        
        source_id = data.get("source_id")
        source_title = data.get("source_title")
        resolved_type = data.get("resolved_source_type")

        if not all([source_id, source_title, resolved_type]):
            resolved = await manager.resolve_source(source_input)
            if not resolved:
                await loading.edit_text(
                    f"{EMOJI['error']} Не удалось найти источник. Проверьте ссылку, ID и доступ.",
                    reply_markup=main_menu_keyboard(),
                )
                await state.clear()
                return
            source_id, resolved_type, source_title = resolved

        source_type = resolved_type
        if resolved_type != selected_source_type:
            logger.info(f"Type adjusted: selected={selected_source_type}, resolved={resolved_type}")
        
        result = await manager.create_target_for_source(source_title, source_type)
        if not result:
            await loading.edit_text(
                f"{EMOJI['error']} Не удалось создать целевой чат для зеркала.",
                reply_markup=main_menu_keyboard(),
            )
            await state.clear()
            return
        
        internal_id, target_id, target_title = result
        
        bridge_id = add_bridge(
            user_id=user_id,
            session_id=session_id,
            source_id=source_id,
            source_type=source_type,
            source_title=source_title,
            target_id=target_id,
            target_type=source_type,
            target_title=target_title,
            keywords=keywords,
        )
        
        # If forum, create only selected topics
        topics_text = ""
        if source_type == "forum":
            available_topics = data.get("available_topics", [])
            selected_topic_ids = ensure_general_topic_selected(
                available_topics,
                set(data.get("selected_topic_ids", [])),
            )
            selected_topics = [topic for topic in available_topics if topic[0] in selected_topic_ids]
            created_count = 0

            for topic_id, topic_title in selected_topics:
                if topic_id == GENERAL_TOPIC_ID:
                    add_topic_rule(
                        bridge_id=bridge_id,
                        source_chat_id=source_id,
                        source_type="topic",
                        source_thread_id=GENERAL_TOPIC_ID,
                        source_title=topic_title,
                        target_chat_id=target_id,
                        target_thread_id=GENERAL_TOPIC_ID,
                        target_title=topic_title,
                    )
                    created_count += 1
                    continue

                target_topic_id = await manager.create_target_topic(target_id, topic_id, topic_title)
                if target_topic_id:
                    create_topic_mapping(bridge_id, topic_id, target_topic_id, topic_title)
                    add_topic_rule(
                        bridge_id=bridge_id,
                        source_chat_id=source_id,
                        source_type="topic",
                        source_thread_id=topic_id,
                        source_title=topic_title,
                        target_chat_id=target_id,
                        target_thread_id=target_topic_id,
                        target_title=topic_title,
                    )
                    created_count += 1

            topics_text = f"\n{EMOJI['forum']} Подключено веток: {created_count}"
        
        await loading.delete()
        
        keywords_text = f"\n{EMOJI['key']} Фильтр: {keywords}" if keywords else f"\n{EMOJI['all']} Фильтр: без ограничений"
        
        await message.answer(
            f"{EMOJI['success']} <b>Зеркало создано</b>\n\n"
            f"ID: <code>{bridge_id}</code>\n"
            f"Аккаунт: {safe_html(session.label)}\n"
            f"Источник: {safe_html(source_title)}\n"
            f"Цель: {safe_html(target_title)}"
            f"{keywords_text}"
            f"{topics_text}",
            reply_markup=main_menu_keyboard(),
        )
        
    except Exception as e:
        logger.exception("Create bridge error")
        await loading.edit_text(f"{EMOJI['error']} Ошибка: {e}", reply_markup=main_menu_keyboard())
    finally:
        await client.disconnect()
        await state.clear()


@dp.callback_query(F.data.startswith("bridge_type:"))
async def cb_select_bridge_type(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    bridge_type = callback.data.split(":")[1]
    await state.update_data(source_type=bridge_type)

    hints = (
        f"• @channelname (публичный канал)\n"
        f"• https://t.me/c/1234567890/1 (ссылка на сообщение)\n"
        f"• -1001234567890 (ID канала)"
        if bridge_type == "channel"
        else f"• Ссылка на форум или сообщение из форума\n"
        f"• https://t.me/c/1234567890/123\n"
        f"• -1001234567890 (ID форума)"
        if bridge_type == "forum"
        else
        f"• Перешлите сообщение из чата\n"
        f"• https://t.me/c/1234567890/1 (ссылка из Web/Desktop)\n"
        f"• -1001234567890 (ID чата)"
    )

    await state.set_state(BridgeCreateStates.entering_source)
    await callback.message.edit_text(
        f"{EMOJI['mirror']} <b>Создание зеркала</b>\n\n"
        f"Тип: {'Канал' if bridge_type == 'channel' else 'Форум' if bridge_type == 'forum' else 'Чат'}\n\n"
        f"Шаг 3 из 4. Отправьте источник\n\n"
        f"{hints}",
        reply_markup=back_keyboard("main_menu"),
    )


@dp.callback_query(F.data.startswith("create_topic_toggle:"))
async def cb_create_topic_toggle(callback: types.CallbackQuery, state: FSMContext) -> None:
    topic_id = int(callback.data.split(":")[1])
    data = await state.get_data()
    topics = data.get("available_topics", [])
    selected_ids = set(data.get("selected_topic_ids", []))
    if topic_id == GENERAL_TOPIC_ID:
        await callback.answer("Ветку General нельзя отключить")
        selected_ids = ensure_general_topic_selected(topics, selected_ids)
    elif topic_id in selected_ids:
        await callback.answer()
        selected_ids.remove(topic_id)
    else:
        await callback.answer()
        selected_ids.add(topic_id)
    selected_ids = ensure_general_topic_selected(topics, selected_ids)
    await state.update_data(selected_topic_ids=list(selected_ids))
    await callback.message.edit_reply_markup(
        reply_markup=topic_selection_keyboard(
            topics,
            selected_ids,
            "create_topic_toggle",
            "create_topics_confirm",
            "cancel_bridge",
            {GENERAL_TOPIC_ID},
        )
    )


@dp.callback_query(F.data == "create_topics_confirm")
async def cb_create_topics_confirm(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    data = await state.get_data()
    selected_ids = ensure_general_topic_selected(
        data.get("available_topics", []),
        set(data.get("selected_topic_ids", [])),
    )
    if not selected_ids:
        await callback.answer("Выберите хотя бы одну ветку", show_alert=True)
        return

    await state.set_state(BridgeCreateStates.selecting_filter)
    await callback.message.edit_text(
        f"{EMOJI['mirror']} <b>Создание зеркала</b>\n\n"
        f"Шаг 4 из 4. Настройте фильтр\n\n"
        f"Выбрано веток: {len(selected_ids)}",
        reply_markup=filter_type_keyboard(),
    )


@dp.callback_query(F.data == "cancel_bridge")
async def cb_cancel_bridge(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.clear()
    await callback.message.edit_text("Создание зеркала отменено.", reply_markup=main_menu_keyboard())


# -------------------- HELP & UNKNOWN --------------------

@dp.callback_query(F.data == "help")
async def cb_help(callback: types.CallbackQuery) -> None:
    await callback.answer()
    await callback.message.edit_text(
        f"{EMOJI['mirror']} <b>Помощь</b>\n\n"
        f"<b>Каналы:</b>\n"
        f"• Ссылка на сообщение (Web/Desktop)\n"
        f"• @username (если публичный)\n\n"
        f"<b>Чаты/Группы:</b>\n"
        f"• Добавьте бота в группу → он покажет ID\n"
        f"• Или перешлите сообщение из группы\n\n"
        f"<b>Форумы:</b>\n"
        f"• Можно указать ссылку на форум или сообщение из нужной ветки\n"
        f"• При создании можно выбрать, какие ветки подключать\n\n"
        f"<b>Telegram Web:</b>\n"
        f"Поддерживаются ссылки из web.telegram.org и tg://",
        reply_markup=main_menu_keyboard(),
    )


# Handle bot added to group
@dp.my_chat_member()
async def on_chat_member_update(update: types.ChatMemberUpdated) -> None:
    """When bot is added to group/channel, show the ID."""
    if update.new_chat_member.status in ["member", "administrator"]:
        chat_id = update.chat.id
        chat_title = update.chat.title or "Unknown"
        chat_type = update.chat.type
        
        try:
            await bot.send_message(
                update.from_user.id,
                f"🆔 <b>Чат подключен</b>\n\n"
                f"Название: {chat_title}\n"
                f"Тип: {chat_type}\n"
                f"ID: <code>{chat_id}</code>\n\n"
                f"Этот ID можно использовать при создании зеркала.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass  # User may have blocked bot


@dp.message(F.forward_from_chat)
async def handle_forwarded_message(message: types.Message) -> None:
    """Extract chat ID from forwarded messages."""
    chat = message.forward_from_chat
    chat_id = chat.id
    chat_title = chat.title or "Unknown"
    chat_type = chat.type
    
    type_names = {
        "channel": "Канал",
        "supergroup": "Супергруппа",
        "group": "Группа",
    }
    type_name = type_names.get(chat_type, chat_type)
    
    await message.answer(
        f"🆔 <b>Источник найден</b>\n\n"
        f"Название: {chat_title}\n"
        f"Тип: {type_name}\n"
        f"ID: <code>{chat_id}</code>\n\n"
        f"Чтобы создать зеркало:\n"
        f"1. Нажмите ➕ <b>Новое зеркало</b>\n"
        f"2. Выберите тип: {'Канал' if chat_type == 'channel' else 'Чат'}\n"
        f"3. Введите: <code>{chat_id}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard(),
    )


@dp.message(F.text.startswith("/"))
async def handle_unknown(message: types.Message) -> None:
    cmd = message.text.split()[0].lower()
    
    known = ["/start", "/help"]
    if cmd in known:
        return
    
    await message.answer(
        f"{EMOJI['error']} Неизвестная команда\n\n"
        f"Используйте кнопки меню или команду /start",
        reply_markup=main_menu_keyboard(),
    )


async def main() -> None:
    init_db()
    logger.info("Starting bot...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
