"""MIRROR Bot - Full button navigation with session management."""

import asyncio
import os
from typing import Any
from urllib.parse import quote_plus

from aiogram import Bot, Dispatcher, F, types
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
    add_bridge,
    create_session,
    delete_bridge,
    delete_session,
    get_active_bridges,
    get_all_bridges,
    get_first_session,
    get_session,
    get_user_bridges,
    get_user_sessions,
    has_any_session,
    init_db,
    migrate_old_user_data,
    toggle_bridge,
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
    selecting_filter = State()
    entering_keywords = State()


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
active_qr_sessions: dict[int, dict[str, Any]] = {}


# ==================== KEYBOARDS ====================

def main_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{EMOJI['add']} Добавить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton(text=f"{EMOJI['account']} Мои аккаунты", callback_data="my_accounts")],
        [InlineKeyboardButton(text=f"{EMOJI['mirror']} Мои зеркала", callback_data="my_bridges_menu")],
        [InlineKeyboardButton(text=f"{EMOJI['add']} Создать зеркало", callback_data="create_bridge")],
        [InlineKeyboardButton(text=f"{EMOJI['settings']} Помощь", callback_data="help")],
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
        [InlineKeyboardButton(text=f"{EMOJI['mirror']} Зеркала этого акка", callback_data=f"session_bridges:{session_id}")],
        [InlineKeyboardButton(text=f"{EMOJI['add']} Создать зеркало", callback_data=f"create_bridge_with_session:{session_id}")],
        [InlineKeyboardButton(text="📋 Получить session string", callback_data=f"get_session_string:{session_id}")],
        [InlineKeyboardButton(text=f"{EMOJI['deleted']} Удалить аккаунт", callback_data=f"delete_session:{session_id}")],
        [InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data="my_accounts")],
    ])


def bridge_list_keyboard(bridges, sessions_map, back_to="my_bridges_menu"):
    buttons = []
    for b in bridges:
        status = EMOJI['active'] if b.is_active else EMOJI['inactive']
        emoji = EMOJI['channel'] if b.source_type == 'channel' else EMOJI['chat']
        session_info = sessions_map.get(b.session_id, "")
        text = f"{status} {emoji} {b.source_title[:20]}"
        if session_info:
            text += f" ({session_info.label[:15]})"
        buttons.append([InlineKeyboardButton(text=text, callback_data=f"bridge_detail:{b.id}")])
    buttons.append([InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data=back_to)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def bridge_detail_keyboard(bridge_id, is_active):
    toggle_text = "🔴 Выключить" if is_active else "🟢 Включить"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=toggle_text, callback_data=f"toggle_bridge:{bridge_id}")],
        [InlineKeyboardButton(text=f"{EMOJI['deleted']} Удалить", callback_data=f"delete_bridge:{bridge_id}")],
        [InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data="my_bridges_menu")],
    ])


def filter_type_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{EMOJI['all']} Все сообщения", callback_data="filter_all")],
        [InlineKeyboardButton(text=f"{EMOJI['key']} По ключевым словам", callback_data="filter_keywords")],
        [InlineKeyboardButton(text=f"{EMOJI['back']} Отмена", callback_data="cancel_bridge")],
    ])


def bridge_type_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{EMOJI['channel']} Канал", callback_data="bridge_type:channel")],
        [InlineKeyboardButton(text=f"{EMOJI['chat']} Чат/Группа", callback_data="bridge_type:chat")],
        [InlineKeyboardButton(text=f"{EMOJI['back']} Отмена", callback_data="main_menu")],
    ])


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
            "Добро пожаловать! Добавьте Telegram аккаунт,\n"
            "чтобы начать создавать зеркала каналов.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"{EMOJI['add']} Добавить аккаунт (QR)", callback_data="add_account")],
            ]),
        )
        return
    
    await message.answer(
        f"{EMOJI['welcome']} <b>MIRROR Bot</b>\n\n"
        f"{EMOJI['account']} Аккаунтов: {len(sessions)}\n\n"
        f"Выберите действие:",
        parse_mode=ParseMode.HTML,
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
        text += f"{EMOJI['account']} Аккаунтов: {len(sessions)}\n\n"
    text += "Выберите действие:"
    
    await callback.message.edit_text(text, reply_markup=main_menu_keyboard())


# -------------------- ACCOUNTS --------------------

@dp.callback_query(F.data == "add_account")
async def cb_add_account(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await cleanup_qr_session(callback.from_user.id)
    await state.set_state(QrStates.waiting_for_api_id)
    await callback.message.edit_text(
        f"{EMOJI['account']} <b>Добавление аккаунта</b>\n\n"
        "Шаг 1/2: Введите API_ID\n"
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
        "Шаг 2/2: Введите API_HASH\n"
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
            f"Создайте первое:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"{EMOJI['add']} Создать", callback_data="create_bridge")],
                [InlineKeyboardButton(text=f"{EMOJI['back']} Назад", callback_data="main_menu")],
            ]),
        )
        return
    
    channels_count = sum(1 for b in bridges if b.source_type == "channel")
    chats_count = sum(1 for b in bridges if b.source_type == "chat")
    
    await callback.message.edit_text(
        f"{EMOJI['mirror']} <b>Мои зеркала</b>\n\n"
        f"📺 Каналы: {channels_count}\n"
        f"💬 Чаты: {chats_count}\n"
        f"📋 Всего: {len(bridges)}\n\n"
        f"Выберите раздел:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"📺 Каналы ({channels_count})", callback_data="my_bridges:channel")],
            [InlineKeyboardButton(text=f"💬 Чаты ({chats_count})", callback_data="my_bridges:chat")],
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
async def cb_bridge_detail(callback: types.CallbackQuery) -> None:
    bridge_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    bridges = get_user_bridges(user_id)
    bridge = next((b for b in bridges if b.id == bridge_id), None)
    
    if not bridge:
        await callback.answer("Зеркало не найдено!")
        return
    
    sessions = {s.session_id: s for s in get_user_sessions(user_id)}
    session = sessions.get(bridge.session_id)
    
    status = "🟢 Активно" if bridge.is_active else "🔴 Выключено"
    keywords_info = f"{EMOJI['key']} {bridge.keywords}" if bridge.keywords else f"{EMOJI['all']} Все сообщения"
    
    text = (
        f"{EMOJI['mirror']} <b>Зеркало {bridge_id}</b>\n\n"
        f"Статус: {status}\n"
        f"Источник: {bridge.source_title}\n"
        f"Цель: {bridge.target_title}\n"
        f"Фильтр: {keywords_info}\n"
    )
    if session:
        text += f"\nАккаунт: {session.label}"
    
    await callback.message.edit_text(
        text,
        reply_markup=bridge_detail_keyboard(bridge_id, bridge.is_active),
    )


@dp.callback_query(F.data.startswith("toggle_bridge:"))
async def cb_toggle_bridge(callback: types.CallbackQuery) -> None:
    bridge_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    
    bridges = get_user_bridges(user_id)
    bridge = next((b for b in bridges if b.id == bridge_id), None)
    
    if not bridge:
        await callback.answer("Зеркало не найдено!")
        return
    
    new_status = not bridge.is_active
    toggle_bridge(bridge_id, new_status)
    await callback.answer("Статус изменен!")
    await cb_bridge_detail(callback)


@dp.callback_query(F.data.startswith("delete_bridge:"))
async def cb_delete_bridge(callback: types.CallbackQuery) -> None:
    bridge_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    
    if delete_bridge(bridge_id):
        await callback.answer("Зеркало удалено")
        await cb_my_bridges(callback)
    else:
        await callback.answer("Не удалось удалить", show_alert=True)


# -------------------- CREATE BRIDGE --------------------

@dp.callback_query(F.data == "create_bridge")
async def cb_create_bridge(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    user_id = callback.from_user.id
    sessions = get_user_sessions(user_id)
    
    if not sessions:
        await callback.answer("Сначала добавьте аккаунт!", show_alert=True)
        return
    
    if len(sessions) == 1:
        # Auto-select if only one session
        await state.update_data(session_id=sessions[0].session_id)
        await state.set_state(BridgeCreateStates.selecting_type)
        await callback.message.edit_text(
            f"{EMOJI['mirror']} <b>Создание зеркала</b>\n\n"
            f"Аккаунт: {sessions[0].label}\n\n"
            f"Шаг 2/4: Выберите тип источника:",
            reply_markup=bridge_type_keyboard(),
        )
        return
    
    # Multiple sessions - let user choose
    await state.set_state(BridgeCreateStates.selecting_account)
    await callback.message.edit_text(
        f"{EMOJI['mirror']} <b>Создание зеркала</b>\n\n"
        f"Шаг 1/4: Выберите аккаунт:",
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
        f"Аккаунт: {session.label}\n\n"
        f"Шаг 2/4: Выберите тип источника:",
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
        f"Аккаунт: {session.label}\n\n"
        f"Шаг 2/4: Выберите тип источника:",
        reply_markup=bridge_type_keyboard(),
    )


@dp.message(BridgeCreateStates.entering_source)
async def process_bridge_source(message: types.Message, state: FSMContext) -> None:
    source_input = message.text.strip()
    
    # Parse link
    if source_input.startswith("https://t.me/"):
        from urllib.parse import urlparse
        path = urlparse(source_input).path.strip("/").split("/")
        if len(path) >= 2 and path[0] == "c":
            try:
                chat_id = int(path[1])
                source_input = f"-100{chat_id}"
            except (ValueError, IndexError):
                pass
        elif len(path) >= 1:
            source_input = f"@{path[0]}"
    
    await state.update_data(source_input=source_input)
    await state.set_state(BridgeCreateStates.selecting_filter)

    await message.answer(
        f"{EMOJI['mirror']} <b>Создание зеркала</b> — Шаг 4/4\n\n"
        f"Выберите тип фильтра:",
        reply_markup=filter_type_keyboard(),
    )


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
        f"{EMOJI['key']} Введите ключевые слова через запятую:\n\n"
        f"<i>Например: btc,eth,новости</i>",
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
        await message.answer(f"{EMOJI['error']} Ошибка данных")
        await state.clear()
        return
    
    session = get_session(session_id)
    if not session:
        await message.answer(f"{EMOJI['error']} Аккаунт не найден")
        await state.clear()
        return
    
    loading = await message.answer(f"{EMOJI['loading']} Создание...")
    
    client = TelegramClient(
        StringSession(session.session_string),
        session.api_id,
        session.api_hash,
    )
    
    try:
        await client.connect()
        manager = ChannelManager(client)
        
        resolved = await manager.resolve_source(source_input)
        if not resolved:
            await loading.edit_text(
                f"{EMOJI['error']} Источник не найден. Проверьте доступ.",
                reply_markup=main_menu_keyboard(),
            )
            await state.clear()
            return
        
        source_id, resolved_type, source_title = resolved
        # Use user-selected type, but log if there's a mismatch
        source_type = selected_source_type
        if resolved_type != selected_source_type:
            logger.warning(f"Type mismatch: selected={selected_source_type}, resolved={resolved_type}")
        
        result = await manager.create_target_for_source(source_title, source_type)
        if not result:
            await loading.edit_text(
                f"{EMOJI['error']} Не удалось создать канал",
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
        
        await loading.delete()
        
        keywords_text = f"\n{EMOJI['key']} Фильтр: {keywords}" if keywords else f"\n{EMOJI['all']} Все сообщения"
        
        await message.answer(
            f"{EMOJI['success']} <b>Зеркало создано!</b>\n\n"
            f"ID: <code>{bridge_id}</code>\n"
            f"Аккаунт: {session.label}\n"
            f"Источник: {source_title}\n"
            f"Цель: {target_title}"
            f"{keywords_text}",
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
    bridge_type = callback.data.split(":")[1]  # channel or chat
    await state.update_data(source_type=bridge_type)

    hints = (
        f"• @channelname (публичный канал)\n"
        f"• https://t.me/c/1234567890/1 (ссылка на сообщение)\n"
        f"• -1001234567890 (ID канала)"
        if bridge_type == "channel"
        else
        f"• Перешлите сообщение из чата\n"
        f"• https://t.me/c/1234567890/1 (ссылка из Web/Desktop)\n"
        f"• -1001234567890 (ID чата)"
    )

    await state.set_state(BridgeCreateStates.entering_source)
    await callback.message.edit_text(
        f"{EMOJI['mirror']} <b>Создание зеркала</b>\n\n"
        f"Тип: {'Канал' if bridge_type == 'channel' else 'Чат'}\n\n"
        f"Шаг 3/4: Отправьте источник\n\n"
        f"{hints}",
        reply_markup=back_keyboard("main_menu"),
    )


@dp.callback_query(F.data == "cancel_bridge")
async def cb_cancel_bridge(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.clear()
    await callback.message.edit_text("Отменено", reply_markup=main_menu_keyboard())


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
        f"<b>Telegram Web:</b>\n"
        f"В адресной строке виден ID чата",
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
                f"🆔 <b>Бот добавлен в чат!</b>\n\n"
                f"Название: {chat_title}\n"
                f"Тип: {chat_type}\n"
                f"ID: <code>{chat_id}</code>\n\n"
                f"Для создания зеркала используйте:\n"
                f"<code>/add {chat_id}</code>",
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
        f"🆔 <b>Информация об источнике:</b>\n\n"
        f"Название: {chat_title}\n"
        f"Тип: {type_name}\n"
        f"ID: <code>{chat_id}</code>\n\n"
        f"Для создания зеркала:\n"
        f"1. Нажмите ➕ <b>Создать зеркало</b>\n"
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
        f"Используйте кнопки или /start",
        reply_markup=main_menu_keyboard(),
    )


async def main() -> None:
    init_db()
    logger.info("Starting bot...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
