"""Management bot for Telegram Group Bridge."""

import asyncio
import os
from typing import Any

from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from dotenv import load_dotenv
from loguru import logger
from telethon import TelegramClient
from telethon.sessions import StringSession

from src.channel_manager import ChannelManager
from src.database import (
    add_bridge,
    delete_bridge,
    get_user_bridges,
    get_user_credentials,
    has_user_session,
    init_db,
    save_session_string,
    save_user_credentials,
    toggle_bridge,
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is required in .env")

# Emoji
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
}

# States for API setup
class SetupStates(StatesGroup):
    waiting_for_api_id = State()
    waiting_for_api_hash = State()
    waiting_for_phone = State()
    waiting_for_code = State()


# States for bridge creation
class BridgeStates(StatesGroup):
    waiting_for_source = State()
    waiting_for_keywords_choice = State()
    waiting_for_keywords = State()


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# Keyboards

def get_setup_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔧 Настроить API", callback_data="setup_api")],
    ])


def get_keywords_choice_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"{EMOJI['all']} Без ключевых слов", callback_data="keywords_none"),
            InlineKeyboardButton(text=f"{EMOJI['key']} С ключевыми словами", callback_data="keywords_yes"),
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")],
    ])


def get_main_menu_keyboard(has_bridges: bool = False):
    buttons = [
        [InlineKeyboardButton(text=f"➕ {EMOJI['mirror']} Создать зеркало", callback_data="create_bridge")],
    ]
    if has_bridges:
        buttons.append([InlineKeyboardButton(text="📜 Мои зеркала", callback_data="list_bridges")])
    buttons.append([InlineKeyboardButton(text="❓ Помощь", callback_data="help")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# Helper functions

async def get_user_client(user_id: int) -> TelegramClient | None:
    creds = get_user_credentials(user_id)
    if not creds or not creds.session_string:
        return None
    
    client = TelegramClient(
        StringSession(creds.session_string),
        creds.api_id,
        creds.api_hash,
    )
    return client


# Handlers

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext) -> None:
    await state.clear()
    user_id = message.from_user.id
    
    if not has_user_session(user_id):
        await message.answer(
            f"{EMOJI['welcome']} <b>Добро пожаловать в MIRROR Bot!</b> {EMOJI['mirror']}\n\n"
            "Для начала работы нужно настроить Telegram API.\n"
            "Это безопасно — данные хранятся только у вас.",
            parse_mode=ParseMode.HTML,
            reply_markup=get_setup_keyboard(),
        )
        return
    
    bridges = get_user_bridges(user_id)
    await message.answer(
        f"{EMOJI['welcome']} <b>MIRROR Bot</b> {EMOJI['mirror']}\n\n"
        "Создавай зеркала каналов и чатов!\n\n"
        f"<b>📋 Команды:</b>\n"
        "━━━━━━━━━━━━━━━\n"
        f"➕ /add — создать зеркало\n"
        f"📜 /list — список зеркал\n"
        f"🗑 /remove — удалить зеркало\n"
        f"🔘 /toggle — вкл/выкл зеркало\n"
        f"🔧 /setup — изменить API",
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_menu_keyboard(bool(bridges)),
    )


@dp.callback_query(F.data == "setup_api")
async def cb_setup_api(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.message.edit_text(
        "🔧 <b>Настройка Telegram API</b>\n\n"
        "Шаг 1/3: Введите ваш <b>API_ID</b>\n\n"
        "<i>Это число, например: 12345678</i>\n\n"
        "<a href='https://my.telegram.org'>Получить API_ID и API_HASH</a>",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    await state.set_state(SetupStates.waiting_for_api_id)


@dp.message(SetupStates.waiting_for_api_id)
async def process_api_id(message: types.Message, state: FSMContext) -> None:
    try:
        api_id = int(message.text.strip())
        await state.update_data(api_id=api_id)
        await message.answer(
            "🔧 <b>Настройка API</b> — Шаг 2/3\n\n"
            "Введите ваш <b>API_HASH</b>\n\n"
            "<i>Это строка из букв и цифр</i>",
            parse_mode=ParseMode.HTML,
        )
        await state.set_state(SetupStates.waiting_for_api_hash)
    except ValueError:
        await message.answer(
            f"{EMOJI['error']} API_ID должен быть числом!\n"
            "Попробуйте снова:",
            parse_mode=ParseMode.HTML,
        )


@dp.message(SetupStates.waiting_for_api_hash)
async def process_api_hash(message: types.Message, state: FSMContext) -> None:
    api_hash = message.text.strip()
    if len(api_hash) < 10:
        await message.answer(
            f"{EMOJI['error']} API_HASH слишком короткий!\n"
            "Попробуйте снова:",
            parse_mode=ParseMode.HTML,
        )
        return
    
    await state.update_data(api_hash=api_hash)
    await message.answer(
        "🔧 <b>Настройка API</b> — Шаг 3/3\n\n"
        "Введите ваш <b>номер телефона</b>\n\n"
        "<i>Формат: +79123456789</i>",
        parse_mode=ParseMode.HTML,
    )
    await state.set_state(SetupStates.waiting_for_phone)


@dp.message(SetupStates.waiting_for_phone)
async def process_phone(message: types.Message, state: FSMContext) -> None:
    phone = message.text.strip()
    if not phone.startswith("+"):
        await message.answer(
            f"{EMOJI['error']} Номер должен начинаться с + !\n"
            "Пример: +79123456789",
            parse_mode=ParseMode.HTML,
        )
        return
    
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    
    # Save credentials
    save_user_credentials(message.from_user.id, api_id, api_hash, phone)
    
    # Start client and send code
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    
    try:
        sent_code = await client.send_code_request(phone)
        await state.update_data(phone=phone, phone_code_hash=sent_code.phone_code_hash)
        await message.answer(
            f"{EMOJI['loading']} <b>Код отправлен!</b>\n\n"
            f"Введите код подтверждения из SMS/Telegram:\n\n"
            f"<i>Формат: 12345</i>",
            parse_mode=ParseMode.HTML,
        )
        await state.set_state(SetupStates.waiting_for_code)
    except Exception as e:
        await message.answer(
            f"{EMOJI['error']} Ошибка отправки кода: {e}\n\n"
            "Начните заново: /start",
            parse_mode=ParseMode.HTML,
        )
        await state.clear()
    finally:
        await client.disconnect()


@dp.message(SetupStates.waiting_for_code)
async def process_code(message: types.Message, state: FSMContext) -> None:
    code = message.text.strip()
    data = await state.get_data()
    
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    phone = data["phone"]
    phone_code_hash = data.get("phone_code_hash")
    
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    
    try:
        await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
        session_string = client.session.save()
        
        # Save session
        save_session_string(message.from_user.id, session_string)
        
        await message.answer(
            f"{EMOJI['success']} <b>API настроен успешно!</b>\n\n"
            f"Теперь можно создавать зеркала.\n\n"
            f"Давайте создадим первое зеркало?",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"➕ {EMOJI['mirror']} Создать зеркало", callback_data="create_bridge")],
                [InlineKeyboardButton(text="❌ Позже", callback_data="cancel")],
            ]),
        )
        await state.clear()
        
    except Exception as e:
        await message.answer(
            f"{EMOJI['error']} Ошибка верификации: {e}\n\n"
            "Проверьте код и попробуйте снова.",
            parse_mode=ParseMode.HTML,
        )
    finally:
        await client.disconnect()


@dp.callback_query(F.data == "create_bridge")
@dp.message(Command("add"))
async def start_create_bridge(message_or_callback, state: FSMContext) -> None:
    if isinstance(message_or_callback, types.CallbackQuery):
        message = message_or_callback.message
        await message_or_callback.answer()
    else:
        message = message_or_callback
    
    user_id = message.chat.id if hasattr(message, 'chat') else message.from_user.id
    
    if not has_user_session(user_id):
        await message.answer(
            f"{EMOJI['error']} <b>Сначала настройте API!</b>\n\n"
            "Используйте /setup или нажмите кнопку ниже:",
            parse_mode=ParseMode.HTML,
            reply_markup=get_setup_keyboard(),
        )
        return
    
    await message.answer(
        f"{EMOJI['mirror']} <b>Создание зеркала</b>\n\n"
        "Шаг 1/3: Отправьте источник\n\n"
        "<b>Это может быть:</b>\n"
        "• Юзернейм канала: @channelname\n"
        "• Ссылка: https://t.me/channel\n"
        "• ID: -1001234567890",
        parse_mode=ParseMode.HTML,
    )
    await state.set_state(BridgeStates.waiting_for_source)


@dp.message(BridgeStates.waiting_for_source)
async def process_bridge_source(message: types.Message, state: FSMContext) -> None:
    source_input = message.text.strip()
    await state.update_data(source_input=source_input)
    
    await message.answer(
        f"{EMOJI['mirror']} <b>Создание зеркала</b> — Шаг 2/3\n\n"
        "<b>Выберите вариант:</b>\n\n"
        f"{EMOJI['all']} <b>Без ключевых слов</b> — будут пересылаться ВСЕ сообщения\n\n"
        f"{EMOJI['key']} <b>С ключевыми словами</b> — только сообщения с определёнными словами",
        parse_mode=ParseMode.HTML,
        reply_markup=get_keywords_choice_keyboard(),
    )
    await state.set_state(BridgeStates.waiting_for_keywords_choice)


@dp.callback_query(F.data == "keywords_none")
async def cb_no_keywords(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await state.update_data(keywords="")
    await create_bridge(callback.message, state)


@dp.callback_query(F.data == "keywords_yes")
async def cb_yes_keywords(callback: types.CallbackQuery, state: FSMContext) -> None:
    await callback.message.edit_text(
        f"{EMOJI['mirror']} <b>Создание зеркала</b> — Шаг 3/3\n\n"
        f"{EMOJI['key']} Введите <b>ключевые слова</b> через запятую:\n\n"
        "<i>Пример: биткоин,btc,новости</i>\n\n"
        "Будут пересылаться только сообщения, содержащие эти слова.",
        parse_mode=ParseMode.HTML,
    )
    await state.set_state(BridgeStates.waiting_for_keywords)


@dp.message(BridgeStates.waiting_for_keywords)
async def process_keywords(message: types.Message, state: FSMContext) -> None:
    keywords = message.text.strip()
    await state.update_data(keywords=keywords)
    await create_bridge(message, state)


async def create_bridge(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    source_input = data["source_input"]
    keywords = data.get("keywords", "")
    user_id = message.from_user.id
    
    loading_msg = await message.answer(f"{EMOJI['loading']} Поиск источника...")
    
    try:
        client = await get_user_client(user_id)
        if not client:
            await loading_msg.edit_text(
                f"{EMOJI['error']} Ошибка: клиент не настроен!",
                parse_mode=ParseMode.HTML,
            )
            await state.clear()
            return
        
        await client.connect()
        manager = ChannelManager(client)
        
        resolved = await manager.resolve_source(source_input)
        if not resolved:
            await loading_msg.edit_text(
                f"{EMOJI['error']} <b>Источник не найден!</b>\n\n"
                "Проверьте:\n"
                "• Правильность ссылки/ID\n"
                "• Доступ бота к источнику",
                parse_mode=ParseMode.HTML,
            )
            await state.clear()
            return
        
        source_id, source_type, source_title = resolved
        
        await loading_msg.edit_text(f"{EMOJI['loading']} Создание {EMOJI['mirror']} зеркала...")
        
        internal_id, target_id, target_title = await manager.create_target_for_source(
            source_title, source_type
        )
        
        bridge_id = add_bridge(
            user_id=user_id,
            source_id=source_id,
            source_type=source_type,
            source_title=source_title,
            target_id=target_id,
            target_type=source_type,
            target_title=target_title,
            keywords=keywords,
        )
        
        await loading_msg.delete()
        
        keywords_text = f"\n{EMOJI['key']} <b>Ключевые слова:</b> <code>{keywords}</code>" if keywords else f"\n{EMOJI['all']} <b>Пересылка:</b> все сообщения"
        
        await message.answer(
            f"{EMOJI['success']} <b>Зеркало создано!</b> {EMOJI['success']}\n\n"
            f"{EMOJI['mirror']} <b>Источник:</b> <code>{source_title}</code>\n"
            f"{EMOJI['channel'] if source_type == 'channel' else EMOJI['chat']} <b>Тип:</b> {source_type.upper()}\n"
            f"🎯 <b>Зеркало:</b> <code>{target_title}</code>\n"
            f"🆔 <b>ID бриджа:</b> <code>{bridge_id}</code>"
            f"{keywords_text}\n\n"
            f"<i>✨ Зеркало активно!</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=get_main_menu_keyboard(has_bridges=True),
        )
        
    except Exception as e:
        logger.exception("Error creating bridge")
        await loading_msg.edit_text(
            f"{EMOJI['error']} <b>Ошибка:</b> <code>{e}</code>",
            parse_mode=ParseMode.HTML,
        )
    finally:
        await client.disconnect()
        await state.clear()


@dp.callback_query(F.data == "cancel")
async def cb_cancel(callback: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.edit_text(
        f"{EMOJI['mirror']} <b>Отменено</b>\n\n"
        "Используйте /start для возврата в меню."
    )


@dp.callback_query(F.data == "list_bridges")
@dp.message(Command("list"))
async def cmd_list(message_or_callback, state: FSMContext) -> None:
    await state.clear()
    
    if isinstance(message_or_callback, types.CallbackQuery):
        message = message_or_callback.message
        await message_or_callback.answer()
    else:
        message = message_or_callback
    
    user_id = message.chat.id if hasattr(message, 'chat') else message.from_user.id
    bridges = get_user_bridges(user_id)

    if not bridges:
        await message.answer(
            f"{EMOJI['mirror']} <b>У вас пока нет зеркал!</b>\n\n"
            "Создайте первое зеркало:\n"
            "/add",
            parse_mode=ParseMode.HTML,
        )
        return

    lines = [f"{EMOJI['mirror']} <b>Ваши зеркала:</b>\n"]
    
    for b in bridges:
        status = f"{EMOJI['active']} ВКЛ" if b.is_active else f"{EMOJI['inactive']} ВЫКЛ"
        emoji = EMOJI['channel'] if b.source_type == "channel" else EMOJI['chat']
        keywords_info = f"{EMOJI['key']} <code>{b.keywords}</code>" if b.keywords else f"{EMOJI['all']} все"
        lines.append(
            f"\n{emoji} <b>ID:</b> <code>{b.id}</code> — {status}\n"
            f"   <b>От:</b> {b.source_title}\n"
            f"   <b>К:</b> {b.target_title}\n"
            f"   <b>Фильтр:</b> {keywords_info}"
        )

    text = "\n".join(lines)
    text += f"\n\n<i>✨ Используйте /toggle ID или /remove ID</i>"

    await message.answer(text, parse_mode=ParseMode.HTML)


@dp.message(Command("remove"))
async def cmd_remove(message: types.Message, state: FSMContext) -> None:
    await state.clear()
    args = message.text.split()
    
    if len(args) < 2:
        await message.answer(
            f"{EMOJI['error']} <b>Использование:</b> <code>/remove ID</code>\n\n"
            "Узнать ID: /list",
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        bridge_id = int(args[1])
    except ValueError:
        await message.answer(
            f"{EMOJI['error']} ID должен быть числом!",
            parse_mode=ParseMode.HTML,
        )
        return

    user_id = message.from_user.id
    bridges = get_user_bridges(user_id)
    bridge = next((b for b in bridges if b.id == bridge_id), None)

    if not bridge:
        await message.answer(
            f"{EMOJI['error']} Зеркало <code>{bridge_id}</code> не найдено!",
            parse_mode=ParseMode.HTML,
        )
        return

    if delete_bridge(bridge_id):
        await message.answer(
            f"{EMOJI['deleted']} <b>Зеркало {bridge_id} удалено!</b>\n\n"
            f"{EMOJI['mirror']} <b>Было:</b> {bridge.source_title} → {bridge.target_title}",
            parse_mode=ParseMode.HTML,
        )
    else:
        await message.answer(
            f"{EMOJI['error']} Не удалось удалить зеркало {bridge_id}",
            parse_mode=ParseMode.HTML,
        )


@dp.message(Command("toggle"))
async def cmd_toggle(message: types.Message, state: FSMContext) -> None:
    await state.clear()
    args = message.text.split()
    
    if len(args) < 2:
        await message.answer(
            f"{EMOJI['error']} <b>Использование:</b> <code>/toggle ID</code>\n\n"
            "Узнать ID: /list",
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        bridge_id = int(args[1])
    except ValueError:
        await message.answer(
            f"{EMOJI['error']} ID должен быть числом!",
            parse_mode=ParseMode.HTML,
        )
        return

    user_id = message.from_user.id
    bridges = get_user_bridges(user_id)
    bridge = next((b for b in bridges if b.id == bridge_id), None)

    if not bridge:
        await message.answer(
            f"{EMOJI['error']} Зеркало <code>{bridge_id}</code> не найдено!",
            parse_mode=ParseMode.HTML,
        )
        return

    new_status = not bridge.is_active
    toggle_bridge(bridge_id, new_status)

    status_emoji = EMOJI['active'] if new_status else EMOJI['inactive']
    status_text = "ВКЛЮЧЕНО" if new_status else "ВЫКЛЮЧЕНО"
    
    await message.answer(
        f"{status_emoji} <b>Зеркало {bridge_id} {status_text}</b>\n\n"
        f"{EMOJI['mirror']} <b>Источник:</b> {bridge.source_title}",
        parse_mode=ParseMode.HTML,
    )


@dp.message(Command("setup"))
async def cmd_setup(message: types.Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer(
        "🔧 <b>Настройка API</b>\n\n"
        "Внимание: текущие настройки будут заменены!\n\n"
        "Продолжить?",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, настроить", callback_data="setup_api")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")],
        ]),
    )


@dp.callback_query(F.data == "help")
async def cb_help(callback: types.CallbackQuery) -> None:
    await callback.message.edit_text(
        f"{EMOJI['mirror']} <b>Помощь MIRROR Bot</b>\n\n"
        "<b>Что такое зеркало?</b>\n"
        "Копия канала или чата, куда автоматически попадают все сообщения.\n\n"
        "<b>Как это работает:</b>\n"
        "1️⃣ Вы добавляете источник с помощью /add\n"
        "2️⃣ Бот создаёт новый канал/чат 'MIRROR: Название'\n"
        "3️⃣ Все сообщения из источника появляются в зеркале\n\n"
        "<b>Варианты при создании:</b>\n"
        f"{EMOJI['all']} Без ключевых слов — все сообщения\n"
        f"{EMOJI['key']} С ключевыми словами — только с определёнными словами\n\n"
        "<b>Ключевые слова:</b>\n"
        "Фильтр сообщений. Например: btc,eth — только сообщения с этими словами.",
        parse_mode=ParseMode.HTML,
    )


async def main() -> None:
    init_db()
    logger.info("Starting bot...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
