# MIRROR Bot — Telegram Bridge

Telegram bridge с красивым ботом управления. Каждый пользователь настраивает свой Telegram API и создаёт зеркала каналов/чатов.

## Как это работает

```
Пользователь → Бот → Создаёт зеркало
                    ↓
Worker запускается с API пользователя
                    ↓
Сообщения из источника → Копируются в зеркало
```

## Возможности

- 🔧 Настройка Telegram API прямо в боте
- 🪞 Создание зеркал каналов и чатов
- 🔑 Ключевые слова (фильтр сообщений)
- 📄 Или все сообщения без фильтра
- 🟢/🔴 Включение/выключение зеркал
- ✨ Копии сообщений (не пересылки)
- 📷 Поддержка медиа (фото, видео, документы)

## Ключевые слова — что это?

**Ключевые слова** — фильтр для сообщений. Если указаны, в зеркало попадут только сообщения, содержащие эти слова.

| Вариант | Описание | Пример |
|---------|----------|--------|
| **Без ключевых слов** | Все сообщения пересылаются | Весь контент канала |
| **С ключевыми словами** | Только сообщения со словами из списка | Только новости о "btc" и "eth" |

**Примеры использования:**
```
# Все сообщения
/add @cryptonews

# Только про биткоин и эфир
/add @cryptonews btc,eth,bitcoin

# Только сигналы
/add @tradingchat signal,покупка,продажа
```

## Структура проекта

```
telegram_group_bridge/
├── main.py              # Bridge Worker (многопользовательский)
├── src/
│   ├── bot.py          # Telegram Bot (aiogram)
│   ├── database.py     # SQLite (users + bridges)
│   ├── channel_manager.py
│   └── parser.py
├── data/               # База данных
├── logs/               # Логи
├── docker-compose.yml
├── Dockerfile
└── README.md
```

## Установка

### 1. Клонируйте репозиторий

```bash
git clone <repo-url> mirror-bot
cd mirror-bot
```

### 2. Создайте необходимые папки

```bash
mkdir -p data logs
```

### 3. Настройте окружение

```bash
cp .env.example .env
nano .env
```

Заполните:
```env
BOT_TOKEN=your_bot_token_from_botfather
```

Получить BOT_TOKEN: https://t.me/BotFather

### 4. Запустите

**Docker (рекомендуется):**
```bash
docker-compose up -d
```

**Или локально:**
```bash
pip install -r requirements.txt

# Запустите бота (в одном терминале)
python src/bot.py

# Запустите worker (в другом терминале)
python main.py
```

## Использование

### 1. Начните диалог с ботом

Отправьте `/start` боту. Бот попросит настроить Telegram API.

### 2. Настройте API (3 шага)

```
Шаг 1: Введите API_ID (число, например: 12345678)
Шаг 2: Введите API_HASH (строка из my.telegram.org)
Шаг 3: Введите номер телефона (+79123456789)
Шаг 4: Введите код подтверждения из SMS/Telegram
```

**Где получить API_ID и API_HASH:** https://my.telegram.org

### 3. Создайте зеркало

```
/add @sourcename
```

Или с ключевыми словами:
```
/add @sourcename btc,eth,новости
```

Бот автоматически:
1. Найдёт источник
2. Создаст канал/чат "MIRROR: Название источника"
3. Начнёт копировать сообщения

### Команды бота

| Команда | Описание |
|---------|----------|
| `/start` | Начать / главное меню |
| `/add` | Создать новое зеркало |
| `/list` | Список ваших зеркал |
| `/toggle ID` | Включить/выключить зеркало |
| `/remove ID` | Удалить зеркало |
| `/setup` | Перенастроить API |

## Деплой на Ubuntu Server

### Docker Compose (рекомендуется)

```bash
# 1. Клонируйте
git clone <repo> mirror-bot && cd mirror-bot

# 2. Создайте папки для данных
mkdir -p data logs

# 3. Настройте только BOT_TOKEN
nano .env

# 4. Запустите
sudo docker-compose up -d

# 5. Проверьте логи
sudo docker-compose logs -f bot
sudo docker-compose logs -f bridge
```

### Systemd (без Docker)

```bash
# Создание папок
mkdir -p data logs

# Установка зависимостей
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Копируйте service файлы
sudo cp telegram-bridge.service /etc/systemd/system/
sudo cp telegram-bridge-bot.service /etc/systemd/system/

# Запуск
sudo systemctl daemon-reload
sudo systemctl enable telegram-bridge telegram-bridge-bot
sudo systemctl start telegram-bridge telegram-bridge-bot

# Логи
sudo journalctl -u telegram-bridge -f
sudo journalctl -u telegram-bridge-bot -f
```

## Безопасность

- API credentials хранятся в SQLite (data/bridge.db)
- Session strings тоже в БД
- Каждый пользователь работает со своим аккаунтом
- Worker не имеет доступа к чужим данным

## Примечания

- Для каждого пользователя создаётся отдельный Telethon клиент
- Worker автоматически переподключается при ошибках
- Зеркала создаются от имени пользователя (в его аккаунте)
- Бот управляет через токен бота (отдельно от worker)
