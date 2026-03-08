# Telegram Mirror Bot — Deployment Guide

## System Requirements

- Ubuntu 20.04+ / Debian 11+ / Any Linux
- Docker & Docker Compose (recommended) OR Python 3.11+

---

## Method 1: Docker Compose (Recommended)

### Step 1: Install Docker

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.23.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Add user to docker group
sudo usermod -aG docker $USER
newgrp docker
```

### Step 2: Create Project Directory

```bash
mkdir -p ~/telegram-mirror-bot
cd ~/telegram-mirror-bot
```

### Step 3: Create Files

**docker-compose.yml:**
```yaml
version: '3.8'

services:
  bridge-worker:
    build: .
    container_name: telegram-bridge-worker
    command: python main.py
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    restart: unless-stopped
    environment:
      - PYTHONUNBUFFERED=1

  bot:
    build: .
    container_name: telegram-bridge-bot
    command: python -m src.bot
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    restart: unless-stopped
    environment:
      - PYTHONUNBUFFERED=1
```

**Dockerfile:**
```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Create directories
RUN mkdir -p data logs

CMD ["python", "main.py"]
```

**requirements.txt:**
```
aiogram==3.4.1
python-dotenv==1.0.0
loguru==0.7.2
telethon==1.33.1
```

**.env:**
```env
BOT_TOKEN=your_bot_token_here
```

### Step 4: Create Source Directory

```bash
mkdir -p src
```

Create `src/__init__.py` (empty):
```bash
touch src/__init__.py
```

**src/database.py:**
[Include the full database.py content]

**src/channel_manager.py:**
[Include the full channel_manager.py content]

**src/bot.py:**
[Include the full bot.py content]

**src/parser.py:**
```python
"""Message parser for keyword filtering."""

import re


def contains_keywords(text: str, keywords: list[str]) -> bool:
    """Check if text contains any of the keywords."""
    if not keywords:
        return True
    if not text:
        return False
    
    text_lower = text.lower()
    return any(keyword.lower() in text_lower for keyword in keywords)


def process_message(text: str) -> str:
    """Process message text (placeholder for future enhancements)."""
    return text
```

**main.py:**
[Include the full main.py content]

### Step 5: Start Services

```bash
# Build and start
docker-compose up -d

# Check logs
docker-compose logs -f bot
docker-compose logs -f bridge-worker

# Stop
docker-compose down

# Restart
docker-compose restart
```

---

## Method 2: Native Python

### Step 1: Install Python & Dependencies

```bash
# Install Python 3.12
sudo apt update
sudo apt install -y python3.12 python3.12-venv python3.12-pip

# Create project directory
mkdir -p ~/telegram-mirror-bot
cd ~/telegram-mirror-bot

# Create virtual environment
python3.12 -m venv venv
source venv/bin/activate

# Install dependencies
pip install aiogram==3.4.1 python-dotenv==1.0.0 loguru==0.7.2 telethon==1.33.1
```

### Step 2: Create Project Structure

Same files as Method 1 (docker-compose.yml not needed).

### Step 3: Run with Systemd

**Create bot service:**
```bash
sudo tee /etc/systemd/system/telegram-bridge-bot.service > /dev/null <<EOF
[Unit]
Description=Telegram Bridge Bot
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=/home/$USER/telegram-mirror-bot
Environment=PYTHONUNBUFFERED=1
ExecStart=/home/$USER/telegram-mirror-bot/venv/bin/python -m src.bot
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
```

**Create worker service:**
```bash
sudo tee /etc/systemd/system/telegram-bridge-worker.service > /dev/null <<EOF
[Unit]
Description=Telegram Bridge Worker
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=/home/$USER/telegram-mirror-bot
Environment=PYTHONUNBUFFERED=1
ExecStart=/home/$USER/telegram-mirror-bot/venv/bin/python main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
```

**Start services:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable telegram-bridge-bot telegram-bridge-worker
sudo systemctl start telegram-bridge-bot telegram-bridge-worker

# Check status
sudo systemctl status telegram-bridge-bot
sudo systemctl status telegram-bridge-worker

# View logs
sudo journalctl -u telegram-bridge-bot -f
sudo journalctl -u telegram-bridge-worker -f
```

---

## Configuration

### Get BOT_TOKEN

1. Go to https://t.me/BotFather
2. Create new bot: `/newbot`
3. Copy token to `.env` file

### Get API_ID and API_HASH (for accounts)

1. Go to https://my.telegram.org
2. Login with phone number
3. Go to "API development tools"
4. Create application
5. Note `api_id` and `api_hash`

---

## Usage

1. **Start bot:** Send `/start` to your bot
2. **Add account:** Click "➕ Добавить аккаунт" → choose QR-code
3. **Scan QR:** In Telegram go to Settings → Devices → Scan QR
4. **Create mirror:** Click "➕ Создать зеркало" → select type → enter source
5. **Done!** Bot will auto-create mirror channel and start copying

---

## Troubleshooting

**Bot not responding:**
```bash
docker-compose logs bot
# OR
sudo journalctl -u telegram-bridge-bot -n 50
```

**Worker not connecting:**
```bash
docker-compose logs bridge-worker
# OR
sudo journalctl -u telegram-bridge-worker -n 50
```

**Database issues:**
```bash
# Reset database (WARNING: deletes all data)
rm -f data/bridge.db
```

**Permission denied:**
```bash
sudo chown -R $USER:$USER ~/telegram-mirror-bot
chmod 600 .env
```

---

## File Structure

```
telegram-mirror-bot/
├── .env                      # BOT_TOKEN
├── docker-compose.yml        # Docker config
├── Dockerfile               # Build config
├── requirements.txt         # Python deps
├── main.py                 # Worker (mirroring)
├── README.md               # User guide
├── data/                   # Database (created auto)
│   └── bridge.db
├── logs/                   # Logs (created auto)
└── src/
    ├── __init__.py
    ├── bot.py             # Telegram bot
    ├── channel_manager.py # Channel/chat creation
    ├── database.py        # SQLite storage
    └── parser.py          # Message filtering
```

---

## Security Notes

- Keep `.env` secret (contains BOT_TOKEN)
- Database `data/bridge.db` contains session strings — backup securely
- Never commit `.env` or `data/` to git (use `.gitignore`)
- Use firewall: `ufw allow 22/tcp && ufw enable`

---

## Quick Commands Reference

```bash
# Docker
start() { docker-compose up -d; }
stop() { docker-compose down; }
logs() { docker-compose logs -f; }
restart() { docker-compose restart; }

# Systemd
start() { sudo systemctl start telegram-bridge-bot telegram-bridge-worker; }
stop() { sudo systemctl stop telegram-bridge-bot telegram-bridge-worker; }
logs() { sudo journalctl -u telegram-bridge-bot -f; }
status() { sudo systemctl status telegram-bridge-bot telegram-bridge-worker; }
```
