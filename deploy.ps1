# Auto-deploy script for MIRROR Bot
# Run: .\deploy.ps1

$ErrorActionPreference = "Stop"

$ServerIP = "108.61.201.108"
$SSHKey = "$env:USERPROFILE\.ssh\slots_b_ssh_key.pem"
$ProjectPath = "C:\Users\Tim\telegram_group_bridge"
$ArchiveName = "mirror-bot.tar.gz"

Write-Host "🚀 Starting deployment to $ServerIP..." -ForegroundColor Green

# Step 1: Check SSH key
Write-Host "🔑 Checking SSH key..." -ForegroundColor Yellow
if (-not (Test-Path $SSHKey)) {
    Write-Error "SSH key not found at $SSHKey"
    exit 1
}

# Step 2: Create archive (excluding __pycache__)
Write-Host "📦 Creating archive..." -ForegroundColor Yellow
Set-Location $ProjectPath

# Remove old archive if exists
if (Test-Path $ArchiveName) {
    Remove-Item $ArchiveName -Force
}

# Create tar archive using 7zip or tar (if available)
# First, let's try to use tar from Git or WSL, or fallback to manual copy
$itemsToArchive = @(
    "src",
    ".env.example",
    "docker-compose.yml",
    "Dockerfile",
    "main.py",
    "README.md",
    "requirements.txt",
    "telegram-bridge.service",
    "telegram-bridge-bot.service"
)

# Check for tar command
$tarCmd = Get-Command tar -ErrorAction SilentlyContinue
if ($tarCmd) {
    Write-Host "Using tar command..." -ForegroundColor Gray
    $items = $itemsToArchive -join " "
    Invoke-Expression "tar -czvf $ArchiveName --exclude='__pycache__' --exclude='*.pyc' $items"
} else {
    # Fallback: use Compress-Archive
    Write-Host "Using Compress-Archive (fallback)..." -ForegroundColor Gray
    Compress-Archive -Path $itemsToArchive -DestinationPath "mirror-bot.zip" -Force
    $ArchiveName = "mirror-bot.zip"
}

# Step 3: Upload to server via SCP
Write-Host "☁️  Uploading to server..." -ForegroundColor Yellow

# Check for scp
$scpCmd = Get-Command scp -ErrorAction SilentlyContinue
if (-not $scpCmd) {
    Write-Error "SCP not found. Please install OpenSSH or Git for Windows."
    exit 1
}

$scpArgs = @(
    "-i", $SSHKey,
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    $ArchiveName,
    "root@${ServerIP}:/root/"
)

& scp @scpArgs
if ($LASTEXITCODE -ne 0) {
    Write-Error "SCP failed with exit code $LASTEXITCODE"
    exit 1
}

# Step 4: Extract and setup on server
Write-Host "🔧 Setting up on server..." -ForegroundColor Yellow

$remoteCommands = @"
#!/bin/bash
set -e

echo "Creating directories..."
cd /root
mkdir -p mirror-bot data logs

echo "Extracting archive..."
if [[ "$ArchiveName" == *.tar.gz ]]; then
    tar -xzvf /root/$ArchiveName -C mirror-bot --strip-components=0 2>/dev/null || tar -xzvf /root/$ArchiveName -C mirror-bot
elif [[ "$ArchiveName" == *.zip ]]; then
    apt-get update && apt-get install -y unzip
    unzip -o /root/mirror-bot.zip -d mirror-bot
fi

echo "Setting permissions..."
chown -R root:root /root/mirror-bot
chmod -R 755 /root/mirror-bot

echo "Creating data and log directories..."
mkdir -p /root/mirror-bot/data /root/mirror-bot/logs
chmod 777 /root/mirror-bot/data /root/mirror-bot/logs

echo "Checking Docker..."
if ! command -v docker &> /dev/null; then
    echo "Installing Docker..."
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
fi

if ! command -v docker-compose &> /dev/null; then
    echo "Installing Docker Compose..."
    apt-get update
    apt-get install -y docker-compose-plugin || apt-get install -y docker-compose
fi

echo "Setup complete!"
echo ""
echo "Next steps:"
echo "1. SSH to server: ssh -i $SSHKey root@$ServerIP"
echo "2. Edit .env: cd /root/mirror-bot && nano .env"
echo "3. Run: docker-compose up -d"
echo ""

rm -f /root/$ArchiveName

echo "✅ Ready for configuration!"
"@

# Save remote commands to temp file
$tempScript = [System.IO.Path]::GetTempFileName() + ".sh"
$remoteCommands | Out-File -FilePath $tempScript -Encoding UTF8

# Upload and execute setup script
$scpSetupArgs = @(
    "-i", $SSHKey,
    "-o", "StrictHostKeyChecking=no",
    $tempScript,
    "root@${ServerIP}:/tmp/setup.sh"
)
& scp @scpSetupArgs

# Execute setup script
$sshArgs = @(
    "-i", $SSHKey,
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "root@${ServerIP}",
    "bash /tmp/setup.sh"
)
& ssh @sshArgs

if ($LASTEXITCODE -ne 0) {
    Write-Error "Remote setup failed with exit code $LASTEXITCODE"
    exit 1
}

# Cleanup
Remove-Item $tempScript -ErrorAction SilentlyContinue
if (Test-Path $ArchiveName) {
    Remove-Item $ArchiveName -Force
}

Write-Host ""
Write-Host "✅ Deployment complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. SSH to server: ssh -i $SSHKey root@$ServerIP" -ForegroundColor White
Write-Host "2. Configure .env file: cd /root/mirror-bot && nano .env" -ForegroundColor White
Write-Host "   (Add your BOT_TOKEN from @BotFather)" -ForegroundColor Gray
Write-Host "3. Start services: docker-compose up -d" -ForegroundColor White
Write-Host "4. Check logs: docker-compose logs -f" -ForegroundColor White
Write-Host ""
Write-Host "Bot will be available at: https://t.me/<YourBotUsername>" -ForegroundColor Yellow
