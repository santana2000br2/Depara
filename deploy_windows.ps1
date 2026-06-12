# deploy_windows.ps1 - Script de deploy para Windows Server 2022

Write-Host "🚀 Iniciando deploy do De x Para..." -ForegroundColor Green

# Parar o site no IIS se existir
if (Get-Website -Name "De-x-Para" -ErrorAction SilentlyContinue) {
    Write-Host "⏹️ Parando site existente..." -ForegroundColor Yellow
    Stop-Website -Name "De-x-Para"
}

# Backup do banco de dados
Write-Host "📦 Fazendo backup do banco de dados..." -ForegroundColor Yellow
$backupPath = "C:\Backups\De-x-Para"
if (!(Test-Path $backupPath)) {
    New-Item -ItemType Directory -Path $backupPath -Force
}
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$backupFile = "$backupPath\backup_$timestamp.bak"

# Comando de backup do SQL Server (ajuste para seu ambiente)
# Backup-SqlDatabase -ServerInstance "seu-servidor" -Database "seu-banco" -BackupFile $backupFile

# Atualizar código da aplicação
Write-Host "📥 Atualizando código..." -ForegroundColor Yellow
# git pull origin main  # Se usando Git
# Ou copiar arquivos manualmente

# Recriar ambiente virtual
Write-Host "🔧 Configurando ambiente Python..." -ForegroundColor Yellow
if (Test-Path "venv") {
    Remove-Item -Recurse -Force "venv"
}
python -m venv venv

# Instalar dependências
.\venv\Scripts\Activate
pip install --upgrade pip
pip install -r requirements.txt

# Criar pasta de logs
$logPath = ".\logs"
if (!(Test-Path $logPath)) {
    New-Item -ItemType Directory -Path $logPath -Force
}

# Configurar site no IIS
Write-Host "🌐 Configurando IIS..." -ForegroundColor Yellow
$siteName = "De-x-Para"
$physicalPath = (Get-Location).Path

# Criar site se não existir
if (!(Get-Website -Name $siteName -ErrorAction SilentlyContinue)) {
    New-Website -Name $siteName -PhysicalPath $physicalPath -Port 80
    Write-Host "✅ Site criado: $siteName" -ForegroundColor Green
}

# Iniciar site
Start-Website -Name $siteName

Write-Host "✅ Deploy concluído com sucesso!" -ForegroundColor Green
Write-Host "🌐 Acesse: http://localhost" -ForegroundColor Cyan
Write-Host "📊 Logs disponíveis em: $logPath" -ForegroundColor Cyan