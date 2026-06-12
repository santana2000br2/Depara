# backup_windows.ps1 - Backup automático para Windows

param(
    [string]$ServerInstance = "localhost",
    [string]$Database = "DeXPara",
    [string]$BackupPath = "C:\Backups\De-x-Para"
)

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$backupFile = "$BackupPath\DeXPara_$timestamp.bak"

try {
    Write-Host "📦 Iniciando backup do banco de dados..." -ForegroundColor Yellow
    
    # Comando de backup usando sqlcmd
    $backupQuery = "BACKUP DATABASE [$Database] TO DISK = '$backupFile' WITH FORMAT, MEDIANAME = 'SQLServerBackups', NAME = 'Full Backup of $Database';"
    
    sqlcmd -S $ServerInstance -Q $backupQuery
    
    # Manter apenas os últimos 7 backups
    Get-ChildItem -Path $BackupPath -Filter "*.bak" | 
        Sort-Object LastWriteTime -Descending | 
        Select-Object -Skip 7 | 
        Remove-Item -Force
    
    Write-Host "✅ Backup criado com sucesso: $backupFile" -ForegroundColor Green
    
} catch {
    Write-Host "❌ Erro no backup: $_" -ForegroundColor Red
}