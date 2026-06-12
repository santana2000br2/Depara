# monitor_windows.ps1 - Monitoramento da aplicação

function Test-ApplicationHealth {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost/health" -UseBasicParsing -TimeoutSec 10
        return $response.StatusCode -eq 200
    } catch {
        return $false
    }
}

function Get-SystemMetrics {
    return @{
        CPU = (Get-Counter "\Processor(_Total)\% Processor Time").CounterSamples.CookedValue
        Memory = (Get-Counter "\Memory\Available MBytes").CounterSamples.CookedValue
        Disk = (Get-Counter "\LogicalDisk(C:)\% Free Space").CounterSamples.CookedValue
    }
}

# Endpoint de health check (adicionar ao app.py)
@app.route('/health')
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})