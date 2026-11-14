param([ValidateSet('Start','Stop','Status')][string]$Action='Start')
$ErrorActionPreference='Stop'
$taskWork=Join-Path $PSScriptRoot 'work\orchestration'
New-Item -ItemType Directory -Path $taskWork -Force | Out-Null
if($Action -eq 'Start') {
    $workerStatePath=Join-Path $taskWork 'worker-state.json'
    if(Test-Path -LiteralPath $workerStatePath) {
        $taskState=Get-Content -LiteralPath $workerStatePath -Raw | ConvertFrom-Json
        $taskProcess=Get-Process -Id $taskState.pid -ErrorAction SilentlyContinue
        if($taskState.running -and $taskProcess -and ((Get-Date).ToUniversalTime()-[DateTime]::Parse($taskState.checked_at_utc).ToUniversalTime()).TotalSeconds -lt 30) {
            Write-Output 'Local Airflow project worker is already running.'
            return
        }
    }
    Start-Process -FilePath 'D:\shujuwajue\pythonw.exe' -ArgumentList @((Join-Path $PSScriptRoot 'tools\run_airflow_worker.py')) -WorkingDirectory $PSScriptRoot -WindowStyle Hidden -RedirectStandardOutput (Join-Path $taskWork 'worker-output.log') -RedirectStandardError (Join-Path $taskWork 'worker-error.log')
    Write-Output 'Local Airflow project worker started.'
} elseif($Action -eq 'Stop') {
    Set-Content -LiteralPath (Join-Path $taskWork 'stop.request') -Value 'stop'
    Write-Output 'Worker will exit after its current action finishes.'
} else {
    Get-Content -LiteralPath (Join-Path $taskWork 'worker-state.json') -ErrorAction SilentlyContinue
}
