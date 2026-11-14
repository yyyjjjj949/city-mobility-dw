param([ValidateSet('Start', 'Stop', 'Status')][string]$Action = 'Start')
$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
$docker = 'D:\CodexData\apps\DockerDesktop\resources\bin\docker.exe'
$env:DOCKER_CONFIG = 'D:\CodexData\app-data\docker-profile\cli'
$env:PATH = 'D:\CodexData\apps\DockerDesktop\resources\bin;' + $env:PATH

if ($Action -eq 'Start') {
    $previousErrorPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    & $docker info --format '{{.ServerVersion}}' 2>$null | Out-Null
    $dockerReady = ($LASTEXITCODE -eq 0)
    $ErrorActionPreference = $previousErrorPreference
    if (-not $dockerReady) {
        & (Join-Path $root 'Start-Docker.ps1')
        $ready = $false
        for ($attempt = 0; $attempt -lt 40; $attempt++) {
            Start-Sleep -Seconds 3
            $ErrorActionPreference = 'Continue'
            & $docker info --format '{{.ServerVersion}}' 2>$null | Out-Null
            $dockerReady = ($LASTEXITCODE -eq 0)
            $ErrorActionPreference = 'Stop'
            if ($dockerReady) { $ready = $true; break }
        }
        if (-not $ready) { throw 'Docker Desktop did not become ready. Check its window and logs.' }
    }
    $lockPath = Join-Path $root 'outputs\image-installation.json'
    if (Test-Path -LiteralPath $lockPath) {
        foreach ($image in (Get-Content -LiteralPath $lockPath -Raw | ConvertFrom-Json)) {
            if ($image.image -like 'apache/kafka:*') { continue } # Retired from the offline project.
            if ($image.status -ne 'verified') { throw ('Unverified image: ' + $image.image) }
            $actual = & $docker image inspect $image.image --format '{{.Id}}'
            if ($LASTEXITCODE -ne 0 -or $actual.Trim() -ne $image.image_id) {
                throw ('Installed image differs from the verified version: ' + $image.image)
            }
        }
    }
    & $docker compose -f (Join-Path $root 'compose.yaml') up -d --wait --wait-timeout 300
    if ($LASTEXITCODE -ne 0) { throw 'Some project services did not start successfully.' }
    & (Join-Path $root 'Manage-Orchestration.ps1') -Action Start
    Write-Output 'Superset: http://localhost:8088'
    Write-Output 'Airflow:  http://localhost:18080'
    Write-Output 'Accounts: outputs\local-credentials.md'
} elseif ($Action -eq 'Stop') {
    $workerStatePath=Join-Path $root 'work\orchestration\worker-state.json'
    if(Test-Path -LiteralPath $workerStatePath) {
        & (Join-Path $root 'Manage-Orchestration.ps1') -Action Stop
        Write-Output 'Waiting for the local Airflow action to finish...'
        for($attempt=0;$attempt -lt 1500;$attempt++) {
            $workerState=Get-Content -LiteralPath $workerStatePath -Raw | ConvertFrom-Json
            if(-not $workerState.running) { break }
            Start-Sleep -Seconds 2
        }
        if($workerState.running) { throw 'Local Airflow worker is still stopping. Services remain running.' }
    }
    & $docker compose -f (Join-Path $root 'compose.yaml') stop
    if ($LASTEXITCODE -ne 0) { throw 'Could not stop project services.' }
} else {
    & $docker compose -f (Join-Path $root 'compose.yaml') ps
}
