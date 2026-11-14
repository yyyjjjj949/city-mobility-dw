$ErrorActionPreference = 'Stop'
$front = 'D:\CodexData\apps\DockerDesktop\frontend'
$backup = 'D:\CodexData\workspaces\city-mobility-dw-setup\outputs\docker-language-backup\4.97.0-original'
$patchRoot = 'D:\CodexData\workspaces\city-mobility-dw-setup\work\docker-chinese'
$docker = 'D:\CodexData\apps\DockerDesktop\resources\bin\docker.exe'
$env:DOCKER_CONFIG = 'D:\CodexData\app-data\docker-profile\cli'

# Refuse to overwrite an upgraded Docker Desktop installation.
$version = (Get-Item -LiteralPath 'D:\CodexData\apps\DockerDesktop\Docker Desktop.exe').VersionInfo.ProductVersion
if ($version -notmatch '^4\.97\.0(?:\D|$)') {
    throw ('This backup is only for Docker Desktop 4.97.0. Installed version: ' + $version)
}
foreach ($entry in (Get-Content -LiteralPath (Join-Path $backup 'original-checksums.json') -Raw | ConvertFrom-Json)) {
    $file = Join-Path $backup $entry.Path
    if ((Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash -ne $entry.SHA256) {
        throw ('Backup checksum mismatch: ' + $entry.Path)
    }
}

& $docker info --format '{{.ServerVersion}}' 2>$null | Out-Null
if ($LASTEXITCODE -eq 0) {
    & (Join-Path $PSScriptRoot 'Manage-Project.ps1') -Action Stop
}
& $docker desktop stop --timeout 45
if ($LASTEXITCODE -ne 0) { throw 'Could not stop Docker Desktop. Close Docker and retry.' }
if (Get-Process | Where-Object { $_.Path -like 'D:\CodexData\apps\DockerDesktop\*' }) {
    throw 'Docker processes are still running. Close Docker and retry.'
}

$unpacked = Join-Path $front 'resources\app.asar.unpacked'
if ((Resolve-Path -LiteralPath $unpacked).Path -ne 'D:\CodexData\apps\DockerDesktop\frontend\resources\app.asar.unpacked') {
    throw 'Unexpected resource directory.'
}
$archive = Join-Path $patchRoot ('before-restore-' + (Get-Date -Format 'yyyyMMdd-HHmmss-fff'))
New-Item -ItemType Directory -Path $archive | Out-Null
Move-Item -LiteralPath $unpacked -Destination (Join-Path $archive 'app.asar.unpacked')
Copy-Item -LiteralPath (Join-Path $backup 'app.asar.unpacked') -Destination $unpacked -Recurse
Copy-Item -LiteralPath (Join-Path $backup 'app.asar') -Destination (Join-Path $front 'resources\app.asar') -Force
Copy-Item -LiteralPath (Join-Path $backup 'Docker Desktop.exe') -Destination (Join-Path $front 'Docker Desktop.exe') -Force
foreach ($entry in (Get-Content -LiteralPath (Join-Path $backup 'original-checksums.json') -Raw | ConvertFrom-Json)) {
    $installed = if ($entry.Path -eq 'Docker Desktop.exe') { Join-Path $front $entry.Path } else { Join-Path $front ('resources\' + $entry.Path) }
    if ((Get-FileHash -LiteralPath $installed -Algorithm SHA256).Hash -ne $entry.SHA256) {
        throw ('Restore checksum mismatch: ' + $entry.Path)
    }
}
& (Join-Path $PSScriptRoot 'Start-Docker.ps1') -ShowDashboard
& (Join-Path $PSScriptRoot 'Manage-Project.ps1') -Action Start
Write-Output 'Original English interface restored. Docker data and project volumes were preserved.'
