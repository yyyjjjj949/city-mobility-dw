param([switch]$ShowDashboard)
$ErrorActionPreference = 'Stop'
$dockerRoot = 'D:\CodexData\apps\DockerDesktop'
$profileRoot = 'D:\CodexData\app-data\docker-profile'
$startInfo = [Diagnostics.ProcessStartInfo]::new()
$startInfo.FileName = Join-Path $dockerRoot 'Docker Desktop.exe'
$startInfo.Arguments = if ($ShowDashboard) { '' } else { '--background' }
$startInfo.WorkingDirectory = $dockerRoot
$startInfo.UseShellExecute = $false
$startInfo.CreateNoWindow = $true
$startInfo.Environment['APPDATA'] = Join-Path $profileRoot 'Roaming'
$startInfo.Environment['LOCALAPPDATA'] = Join-Path $profileRoot 'Local'
$startInfo.Environment['DOCKER_CONFIG'] = Join-Path $profileRoot 'cli'
[Diagnostics.Process]::Start($startInfo) | Out-Null
Write-Output 'Docker Desktop is starting with its configuration and runtime storage on D:.'
