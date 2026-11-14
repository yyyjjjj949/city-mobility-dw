$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$dockerExe = 'D:\CodexData\apps\DockerDesktop\resources\bin\docker.exe'
$env:DOCKER_CONFIG = 'D:\CodexData\app-data\docker-profile\cli'
$dwdWork = Join-Path $projectRoot 'work\dwd'
New-Item -ItemType Directory -Path $dwdWork -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $projectRoot 'config\dwd-requirements.txt') -Destination (Join-Path $dwdWork 'requirements.txt') -Force
$checkCode = "import sys;sys.path.insert(0,'/workspace/work/dwd/python-deps-np126');import numpy,shapely,pyproj,shapefile;v=(numpy.__version__,shapely.__version__,pyproj.__version__,shapefile.__version__);assert v==('1.26.4','2.1.2','3.7.1','2.3.1'),v;print(v)"
& $dockerExe compose -f (Join-Path $projectRoot 'compose.yaml') exec -T spark python3 -c $checkCode
if ($LASTEXITCODE -eq 0) {
    Write-Output 'DWD spatial dependencies are already installed and verified.'
    exit 0
}
& $dockerExe compose -f (Join-Path $projectRoot 'compose.yaml') exec -T spark python3 -m pip install --disable-pip-version-check --only-binary=:all: --upgrade --cache-dir /workspace/work/dwd/pip-cache --target /workspace/work/dwd/python-deps-np126 -r /workspace/work/dwd/requirements.txt
if ($LASTEXITCODE -ne 0) { throw 'DWD dependency installation failed.' }
& $dockerExe compose -f (Join-Path $projectRoot 'compose.yaml') exec -T spark python3 -c $checkCode
if ($LASTEXITCODE -ne 0) { throw 'DWD dependency import verification failed.' }
