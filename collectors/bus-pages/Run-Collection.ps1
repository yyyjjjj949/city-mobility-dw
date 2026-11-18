param([int]$MaxPages = 100)
$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
$nodeCommand = Get-Command node.exe -ErrorAction Stop
$env:NODE_USE_ENV_PROXY = '1'
Push-Location -LiteralPath $PSScriptRoot
try {
    if (-not (Test-Path -LiteralPath (Join-Path $PSScriptRoot 'node_modules\cheerio'))) { throw 'Run npm ci --ignore-scripts in collectors\bus-pages first.' }
    & $nodeCommand.Source 'crawl.mjs' '--max-pages' $MaxPages
    if ($LASTEXITCODE -ne 0) { throw 'Business HTML collection did not pass; inspect its manifest.' }
    $chat = '2026-11-18-theme-definition-e721eb90'
    $outputRoot = Join-Path $projectRoot "outputs\$chat\business-page-crawl"
    $latest = Get-Content -LiteralPath (Join-Path $outputRoot 'latest.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    $runInput = Join-Path $projectRoot "work\$chat\business-page-crawl\$($latest.run_id)"
    $runOutput = Join-Path $outputRoot $latest.run_id
    $pythonCommand = 'D:\shujuwajue\python.exe'
    if (-not (Test-Path -LiteralPath $pythonCommand)) { $pythonCommand = (Get-Command python.exe -ErrorAction Stop).Source }
    $env:PYTHONIOENCODING = 'utf-8'
    & $pythonCommand (Join-Path $projectRoot 'tools\clean_bus_page_snapshot.py') '--input-run' $runInput '--output' $runOutput
    if ($LASTEXITCODE -ne 0) { throw 'Local snapshot cleaning failed.' }
    Write-Host ('Data preview: '+(Join-Path $runOutput 'index.html'))
} finally { Pop-Location }
