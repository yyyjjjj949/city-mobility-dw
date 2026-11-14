$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$drawio = 'D:\CodexData\apps\drawio\draw.io.exe'
$diagram = Join-Path $root 'docs\diagrams\城市出行数仓设计.drawio'
$logRoot = Join-Path $root 'work\diagram-render'
New-Item -ItemType Directory -Force -Path $logRoot | Out-Null
$names = @('系统架构', '行程维度模型')
for ($index = 0; $index -lt $names.Count; $index++) {
    $page = $index + 1 # draw.io >=27.0.2 uses one-based page indexes.
    $output = Join-Path $root ('docs\diagrams\{0:D2}-{1}.png' -f $page, $names[$index])
    $process = Start-Process -FilePath $drawio -ArgumentList @(
        ('--user-data-dir=' + (Join-Path $logRoot ('export-profile-' + $page))),
        '--disable-gpu', '--export', '--format', 'png', '--page-index', [string]$page,
        '--scale', '2', '--output', $output, $diagram
    ) -PassThru -WindowStyle Hidden -RedirectStandardOutput (Join-Path $logRoot ('export-' + $page + '.out')) -RedirectStandardError (Join-Path $logRoot ('export-' + $page + '.err'))
    if (-not $process.WaitForExit(30000)) { throw ('Diagram export timed out: ' + $page) }
    $process.Refresh()
    if (($null -ne $process.ExitCode -and $process.ExitCode -ne 0) -or -not (Test-Path -LiteralPath $output)) { throw ('Diagram export failed: ' + $page) }
    Write-Output $output
}
