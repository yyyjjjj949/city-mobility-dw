@echo off
cd /d "%~dp0"
python tools\ingest_sources.py --month 2025-06 --discover-only
if errorlevel 1 (
  echo Discovery failed. Check outputs chat crawl receipts.
) else (
  echo Official source links discovered. No large data download was requested.
)
pause
