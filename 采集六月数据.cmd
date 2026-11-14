@echo off
cd /d "%~dp0"
python tools\ingest_sources.py --month 2025-06
if errorlevel 1 (
  echo Ingestion failed. Check work\ingestion\runs.
) else (
  echo Ingestion finished. See outputs\ods-ingestion-latest.json.
)
pause
