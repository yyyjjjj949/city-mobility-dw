@echo off
cd /d "%~dp0"
python tools\build_marts.py
if errorlevel 1 goto end
python tools\build_dashboards.py
if errorlevel 1 goto end
python tools\verify_dashboards.py
if errorlevel 1 goto end
python tools\export_marts_preview.py
:end
pause
