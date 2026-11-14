@echo off
cd /d "%~dp0"
python tools\build_dwd.py --month 2025-06
pause
