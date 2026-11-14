@echo off
cd /d "%~dp0"
"D:\shujuwajue\python.exe" tools\monitor_project.py
start "" "%~dp0outputs\运行状态.html"
pause
