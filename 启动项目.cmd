@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0Manage-Project.ps1" -Action Start
pause
