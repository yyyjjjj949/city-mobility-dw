@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0collectors\bus-pages\Run-Collection.ps1"
if errorlevel 1 pause
