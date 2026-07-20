@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0send-to-kafka.ps1"
pause
