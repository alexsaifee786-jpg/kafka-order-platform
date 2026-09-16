@echo off
if "%~1"=="" (
    echo Usage: scripts\test-failed-order ORDER_ID
    exit /b 1
)
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0test-failed-order.ps1" -OrderId "%~1"
