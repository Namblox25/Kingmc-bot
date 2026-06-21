@echo off
title Mine Bot Manager - Antares
chcp 65001 >nul
color 0B

echo ========================================
echo   MINE BOT MANAGER - Antares
echo ========================================
echo.

:: Kiểm tra Node.js
node --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [LOI] Chua cai dat Node.js!
    pause
    exit /b 1
)

echo [OK] Dang khoi dong bot...
echo.

:: Chạy file main.js
node main.js

:: Nếu bot crash, hiện thông báo và pause lại
echo.
echo ========================================
echo   Bot da dung!
echo ========================================
pause