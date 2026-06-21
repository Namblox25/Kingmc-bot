@echo off
title Cai dat thu vien cho Mine Bot Manager
chcp 65001 >nul
color 0B

echo ========================================
echo   CÀI ĐẶT THƯ VIỆN - Antares
echo ========================================
echo.

node --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [LOI] Chua cai dat Node.js!
    pause
    exit /b 1
)

echo [OK] Dang cai dat thu vien...
echo.

call npm install

echo.
echo ========================================
echo   HOAN TAT!
echo ========================================
pause