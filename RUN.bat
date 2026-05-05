@echo off
REM ============================================================================
REM  Terminal launcher (Windows)
REM  Created by Oscar Zarraga Perez. Copyright (c) 2026 Oscar Zarraga Perez.
REM  Released under the MIT License.
REM ============================================================================
cd /d "%~dp0"
title Terminal
echo.
echo ==========================================================
echo   Terminal  -  starting local server
echo   Created by Oscar Zarraga Perez
echo   Copyright (c) 2026 Oscar Zarraga Perez  -  MIT License
echo ==========================================================
echo.
echo A browser window will open automatically in a few seconds.
echo Leave this window open. Close it to stop the server.
echo.
python server.py
echo.
echo Server exited. Any error is shown above.
pause
