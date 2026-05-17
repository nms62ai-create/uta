@echo off
REM One-click launcher for UTA + heatmap-sdk (Windows).
REM
REM Double-click this file (or run it from cmd / PowerShell). On first
REM run it creates a virtualenv, extracts heatmap-sdk, applies the
REM integration patch, installs Python dependencies, and prompts for
REM Binance API credentials. Subsequent runs skip steps that are
REM already done and just launch the server.

setlocal

REM cd into repo root regardless of where the .bat was invoked from.
cd /d "%~dp0\.."

REM Pick the Python interpreter. ``py -3`` is the Windows launcher
REM that ships with python.org installers; ``python`` is what most
REM users have on PATH. We try ``py`` first since it auto-resolves
REM to the latest Python 3 even if multiple are installed.
where py >nul 2>nul
if %ERRORLEVEL%==0 (
    set "PY=py -3"
) else (
    where python >nul 2>nul
    if %ERRORLEVEL%==0 (
        set "PY=python"
    ) else (
        echo.
        echo [launcher] ERROR: Python 3.11+ not found on PATH.
        echo Install it from https://www.python.org/downloads/ (tick
        echo "Add python.exe to PATH" during install^), then run this
        echo file again.
        echo.
        pause
        exit /b 1
    )
)

%PY% launcher\_launcher.py %*
set EXITCODE=%ERRORLEVEL%

REM Keep the console window open when launched by double-click so
REM the user can read any error messages instead of seeing the
REM window vanish.
if %EXITCODE% NEQ 0 (
    echo.
    echo [launcher] exited with code %EXITCODE%
    pause
)

exit /b %EXITCODE%
