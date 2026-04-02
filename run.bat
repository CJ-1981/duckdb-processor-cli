@echo off
REM DuckDB Processor Gradio App Launcher
REM One-click launcher for Windows with automatic venv setup

setlocal enabledelayedexpansion

REM Script directory
set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

REM Virtual environment directory
set VENV_DIR=venv

REM Python command
set PYTHON_CMD=python

echo ╔════════════════════════════════════════════════════════════╗
echo ║         DuckDB Processor Gradio App Launcher               ║
echo ╚════════════════════════════════════════════════════════════╝
echo.

REM Check if Python is installed
where %PYTHON_CMD% >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Python not found. Please install Python 3.10+ first.
    echo Visit: https://www.python.org/downloads/
    pause
    exit /b 1
)

for /f "tokens=2" %%i in ('%PYTHON_CMD% --version 2^>^&1') do set PYTHON_VERSION=%%i
echo [OK] Found Python %PYTHON_VERSION%

REM Create virtual environment if it doesn't exist
if not exist "%VENV_DIR%" (
    echo [INFO] Creating virtual environment...
    %PYTHON_CMD% -m venv %VENV_DIR%

    if %ERRORLEVEL% neq 0 (
        echo [ERROR] Failed to create virtual environment
        pause
        exit /b 1
    )
    echo [OK] Virtual environment created
)

REM Activate virtual environment
echo [INFO] Activating virtual environment...
call "%VENV_DIR%\Scripts\activate.bat"

REM Upgrade pip
echo [INFO] Upgrading pip...
python -m pip install --quiet --upgrade pip

REM Install/update dependencies
echo [INFO] Checking dependencies...

REM Install base dependencies
echo   [INFO] Installing base dependencies...
pip install --quiet -e .

if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to install base dependencies
    pause
    exit /b 1
)

REM Install UI dependencies
echo   [INFO] Installing UI dependencies (Gradio, etc.)...
pip install --quiet -e ".[ui]"

if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to install UI dependencies
    pause
    exit /b 1
)

echo [OK] Dependencies installed

REM Check if gradio_app.py exists
if not exist "gradio_app.py" (
    echo [ERROR] gradio_app.py not found in current directory
    pause
    exit /b 1
)

REM Launch Gradio app
echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║  Starting DuckDB Processor Gradio Interface...             ║
echo ╚════════════════════════════════════════════════════════════╝
echo.

REM Run the Gradio app
%PYTHON_CMD% gradio_app.py

REM Handle exit
set exit_code=%ERRORLEVEL%
if %exit_code% neq 0 (
    echo.
    echo [ERROR] Gradio app exited with error code %exit_code%
    pause
    exit /b %exit_code%
)

pause
