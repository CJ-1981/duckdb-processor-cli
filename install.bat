@echo off
REM DuckDB Processor - Windows Installation Script
REM Sets up a fresh environment with all dependencies

setlocal enabledelayedexpansion

REM Script directory - properly quoted
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

REM Virtual environment directory and python executable
set "VENV_DIR=%SCRIPT_DIR%venv"
set "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"

echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║    DuckDB Processor - Environment Setup for Windows        ║
echo ║                    (Fresh Installation)                    ║
echo ╚════════════════════════════════════════════════════════════╝
echo.

REM ========================================================
REM 1. Check for Python
REM ========================================================
echo [STEP 1/3] Checking Python installation...
python --version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Python not found. Please install Python 3.10+.
    pause
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version 2^>^&1') do set "PYTHON_VERSION=%%i"
echo [OK] Found Python %PYTHON_VERSION%
echo.

REM ========================================================
REM 2. Create virtual environment
REM ========================================================
echo [STEP 2/3] Preparing virtual environment...
if exist "%VENV_DIR%" (
    echo [INFO] Removing existing virtual environment...
    rmdir /s /q "%VENV_DIR%"
)

python -m venv "%VENV_DIR%"
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to create virtual environment
    pause
    exit /b 1
)
echo [OK] Virtual environment created at %VENV_DIR%
echo.

REM ========================================================
REM 3. Install all project dependencies
REM ========================================================
echo [STEP 3/3] Installing dependencies...
"%PYTHON_EXE%" -m pip install --quiet --upgrade pip setuptools wheel
"%PYTHON_EXE%" -m pip install --quiet -e ".[ui,export,dev]"

if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to install dependencies
    pause
    exit /b 1
)
echo [OK] All dependencies installed successfully
echo.

REM ========================================================
REM Summary
REM ========================================================
echo ╔════════════════════════════════════════════════════════════╗
echo ║            Installation Complete!                          ║
echo ╚════════════════════════════════════════════════════════════╝
echo.
echo [INFO] Environment setup is complete.
echo [NEXT STEP] Run: run.bat
echo.
pause
