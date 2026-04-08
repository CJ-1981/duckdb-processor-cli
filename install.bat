@echo off
REM DuckDB Processor - Windows Installation Script
REM Sets up a fresh environment with all dependencies

setlocal enabledelayedexpansion

REM Script directory
set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"

REM Virtual environment directory
set VENV_DIR=venv

echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║    DuckDB Processor - Environment Setup for Windows        ║
echo ║                    (Fresh Installation)                    ║
echo ╚════════════════════════════════════════════════════════════╝
echo.

REM ========================================================
REM 1. Check for Python
REM ========================================================
echo [STEP 1/5] Checking Python installation...
python --version >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Python not found. Please install Python 3.10+ first.
    echo Visit: https://www.python.org/downloads/
    echo.
    echo Make sure to check "Add Python to PATH" during installation.
    pause
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo [OK] Found Python %PYTHON_VERSION%
echo.

REM ========================================================
REM 2. Check for existing venv and backup if needed
REM ========================================================
echo [STEP 2/5] Checking for existing virtual environment...
if exist "%VENV_DIR%" (
    echo [WARN] Virtual environment already exists at %VENV_DIR%
    echo [INFO] Removing existing virtual environment...
    rmdir /s /q "%VENV_DIR%"
    if %ERRORLEVEL% neq 0 (
        echo [ERROR] Failed to remove existing virtual environment
        pause
        exit /b 1
    )
    echo [OK] Old virtual environment removed
)
echo.

REM ========================================================
REM 3. Create virtual environment
REM ========================================================
echo [STEP 3/5] Creating virtual environment...
python -m venv "%VENV_DIR%"

if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to create virtual environment
    pause
    exit /b 1
)
echo [OK] Virtual environment created at %VENV_DIR%
echo.

REM ========================================================
REM 4. Activate virtual environment and upgrade pip
REM ========================================================
echo [STEP 4/5] Activating virtual environment and upgrading pip...
call "%VENV_DIR%\Scripts\activate.bat"

if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to activate virtual environment
    pause
    exit /b 1
)

python -m pip install --upgrade pip setuptools wheel

if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to upgrade pip
    pause
    exit /b 1
)
echo [OK] Pip upgraded successfully
echo.

REM ========================================================
REM 5. Install all project dependencies
REM ========================================================
echo [STEP 5/5] Installing project dependencies...
echo.
echo   [INFO] Installing base dependencies (duckdb, pandas, rich)...
pip install -e .

if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to install base dependencies
    pause
    exit /b 1
)

echo   [INFO] Installing optional UI dependencies (gradio, sqlparse, fpdf2, etc.)...
pip install -e ".[ui]"

if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to install UI dependencies
    pause
    exit /b 1
)

echo   [INFO] Installing optional export dependencies (openpyxl, pyarrow)...
pip install -e ".[export]"

if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to install export dependencies
    pause
    exit /b 1
)

echo   [INFO] Installing development dependencies (pytest)...
pip install -e ".[dev]"

if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to install dev dependencies
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
echo [INFO] Environment setup is complete. Here's what was done:
echo.
echo   1. Verified Python %PYTHON_VERSION% installation
echo   2. Created fresh virtual environment in: %VENV_DIR%
echo   3. Upgraded pip, setuptools, and wheel
echo   4. Installed base dependencies:
echo      - duckdb, pandas, rich
echo   5. Installed UI dependencies:
echo      - gradio, sqlparse, autopep8, fpdf2, tabulate
echo   6. Installed export dependencies:
echo      - openpyxl, pyarrow
echo   7. Installed dev dependencies:
echo      - pytest
echo.
echo [NEXT STEP] Run: run.bat
echo.
pause
