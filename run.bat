@echo off
REM DuckDB Processor Gradio App Launcher
REM Optimized for Windows/OneDrive environments

setlocal enabledelayedexpansion

REM Script directory - properly quoted
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

REM Virtual environment directory
set "VENV_DIR=%SCRIPT_DIR%venv"
set "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"

echo ╔════════════════════════════════════════════════════════════╗
echo ║         DuckDB Processor Gradio App Launcher               ║
echo ╚════════════════════════════════════════════════════════════╝
echo.

REM Check if Python is installed and check version
set "PYTHON_CMD=python"
where %PYTHON_CMD% >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Python not found. Please install Python 3.10+.
    pause
    exit /b 1
)

REM Verify Python >= 3.10
for /f "tokens=2" %%i in ('%PYTHON_CMD% -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"') do set "PY_VER=%%i"
for /f "tokens=1,2 delims=." %%a in ("%PY_VER%") do (
    if %%a LSS 3 (
        echo [ERROR] Python 3.10+ required. Found %PY_VER%
        pause
        exit /b 1
    )
    if %%a EQU 3 if %%b LSS 10 (
        echo [ERROR] Python 3.10+ required. Found %PY_VER%
        pause
        exit /b 1
    )
)
echo [OK] Found Python %PY_VER%

REM Create virtual environment if it doesn't exist
if not exist "%VENV_DIR%" (
    echo [INFO] Creating virtual environment...
    %PYTHON_CMD% -m venv "%VENV_DIR%"
    if %ERRORLEVEL% neq 0 (
        echo [ERROR] Failed to create virtual environment
        pause
        exit /b 1
    )
)

REM Ensure pip is up to date and install all dependencies in one pass
echo [INFO] Updating dependencies...
"%PYTHON_EXE%" -m pip install --quiet --upgrade pip setuptools wheel
"%PYTHON_EXE%" -m pip install --quiet -e ".[ui,export]"
"%PYTHON_EXE%" -m pip install --quiet mistletoe itables

if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to install dependencies
    pause
    exit /b 1
)

REM Launch Gradio app using the virtual environment's interpreter
echo.
echo [INFO] Launching Gradio...
"%PYTHON_EXE%" gradio_app.py

set exit_code=%ERRORLEVEL%
if %exit_code% neq 0 (
    echo [ERROR] Gradio app exited with error code %exit_code%
    pause
    exit /b %exit_code%
)

pause
