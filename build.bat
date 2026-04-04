@echo off
setlocal enabledelayedexpansion

echo ========================================================
echo   DuckDB Processor - Windows Build Script
echo ========================================================

:: Check for Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python not found. Please install Python and add it to your PATH.
    echo Get it from https://www.python.org/downloads/
    pause
    exit /b 1
)

:: Virtual Environment Setup (Recommended)
if not exist "venv" (
    echo [INFO] Creating virtual environment...
    python -m venv venv
)

echo [INFO] Activating virtual environment...
call venv\Scripts\activate

:: Install/Upgrade Dependencies
echo [INFO] Installing/Upgrading dependencies...
python -m pip install --upgrade pip
python -m pip install .[ui,export] pyinstaller

:: Build CLI Executable
echo [INFO] Building CLI executable: duckdb-processor-cli.exe...
pyinstaller --onefile ^
            --console ^
            --name duckdb-processor-cli ^
            --clean ^
            main.py

:: Build GUI Executable
:: Note: --collect-all gradio ensures all assets are included
:: Note: --windowed hides the console window for a better GUI experience
echo [INFO] Building GUI executable: duckdb-processor-gui.exe...
pyinstaller --onefile ^
            --windowed ^
            --name duckdb-processor-gui ^
            --collect-all gradio ^
            --add-data "duckdb_processor/analysts;duckdb_processor/analysts" ^
            --add-data "sql_patterns.json;." ^
            --add-data "report_templates.json;." ^
            --clean ^
            gradio_app.py

echo ========================================================
echo   Build Complete!
echo   Check the 'dist' folder for:
echo     - duckdb-processor-cli.exe
echo     - duckdb-processor-gui.exe
echo ========================================================
pause
