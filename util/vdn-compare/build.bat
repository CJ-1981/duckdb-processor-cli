@echo off
setlocal EnableDelayedExpansion

echo ============================================================
echo  Building vdn_compare.exe with PyInstaller
echo ============================================================

:: Check PyInstaller is available
where pyinstaller >nul 2>&1
if errorlevel 1 (
    echo [ERROR] PyInstaller not found. Installing...
    pip install pyinstaller
)

:: Clean previous build artifacts
if exist build rmdir /s /q build
if exist dist  rmdir /s /q dist
if exist vdn_compare.spec del /q vdn_compare.spec

echo.
echo [INFO] Running PyInstaller...
echo.

pyinstaller ^
    --onefile ^
    --console ^
    --name vdn_compare ^
    --hidden-import duckdb ^
    --hidden-import pandas ^
    --hidden-import openpyxl ^
    --hidden-import rich ^
    --hidden-import rich.console ^
    --hidden-import rich.table ^
    --hidden-import rich.box ^
    --hidden-import tqdm ^
    --hidden-import tkinter ^
    --hidden-import tabulate ^
    vdn_compare.py

if errorlevel 1 (
    echo.
    echo [ERROR] Build FAILED. Check the output above for details.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo  Build SUCCESS!
echo  Executable: dist\vdn_compare.exe
echo ============================================================
echo.

pause
