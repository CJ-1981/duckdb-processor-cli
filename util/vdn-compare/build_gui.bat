@echo off
set EXE_NAME=vdn_compare_gui.exe
echo Building VDN Compare GUI + CLI executable...

:: Try to kill any running instances that might lock files
taskkill /f /im %EXE_NAME% >nul 2>&1

echo Cleaning up old build artifacts...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist

:: If cleanup failed (likely OneDrive or Explorer lock), notify user
if exist build (
    echo [WARNING] Could not remove 'build' directory. 
    echo This usually happens because OneDrive or File Explorer is locking the folder.
    echo Please close any open folders and pause OneDrive sync, then try again.
    pause
    exit /b 1
)

echo Packaging pandas, duckdb, rich, and other libraries...
echo.

python -m PyInstaller --clean vdn_compare_gui.spec


echo.
echo Build complete! You can find vdn_compare_gui.exe in the 'dist' folder.
pause
