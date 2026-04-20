@echo off
setlocal enabledelayedexpansion

echo ============================================================
echo   VDN COMPARE - DIAGNOSTIC SUITE
echo ============================================================

echo.
echo [1/5] Running Scratch Test 1 (Basic DuckDB JSON Parsing)...
python scratch_test.py
if !errorlevel! neq 0 echo [FAILED] scratch_test.py failed & pause

echo.
echo [2/5] Running Scratch Test 2 (DuckDB Type Mapping)...
python scratch_test2.py
if !errorlevel! neq 0 echo [FAILED] scratch_test2.py failed & pause

echo.
echo [3/5] Running JSON Diff logic validation...
python json_diff_test.py
if !errorlevel! neq 0 echo [FAILED] json_diff_test.py failed & pause

echo.
echo [4/5] Running Data Extraction check (requires DB.csv/PIE.csv)...
python extract_test.py
if !errorlevel! neq 0 echo [WARNING] extract_test.py failed (likely missing files)

echo.
echo [5/5] Running Main Application (Dry run with --use-default-input)...
python vdn_compare.py --use-default-input
if !errorlevel! neq 0 echo [FAILED] vdn_compare.py failed & pause

echo.
echo ============================================================
echo   ALL DIAGNOSTICS COMPLETED
echo ============================================================
pause
