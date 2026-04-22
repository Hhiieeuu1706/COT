@echo off
setlocal

REM ==============================================================
REM Fetch DX.f price data from BlackBull MT5 and update price_cache.json
REM Requirements:
REM   - Python 3 accessible via PATH (python.exe)
REM   - MetaTrader5 library installed (pip install MetaTrader5)
REM   - BlackBull MT5 terminal installed and logged in
REM ==============================================================

set "SCRIPT_DIR=%~dp0"
set "FETCH_SCRIPT=%SCRIPT_DIR%fetch_dx_mt5.py"
set "PAUSE_AT_END=1"
set "PYTHON_EXE="

REM Prefer repo .venv python (same as COT\START.bat)
set "VENV_PY=%SCRIPT_DIR%..\..\.venv\Scripts\python.exe"
if exist "%VENV_PY%" set "PYTHON_EXE=%VENV_PY%"

REM Fallback to system python if venv missing
if "%PYTHON_EXE%"=="" set "PYTHON_EXE=python"

:parse_args
if "%~1"=="" goto args_done
if /i "%~1"=="--no-pause" (
    set "PAUSE_AT_END="
    shift
    goto parse_args
)
shift
goto parse_args

:args_done

if not exist "%FETCH_SCRIPT%" (
    echo [ERROR] Script not found: %FETCH_SCRIPT%
    goto end
)

echo [INFO] Fetching DX.f from BlackBull MT5...
"%PYTHON_EXE%" "%FETCH_SCRIPT%"

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Script failed with exit code %ERRORLEVEL%
) else (
    echo [SUCCESS] DX.f data fetched and cache updated
)

:end
if "%PAUSE_AT_END%"=="1" pause