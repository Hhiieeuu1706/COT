@echo off
setlocal
cd /d "%~dp0"
set "COT_PYTHON=%~dp0.venv\Scripts\python.exe"
if not exist "%COT_PYTHON%" (
    where py >nul 2>&1
    if errorlevel 1 goto missing_python
    py -3.12 -m venv .venv
    if errorlevel 1 goto failed
)
"%COT_PYTHON%" -c "import flask, flask_cors, pandas, requests, yfinance" >nul 2>&1
if errorlevel 1 (
    "%COT_PYTHON%" -m pip install -r requirements.txt
    if errorlevel 1 goto failed
)

powershell -NoProfile -Command "try { if ((Invoke-RestMethod 'http://127.0.0.1:5056/api/health' -TimeoutSec 1).app -eq 'cot') { exit 0 } } catch {}; exit 1"
if errorlevel 1 start "COT Local Server" /min "%COT_PYTHON%" "%~dp0backend\app.py"

powershell -NoProfile -Command "$cotDeadline = (Get-Date).AddSeconds(12); do { try { if ((Invoke-RestMethod 'http://127.0.0.1:5056/api/health' -TimeoutSec 1).app -eq 'cot') { exit 0 } } catch {}; Start-Sleep -Milliseconds 200 } while ((Get-Date) -lt $cotDeadline); exit 1"
if errorlevel 1 goto failed
start "" "http://127.0.0.1:5056"
exit /b 0

:missing_python
echo Install 64-bit Python 3.12 with the Python launcher, then run START.bat again.
pause
exit /b 1

:failed
echo COT could not start. Check the COT server window, data\backend.log and port 5056.
pause
exit /b 1
