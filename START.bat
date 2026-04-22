@echo off
set PYTHON_EXE=e:\Trade folder\Trading_analyze\.venv\Scripts\python.exe
set APP_PATH=e:\Trade folder\Trading_analyze\COT\backend\app.py

powershell -NoProfile -ExecutionPolicy Bypass -Command "$conn = Get-NetTCPConnection -LocalPort 5056 -State Listen -ErrorAction SilentlyContinue; if ($conn) { $conn | Select-Object -ExpandProperty OwningProcess -Unique | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue } }" >nul 2>&1

REM Start BlackBull MT5
start "" "C:\Program Files\BlackBull Markets MT5\terminal64.exe"

REM Wait a bit for MT5 to start
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Sleep -Seconds 5" >nul 2>&1

REM Fetch DX.f data from MT5
call "%~dp0backend\fetch_dx_mt5.bat" --no-pause

start "COT Local Server" cmd /k ""%PYTHON_EXE%" "%APP_PATH%""
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Sleep -Seconds 3" >nul 2>&1
start "" http://127.0.0.1:5056
