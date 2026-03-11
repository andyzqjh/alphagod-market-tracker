@echo off
setlocal

echo ============================================
echo  HTML Market Dashboard
echo ============================================
echo.

set "PY_CMD="
where python >nul 2>&1
if not errorlevel 1 set "PY_CMD=python"

if not defined PY_CMD (
    where py >nul 2>&1
    if not errorlevel 1 set "PY_CMD=py -3"
)

if not defined PY_CMD (
    echo ERROR: Python was not found in PATH.
    echo Install Python and enable "Add Python to PATH", then try again.
    pause
    exit /b 1
)

echo Starting backend API on http://localhost:8000 ...
start "Market Dashboard - Backend" cmd /k "cd /d ""%~dp0backend"" && %PY_CMD% -m uvicorn main:app --reload --host 0.0.0.0 --port 8000"

echo Waiting for backend to start...
timeout /t 4 /nobreak >nul

echo Opening HTML dashboard...
start "" "%~dp0market-dashboard.html"

echo.
echo The dashboard file uses the backend API at http://localhost:8000
echo You can close this window after the browser opens.
echo.
