@echo off
echo ============================================
echo  Stock Dashboard - Starting...
echo ============================================
echo.

REM Check .env exists
if not exist "%~dp0backend\.env" (
    echo ERROR: backend\.env not found.
    echo Please run setup.bat first, then edit backend\.env with your API key.
    pause
    exit /b 1
)

echo Starting backend API server...
start "Stock Dashboard - Backend" cmd /k "cd /d "%~dp0backend" && python -m uvicorn main:app --reload --port 8000"

echo Waiting for backend to start...
timeout /t 3 /nobreak >/dev/null

echo Starting frontend...
start "Stock Dashboard - Frontend" cmd /k "cd /d "%~dp0frontend" && npm run dev"

echo.
echo ============================================
echo  Both servers are starting up!
echo.
echo  Open your browser and go to:
echo  http://localhost:5173
echo.
echo  Close this window when done.
echo ============================================
timeout /t 5 /nobreak >/dev/null
start "" "http://localhost:5173"
