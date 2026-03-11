@echo off
echo ============================================
echo  Stock Dashboard
echo ============================================
echo.

python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found.
    echo Please install Python from https://python.org
    echo Check "Add Python to PATH" during install!
    pause & exit /b 1
)

if not exist "%~dp0backend\.env" (
    echo ERROR: backend\.env not found.
    echo.
    echo Step 1: Run setup.bat first
    echo Step 2: Edit backend\.env and add your Anthropic API key
    echo         Get one free at: https://console.anthropic.com
    pause & exit /b 1
)

echo Installing/checking Python packages...
pip install fastapi uvicorn yfinance pandas anthropic python-dotenv requests aiofiles -q

echo.
echo Starting dashboard...
echo.
echo >>> Opening http://localhost:8000 in your browser
echo >>> Press Ctrl+C in this window to stop
echo.

timeout /t 2 /nobreak >nul
start "" "http://localhost:8000"

cd /d "%~dp0backend"
python -m uvicorn main:app --host 0.0.0.0 --port 8000
