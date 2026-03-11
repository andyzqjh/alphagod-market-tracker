@echo off
echo ============================================
echo  Stock Dashboard - First Time Setup
echo ============================================
echo.

REM Check Python
python --version >/dev/null 2>&1
if errorlevel 1 (
    echo ERROR: Python not found.
    echo.
    echo Please install Python from: https://python.org
    echo IMPORTANT: Check the box "Add Python to PATH" during install!
    echo Then close this window and run setup.bat again.
    pause
    exit /b 1
)
echo [OK] Python found.

REM Check Node / npm
npm --version >/dev/null 2>&1
if errorlevel 1 (
    echo.
    echo ERROR: Node.js / npm not found.
    echo.
    echo Please install Node.js from: https://nodejs.org
    echo Download the LTS version and run the installer.
    echo After installing, CLOSE this window and run setup.bat again.
    pause
    exit /b 1
)
echo [OK] Node.js found.

echo.
echo [1/3] Installing Python backend dependencies...
cd /d "%~dp0backend"
pip install -r requirements.txt
if errorlevel 1 ( echo ERROR installing Python packages & pause & exit /b 1 )

echo.
echo [2/3] Installing frontend dependencies...
cd /d "%~dp0frontend"
call npm install
if errorlevel 1 ( echo ERROR installing npm packages & pause & exit /b 1 )

echo.
echo [3/3] Creating .env file...
cd /d "%~dp0backend"
if not exist ".env" (
    copy ".env.example" ".env"
    echo Created backend\.env
) else (
    echo backend\.env already exists.
)

echo.
echo ============================================
echo  Setup complete!
echo.
echo  NEXT STEP: Open backend\.env in Notepad
echo  and replace "your_api_key_here" with your
echo  Anthropic API key from:
echo  https://console.anthropic.com
echo.
echo  Then double-click start.bat to launch!
echo ============================================
pause
