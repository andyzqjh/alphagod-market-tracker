# Stock Dashboard

A personal stock dashboard with pre-market screener, sector theme board, earnings tracker, and AI-powered analysis.

## First Time Setup

### Step 1: Install Python
Download from https://python.org (check "Add Python to PATH")

### Step 2: Install Node.js
Download from https://nodejs.org (LTS version)

### Step 3: Get a Claude API Key
1. Go to https://console.anthropic.com
2. Sign up / log in
3. Click "API Keys" > "Create Key"
4. Copy your key

### Step 4: Run setup
Double-click `setup.bat`

### Step 5: Add your API key
Open `backend\.env` and replace the placeholder value with your current key.

### Step 6: Launch locally
Double-click `start.bat` or `open-html-dashboard.bat`

---

## Free Website Deployment

The easiest free option is Render using its built-in `onrender.com` URL.

### What you get
- A free public website URL
- HTTPS included
- The dashboard auto-refreshes every 5 minutes in the browser
- No custom domain purchase needed

### Important limitation
Render free web services sleep after inactivity, so the first load after being idle may take a little longer.

### Free deploy flow
1. Put this folder in a GitHub repository.
2. Create a free Render account using GitHub.
3. In Render, create a new Blueprint or Web Service from the repo.
4. Render will read [render.yaml](/Users/AndyQuek/OneDrive%20-%20Unique%20Point%20Management%20Pte%20Ltd/Desktop/ta%20folder/stock-dashboard/render.yaml).
5. In Render environment variables, set `ANTHROPIC_API_KEY` to your current rotated key.
6. Deploy.

The public URL will be an `onrender.com` address based on the service name, typically something close to `alphagod-market-tracker.onrender.com`.

### Docker hosting files
- [Dockerfile](/Users/AndyQuek/OneDrive%20-%20Unique%20Point%20Management%20Pte%20Ltd/Desktop/ta%20folder/stock-dashboard/Dockerfile)
- [render.yaml](/Users/AndyQuek/OneDrive%20-%20Unique%20Point%20Management%20Pte%20Ltd/Desktop/ta%20folder/stock-dashboard/render.yaml)

---

## Features
- **Overview** - Live market snapshot from Yahoo Finance
- **Theme Board** - Theme performance plus full stock constituents
- **ETF Flows** - ETF board to track capital rotation
- **ETF RRG** - Sector ETF relative rotation view
- **Earnings Tracker** - Upcoming U.S. earnings with AI summary
- **Chart Desk** - TradingView chart with AI reasoning and latest-news impact

## Data Sources
- Market data: Yahoo Finance (free, via yfinance)
- AI analysis: Anthropic Claude API
- Charts: TradingView (free embed)

## Notes
- Frontend refresh runs every 5 minutes.
- Backend endpoints are cached for short intervals to reduce repeated requests.
- Pre-market data is available 4am-9:30am ET on weekdays.
- If Yahoo blocks or delays one quote source, the backend now falls back to chart-derived prices and then yfinance.
