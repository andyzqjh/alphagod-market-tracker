import os
import time
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from ai_analyzer import analyze_chart_reasoning, analyze_stock, build_earnings_brief, build_market_brief
from data_fetcher import (
    get_chart_snapshot,
    get_etf_dashboard,
    get_etf_rrg_data,
    get_earnings_tracker,
    get_market_overview,
    get_screener_data,
    get_stock_detail,
    get_theme_dashboard,
    get_theme_data,
)
from news_fetcher import get_reddit_posts, get_stock_news

app = FastAPI(title='Stock Dashboard')
app.mount('/static', StaticFiles(directory=os.path.join(os.path.dirname(__file__), 'static')), name='static')
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])

_cache: dict = {}
_cache_time: dict = {}
CACHE_TTL = 300
MARKET_BRIEF_TICKERS = ['SPY', 'QQQ', 'IWM', 'TLT', 'GLD', 'BTC-USD']


def get_cached(key: str, ttl: int = CACHE_TTL):
    if key in _cache and time.time() - _cache_time.get(key, 0) < ttl:
        return _cache[key]
    return None


def set_cache(key: str, value):
    _cache[key] = value
    _cache_time[key] = time.time()


def _collect_market_headlines() -> List[dict]:
    headlines = []
    seen = set()
    for ticker in MARKET_BRIEF_TICKERS:
        for item in get_stock_news(ticker)[:3]:
            key = item.get('url') or f"{ticker}:{item.get('title')}"
            if key in seen:
                continue
            seen.add(key)
            headlines.append(item)
    headlines.sort(key=lambda item: item.get('time', 0), reverse=True)
    return headlines[:12]


@app.get('/')
def index():
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dashboard.html')
    with open(html_path, encoding='utf-8') as file_handle:
        html = file_handle.read()
    return HTMLResponse(content=html, headers={'Cache-Control': 'no-store'})


@app.get('/api/health')
def health():
    return {'status': 'ok'}


@app.get('/api/market-overview')
def market_overview():
    cached = get_cached('market_overview', ttl=60)
    if cached:
        return cached
    data = get_market_overview()
    set_cache('market_overview', data)
    return data


@app.get('/api/themes')
def themes():
    cached = get_cached('themes', ttl=120)
    if cached:
        return cached
    data = get_theme_data()
    set_cache('themes', data)
    return data


@app.get('/api/theme-dashboard')
def theme_dashboard():
    cached = get_cached('theme_dashboard', ttl=120)
    if cached:
        return cached
    data = get_theme_dashboard()
    set_cache('theme_dashboard', data)
    return data


@app.get('/api/etf-dashboard')
def etf_dashboard():
    cached = get_cached('etf_dashboard', ttl=120)
    if cached:
        return cached
    data = get_etf_dashboard()
    set_cache('etf_dashboard', data)
    return data


@app.get('/api/etf-rrg')
def etf_rrg():
    cached = get_cached('etf_rrg', ttl=180)
    if cached:
        return cached
    data = get_etf_rrg_data()
    set_cache('etf_rrg', data)
    return data


@app.get('/api/market-brief')
def market_brief():
    cached = get_cached('market_brief', ttl=300)
    if cached:
        return cached

    overview = get_market_overview()
    themes = get_theme_dashboard()
    etfs = get_etf_dashboard()
    headlines = _collect_market_headlines()
    brief = build_market_brief(overview, themes, etfs, headlines)
    payload = {
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'brief': brief,
        'headlines': headlines,
    }
    set_cache('market_brief', payload)
    return payload


@app.get('/api/chart-workspace/{ticker}')
def chart_workspace(ticker: str):
    ticker = ticker.upper()
    key = f'chart_workspace_{ticker}'
    cached = get_cached(key, ttl=240)
    if cached:
        return cached

    snapshot = get_chart_snapshot(ticker)
    detail = snapshot.get('detail') or get_stock_detail(ticker)
    headlines = get_stock_news(ticker)[:5]
    reasoning = analyze_chart_reasoning(ticker, detail, snapshot, headlines)
    payload = {
        'ticker': ticker,
        'detail': detail,
        'snapshot': snapshot,
        'headlines': headlines,
        'reasoning': reasoning,
    }
    set_cache(key, payload)
    return payload


@app.get('/api/earnings-tracker')
def earnings_tracker(days_ahead: int = Query(default=21), limit: int = Query(default=120)):
    key = f'earnings_tracker_{days_ahead}_{limit}'
    cached = get_cached(key, ttl=1800)
    if cached:
        return cached

    tracker = get_earnings_tracker(days_ahead=days_ahead, limit=limit)
    tracker['brief'] = build_earnings_brief(tracker)
    set_cache(key, tracker)
    return tracker

@app.get('/api/screener')
def screener(min_pct: float = Query(default=3.0), limit: int = Query(default=25)):
    key = f'screener_{min_pct}_{limit}'
    cached = get_cached(key, ttl=300)
    if cached:
        return cached
    data = get_screener_data(min_pct=min_pct, limit=limit)
    set_cache(key, data)
    return data


@app.get('/api/analyze/{ticker}')
def analyze(ticker: str, news: Optional[str] = None):
    ticker = ticker.upper()
    detail = get_stock_detail(ticker)
    if 'error' in detail:
        return {'ticker': ticker, 'detail': detail, 'analysis': None}
    analysis = analyze_stock(ticker, detail, news)
    return {'ticker': ticker, 'detail': detail, 'analysis': analysis}


@app.get('/api/news/{ticker}')
def stock_news(ticker: str):
    ticker = ticker.upper()
    key = f'news_{ticker}'
    cached = get_cached(key, ttl=300)
    if cached:
        return cached
    data = get_stock_news(ticker)
    set_cache(key, data)
    return data


@app.get('/api/reddit')
def reddit(tickers: str = Query(default='')):
    ticker_list = [ticker.strip().upper() for ticker in tickers.split(',') if ticker.strip()]
    if not ticker_list:
        return []
    key = 'reddit_' + '_'.join(sorted(ticker_list))
    cached = get_cached(key, ttl=300)
    if cached:
        return cached
    data = get_reddit_posts(ticker_list)
    set_cache(key, data)
    return data


@app.delete('/api/cache')
def clear_cache():
    _cache.clear()
    _cache_time.clear()
    return {'message': 'Cache cleared'}




