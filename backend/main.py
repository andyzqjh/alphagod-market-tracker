import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    get_sp500_heatmap,
    get_sp500_latest_news,
    get_watchlist_news,
    get_screener_data,
    get_session_movers,
    get_stock_detail,
    get_theme_dashboard,
    get_theme_data,
)
from news_fetcher import get_reddit_posts, get_stock_news
from stockbee_fetcher import get_stockbee_monitor

app = FastAPI(title='Stock Dashboard')
app.mount('/static', StaticFiles(directory=os.path.join(os.path.dirname(__file__), 'static')), name='static')
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])

_cache: dict = {}
_cache_time: dict = {}
CACHE_TTL = 300
MARKET_BRIEF_HEADLINE_TARGETS = [
    {'ticker': 'SPY', 'company_name': 'SPDR S&P 500 ETF Trust'},
    {'ticker': 'QQQ', 'company_name': 'Invesco QQQ Trust'},
    {'ticker': 'IWM', 'company_name': 'iShares Russell 2000 ETF'},
    {'ticker': 'TLT', 'company_name': 'iShares 20+ Year Treasury Bond ETF'},
    {'ticker': 'GLD', 'company_name': 'SPDR Gold Shares'},
    {'ticker': 'BTC-USD', 'company_name': 'Bitcoin'},
]


def get_cached(key: str, ttl: int = CACHE_TTL):
    if key in _cache and time.time() - _cache_time.get(key, 0) < ttl:
        return _cache[key]
    return None


def set_cache(key: str, value):
    _cache[key] = value
    _cache_time[key] = time.time()


def _collect_market_headlines() -> List[dict]:
    cached = get_cached('market_brief_headlines', ttl=180)
    if cached:
        return cached

    headlines = []
    seen = set()

    def fetch_target(target: dict) -> List[dict]:
        ticker = target.get('ticker') or ''
        company_name = target.get('company_name') or ticker
        items = get_stock_news(ticker, company_name=company_name, limit=6)
        filtered = [item for item in items if item.get('verified')] or items
        selected = []
        for item in filtered:
            payload = dict(item)
            payload.setdefault('ticker', ticker)
            selected.append(payload)
            if len(selected) >= 2:
                break
        return selected

    with ThreadPoolExecutor(max_workers=min(4, len(MARKET_BRIEF_HEADLINE_TARGETS))) as executor:
        futures = {
            executor.submit(fetch_target, target): target['ticker']
            for target in MARKET_BRIEF_HEADLINE_TARGETS
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                items = future.result() or []
            except Exception:
                items = []
            for item in items:
                key = item.get('url') or f"{ticker}:{item.get('title')}"
                if key in seen:
                    continue
                seen.add(key)
                headlines.append(item)

    headlines.sort(
        key=lambda item: (
            0 if item.get('verified') else 1,
            -(item.get('match_score') or 0),
            -(item.get('time') or 0),
        )
    )
    payload = headlines[:12]
    set_cache('market_brief_headlines', payload)
    return payload


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
    cached = get_cached('etf_rrg', ttl=60)
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
    headlines = get_stock_news(ticker, company_name=detail.get('company_name') or ticker)[:5]
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


@app.get('/api/session-movers/{session}')
def session_movers(session: str, min_move: float = Query(default=0.5), limit: int = Query(default=15)):
    session = 'post' if session.lower() == 'post' else 'pre'
    key = f'session_movers_{session}_{min_move}_{limit}'
    cached = get_cached(key, ttl=300)
    if cached:
        return cached
    data = get_session_movers(session=session, min_move=min_move, limit=limit)
    set_cache(key, data)
    return data

@app.get('/api/earnings-tracker')
def earnings_tracker(days_ahead: int = Query(default=21), limit: int = Query(default=120)):
    key = f'earnings_tracker_{days_ahead}_{limit}'
    cached = get_cached(key, ttl=120)
    if cached:
        return cached

    tracker = get_earnings_tracker(days_ahead=days_ahead, limit=limit)
    tracker['brief'] = build_earnings_brief(tracker)
    if tracker.get('summary', {}).get('total_events', 0) > 0:
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
    detail = get_stock_detail(ticker)
    data = get_stock_news(ticker, company_name=detail.get('company_name') or ticker)
    set_cache(key, data)
    return data


@app.get('/api/stockbee-monitor')
def stockbee_monitor():
    cached = get_cached('stockbee_monitor', ttl=600)
    if cached:
        return cached
    data = get_stockbee_monitor()
    set_cache('stockbee_monitor', data)
    return data


@app.get('/api/sp500-heatmap')
def sp500_heatmap():
    cached = get_cached('sp500_heatmap', ttl=120)
    if cached:
        return cached
    data = get_sp500_heatmap()
    set_cache('sp500_heatmap', data)
    return data


@app.get('/api/sp500-news')
def sp500_news(limit: int = Query(default=18)):
    key = f'sp500_news_{limit}'
    cached = get_cached(key, ttl=300)
    if cached:
        return cached
    data = get_sp500_latest_news(limit=limit)
    set_cache(key, data)
    return data


@app.get('/api/watchlist-news')
def watchlist_news(
    tickers: str = Query(default=''),
    limit_per_ticker: int = Query(default=3, ge=1, le=5),
):
    ticker_list = []
    seen = set()
    for raw in tickers.split(','):
        ticker = raw.strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        ticker_list.append(ticker)
    ticker_list = ticker_list[:40]

    key = f'watchlist_news_{"_".join(ticker_list)}_{limit_per_ticker}'
    cached = get_cached(key, ttl=240)
    if cached:
        return cached
    data = get_watchlist_news(ticker_list, limit_per_ticker=limit_per_ticker)
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




