import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from urllib.parse import quote as url_quote
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import yfinance as yf

from themes_config import ETF_UNIVERSE, MARKET_OVERVIEW, SECTOR_ETF_BENCHMARK, SECTOR_ETFS, THEMES
from universe import STOCK_UNIVERSE

YAHOO_QUOTE_URL = 'https://query1.finance.yahoo.com/v7/finance/quote'
YAHOO_CHART_URL = 'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}'
YAHOO_SUMMARY_URL = 'https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}'
NASDAQ_EARNINGS_URL = 'https://api.nasdaq.com/api/calendar/earnings'
REQUEST_TIMEOUT = 12
QUOTE_BATCH_SIZE = 40
GROUP_ORDER = ['Broad Market', 'Style & Factors', 'Sectors', 'Rates & Credit', 'Commodities', 'International', 'Thematic', 'Digital Assets']

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
})
LOGGER = logging.getLogger(__name__)

THEME_LOOKUP = {}
for theme_name, tickers in THEMES.items():
    for ticker in tickers:
        THEME_LOOKUP.setdefault(ticker, []).append(theme_name)


def _safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value):
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _round_number(value, digits: int = 2):
    number = _safe_float(value)
    return round(number, digits) if number is not None else None


def _chunked(items: List[str], size: int):
    for index in range(0, len(items), size):
        yield items[index:index + size]


def _request_json(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> dict:
    response = SESSION.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def _fetch_quote_summary_modules(symbol: str, modules: List[str]) -> dict:
    if not modules:
        return {}
    try:
        data = _request_json(
            YAHOO_SUMMARY_URL.format(symbol=url_quote(symbol, safe='')),
            params={'modules': ','.join(modules)},
        )
        result = data.get('quoteSummary', {}).get('result') or []
        return result[0] if result else {}
    except Exception:
        return {}


def _shape_quote(raw: dict) -> Optional[dict]:
    symbol = raw.get('symbol')
    if not symbol:
        return None

    price = _safe_float(raw.get('regularMarketPrice'))
    previous_close = _safe_float(raw.get('regularMarketPreviousClose') or raw.get('previousClose'))
    change = _safe_float(raw.get('regularMarketChange'))
    change_pct = _safe_float(raw.get('regularMarketChangePercent'))
    pre_market_price = _safe_float(raw.get('preMarketPrice'))
    pre_market_change_pct = _safe_float(raw.get('preMarketChangePercent'))
    post_market_price = _safe_float(raw.get('postMarketPrice'))
    post_market_change_pct = _safe_float(raw.get('postMarketChangePercent'))

    if pre_market_change_pct is None and pre_market_price is not None and previous_close not in (None, 0):
        pre_market_change_pct = ((pre_market_price - previous_close) / previous_close) * 100
    if post_market_change_pct is None and post_market_price is not None and previous_close not in (None, 0):
        post_market_change_pct = ((post_market_price - previous_close) / previous_close) * 100

    extended_price = pre_market_price if pre_market_price is not None else post_market_price
    extended_change_pct = pre_market_change_pct if pre_market_price is not None else post_market_change_pct
    extended_session = 'pre' if pre_market_price is not None else ('post' if post_market_price is not None else None)

    return {
        'symbol': symbol,
        'price': _round_number(price),
        'previous_close': _round_number(previous_close),
        'change': _round_number(change),
        'change_pct': _round_number(change_pct),
        'pre_market_price': _round_number(pre_market_price),
        'pre_market_change_pct': _round_number(pre_market_change_pct),
        'post_market_price': _round_number(post_market_price),
        'post_market_change_pct': _round_number(post_market_change_pct),
        'extended_price': _round_number(extended_price),
        'extended_change_pct': _round_number(extended_change_pct),
        'extended_session': extended_session,
        'volume': _safe_int(raw.get('regularMarketVolume') or raw.get('volume')),
        'average_volume': _safe_int(raw.get('averageDailyVolume3Month') or raw.get('averageDailyVolume10Day')),
        'market_cap': _safe_float(raw.get('marketCap')),
        'day_high': _round_number(raw.get('regularMarketDayHigh') or raw.get('regularMarketOpen')),
        'day_low': _round_number(raw.get('regularMarketDayLow') or raw.get('regularMarketOpen')),
        'currency': raw.get('financialCurrency') or raw.get('currency') or 'USD',
        'market_state': raw.get('marketState'),
        'long_name': raw.get('longName') or raw.get('shortName') or symbol,
        'quote_source': 'yahoo_quote_api',
    }


def _empty_stock_row(ticker: str) -> dict:
    return {
        'ticker': ticker,
        'company_name': ticker,
        'premarket_pct': None,
        'premarket_price': None,
        'daily_pct': None,
        'display_pct': None,
        'curr_price': None,
        'prev_close': None,
        'volume': None,
        'avg_volume': None,
        'rvol': None,
        'market_cap': None,
        'themes': THEME_LOOKUP.get(ticker, []),
        'quote_status': 'unavailable',
    }


def _fetch_yfinance_quote(symbol: str) -> Optional[dict]:
    try:
        ticker = yf.Ticker(symbol)
        history = ticker.history(period='1mo', interval='1d', auto_adjust=False, prepost=False)
        if history.empty or 'Close' not in history:
            return None

        history = history.dropna(subset=['Close']).copy()
        if history.empty:
            return None

        fast_info = {}
        info = {}
        try:
            fast_info = dict(ticker.fast_info or {})
        except Exception:
            fast_info = {}

        try:
            info = ticker.info or {}
        except Exception:
            info = {}

        latest = history.iloc[-1]
        previous_close = _safe_float(fast_info.get('previousClose'))
        if previous_close is None and len(history) >= 2:
            previous_close = _safe_float(history['Close'].iloc[-2])

        price = _safe_float(fast_info.get('lastPrice'))
        if price is None:
            price = _safe_float(latest.get('Close'))

        change = None
        change_pct = None
        if price is not None and previous_close not in (None, 0):
            change = price - previous_close
            change_pct = ((price - previous_close) / previous_close) * 100

        avg_volume = _safe_int(fast_info.get('threeMonthAverageVolume') or fast_info.get('tenDayAverageVolume'))
        if avg_volume is None and 'Volume' in history:
            avg_volume = _safe_int(history['Volume'].tail(min(len(history), 20)).mean())

        volume = _safe_int(fast_info.get('lastVolume'))
        if volume is None:
            volume = _safe_int(latest.get('Volume'))

        return {
            'symbol': symbol,
            'price': _round_number(price),
            'previous_close': _round_number(previous_close),
            'change': _round_number(change),
            'change_pct': _round_number(change_pct),
            'pre_market_price': None,
            'pre_market_change_pct': None,
            'post_market_price': None,
            'post_market_change_pct': None,
            'extended_price': None,
            'extended_change_pct': None,
            'extended_session': None,
            'volume': volume,
            'average_volume': avg_volume,
            'market_cap': _safe_float(info.get('marketCap')),
            'day_high': _round_number(fast_info.get('dayHigh') or latest.get('High') or price),
            'day_low': _round_number(fast_info.get('dayLow') or latest.get('Low') or price),
            'currency': info.get('currency') or 'USD',
            'market_state': None,
            'long_name': info.get('longName') or info.get('shortName') or symbol,
            'quote_source': 'yfinance_fallback',
        }
    except Exception as exc:
        LOGGER.warning('Fallback quote fetch failed for %s: %s', symbol, exc)
        return None


def _batch_fetch_yfinance_quotes(symbols: List[str]) -> dict:
    fallback_map = {}
    if not symbols:
        return fallback_map

    with ThreadPoolExecutor(max_workers=min(len(symbols), 8)) as executor:
        futures = {executor.submit(_fetch_yfinance_quote, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            shaped = future.result()
            if shaped:
                fallback_map[shaped['symbol']] = shaped

    return fallback_map


def _fetch_chart_quote(symbol: str) -> Optional[dict]:
    frame = _fetch_chart_frame(symbol, range_value='10d', interval='1d')
    if frame.empty or len(frame) < 2:
        return None

    close = frame['adjclose'].ffill()
    latest_row = frame.iloc[-1]
    latest_close = _safe_float(close.iloc[-1])
    previous_close = _safe_float(close.iloc[-2]) if len(close) >= 2 else None
    if latest_close is None:
        return None

    change = None
    change_pct = None
    if previous_close not in (None, 0):
        change = latest_close - previous_close
        change_pct = ((latest_close - previous_close) / previous_close) * 100

    avg_volume = None
    if 'volume' in frame and len(frame['volume'].dropna()) > 0:
        avg_volume = _safe_int(frame['volume'].tail(min(len(frame), 20)).mean())

    return {
        'symbol': symbol,
        'price': _round_number(latest_close),
        'previous_close': _round_number(previous_close),
        'change': _round_number(change),
        'change_pct': _round_number(change_pct),
        'pre_market_price': None,
        'pre_market_change_pct': None,
        'post_market_price': None,
        'post_market_change_pct': None,
        'extended_price': None,
        'extended_change_pct': None,
        'extended_session': None,
        'volume': _safe_int(latest_row.get('volume')),
        'average_volume': avg_volume,
        'market_cap': None,
        'day_high': _round_number(latest_row.get('high') or latest_close),
        'day_low': _round_number(latest_row.get('low') or latest_close),
        'currency': 'USD',
        'market_state': None,
        'long_name': symbol,
        'quote_source': 'chart_fallback',
    }


def _batch_fetch_chart_quotes(symbols: List[str]) -> dict:
    quote_map = {}
    if not symbols:
        return quote_map

    with ThreadPoolExecutor(max_workers=min(len(symbols), 8)) as executor:
        futures = {executor.submit(_fetch_chart_quote, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                shaped = future.result()
            except Exception as exc:
                LOGGER.warning('Chart quote fallback failed for %s: %s', symbol, exc)
                continue
            if shaped:
                quote_map[symbol] = shaped

    return quote_map


def _batch_fetch_quotes(symbols: List[str]) -> dict:
    unique_symbols = list(dict.fromkeys(symbols))
    quote_map = {}

    for chunk in _chunked(unique_symbols, QUOTE_BATCH_SIZE):
        try:
            data = _request_json(YAHOO_QUOTE_URL, params={'symbols': ','.join(chunk)})
        except Exception as exc:
            LOGGER.warning('Primary Yahoo quote batch failed for %s: %s', ','.join(chunk), exc)
            continue

        for raw in data.get('quoteResponse', {}).get('result', []):
            shaped = _shape_quote(raw)
            if shaped:
                quote_map[shaped['symbol']] = shaped

    missing_symbols = [symbol for symbol in unique_symbols if symbol not in quote_map]
    if missing_symbols:
        quote_map.update(_batch_fetch_chart_quotes(missing_symbols))

    missing_symbols = [symbol for symbol in unique_symbols if symbol not in quote_map]
    if missing_symbols:
        quote_map.update(_batch_fetch_yfinance_quotes(missing_symbols))

    return quote_map

def _fetch_single_quote(symbol: str) -> Optional[dict]:
    return _batch_fetch_quotes([symbol]).get(symbol)


def _fetch_chart_frame(symbol: str, range_value: str = '1y', interval: str = '1d') -> pd.DataFrame:
    try:
        data = _request_json(
            YAHOO_CHART_URL.format(symbol=url_quote(symbol, safe='')),
            params={
                'range': range_value,
                'interval': interval,
                'includePrePost': 'false',
                'events': 'div,splits',
            },
        )
        result = data.get('chart', {}).get('result') or []
        if not result:
            return pd.DataFrame()

        payload = result[0]
        timestamps = payload.get('timestamp') or []
        quotes = payload.get('indicators', {}).get('quote', [{}])[0]
        if not timestamps or not quotes:
            return pd.DataFrame()

        count = min(
            len(timestamps),
            len(quotes.get('open', timestamps)),
            len(quotes.get('high', timestamps)),
            len(quotes.get('low', timestamps)),
            len(quotes.get('close', timestamps)),
            len(quotes.get('volume', timestamps)),
        )
        if count == 0:
            return pd.DataFrame()

        frame = pd.DataFrame({
            'open': quotes.get('open', [])[:count],
            'high': quotes.get('high', [])[:count],
            'low': quotes.get('low', [])[:count],
            'close': quotes.get('close', [])[:count],
            'volume': quotes.get('volume', [])[:count],
        })

        adjclose = payload.get('indicators', {}).get('adjclose', [])
        if adjclose:
            frame['adjclose'] = adjclose[0].get('adjclose', [])[:count]
        else:
            frame['adjclose'] = frame['close']

        frame.index = pd.to_datetime(timestamps[:count], unit='s', utc=True).tz_convert(None)
        frame = frame.apply(pd.to_numeric, errors='coerce').dropna(subset=['close']).sort_index()
        return frame
    except Exception:
        return pd.DataFrame()


def _compute_rsi(series: pd.Series, period: int = 14) -> Optional[float]:
    if len(series) < period + 1:
        return None
    delta = series.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.rolling(period).mean()
    avg_loss = losses.rolling(period).mean()
    if avg_loss.iloc[-1] == 0:
        return 100.0
    rs = avg_gain.iloc[-1] / avg_loss.iloc[-1]
    return 100 - (100 / (1 + rs))


def _return_pct(series: pd.Series, periods: int) -> Optional[float]:
    if len(series) <= periods:
        return None
    base = series.iloc[-periods - 1]
    if base in (None, 0):
        return None
    return round(((series.iloc[-1] - base) / base) * 100, 2)


def _latest_rrg_price(quote_data: dict) -> Optional[float]:
    return _safe_float(
        quote_data.get('extended_price')
        or quote_data.get('price')
        or quote_data.get('post_market_price')
        or quote_data.get('pre_market_price')
    )


def _latest_rrg_change_pct(quote_data: dict) -> Optional[float]:
    return _safe_float(
        quote_data.get('extended_change_pct')
        or quote_data.get('change_pct')
        or quote_data.get('post_market_change_pct')
        or quote_data.get('pre_market_change_pct')
    )


def _overlay_live_rrg_point(series: pd.Series, live_price: Optional[float]) -> pd.Series:
    if series.empty or live_price in (None, 0):
        return series

    updated = series.copy()
    live_index = pd.Timestamp(datetime.now(timezone.utc).date())
    last_index = updated.index[-1]

    if last_index.date() == live_index.date():
        updated.iloc[-1] = live_price
        return updated

    if live_index > last_index:
        updated.loc[live_index] = live_price
    else:
        updated.iloc[-1] = live_price

    return updated.sort_index()


def _stock_row_from_quote(quote_data: dict) -> Optional[dict]:
    if not quote_data:
        return None

    pre_pct = quote_data.get('pre_market_change_pct')
    daily_pct = quote_data.get('change_pct')
    volume = quote_data.get('volume') or 0
    avg_vol = quote_data.get('average_volume') or 1
    rvol = round(volume / avg_vol, 2) if avg_vol and avg_vol > 0 else 1.0
    display_pct = pre_pct if (daily_pct is None or daily_pct == 0.0) and pre_pct is not None else daily_pct

    return {
        'ticker': quote_data['symbol'],
        'company_name': quote_data.get('long_name') or quote_data['symbol'],
        'premarket_pct': pre_pct,
        'premarket_price': quote_data.get('pre_market_price'),
        'daily_pct': daily_pct,
        'display_pct': display_pct,
        'curr_price': quote_data.get('price'),
        'prev_close': quote_data.get('previous_close'),
        'volume': int(volume),
        'avg_volume': int(avg_vol) if avg_vol else None,
        'rvol': rvol,
        'market_cap': quote_data.get('market_cap'),
        'themes': THEME_LOOKUP.get(quote_data['symbol'], []),
    }


def get_market_overview() -> dict:
    quotes = _batch_fetch_quotes([item['symbol'] for item in MARKET_OVERVIEW])
    items = []

    for item in MARKET_OVERVIEW:
        quote_data = quotes.get(item['symbol']) or {}
        items.append({
            'symbol': item['symbol'],
            'label': item['label'],
            'group': item['group'],
            'price': quote_data.get('price'),
            'change': quote_data.get('change'),
            'change_pct': quote_data.get('change_pct'),
            'previous_close': quote_data.get('previous_close'),
            'extended_price': quote_data.get('extended_price'),
            'extended_change_pct': quote_data.get('extended_change_pct'),
            'extended_session': quote_data.get('extended_session'),
            'day_high': quote_data.get('day_high'),
            'day_low': quote_data.get('day_low'),
            'volume': quote_data.get('volume'),
            'currency': quote_data.get('currency'),
            'quote_status': 'available' if quote_data else 'unavailable',
        })

    positive = sum(1 for item in items if (item.get('change_pct') or 0) > 0)
    negative = sum(1 for item in items if (item.get('change_pct') or 0) < 0)
    flat = len(items) - positive - negative

    return {
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'summary': {
            'positive': positive,
            'negative': negative,
            'flat': flat,
        },
        'items': items,
    }

def get_stock_detail(ticker: str) -> dict:
    quote_data = _fetch_single_quote(ticker) or {}
    info = {}
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception:
        info = {}

    volume = quote_data.get('volume') or info.get('volume') or 0
    avg_volume = quote_data.get('average_volume') or info.get('averageVolume') or 1
    rvol = round(volume / avg_volume, 2) if avg_volume and avg_volume > 0 else 1.0
    short_pct = info.get('shortPercentOfFloat')

    return {
        'ticker': ticker,
        'company_name': quote_data.get('long_name') or info.get('longName', ticker),
        'price': quote_data.get('price'),
        'change_pct': quote_data.get('change_pct'),
        'premarket_pct': quote_data.get('pre_market_change_pct'),
        'premarket_price': quote_data.get('pre_market_price'),
        'postmarket_pct': quote_data.get('post_market_change_pct'),
        'postmarket_price': quote_data.get('post_market_price'),
        'prev_close': quote_data.get('previous_close') or info.get('previousClose'),
        'volume': int(volume),
        'avg_volume': int(avg_volume),
        'rvol': rvol,
        'short_interest': round(float(short_pct) * 100, 1) if short_pct else None,
        'float_shares': info.get('floatShares'),
        'industry': info.get('industry'),
        'sector': info.get('sector'),
        'market_cap': quote_data.get('market_cap') or info.get('marketCap'),
        'description': info.get('longBusinessSummary', '')[:600] if info.get('longBusinessSummary') else '',
        'recommendation': info.get('recommendationKey'),
        'analyst_count': info.get('numberOfAnalystOpinions'),
        'target_mean_price': _round_number(info.get('targetMeanPrice')),
        'target_high_price': _round_number(info.get('targetHighPrice')),
        'target_low_price': _round_number(info.get('targetLowPrice')),
        'revenue_growth': _round_number((_safe_float(info.get('revenueGrowth')) or _safe_float(info.get('quarterlyRevenueGrowth'))) * 100) if (_safe_float(info.get('revenueGrowth')) is not None or _safe_float(info.get('quarterlyRevenueGrowth')) is not None) else None,
        'earnings_growth': _round_number((_safe_float(info.get('earningsGrowth')) or _safe_float(info.get('earningsQuarterlyGrowth'))) * 100) if (_safe_float(info.get('earningsGrowth')) is not None or _safe_float(info.get('earningsQuarterlyGrowth')) is not None) else None,
        'gross_margin': _round_number(_safe_float(info.get('grossMargins')) * 100) if _safe_float(info.get('grossMargins')) is not None else None,
        'operating_margin': _round_number(_safe_float(info.get('operatingMargins')) * 100) if _safe_float(info.get('operatingMargins')) is not None else None,
        'profit_margin': _round_number(_safe_float(info.get('profitMargins')) * 100) if _safe_float(info.get('profitMargins')) is not None else None,
        'forward_pe': _round_number(info.get('forwardPE')),
        'trailing_pe': _round_number(info.get('trailingPE')),
        'price_to_sales': _round_number(info.get('priceToSalesTrailing12Months')),
        'enterprise_to_ebitda': _round_number(info.get('enterpriseToEbitda')),
        'beta': _round_number(info.get('beta')),
        'themes': THEME_LOOKUP.get(ticker, []),
    }


def get_chart_snapshot(ticker: str) -> dict:
    frame = _fetch_chart_frame(ticker, range_value='1y', interval='1d')
    quote_data = _fetch_single_quote(ticker) or {}
    detail = get_stock_detail(ticker)

    if frame.empty:
        return {
            'ticker': ticker,
            'detail': detail,
            'metrics': {},
            'error': 'Unable to load chart data right now.',
        }

    close = frame['adjclose'].ffill()
    volume = frame['volume'].fillna(0)
    latest_close = close.iloc[-1]

    sma20 = close.rolling(20).mean().iloc[-1] if len(close) >= 20 else None
    sma50 = close.rolling(50).mean().iloc[-1] if len(close) >= 50 else None
    sma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else None
    high20 = close.tail(20).max() if len(close) >= 20 else close.max()
    low20 = close.tail(20).min() if len(close) >= 20 else close.min()
    high52 = close.max()
    low52 = close.min()
    avg_volume20 = volume.tail(20).mean() if len(volume) >= 20 else volume.mean()
    relative_volume20 = round(volume.iloc[-1] / avg_volume20, 2) if avg_volume20 else None
    rsi14 = _compute_rsi(close, 14)

    trend_state = 'Range-bound'
    if sma20 and sma50 and sma200:
        if latest_close > sma20 > sma50 > sma200:
            trend_state = 'Strong uptrend'
        elif latest_close < sma20 < sma50 < sma200:
            trend_state = 'Strong downtrend'
        elif latest_close > sma50 > sma200:
            trend_state = 'Constructive uptrend'
        elif latest_close < sma50 < sma200:
            trend_state = 'Weakening downtrend'

    price_vs_high20 = round(((latest_close / high20) - 1) * 100, 2) if high20 else None
    price_vs_high52 = round(((latest_close / high52) - 1) * 100, 2) if high52 else None

    return {
        'ticker': ticker,
        'detail': detail,
        'metrics': {
            'last_close': _round_number(latest_close),
            'sma20': _round_number(sma20),
            'sma50': _round_number(sma50),
            'sma200': _round_number(sma200),
            'high20': _round_number(high20),
            'low20': _round_number(low20),
            'high52': _round_number(high52),
            'low52': _round_number(low52),
            'avg_volume20': _safe_int(avg_volume20),
            'relative_volume20': _round_number(relative_volume20),
            'rsi14': _round_number(rsi14),
            'return_1w': _return_pct(close, 5),
            'return_1m': _return_pct(close, 21),
            'return_3m': _return_pct(close, 63),
            'price_vs_high20': price_vs_high20,
            'price_vs_high52': price_vs_high52,
            'trend_state': trend_state,
            'today_open': _round_number(frame['open'].iloc[-1]),
            'today_high': _round_number(frame['high'].iloc[-1]),
            'today_low': _round_number(frame['low'].iloc[-1]),
            'today_close': _round_number(frame['close'].iloc[-1]),
            'today_volume': _safe_int(frame['volume'].iloc[-1]),
            'change_pct': quote_data.get('change_pct'),
        },
        'history': [
            {
                'date': index.strftime('%Y-%m-%d'),
                'close': _round_number(row['adjclose']),
                'volume': _safe_int(row['volume']),
            }
            for index, row in frame.tail(120).iterrows()
        ],
    }


def get_screener_data(min_pct: float = 3.0, limit: int = 25) -> list:
    quotes = _batch_fetch_quotes(STOCK_UNIVERSE)
    rows = []

    for ticker in STOCK_UNIVERSE:
        stock_row = _stock_row_from_quote(quotes.get(ticker))
        if stock_row and stock_row.get('premarket_pct') is not None and stock_row['premarket_pct'] >= min_pct:
            rows.append(stock_row)

    rows.sort(key=lambda item: item.get('premarket_pct', 0), reverse=True)
    return rows[:limit]


def get_theme_data() -> list:
    theme_symbols = []
    for tickers in THEMES.values():
        theme_symbols.extend(tickers)

    quotes = _batch_fetch_quotes(theme_symbols)
    theme_results = []

    for theme_name, tickers in THEMES.items():
        stocks = []
        up_count = 0
        down_count = 0

        for ticker in tickers:
            stock_row = _stock_row_from_quote(quotes.get(ticker)) or _empty_stock_row(ticker)
            display_pct = stock_row.get('display_pct')
            stocks.append(stock_row)
            if display_pct is not None:
                if display_pct > 0:
                    up_count += 1
                elif display_pct < 0:
                    down_count += 1

        stocks.sort(key=lambda stock: stock.get('display_pct') or 0, reverse=True)
        valid = [stock['display_pct'] for stock in stocks if stock.get('display_pct') is not None]
        avg_pct = round(sum(valid) / len(valid), 2) if valid else 0.0

        theme_results.append({
            'theme': theme_name,
            'avg_pct': avg_pct,
            'up_count': up_count,
            'down_count': down_count,
            'stock_count': len(stocks),
            'leaders': stocks[:5],
            'laggards': list(reversed(stocks[-5:])),
            'constituents': stocks,
        })

    theme_results.sort(key=lambda item: item['avg_pct'], reverse=True)
    return theme_results


def get_theme_dashboard() -> dict:
    themes = get_theme_data()
    positive = sum(1 for theme in themes if theme.get('avg_pct', 0) > 0)
    negative = sum(1 for theme in themes if theme.get('avg_pct', 0) < 0)
    flat = len(themes) - positive - negative

    leaders = themes[:6]
    laggards = sorted(themes, key=lambda item: item.get('avg_pct', 0))[:6]

    return {
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'summary': {
            'total_themes': len(themes),
            'positive': positive,
            'negative': negative,
            'flat': flat,
            'best_theme': leaders[0]['theme'] if leaders else None,
            'worst_theme': laggards[0]['theme'] if laggards else None,
        },
        'leaders': leaders,
        'laggards': laggards,
        'all': themes,
    }


def get_etf_dashboard() -> dict:
    quotes = _batch_fetch_quotes([item['symbol'] for item in ETF_UNIVERSE])
    all_items = []

    for item in ETF_UNIVERSE:
        quote_data = quotes.get(item['symbol']) or {}
        volume = quote_data.get('volume') or 0
        avg_volume = quote_data.get('average_volume') or 1
        rvol = round(volume / avg_volume, 2) if avg_volume and avg_volume > 0 else 1.0
        change_pct = quote_data.get('change_pct') or 0.0
        flow_proxy = round(change_pct * max(min(rvol, 5), 1), 2) if quote_data else 0.0

        all_items.append({
            'symbol': item['symbol'],
            'label': item['label'],
            'group': item['group'],
            'price': quote_data.get('price'),
            'change_pct': quote_data.get('change_pct'),
            'extended_change_pct': quote_data.get('extended_change_pct'),
            'extended_session': quote_data.get('extended_session'),
            'volume': volume if quote_data else None,
            'avg_volume': avg_volume if quote_data else None,
            'rvol': rvol if quote_data else None,
            'flow_proxy': flow_proxy,
            'quote_status': 'available' if quote_data else 'unavailable',
        })

    group_map = {}
    for item in all_items:
        group_map.setdefault(item['group'], []).append(item)

    for group_items in group_map.values():
        group_items.sort(key=lambda entry: entry.get('flow_proxy', 0), reverse=True)

    groups = [
        {
            'group': group,
            'avg_change_pct': round(sum((entry.get('change_pct') or 0) for entry in entries) / len(entries), 2) if entries else 0.0,
            'avg_flow_proxy': round(sum((entry.get('flow_proxy') or 0) for entry in entries) / len(entries), 2) if entries else 0.0,
            'items': entries,
        }
        for group, entries in sorted(group_map.items(), key=lambda pair: GROUP_ORDER.index(pair[0]) if pair[0] in GROUP_ORDER else len(GROUP_ORDER))
    ]

    leaders = sorted(all_items, key=lambda entry: entry.get('flow_proxy', 0), reverse=True)[:10]
    laggards = sorted(all_items, key=lambda entry: entry.get('flow_proxy', 0))[:10]
    best_group = max(groups, key=lambda group: group.get('avg_flow_proxy', 0), default=None)

    return {
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'summary': {
            'total_etfs': len(all_items),
            'best_group': best_group['group'] if best_group else None,
            'risk_on_proxy': next((item.get('change_pct') for item in all_items if item['symbol'] == 'QQQ'), None),
            'defensive_proxy': next((item.get('change_pct') for item in all_items if item['symbol'] == 'TLT'), None),
        },
        'leaders': leaders,
        'laggards': laggards,
        'groups': groups,
        'all': all_items,
    }

def _classify_rrg_point(rs_ratio: float, rs_momentum: float) -> str:
    if rs_ratio >= 100 and rs_momentum >= 100:
        return 'Leading'
    if rs_ratio >= 100 and rs_momentum < 100:
        return 'Weakening'
    if rs_ratio < 100 and rs_momentum < 100:
        return 'Lagging'
    return 'Improving'


def get_etf_rrg_data() -> dict:
    benchmark_symbol = SECTOR_ETF_BENCHMARK['symbol']
    benchmark_label = SECTOR_ETF_BENCHMARK['label']
    symbols = [item['symbol'] for item in SECTOR_ETFS]
    universe = [benchmark_symbol] + symbols

    history_map = {}
    with ThreadPoolExecutor(max_workers=min(len(universe), 8)) as executor:
        futures = {executor.submit(_fetch_chart_frame, symbol, '2y', '1wk'): symbol for symbol in universe}
        for future in as_completed(futures):
            symbol = futures[future]
            frame = future.result()
            history_map[symbol] = frame['adjclose'] if not frame.empty else pd.Series(dtype='float64')

    benchmark_series = history_map.get(benchmark_symbol, pd.Series(dtype='float64'))
    if benchmark_series.empty:
        return {
            'updated_at': datetime.now(timezone.utc).isoformat(),
            'benchmark': {'symbol': benchmark_symbol, 'label': benchmark_label},
            'center': {'rs_ratio': 100, 'rs_momentum': 100},
            'items': [],
            'error': 'Unable to calculate ETF relative rotation graph right now.',
        }

    quotes = _batch_fetch_quotes(universe)
    benchmark_quote = quotes.get(benchmark_symbol, {})
    benchmark_live_price = _latest_rrg_price(benchmark_quote)
    benchmark_series_live = _overlay_live_rrg_point(benchmark_series, benchmark_live_price)
    items = []

    for item in SECTOR_ETFS:
        symbol = item['symbol']
        asset_series = history_map.get(symbol, pd.Series(dtype='float64'))
        if asset_series.empty:
            continue

        quote_data = quotes.get(symbol, {})
        live_price = _latest_rrg_price(quote_data)
        asset_series_live = _overlay_live_rrg_point(asset_series, live_price)
        aligned = pd.concat([asset_series_live, benchmark_series_live], axis=1, join='inner').dropna()
        aligned.columns = ['asset', 'benchmark']
        if len(aligned) < 16:
            continue

        relative_strength = (aligned['asset'] / aligned['benchmark']) * 100
        rs_ratio = 100 + ((relative_strength / relative_strength.rolling(10).mean()) - 1) * 100
        rs_momentum = 100 + ((rs_ratio / rs_ratio.rolling(4).mean()) - 1) * 100
        frame = pd.DataFrame({'rs_ratio': rs_ratio, 'rs_momentum': rs_momentum}).dropna()
        if len(frame) < 8:
            continue

        trail = frame.tail(8)
        latest = trail.iloc[-1]
        weekly_change_pct = _return_pct(aligned['asset'], 1)
        latest_price = _latest_rrg_price(quote_data) or _round_number(aligned['asset'].iloc[-1])
        latest_change_pct = _latest_rrg_change_pct(quote_data)

        items.append({
            'symbol': symbol,
            'label': item['label'],
            'group': item['group'],
            'price': latest_price,
            'change_pct': latest_change_pct if latest_change_pct is not None else weekly_change_pct,
            'weekly_change_pct': weekly_change_pct,
            'rs_ratio': _round_number(latest['rs_ratio']),
            'rs_momentum': _round_number(latest['rs_momentum']),
            'quadrant': _classify_rrg_point(float(latest['rs_ratio']), float(latest['rs_momentum'])),
            'live_price': bool(live_price),
            'trail': [
                {
                    'date': index.strftime('%Y-%m-%d'),
                    'rs_ratio': _round_number(row['rs_ratio']),
                    'rs_momentum': _round_number(row['rs_momentum']),
                }
                for index, row in trail.iterrows()
            ],
        })

    items.sort(key=lambda entry: (entry.get('quadrant') != 'Leading', -(entry.get('rs_ratio') or 0), -(entry.get('rs_momentum') or 0)))

    return {
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'benchmark': {
            'symbol': benchmark_symbol,
            'label': benchmark_label,
            'price': benchmark_live_price or benchmark_quote.get('price'),
            'change_pct': _latest_rrg_change_pct(benchmark_quote) if _latest_rrg_change_pct(benchmark_quote) is not None else benchmark_quote.get('change_pct'),
        },
        'center': {'rs_ratio': 100, 'rs_momentum': 100},
        'items': items,
        'live_prices': True,
    }








def _coerce_timestamp(value) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        if stripped.isdigit():
            value = int(stripped)

    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric <= 0:
            return None
        if numeric > 1_000_000_000_000:
            numeric = numeric / 1000
        try:
            return datetime.fromtimestamp(numeric, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None

    try:
        timestamp = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(timestamp):
        return None
    if timestamp.tzinfo is None:
        return timestamp.to_pydatetime().replace(tzinfo=timezone.utc)
    return timestamp.tz_convert('UTC').to_pydatetime()


def _extract_calendar_dates(raw_value) -> List[datetime]:
    if raw_value is None:
        return []

    if isinstance(raw_value, pd.DataFrame):
        values = raw_value.values.flatten().tolist()
    elif isinstance(raw_value, pd.Series):
        values = raw_value.tolist()
    elif isinstance(raw_value, (list, tuple, set)):
        values = list(raw_value)
    else:
        values = [raw_value]

    dates = []
    seen = set()
    for value in values:
        timestamp = _coerce_timestamp(value)
        if timestamp:
            key = timestamp.isoformat()
            if key not in seen:
                seen.add(key)
                dates.append(timestamp)
    dates.sort()
    return dates


def _extract_info_earnings_dates(info: dict) -> List[datetime]:
    dates = []
    seen = set()
    for key in ('earningsTimestamp', 'earningsTimestampStart', 'earningsTimestampEnd'):
        timestamp = _coerce_timestamp((info or {}).get(key))
        if timestamp:
            iso = timestamp.isoformat()
            if iso not in seen:
                seen.add(iso)
                dates.append(timestamp)
    dates.sort()
    return dates


def _clean_numeric_text(value):
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.replace('$', '').replace(',', '').replace('%', '').strip()
        if not cleaned or cleaned in ('--', 'N/A', 'n/a'):
            return None
        return _safe_float(cleaned)
    return _safe_float(value)


def _fetch_nasdaq_earnings_rows(target_date) -> List[dict]:
    headers = {
        'accept': 'application/json, text/plain, */*',
        'origin': 'https://www.nasdaq.com',
        'referer': 'https://www.nasdaq.com/',
        'user-agent': SESSION.headers.get('User-Agent', 'Mozilla/5.0'),
    }
    try:
        payload = _request_json(
            NASDAQ_EARNINGS_URL,
            params={'date': target_date.strftime('%Y-%m-%d')},
            headers=headers,
        )
    except Exception:
        return []

    rows = payload.get('data', {}).get('rows') or []
    return rows if isinstance(rows, list) else []


def _nasdaq_row_time_label(row: dict) -> str:
    text = ' '.join(str(row.get(key, '')) for key in ('time', 'timeFrame', 'when')).strip().lower()
    if any(term in text for term in ('bmo', 'before market', 'before-market', 'pre-market', 'premarket')):
        return 'BMO'
    if any(term in text for term in ('amc', 'after market', 'after-market', 'post-market', 'postmarket')):
        return 'AMC'
    return 'TNS'


def _nasdaq_event_datetime(target_date, row: dict) -> datetime:
    label = _nasdaq_row_time_label(row)
    if label == 'BMO':
        et_dt = datetime(target_date.year, target_date.month, target_date.day, 8, 0, tzinfo=EASTERN_TZ)
    elif label == 'AMC':
        et_dt = datetime(target_date.year, target_date.month, target_date.day, 16, 5, tzinfo=EASTERN_TZ)
    else:
        et_dt = datetime(target_date.year, target_date.month, target_date.day, 12, 0, tzinfo=EASTERN_TZ)
    return et_dt.astimezone(timezone.utc)


def _extract_nasdaq_earnings_candidates(tracked_universe: List[str], start_dt: datetime, end_dt: datetime) -> List[dict]:
    tracked = {ticker.upper() for ticker in tracked_universe}
    candidate_map = {}
    current_date = start_dt.astimezone(EASTERN_TZ).date()
    end_date = end_dt.astimezone(EASTERN_TZ).date()

    while current_date <= end_date:
        for row in _fetch_nasdaq_earnings_rows(current_date):
            ticker = str(row.get('symbol') or row.get('ticker') or '').upper().strip()
            if not ticker or ticker not in tracked:
                continue
            earnings_dt = _nasdaq_event_datetime(current_date, row)
            if earnings_dt < start_dt or earnings_dt > end_dt:
                continue
            candidate = _make_earnings_candidate(
                ticker,
                earnings_dt,
                'nasdaq_calendar',
                eps_estimate=_clean_numeric_text(row.get('epsForecast') or row.get('estimate') or row.get('epsEstimate')),
                reported_eps=_clean_numeric_text(row.get('eps') or row.get('epsActual') or row.get('reportedEPS')),
                surprise_pct=_clean_numeric_text(row.get('surprise') or row.get('surprisePercentage') or row.get('surprisePercent')),
            )
            candidate['report_time'] = _nasdaq_row_time_label(row)
            existing = candidate_map.get(ticker)
            if not existing or abs((candidate['earnings_date'] - start_dt).total_seconds()) < abs((existing['earnings_date'] - start_dt).total_seconds()):
                candidate_map[ticker] = candidate
        current_date += timedelta(days=1)

    return list(candidate_map.values())


def _earnings_source_label(source: str) -> str:
    return {
        'nasdaq_calendar': 'Nasdaq calendar',
        'quote_summary': 'Yahoo quote summary',
        'earnings_dates': 'Yahoo earnings dates',
        'calendar': 'Yahoo calendar',
        'info_timestamp': 'Yahoo profile timestamps',
    }.get(source, 'Yahoo earnings feed')


def _earnings_source_rank(source: str) -> int:
    return {
        'nasdaq_calendar': 0,
        'quote_summary': 1,
        'earnings_dates': 2,
        'calendar': 3,
        'info_timestamp': 4,
    }.get(source, 9)


def _make_earnings_candidate(
    ticker: str,
    earnings_dt: datetime,
    source: str,
    eps_estimate=None,
    reported_eps=None,
    surprise_pct=None,
) -> dict:
    return {
        'ticker': ticker,
        'earnings_date': earnings_dt,
        'eps_estimate': _round_number(eps_estimate),
        'reported_eps': _round_number(reported_eps),
        'surprise_pct': _round_number(surprise_pct),
        'event_source': source,
        'event_source_label': _earnings_source_label(source),
    }


def _pick_earnings_candidate(candidates: List[dict], now_dt: datetime) -> Optional[dict]:
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda item: (
            abs((item['earnings_date'] - now_dt).total_seconds()),
            _earnings_source_rank(item.get('event_source')),
            item['earnings_date'],
        ),
    )[0]


def _extract_quote_summary_earnings_dates(summary: dict) -> List[dict]:
    earnings = ((summary or {}).get('calendarEvents') or {}).get('earnings') or {}
    raw_dates = earnings.get('earningsDate') or []
    if isinstance(raw_dates, dict):
        raw_dates = [raw_dates]

    eps_estimate = None
    earnings_average = earnings.get('earningsAverage')
    if isinstance(earnings_average, dict):
        eps_estimate = earnings_average.get('raw', earnings_average.get('fmt'))
    else:
        eps_estimate = earnings_average

    items = []
    for value in raw_dates:
        raw_value = value.get('raw') if isinstance(value, dict) else value
        earnings_dt = _coerce_timestamp(raw_value)
        if not earnings_dt:
            continue
        items.append({
            'earnings_date': earnings_dt,
            'eps_estimate': eps_estimate,
        })
    return items


def _fetch_single_earnings_event(ticker: str, start_dt: datetime, end_dt: datetime, now_dt: datetime) -> Optional[dict]:
    candidates = []

    summary = _fetch_quote_summary_modules(ticker, ['calendarEvents'])
    for item in _extract_quote_summary_earnings_dates(summary):
        earnings_dt = item.get('earnings_date')
        if earnings_dt and start_dt <= earnings_dt <= end_dt:
            candidates.append(_make_earnings_candidate(
                ticker,
                earnings_dt,
                'quote_summary',
                eps_estimate=item.get('eps_estimate'),
            ))

    try:
        stock = yf.Ticker(ticker)
    except Exception:
        return _pick_earnings_candidate(candidates, now_dt)

    getter = getattr(stock, 'get_earnings_dates', None)
    if callable(getter):
        try:
            frame = getter(limit=16)
            if isinstance(frame, pd.DataFrame) and not frame.empty:
                working = frame.reset_index()
                date_column = working.columns[0]
                for _, row in working.iterrows():
                    earnings_dt = _coerce_timestamp(row.get(date_column))
                    if not earnings_dt or earnings_dt < start_dt or earnings_dt > end_dt:
                        continue
                    candidates.append(_make_earnings_candidate(
                        ticker,
                        earnings_dt,
                        'earnings_dates',
                        eps_estimate=row.get('EPS Estimate'),
                        reported_eps=row.get('Reported EPS'),
                        surprise_pct=row.get('Surprise(%)'),
                    ))
        except Exception:
            pass

    try:
        calendar = stock.calendar
    except Exception:
        calendar = None

    raw_dates = None
    if isinstance(calendar, dict):
        raw_dates = calendar.get('Earnings Date') or calendar.get('Earnings Date*')
    elif isinstance(calendar, pd.DataFrame):
        if 'Earnings Date' in calendar.columns:
            raw_dates = calendar['Earnings Date']
        elif 'Earnings Date' in calendar.index:
            raw_dates = calendar.loc['Earnings Date']

    for earnings_dt in _extract_calendar_dates(raw_dates):
        if start_dt <= earnings_dt <= end_dt:
            candidates.append(_make_earnings_candidate(ticker, earnings_dt, 'calendar'))

    info = {}
    try:
        info = stock.info or {}
    except Exception:
        info = {}

    for earnings_dt in _extract_info_earnings_dates(info):
        if start_dt <= earnings_dt <= end_dt:
            candidates.append(_make_earnings_candidate(ticker, earnings_dt, 'info_timestamp'))

    return _pick_earnings_candidate(candidates, now_dt)


def _build_earnings_reasoning(event: dict, quote_data: dict, themes: List[str], now_dt: datetime) -> str:
    earnings_dt = event.get('earnings_date')
    surprise_pct = event.get('surprise_pct')
    change_pct = quote_data.get('change_pct')
    price = quote_data.get('price')
    source_label = event.get('event_source_label') or 'Yahoo earnings feed'
    theme_text = ', '.join(themes[:2]) if themes else 'the broader tape'

    parts = []
    if earnings_dt and earnings_dt.date() < now_dt.date():
        if surprise_pct is not None:
            if surprise_pct > 0:
                parts.append(f'Recent results beat EPS by {surprise_pct:.2f}%, so traders are watching for post-earnings follow-through instead of a quick fade.')
            elif surprise_pct < 0:
                parts.append(f'Recent results missed EPS by {abs(surprise_pct):.2f}%, which keeps the focus on whether sellers still control the post-earnings tape.')
            else:
                parts.append('Recent results were close to expectations, so price reaction matters more than the raw print now.')
        elif event.get('reported_eps') is not None:
            parts.append(f'The report is already out with reported EPS at {event["reported_eps"]:.2f}, so traders are judging the market reaction rather than waiting for the catalyst.')
        else:
            parts.append('The earnings event recently hit the tape, so the main question is whether the market is accepting the result or still repricing it.')
    else:
        if event.get('eps_estimate') is not None:
            parts.append(f'The next report is on deck with Street EPS estimate at {event["eps_estimate"]:.2f}, so traders will watch whether price is leaning into the print too early.')
        else:
            parts.append('An earnings date is coming up, so the key setup question is whether the stock is coiling for expansion or already extended into the event.')

    if change_pct is not None and price is not None:
        direction = 'up' if change_pct >= 0 else 'down'
        parts.append(f'Shares are {direction} {abs(change_pct):.2f}% at about {price:.2f}, which gives a live read on how seriously the market is taking the setup.')

    parts.append(f'This name also matters for {theme_text}, so a clean reaction can spill over into related peers.')
    parts.append(f'Date source: {source_label}.')
    return ' '.join(parts)


def get_earnings_tracker(days_ahead: int = 21, limit: int = 120, lookback_days: int = 7) -> dict:
    now_utc = datetime.now(timezone.utc)
    market_now = now_utc.astimezone(EASTERN_TZ)
    start_dt = now_utc - timedelta(days=max(lookback_days, 1))
    end_dt = now_utc + timedelta(days=max(days_ahead, 1))

    tracked_universe = list(dict.fromkeys(STOCK_UNIVERSE + [ticker for tickers in THEMES.values() for ticker in tickers]))
    events_by_ticker = {item['ticker']: item for item in _extract_nasdaq_earnings_candidates(tracked_universe, start_dt, end_dt)}

    fallback_symbols = [ticker for ticker in tracked_universe if ticker not in events_by_ticker]
    with ThreadPoolExecutor(max_workers=min(len(fallback_symbols), 10) if fallback_symbols else 1) as executor:
        futures = {
            executor.submit(_fetch_single_earnings_event, ticker, start_dt, end_dt, now_utc): ticker
            for ticker in fallback_symbols
        }
        for future in as_completed(futures):
            try:
                event = future.result()
            except Exception as exc:
                LOGGER.warning('Earnings lookup failed for %s: %s', futures[future], exc)
                continue
            if event:
                events_by_ticker[event['ticker']] = event

    all_events = sorted(events_by_ticker.values(), key=lambda item: (
        abs((item['earnings_date'] - now_utc).total_seconds()),
        item['earnings_date'],
        item['ticker'],
    ))
    visible_events = all_events[:limit]
    quotes = _batch_fetch_quotes([item['ticker'] for item in visible_events])

    items = []
    for event in visible_events:
        ticker = event['ticker']
        quote_data = quotes.get(ticker, {})
        earnings_dt = event['earnings_date']
        days_until = (earnings_dt.astimezone(EASTERN_TZ).date() - market_now.date()).days
        themes = THEME_LOOKUP.get(ticker, [])
        status = 'Today' if days_until == 0 else ('Upcoming' if days_until > 0 else 'Recent')
        display = earnings_dt.astimezone(EASTERN_TZ).strftime('%a, %b %d %Y %I:%M %p ET')
        if event.get('report_time') in ('BMO', 'AMC', 'TNS'):
            display = f"{display} ({event['report_time']})"
        items.append({
            'ticker': ticker,
            'company_name': quote_data.get('long_name') or ticker,
            'earnings_date': earnings_dt.isoformat(),
            'earnings_date_display': display,
            'days_until': days_until,
            'status': status,
            'eps_estimate': event.get('eps_estimate'),
            'reported_eps': event.get('reported_eps'),
            'surprise_pct': event.get('surprise_pct'),
            'price': quote_data.get('price'),
            'change_pct': quote_data.get('change_pct'),
            'volume': quote_data.get('volume'),
            'avg_volume': quote_data.get('average_volume'),
            'rvol': round((quote_data.get('volume') or 0) / (quote_data.get('average_volume') or 1), 2) if quote_data.get('average_volume') else None,
            'market_cap': quote_data.get('market_cap'),
            'themes': themes,
            'quote_status': 'available' if quote_data else 'unavailable',
            'event_source': event.get('event_source'),
            'event_source_label': event.get('event_source_label'),
            'reasoning': _build_earnings_reasoning(event, quote_data, themes, now_utc),
        })

    theme_counts = {}
    for event in all_events:
        for theme in THEME_LOOKUP.get(event['ticker'], []):
            theme_counts[theme] = theme_counts.get(theme, 0) + 1
    top_theme = next(iter(sorted(theme_counts, key=theme_counts.get, reverse=True)), None) if theme_counts else None

    return {
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'summary': {
            'coverage_universe': len(tracked_universe),
            'total_events': len(all_events),
            'recent_count': sum(1 for event in all_events if (event['earnings_date'].astimezone(EASTERN_TZ).date() - market_now.date()).days < 0),
            'upcoming_count': sum(1 for event in all_events if (event['earnings_date'].astimezone(EASTERN_TZ).date() - market_now.date()).days >= 0),
            'today_count': sum(1 for event in all_events if (event['earnings_date'].astimezone(EASTERN_TZ).date() - market_now.date()).days == 0),
            'next_7_days': sum(1 for event in all_events if 0 <= (event['earnings_date'].astimezone(EASTERN_TZ).date() - market_now.date()).days <= 7),
            'with_live_quotes': sum(1 for item in items if item.get('price') is not None),
            'top_theme': top_theme,
        },
        'items': items,
    }

EASTERN_TZ = ZoneInfo('America/New_York')


def _session_field_name(session: str) -> str:
    return 'pre_market_change_pct' if str(session).lower() == 'pre' else 'post_market_change_pct'


def _session_price_name(session: str) -> str:
    return 'pre_market_price' if str(session).lower() == 'pre' else 'post_market_price'


def _session_label(session: str) -> str:
    return 'Pre-market' if str(session).lower() == 'pre' else 'Post-market'


def _session_cutoff_minutes(session: str) -> int:
    return 240 if str(session).lower() == 'pre' else 960


def _session_minutes_mask(index: pd.DatetimeIndex, session: str):
    minutes = index.hour * 60 + index.minute
    if str(session).lower() == 'pre':
        return (minutes >= 240) & (minutes < 570)
    return (minutes >= 960) & (minutes < 1200)


def _regular_minutes_mask(index: pd.DatetimeIndex):
    minutes = index.hour * 60 + index.minute
    return (minutes >= 570) & (minutes < 960)


def _fetch_extended_intraday_frame(symbol: str) -> pd.DataFrame:
    try:
        data = _request_json(
            YAHOO_CHART_URL.format(symbol=url_quote(symbol, safe='')),
            params={
                'range': '2d',
                'interval': '5m',
                'includePrePost': 'true',
                'events': 'div,splits',
            },
        )
        result = data.get('chart', {}).get('result') or []
        if not result:
            return pd.DataFrame()

        payload = result[0]
        timestamps = payload.get('timestamp') or []
        quotes = payload.get('indicators', {}).get('quote', [{}])[0]
        if not timestamps or not quotes:
            return pd.DataFrame()

        count = min(
            len(timestamps),
            len(quotes.get('open', timestamps)),
            len(quotes.get('high', timestamps)),
            len(quotes.get('low', timestamps)),
            len(quotes.get('close', timestamps)),
            len(quotes.get('volume', timestamps)),
        )
        if count == 0:
            return pd.DataFrame()

        frame = pd.DataFrame({
            'open': quotes.get('open', [])[:count],
            'high': quotes.get('high', [])[:count],
            'low': quotes.get('low', [])[:count],
            'close': quotes.get('close', [])[:count],
            'volume': quotes.get('volume', [])[:count],
        })
        frame['adjclose'] = frame['close']
        frame.index = pd.to_datetime(timestamps[:count], unit='s', utc=True).tz_convert(EASTERN_TZ)
        frame = frame.apply(pd.to_numeric, errors='coerce').dropna(subset=['close']).sort_index()
        return frame
    except Exception:
        return pd.DataFrame()


def _previous_regular_close(frame: pd.DataFrame, session: str, session_date) -> Optional[float]:
    if frame.empty:
        return None
    regular = frame[_regular_minutes_mask(frame.index)]
    if regular.empty:
        return None

    if str(session).lower() == 'pre':
        prior = regular[regular.index.date < session_date]
        if prior.empty:
            return None
        return _safe_float(prior['close'].iloc[-1])

    same_day = regular[regular.index.date == session_date]
    if not same_day.empty:
        return _safe_float(same_day['close'].iloc[-1])

    prior = regular[regular.index.date < session_date]
    if prior.empty:
        return None
    return _safe_float(prior['close'].iloc[-1])


def _resolve_session_rows(frame: pd.DataFrame, session: str, now_et: datetime) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()

    session_rows = frame[_session_minutes_mask(frame.index, session)]
    if session_rows.empty:
        return pd.DataFrame()

    available_dates = sorted({stamp.date() for stamp in session_rows.index if stamp.date() <= now_et.date()})
    if not available_dates:
        return pd.DataFrame()

    now_minutes = now_et.hour * 60 + now_et.minute
    cutoff = _session_cutoff_minutes(session)
    if now_minutes < cutoff:
        available_dates = [value for value in available_dates if value < now_et.date()]
        if not available_dates:
            return pd.DataFrame()

    target_date = available_dates[-1]
    return session_rows[session_rows.index.date == target_date]


def _fetch_session_snapshot(symbol: str, session: str) -> Optional[dict]:
    frame = _fetch_extended_intraday_frame(symbol)
    if frame.empty:
        return None

    now_et = datetime.now(timezone.utc).astimezone(EASTERN_TZ)
    session_rows = _resolve_session_rows(frame, session, now_et)
    if session_rows.empty:
        return None

    session_price = _safe_float(session_rows['close'].iloc[-1])
    session_date = session_rows.index[0].date()
    prev_close = _previous_regular_close(frame, session, session_date)
    if session_price is None or prev_close in (None, 0):
        return None

    session_pct = ((session_price - prev_close) / prev_close) * 100
    session_volume = _safe_int(session_rows['volume'].fillna(0).sum())
    return {
        'symbol': symbol,
        'session_price': _round_number(session_price),
        'session_pct': _round_number(session_pct),
        'session_volume': session_volume,
        'previous_close': _round_number(prev_close),
    }


def _batch_fetch_session_snapshots(symbols: List[str], session: str) -> dict:
    snapshot_map = {}
    if not symbols:
        return snapshot_map

    with ThreadPoolExecutor(max_workers=min(len(symbols), 10)) as executor:
        futures = {executor.submit(_fetch_session_snapshot, symbol, session): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                LOGGER.warning('Extended session fetch failed for %s: %s', symbol, exc)
                continue
            if result:
                snapshot_map[symbol] = result
    return snapshot_map


def _session_grade(item: dict, detail: dict) -> str:
    score = 0
    session_pct = abs(item.get('session_pct') or 0)
    short_interest = detail.get('short_interest') or 0
    float_shares = detail.get('float_shares')
    session_rvol = item.get('session_rvol')

    if item.get('has_verified_headline'):
        score += 2
    if session_pct >= 10:
        score += 2
    elif session_pct >= 6:
        score += 1

    if short_interest >= 15:
        score += 1
    if float_shares is not None and float_shares <= 80_000_000:
        score += 1
    if item.get('event_label') in ('Earnings', 'FDA / Clinical', 'Strategic / Demand', 'M&A'):
        score += 1
    if session_rvol is not None and session_rvol >= 0.1:
        score += 2
    elif session_rvol is not None and session_rvol >= 0.04:
        score += 1

    if score >= 6:
        return 'A'
    if score >= 4:
        return 'B'
    if score >= 2:
        return 'C'
    return 'D'


def _format_share_count(value) -> str:
    number = _safe_float(value)
    if number is None:
        return 'n/a'
    if number >= 1_000_000_000:
        return f'{number / 1_000_000_000:.2f}B'
    if number >= 1_000_000:
        return f'{number / 1_000_000:.1f}M'
    if number >= 1_000:
        return f'{number / 1_000:.1f}K'
    return f'{number:.0f}'


def _format_headline_stamp(value: Optional[str]) -> str:
    if not value:
        return 'Time unavailable'
    try:
        dt = datetime.fromisoformat(str(value).replace('Z', '+00:00')).astimezone(ZoneInfo('America/New_York'))
        return dt.strftime('%b %d, %I:%M %p ET').replace(' 0', ' ')
    except Exception:
        return str(value)


def _classify_session_catalyst(title: str) -> str:
    lowered = (title or '').lower()
    if any(term in lowered for term in ['earnings', 'eps', 'revenue', 'guidance', 'beat', 'miss', 'quarter', 'q1', 'q2', 'q3', 'q4']):
        return 'Earnings'
    if any(term in lowered for term in ['fda', 'approval', 'designation', 'trial', 'study', 'phase', 'patient', 'therapy', 'clinical']):
        return 'FDA / Clinical'
    if any(term in lowered for term in ['upgrade', 'downgrade', 'price target', 'target', 'initiat']):
        return 'Analyst'
    if any(term in lowered for term in ['deal', 'partnership', 'contract', 'order', 'customer', 'launch', 'expansion', 'distribution', 'investment', 'stake', 'nvidia']):
        return 'Strategic / Demand'
    if any(term in lowered for term in ['policy', 'government', 'tariff', 'regulation', 'senate', 'administration']):
        return 'Government Policy'
    if any(term in lowered for term in ['merger', 'acquisition', 'takeover', 'buyout']):
        return 'M&A'
    return 'Themes / Narratives'


def _build_session_analysis_blocks(item: dict, headline: Optional[dict], category: str) -> List[dict]:
    session_pct = item.get('session_pct') or 0
    session_rvol = item.get('session_rvol')
    short_interest = item.get('short_interest')
    float_shares = item.get('float_shares')
    direction = 'upside' if session_pct >= 0 else 'downside'
    volume_text = f'{session_rvol:.2f}x extended relative volume' if session_rvol is not None else 'unclear extended-volume confirmation'
    short_interest_text = f'{short_interest:.2f}% short interest' if short_interest is not None else 'limited short-interest visibility'
    float_text = _format_share_count(float_shares) if float_shares is not None else 'an unavailable float reading'
    blocks = [{
        'title': 'The Catalyst',
        'body': (
            f'{headline.get("title")} is the verified lead headline. Source: {headline.get("source")}. '
            f'Published {_format_headline_stamp(headline.get("published_at"))}.'
            if headline else
            'No verified headline was found, so this row should be treated as watchlist-only rather than a confirmed catalyst setup.'
        ),
    }]

    if category == 'Earnings':
        blocks.append({
            'title': 'The Beat / Surprise Factor',
            'body': 'The headline is earnings-linked, so the key question is whether the move reflects a real reset in forward estimates or only a one-print reaction. Watch for follow-through tied to beats, raised guidance, or stronger margin language.',
        })
        blocks.append({
            'title': 'The Growth / Momentum',
            'body': f'The stock is showing {session_pct:+.2f}% in the session with {volume_text}. If the open holds and the move stays supported versus the prior close, the tape is treating the result as a real repricing.',
        })
    elif category == 'FDA / Clinical':
        blocks.append({
            'title': 'The Fundamental Shift',
            'body': 'This kind of regulatory or clinical headline matters because it can change probability-weighted future cash flows, not just sentiment. The market is testing whether the data point materially improves the path for the lead asset.',
        })
        blocks.append({
            'title': 'The Statistical Edge',
            'body': f'Biotech moves with a verified catalyst often need both strong early volume and a tight float. Here the board is seeing {volume_text} with {float_text}, so size discipline still matters.',
        })
    elif category == 'Analyst':
        blocks.append({
            'title': 'Re-rating Potential',
            'body': 'Analyst-driven moves can sustain when they validate a broader valuation change already building in the tape. The key is whether other desks repeat the call and whether the market keeps paying for the theme after the open.',
        })
        blocks.append({
            'title': 'Explosiveness',
            'body': f'This setup combines {session_pct:+.2f}% session action with {short_interest_text}. That mix can fuel additional squeezing, but analyst-only catalysts usually fade faster than hard earnings or regulatory news if volume drops.',
        })
    elif category in ('Strategic / Demand', 'M&A'):
        blocks.append({
            'title': 'Demand Signal',
            'body': 'A strategic or demand headline matters when it changes the revenue path, customer quality, or scarcity premium around the name. The question is whether the article points to durable demand rather than a one-day narrative spike.',
        })
        blocks.append({
            'title': 'Explosiveness',
            'body': f'The move is being expressed through {volume_text}. If that keeps building into the cash open, the market is likely treating the headline as a genuine {direction} repricing rather than just a sympathy pop.',
        })
    else:
        blocks.append({
            'title': 'Tape Confirmation',
            'body': f'This is currently classified as {category}. Without a hard balance-sheet or earnings datapoint, the move needs volume confirmation and open-to-open follow-through to hold its {direction} profile.',
        })
        blocks.append({
            'title': 'Risk',
            'body': 'Headline-driven trades without a clean fundamental reset can fade quickly if the news is already fully understood or the first spike is mostly retail positioning.',
        })

    return blocks


def _perception_before(detail: dict) -> str:
    operating_margin = detail.get('operating_margin')
    gross_margin = detail.get('gross_margin')
    forward_pe = detail.get('forward_pe')
    revenue_growth = detail.get('revenue_growth')

    if operating_margin is not None and operating_margin <= 10:
        return 'a lower-margin execution story where upside depends on proving better mix and better margins'
    if gross_margin is not None and gross_margin >= 60:
        return 'a premium-margin platform where the market already expects quality to stay high'
    if revenue_growth is not None and revenue_growth >= 18 and forward_pe is not None and forward_pe >= 28:
        return 'a high-expectation growth story where the market is already paying for continued expansion'
    if forward_pe is not None and forward_pe <= 12:
        return 'a value-style setup where the market has been cautious on growth durability'
    return 'an execution-sensitive story where investors still need proof on growth, margins, or durability'


def _analyst_expectation(detail: dict) -> str:
    recommendation = (detail.get('recommendation') or '').lower()
    analyst_count = detail.get('analyst_count')
    price = detail.get('price')
    target = detail.get('target_mean_price')
    spread = None
    if price not in (None, 0) and target is not None:
        spread = ((target - price) / price) * 100

    rec_text = 'Analyst stance is mixed.'
    if recommendation in ('buy', 'strong_buy'):
        rec_text = 'The Street is leaning constructive.'
    elif recommendation in ('hold', 'neutral'):
        rec_text = 'The Street is mostly in wait-and-see mode.'
    elif recommendation in ('underperform', 'sell', 'strong_sell'):
        rec_text = 'The Street is leaning cautious.'

    target_text = 'Published target data is limited.'
    if spread is not None:
        if spread >= 12:
            target_text = f'Consensus target still sits about {spread:.1f}% above the current tape, so expectations still leave room for upside.'
        elif spread <= -8:
            target_text = f'The stock is trading roughly {abs(spread):.1f}% above consensus target, so expectations may already be rich.'
        else:
            target_text = 'The stock is trading near consensus target levels, so follow-through matters more than headline excitement.'

    coverage_text = f'{analyst_count} analysts are in the published set.' if analyst_count else 'Analyst coverage detail is thin.'
    return f'{rec_text} {target_text} {coverage_text}'


def _build_session_reasoning(item: dict, detail: dict, headlines: List[dict]) -> dict:
    session_pct = item.get('session_pct') or 0
    session_rvol = item.get('session_rvol')
    headline = headlines[0] if headlines else None
    perception_before = _perception_before(detail)
    analyst_view = _analyst_expectation(detail)

    if not headline:
        return {
            'has_verified_headline': False,
            'headline_title': None,
            'headline_source': None,
            'headline_url': None,
            'headline_published_at': None,
            'headline_label': 'No verified catalyst',
            'event_label': 'No verified catalyst',
            'perception_before': f'Before the move, the setup looked like {perception_before}.',
            'what_changed': 'Treat the tape as watchlist-only until a clean company-specific headline appears.',
            'market_view': 'The move may still matter, but it is not being explained by a verified catalyst feed right now.',
            'analyst_view': analyst_view,
            'reasoning': 'No verified catalyst was found in the live headline feed, so this move should be treated as tape-driven until a real source appears.',
            'analysis_blocks': _build_session_analysis_blocks(item, None, 'No verified catalyst'),
        }

    category = _classify_session_catalyst(headline.get('title') or '')
    source = headline.get('source') or 'News feed'
    published_at = headline.get('published_at')
    direction = 'higher' if session_pct >= 0 else 'lower'
    volume_line = ''
    if session_rvol is not None:
        volume_line = f' Extended volume is running at {session_rvol:.2f}x of the 20-day average, which helps separate a catalyst-driven move from a weak headline drift.'
    reasoning = (
        f'{item.get("ticker")} is trading {direction} after "{headline.get("title")}" ({source}). '
        f'This reads as a {category.lower()} catalyst rather than a generic tape move.{volume_line}'
    )
    market_view = (
        'If the open holds, the tape is treating this as a real repricing rather than a sympathy move.'
        if session_pct >= 0 else
        'If the weakness holds into the open, the market is treating the headline as a genuine de-risking event.'
    )
    return {
        'has_verified_headline': True,
        'headline_title': headline.get('title'),
        'headline_source': source,
        'headline_url': headline.get('url'),
        'headline_published_at': published_at,
        'headline_label': f'{source} | {_format_headline_stamp(published_at)}',
        'event_label': category,
        'perception_before': f'Before the move, the setup looked like {perception_before}.',
        'what_changed': f'Verified lead headline: "{headline.get("title")}".',
        'market_view': market_view,
        'analyst_view': analyst_view,
        'reasoning': reasoning,
        'analysis_blocks': _build_session_analysis_blocks(item, headline, category),
    }


def _session_seed_symbols(tracked_universe: List[str], quotes: dict, session: str, min_move: float, limit: int) -> List[str]:
    session_field = _session_field_name(session)
    direct = sorted(
        tracked_universe,
        key=lambda symbol: (
            -abs(_safe_float((quotes.get(symbol) or {}).get(session_field)) or 0),
            -(_safe_int((quotes.get(symbol) or {}).get('average_volume')) or 0),
            -(_safe_float((quotes.get(symbol) or {}).get('market_cap')) or 0),
        ),
    )
    liquid = sorted(
        tracked_universe,
        key=lambda symbol: (
            -(_safe_int((quotes.get(symbol) or {}).get('average_volume')) or 0),
            -(_safe_float((quotes.get(symbol) or {}).get('market_cap')) or 0),
        ),
    )

    desired = max(limit * 4, 36)
    seeds: List[str] = []
    for pool in (direct, liquid):
        for symbol in pool:
            quote_data = quotes.get(symbol) or {}
            direct_move = _safe_float(quote_data.get(session_field))
            daily_move = _safe_float(quote_data.get('change_pct'))
            average_volume = _safe_int(quote_data.get('average_volume')) or 0
            market_cap = _safe_float(quote_data.get('market_cap')) or 0
            if direct_move is None and average_volume < 250000 and market_cap < 250_000_000:
                continue
            if direct_move is None and abs(daily_move or 0) < max(min_move, 1.5):
                continue
            if symbol not in seeds:
                seeds.append(symbol)
            if len(seeds) >= desired:
                return seeds
    return seeds


def _build_proxy_session_candidates(tracked_universe: List[str], quotes: dict, session: str, min_move: float, limit: int) -> List[dict]:
    proxy_rows = []
    floor_move = max(min_move, 2.0)
    for ticker in tracked_universe:
        quote_data = quotes.get(ticker) or {}
        daily_move = _safe_float(quote_data.get('change_pct'))
        price = _safe_float(quote_data.get('price'))
        if daily_move is None or abs(daily_move) < floor_move or price in (None, 0):
            continue

        avg_volume = _safe_int(quote_data.get('average_volume'))
        volume = _safe_int(quote_data.get('volume'))
        proxy_rows.append({
            'ticker': ticker,
            'company_name': quote_data.get('long_name') or ticker,
            'session': session,
            'session_label': _session_label(session),
            'session_pct': _round_number(daily_move),
            'session_price': _round_number(price),
            'session_volume': volume,
            'session_rvol': round((volume or 0) / avg_volume, 2) if avg_volume else None,
            'price': _round_number(price),
            'change_pct': _round_number(daily_move),
            'volume': volume,
            'avg_volume': avg_volume,
            'rvol': round((volume or 0) / avg_volume, 2) if avg_volume else None,
            'market_cap': quote_data.get('market_cap'),
            'themes': THEME_LOOKUP.get(ticker, []),
            'quote_status': 'available',
            'session_source': 'daily_proxy',
        })

    return sorted(proxy_rows, key=lambda item: abs(item.get('session_pct') or 0), reverse=True)[:max(limit * 2, 12)]


def get_session_movers(session: str = 'pre', min_move: float = 0.5, limit: int = 15) -> dict:
    from news_fetcher import get_stock_news

    session = 'pre' if str(session).lower() != 'post' else 'post'
    tracked_universe = list(dict.fromkeys(STOCK_UNIVERSE + [ticker for tickers in THEMES.values() for ticker in tickers]))
    quotes = _batch_fetch_quotes(tracked_universe)

    priority_symbols = _session_seed_symbols(tracked_universe, quotes, session, min_move, limit)
    extended_map = _batch_fetch_session_snapshots(priority_symbols, session) if priority_symbols else {}

    candidates = []
    for ticker in tracked_universe:
        quote_data = quotes.get(ticker) or {}
        extended = extended_map.get(ticker) or {}
        session_pct = extended.get('session_pct')
        if session_pct is None:
            session_pct = quote_data.get(_session_field_name(session))
        session_price = extended.get('session_price')
        if session_price is None:
            session_price = quote_data.get(_session_price_name(session))

        if session_pct is None or abs(session_pct) < min_move:
            continue

        avg_volume = quote_data.get('average_volume')
        session_volume = extended.get('session_volume')
        session_rvol = round((session_volume or 0) / avg_volume, 2) if avg_volume else None
        candidates.append({
            'ticker': ticker,
            'company_name': quote_data.get('long_name') or ticker,
            'session': session,
            'session_label': _session_label(session),
            'session_pct': _round_number(session_pct),
            'session_price': _round_number(session_price),
            'session_volume': session_volume,
            'session_rvol': session_rvol,
            'price': quote_data.get('price'),
            'change_pct': quote_data.get('change_pct'),
            'volume': quote_data.get('volume'),
            'avg_volume': avg_volume,
            'rvol': round((quote_data.get('volume') or 0) / (avg_volume or 1), 2) if avg_volume else None,
            'market_cap': quote_data.get('market_cap'),
            'themes': THEME_LOOKUP.get(ticker, []),
            'quote_status': 'available',
            'session_source': 'extended' if extended else ('quote' if quote_data.get(_session_field_name(session)) is not None else 'daily'),
        })

    source_mode = 'live_session'
    if not candidates:
        candidates = _build_proxy_session_candidates(tracked_universe, quotes, session, min_move, limit)
        source_mode = 'daily_proxy'

    leaders = sorted(candidates, key=lambda item: item.get('session_pct') or 0, reverse=True)[:limit]
    laggards = sorted(candidates, key=lambda item: item.get('session_pct') or 0)[:limit]

    ticker_map = {}
    for item in leaders + laggards:
        ticker_map[item['ticker']] = dict(item)

    for ticker, item in ticker_map.items():
        detail = get_stock_detail(ticker)
        item['short_interest'] = detail.get('short_interest')
        item['float_shares'] = detail.get('float_shares')
        item['industry'] = detail.get('industry')
        item['sector'] = detail.get('sector')
        news_items = get_stock_news(ticker, company_name=item.get('company_name') or detail.get('company_name') or ticker, limit=6)
        headlines = [headline for headline in news_items if headline.get('verified')][:3]
        session_context = _build_session_reasoning(item, detail, headlines)
        item.update({
            'has_verified_headline': session_context.get('has_verified_headline'),
            'headline_title': session_context.get('headline_title'),
            'headline_source': session_context.get('headline_source'),
            'headline_url': session_context.get('headline_url'),
            'headline_published_at': session_context.get('headline_published_at'),
            'headline_label': session_context.get('headline_label'),
            'event_label': session_context.get('event_label'),
            'perception_before': session_context.get('perception_before'),
            'what_changed': session_context.get('what_changed'),
            'market_view': session_context.get('market_view'),
            'analyst_view': session_context.get('analyst_view'),
            'reasoning': session_context.get('reasoning'),
            'analysis_blocks': session_context.get('analysis_blocks') or [],
            'headline_summary': headlines[0].get('summary') if headlines else '',
            'verified_headline_count': len(headlines),
            'category': 'Proxy / Daily Momentum' if item.get('session_source') == 'daily_proxy' else session_context.get('event_label'),
        })
        item['grade'] = _session_grade(item, detail)

    verified_rows = [row for row in ticker_map.values() if row.get('has_verified_headline')]
    display_rows = verified_rows if verified_rows else list(ticker_map.values())
    leader_rows = sorted(display_rows, key=lambda row: row.get('session_pct') or 0, reverse=True)[:limit]
    laggard_rows = sorted(display_rows, key=lambda row: row.get('session_pct') or 0)[:limit]
    all_rows = sorted(display_rows, key=lambda row: abs(row.get('session_pct') or 0), reverse=True)

    return {
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'session': session,
        'summary': {
            'candidate_count': len(candidates),
            'matched_count': len(candidates),
            'verified_headline_count': len(verified_rows),
            'rendered_count': len(display_rows),
            'leaders_count': sum(1 for item in display_rows if (item.get('session_pct') or 0) > 0),
            'laggards_count': sum(1 for item in display_rows if (item.get('session_pct') or 0) < 0),
            'biggest_up': leader_rows[0]['ticker'] if leader_rows else None,
            'biggest_down': laggard_rows[0]['ticker'] if laggard_rows else None,
            'source_mode': source_mode,
        },
        'leaders': leader_rows,
        'laggards': laggard_rows,
        'all': all_rows,
    }
