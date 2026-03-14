import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from io import StringIO
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
SP500_WIKI_URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
SP500_CSV_FALLBACK_URL = 'https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv'
REQUEST_TIMEOUT = 12
QUOTE_BATCH_SIZE = 40
GROUP_ORDER = ['Broad Market', 'Style & Factors', 'Sectors', 'Rates & Credit', 'Commodities', 'International', 'Thematic', 'Digital Assets']
SP500_CONSTITUENT_CACHE_TTL = timedelta(hours=6)
SP500_HEATMAP_CACHE_TTL = timedelta(seconds=90)

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
})
LOGGER = logging.getLogger(__name__)
_SP500_CONSTITUENTS_CACHE = {
    'expires_at': datetime.fromtimestamp(0, tz=timezone.utc),
    'items': [],
}
_SP500_HEATMAP_CACHE = {
    'expires_at': datetime.fromtimestamp(0, tz=timezone.utc),
    'rows': [],
}

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


def _batch_fetch_quotes(symbols: List[str], allow_fallbacks: bool = True) -> dict:
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

    if not allow_fallbacks:
        return quote_map

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


def _normalize_rrg_series(series: pd.Series) -> pd.Series:
    if series.empty:
        return series

    normalized = series.copy()
    normalized.index = pd.DatetimeIndex(normalized.index).normalize()
    normalized = normalized[~normalized.index.duplicated(keep='last')]
    return normalized.sort_index()


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


def _normalize_sp500_columns(frame: pd.DataFrame) -> dict:
    lookup = {}
    for column in frame.columns:
        normalized = str(column).strip().lower().replace('\xa0', ' ')
        normalized = normalized.replace('-', ' ').replace('_', ' ')
        normalized = ' '.join(normalized.split())
        lookup[normalized] = column
    return lookup


def _rows_from_sp500_frame(frame: pd.DataFrame) -> List[dict]:
    columns = _normalize_sp500_columns(frame)
    symbol_col = columns.get('symbol')
    company_col = columns.get('security') or columns.get('name')
    sector_col = columns.get('gics sector') or columns.get('sector')
    sub_industry_col = columns.get('gics sub industry') or columns.get('sub industry')
    if not symbol_col or not company_col or not sector_col:
        return []

    seen = set()
    rows = []
    for record in frame.to_dict('records'):
        ticker = str(record.get(symbol_col) or '').strip().upper()
        if not ticker:
            continue
        ticker = ticker.replace('.', '-')
        if ticker in seen:
            continue
        seen.add(ticker)
        rows.append({
            'ticker': ticker,
            'company_name': str(record.get(company_col) or ticker).strip(),
            'sector': str(record.get(sector_col) or 'Unassigned').strip() or 'Unassigned',
            'sub_industry': str(record.get(sub_industry_col) or '').strip() or None,
        })
    return rows


def _fetch_sp500_constituents_from_wikipedia() -> List[dict]:
    response = SESSION.get(SP500_WIKI_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    for frame in pd.read_html(StringIO(response.text)):
        rows = _rows_from_sp500_frame(frame)
        if rows:
            return rows
    return []


def _fetch_sp500_constituents_from_csv() -> List[dict]:
    response = SESSION.get(SP500_CSV_FALLBACK_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    frame = pd.read_csv(StringIO(response.text))
    return _rows_from_sp500_frame(frame)


def _get_sp500_constituents(force_refresh: bool = False) -> List[dict]:
    now = datetime.now(timezone.utc)
    if not force_refresh and _SP500_CONSTITUENTS_CACHE['items'] and now < _SP500_CONSTITUENTS_CACHE['expires_at']:
        return _SP500_CONSTITUENTS_CACHE['items']

    loaders = (
        _fetch_sp500_constituents_from_wikipedia,
        _fetch_sp500_constituents_from_csv,
    )
    errors = []
    for loader in loaders:
        try:
            rows = loader()
        except Exception as exc:
            errors.append(f'{loader.__name__}: {exc}')
            continue
        if rows:
            _SP500_CONSTITUENTS_CACHE['items'] = rows
            _SP500_CONSTITUENTS_CACHE['expires_at'] = now + SP500_CONSTITUENT_CACHE_TTL
            return rows

    if _SP500_CONSTITUENTS_CACHE['items']:
        LOGGER.warning('Using stale S&P 500 constituents cache after refresh failure: %s', ' | '.join(errors))
        return _SP500_CONSTITUENTS_CACHE['items']

    LOGGER.warning('Unable to load S&P 500 constituents: %s', ' | '.join(errors) or 'unknown error')
    return []


def _sp500_row_from_quote(constituent: dict, quote_data: dict) -> dict:
    volume = _safe_int(quote_data.get('volume'))
    avg_volume = _safe_int(quote_data.get('average_volume'))
    regular_price = _safe_float(quote_data.get('price'))
    regular_change_pct = _safe_float(quote_data.get('change_pct'))
    extended_price = _safe_float(quote_data.get('extended_price'))
    extended_change_pct = _safe_float(quote_data.get('extended_change_pct'))
    display_price = extended_price if extended_price is not None else regular_price
    display_change_pct = extended_change_pct if extended_change_pct is not None else regular_change_pct
    rvol = round(volume / avg_volume, 2) if volume is not None and avg_volume not in (None, 0) else None

    return {
        'ticker': constituent['ticker'],
        'company_name': constituent['company_name'],
        'sector': constituent.get('sector') or 'Unassigned',
        'sub_industry': constituent.get('sub_industry'),
        'price': _round_number(regular_price),
        'display_price': _round_number(display_price),
        'change_pct': _round_number(regular_change_pct),
        'display_change_pct': _round_number(display_change_pct),
        'extended_price': _round_number(extended_price),
        'extended_change_pct': _round_number(extended_change_pct),
        'extended_session': quote_data.get('extended_session'),
        'volume': volume,
        'avg_volume': avg_volume,
        'rvol': rvol,
        'market_cap': _safe_float(quote_data.get('market_cap')),
        'market_state': quote_data.get('market_state'),
        'quote_status': 'available' if quote_data else 'unavailable',
    }


def _get_sp500_heatmap_rows(force_refresh: bool = False) -> List[dict]:
    now = datetime.now(timezone.utc)
    if not force_refresh and _SP500_HEATMAP_CACHE['rows'] and now < _SP500_HEATMAP_CACHE['expires_at']:
        return _SP500_HEATMAP_CACHE['rows']

    constituents = _get_sp500_constituents(force_refresh=force_refresh)
    if not constituents:
        return []

    symbols = [item['ticker'] for item in constituents]
    quotes = _batch_fetch_quotes(symbols, allow_fallbacks=False)
    quote_coverage = sum(1 for symbol in symbols if (quotes.get(symbol) or {}).get('price') is not None)
    minimum_fast_coverage = max(int(len(symbols) * 0.7), 350)
    if quote_coverage < minimum_fast_coverage:
        LOGGER.warning(
            'Fast S&P 500 quote coverage dropped to %s/%s, retrying with fallbacks.',
            quote_coverage,
            len(symbols),
        )
        quotes = _batch_fetch_quotes(symbols, allow_fallbacks=True)
    rows = [_sp500_row_from_quote(constituent, quotes.get(constituent['ticker']) or {}) for constituent in constituents]
    _SP500_HEATMAP_CACHE['rows'] = rows
    _SP500_HEATMAP_CACHE['expires_at'] = now + SP500_HEATMAP_CACHE_TTL
    return rows


def _weighted_change_pct(items: List[dict]) -> Optional[float]:
    weighted = [(item.get('display_change_pct'), item.get('market_cap')) for item in items if item.get('display_change_pct') is not None and item.get('market_cap')]
    if weighted:
        total_weight = sum(weight for _, weight in weighted)
        if total_weight:
            return round(sum(change * weight for change, weight in weighted) / total_weight, 2)

    valid = [item.get('display_change_pct') for item in items if item.get('display_change_pct') is not None]
    if not valid:
        return None
    return round(sum(valid) / len(valid), 2)


def get_sp500_heatmap() -> dict:
    rows = _get_sp500_heatmap_rows()
    if not rows:
        return {
            'updated_at': datetime.now(timezone.utc).isoformat(),
            'summary': {
                'total_constituents': 0,
                'quoted_count': 0,
                'advancers': 0,
                'decliners': 0,
                'flat': 0,
                'best_sector': None,
                'worst_sector': None,
                'biggest_up': None,
                'biggest_down': None,
            },
            'leaders': [],
            'laggards': [],
            'sectors': [],
            'error': 'Unable to load the S&P 500 heatmap right now.',
        }

    live_rows = [row for row in rows if row.get('display_change_pct') is not None]
    advancers = sum(1 for row in live_rows if (row.get('display_change_pct') or 0) > 0)
    decliners = sum(1 for row in live_rows if (row.get('display_change_pct') or 0) < 0)
    flat = len(live_rows) - advancers - decliners

    sector_map = {}
    for row in rows:
        sector_map.setdefault(row.get('sector') or 'Unassigned', []).append(row)

    sectors = []
    for sector_name, items in sector_map.items():
        sector_live_rows = [item for item in items if item.get('display_change_pct') is not None]
        total_market_cap = sum(item.get('market_cap') or 0 for item in items)
        sectors.append({
            'sector': sector_name,
            'avg_change_pct': _weighted_change_pct(items),
            'total_market_cap': total_market_cap or None,
            'advancers': sum(1 for item in sector_live_rows if (item.get('display_change_pct') or 0) > 0),
            'decliners': sum(1 for item in sector_live_rows if (item.get('display_change_pct') or 0) < 0),
            'flat': sum(1 for item in sector_live_rows if (item.get('display_change_pct') or 0) == 0),
            'quoted_count': len(sector_live_rows),
            'items': sorted(items, key=lambda item: (-(item.get('market_cap') or 0), item.get('ticker') or '')),
        })

    sectors.sort(key=lambda item: (-(item.get('total_market_cap') or 0), item.get('sector') or ''))
    ranked_sectors = sorted(
        [item for item in sectors if item.get('avg_change_pct') is not None],
        key=lambda item: item.get('avg_change_pct') or 0,
        reverse=True,
    )
    leaders = sorted(live_rows, key=lambda item: item.get('display_change_pct') or 0, reverse=True)[:12]
    laggards = sorted(live_rows, key=lambda item: item.get('display_change_pct') or 0)[:12]

    return {
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'summary': {
            'total_constituents': len(rows),
            'quoted_count': len(live_rows),
            'advancers': advancers,
            'decliners': decliners,
            'flat': flat,
            'best_sector': ranked_sectors[0]['sector'] if ranked_sectors else None,
            'worst_sector': ranked_sectors[-1]['sector'] if ranked_sectors else None,
            'biggest_up': leaders[0]['ticker'] if leaders else None,
            'biggest_down': laggards[0]['ticker'] if laggards else None,
        },
        'leaders': leaders,
        'laggards': laggards,
        'sectors': sectors,
    }


def _select_sp500_news_candidates(rows: List[dict], target_count: int = 24) -> List[dict]:
    liquid_rows = [row for row in rows if row.get('display_change_pct') is not None]
    movers = sorted(liquid_rows, key=lambda item: abs(item.get('display_change_pct') or 0), reverse=True)
    large_caps = sorted(liquid_rows, key=lambda item: item.get('market_cap') or 0, reverse=True)
    active = sorted(
        liquid_rows,
        key=lambda item: (
            -(item.get('rvol') or 0),
            -abs(item.get('display_change_pct') or 0),
            -(item.get('market_cap') or 0),
        ),
    )

    candidates = []
    seen = set()
    for pool in (movers[:16], large_caps[:10], active[:10]):
        for item in pool:
            ticker = item.get('ticker')
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            candidates.append(item)
            if len(candidates) >= target_count:
                return candidates
    return candidates


def _sp500_news_row(row: dict) -> Optional[dict]:
    from news_fetcher import get_stock_news

    headlines = get_stock_news(row['ticker'], company_name=row.get('company_name') or row['ticker'], limit=4)
    verified = [item for item in headlines if item.get('verified')]
    if not verified:
        return None

    headline = verified[0]
    return {
        'ticker': row['ticker'],
        'company_name': row.get('company_name') or row['ticker'],
        'sector': row.get('sector'),
        'sub_industry': row.get('sub_industry'),
        'display_price': row.get('display_price'),
        'display_change_pct': row.get('display_change_pct'),
        'change_pct': row.get('change_pct'),
        'extended_session': row.get('extended_session') or 'regular',
        'market_cap': row.get('market_cap'),
        'rvol': row.get('rvol'),
        'title': headline.get('title'),
        'source': headline.get('source'),
        'url': headline.get('url'),
        'published_at': headline.get('published_at'),
        'time': headline.get('time') or 0,
        'summary': headline.get('summary'),
        'verified': True,
        'match_score': headline.get('match_score'),
    }


def get_sp500_latest_news(limit: int = 18) -> dict:
    rows = _get_sp500_heatmap_rows()
    candidates = _select_sp500_news_candidates(rows)
    news_rows = []

    with ThreadPoolExecutor(max_workers=min(max(len(candidates), 1), 6)) as executor:
        futures = {executor.submit(_sp500_news_row, item): item for item in candidates}
        for future in as_completed(futures):
            try:
                item = future.result()
            except Exception as exc:
                ticker = futures[future].get('ticker')
                LOGGER.warning('Unable to load verified news for %s: %s', ticker, exc)
                continue
            if item:
                news_rows.append(item)

    news_rows.sort(key=lambda item: (-(item.get('time') or 0), -(abs(item.get('display_change_pct') or 0)), -(item.get('match_score') or 0)))
    trimmed = news_rows[:max(limit, 1)]
    sectors = sorted({item.get('sector') for item in trimmed if item.get('sector')})

    return {
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'summary': {
            'candidate_count': len(candidates),
            'rendered_count': len(trimmed),
            'sectors_represented': len(sectors),
            'latest_headline_at': trimmed[0].get('published_at') if trimmed else None,
            'biggest_move_with_news': max(trimmed, key=lambda item: abs(item.get('display_change_pct') or 0), default={}).get('ticker'),
        },
        'items': trimmed,
    }


def _watchlist_headline_view(headlines: List[dict], limit_per_ticker: int) -> tuple[List[dict], bool]:
    verified = [item for item in headlines if item.get('verified')]
    if verified:
        return verified[:limit_per_ticker], True
    return headlines[:limit_per_ticker], False


def _build_x_search_url(ticker: str, company_name: Optional[str] = None) -> str:
    query_terms = [f'${ticker}']
    if company_name:
        query_terms.append(f'"{company_name}"')
    query_terms.append('lang:en')
    query = ' OR '.join(query_terms[:2]) if company_name else query_terms[0]
    query = f'{query} lang:en'
    return f'https://x.com/search?q={url_quote(query, safe="")}&src=typed_query&f=live'


def _contains_any(text: str, terms: List[str]) -> bool:
    lower = str(text or '').lower()
    return any(term in lower for term in terms)


def _headline_text_blob(headlines: List[dict]) -> str:
    return ' '.join(
        f"{item.get('title') or ''} {item.get('summary') or ''}"
        for item in headlines[:5]
    ).lower()


def _live_quote_view(data: dict) -> dict:
    pre_price = data.get('premarket_price')
    if pre_price is None:
        pre_price = data.get('pre_market_price')
    pre_change = data.get('premarket_pct')
    if pre_change is None:
        pre_change = data.get('pre_market_change_pct')

    post_price = data.get('postmarket_price')
    if post_price is None:
        post_price = data.get('post_market_price')
    post_change = data.get('postmarket_pct')
    if post_change is None:
        post_change = data.get('post_market_change_pct')

    regular_price = data.get('price')
    regular_change = data.get('change_pct')

    display_price = regular_price
    display_change = regular_change
    session = '1D'
    session_label = 'Regular'

    if pre_price is not None:
        display_price = pre_price
        display_change = pre_change
        session = 'PM'
        session_label = 'Pre-market'
    elif post_price is not None:
        display_price = post_price
        display_change = post_change
        session = 'AH'
        session_label = 'After-hours'

    return {
        'display_price': display_price,
        'display_change_pct': display_change,
        'display_session': session,
        'display_session_label': session_label,
        'regular_price': regular_price,
        'regular_change_pct': regular_change,
        'pre_market_price': pre_price,
        'pre_market_change_pct': pre_change,
        'post_market_price': post_price,
        'post_market_change_pct': post_change,
    }


def _theme_driver_text(themes: List[str], sector: Optional[str], industry: Optional[str]) -> str:
    theme_text = ' '.join(themes or []).lower()
    sector_text = str(sector or '').lower()
    industry_text = str(industry or '').lower()

    if 'cyber' in theme_text or 'security' in theme_text:
        return 'durable security spending and higher platform attach rates'
    if 'cloud' in theme_text or 'saas' in theme_text:
        return 'enterprise software durability plus new AI upsell'
    if 'semiconductor' in theme_text or 'data center' in theme_text or 'ai' in theme_text:
        return 'AI infrastructure demand and better operating leverage'
    if 'defense' in theme_text or 'aerospace' in theme_text:
        return 'budget resilience, backlog durability, and multi-year demand visibility'
    if 'biotech' in theme_text or 'pharma' in theme_text:
        return 'pipeline progress and cleaner commercial execution'
    if 'ev' in theme_text or 'clean energy' in theme_text:
        return 'new product-cycle upside and longer-dated optionality'
    if 'crypto' in theme_text:
        return 'crypto-linked operating leverage and a friendlier risk backdrop'
    if 'oil' in theme_text or 'gas' in theme_text or 'energy' in theme_text:
        return 'commodity support and capital discipline'
    if 'software' in industry_text or sector_text == 'technology':
        return 'better software monetization and cleaner estimate revisions'
    if sector_text == 'healthcare':
        return 'better growth durability and margin quality'
    if sector_text == 'industrials':
        return 'order visibility and execution against backlog'
    return 'a cleaner multi-quarter growth and execution story'


def _watchlist_stance(detail: dict) -> str:
    recommendation = str(detail.get('recommendation') or '').lower()
    revenue_growth = detail.get('revenue_growth') or 0
    earnings_growth = detail.get('earnings_growth') or 0
    forward_pe = detail.get('forward_pe')

    if recommendation in ('sell', 'strong_sell', 'underperform'):
        return 'Cautious'
    if recommendation in ('buy', 'strong_buy') and (revenue_growth >= 12 or earnings_growth >= 15):
        return 'Bullish'
    if forward_pe is not None and forward_pe >= 45 and revenue_growth < 15:
        return 'Cautious'
    return 'Neutral'


def _watchlist_market_pricing_for(detail: dict, headlines: List[dict], themes: List[str]) -> str:
    company_name = detail.get('company_name') or detail.get('ticker') or 'This stock'
    driver_text = _theme_driver_text(themes, detail.get('sector'), detail.get('industry'))
    spread = _target_spread(detail)
    headline_blob = _headline_text_blob(headlines)

    if _contains_any(headline_blob, ['guidance', 'raise', 'beat', 'strong demand', 'record']):
        tail = 'Fresh verified headline flow is reinforcing that upside case instead of cooling it off.'
    elif _contains_any(headline_blob, ['miss', 'cut', 'slowdown', 'probe', 'investigation', 'delay']):
        tail = 'Recent headline flow has introduced enough friction that the market is demanding proof, not just the story.'
    elif spread is not None and spread >= 12:
        tail = f'Consensus target still sits about {spread:.1f}% above spot, so the Street is still underwriting that optionality.'
    else:
        tail = 'That means the tape is reacting more to the next few quarters than to the last quarter alone.'

    return f'The market is increasingly valuing {company_name} for {driver_text} rather than just a steady-state business. {tail}'


def _watchlist_core_thesis(detail: dict, headlines: List[dict], quote_view: dict) -> str:
    company_name = detail.get('company_name') or detail.get('ticker') or 'The company'
    growth_bits = []
    if detail.get('revenue_growth') is not None:
        growth_bits.append(f"revenue growth around {detail['revenue_growth']:.1f}%")
    if detail.get('earnings_growth') is not None:
        growth_bits.append(f"earnings growth around {detail['earnings_growth']:.1f}%")
    if detail.get('operating_margin') is not None:
        growth_bits.append(f"{detail['operating_margin']:.1f}% operating margin")

    live_bits = []
    if quote_view.get('display_price') is not None:
        live_bits.append(f"shares are trading near {quote_view['display_price']:.2f}")
    if quote_view.get('display_change_pct') is not None:
        live_bits.append(f"with a {quote_view['display_change_pct']:+.2f}% {quote_view['display_session'].lower()} move")

    lead_headline = headlines[0] if headlines else None
    headline_line = ''
    if lead_headline:
        headline_line = f' The latest verified headline is "{lead_headline.get("title")}", which keeps the narrative anchored to a real company-specific catalyst.'

    growth_text = ', '.join(growth_bits) if growth_bits else 'limited published growth data'
    live_text = ' '.join(live_bits) if live_bits else 'the live tape needs a refresh'
    return f'{company_name} currently screens like {_perception_before(detail)}. Published fundamentals show {growth_text}, and {live_text}.{headline_line}'


def _watchlist_pillars(detail: dict, headlines: List[dict], quote_view: dict, themes: List[str]) -> List[dict]:
    driver_text = _theme_driver_text(themes, detail.get('sector'), detail.get('industry'))
    short_interest = detail.get('short_interest')
    rvol = detail.get('rvol')
    spread = _target_spread(detail)

    tape_line = []
    if quote_view.get('display_change_pct') is not None:
        tape_line.append(f"the latest move is {quote_view['display_change_pct']:+.2f}%")
    if rvol is not None:
        tape_line.append(f"relative volume is {rvol:.2f}x")
    if short_interest is not None:
        tape_line.append(f"short interest is {short_interest:.1f}%")

    valuation_body = _valuation_frame(detail)
    if spread is not None and spread >= 10:
        valuation_body = f'Consensus target still sits about {spread:.1f}% above spot. {valuation_body}'

    return [
        {
            'title': 'Core Thesis',
            'body': f'The bull case rests on {driver_text}. The key is whether investors keep paying for the next leg of the story instead of fading back to a plain-vanilla sector multiple.',
        },
        {
            'title': 'Fundamental Reality',
            'body': _fundamental_snapshot(detail),
        },
        {
            'title': 'Tape Check',
            'body': ('Right now ' + ', '.join(tape_line) + '.')
            if tape_line else
            'Price, volume, and positioning are not offering a clean tape read yet, so follow-through matters more than the opening impression.',
        },
        {
            'title': 'Valuation Frame',
            'body': valuation_body,
        },
    ]


def _watchlist_catalysts(headlines: List[dict]) -> List[dict]:
    catalysts = []
    for headline in headlines[:3]:
        summary = (headline.get('summary') or '').strip()
        note = summary[:220] if summary else 'Fresh matched headline returned without a long publisher summary.'
        catalysts.append({
            'title': headline.get('title') or 'Headline',
            'source': headline.get('source') or 'News feed',
            'published_at': headline.get('published_at'),
            'note': note,
            'url': headline.get('url'),
            'verified': bool(headline.get('verified')),
        })
    return catalysts


def _watchlist_risks(detail: dict, headlines: List[dict]) -> List[str]:
    risks = [_risk_invalidation_read('Strategic / Demand', detail)]
    headline_blob = _headline_text_blob(headlines)
    if _contains_any(headline_blob, ['cut', 'miss', 'slowdown', 'delay', 'probe', 'investigation']):
        risks.append('Recent headline flow has at least one visible risk flag in it, so the market may stay skeptical until the next clean company update resolves that doubt.')
    if detail.get('forward_pe') is not None and detail['forward_pe'] >= 35:
        risks.append(f'At roughly {detail["forward_pe"]:.1f}x forward earnings, the multiple already assumes quality, so execution misses can compress the stock quickly.')
    elif detail.get('operating_margin') is not None and detail['operating_margin'] <= 10:
        risks.append('Margins are still low enough that investors may treat this as an execution story until profitability improves materially.')
    else:
        risks.append('If the next catalyst does not improve estimates, the stock can drift back into a range even if the long-term narrative still sounds attractive.')
    return risks[:3]


def _watchlist_decision_frame(detail: dict, quote_view: dict) -> str:
    company_name = detail.get('company_name') or detail.get('ticker') or 'The stock'
    if quote_view.get('display_change_pct') is not None and quote_view['display_change_pct'] >= 4:
        return f'{company_name} is already getting a meaningful live tape response, so the next question is whether that move can hold once the first headline reaction fades.'
    if quote_view.get('display_change_pct') is not None and quote_view['display_change_pct'] <= -4:
        return f'{company_name} is under pressure in the live tape, so the real test is whether bad news is now priced in or whether the story is still deteriorating.'
    return f'{company_name} still needs a cleaner trigger. Treat this as a prepared thesis card: know the narrative, watch the next catalyst, and let price decide whether the story is being accepted or rejected.'


def _build_watchlist_item(ticker: str, limit_per_ticker: int) -> dict:
    from news_fetcher import get_stock_news

    detail = get_stock_detail(ticker)
    company_name = detail.get('company_name') or ticker
    headlines = get_stock_news(ticker, company_name=company_name, limit=max(limit_per_ticker * 2, 6))
    display_headlines, has_verified = _watchlist_headline_view(headlines, limit_per_ticker)
    quote_view = _live_quote_view(detail)
    themes = detail.get('themes') or []
    catalysts = _watchlist_catalysts(display_headlines)
    pillars = _watchlist_pillars(detail, display_headlines, quote_view, themes)

    return {
        'ticker': ticker,
        'company_name': company_name,
        'price': quote_view.get('display_price'),
        'change_pct': quote_view.get('display_change_pct'),
        'regular_change_pct': detail.get('change_pct'),
        'regular_price': detail.get('price'),
        'display_session': quote_view.get('display_session'),
        'display_session_label': quote_view.get('display_session_label'),
        'sector': detail.get('sector'),
        'industry': detail.get('industry'),
        'market_cap': detail.get('market_cap'),
        'short_interest': detail.get('short_interest'),
        'rvol': detail.get('rvol'),
        'stance': _watchlist_stance(detail),
        'themes': themes,
        'headline_mode': 'verified' if has_verified else 'recent',
        'has_verified_headline': has_verified,
        'headline_count': len(display_headlines),
        'latest_headline_at': display_headlines[0].get('published_at') if display_headlines else None,
        'x_search_url': _build_x_search_url(ticker, company_name),
        'what_market_is_pricing_for': _watchlist_market_pricing_for(detail, display_headlines, themes),
        'core_thesis': _watchlist_core_thesis(detail, display_headlines, quote_view),
        'pillars': pillars,
        'decision_frame': _watchlist_decision_frame(detail, quote_view),
        'catalysts': catalysts,
        'risks': _watchlist_risks(detail, display_headlines),
        'analyst_view': _analyst_expectation(detail),
        'fundamental_snapshot': _fundamental_snapshot(detail),
        'headlines': [
            {
                'title': item.get('title'),
                'source': item.get('source'),
                'url': item.get('url'),
                'published_at': item.get('published_at'),
                'summary': item.get('summary'),
                'verified': bool(item.get('verified')),
                'match_score': item.get('match_score'),
            }
            for item in display_headlines
        ],
    }


def get_watchlist_news(tickers: List[str], limit_per_ticker: int = 3) -> dict:
    clean_tickers = []
    seen = set()
    for raw in tickers:
        ticker = str(raw or '').strip().upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        clean_tickers.append(ticker)

    if not clean_tickers:
        return {
            'updated_at': datetime.now(timezone.utc).isoformat(),
            'summary': {
                'total_tickers': 0,
                'with_news_count': 0,
                'with_verified_news_count': 0,
                'latest_headline_at': None,
                'total_headlines': 0,
            },
            'items': [],
        }

    items = [None] * len(clean_tickers)
    with ThreadPoolExecutor(max_workers=min(len(clean_tickers), 6)) as executor:
        futures = {
            executor.submit(_build_watchlist_item, ticker, limit_per_ticker): index
            for index, ticker in enumerate(clean_tickers)
        }
        for future in as_completed(futures):
            index = futures[future]
            ticker = clean_tickers[index]
            try:
                items[index] = future.result()
            except Exception as exc:
                LOGGER.warning('Unable to build watchlist row for %s: %s', ticker, exc)
                items[index] = {
                    'ticker': ticker,
                    'company_name': ticker,
                    'price': None,
                    'change_pct': None,
                    'regular_change_pct': None,
                    'sector': None,
                    'industry': None,
                    'market_cap': None,
                    'short_interest': None,
                    'rvol': None,
                    'headline_mode': 'recent',
                    'has_verified_headline': False,
                    'headline_count': 0,
                    'latest_headline_at': None,
                    'x_search_url': _build_x_search_url(ticker, ticker),
                    'headlines': [],
                }

    latest_headline_at = None
    timestamps = [item.get('latest_headline_at') for item in items if item and item.get('latest_headline_at')]
    if timestamps:
        latest_headline_at = max(timestamps)

    return {
        'updated_at': datetime.now(timezone.utc).isoformat(),
            'summary': {
                'total_tickers': len(items),
                'with_news_count': sum(1 for item in items if item and item.get('headline_count')),
                'with_verified_news_count': sum(1 for item in items if item and item.get('has_verified_headline')),
                'latest_headline_at': latest_headline_at,
                'total_headlines': sum((item.get('headline_count') or 0) for item in items if item),
                'bullish_count': sum(1 for item in items if item and item.get('stance') == 'Bullish'),
                'cautious_count': sum(1 for item in items if item and item.get('stance') == 'Cautious'),
            },
        'items': items,
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

    benchmark_series = _normalize_rrg_series(history_map.get(benchmark_symbol, pd.Series(dtype='float64')))
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
        asset_series = _normalize_rrg_series(history_map.get(symbol, pd.Series(dtype='float64')))
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
    last_date = end_dt.astimezone(EASTERN_TZ).date()
    target_dates = []
    while current_date <= last_date:
        target_dates.append(current_date)
        current_date += timedelta(days=1)

    with ThreadPoolExecutor(max_workers=min(len(target_dates), 6) if target_dates else 1) as executor:
        futures = {executor.submit(_fetch_nasdaq_earnings_rows, target_date): target_date for target_date in target_dates}
        for future in as_completed(futures):
            target_date = futures[future]
            try:
                rows = future.result()
            except Exception:
                rows = []
            for row in rows:
                ticker = str(row.get('symbol') or row.get('ticker') or '').upper().strip()
                if not ticker or ticker not in tracked:
                    continue
                earnings_dt = _nasdaq_event_datetime(target_date, row)
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

def _earnings_lead_headline(headlines: List[dict]) -> Optional[dict]:
    terms = ['earnings', 'guidance', 'results', 'quarter', 'eps', 'revenue', 'outlook', 'profit']
    for headline in headlines:
        text = f"{headline.get('title') or ''} {headline.get('summary') or ''}"
        if _contains_any(text, terms):
            return headline
    return headlines[0] if headlines else None


def _criteria_item(label: str, passed, note: str) -> dict:
    return {
        'label': label,
        'passed': passed,
        'note': note,
    }


def _earnings_verdict(event: dict, quote_view: dict, headlines: List[dict]) -> str:
    surprise_pct = event.get('surprise_pct')
    reaction_pct = quote_view.get('display_change_pct')
    headline_blob = _headline_text_blob(headlines)

    if surprise_pct is not None and reaction_pct is not None:
        if surprise_pct >= 5 and reaction_pct >= 3:
            return 'better'
        if surprise_pct < 0 and reaction_pct <= -2:
            return 'worse'
        if surprise_pct >= 0 and reaction_pct <= -2:
            return 'mixed'
        if surprise_pct < 0 and reaction_pct >= 2:
            return 'mixed'

    if reaction_pct is not None:
        if reaction_pct >= 5:
            return 'better'
        if reaction_pct <= -5:
            return 'worse'

    if _contains_any(headline_blob, ['raise', 'record', 'strong demand', 'accelerat', 'backlog']):
        return 'better'
    if _contains_any(headline_blob, ['miss', 'cut', 'slowdown', 'pressure', 'declin', 'cannibal']):
        return 'worse'
    return 'mixed'


def _earnings_narrative_shift(event: dict, detail: dict, quote_view: dict, headlines: List[dict]) -> str:
    verdict = _earnings_verdict(event, quote_view, headlines)
    surprise_pct = event.get('surprise_pct')
    reaction_pct = quote_view.get('display_change_pct')

    if verdict == 'better':
        if surprise_pct is not None and reaction_pct is not None:
            return f'The quarter looks better than the prior setup: EPS surprise landed at {surprise_pct:+.2f}% and shares are reacting {reaction_pct:+.2f}% in {quote_view.get("display_session")}.'
        return 'The post-print read looks constructive: the market is leaning toward a real estimate reset instead of a one-day headline pop.'
    if verdict == 'worse':
        if surprise_pct is not None and reaction_pct is not None:
            return f'The report is being treated as worse than hoped: EPS surprise is {surprise_pct:+.2f}% and the live reaction is {reaction_pct:+.2f}% in {quote_view.get("display_session")}.'
        return 'The post-print read looks weaker: the market is de-risking rather than paying up for the next leg of the story.'
    return 'The quarter looks mixed: the headline numbers and the tape reaction are not pointing cleanly in the same direction yet.'


def _earnings_before_view(event: dict, detail: dict, themes: List[str]) -> str:
    company_name = detail.get('company_name') or detail.get('ticker') or 'The company'
    eps_estimate = event.get('eps_estimate')
    expectation = _analyst_expectation(detail)
    driver_text = _theme_driver_text(themes, detail.get('sector'), detail.get('industry'))

    estimate_line = f'Street EPS estimate sat near {eps_estimate:.2f}. ' if eps_estimate is not None else ''
    return (
        f'Before earnings, the market viewed {company_name} as {_perception_before(detail)}. '
        f'{estimate_line}The bull case going into the print rested on {driver_text}. '
        f'{expectation}'
    )


def _earnings_after_view(event: dict, detail: dict, quote_view: dict, headlines: List[dict]) -> str:
    verdict = _earnings_verdict(event, quote_view, headlines)
    lead_headline = _earnings_lead_headline(headlines)
    reaction_pct = quote_view.get('display_change_pct')
    surprise_pct = event.get('surprise_pct')

    surprise_line = ''
    if surprise_pct is not None:
        surprise_line = f'EPS surprise printed at {surprise_pct:+.2f}%. '
    reaction_line = ''
    if reaction_pct is not None:
        reaction_line = f'The live reaction is {reaction_pct:+.2f}% in {quote_view.get("display_session")}. '
    headline_line = ''
    if lead_headline:
        headline_line = (lead_headline.get('summary') or lead_headline.get('title') or '').strip()
        if headline_line:
            headline_line = f'Lead read: {headline_line[:240]}'

    if verdict == 'better':
        prefix = 'After earnings, the setup looks better than it did before the print.'
    elif verdict == 'worse':
        prefix = 'After earnings, the setup looks worse than the pre-report narrative.'
    else:
        prefix = 'After earnings, the setup still needs interpretation because the reaction is mixed.'

    return ' '.join(part for part in (prefix, surprise_line, reaction_line, headline_line) if part).strip()


def _earnings_what_they_said(event: dict, quote_view: dict, lead_headline: Optional[dict]) -> str:
    pieces = []
    if event.get('reported_eps') is not None and event.get('eps_estimate') is not None:
        pieces.append(f"Reported EPS {event['reported_eps']:.2f} versus {event['eps_estimate']:.2f} expected.")
    elif event.get('reported_eps') is not None:
        pieces.append(f"Reported EPS {event['reported_eps']:.2f}.")
    if event.get('surprise_pct') is not None:
        pieces.append(f"Surprise {event['surprise_pct']:+.2f}%.")
    if quote_view.get('display_price') is not None and quote_view.get('display_change_pct') is not None:
        pieces.append(
            f"Shares are trading near {quote_view['display_price']:.2f} with a {quote_view['display_change_pct']:+.2f}% {quote_view.get('display_session')} reaction."
        )
    if lead_headline:
        summary = (lead_headline.get('summary') or '').strip()
        if summary:
            pieces.append(summary[:240])
        elif lead_headline.get('title'):
            pieces.append(lead_headline['title'])
    return ' '.join(pieces) or 'The report is on deck, but a detailed company statement has not been parsed yet.'


def _earnings_ai_reasoning(event: dict, detail: dict, quote_view: dict, themes: List[str], headlines: List[dict]) -> str:
    company_name = detail.get('company_name') or detail.get('ticker') or 'The company'
    verdict = _earnings_verdict(event, quote_view, headlines)
    driver_text = _theme_driver_text(themes, detail.get('sector'), detail.get('industry'))
    valuation = _valuation_frame(detail)
    expectation = _analyst_expectation(detail)
    lead_headline = _earnings_lead_headline(headlines)
    headline_text = ''
    if lead_headline:
        headline_text = (lead_headline.get('summary') or lead_headline.get('title') or '').strip()
        headline_text = headline_text[:220]

    if verdict == 'better':
        opener = f'{company_name} looks like a better setup after the print because the market is starting to pay for {driver_text} instead of waiting for more proof.'
    elif verdict == 'worse':
        opener = f'{company_name} looks worse after the print because the market is treating the report as a threat to the prior rerating case around {driver_text}.'
    else:
        opener = f'{company_name} still looks mixed after the print: the quarter moved the story, but not enough to settle whether {driver_text} is truly improving or merely getting deferred.'

    middle = headline_text or _earnings_narrative_shift(event, detail, quote_view, headlines)
    return f'{opener} {middle} {valuation} {expectation}'


def _earnings_quallamaggie_criteria(event: dict, detail: dict, quote_view: dict, headlines: List[dict]) -> dict:
    reaction_pct = quote_view.get('display_change_pct')
    price = quote_view.get('display_price') or detail.get('price')
    surprise_pct = event.get('surprise_pct')
    rvol = detail.get('rvol')
    growth_ok = (detail.get('revenue_growth') or 0) >= 15 or (detail.get('earnings_growth') or 0) >= 25

    items = [
        _criteria_item('Gap / reaction > 10%', reaction_pct >= 10 if reaction_pct is not None else None, 'Needs a decisive post-print expansion, not just a modest drift.'),
        _criteria_item('Price above $5', price > 5 if price is not None else None, 'Avoids the lowest-quality penny-stock setups.'),
        _criteria_item('Strong catalyst', True if event.get('reported_eps') is not None or event.get('eps_estimate') is not None else None, 'An actual earnings event is present in the tracker.'),
        _criteria_item('Growth acceleration', growth_ok if detail.get('revenue_growth') is not None or detail.get('earnings_growth') is not None else None, 'Uses the published growth profile as a proxy for fundamental strength.'),
        _criteria_item('Meaningful analyst beat', surprise_pct >= 5 if surprise_pct is not None else None, 'Looks for a real EPS beat, not just an inline print.'),
        _criteria_item('Volume expansion > 2x', rvol >= 2 if rvol is not None else None, 'Uses current relative volume as the tape confirmation proxy.'),
        _criteria_item('Fresh headline support', bool(headlines), 'At least one matched company-specific headline is available to explain the move.'),
    ]
    passed_count = sum(1 for item in items if item['passed'] is True)
    applicable_count = sum(1 for item in items if item['passed'] is not None)
    return {
        'name': 'Quallamaggie EP Criteria',
        'passed_count': passed_count,
        'applicable_count': applicable_count,
        'items': items,
    }


def _earnings_stockbee_criteria(event: dict, detail: dict, quote_view: dict) -> dict:
    reaction_pct = quote_view.get('display_change_pct')
    price = quote_view.get('display_price') or detail.get('price')
    surprise_pct = event.get('surprise_pct')
    analyst_count = detail.get('analyst_count')
    rvol = detail.get('rvol')

    items = [
        _criteria_item('EPS surprise positive', surprise_pct >= 0 if surprise_pct is not None else None, 'Positive surprise is the simplest first filter.'),
        _criteria_item('Revenue growth > 5%', (detail.get('revenue_growth') or 0) >= 5 if detail.get('revenue_growth') is not None else None, 'Uses published growth data as the sales acceleration check.'),
        _criteria_item('Earnings growth > 25%', (detail.get('earnings_growth') or 0) >= 25 if detail.get('earnings_growth') is not None else None, 'Looks for meaningful operating leverage, not a token beat.'),
        _criteria_item('Volume > 3x average', rvol >= 3 if rvol is not None else None, 'Uses current relative volume as the liquidity confirmation.'),
        _criteria_item('Price reaction > 4%', reaction_pct >= 4 if reaction_pct is not None else None, 'Post-print reaction should be large enough to matter.'),
        _criteria_item('Neglected / lighter coverage', analyst_count <= 20 if analyst_count is not None else None, 'Uses analyst count as a rough proxy for crowding.'),
        _criteria_item('Price above $5', price > 5 if price is not None else None, 'Keeps the scanner in liquid names.'),
    ]
    passed_count = sum(1 for item in items if item['passed'] is True)
    applicable_count = sum(1 for item in items if item['passed'] is not None)
    return {
        'name': 'Stockbee EP Criteria',
        'passed_count': passed_count,
        'applicable_count': applicable_count,
        'items': items,
    }


def _build_earnings_reasoning(event: dict, detail: dict, quote_view: dict, themes: List[str], headlines: List[dict]) -> dict:
    lead_headline = _earnings_lead_headline(headlines)
    verdict = _earnings_verdict(event, quote_view, headlines)
    reaction_pct = quote_view.get('display_change_pct')
    analysis_seed = {
        'ticker': detail.get('ticker'),
        'session_pct': reaction_pct or 0,
        'session_rvol': detail.get('rvol'),
        'short_interest': detail.get('short_interest'),
        'float_shares': detail.get('float_shares'),
    }
    criteria_sets = [
        _earnings_quallamaggie_criteria(event, detail, quote_view, headlines),
        _earnings_stockbee_criteria(event, detail, quote_view),
    ]

    return {
        'narrative_shift': _earnings_narrative_shift(event, detail, quote_view, headlines),
        'before_earnings': _earnings_before_view(event, detail, themes),
        'after_earnings': _earnings_after_view(event, detail, quote_view, headlines),
        'what_they_said': _earnings_what_they_said(event, quote_view, lead_headline),
        'ai_reasoning': _earnings_ai_reasoning(event, detail, quote_view, themes, headlines),
        'verdict': verdict,
        'criteria_sets': criteria_sets,
        'analysis_blocks': _build_session_analysis_blocks(analysis_seed, detail, lead_headline, 'Earnings'),
        'lead_headline': lead_headline,
    }


def get_earnings_tracker(days_ahead: int = 21, limit: int = 120, lookback_days: int = 7) -> dict:
    from news_fetcher import get_stock_news

    now_utc = datetime.now(timezone.utc)
    market_now = now_utc.astimezone(EASTERN_TZ)
    start_dt = now_utc - timedelta(days=max(lookback_days, 1))
    end_dt = now_utc + timedelta(days=max(days_ahead, 1))

    tracked_universe = list(dict.fromkeys(STOCK_UNIVERSE + [ticker for tickers in THEMES.values() for ticker in tickers]))
    events_by_ticker = {item['ticker']: item for item in _extract_nasdaq_earnings_candidates(tracked_universe, start_dt, end_dt)}

    fallback_symbols = [] if events_by_ticker else tracked_universe[:40]
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
    details_map = {}
    if visible_events:
        with ThreadPoolExecutor(max_workers=min(len(visible_events), 6)) as executor:
            futures = {
                executor.submit(get_stock_detail, event['ticker']): event['ticker']
                for event in visible_events
            }
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    details_map[ticker] = future.result()
                except Exception as exc:
                    LOGGER.warning('Detail lookup failed for %s: %s', ticker, exc)
                    details_map[ticker] = {'ticker': ticker, 'company_name': ticker, 'themes': THEME_LOOKUP.get(ticker, [])}

    headlines_map = {}
    if visible_events:
        with ThreadPoolExecutor(max_workers=min(len(visible_events), 6)) as executor:
            futures = {}
            for event in visible_events:
                ticker = event['ticker']
                detail = details_map.get(ticker) or {}
                company_name = detail.get('company_name') or ticker
                futures[executor.submit(get_stock_news, ticker, company_name=company_name, limit=6)] = ticker
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    headlines_map[ticker] = future.result()
                except Exception as exc:
                    LOGGER.warning('Headline lookup failed for %s: %s', ticker, exc)
                    headlines_map[ticker] = []

    items = []
    for event in visible_events:
        ticker = event['ticker']
        quote_data = quotes.get(ticker, {})
        detail = dict(details_map.get(ticker) or {'ticker': ticker, 'company_name': ticker})
        detail.update({
            'ticker': ticker,
            'company_name': detail.get('company_name') or quote_data.get('long_name') or ticker,
            'price': quote_data.get('price') if quote_data.get('price') is not None else detail.get('price'),
            'change_pct': quote_data.get('change_pct') if quote_data.get('change_pct') is not None else detail.get('change_pct'),
            'pre_market_price': quote_data.get('pre_market_price'),
            'pre_market_change_pct': quote_data.get('pre_market_change_pct'),
            'post_market_price': quote_data.get('post_market_price'),
            'post_market_change_pct': quote_data.get('post_market_change_pct'),
            'market_cap': quote_data.get('market_cap') if quote_data.get('market_cap') is not None else detail.get('market_cap'),
            'volume': quote_data.get('volume') if quote_data.get('volume') is not None else detail.get('volume'),
            'avg_volume': quote_data.get('average_volume') if quote_data.get('average_volume') is not None else detail.get('avg_volume'),
        })
        if detail.get('volume') is not None and detail.get('avg_volume'):
            detail['rvol'] = round((detail.get('volume') or 0) / detail.get('avg_volume'), 2) if detail.get('avg_volume') else detail.get('rvol')
        quote_view = _live_quote_view(detail)
        earnings_dt = event['earnings_date']
        days_until = (earnings_dt.astimezone(EASTERN_TZ).date() - market_now.date()).days
        themes = detail.get('themes') or THEME_LOOKUP.get(ticker, [])
        status = 'Today' if days_until == 0 else ('Upcoming' if days_until > 0 else 'Recent')
        display = earnings_dt.astimezone(EASTERN_TZ).strftime('%a, %b %d %Y %I:%M %p ET')
        if event.get('report_time') in ('BMO', 'AMC', 'TNS'):
            display = f"{display} ({event['report_time']})"
        matched_headlines = headlines_map.get(ticker) or []
        verified_headlines = [headline for headline in matched_headlines if headline.get('verified')]
        lead_headlines = verified_headlines or matched_headlines
        reasoning = _build_earnings_reasoning(event, detail, quote_view, themes, lead_headlines)
        reaction_pct = quote_view.get('display_change_pct')
        criteria_passed = sum(group.get('passed_count', 0) for group in reasoning.get('criteria_sets', []))
        criteria_total = sum(group.get('applicable_count', 0) for group in reasoning.get('criteria_sets', []))
        items.append({
            'ticker': ticker,
            'company_name': detail.get('company_name') or quote_data.get('long_name') or ticker,
            'earnings_date': earnings_dt.isoformat(),
            'earnings_date_display': display,
            'report_time': event.get('report_time'),
            'days_until': days_until,
            'status': status,
            'eps_estimate': event.get('eps_estimate'),
            'reported_eps': event.get('reported_eps'),
            'surprise_pct': event.get('surprise_pct'),
            'price': detail.get('price'),
            'change_pct': detail.get('change_pct'),
            'display_price': quote_view.get('display_price'),
            'display_change_pct': reaction_pct,
            'display_session': quote_view.get('display_session'),
            'display_session_label': quote_view.get('display_session_label'),
            'volume': detail.get('volume'),
            'avg_volume': detail.get('avg_volume'),
            'rvol': detail.get('rvol'),
            'market_cap': detail.get('market_cap'),
            'themes': themes,
            'quote_status': 'available' if quote_data or detail.get('price') is not None else 'unavailable',
            'event_source': event.get('event_source'),
            'event_source_label': event.get('event_source_label'),
            'reaction_pct': reaction_pct,
            'reaction_label': quote_view.get('display_session'),
            'stance': reasoning.get('verdict'),
            'narrative_shift': reasoning.get('narrative_shift'),
            'before_earnings': reasoning.get('before_earnings'),
            'after_earnings': reasoning.get('after_earnings'),
            'what_they_said': reasoning.get('what_they_said'),
            'ai_reasoning': reasoning.get('ai_reasoning'),
            'reasoning': reasoning.get('ai_reasoning'),
            'analysis_blocks': reasoning.get('analysis_blocks') or [],
            'criteria_sets': reasoning.get('criteria_sets') or [],
            'criteria_score': f'{criteria_passed}/{criteria_total}' if criteria_total else 'n/a',
            'lead_headline': reasoning.get('lead_headline'),
            'headlines': lead_headlines[:3],
            'analyst_view': _analyst_expectation(detail),
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
            'reported_count': sum(1 for item in items if item.get('reported_eps') is not None),
            'with_positive_reaction': sum(1 for item in items if (item.get('reaction_pct') or 0) > 0),
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
    news_quality = str(item.get('news_quality') or '').lower()

    if item.get('has_verified_headline'):
        score += 2
    elif news_quality == 'high':
        score += 1
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
    if news_quality == 'high':
        score += 1
    if item.get('session_source') == 'daily_proxy':
        score = max(score - 1, 0)

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


def _join_clauses(items: List[str]) -> str:
    cleaned = [str(item).strip().rstrip('.') for item in items if item and str(item).strip()]
    if not cleaned:
        return ''
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f'{cleaned[0]} and {cleaned[1]}'
    return ', '.join(cleaned[:-1]) + f', and {cleaned[-1]}'


def _target_spread(detail: dict) -> Optional[float]:
    price = detail.get('price')
    target = detail.get('target_mean_price')
    if price in (None, 0) or target is None:
        return None
    try:
        return ((target - price) / price) * 100
    except Exception:
        return None


def _fundamental_snapshot(detail: dict) -> str:
    revenue_growth = detail.get('revenue_growth')
    earnings_growth = detail.get('earnings_growth')
    gross_margin = detail.get('gross_margin')
    operating_margin = detail.get('operating_margin')
    profit_margin = detail.get('profit_margin')
    forward_pe = detail.get('forward_pe')
    price_to_sales = detail.get('price_to_sales')

    pieces = []
    if revenue_growth is not None:
        if revenue_growth >= 0:
            pieces.append(f'revenue growth running near {revenue_growth:.1f}%')
        else:
            pieces.append(f'revenue still contracting about {abs(revenue_growth):.1f}%')
    if earnings_growth is not None:
        if earnings_growth >= 0:
            pieces.append(f'earnings growth near {earnings_growth:.1f}%')
        else:
            pieces.append(f'earnings still down roughly {abs(earnings_growth):.1f}%')
    if gross_margin is not None:
        pieces.append(f'gross margin around {gross_margin:.1f}%')
    if operating_margin is not None:
        pieces.append(f'operating margin near {operating_margin:.1f}%')
    if profit_margin is not None:
        pieces.append(f'net margin around {profit_margin:.1f}%')
    if forward_pe is not None:
        pieces.append(f'forward P/E near {forward_pe:.1f}x')
    elif price_to_sales is not None:
        pieces.append(f'price-to-sales near {price_to_sales:.1f}x')

    if not pieces:
        return 'Published fundamental detail is thin, so traders will have to lean more heavily on the verified headline, estimate changes, and raw tape confirmation.'
    return f'The broader setup still shows {_join_clauses(pieces[:5])}. That matters because the market will decide whether the headline changes this profile or only creates a one-session reaction.'


def _valuation_context(detail: dict) -> str:
    spread = _target_spread(detail)
    forward_pe = detail.get('forward_pe')
    price_to_sales = detail.get('price_to_sales')

    if spread is not None and spread >= 12:
        return f'Consensus target still sits about {spread:.1f}% above spot, so valuation still leaves room if the better story starts feeding into estimates.'
    if spread is not None and spread <= -8:
        return f'The stock is already trading roughly {abs(spread):.1f}% above consensus target, so upside probably needs stronger numbers than the Street currently publishes.'
    if forward_pe is not None and forward_pe >= 30:
        return f'At roughly {forward_pe:.1f}x forward earnings, the multiple already assumes a fair amount of quality, so the next leg needs more than a one-day headline.'
    if forward_pe is not None and forward_pe <= 12:
        return f'At only about {forward_pe:.1f}x forward earnings, the valuation can still rerate if execution and estimates inflect.'
    if price_to_sales is not None and price_to_sales >= 8:
        return f'At about {price_to_sales:.1f}x sales, investors will want cleaner proof that growth and margins can keep compounding.'
    return 'Valuation is not extreme either way, so follow-through will depend more on estimate revisions and whether institutions keep paying for the new narrative.'


def _catalyst_reset_read(category: str, detail: dict) -> str:
    if category == 'Earnings':
        return 'it reopens the debate around whether revenue is accelerating, margins are inflecting, and guidance deserves higher estimates'
    if category == 'FDA / Clinical':
        return 'it can materially change the probability-weighted value of the pipeline, financing flexibility, or partnering leverage'
    if category == 'Analyst':
        return 'it can start a broader valuation debate if other desks echo the call and the underlying numbers support higher targets'
    if category in ('Strategic / Demand', 'M&A'):
        return 'it can change the revenue path, customer mix, or scarcity premium if the announcement translates into real demand'
    if category == 'Government Policy':
        return 'it can alter demand, pricing, or cost assumptions if the policy detail turns into something operationally real'
    return 'the market is testing whether the narrative now deserves a different multiple rather than just a one-day sympathy move'


def _potential_catalyst_path(detail: dict, category: str) -> str:
    revenue_growth = detail.get('revenue_growth')
    earnings_growth = detail.get('earnings_growth')
    operating_margin = detail.get('operating_margin')
    gross_margin = detail.get('gross_margin')

    drivers = []
    if revenue_growth is None:
        drivers.append('clean evidence that revenue growth is accelerating')
    elif revenue_growth < 8:
        drivers.append(f'a visible revenue reacceleration from the current {revenue_growth:.1f}% pace')
    elif revenue_growth < 18:
        drivers.append(f'revenue growth stepping up from the current {revenue_growth:.1f}% rate')
    else:
        drivers.append(f'proof the current roughly {revenue_growth:.1f}% revenue growth can stay durable')

    if operating_margin is None:
        drivers.append('better margin conversion')
    elif operating_margin < 10:
        drivers.append(f'operating margin expansion from the current {operating_margin:.1f}% base')
    elif operating_margin < 20:
        drivers.append(f'further operating leverage above the current {operating_margin:.1f}% operating margin')
    else:
        drivers.append(f'sustained margin discipline around the current {operating_margin:.1f}% operating margin')

    if earnings_growth is not None:
        if earnings_growth < 0:
            drivers.append('an earnings-growth inflection back into positive territory')
        elif revenue_growth is not None and earnings_growth >= revenue_growth + 8:
            drivers.append('EPS growth continuing to outpace revenue, which would confirm leverage in the model')
        elif earnings_growth < 10:
            drivers.append('stronger bottom-line conversion after the top-line move')
    elif gross_margin is not None and gross_margin < 45:
        drivers.append(f'gross margin improvement from the current {gross_margin:.1f}% level')

    if category == 'Earnings':
        drivers.append('upward estimate revisions and better guidance credibility')
    elif category == 'Analyst':
        drivers.append('follow-on upgrades, higher targets, and confirmation from other desks')
    elif category in ('Strategic / Demand', 'M&A'):
        drivers.append('evidence the headline feeds into bookings, backlog, or better customer mix')
    elif category == 'FDA / Clinical':
        drivers.append('evidence the milestone improves commercialization odds rather than just sentiment')
    elif category == 'Government Policy':
        drivers.append('proof the policy shift turns into real demand, pricing power, or cost relief')
    else:
        drivers.append('continued follow-through after the open instead of a one-print spike')

    driver_text = _join_clauses(drivers[:4]) or 'cleaner fundamental proof and post-open confirmation'
    return f'For the move to keep rerating, the market likely needs {driver_text}. {_valuation_context(detail)}'


def _tape_confirmation_read(
    session_pct: float,
    volume_text: str,
    short_interest: Optional[float],
    short_interest_text: str,
    float_shares: Optional[float],
    float_text: str,
) -> str:
    pieces = [f'The tape is showing {session_pct:+.2f}% with {volume_text}.']
    if session_pct >= 0:
        pieces.append('If buyers keep defending the open and the move does not fully fade, that usually means the market is treating the news as a genuine repricing attempt.')
    else:
        pieces.append('If sellers keep control after the open, the market is treating the headline as a real de-risking event rather than a temporary shakeout.')

    if short_interest is not None and short_interest >= 10:
        pieces.append(f'{short_interest_text.capitalize()} can add fuel if price keeps pressing in the same direction.')
    elif float_shares is not None and float_shares <= 250_000_000:
        pieces.append(f'With {float_text} in float, supply is not unlimited, so a strong opening drive can become self-reinforcing.')
    else:
        pieces.append('If early volume fades fast, the move can still collapse back into a headline spike with no durable sponsorship.')
    return ' '.join(pieces)


def _risk_invalidation_read(category: str, detail: dict) -> str:
    revenue_growth = detail.get('revenue_growth')
    operating_margin = detail.get('operating_margin')
    if category == 'Earnings':
        return 'The main risk is that traders pay for the print before the market sees whether the better quarter really changes the next few estimates. If revenue growth rolls back over or margin expansion fails to hold, the rerating can stall quickly.'
    if category == 'FDA / Clinical':
        return 'The risk is that the headline improves sentiment more than economics. If financing, commercialization, or timeline questions remain unresolved, biotech can give back a sharp initial move very quickly.'
    if category == 'Analyst':
        return 'Single-desk upgrades can fade fast if the broader Street does not validate the call or if the stock was already priced near a full valuation. Without estimate revisions, the move can revert into noise.'
    if category in ('Strategic / Demand', 'M&A'):
        return 'The key risk is that the announcement sounds important but does not translate into durable bookings, pricing power, or margin benefit. If investors cannot model the economic impact, the rerating usually cools off.'
    if category == 'Government Policy':
        return 'Policy-driven trades can unwind when the market realizes the path from headline to actual earnings power is slower, smaller, or less direct than first assumed.'
    if revenue_growth is not None and revenue_growth < 8:
        return 'The risk is that the narrative improves before the underlying growth does. Without cleaner revenue acceleration and better post-open confirmation, the tape can lose interest quickly.'
    if operating_margin is not None and operating_margin <= 10:
        return 'The risk is that investors still see the stock as a low-margin execution story. If the company does not prove better mix or cost control, a higher multiple may not stick.'
    return 'The main risk is that the narrative sounds better than the actual reset in growth, margins, or estimates. If follow-through fades after the open, the move can revert into a tactical trade instead of a durable rerating.'


def _build_session_analysis_blocks(item: dict, detail: dict, headline: Optional[dict], category: str) -> List[dict]:
    session_pct = item.get('session_pct') or 0
    session_rvol = item.get('session_rvol')
    short_interest = item.get('short_interest')
    float_shares = item.get('float_shares')
    volume_text = f'{session_rvol:.2f}x extended relative volume' if session_rvol is not None else 'unclear extended-volume confirmation'
    short_interest_text = f'{short_interest:.2f}% short interest' if short_interest is not None else 'limited short-interest visibility'
    float_text = _format_share_count(float_shares) if float_shares is not None else 'an unavailable float reading'
    headline_summary = (headline.get('summary') or '').strip() if headline else ''
    blocks = [{
        'title': 'The Catalyst',
        'body': (
            f'{headline.get("title")} is the verified lead headline. Source: {headline.get("source")}. '
            f'{headline_summary} '
            f'Published {_format_headline_stamp(headline.get("published_at"))}.'
            if headline else
            'No verified headline was found, so this row should be treated as watchlist-only rather than a confirmed catalyst setup.'
        ),
    }]

    blocks.append({
        'title': 'Fundamental Setup',
        'body': _fundamental_snapshot(detail),
    })

    if not headline:
        blocks.append({
            'title': 'Potential Catalyst Path',
            'body': _potential_catalyst_path(detail, category),
        })
        blocks.append({
            'title': 'Risk / Invalidation',
            'body': 'Without a verified company-specific catalyst, the move should still be treated as tape-first. If no real headline appears, the action can fade quickly once positioning pressure eases.',
        })
        return blocks

    if category == 'Earnings':
        blocks.append({
            'title': 'Estimate Reset',
            'body': f'This headline matters because {_catalyst_reset_read(category, detail)}. The key question is whether the quarter changes how investors model the next few prints, not just whether the last quarter looked good.',
        })
    elif category == 'FDA / Clinical':
        blocks.append({
            'title': 'The Fundamental Shift',
            'body': f'This kind of catalyst matters because {_catalyst_reset_read(category, detail)}. The market is deciding whether the headline materially improves the path for the lead asset instead of just extending the story by a news cycle.',
        })
    elif category == 'Analyst':
        blocks.append({
            'title': 'Re-rating Potential',
            'body': f'The call matters because {_catalyst_reset_read(category, detail)}. Analyst-driven moves last longer when they are validating a fundamental change the market was already starting to notice.',
        })
    elif category in ('Strategic / Demand', 'M&A'):
        blocks.append({
            'title': 'Demand Signal',
            'body': f'The headline matters because {_catalyst_reset_read(category, detail)}. The real test is whether investors can connect the announcement to durable demand instead of a one-day narrative spike.',
        })
    elif category == 'Government Policy':
        blocks.append({
            'title': 'Policy Transmission',
            'body': f'This setup matters because {_catalyst_reset_read(category, detail)}. Policy headlines only hold when the market can see a believable path into demand, pricing, cost relief, or estimate changes.',
        })
    else:
        blocks.append({
            'title': 'Narrative Test',
            'body': f'This is currently classified as {category}. The market is effectively testing whether {_catalyst_reset_read(category, detail)}.',
        })

    blocks.append({
        'title': 'Potential Catalyst Path',
        'body': _potential_catalyst_path(detail, category),
    })
    blocks.append({
        'title': 'Tape Confirmation',
        'body': _tape_confirmation_read(session_pct, volume_text, short_interest, short_interest_text, float_shares, float_text),
    })
    blocks.append({
        'title': 'Risk / Invalidation',
        'body': _risk_invalidation_read(category, detail),
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
    revenue_growth = detail.get('revenue_growth')
    operating_margin = detail.get('operating_margin')
    spread = _target_spread(detail)

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

    quality_text = ''
    if revenue_growth is not None and operating_margin is not None:
        quality_text = f'The current profile shows about {revenue_growth:.1f}% revenue growth with {operating_margin:.1f}% operating margin, which helps frame how much improvement still needs to be proven.'
    elif revenue_growth is not None:
        quality_text = f'Revenue growth is running near {revenue_growth:.1f}%, so the next question is whether the headline can accelerate that pace.'
    elif operating_margin is not None:
        quality_text = f'Operating margin is around {operating_margin:.1f}%, so the rerating case still depends on whether profitability can improve from here.'

    coverage_text = f'{analyst_count} analysts are in the published set.' if analyst_count else 'Analyst coverage detail is thin.'
    return ' '.join(part for part in (rec_text, target_text, quality_text, coverage_text) if part)


def _headline_quality_label(headline: Optional[dict]) -> Optional[str]:
    if not headline:
        return None
    score = headline.get('match_score') or 0
    if score >= 20:
        return 'High'
    if score >= 12:
        return 'Medium'
    return 'Low'


def _build_session_reasoning(item: dict, detail: dict, headlines: List[dict]) -> dict:
    session_pct = item.get('session_pct') or 0
    session_rvol = item.get('session_rvol')
    headline = headlines[0] if headlines else None
    headline_summary = (headline.get('summary') or '').strip() if headline else ''
    news_quality = _headline_quality_label(headline)
    perception_before = _perception_before(detail)
    analyst_view = _analyst_expectation(detail)
    no_headline_path = _potential_catalyst_path(detail, 'No verified catalyst')
    session_source = item.get('session_source') or 'quote'
    label = item.get('session_label') or _session_label(item.get('session') or 'pre')
    source_read = (
        f'This read is coming from the actual {label.lower()} tape.'
        if session_source == 'extended' else
        'The move is being confirmed by the live quote feed.'
        if session_source == 'quote' else
        f'This row is still leaning on a daily proxy, so confidence stays lower until the live {label.lower()} tape confirms it.'
    )

    if not headline:
        reasoning_parts = [
            'No verified catalyst was found in the live headline feed, so this move should be treated as tape-driven until a real source appears.',
            source_read,
            no_headline_path,
        ]
        return {
            'has_verified_headline': False,
            'headline_title': None,
            'headline_source': None,
            'headline_url': None,
            'headline_published_at': None,
            'headline_summary': '',
            'headline_label': 'No verified catalyst',
            'news_quality': None,
            'event_label': 'No verified catalyst',
            'perception_before': f'Before the move, the setup looked like {perception_before}.',
            'what_changed': 'Treat the tape as watchlist-only until a clean company-specific headline appears.',
            'market_view': 'The move may still matter, but it is not being explained by a verified catalyst feed right now. Without a clean headline, investors will assume this is positioning until proven otherwise.',
            'analyst_view': analyst_view,
            'reasoning': ' '.join(part for part in reasoning_parts if part),
            'analysis_blocks': _build_session_analysis_blocks(item, detail, None, 'No verified catalyst'),
        }

    category = _classify_session_catalyst(headline.get('title') or '')
    source = headline.get('source') or 'News feed'
    published_at = headline.get('published_at')
    direction = 'higher' if session_pct >= 0 else 'lower'
    reset_read = _catalyst_reset_read(category, detail)
    catalyst_path = _potential_catalyst_path(detail, category)
    if session_pct >= 8:
        reaction = f'The {label.lower()} move is large enough to look like a genuine rerating attempt rather than a routine gap.'
    elif session_pct > 0:
        reaction = f'The {label.lower()} move is constructive, but it still needs the cash open to hold before traders will fully trust it.'
    elif session_pct <= -8:
        reaction = f'The {label.lower()} move is a hard de-risking move, so weak bounces are vulnerable unless price quickly repairs the damage.'
    else:
        reaction = f'The {label.lower()} move is negative, but it still needs follow-through after the open before it turns into a more durable breakdown read.'

    participation_parts = [source_read]
    if session_rvol is not None and session_rvol >= 0.1:
        participation_parts.append(f'Extended volume is already around {session_rvol:.2f}x a normal full-day baseline')
    elif session_rvol is not None and session_rvol >= 0.04:
        participation_parts.append(f'Extended volume has started to show up at about {session_rvol:.2f}x of a normal baseline')
    if item.get('float_shares') is not None and item.get('float_shares') <= 250_000_000:
        participation_parts.append(f'the float is only about {_format_share_count(item.get("float_shares"))}')
    if (item.get('short_interest') or 0) >= 12:
        participation_parts.append(f'short interest near {item.get("short_interest"):.1f}% adds squeeze sensitivity')
    participation = '. '.join(part.rstrip('.') for part in participation_parts if part)
    if participation:
        participation += '.'

    if session_source == 'daily_proxy':
        market_view = f'This still needs live {label.lower()} confirmation, so it is better treated as a watchlist candidate than a fully confirmed catalyst board name.'
    elif session_pct >= 0:
        market_view = f'If the open holds, the tape is treating this as a real repricing rather than a sympathy move. {catalyst_path}'
    else:
        market_view = f'If the weakness holds into the open, the market is treating the headline as a genuine de-risking event. {catalyst_path}'

    reasoning = ' '.join(part for part in (
        f'{item.get("ticker")} is trading {direction} after "{headline.get("title")}" ({source}).',
        f'This reads as a {category.lower()} catalyst because {reset_read}.',
        reaction,
        participation,
        catalyst_path,
    ) if part)
    return {
        'has_verified_headline': True,
        'headline_title': headline.get('title'),
        'headline_source': source,
        'headline_url': headline.get('url'),
        'headline_published_at': published_at,
        'headline_summary': headline_summary,
        'headline_label': f'{source} | {_format_headline_stamp(published_at)}',
        'news_quality': news_quality,
        'event_label': category,
        'perception_before': f'Before the move, the setup looked like {perception_before}.',
        'what_changed': f'Verified lead headline: "{headline.get("title")}". The new information matters because {reset_read}.',
        'market_view': market_view,
        'analyst_view': analyst_view,
        'reasoning': reasoning,
        'analysis_blocks': _build_session_analysis_blocks(item, detail, headline, category),
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

    desired = max(limit * 6, 80)
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
            if direct_move is None and abs(daily_move or 0) < max(min_move, 1.5) and average_volume < 2_000_000 and market_cap < 5_000_000_000:
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
            'headline_summary': session_context.get('headline_summary') or (headlines[0].get('summary') if headlines else ''),
            'news_quality': session_context.get('news_quality'),
            'event_label': session_context.get('event_label'),
            'perception_before': session_context.get('perception_before'),
            'what_changed': session_context.get('what_changed'),
            'market_view': session_context.get('market_view'),
            'analyst_view': session_context.get('analyst_view'),
            'reasoning': session_context.get('reasoning'),
            'analysis_blocks': session_context.get('analysis_blocks') or [],
            'news_items': news_items[:3],
            'verified_headline_count': len(headlines),
            'category': 'Proxy / Daily Momentum' if item.get('session_source') == 'daily_proxy' else session_context.get('event_label'),
        })
        item['grade'] = _session_grade(item, detail)

    verified_rows = [row for row in ticker_map.values() if row.get('has_verified_headline')]
    display_rows = verified_rows if verified_rows else list(ticker_map.values())
    leader_rows = sorted(display_rows, key=lambda row: row.get('session_pct') or 0, reverse=True)[:limit]
    laggard_rows = sorted(display_rows, key=lambda row: row.get('session_pct') or 0)[:limit]
    all_rows = sorted(
        display_rows,
        key=lambda row: (
            row.get('session_source') == 'daily_proxy',
            -abs(row.get('session_pct') or 0),
        ),
    )

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
