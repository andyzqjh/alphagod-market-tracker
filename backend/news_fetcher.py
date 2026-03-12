import html
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import List, Optional
from urllib.parse import urlparse
from xml.etree import ElementTree

import requests
import yfinance as yf

HEADERS = {'User-Agent': 'StockDashboard/1.0 (personal use)'}
REQUEST_TIMEOUT = 12
YAHOO_SEARCH_URL = 'https://query1.finance.yahoo.com/v1/finance/search'
GOOGLE_NEWS_URL = 'https://news.google.com/rss/search'
COMPANY_SUFFIX_TOKENS = {
    'inc', 'incorporated', 'corp', 'corporation', 'co', 'company', 'ltd', 'limited',
    'plc', 'se', 'sa', 'ag', 'nv', 'holdings', 'holding', 'group',
}
GENERIC_COMPANY_TOKENS = {
    'health', 'energy', 'technologies', 'technology', 'systems', 'therapeutics', 'pharma',
    'pharmaceuticals', 'software', 'infrastructure', 'services', 'industries',
    'communications', 'research', 'biotech', 'medical',
}
LOW_SIGNAL_HEADLINE_PATTERN = re.compile(
    r'(stocks to watch|social buzz|market today|equities mostly|futures edge|futures rise|'
    r'futures fall|live:|roundup|recap|wallstreetbets|movers:|stock market today)',
    re.IGNORECASE,
)
DOMAIN_SOURCE_MAP = {
    'finance.yahoo.com': 'Yahoo Finance',
    'fool.com': 'The Motley Fool',
    'nasdaq.com': 'Nasdaq',
    'benzinga.com': 'Benzinga',
    'businesswire.com': 'Business Wire',
    'globenewswire.com': 'GlobeNewswire',
    'seekingalpha.com': 'Seeking Alpha',
    'investors.com': "Investor's Business Daily",
    'marketwatch.com': 'MarketWatch',
    'investing.com': 'Investing.com',
    'prnewswire.com': 'PR Newswire',
    'zacks.com': 'Zacks',
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def _clean_text(value) -> str:
    if value is None:
        return ''
    text = html.unescape(str(value))
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def _trim_text(value, limit: int = 420) -> str:
    text = _clean_text(value)
    if len(text) <= limit:
        return text
    clipped = text[:limit - 3].rsplit(' ', 1)[0].rstrip(' ,;:')
    return (clipped or text[:limit - 3]).rstrip() + '...'


def _normalize_text(value: str) -> str:
    cleaned = _clean_text(value).lower().replace('&', ' and ')
    return re.sub(r'\s+', ' ', re.sub(r'[^a-z0-9]+', ' ', cleaned)).strip()


def _strip_company_suffixes(tokens: List[str]) -> List[str]:
    items = list(tokens)
    while items and items[-1] in COMPANY_SUFFIX_TOKENS:
        items.pop()
    return items


def _contains_normalized_term(text: str, term: str) -> bool:
    normalized_text = _normalize_text(text)
    normalized_term = _normalize_text(term)
    if not normalized_text or not normalized_term:
        return False
    return re.search(rf'(^| ){re.escape(normalized_term)}(?= |$)', normalized_text, re.IGNORECASE) is not None


def _contains_ticker_mention(text: str, symbol: str) -> bool:
    if not symbol:
        return False
    return re.search(rf'(^|[^A-Za-z0-9])\$?{re.escape(symbol)}(?=$|[^A-Za-z0-9])', str(text or ''), re.IGNORECASE) is not None


def _source_from_url(url: str) -> str:
    host = urlparse(url or '').netloc.lower().removeprefix('www.')
    if not host:
        return 'News feed'
    for suffix, label in DOMAIN_SOURCE_MAP.items():
        if host.endswith(suffix):
            return label
    parts = host.split('.')
    core = parts[-2] if len(parts) >= 2 else parts[0]
    return core.replace('-', ' ').title()


def _build_company_matcher(company_name: str) -> dict:
    normalized_name = _normalize_text(company_name)
    base_tokens = _strip_company_suffixes([token for token in normalized_name.split(' ') if token])
    core_name = ' '.join(base_tokens)
    distinctive_tokens = [token for token in base_tokens if len(token) >= 3 and token not in GENERIC_COMPANY_TOKENS]
    return {
        'normalized_name': normalized_name,
        'core_name': core_name,
        'distinctive_tokens': distinctive_tokens,
    }


def _parse_timestamp(value) -> tuple[Optional[str], Optional[int]]:
    if value in (None, ''):
        return None, None

    dt = None
    try:
        if isinstance(value, (int, float)) or str(value).isdigit():
            dt = datetime.fromtimestamp(int(value), tz=timezone.utc)
        else:
            try:
                dt = parsedate_to_datetime(str(value))
            except Exception:
                dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
    except Exception:
        return None, None

    return dt.isoformat(), int(dt.timestamp())


def _headline_age_hours(published_at: Optional[str]) -> Optional[float]:
    _, timestamp = _parse_timestamp(published_at)
    if timestamp is None:
        return None
    return (time.time() - timestamp) / 3600


def _is_fresh_headline(published_at: Optional[str], max_age_hours: int = 14 * 24) -> bool:
    age_hours = _headline_age_hours(published_at)
    return age_hours is not None and 0 <= age_hours <= max_age_hours


def _score_headline_recency(published_at: Optional[str]) -> int:
    age_hours = _headline_age_hours(published_at)
    if age_hours is None or age_hours < 0:
        return -6
    if age_hours <= 24:
        return 12
    if age_hours <= 72:
        return 9
    if age_hours <= 7 * 24:
        return 6
    if age_hours <= 14 * 24:
        return 3
    return -8


def _resolve_company_name(ticker: str, company_name: Optional[str]) -> str:
    if company_name:
        return company_name
    try:
        info = yf.Ticker(ticker).info or {}
        return info.get('longName') or info.get('shortName') or ticker
    except Exception:
        return ticker


def _analyze_headline_entity(item: dict, symbol: str, company_name: str) -> dict:
    title = _clean_text(item.get('title'))
    summary = _clean_text(item.get('summary') or '')
    combined_text = f'{title} {summary}'.strip()
    matcher = _build_company_matcher(company_name)
    related_tickers = [str(ticker).upper() for ticker in item.get('related_tickers') or []]
    ticker_in_title = _contains_ticker_mention(title, symbol)
    related_ticker_match = str(symbol or '').upper() in related_tickers
    core_name_match = _contains_normalized_term(combined_text, matcher['core_name']) if matcher['core_name'] else False
    token_hits = sum(1 for token in matcher['distinctive_tokens'] if _contains_normalized_term(combined_text, token))
    requires_company_confirmation = len(str(symbol or '')) <= 3

    return {
        'ticker_in_title': ticker_in_title,
        'related_ticker_match': related_ticker_match,
        'core_name_match': core_name_match,
        'token_hits': token_hits,
        'requires_company_confirmation': requires_company_confirmation,
        'strong_entity_match': core_name_match or token_hits >= 1 or (not requires_company_confirmation and ticker_in_title),
    }


def _score_headline_match(item: dict, symbol: str, company_name: str) -> int:
    title = _clean_text(item.get('title'))
    summary = _clean_text(item.get('summary') or '')
    lower_text = f'{title} {summary}'.lower()
    entity = _analyze_headline_entity(item, symbol, company_name)
    score = 0

    if entity['related_ticker_match']:
        score += 4
    if entity['ticker_in_title']:
        score += 3 if len(str(symbol or '')) <= 3 else 5
    if entity['core_name_match']:
        score += 6
    if entity['token_hits'] >= 2:
        score += 5
    elif entity['token_hits'] == 1:
        score += 3
    score += _score_headline_recency(item.get('published_at'))

    if re.search(r'(earnings|guidance|beat|miss|revenue|eps|results|profit|outlook)', lower_text):
        score += 4
    if re.search(r'(upgrade|downgrade|target|initiat|buy from|price objective)', lower_text):
        score += 3
    if re.search(r'(fda|approval|designation|trial|study|phase|clinical)', lower_text):
        score += 4
    if re.search(r'(deal|partnership|contract|investment|launch|order|acquisition|merger|stake|dividend)', lower_text):
        score += 4
    if LOW_SIGNAL_HEADLINE_PATTERN.search(title):
        score -= 8
    if re.search(r'(decreases stake|cuts holdings|boosts holdings|stock holdings|holdings in|holdings decreased|holds [0-9,]+ shares|acquires [0-9,]+ shares|sells [0-9,]+ shares|position in|institutional investors?)', lower_text):
        score -= 6
    if re.search(r'(stock is up today|signal more upside|what is|good stock to buy|wallstreetbets)', lower_text):
        score -= 4

    return score


def _is_verified_headline(item: dict, symbol: str, company_name: str) -> bool:
    entity = _analyze_headline_entity(item, symbol, company_name)
    if not _is_fresh_headline(item.get('published_at')):
        return False
    if LOW_SIGNAL_HEADLINE_PATTERN.search(str(item.get('title') or '')):
        return False
    if not entity['strong_entity_match']:
        return False
    if entity['requires_company_confirmation'] and not entity['core_name_match'] and entity['token_hits'] == 0:
        return False
    if entity['related_ticker_match'] and not entity['ticker_in_title'] and not entity['core_name_match'] and entity['token_hits'] == 0:
        return False
    return _score_headline_match(item, symbol, company_name) >= 6


def _normalize_yahoo_news(items: list, symbol: str, company_name: str) -> list:
    normalized = []
    for item in items:
        published_at, timestamp = _parse_timestamp(item.get('providerPublishTime'))
        thumbnail = ''
        if isinstance(item.get('thumbnail'), dict):
            resolutions = item['thumbnail'].get('resolutions', [])
            if resolutions:
                thumbnail = resolutions[0].get('url', '')

        content = item.get('content') if isinstance(item.get('content'), dict) else {}
        click_through = item.get('clickThroughUrl') if isinstance(item.get('clickThroughUrl'), dict) else {}
        url = item.get('link', '') or click_through.get('url', '')
        summary = (
            item.get('summary')
            or item.get('description')
            or content.get('summary')
            or content.get('description')
            or ''
        )

        normalized_item = {
            'type': 'news',
            'ticker': symbol,
            'title': _clean_text(item.get('title', '')),
            'source': item.get('publisher', '') or _source_from_url(url) or 'Yahoo Finance',
            'url': url,
            'time': timestamp or 0,
            'published_at': published_at,
            'thumbnail': thumbnail,
            'summary': _trim_text(summary),
            'related_tickers': item.get('relatedTickers') or [],
        }
        normalized_item['match_score'] = _score_headline_match(normalized_item, symbol, company_name)
        normalized_item['verified'] = _is_verified_headline(normalized_item, symbol, company_name)
        normalized.append(normalized_item)
    return normalized


def _parse_google_news_feed(xml_text: str) -> list:
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return []

    items = []
    for item_node in root.findall('.//item'):
        raw_title = (item_node.findtext('title') or '').strip()
        source_node = item_node.find('source')
        source = (source_node.text or '').strip() if source_node is not None else ''
        title_parts = raw_title.rsplit(' - ', 1)
        title = raw_title
        if len(title_parts) == 2 and not source:
            title = title_parts[0].strip()
            source = title_parts[1].strip()
        elif len(title_parts) == 2 and source and title_parts[1].strip().lower() == source.lower():
            title = title_parts[0].strip()

        published_at, timestamp = _parse_timestamp(item_node.findtext('pubDate'))
        items.append({
            'type': 'news',
            'title': _clean_text(title),
            'source': source or 'Google News',
            'url': (item_node.findtext('link') or '').strip(),
            'published_at': published_at,
            'time': timestamp or 0,
            'thumbnail': '',
            'summary': _trim_text(item_node.findtext('description') or ''),
            'related_tickers': [],
        })
    return items


def _fetch_google_news(symbol: str, company_name: str) -> list:
    query = f'"{company_name}" {symbol} stock'
    response = SESSION.get(
        GOOGLE_NEWS_URL,
        params={'q': query, 'hl': 'en-US', 'gl': 'US', 'ceid': 'US:en'},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    items = _parse_google_news_feed(response.text)
    for item in items:
        item['ticker'] = symbol
        item['match_score'] = _score_headline_match(item, symbol, company_name)
        item['verified'] = _is_verified_headline(item, symbol, company_name)
    return items


def _fetch_yahoo_search_news(symbol: str, company_name: str) -> list:
    response = SESSION.get(
        YAHOO_SEARCH_URL,
        params={'q': f'{symbol} {company_name}'},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json() or {}
    return payload.get('news') or []


def _parse_rss_feed(url: str, symbol: str) -> list:
    try:
        response = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        root = ElementTree.fromstring(response.text)
    except Exception:
        return []

    items = []
    for item_node in root.findall('./channel/item')[:24]:
        title = item_node.findtext('title') or ''
        link = item_node.findtext('link') or ''
        if not title or not link:
            continue
        items.append({
            'type': 'news',
            'ticker': symbol,
            'title': title,
            'source': item_node.findtext('source') or _source_from_url(link),
            'url': link,
            'time': item_node.findtext('pubDate') or item_node.findtext('date') or 0,
            'thumbnail': '',
            'summary': item_node.findtext('description') or '',
            'related_tickers': [],
        })
    return items


def _fetch_yahoo_rss_news(symbol: str) -> list:
    return _parse_rss_feed(
        f'https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US',
        symbol,
    )


def _fetch_nasdaq_rss_news(symbol: str) -> list:
    return _parse_rss_feed(f'https://www.nasdaq.com/feed/rssoutbound?symbol={symbol}', symbol)


def _normalize_feed_news(items: list, symbol: str, company_name: str) -> list:
    normalized = []
    for item in items:
        published_at, timestamp = _parse_timestamp(item.get('time') or item.get('published_at'))
        normalized_item = {
            'type': 'news',
            'ticker': symbol,
            'title': _clean_text(item.get('title', '')),
            'source': item.get('source') or _source_from_url(item.get('url', '')) or 'News feed',
            'url': item.get('url', ''),
            'time': timestamp or 0,
            'published_at': published_at,
            'thumbnail': item.get('thumbnail', ''),
            'summary': _trim_text(item.get('summary') or ''),
            'related_tickers': item.get('related_tickers') or [],
        }
        normalized_item['match_score'] = _score_headline_match(normalized_item, symbol, company_name)
        normalized_item['verified'] = _is_verified_headline(normalized_item, symbol, company_name)
        normalized.append(normalized_item)
    return normalized


def get_stock_news(ticker: str, company_name: Optional[str] = None, limit: int = 12) -> list:
    ticker = str(ticker or '').upper()
    if not ticker:
        return []

    company_name = _resolve_company_name(ticker, company_name)
    yahoo_items = []
    yahoo_search_items = []
    google_items = []
    rss_items = []

    try:
        yahoo_items = yf.Ticker(ticker).news or []
    except Exception:
        yahoo_items = []

    try:
        yahoo_search_items = _fetch_yahoo_search_news(ticker, company_name)
    except Exception:
        yahoo_search_items = []

    try:
        google_items = _fetch_google_news(ticker, company_name)
    except Exception:
        google_items = []

    try:
        rss_items.extend(_fetch_yahoo_rss_news(ticker))
    except Exception:
        pass

    try:
        rss_items.extend(_fetch_nasdaq_rss_news(ticker))
    except Exception:
        pass

    merged = (
        _normalize_yahoo_news(yahoo_items + yahoo_search_items, ticker, company_name)
        + google_items
        + _normalize_feed_news(rss_items, ticker, company_name)
    )
    merged = [item for item in merged if item.get('title') and item.get('url') and not LOW_SIGNAL_HEADLINE_PATTERN.search(str(item.get('title') or ''))]
    merged.sort(key=lambda item: (
        0 if item.get('verified') else 1,
        -(item.get('match_score') or 0),
        -(item.get('time') or 0),
    ))

    deduped = []
    seen = set()
    for item in merged:
        key = (str(item.get('title') or '').strip().lower(), str(item.get('url') or '').strip().lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    results = []
    for item in deduped:
        if not _is_fresh_headline(item.get('published_at')) and not item.get('verified'):
            continue
        results.append({
            'type': 'news',
            'ticker': ticker,
            'title': item.get('title', ''),
            'source': item.get('source', ''),
            'url': item.get('url', ''),
            'time': item.get('time', 0),
            'published_at': item.get('published_at'),
            'thumbnail': item.get('thumbnail', ''),
            'summary': _trim_text(item.get('summary') or '', limit=420),
            'verified': bool(item.get('verified')),
            'match_score': item.get('match_score'),
            'related_tickers': item.get('related_tickers') or [],
        })
        if len(results) >= limit:
            break

    return results


def get_reddit_posts(tickers: list) -> list:
    """Get recent Reddit posts mentioning given tickers (no API key needed)."""
    subs = ['wallstreetbets', 'stocks', 'investing', 'StockMarket']
    results = []
    for ticker in tickers[:6]:
        for sub in subs[:2]:
            try:
                url = f'https://www.reddit.com/r/{sub}/search.json?q={ticker}&sort=new&limit=5&t=week'
                response = SESSION.get(url, timeout=8)
                if response.status_code != 200:
                    continue
                posts = response.json().get('data', {}).get('children', [])
                for post in posts:
                    data = post.get('data', {})
                    title = data.get('title', '')
                    if str(ticker).upper() not in title.upper():
                        continue
                    results.append({
                        'type': 'reddit',
                        'ticker': ticker,
                        'subreddit': sub,
                        'title': title,
                        'score': data.get('score', 0),
                        'comments': data.get('num_comments', 0),
                        'url': 'https://reddit.com' + data.get('permalink', ''),
                        'time': int(data.get('created_utc', 0)),
                        'text': _trim_text(data.get('selftext', ''), limit=300),
                    })
            except Exception:
                pass
            time.sleep(0.3)

    results.sort(key=lambda item: item.get('time', 0), reverse=True)
    return results[:40]
