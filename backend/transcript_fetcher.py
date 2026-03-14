import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv()

ALPHA_VANTAGE_API_KEY = os.environ.get('ALPHA_VANTAGE_API_KEY')
FMP_API_KEY = os.environ.get('FMP_API_KEY')
REQUEST_TIMEOUT = 20
EASTERN_TZ = ZoneInfo('America/New_York')

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
})
LOGGER = logging.getLogger(__name__)

CATALYST_RULES = [
    (
        'Margin Inflection',
        ['margin', 'gross margin', 'operating margin', 'profitability', 'mix'],
        'If margins are structurally improving, the market can stop treating this as a low-quality business and start paying a higher multiple.',
    ),
    (
        'Guidance Reset',
        ['guidance', 'outlook', 'raise', 'reaffirm', 'accelerat', 'improv'],
        'Guidance and tone determine whether investors model this as a one-quarter beat or a multi-quarter estimate reset.',
    ),
    (
        'Demand / Pipeline',
        ['demand', 'pipeline', 'bookings', 'orders', 'backlog', 'arr', 'subscription'],
        'Stronger demand, pipeline, or ARR language can be the evidence that the current growth narrative is broadening rather than peaking.',
    ),
    (
        'AI / New Product',
        ['ai', 'gen ai', 'product', 'launch', 'platform', 'roadmap'],
        'New product or AI commentary matters when it can change the duration of growth or improve pricing power.',
    ),
    (
        'Cash Flow / Efficiency',
        ['free cash flow', 'cash flow', 'efficiency', 'productivity', 'expense', 'capex'],
        'Cash flow and efficiency comments matter because they can turn a story stock into a higher-quality compounder.',
    ),
]

MANAGEMENT_HINTS = (
    'chief', 'ceo', 'cfo', 'president', 'chairman', 'founder', 'coo',
    'cto', 'svp', 'evp', 'investor relations', 'vp', 'officer'
)


def _safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _request_json(url: str, params: Optional[dict] = None, headers: Optional[dict] = None):
    response = SESSION.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def _coerce_datetime(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y/%m/%d'):
        try:
            parsed = datetime.strptime(text[:19], fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _squash(text: str, limit: Optional[int] = None) -> str:
    cleaned = re.sub(r'\s+', ' ', str(text or '')).strip()
    if limit and len(cleaned) > limit:
        return cleaned[:limit - 3].rsplit(' ', 1)[0] + '...'
    return cleaned


def _calendar_quarter(dt: datetime) -> int:
    return ((dt.month - 1) // 3) + 1


def _shift_quarter(year: int, quarter: int, offset: int):
    index = (year * 4 + quarter - 1) + offset
    shifted_year = index // 4
    shifted_quarter = (index % 4) + 1
    return shifted_year, shifted_quarter


def _candidate_alpha_quarters(earnings_date) -> List[str]:
    base_dt = _coerce_datetime(earnings_date) or datetime.now(timezone.utc)
    year = base_dt.astimezone(EASTERN_TZ).year
    quarter = _calendar_quarter(base_dt.astimezone(EASTERN_TZ))
    candidates = []
    seen = set()
    for offset in (0, -1, 1, -2, 2, -3):
        y, q = _shift_quarter(year, quarter, offset)
        label = f'{y}Q{q}'
        if label in seen:
            continue
        seen.add(label)
        candidates.append(label)
    return candidates


def _speaker_entry(speaker: str, title: str, content: str, sentiment=None) -> Optional[dict]:
    text = _squash(content)
    if len(text) < 24:
        return None
    return {
        'speaker': _squash(speaker or 'Management', 80),
        'title': _squash(title or '', 120),
        'content': text,
        'sentiment': _safe_float(sentiment),
    }


def _entries_from_blob(text: str) -> List[dict]:
    blob = str(text or '').replace('\r', '\n')
    chunks = re.split(r'\n{2,}|(?=\n[A-Z][A-Za-z .,&\'/-]{1,80}:)', blob)
    entries = []
    for chunk in chunks:
        cleaned = _squash(chunk)
        if len(cleaned) < 24:
            continue
        if ':' in cleaned[:90]:
            speaker, content = cleaned.split(':', 1)
            if 1 <= len(speaker.strip()) <= 80:
                entry = _speaker_entry(speaker.strip(), '', content)
                if entry:
                    entries.append(entry)
                    continue
        entry = _speaker_entry('Management', '', cleaned)
        if entry:
            entries.append(entry)
    return entries


def _is_management(entry: dict) -> bool:
    combined = f"{entry.get('speaker') or ''} {entry.get('title') or ''}".lower()
    return any(term in combined for term in MANAGEMENT_HINTS)


def _excerpt(entries: List[dict], management_only: bool, limit: int = 6) -> str:
    filtered = [entry for entry in entries if _is_management(entry) == management_only]
    if not filtered:
        filtered = entries
    pieces = []
    for entry in filtered[:limit]:
        label = entry.get('speaker') or 'Management'
        title = f" ({entry.get('title')})" if entry.get('title') else ''
        pieces.append(f'{label}{title}: {_squash(entry.get("content"), 320)}')
    return '\n'.join(pieces)


def _digest(entries: List[dict], limit: int = 18) -> str:
    chosen = []
    management = [entry for entry in entries if _is_management(entry)]
    others = [entry for entry in entries if not _is_management(entry)]
    for entry in management[:10] + others[:8]:
        text = _squash(entry.get('content'), 420)
        if text:
            chosen.append(f'{entry.get("speaker") or "Speaker"}: {text}')
        if len(chosen) >= limit:
            break
    return '\n'.join(chosen)


def _extract_transcript_catalysts(entries: List[dict], limit: int = 5) -> List[dict]:
    scored = []
    seen_themes = set()
    for entry in entries:
        content = str(entry.get('content') or '')
        lower = content.lower()
        score = 0
        theme = 'Execution / Narrative'
        reason = 'Management is saying something that can change the market story if the next few quarters confirm it.'
        for label, keywords, why_it_matters in CATALYST_RULES:
            hits = sum(1 for keyword in keywords if keyword in lower)
            if hits:
                score += hits * 3
                if label not in seen_themes:
                    theme = label
                    reason = why_it_matters
        if _is_management(entry):
            score += 2
        sentiment = abs(_safe_float(entry.get('sentiment')) or 0)
        score += int(sentiment * 2)
        if score < 4:
            continue
        scored.append({
            'theme': theme,
            'speaker': entry.get('speaker') or 'Management',
            'title': entry.get('title') or '',
            'quote': _squash(content, 320),
            'why_it_matters': reason,
            'score': score,
        })

    scored.sort(key=lambda item: item.get('score', 0), reverse=True)
    results = []
    for item in scored:
        theme = item.get('theme')
        if theme in seen_themes:
            continue
        seen_themes.add(theme)
        results.append({key: value for key, value in item.items() if key != 'score'})
        if len(results) >= limit:
            break
    return results


def _normalize_transcript(provider: str, source_label: str, symbol: str, quarter: str, call_date, entries: List[dict]) -> dict:
    return {
        'status': 'available' if entries else 'unavailable',
        'provider': provider,
        'source_label': source_label,
        'symbol': symbol,
        'quarter': quarter,
        'date': _coerce_datetime(call_date).isoformat() if _coerce_datetime(call_date) else None,
        'entry_count': len(entries),
        'management_excerpt': _excerpt(entries, management_only=True),
        'qa_excerpt': _excerpt(entries, management_only=False),
        'digest': _digest(entries),
        'catalysts': _extract_transcript_catalysts(entries),
    }


def _alpha_payload_ok(payload) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get('Error Message') or payload.get('Note') or payload.get('Information'):
        return False
    return isinstance(payload.get('transcript'), list) and bool(payload.get('transcript'))


def _fetch_alpha_transcript(symbol: str, earnings_date=None) -> Optional[dict]:
    api_key = ALPHA_VANTAGE_API_KEY
    if not api_key:
        return None

    url = 'https://www.alphavantage.co/query'
    for quarter in _candidate_alpha_quarters(earnings_date):
        try:
            payload = _request_json(url, params={
                'function': 'EARNINGS_CALL_TRANSCRIPT',
                'symbol': symbol,
                'quarter': quarter,
                'apikey': api_key,
            })
        except Exception as exc:
            LOGGER.warning('Alpha Vantage transcript lookup failed for %s %s: %s', symbol, quarter, exc)
            continue

        if not _alpha_payload_ok(payload):
            continue
        entries = []
        for row in payload.get('transcript') or []:
            entry = _speaker_entry(
                row.get('speaker'),
                row.get('title'),
                row.get('content'),
                row.get('sentiment'),
            )
            if entry:
                entries.append(entry)
        if entries:
            return _normalize_transcript(
                'alphavantage',
                'Alpha Vantage earnings-call transcript',
                symbol,
                quarter,
                None,
                entries,
            )
    return None


def _fmp_params(params: dict) -> dict:
    query = dict(params)
    if FMP_API_KEY:
        query['apikey'] = FMP_API_KEY
    return query


def _pick_fmp_date(rows: List[dict], earnings_date=None) -> Optional[dict]:
    if not rows:
        return None
    target = _coerce_datetime(earnings_date) or datetime.now(timezone.utc)

    def sort_key(row: dict):
        row_date = _coerce_datetime(row.get('date') or row.get('publishedDate') or row.get('acceptedDate'))
        distance = abs((row_date - target).total_seconds()) if row_date else 9e18
        year = row.get('year') or 0
        quarter = row.get('quarter') or row.get('period') or ''
        return (distance, -int(year or 0), str(quarter))

    return sorted(rows, key=sort_key)[0]


def _parse_fmp_quarter(row: dict):
    quarter = row.get('quarter') or row.get('period')
    year = row.get('year')
    if year is None:
        return None, None, None
    if isinstance(quarter, str):
        match = re.search(r'(\d)', quarter)
        quarter_num = int(match.group(1)) if match else None
        quarter_label = quarter if quarter.startswith('Q') else f'Q{quarter_num}' if quarter_num else str(quarter)
    else:
        quarter_num = int(quarter) if quarter is not None else None
        quarter_label = f'Q{quarter_num}' if quarter_num else None
    if not quarter_num:
        return None, None, None
    return int(year), quarter_num, f'{int(year)}{quarter_label}'


def _fetch_fmp_transcript(symbol: str, earnings_date=None) -> Optional[dict]:
    url_dates = 'https://financialmodelingprep.com/stable/earning-call-transcript-dates'
    url_transcript = 'https://financialmodelingprep.com/stable/earning-call-transcript'
    try:
        payload = _request_json(url_dates, params=_fmp_params({'symbol': symbol}))
    except Exception as exc:
        LOGGER.warning('FMP transcript dates lookup failed for %s: %s', symbol, exc)
        return None

    rows = payload if isinstance(payload, list) else payload.get('data') or []
    selected = _pick_fmp_date(rows, earnings_date=earnings_date)
    if not selected:
        return None

    year, quarter_num, quarter_label = _parse_fmp_quarter(selected)
    if not year or not quarter_num:
        return None

    try:
        transcript_payload = _request_json(url_transcript, params=_fmp_params({
            'symbol': symbol,
            'year': year,
            'quarter': quarter_num,
        }))
    except Exception as exc:
        LOGGER.warning('FMP transcript lookup failed for %s %sQ%s: %s', symbol, year, quarter_num, exc)
        return None

    rows = transcript_payload if isinstance(transcript_payload, list) else transcript_payload.get('data') or []
    if not rows:
        return None
    content = rows[0].get('content') or ''
    entries = _entries_from_blob(content)
    if not entries:
        return None
    return _normalize_transcript(
        'fmp',
        'Financial Modeling Prep transcript',
        symbol,
        quarter_label,
        rows[0].get('date') or selected.get('date'),
        entries,
    )


def get_earnings_call_transcript(symbol: str, earnings_date=None) -> dict:
    ticker = str(symbol or '').upper().strip()
    if not ticker:
        return {
            'status': 'unavailable',
            'provider': None,
            'source_label': 'No transcript source configured',
            'symbol': '',
            'quarter': None,
            'date': None,
            'entry_count': 0,
            'management_excerpt': '',
            'qa_excerpt': '',
            'digest': '',
            'catalysts': [],
        }

    if not FMP_API_KEY and not ALPHA_VANTAGE_API_KEY:
        return {
            'status': 'unavailable',
            'provider': None,
            'source_label': 'Add FMP_API_KEY or ALPHA_VANTAGE_API_KEY to enable transcript ingestion',
            'symbol': ticker,
            'quarter': None,
            'date': None,
            'entry_count': 0,
            'management_excerpt': '',
            'qa_excerpt': '',
            'digest': '',
            'catalysts': [],
        }

    transcript = _fetch_fmp_transcript(ticker, earnings_date=earnings_date)
    if transcript:
        return transcript

    transcript = _fetch_alpha_transcript(ticker, earnings_date=earnings_date)
    if transcript:
        return transcript

    return {
        'status': 'unavailable',
        'provider': None,
        'source_label': 'Transcript unavailable',
        'symbol': ticker,
        'quarter': None,
        'date': None,
        'entry_count': 0,
        'management_excerpt': '',
        'qa_excerpt': '',
        'digest': '',
        'catalysts': [],
    }
