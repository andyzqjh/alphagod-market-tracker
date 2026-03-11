import csv
import io
from datetime import datetime, timezone

import requests

REQUEST_TIMEOUT = 12
STOCKBEE_MONITOR_PAGE_URL = 'https://stockbee.blogspot.com/p/mm.html'
STOCKBEE_MONITOR_SHEET_URL = 'https://docs.google.com/spreadsheets/d/1O6OhS7ciA8zwfycBfGPbP2fWJnR0pn2UUvFZVDP9jpE/export?format=csv'
STOCKBEE_MONITOR_GUIDES = [
    {
        'label': 'Market Monitor Scans',
        'url': 'https://stockbee.blogspot.com/2022/12/market-monitor-scans.html',
    },
    {
        'label': 'How to use market breadth',
        'url': 'https://stockbee.blogspot.com/2011/08/how-to-use-market-breadth-to-avoid.html',
    },
]
STOCKBEE_MONITOR_COLUMNS = [
    {'key': 'date', 'short_label': 'Date', 'type': 'date', 'better': 'neutral'},
    {'key': 'up4', 'short_label': 'Up 4%+', 'type': 'number', 'better': 'higher'},
    {'key': 'down4', 'short_label': 'Down 4%+', 'type': 'number', 'better': 'lower'},
    {'key': 'ratio5', 'short_label': '5d Ratio', 'type': 'number_2', 'better': 'higher'},
    {'key': 'ratio10', 'short_label': '10d Ratio', 'type': 'number_2', 'better': 'higher'},
    {'key': 'up25_quarter', 'short_label': 'Up 25% Qtr', 'type': 'number', 'better': 'higher'},
    {'key': 'down25_quarter', 'short_label': 'Down 25% Qtr', 'type': 'number', 'better': 'lower'},
    {'key': 'up25_month', 'short_label': 'Up 25% Mth', 'type': 'number', 'better': 'higher'},
    {'key': 'down25_month', 'short_label': 'Down 25% Mth', 'type': 'number', 'better': 'lower'},
    {'key': 'up50_month', 'short_label': 'Up 50% Mth', 'type': 'number', 'better': 'higher'},
    {'key': 'down50_month', 'short_label': 'Down 50% Mth', 'type': 'number', 'better': 'lower'},
    {'key': 'up13_in_34', 'short_label': 'Up 13% / 34d', 'type': 'number', 'better': 'higher'},
    {'key': 'down13_in_34', 'short_label': 'Down 13% / 34d', 'type': 'number', 'better': 'lower'},
    {'key': 'universe', 'short_label': 'Universe', 'type': 'number', 'better': 'neutral'},
    {'key': 't2108', 'short_label': 'T2108', 'type': 'percent', 'better': 'higher'},
    {'key': 'sp', 'short_label': 'S&P', 'type': 'sp', 'better': 'higher'},
]

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
})


def _parse_number(value):
    cleaned = str(value or '').replace(',', '').strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _cell(row, index: int):
    return row[index] if len(row) > index else ''


def get_stockbee_monitor() -> dict:
    response = SESSION.get(STOCKBEE_MONITOR_SHEET_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    reader = csv.reader(io.StringIO(response.text))
    rows = list(reader)
    data_rows = []
    for row in rows[2:]:
        if not row or not str(_cell(row, 0)).strip():
            continue
        data_rows.append({
            'date': _cell(row, 0) or None,
            'up4': _parse_number(_cell(row, 1)),
            'down4': _parse_number(_cell(row, 2)),
            'ratio5': _parse_number(_cell(row, 3)),
            'ratio10': _parse_number(_cell(row, 4)),
            'up25_quarter': _parse_number(_cell(row, 5)),
            'down25_quarter': _parse_number(_cell(row, 6)),
            'up25_month': _parse_number(_cell(row, 7)),
            'down25_month': _parse_number(_cell(row, 8)),
            'up50_month': _parse_number(_cell(row, 9)),
            'down50_month': _parse_number(_cell(row, 10)),
            'up13_in_34': _parse_number(_cell(row, 11)),
            'down13_in_34': _parse_number(_cell(row, 12)),
            'universe': _parse_number(_cell(row, 13)),
            't2108': _parse_number(_cell(row, 14)),
            'sp': _parse_number(_cell(row, 15)),
        })

    latest = data_rows[0] if data_rows else {}
    return {
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'description': 'Stockbee Market Monitor is a breadth-based market timing tracker. This tab pulls the live published sheet into the dashboard.',
        'source_url': STOCKBEE_MONITOR_PAGE_URL,
        'sheet_url': STOCKBEE_MONITOR_SHEET_URL,
        'guides': STOCKBEE_MONITOR_GUIDES,
        'columns': STOCKBEE_MONITOR_COLUMNS,
        'summary': {
            'latest_date': latest.get('date'),
            'up4': latest.get('up4'),
            'down4': latest.get('down4'),
            'ratio5': latest.get('ratio5'),
            'ratio10': latest.get('ratio10'),
            't2108': latest.get('t2108'),
            'sp': latest.get('sp'),
        },
        'rows': data_rows[:40],
    }
