import yfinance as yf
import requests
import time

HEADERS = {'User-Agent': 'StockDashboard/1.0 (personal use)'}

def get_stock_news(ticker: str) -> list:
    """Get recent news for a ticker via yfinance"""
    try:
        t = yf.Ticker(ticker)
        raw = t.news or []
        result = []
        for n in raw[:12]:
            thumb = ''
            if n.get('thumbnail'):
                res = n['thumbnail'].get('resolutions', [])
                if res:
                    thumb = res[0].get('url', '')

            summary = (
                n.get('summary')
                or n.get('description')
                or n.get('content', {}).get('summary')
                or n.get('content', {}).get('description')
                or ''
            )

            result.append({
                'type': 'news',
                'ticker': ticker,
                'title': n.get('title', ''),
                'source': n.get('publisher', ''),
                'url': n.get('link', ''),
                'time': n.get('providerPublishTime', 0),
                'thumbnail': thumb,
                'summary': summary[:420],
            })
        return result
    except Exception as e:
        return []

def get_reddit_posts(tickers: list) -> list:
    """Get recent Reddit posts mentioning given tickers (no API key needed)"""
    subs = ['wallstreetbets', 'stocks', 'investing', 'StockMarket']
    results = []
    for ticker in tickers[:6]:
        for sub in subs[:2]:
            try:
                url = f'https://www.reddit.com/r/{sub}/search.json?q={ticker}&sort=new&limit=5&t=week'
                r = requests.get(url, headers=HEADERS, timeout=8)
                if r.status_code != 200:
                    continue
                posts = r.json().get('data', {}).get('children', [])
                for p in posts:
                    d = p.get('data', {})
                    title = d.get('title', '')
                    # Only include if ticker is mentioned in title
                    if ticker.upper() not in title.upper():
                        continue
                    results.append({
                        'type': 'reddit',
                        'ticker': ticker,
                        'subreddit': sub,
                        'title': title,
                        'score': d.get('score', 0),
                        'comments': d.get('num_comments', 0),
                        'url': 'https://reddit.com' + d.get('permalink', ''),
                        'time': int(d.get('created_utc', 0)),
                        'text': d.get('selftext', '')[:300],
                    })
            except Exception:
                pass
            time.sleep(0.3)  # be polite to Reddit

    results.sort(key=lambda x: x.get('time', 0), reverse=True)
    return results[:40]

