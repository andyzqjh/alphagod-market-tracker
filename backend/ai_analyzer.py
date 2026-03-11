import json
import os
from typing import List

from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ.get('ANTHROPIC_API_KEY')
client = Anthropic(api_key=API_KEY) if API_KEY else None


def _safe_json_load(text: str, fallback: dict) -> dict:
    try:
        cleaned = text.strip()
        if '```' in cleaned:
            cleaned = cleaned.split('```')[1]
            if cleaned.startswith('json'):
                cleaned = cleaned[4:]
        return json.loads(cleaned.strip())
    except Exception:
        return fallback


def _call_json(prompt: str, fallback: dict, max_tokens: int = 900) -> dict:
    if not client:
        return fallback

    try:
        message = client.messages.create(
            model='claude-sonnet-4-6',
            max_tokens=max_tokens,
            messages=[{'role': 'user', 'content': prompt}],
        )
        text = message.content[0].text if message.content else ''
        return _safe_json_load(text, fallback)
    except Exception as exc:
        fallback = dict(fallback)
        fallback.setdefault('error', str(exc))
        return fallback


def _join_moves(items: List[dict], label_key: str, value_key: str, limit: int = 3) -> str:
    formatted = []
    for item in items[:limit]:
        label = item.get(label_key, 'n/a')
        value = item.get(value_key)
        move = f'{value:+.2f}%' if value is not None else 'n/a'
        formatted.append(f'{label} {move}')
    return ', '.join(formatted) or 'no standout names yet'


def _join_headlines(items: List[dict], limit: int = 3) -> str:
    formatted = []
    for item in items[:limit]:
        title = item.get('title') or 'headline unavailable'
        source = item.get('source') or item.get('ticker') or 'news feed'
        formatted.append(f'{title} ({source})')
    return '; '.join(formatted) or 'No fresh headline feed was available.'


def analyze_stock(ticker: str, data: dict, news: str = None) -> dict:
    float_str = 'N/A'
    if data.get('float_shares'):
        float_value = data['float_shares']
        float_str = f"{float_value / 1e9:.1f}B" if float_value >= 1_000_000_000 else f"{float_value / 1e6:.1f}M"

    short_str = f"{data['short_interest']}%" if data.get('short_interest') else 'N/A'

    market_cap = data.get('market_cap')
    market_cap_str = 'N/A'
    if market_cap:
        if market_cap >= 1e12:
            market_cap_str = f"${market_cap / 1e12:.1f}T"
        elif market_cap >= 1e9:
            market_cap_str = f"${market_cap / 1e9:.1f}B"
        else:
            market_cap_str = f"${market_cap / 1e6:.0f}M"

    context = f"""
Ticker: {ticker} | Company: {data.get('company_name', ticker)}
Industry: {data.get('industry', 'N/A')} | Sector: {data.get('sector', 'N/A')}
Market Cap: {market_cap_str}
Pre-market Change: {data.get('premarket_pct', 'N/A')}%
Pre-market Price: ${data.get('premarket_price', 'N/A')}
Previous Close: ${data.get('prev_close', 'N/A')}
Volume: {data.get('volume', 0):,} | Avg Volume: {data.get('avg_volume', 0):,}
Relative Volume (RVol): {data.get('rvol', 'N/A')}x
Float: {float_str} | Short Interest: {short_str}
Business: {data.get('description', 'N/A')}
Themes: {', '.join(data.get('themes', [])) or 'None'}
{f'News/Catalyst provided: {news}' if news else 'No news catalyst provided.'}
"""

    fallback = {
        'grade': 'C',
        'category': 'Others',
        'brief_reasoning': 'AI analysis unavailable.',
        'impact': 'The setup needs manual review because the AI response was unavailable.',
        'explosiveness': 'Watch relative volume, catalyst quality, and whether price is holding key levels.',
        'statistical_edge': 'Use price vs prior close, trend alignment, and broad theme strength to judge the edge.',
        'risk_factors': 'Headline risk and failed follow-through remain the main risks.',
    }

    prompt = f"""You are a professional stock trader analyst. Analyze this stock concisely for a trader.

{context}

Return ONLY a valid JSON object with this exact structure:
{{
  "grade": "A",
  "category": "Earnings",
  "brief_reasoning": "One sentence max 100 chars",
  "impact": "2-3 sentence fundamental impact analysis",
  "explosiveness": "2-3 sentence analysis of move potential and momentum",
  "statistical_edge": "2-3 sentence data/technical edge analysis",
  "risk_factors": "1-2 sentence key risks"
}}

Grade: A=strong catalyst+conviction, B=good setup, C=speculative, D=weak/no catalyst
Category options: Earnings | New Contracts Partnerships | FDA | Themes Narratives | Others

Return ONLY the JSON, no markdown, no extra text."""

    return _call_json(prompt, fallback, max_tokens=700)


def _fallback_market_brief(market_overview: dict, theme_dashboard: dict, etf_dashboard: dict, headlines: List[dict]) -> dict:
    overview_items = market_overview.get('items', [])
    themes = theme_dashboard.get('all', [])
    etf_leaders = etf_dashboard.get('leaders', [])
    etf_laggards = etf_dashboard.get('laggards', [])
    notable_overview = sorted(overview_items, key=lambda item: abs(item.get('change_pct') or 0), reverse=True)[:4]
    top_themes = themes[:3]
    weak_themes = sorted(themes, key=lambda item: item.get('avg_pct', 0))[:3]
    lead_group = etf_dashboard.get('summary', {}).get('best_group') or 'ETF groups'
    headline_text = '; '.join(item.get('title', '') for item in headlines[:4]) or 'No major headlines were available from the feed.'

    sentiment = 'Neutral'
    if market_overview.get('summary', {}).get('positive', 0) > market_overview.get('summary', {}).get('negative', 0):
        sentiment = 'Bullish'
    elif market_overview.get('summary', {}).get('positive', 0) < market_overview.get('summary', {}).get('negative', 0):
        sentiment = 'Bearish'

    paragraphs = [
        f"The tape is currently {sentiment.lower()} overall, with {market_overview.get('summary', {}).get('positive', 0)} advancing instruments versus {market_overview.get('summary', {}).get('negative', 0)} declining across the core market overview board. The biggest immediate moves are coming from {_join_moves(notable_overview, 'label', 'change_pct')}.",
        f"Theme leadership is concentrated in {_join_moves(top_themes, 'theme', 'avg_pct')}. That tells us where traders are still willing to pay up for growth, momentum, or narrative strength inside the current session.",
        f"On the weak side, pressure is showing up in {_join_moves(weak_themes, 'theme', 'avg_pct')}. If those groups continue to lag while leaders keep expanding, market participation is becoming more selective rather than broadly strong.",
        f"The ETF capital-flow proxy points toward {lead_group} as the current leadership pocket. The strongest ETF leaders right now are {_join_moves(etf_leaders, 'symbol', 'change_pct', limit=4)}, while the main pressure points are {_join_moves(etf_laggards, 'symbol', 'change_pct', limit=4)}.",
        f"Headline context is still important because the market can rotate quickly when macro narratives change. The current feed is highlighting: {headline_text}",
        f"Tactically, the key question is whether leadership continues to broaden or narrows into only a few pockets. If ETF leadership, theme leadership, and index breadth keep confirming one another, the market can sustain upside; if they diverge, traders should expect more chop, failed breakouts, and faster sector rotations.",
    ]

    bullets = [
        {'tone': 'Bullish', 'text': f"Leadership theme: {top_themes[0]['theme']}"} if top_themes else {'tone': 'Neutral', 'text': 'Watch for clearer theme leadership.'},
        {'tone': 'Bearish', 'text': f"Weakest theme: {weak_themes[0]['theme']}"} if weak_themes else {'tone': 'Neutral', 'text': 'No weak theme standout yet.'},
        {'tone': 'Bullish', 'text': f"Top ETF flow proxy: {etf_leaders[0]['symbol']}"} if etf_leaders else {'tone': 'Neutral', 'text': 'ETF leaders unavailable.'},
        {'tone': 'Bearish', 'text': f"Main ETF laggard: {etf_laggards[0]['symbol']}"} if etf_laggards else {'tone': 'Neutral', 'text': 'ETF laggards unavailable.'},
        {'tone': 'Neutral', 'text': 'Use breadth plus theme confirmation before pressing size.'},
        {'tone': 'Neutral', 'text': 'Watch whether capital stays in growth, rotates defensive, or moves into rates and commodities.'},
    ]

    return {
        'title': 'AI Market Brief',
        'sentiment': sentiment,
        'paragraphs': paragraphs,
        'bullets': bullets,
    }


def build_market_brief(market_overview: dict, theme_dashboard: dict, etf_dashboard: dict, headlines: List[dict]) -> dict:
    fallback = _fallback_market_brief(market_overview, theme_dashboard, etf_dashboard, headlines)

    prompt = f"""You are a market strategist writing a concise but insightful daily market brief for an active trader.

Market overview summary:
{json.dumps(market_overview.get('summary', {}), ensure_ascii=False)}

Key market instruments:
{json.dumps(market_overview.get('items', [])[:10], ensure_ascii=False)}

Theme leadership:
{json.dumps(theme_dashboard.get('leaders', []), ensure_ascii=False)}

Theme laggards:
{json.dumps(theme_dashboard.get('laggards', []), ensure_ascii=False)}

ETF capital-flow proxy leaders:
{json.dumps(etf_dashboard.get('leaders', []), ensure_ascii=False)}

ETF capital-flow proxy laggards:
{json.dumps(etf_dashboard.get('laggards', []), ensure_ascii=False)}

News headlines:
{json.dumps(headlines[:8], ensure_ascii=False)}

Return ONLY valid JSON with this exact structure:
{{
  "title": "AI Market Brief",
  "sentiment": "Bullish",
  "paragraphs": ["paragraph 1", "paragraph 2", "paragraph 3", "paragraph 4", "paragraph 5", "paragraph 6"],
  "bullets": [
    {{"tone": "Bullish", "text": "short point"}},
    {{"tone": "Bearish", "text": "short point"}},
    {{"tone": "Neutral", "text": "short point"}},
    {{"tone": "Bullish", "text": "short point"}},
    {{"tone": "Bearish", "text": "short point"}},
    {{"tone": "Neutral", "text": "short point"}}
  ]
}}

Rules:
- Write exactly 6 paragraphs.
- Each paragraph should be 2-4 sentences.
- Focus on what is leading, what is lagging, and where capital appears to be rotating.
- Speak in trader language, not academic language.
- Do not use markdown.
- Return JSON only."""

    return _call_json(prompt, fallback, max_tokens=1200)


def _fallback_chart_reasoning(ticker: str, detail: dict, snapshot: dict, headlines: List[dict]) -> dict:
    metrics = snapshot.get('metrics', {})
    rsi = metrics.get('rsi14')
    trend_state = metrics.get('trend_state', 'Range-bound')
    ret_1m = metrics.get('return_1m')
    rel_vol = metrics.get('relative_volume20')
    high20 = metrics.get('high20')
    low20 = metrics.get('low20')
    themes = ', '.join(detail.get('themes', [])) or 'no tracked themes'
    top_headline = headlines[0].get('title') if headlines else 'No fresh headline feed was available.'
    news_rollup = _join_headlines(headlines, limit=3)

    bias = 'Neutral'
    if trend_state in ('Strong uptrend', 'Constructive uptrend'):
        bias = 'Bullish'
    elif trend_state in ('Strong downtrend', 'Weakening downtrend'):
        bias = 'Bearish'

    ret_text = f'{ret_1m:+.2f}%' if ret_1m is not None else 'n/a'
    rel_vol_text = f'{rel_vol}x' if rel_vol is not None else 'n/a'

    return {
        'bias': bias,
        'headline': f"{ticker} is in a {trend_state.lower()} with traders focused on follow-through.",
        'summary': f"{ticker} is part of {themes}. The stock is showing {ret_text} over the last month, which helps frame whether this is continuation behavior or just noise inside a base.",
        'trend': f"Trend state reads as {trend_state}. Price versus the 20, 50, and 200-day moving averages is the quickest read on whether institutions are still supporting the move.",
        'levels': f"The immediate reference zone is the recent 20-day range between {low20 if low20 is not None else 'n/a'} and {high20 if high20 is not None else 'n/a'}. A clean move through that band would change the chart character faster than any single headline.",
        'volume': f"Relative volume is running at {rel_vol_text} versus the 20-day average. That tells you whether the move has real sponsorship or is still vulnerable to fading back into the range.",
        'news_summary': f"Latest headlines: {news_rollup}",
        'news_reasoning': f"If traders keep leaning on the current news flow, the tape can either confirm the existing {trend_state.lower()} or fade quickly if the headline impulse does not convert into volume-backed follow-through. The key catalyst in the feed right now is: {top_headline}",
        'risk': f"RSI is {rsi if rsi is not None else 'n/a'}, so watch for exhaustion if momentum is already stretched. The latest headline context is: {top_headline}",
    }


def analyze_chart_reasoning(ticker: str, detail: dict, snapshot: dict, headlines: List[dict]) -> dict:
    fallback = _fallback_chart_reasoning(ticker, detail, snapshot, headlines)

    prompt = f"""You are a trader's chart assistant. Explain what is happening on this stock's chart in plain English.

Ticker: {ticker}
Company detail:
{json.dumps(detail, ensure_ascii=False)}

Chart snapshot:
{json.dumps(snapshot.get('metrics', {}), ensure_ascii=False)}

Recent headlines:
{json.dumps(headlines[:5], ensure_ascii=False)}

Return ONLY valid JSON with this exact structure:
{{
  "bias": "Bullish",
  "headline": "one sentence headline",
  "summary": "2-3 sentences on the bigger picture",
  "trend": "2-3 sentences on trend structure",
  "levels": "2-3 sentences on support/resistance and trigger levels",
  "volume": "2-3 sentences on volume/participation",
  "news_summary": "1-2 sentences summarizing the latest news feed",
  "news_reasoning": "2-3 sentences connecting the news flow to the chart behavior",
  "risk": "1-2 sentences on what can go wrong"
}}

Rules:
- Speak like a trader, not a textbook.
- Explain what is happening, not just indicators in isolation.
- Mention whether the chart looks like breakout, pullback, base, or breakdown behavior.
- Explicitly explain what the latest news suggests for price action.
- Return JSON only."""

    return _call_json(prompt, fallback, max_tokens=900)






def _fallback_earnings_brief(earnings_tracker: dict) -> dict:
    items = earnings_tracker.get('items', [])
    summary = earnings_tracker.get('summary', {})
    top_names = items[:5]
    strongest = sorted(items, key=lambda item: item.get('change_pct') or 0, reverse=True)[:4]
    weakest = sorted(items, key=lambda item: item.get('change_pct') or 0)[:4]

    theme_counts = {}
    for item in items:
        for theme in item.get('themes', []):
            theme_counts[theme] = theme_counts.get(theme, 0) + 1
    top_themes = sorted(theme_counts.items(), key=lambda pair: pair[1], reverse=True)[:5]

    return {
        'headline': 'Upcoming U.S. earnings are clustering in the tracked liquid-stock universe.',
        'summary': f"There are {summary.get('upcoming_count', 0)} earnings events on deck in the current tracker, with {summary.get('today_count', 0)} due today and {summary.get('next_7_days', 0)} scheduled over the next week. The nearest names on the calendar are {', '.join(item.get('ticker', 'n/a') for item in top_names) or 'not available yet'}.",
        'focus': f"Traders should focus on where earnings are concentrated by theme and whether price is already leaning into the print. Current leaders into earnings are {_join_moves(strongest, 'ticker', 'change_pct', limit=4)}, while weaker setups include {_join_moves(weakest, 'ticker', 'change_pct', limit=4)}.",
        'themes': f"The earnings slate is most connected to these tracked themes: {', '.join(theme for theme, _ in top_themes) or 'no clear theme concentration yet'}. That matters because a strong report can spill into peers and ETFs tied to the same narrative.",
        'risk': 'The main risk is that crowded names are already pricing in good news, which raises the odds of post-print reversals even on decent numbers. Watch whether implied expectations look too high versus the current tape.'
    }


def build_earnings_brief(earnings_tracker: dict) -> dict:
    fallback = _fallback_earnings_brief(earnings_tracker)

    prompt = f"""You are writing a concise earnings tracker brief for an active trader.

Earnings tracker summary:
{json.dumps(earnings_tracker.get('summary', {}), ensure_ascii=False)}

Upcoming earnings rows:
{json.dumps(earnings_tracker.get('items', [])[:20], ensure_ascii=False)}

Return ONLY valid JSON with this exact structure:
{{
  "headline": "one sentence summary",
  "summary": "2-3 sentences on what the earnings slate looks like",
  "focus": "2-3 sentences on what traders should focus on",
  "themes": "2-3 sentences on theme or sector concentration",
  "risk": "1-2 sentences on the main risks"
}}

Rules:
- Speak like a trader, not a textbook.
- Focus on concentration, expectations, and where the biggest reactions may happen.
- Mention when price action suggests names may already be leaning into the print.
- Return JSON only."""

    return _call_json(prompt, fallback, max_tokens=900)
