import json
import os
from datetime import datetime, timezone
from typing import List, Optional

from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ.get('ANTHROPIC_API_KEY')
client = Anthropic(api_key=API_KEY) if API_KEY else None


def _json_text(value) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)
AI_TIMEOUT_SECONDS = float(os.environ.get('ANTHROPIC_TIMEOUT_SECONDS', '18'))


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
            timeout=AI_TIMEOUT_SECONDS,
        )
        text = message.content[0].text if message.content else ''
        return _safe_json_load(text, fallback)
    except Exception as exc:
        fallback = dict(fallback)
        fallback.setdefault('error', str(exc))
        return fallback


def _merge_with_fallback(fallback: dict, candidate: dict) -> dict:
    merged = dict(fallback)
    if not isinstance(candidate, dict):
        return merged
    for key, value in candidate.items():
        if value in (None, '', []):
            continue
        merged[key] = value
    return merged


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


def _headline_text(item: dict) -> str:
    title = item.get('title') or ''
    summary = item.get('summary') or ''
    return f'{title} {summary}'.strip().lower()


def _headline_tone(item: dict) -> str:
    text = _headline_text(item)
    bullish_terms = [
        'beat', 'beats', 'guidance raise', 'raised guidance', 'guidance hike', 'upgrade',
        'contract', 'award', 'approval', 'partnership', 'deal', 'record', 'strong demand',
        'backlog', 'buyback', 'launch', 'expansion', 'orders', 'surge', 'growth'
    ]
    bearish_terms = [
        'miss', 'misses', 'guidance cut', 'cut guidance', 'downgrade', 'lawsuit', 'probe',
        'investigation', 'offering', 'dilution', 'delay', 'weak demand', 'recall', 'tariff',
        'restriction', 'cut', 'layoff', 'bankruptcy', 'fraud', 'warning'
    ]

    bullish_hits = sum(1 for term in bullish_terms if term in text)
    bearish_hits = sum(1 for term in bearish_terms if term in text)
    if bullish_hits > bearish_hits:
        return 'Bullish'
    if bearish_hits > bullish_hits:
        return 'Bearish'
    return 'Neutral'


def _headline_impact(item: dict, trend_state: str) -> str:
    text = _headline_text(item)
    tone = _headline_tone(item)
    trend = (trend_state or 'range-bound').lower()

    if any(term in text for term in ['beat', 'miss', 'guidance', 'earnings', 'revenue', 'eps']):
        if tone == 'Bullish':
            return 'This reads like an earnings or guidance confirmation catalyst, so traders will look for the chart to hold strength instead of fading back into the prior range.'
        if tone == 'Bearish':
            return 'This reads like an earnings or guidance problem, so any bounce is vulnerable unless price quickly reclaims broken support with volume.'
        return 'This is earnings-related news, so the next clue is whether the market treats it as already priced in or a fresh reset for expectations.'

    if any(term in text for term in ['contract', 'award', 'deal', 'partnership', 'approval', 'launch', 'backlog', 'orders']):
        if tone == 'Bullish':
            return 'This headline matters because it can refresh the growth narrative and keep momentum traders involved if the breakout levels keep holding.'
        if tone == 'Bearish':
            return 'This headline matters because it introduces execution risk into the growth narrative, which can turn prior support into supply.'

    if any(term in text for term in ['tariff', 'restriction', 'probe', 'investigation', 'lawsuit', 'recall', 'delay']):
        return 'This is the kind of headline that can cap multiple expansion and keep traders defensive until price proves it can absorb the bad news.'

    if tone == 'Bullish':
        return f'The feed is supportive, so the main tell is whether the current {trend} keeps expanding with volume instead of stalling at obvious resistance.'
    if tone == 'Bearish':
        return f'The feed is a headwind, so the main tell is whether the current {trend} loses support and turns into distribution.'
    return 'The feed is mixed, so price reaction matters more than the headline itself. Traders will watch whether the tape confirms the story with follow-through.'


def _build_headline_impacts(headlines: List[dict], trend_state: str) -> List[dict]:
    impacts = []
    for item in headlines[:3]:
        title = item.get('title') or 'Headline unavailable'
        source = item.get('source') or item.get('ticker') or 'News feed'
        summary = (item.get('summary') or '').strip()
        tone = _headline_tone(item)
        impacts.append({
            'headline': title,
            'source': source,
            'tone': tone,
            'summary': summary[:220] if summary else f'{source} is the source of the latest catalyst tied to {title.lower()}.',
            'impact': _headline_impact(item, trend_state),
        })
    return impacts


def _perception_before(detail: dict) -> str:
    operating_margin = detail.get('operating_margin')
    gross_margin = detail.get('gross_margin')
    forward_pe = detail.get('forward_pe')
    revenue_growth = detail.get('revenue_growth')

    if operating_margin is not None and operating_margin <= 10:
        return 'The market has mostly treated this as a lower-margin execution story, so any proof of margin improvement can change the multiple fast.'
    if gross_margin is not None and gross_margin >= 60:
        return 'The market already views this as a premium-margin quality name, so the bar for upside surprise is higher.'
    if revenue_growth is not None and revenue_growth >= 18 and forward_pe is not None and forward_pe >= 28:
        return 'The stock trades like a growth name where the market is already paying for continued expansion.'
    if forward_pe is not None and forward_pe <= 12:
        return 'The market has been valuing this more like a cautious value setup than a clean growth rerating story.'
    return 'The market still seems to want proof on execution, durability, or quality before giving the stock a higher multiple.'



def _expectation_view(detail: dict) -> str:
    recommendation = (detail.get('recommendation') or '').lower()
    analyst_count = detail.get('analyst_count')
    price = detail.get('price')
    target = detail.get('target_mean_price')
    spread = None
    if price not in (None, 0) and target is not None:
        spread = ((target - price) / price) * 100

    pieces = []
    if recommendation in ('buy', 'strong_buy'):
        pieces.append('Analysts are leaning constructive.')
    elif recommendation in ('hold', 'neutral'):
        pieces.append('Analysts are mostly neutral.')
    elif recommendation:
        pieces.append('Analysts are leaning cautious.')

    if spread is not None:
        if spread >= 12:
            pieces.append(f'Consensus target still sits about {spread:.1f}% above spot, so the Street still sees upside if execution confirms.')
        elif spread <= -8:
            pieces.append(f'The stock is already roughly {abs(spread):.1f}% above consensus target, so the market may already be pricing a better story than analysts publish.')
        else:
            pieces.append('The stock is trading near consensus target, so follow-through matters more than the headline itself.')

    if detail.get('revenue_growth') is not None:
        pieces.append(f'Revenue growth is running around {detail["revenue_growth"]:.1f}%.')
    if detail.get('earnings_growth') is not None:
        pieces.append(f'Earnings growth is running around {detail["earnings_growth"]:.1f}%.')
    if analyst_count:
        pieces.append(f'{analyst_count} analysts are in the published set.')

    return ' '.join(pieces) or 'Published expectation data is limited, so traders should lean more on price reaction and the latest headlines.'



def _key_events_summary(headlines: List[dict]) -> str:
    if not headlines:
        return 'No fresh company-specific headlines were returned, so the chart is being driven more by positioning and tape behavior than a clean new event.'

    pieces = []
    for item in headlines[:3]:
        title = item.get('title') or 'Headline unavailable'
        summary = (item.get('summary') or '').strip()
        if summary:
            pieces.append(f'{title}: {summary[:180]}')
        else:
            pieces.append(title)
    return 'Key events in the feed right now: ' + ' | '.join(pieces)



def _rerating_read(detail: dict, headlines: List[dict], trend_state: str) -> str:
    text = ' '.join(f"{item.get('title') or ''} {item.get('summary') or ''}" for item in headlines[:3]).lower()
    if 'margin' in text:
        return 'If the market believes margins are inflecting higher, the stock can rerate because the old low-quality or low-margin view starts to break.'
    if any(term in text for term in ['guidance', 'earnings', 'eps', 'revenue', 'beat']):
        return 'If the market treats the latest earnings read as a multi-quarter reset instead of a one-off beat, the stock can support a higher valuation band.'
    if any(term in text for term in ['contract', 'deal', 'partnership', 'order', 'backlog']):
        return 'If the new business momentum looks durable, investors can shift from waiting to paying up for better visibility.'
    if trend_state in ('Strong uptrend', 'Constructive uptrend'):
        return 'The chart is already constructive, so the next rerating step depends on the stock proving this is institutional accumulation rather than a short-term squeeze.'
    return 'The rerating question is whether the next catalyst changes the market story enough to justify a higher multiple, not just a one-day move.'

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

    result = _call_json(prompt, fallback, max_tokens=700)
    return _merge_with_fallback(fallback, result)


def _fallback_market_brief(market_overview: dict, theme_dashboard: dict, etf_dashboard: dict, headlines: List[dict]) -> dict:
    summary = market_overview.get('summary', {})
    overview_items = market_overview.get('items', [])
    themes = theme_dashboard.get('all', [])
    etf_leaders = etf_dashboard.get('leaders', [])
    etf_laggards = etf_dashboard.get('laggards', [])
    notable_overview = sorted(overview_items, key=lambda item: abs(item.get('change_pct') or 0), reverse=True)[:4]
    top_themes = themes[:3]
    weak_themes = sorted(themes, key=lambda item: item.get('avg_pct', 0))[:3]
    lead_group = etf_dashboard.get('summary', {}).get('best_group') or 'ETF groups'
    headline_text = '; '.join(item.get('title', '') for item in headlines[:4]) or 'No major headlines were available from the feed.'
    positive = summary.get('positive', 0)
    negative = summary.get('negative', 0)
    neutral = summary.get('neutral', 0)
    breadth_delta = positive - negative
    if breadth_delta >= 4:
        breadth_read = 'broadly constructive'
    elif breadth_delta <= -4:
        breadth_read = 'broadly defensive'
    elif breadth_delta > 0:
        breadth_read = 'slightly constructive but not one-sided'
    elif breadth_delta < 0:
        breadth_read = 'slightly defensive but not washed out'
    else:
        breadth_read = 'mixed and rotational'

    sentiment = 'Neutral'
    if positive > negative:
        sentiment = 'Bullish'
    elif positive < negative:
        sentiment = 'Bearish'

    paragraphs = [
        f"The tape is reading as {breadth_read}, with {positive} advancing instruments versus {negative} decliners and {neutral} roughly unchanged on the overview board. The biggest index and cross-asset swings are still coming from {_join_moves(notable_overview, 'label', 'change_pct', limit=4)}, so the first read is whether those leaders are dragging the tape or being confirmed by the rest of the board.",
        f"Leadership quality matters more than the raw index move, and right now the strongest thematic participation is coming from {_join_moves(top_themes, 'theme', 'avg_pct')}. When those same groups keep attracting buyers on dips, it usually means institutions are still willing to pay for growth, momentum, or narrative durability rather than just hiding in a handful of mega-caps.",
        f"The weak side of the tape is concentrated in {_join_moves(weak_themes, 'theme', 'avg_pct')}. That matters because if lagging groups keep getting sold while the winners continue to crowd, the market can still grind higher, but it becomes a narrower and less forgiving tape with faster rotations under the surface.",
        f"The ETF rotation board is pointing toward {lead_group} as the current capital destination. Leaders such as {_join_moves(etf_leaders, 'symbol', 'change_pct', limit=4)} are where money appears to be pressing risk, while laggards like {_join_moves(etf_laggards, 'symbol', 'change_pct', limit=4)} are the pockets being used as funding sources or avoided entirely.",
        f"Headline context is not just background noise here because the market is still trading macro and narrative cross-currents. The live feed is emphasizing: {headline_text} That means traders need to judge whether the news is reinforcing the current leadership map or creating the next rotation out of it.",
        f"From a tactical standpoint, the most important question is whether breadth starts to broaden behind the winners or whether leadership remains concentrated in only a few high-beta and theme-heavy groups. Broadening participation usually supports trend continuation, while narrow leadership often produces sharp index resilience on the surface but more failed breakouts and air pockets underneath.",
        f"Into the next session, the cleanest constructive setup would be leaders holding their gains, ETF inflows staying aligned with the strongest themes, and laggards stabilizing instead of accelerating lower. If those pieces diverge, the smarter posture is to trade more selectively, keep size tighter, and treat strength as tactical until the market proves it can sustain a broader rerating.",
    ]

    bullets = [
        {'tone': 'Bullish', 'text': f"Leadership is being carried by {top_themes[0]['theme']}."} if top_themes else {'tone': 'Neutral', 'text': 'Watch for clearer theme leadership before leaning too hard into one narrative.'},
        {'tone': 'Bearish', 'text': f"The weakest thematic pocket is {weak_themes[0]['theme']}, which is the first place to look for failed bounces."} if weak_themes else {'tone': 'Neutral', 'text': 'No weak theme standout has emerged yet.'},
        {'tone': 'Bullish', 'text': f"The top ETF flow proxy is {etf_leaders[0]['symbol']}, showing where capital is still comfortable pressing."} if etf_leaders else {'tone': 'Neutral', 'text': 'ETF leadership data is limited right now.'},
        {'tone': 'Bearish', 'text': f"The main ETF laggard is {etf_laggards[0]['symbol']}, which suggests where money is being pulled from first."} if etf_laggards else {'tone': 'Neutral', 'text': 'ETF laggard data is limited right now.'},
        {'tone': 'Neutral', 'text': 'Breadth confirmation matters more than the index print if you are deciding whether this move can actually persist.'},
        {'tone': 'Neutral', 'text': 'The best long setups are the names where headline support, theme leadership, and ETF rotation are all pointing the same way.'},
        {'tone': 'Bearish', 'text': 'If winners stop broadening and laggards keep making new lows, expect a more fragile and selective tape.'},
        {'tone': 'Neutral', 'text': 'The next real tell is whether capital stays in the current leadership bucket or rotates toward a different macro regime.'},
    ]

    return {
        'title': 'AI Market Brief',
        'sentiment': sentiment,
        'paragraphs': paragraphs,
        'bullets': bullets,
    }


def build_market_brief(market_overview: dict, theme_dashboard: dict, etf_dashboard: dict, headlines: List[dict]) -> dict:
    fallback = _fallback_market_brief(market_overview, theme_dashboard, etf_dashboard, headlines)

    prompt = f"""You are a senior market strategist writing a smart desk-style market brief for an active trader or portfolio manager.

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
  "paragraphs": ["paragraph 1", "paragraph 2", "paragraph 3", "paragraph 4", "paragraph 5", "paragraph 6", "paragraph 7"],
  "bullets": [
    {{"tone": "Bullish", "text": "short point"}},
    {{"tone": "Bearish", "text": "short point"}},
    {{"tone": "Neutral", "text": "short point"}},
    {{"tone": "Bullish", "text": "short point"}},
    {{"tone": "Bearish", "text": "short point"}},
    {{"tone": "Neutral", "text": "short point"}},
    {{"tone": "Bullish", "text": "short point"}},
    {{"tone": "Neutral", "text": "short point"}}
  ]
}}

Rules:
- Write exactly 7 paragraphs.
- Each paragraph should be 3-5 sentences.
- Write like a sharp trading-desk strategist, not a newsletter writer.
- Explain whether participation is broadening or narrowing beneath the surface.
- Focus on what is leading, what is lagging, where capital appears to be rotating, and what that says about the next session.
- Explicitly connect theme leadership, ETF rotation, macro/headline context, and tactical confirmation or failure signals.
- Mention what would confirm upside follow-through and what would warn that the move is getting fragile.
- Make the bullets actionable, specific, and one sentence each.
- Do not use markdown.
- Return JSON only."""

    result = _call_json(prompt, fallback, max_tokens=1800)
    return _merge_with_fallback(fallback, result)


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
    headline_impacts = _build_headline_impacts(headlines, trend_state)
    lead_impact = headline_impacts[0]['impact'] if headline_impacts else f'The feed is quiet, so traders will lean more heavily on the current {trend_state.lower()} and the nearest chart levels.'

    return {
        'bias': bias,
        'headline': f"{ticker} is in a {trend_state.lower()} with traders focused on follow-through.",
        'summary': f"{ticker} is part of {themes}. The stock is showing {ret_text} over the last month, which helps frame whether this is continuation behavior or just noise inside a base.",
        'trend': f"Trend state reads as {trend_state}. Price versus the 20, 50, and 200-day moving averages is the quickest read on whether institutions are still supporting the move.",
        'levels': f"The immediate reference zone is the recent 20-day range between {low20 if low20 is not None else 'n/a'} and {high20 if high20 is not None else 'n/a'}. A clean move through that band would change the chart character faster than any single headline.",
        'volume': f"Relative volume is running at {rel_vol_text} versus the 20-day average. That tells you whether the move has real sponsorship or is still vulnerable to fading back into the range.",
        'news_summary': f"Latest headlines: {news_rollup}",
        'news_reasoning': lead_impact,
        'key_events': _key_events_summary(headlines),
        'market_perception': _perception_before(detail),
        'expectation': _expectation_view(detail),
        'rerating_trigger': _rerating_read(detail, headlines, trend_state),
        'risk': f"RSI is {rsi if rsi is not None else 'n/a'}, so watch for exhaustion if momentum is already stretched. The latest headline context is: {top_headline}",
        'headline_impacts': headline_impacts,
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
  "key_events": "2-3 sentences on the key company events in the feed right now",
  "market_perception": "2-3 sentences on how the market seems to view the stock right now",
  "expectation": "2-3 sentences on what the market or Street appears to be expecting",
  "rerating_trigger": "1-2 sentences on what could force a rerating or de-rating",
  "risk": "1-2 sentences on what can go wrong",
  "headline_impacts": [
    {{"headline": "headline title", "tone": "Bullish", "summary": "one sentence recap", "impact": "1-2 sentence why it matters for the chart now"}},
    {{"headline": "headline title", "tone": "Bearish", "summary": "one sentence recap", "impact": "1-2 sentence why it matters for the chart now"}}
  ]
}}

Rules:
- Speak like a trader, not a textbook.
- Explain what is happening, not just indicators in isolation.
- Mention whether the chart looks like breakout, pullback, base, or breakdown behavior.
- Explicitly explain the key company events in the feed right now.
- Explain the current market perception and what expectations look like.
- Use the headline_impacts array to explain the latest news one headline at a time.
- Return JSON only."""

    result = _call_json(prompt, fallback, max_tokens=1100)
    merged = _merge_with_fallback(fallback, result)
    if not isinstance(merged.get('headline_impacts'), list) or not merged.get('headline_impacts'):
        merged['headline_impacts'] = fallback.get('headline_impacts', [])
    return merged


def _fallback_earnings_brief(earnings_tracker: dict) -> dict:
    items = earnings_tracker.get('items', [])
    summary = earnings_tracker.get('summary', {})
    recent_items = [item for item in items if item.get('status') == 'Recent']
    upcoming_items = [item for item in items if item.get('status') in ('Today', 'Upcoming')]
    yesterday_items = [item for item in recent_items if item.get('days_until') == -1]
    top_names = upcoming_items[:5] or items[:5]
    strongest = sorted(items, key=lambda item: item.get('change_pct') or 0, reverse=True)[:4]
    weakest = sorted(items, key=lambda item: item.get('change_pct') or 0)[:4]
    today_items = [item for item in items if item.get('status') == 'Today']
    today_bmo = [item.get('ticker') for item in today_items if item.get('report_time') == 'BMO'][:4]
    today_amc = [item.get('ticker') for item in today_items if item.get('report_time') == 'AMC'][:4]
    today_reported = [item for item in today_items if item.get('reported_eps') is not None or item.get('surprise_pct') is not None]
    leaning_names = sorted(
        [item for item in upcoming_items if item.get('change_pct') is not None],
        key=lambda item: abs(item.get('change_pct') or 0),
        reverse=True,
    )[:4]
    post_print_dispersion = sorted(
        [item for item in recent_items if item.get('change_pct') is not None],
        key=lambda item: abs(item.get('change_pct') or 0),
        reverse=True,
    )[:4]
    yesterday_dispersion = sorted(
        [item for item in yesterday_items if item.get('change_pct') is not None],
        key=lambda item: abs(item.get('change_pct') or 0),
        reverse=True,
    )[:4]

    theme_counts = {}
    for item in items:
        for theme in item.get('themes', []):
            theme_counts[theme] = theme_counts.get(theme, 0) + 1
    top_themes = sorted(theme_counts.items(), key=lambda pair: pair[1], reverse=True)[:5]
    theme_text = ', '.join(theme for theme, _ in top_themes) or 'no clear theme concentration yet'
    bmo_text = ', '.join(today_bmo) or 'none'
    amc_text = ', '.join(today_amc) or 'none'
    yesterday_text = _join_moves(yesterday_dispersion or post_print_dispersion, 'ticker', 'change_pct', limit=4)
    today_names_text = ', '.join(item.get('ticker', 'n/a') for item in today_items[:6]) or 'no tracked names yet'
    today_reported_text = _join_moves(today_reported, 'ticker', 'change_pct', limit=4)

    return {
        'headline': 'The earnings calendar is active enough that traders should treat it like a catalyst map, not just a date list.',
        'summary': f"There are {summary.get('total_events', 0)} earnings events in the current tracker, with {summary.get('recent_count', 0)} recent prints, {summary.get('today_count', 0)} due today, and {summary.get('next_7_days', 0)} more scheduled over the next week. The nearest names on deck are {', '.join(item.get('ticker', 'n/a') for item in top_names) or 'not available yet'}, with today's before-the-open names at {bmo_text} and after-the-close names at {amc_text}.",
        'yesterday_review': f"Yesterday's earnings review is centered on {yesterday_text}. The real read is whether those reactions are being accepted as durable estimate resets or are already fading back once the first headline excitement wears off.",
        'today_setup': f"Today's earnings focus is {today_names_text}. Traders should care most about where the tape is already leaning into the print, especially around BMO names at {bmo_text} and AMC names at {amc_text}.",
        'today_commentary': (
            f"Names that have already spoken today are reading as {today_reported_text}. The important question is whether what management said is strong enough to hold the first reaction once cash trading settles."
            if today_reported else
            f"Most of today's slate has not spoken yet, so the cleaner read is the setup going in: {_join_moves(leaning_names, 'ticker', 'change_pct', limit=4)}. That price lean tells you where expectations may already be stretched before management even speaks."
        ),
        'focus': f"The setups already leaning hardest into earnings are {_join_moves(leaning_names, 'ticker', 'change_pct', limit=4)}. The biggest post-print reactions so far are {_join_moves(post_print_dispersion, 'ticker', 'change_pct', limit=4)}, so traders should watch whether the next reports confirm momentum or fade once expectations are tested.",
        'themes': f"The current slate clusters most around {theme_text}. That matters because a strong or weak report can spill over into peers, sector baskets, and the narrative complex tied to the same theme.",
        'risk': 'The main risk is that crowded names are already pricing in good news, which raises the odds of post-print reversals even on decent numbers. Watch whether implied expectations look too high versus the current tape.'
    }


def build_earnings_brief(earnings_tracker: dict) -> dict:
    fallback = _fallback_earnings_brief(earnings_tracker)

    prompt = f"""You are writing a concise earnings tracker brief for an active trader.

Earnings tracker summary:
{_json_text(earnings_tracker.get('summary', {}))}

Recent and upcoming earnings rows:
{_json_text(earnings_tracker.get('items', [])[:30])}

Return ONLY valid JSON with this exact structure:
{{
  "headline": "one sentence summary",
  "summary": "2-3 sentences on what the earnings slate looks like",
  "yesterday_review": "2-3 sentences reviewing yesterday's reports and what price reaction is saying",
  "today_setup": "2-3 sentences on today's earnings names and what traders should watch before the print",
  "today_commentary": "2-3 sentences on what today's reporters already said, or what the market is waiting to hear",
  "focus": "2-3 sentences on what traders should focus on",
  "themes": "2-3 sentences on theme or sector concentration",
  "risk": "1-2 sentences on the main risks"
}}

Rules:
- Speak like a trader, not a textbook.
- Focus on concentration, expectations, and where the biggest reactions may happen.
- Include a specific yesterday review section and a specific today setup section.
- If some names already reported today, explain what management or the print appears to be saying and why the tape cares.
- Mention when price action suggests names may already be leaning into the print.
- Call out if the busiest names are before the open or after the close.
- Explain what recent price reaction is saying about expectations, not just the dates.
- Include both recent and upcoming earnings context if available.
- Return JSON only."""

    result = _call_json(prompt, fallback, max_tokens=900)
    merged = _merge_with_fallback(fallback, result)
    merged.pop('error', None)
    return merged


def _market_context_read(market_context: dict) -> str:
    overview = market_context.get('overview_summary', {})
    positive = overview.get('positive', 0)
    negative = overview.get('negative', 0)
    best_theme = market_context.get('best_theme') or 'no clear leading theme'
    best_group = market_context.get('best_group') or 'no dominant ETF group'
    headline_rollup = '; '.join(item.get('title', '') for item in market_context.get('market_headlines', [])[:3]) or 'No major market headlines were supplied.'
    tone = 'mixed'
    if positive > negative:
        tone = 'constructive'
    elif negative > positive:
        tone = 'defensive'
    return f"The broader tape is currently {tone}, with {positive} advancing market proxies versus {negative} decliners. Leadership is leaning toward {best_theme}, while ETF rotation is pointing at {best_group}. Market headlines in the background include: {headline_rollup}"


def _transcript_catalyst_lines(transcript: dict) -> List[dict]:
    lines = []
    for item in transcript.get('catalysts', [])[:4]:
        lines.append({
            'title': item.get('theme') or 'Transcript catalyst',
            'quote': item.get('quote') or '',
            'why_it_matters': item.get('why_it_matters') or 'This could matter if the market starts to treat it as a durable change rather than a one-quarter soundbite.',
        })
    return lines


def _fallback_earnings_deep_dive(payload: dict) -> dict:
    item = payload.get('earnings') or {}
    detail = payload.get('detail') or {}
    headlines = payload.get('headlines') or []
    transcript = payload.get('transcript') or {}
    catalyst_lines = _transcript_catalyst_lines(transcript)
    company_name = detail.get('company_name') or item.get('company_name') or item.get('ticker') or 'The company'
    reaction = item.get('reaction_pct')
    reaction_text = f'{reaction:+.2f}%' if reaction is not None else 'n/a'
    transcript_signal = catalyst_lines[0]['why_it_matters'] if catalyst_lines else ''
    transcript_quote = catalyst_lines[0]['quote'] if catalyst_lines else ''
    transcript_summary = transcript.get('management_excerpt') or transcript.get('digest') or ''
    fundamentals = _fundamental_snapshot(detail)
    valuation = _valuation_context(detail)

    market_view = (
        f"{_market_context_read(payload.get('market_context') or {})} Against that backdrop, {company_name} is reacting {reaction_text} "
        f"in {item.get('reaction_label') or '1D'}, so the market is clearly making a fresh judgment on the quarter. {valuation}"
    )

    impact_news = (
        f"Impact news around the print includes {_join_headlines(headlines, limit=3)}. "
        f"{fundamentals}"
    )

    market_perception_before = (
        f"Before earnings, the market largely viewed {company_name} as {_perception_before(detail)} "
        f"{_expectation_view(detail)}"
    )

    what_changed_after = (
        f"After earnings, the story changed because {item.get('narrative_shift') or item.get('after_earnings') or 'the quarter forced investors to reassess the prior thesis.'} "
        f"{transcript_signal} {item.get('ai_reasoning') or ''} "
        f"{f'Management sounded most explicit in saying: {transcript_quote}' if transcript_quote else transcript_summary[:260]}"
    ).strip()

    bull_case = (
        f"The bullish case is that management commentary and the live reaction are supporting a real reset in how investors think about {company_name}. "
        f"If the new margin, demand, or product commentary feeds into estimates, the stock can rerate beyond the first move. {transcript_signal or valuation}"
    )

    bear_case = (
        f"The bearish case is that the print only looks better on the surface and the market is still paying too much for an uncertain setup. "
        "If guidance, margin durability, or demand quality fail to hold up, the post-earnings move can unwind fast."
    )

    thesis_breaker = (
        "The thesis breaks if the supposed improvement does not show up in the next few quarters, especially if margins, growth, or cash generation slide back toward the old profile."
    )

    today_view = (
        f"My current view: {company_name} deserves attention because the quarter moved the conversation, but the decisive tell is whether the first reaction becomes sustained follow-through. "
        f"Treat transcript comments and price action together, not in isolation. {valuation}"
    )

    return {
        'market_view': market_view,
        'impact_news': impact_news,
        'market_perception_before': market_perception_before,
        'what_changed_after': what_changed_after,
        'transcript_catalysts': catalyst_lines,
        'bull_case': bull_case,
        'bear_case': bear_case,
        'thesis_breaker': thesis_breaker,
        'today_view': today_view,
    }


def build_earnings_deep_dive(payload: dict) -> dict:
    fallback = _fallback_earnings_deep_dive(payload)

    prompt = f"""You are a senior hedge-fund analyst writing a sharp post-earnings view for an active trader.

Current UTC time:
{datetime.now(timezone.utc).isoformat()}

Broader market context:
{_json_text(payload.get('market_context', {}))}

Earnings row:
{_json_text(payload.get('earnings', {}))}

Company detail:
{_json_text(payload.get('detail', {}))}

Relevant company news:
{_json_text(payload.get('headlines', [])[:6])}

Transcript metadata:
{_json_text({
    'status': (payload.get('transcript') or {}).get('status'),
    'provider': (payload.get('transcript') or {}).get('provider'),
    'quarter': (payload.get('transcript') or {}).get('quarter'),
    'management_excerpt': (payload.get('transcript') or {}).get('management_excerpt'),
    'qa_excerpt': (payload.get('transcript') or {}).get('qa_excerpt'),
    'catalysts': (payload.get('transcript') or {}).get('catalysts'),
})}

Return ONLY valid JSON with this exact structure:
{{
  "market_view": "2-3 sentences on your actual current market view for this setup in today's tape",
  "impact_news": "2-3 sentences on the relevant impact news around the print and sector",
  "market_perception_before": "2-3 sentences on how the market saw the stock before earnings",
  "what_changed_after": "2-4 sentences on what changed after earnings and why that matters",
  "transcript_catalysts": [
    {{"title": "short catalyst label", "quote": "short excerpt under 220 chars", "why_it_matters": "1-2 sentences"}},
    {{"title": "short catalyst label", "quote": "short excerpt under 220 chars", "why_it_matters": "1-2 sentences"}}
  ],
  "bull_case": "2-3 sentences on the bull case from here",
  "bear_case": "2-3 sentences on the bear case from here",
  "thesis_breaker": "1-2 sentences on what would invalidate or make the thesis moot",
  "today_view": "2-3 sentences with your actual desk-style conclusion today"
}}

Rules:
- Do not recycle generic earnings commentary.
- Explicitly compare the market perception before earnings to what changed after earnings.
- If the transcript suggests a structural improvement, say exactly what changed: margins, demand quality, pricing, cash flow, product mix, AI monetization, backlog, etc.
- Use transcript excerpts only when they actually matter.
- Speak like a real market participant, not a cheerful assistant.
- Be willing to say the market may be overreacting, underreacting, or still confused.
- Return JSON only."""

    result = _call_json(prompt, fallback, max_tokens=1400)
    merged = _merge_with_fallback(fallback, result)
    if not isinstance(merged.get('transcript_catalysts'), list) or not merged.get('transcript_catalysts'):
        merged['transcript_catalysts'] = fallback.get('transcript_catalysts', [])
    return merged


def _fallback_watchlist_thesis(payload: dict) -> dict:
    detail = payload.get('detail') or {}
    headlines = payload.get('headlines') or []
    company_name = detail.get('company_name') or detail.get('ticker') or 'The company'
    fundamentals = _fundamental_snapshot(detail)
    valuation = _valuation_context(detail)

    return {
        'market_view': (
            f"{_market_context_read(payload.get('market_context') or {})} Against that background, {company_name} matters because its own narrative can either confirm or break the current leadership map. {valuation}"
        ),
        'impact_news': (
            f"Fresh impact news for {company_name} includes {_join_headlines(headlines, limit=3)}. "
            f"That matters because the stock is only actionable when the company-specific story and the broader tape line up. {fundamentals}"
        ),
        'what_market_is_pricing_for': (
            f"The market is pricing {company_name} for {_rerating_read(detail, headlines, 'Constructive uptrend').lower()} "
            f"{_expectation_view(detail)} {valuation}"
        ),
        'bull_case': (
            f"The bullish case is that {company_name} proves the better version of the story: cleaner execution, better margins, and a catalyst that pulls estimates higher. "
            f"If that happens while the broader market still rewards the theme, the stock can stay on offense. {fundamentals}"
        ),
        'bear_case': (
            f"The bearish case is that the market is already paying for a better story than the company can deliver. "
            f"If the next update disappoints or the sector loses sponsorship, the stock can rerate lower even without a disaster quarter. {valuation}"
        ),
        'key_negatives': (
            "The main negatives are valuation compression, stale catalysts, and the risk that good narrative language never shows up in margins, demand, or cash flow."
        ),
        'investment_moot': (
            "The watchlist thesis becomes moot if the expected catalyst disappears, the business reverts back toward the old lower-quality profile, or the broader market stops paying for the theme."
        ),
        'today_view': (
            f"My current view: keep {company_name} on the list only if you can still explain what the market is paying for and what specific evidence would confirm or kill that thesis. {fundamentals}"
        ),
    }


def build_watchlist_thesis(payload: dict) -> dict:
    fallback = _fallback_watchlist_thesis(payload)

    prompt = f"""You are a portfolio manager writing a live watchlist thesis note for one stock.

Current UTC time:
{datetime.now(timezone.utc).isoformat()}

Broader market context:
{_json_text(payload.get('market_context', {}))}

Company detail:
{_json_text(payload.get('detail', {}))}

Recent company news:
{_json_text(payload.get('headlines', [])[:6])}

Return ONLY valid JSON with this exact structure:
{{
  "market_view": "2-3 sentences on your current market view for this stock in today's tape",
  "impact_news": "2-3 sentences on the impact news that matters for the stock right now",
  "what_market_is_pricing_for": "2-4 sentences on what the market appears to be paying for",
  "bull_case": "2-3 sentences on the bullish case",
  "bear_case": "2-3 sentences on the bearish case or negatives",
  "key_negatives": "1-2 sentences on the key negatives",
  "investment_moot": "1-2 sentences on what would make the thesis moot and remove it from a serious watchlist",
  "today_view": "2-3 sentences with your actual current conclusion"
}}

Rules:
- Be specific about what the market is pricing for.
- Tie the thesis to margins, growth durability, product cycle, AI monetization, backlog, pricing, demand quality, or cash flow when relevant.
- Explain both the bullish version and the negative version of the story.
- Tell me what would make the watchlist thesis moot.
- Write like a real investor memo, not a generic app summary.
- Return JSON only."""

    result = _call_json(prompt, fallback, max_tokens=1100)
    return _merge_with_fallback(fallback, result)
