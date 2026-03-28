#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const https = require('https');

const SGT_OFFSET_MS = 8 * 60 * 60 * 1000;
const USER_AGENT = 'Mozilla/5.0 (compatible; morning-brief-bot/1.0)';

function nowSgt() {
  return new Date(Date.now() + SGT_OFFSET_MS);
}

function formatSgtDateParts(date = nowSgt()) {
  const weekday = new Intl.DateTimeFormat('en-US', { weekday: 'long', timeZone: 'Asia/Singapore' }).format(new Date());
  const month = new Intl.DateTimeFormat('en-US', { month: 'short', timeZone: 'Asia/Singapore' }).format(new Date());
  const day = new Intl.DateTimeFormat('en-US', { day: '2-digit', timeZone: 'Asia/Singapore' }).format(new Date());
  const year = new Intl.DateTimeFormat('en-US', { year: 'numeric', timeZone: 'Asia/Singapore' }).format(new Date());
  return { weekday, display: `${month} ${day}, ${year}` };
}

function getJson(url) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, { method: 'GET', headers: { 'User-Agent': USER_AGENT } }, (res) => {
      let data = '';
      res.on('data', (c) => (data += c));
      res.on('end', () => {
        if (res.statusCode < 200 || res.statusCode >= 300) return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        try {
          resolve(JSON.parse(data));
        } catch (err) {
          reject(new Error(`Invalid JSON from ${url}: ${err.message}`));
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(10000, () => req.destroy(new Error(`Timeout for ${url}`)));
    req.end();
  });
}

function getText(url) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, { method: 'GET', headers: { 'User-Agent': USER_AGENT } }, (res) => {
      let data = '';
      res.on('data', (c) => (data += c));
      res.on('end', () => {
        if (res.statusCode < 200 || res.statusCode >= 300) return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        resolve(data);
      });
    });
    req.on('error', reject);
    req.setTimeout(10000, () => req.destroy(new Error(`Timeout for ${url}`)));
    req.end();
  });
}

function parseConfig() {
  const candidates = [
    process.env.MARKET_BRIEF_CONFIG,
    'C:\\Users\\andyz.claude\\market-brief-config.json',
    '/mnt/c/Users/andyz.claude/market-brief-config.json',
    path.join(process.cwd(), 'market-brief-config.json')
  ].filter(Boolean);

  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        const cfg = JSON.parse(fs.readFileSync(p, 'utf8'));
        if (!cfg.bot_token || !Array.isArray(cfg.chat_ids) || !cfg.watchlist || typeof cfg.watchlist !== 'object') {
          throw new Error(`Invalid config schema in ${p}`);
        }
        return { ...cfg, _configPath: p };
      }
    } catch (err) {
      throw new Error(`Failed reading config ${p}: ${err.message}`);
    }
  }
  throw new Error('market-brief-config.json not found. Set MARKET_BRIEF_CONFIG to the file path.');
}

async function fetchYahooQuote(symbol) {
  try {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=3mo&interval=1d`;
    const data = await getJson(url);
    const result = data?.chart?.result?.[0];
    const meta = result?.meta || {};
    const closes = result?.indicators?.quote?.[0]?.close || [];
    const valid = closes.filter((v) => typeof v === 'number');
    const lastClose = meta.regularMarketPrice ?? valid[valid.length - 1] ?? null;
    const prevClose = meta.chartPreviousClose ?? null;
    const dayPct = lastClose && prevClose ? ((lastClose - prevClose) / prevClose) * 100 : null;
    const first = valid[0] ?? null;
    const threeMonthPct = lastClose && first ? ((lastClose - first) / first) * 100 : null;
    return { symbol, price: lastClose, dayPct, threeMonthPct };
  } catch (_) {
    return { symbol, price: null, dayPct: null, threeMonthPct: null };
  }
}

function parseGoogleRss(xml, limit = 5) {
  const items = [];
  const blocks = xml.match(/<item>[\s\S]*?<\/item>/g) || [];
  for (const block of blocks.slice(0, limit)) {
    const title = (block.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>/)?.[1] || block.match(/<title>(.*?)<\/title>/)?.[1] || '').trim();
    const pubDate = (block.match(/<pubDate>(.*?)<\/pubDate>/)?.[1] || '').trim();
    if (title) items.push({ title: title.replace(/ - [^-]+$/, ''), pubDate });
  }
  return items;
}

async function fetchNews(query, limit = 5) {
  try {
    const url = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=en-US&gl=US&ceid=US:en`;
    const xml = await getText(url);
    return parseGoogleRss(xml, limit);
  } catch (_) {
    return [];
  }
}

function classifyHeadline(title) {
  const t = title.toLowerCase();
  const bullish = ['surge', 'rally', 'beat', 'gain', 'record high', 'upgrade', 'eases inflation', 'cuts rates', 'stimulus'];
  const bearish = ['selloff', 'drop', 'miss', 'downgrade', 'tariff', 'war', 'sanction', 'hawkish', 'inflation rises'];
  if (bullish.some((k) => t.includes(k))) return '🟢';
  if (bearish.some((k) => t.includes(k))) return '🔴';
  return '🟡';
}

function fmtPct(v, digits = 1) {
  if (typeof v !== 'number' || Number.isNaN(v)) return '—';
  const s = v >= 0 ? '▲' : '▼';
  return `${s}${Math.abs(v).toFixed(digits)}%`;
}

function fmtNum(v, digits = 2) {
  if (typeof v !== 'number' || Number.isNaN(v)) return '—';
  return v.toLocaleString('en-US', { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

function renderMacroTable(rows) {
  if (!rows.length) return '<code>No major releases.</code>';
  const head = '<code>Indicator      Actual   Est    Prior   Signal</code>';
  const body = rows.map((r) => `<code>${(r.indicator || '—').padEnd(14)} ${(r.actual || '—').padEnd(7)} ${(r.est || '—').padEnd(6)} ${(r.prior || '—').padEnd(7)} ${r.signal || '🟡'}</code>`).join('\n');
  return `${head}\n${body}`;
}

async function fetchEarningsSnapshot() {
  const today = new Date().toISOString().slice(0, 10);
  const yesterday = new Date(Date.now() - 86400000).toISOString().slice(0, 10);
  const urls = [
    `https://financialmodelingprep.com/api/v3/earning_calendar?from=${yesterday}&to=${today}&apikey=demo`,
    `https://financialmodelingprep.com/api/v3/earning_calendar_confirmed?from=${yesterday}&to=${today}&apikey=demo`
  ];
  const all = await Promise.all(urls.map((u) => getJson(u).catch(() => [])));
  const merged = [...(Array.isArray(all[0]) ? all[0] : []), ...(Array.isArray(all[1]) ? all[1] : [])];
  const map = new Map();
  merged.forEach((r) => {
    if (r?.symbol && !map.has(r.symbol)) map.set(r.symbol, r);
  });
  return [...map.values()].slice(0, 8);
}

async function sendMessage(token, chatId, text) {
  const body = JSON.stringify({ chat_id: chatId, text, parse_mode: 'HTML' });
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: 'api.telegram.org',
      path: `/bot${token}/sendMessage`,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
    }, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.ok) resolve(parsed);
          else reject(new Error(JSON.stringify(parsed)));
        } catch (err) {
          reject(new Error(`Telegram response parse error: ${err.message}`));
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function main() {
  const config = parseConfig();
  const { weekday, display } = formatSgtDateParts();
  const dateTag = new Date().toISOString().slice(0, 10);

  const sectorMap = {
    XLK: 'Technology', XLC: 'Communication Svcs', XLF: 'Financials', XLE: 'Energy', XLB: 'Materials', XLI: 'Industrials', XLY: 'Consumer Discret.', XLP: 'Consumer Staples', XLV: 'Healthcare', XLU: 'Utilities', XLRE: 'Real Estate', IGV: 'Software', SMH: 'Semiconductors', IBB: 'Biotech', KRE: 'Regional Banks', IYT: 'Transports', XOP: 'Oil & Gas', HACK: 'Cybersecurity', XRT: 'Retail', ITB: 'Home Construction', KWEB: 'China Internet', ITA: 'Aerospace'
  };

  const trackers = ['SPY', 'QQQ', 'IWM', 'DIA', '^VIX', 'GC=F', 'CL=F', 'BZ=F', 'BTC-USD', 'DX-Y.NYB', '^TNX', 'ES=F', 'NQ=F', 'YM=F', '^N225', '^HSI', '^AXJO', '^STI'];
  const watchlist = config.watchlist;
  const watchTickers = [...new Set(Object.values(watchlist).flat())];

  const [marketNews, macroNews, calendarNews, earningsNews, aiNews, dcNews, cryptoNews, earningsRaw, trackerQuotes, sectorQuotes, watchQuotes] = await Promise.all([
    fetchNews(`stock market news impact today ${dateTag}`, 6),
    fetchNews(`economic data released today CPI PPI jobs Fed ${dateTag}`, 8),
    fetchNews(`economic calendar this week ${dateTag}`, 6),
    fetchNews(`earnings reports today after hours yesterday ${dateTag}`, 8),
    fetchNews(`AI chips semiconductors news ${dateTag}`, 4),
    fetchNews(`data center hyperscaler news ${dateTag}`, 4),
    fetchNews(`crypto stocks news ${dateTag}`, 4),
    fetchEarningsSnapshot().catch(() => []),
    Promise.all(trackers.map(fetchYahooQuote)),
    Promise.all(Object.keys(sectorMap).map(fetchYahooQuote)),
    Promise.all(watchTickers.map(fetchYahooQuote))
  ]);

  const q = Object.fromEntries(trackerQuotes.map((x) => [x.symbol, x]));
  const sq = sectorQuotes.map((x) => ({ ...x, name: sectorMap[x.symbol] })).filter((x) => typeof x.dayPct === 'number').sort((a, b) => b.dayPct - a.dayPct);
  const wq = Object.fromEntries(watchQuotes.map((x) => [x.symbol, x]));

  const topNews = marketNews.slice(0, 5).map((n) => ({ ...n, signal: classifyHeadline(n.title) }));
  const macroRows = macroNews.slice(0, 6).map((n) => ({ indicator: n.title.slice(0, 13), actual: '—', est: '—', prior: '—', signal: '🟡' }));

  const earningsList = Array.isArray(earningsRaw) ? earningsRaw.slice(0, 8) : [];

  const msg1 = [
    `🌅 <b>Morning Brief — ${weekday}, ${display} SGT</b>`,
    '<i>Overnight U.S. close · Asian session opening</i>',
    '',
    '<b>📰 MARKET-IMPACT NEWS</b>',
    ...(topNews.length ? topNews.map((n) => `${n.signal} <b>[${n.title}]</b> — Potential cross-asset sentiment driver for Asia open.`) : ['🟡 <b>[No clear headline concentration]</b> — No single dominant catalyst; monitor futures tone.']),
    '',
    '<b>📊 MACRO DATA SCORECARD</b>',
    renderMacroTable(macroRows),
    '',
    `📅 <b>Watch This Week:</b> ${calendarNews.slice(0, 3).map((n) => n.title).join(' | ') || '—'}`,
    '',
    '<b>💼 EARNINGS ROUNDUP</b>',
    ...(earningsList.length ? earningsList.map((e) => `<b>${e.symbol || '—'}</b> — EPS ${e.eps ?? '—'} vs est ${e.epsEstimated ?? '—'} | Rev ${e.revenue ?? '—'} vs est ${e.revenueEstimated ?? '—'} | Stock —`) : ['No major earnings.']),
    '',
    '<b>📈 INDICES &amp; MACRO ASSETS</b>',
    `SPY ${fmtPct(q.SPY?.dayPct)} | QQQ ${fmtPct(q.QQQ?.dayPct)} | IWM ${fmtPct(q.IWM?.dayPct)} | DIA ${fmtPct(q.DIA?.dayPct)} | VIX ${fmtNum(q['^VIX']?.price, 1)}`,
    `Gold $${fmtNum(q['GC=F']?.price, 2)} | WTI $${fmtNum(q['CL=F']?.price, 2)} | BTC $${fmtNum(q['BTC-USD']?.price, 0)} | DXY ${fmtNum(q['DX-Y.NYB']?.price, 2)} | 10Y ${fmtNum(q['^TNX']?.price / 100, 2)}%`,
    '',
    `ES fut ${fmtPct(q['ES=F']?.dayPct)} | NQ fut ${fmtPct(q['NQ=F']?.dayPct)}`,
    `Nikkei ${fmtNum(q['^N225']?.price, 0)} | ASX ${fmtNum(q['^AXJO']?.price, 0)} | HSI ${fmtNum(q['^HSI']?.price, 0)} | STI ${fmtNum(q['^STI']?.price, 0)}`,
    '',
    '<b>🔭 FORWARD IMPACT (Next 5 Days)</b>',
    '🟢 Softer inflation / lower yields can extend growth leadership if macro prints cooperate.',
    '🔴 Repricing higher in rates or geopolitical escalation can pressure high-beta themes quickly.',
    '🟡 Key swing factor: incoming macro calendar and Fed tone vs current risk-on positioning.',
    '',
    '<i>Generated 8:30 AM SGT · Morning Brief 1/2</i>'
  ].join('\n');

  const best = sq[0];
  const worst = sq[sq.length - 1];
  const avg = sq.length ? sq.reduce((a, b) => a + b.dayPct, 0) / sq.length : null;

  function themeBlock(key, title, emoji, reasonPool) {
    const tickers = watchlist[key] || [];
    let bestTicker = null;
    tickers.forEach((t) => {
      if (!bestTicker || (wq[t]?.dayPct ?? -Infinity) > (wq[bestTicker]?.dayPct ?? -Infinity)) bestTicker = t;
    });
    const line = tickers.map((t) => {
      const text = `${t} ${fmtPct(wq[t]?.dayPct)}`;
      return t === bestTicker ? `<b>${text}</b>` : text;
    }).join(' | ');
    const why = reasonPool[0]?.title ? `${reasonPool[0].title}.` : 'No specific catalyst; tracking macro tone.';
    return `${emoji} <b>${title}</b>\n${line}\n💡 ${why}`;
  }

  const heatRows = sq.map((s, idx) => {
    const badge = s.dayPct >= 2 ? '🔥' : s.dayPct <= -1 ? '🧊' : '  ';
    const day = `${s.dayPct >= 0 ? '▲' : '▼'}${Math.abs(s.dayPct).toFixed(1)}%`;
    const m3 = `${s.threeMonthPct >= 0 ? '+' : ''}${(s.threeMonthPct ?? 0).toFixed(1)}%`;
    return `<code>${String(idx + 1).padStart(2)} ${s.name.padEnd(18)} ${badge} ${day.padEnd(6)} ${m3}</code>`;
  });

  const msg2 = [
    '<b>🏭 SECTOR HEATMAP</b>',
    `<i>Best: ${best?.name || '—'} ${fmtPct(best?.dayPct)} · Worst: ${worst?.name || '—'} ${fmtPct(worst?.dayPct)} · Avg: ${fmtPct(avg)}</i>`,
    '',
    '<code> # Sector             Day     3M   </code>',
    ...heatRows,
    '',
    '<b>🎯 THEME WATCHLIST</b>',
    '',
    themeBlock('aichips', 'AI CHIPS', '🤖', aiNews),
    '',
    themeBlock('neoclouds', 'NEOCLOUDS', '⛅', aiNews),
    '',
    themeBlock('datacenters', 'DATA CENTERS', '🏢', dcNews),
    '',
    themeBlock('cooling', 'COOLING', '❄️', dcNews),
    '',
    themeBlock('semieqp', 'SEMI EQUIPMENT', '⚙️', aiNews),
    '',
    themeBlock('memory', 'MEMORY', '💾', aiNews),
    '',
    themeBlock('fiber', 'FIBER / OPTICAL', '🔌', dcNews),
    '',
    themeBlock('crypto', 'CRYPTO', '₿', cryptoNews),
    '',
    themeBlock('aisoft', 'AI SOFTWARE', '💻', aiNews),
    '',
    themeBlock('aienergy', 'AI ENERGY', '⚡', dcNews),
    '',
    themeBlock('space', 'SPACE', '🚀', marketNews),
    '',
    '<i>Generated 8:30 AM SGT · Morning Brief 2/2</i>'
  ].join('\n');

  for (const chatId of config.chat_ids) {
    try {
      await sendMessage(config.bot_token, chatId, msg1);
      console.log(`✅ Message 1 sent to ${chatId}`);
    } catch (e) {
      console.log(`Retrying msg1 for ${chatId}...`);
      await new Promise((r) => setTimeout(r, 10000));
      await sendMessage(config.bot_token, chatId, msg1);
    }

    await new Promise((r) => setTimeout(r, 1000));

    try {
      await sendMessage(config.bot_token, chatId, msg2);
      console.log(`✅ Message 2 sent to ${chatId}`);
    } catch (e) {
      console.log(`Retrying msg2 for ${chatId}...`);
      await new Promise((r) => setTimeout(r, 10000));
      await sendMessage(config.bot_token, chatId, msg2);
    }
  }

  console.log(`Completed morning brief dispatch using config: ${config._configPath}`);
}

main().catch((err) => {
  console.error('Morning brief failed:', err.message);
  process.exit(1);
});
