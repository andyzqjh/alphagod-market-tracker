(() => {
  const API_BASE_CANDIDATES = window.location.protocol === 'file:'
    ? ['https://alphagod-market-tracker.onrender.com', 'http://localhost:8000']
    : [''];
  const CHART_OVERRIDES = {
    '^GSPC': 'SP:SPX',
    '^IXIC': 'NASDAQ:IXIC',
    '^DJI': 'DJ:DJI',
    '^RUT': 'RUSSELL:RUT',
    'BTC-USD': 'BITSTAMP:BTCUSD',
  };
  const TAB_ORDER = ['overview', 'premarket', 'postmarket', 'themes', 'flows', 'rrg', 'earnings', 'chart'];
  const TAB_LABELS = {
    overview: 'Overview',
    premarket: 'Pre-market',
    postmarket: 'Post-market',
    themes: 'Themes',
    flows: 'ETF Flows',
    rrg: 'ETF RRG',
    earnings: 'Earnings',
    chart: 'Chart Desk',
  };
  const INTERVALS = [
    { label: '1h', value: '60' },
    { label: '4h', value: '240' },
    { label: '1D', value: 'D' },
    { label: '1W', value: 'W' },
  ];
  const AUTO_REFRESH_MS = 300000;
  const QUADRANT_COLORS = {
    Leading: '#34d399',
    Weakening: '#f59e0b',
    Lagging: '#fb7185',
    Improving: '#60a5fa',
  };
  const THEME_FALLBACKS = {
    'Fiber Optics & Connectivity': ['AXTI', 'AAOI', 'LITE', 'CIEN', 'COHR', 'VIAV', 'INFN', 'COMM', 'IPGP'],
    'Memory & Storage': ['MU', 'WDC', 'STX', 'SNDK', 'NTAP', 'PURE', 'SIMO'],
    'AI Related Energy': ['BE', 'AMPX', 'POWL', 'FTAI', 'TE', 'FCEL', 'PLUG', 'BLDP'],
    'Semiconductor Equipment': ['LRCX', 'AMAT', 'KLAC', 'TER', 'FORM', 'ONTO', 'ACLS', 'CAMT', 'ICHR', 'ENTG'],
    Cooling: ['VRT', 'SMCI', 'AAON', 'MOD', 'JCI', 'TT'],
    'Computing & AI Software': ['MRVL', 'APP', 'DELL', 'TTMI', 'FSLY', 'NET', 'SNOW', 'DDOG', 'MDB'],
    Crypto: ['COIN', 'HOOD', 'MSTR', 'RIOT', 'MARA', 'CLSK', 'CIFR', 'HUT', 'IREN', 'APLD', 'CORZ'],
    'Data Center': ['EQIX', 'DLR', 'AMT', 'VRT', 'APLD', 'NBIS', 'IREN', 'CIFR', 'CORZ'],
    'Defense & Aerospace': ['LMT', 'RTX', 'NOC', 'GD', 'KTOS', 'PLTR', 'CACI', 'SAIC', 'RKLB', 'ASTS'],
    'Biotech & Pharma': ['MRNA', 'BNTX', 'VRTX', 'REGN', 'BIIB', 'AMGN', 'GILD', 'HIMS', 'QURE', 'BHVN', 'CAPR'],
    'EV & Clean Energy': ['TSLA', 'RIVN', 'LCID', 'NIO', 'XPEV', 'LI', 'CHPT', 'BLNK', 'EVGO'],
    Cybersecurity: ['CRWD', 'ZS', 'PANW', 'S', 'FTNT', 'OKTA', 'CYBR', 'SAIL'],
    'Cloud & SaaS': ['CRM', 'NOW', 'ADBE', 'ORCL', 'INTU', 'WDAY', 'TEAM', 'HUBS'],
    Semiconductors: ['NVDA', 'AMD', 'INTC', 'QCOM', 'AVGO', 'TXN', 'ARM', 'MCHP', 'SWKS', 'QRVO'],
    'Oil & Gas': ['XOM', 'CVX', 'OXY', 'COP', 'EOG', 'SLB', 'HAL', 'WTI', 'BATL'],
  };

  const state = {
    activeTab: 'overview',
    clock: '',
    chartSymbol: 'SPY',
    chartInput: 'SPY',
    chartInterval: 'D',
    themeFilter: 'all',
    selectedTheme: '',
    overview: null,
    briefData: null,
    premarket: null,
    postmarket: null,
    themes: null,
    etfs: null,
    rrg: null,
    earnings: null,
    chartWorkspace: null,
    loading: {
      overview: false,
      brief: false,
      premarket: false,
      postmarket: false,
      themes: false,
      etfs: false,
      rrg: false,
      earnings: false,
      chart: false,
    },
    errors: {},
  };

  function esc(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/\"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function fmtNumber(value, digits = 2) {
    if (value == null || Number.isNaN(Number(value))) return 'n/a';
    return Number(value).toLocaleString('en-US', {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
  }

  function fmtPrice(value, prefix = true) {
    if (value == null || Number.isNaN(Number(value))) return 'n/a';
    const digits = Math.abs(Number(value)) >= 1000 ? 0 : 2;
    return `${prefix ? '$' : ''}${fmtNumber(value, digits)}`;
  }

  function fmtPercent(value) {
    if (value == null || Number.isNaN(Number(value))) return 'n/a';
    const number = Number(value);
    return `${number > 0 ? '+' : ''}${number.toFixed(2)}%`;
  }

  function fmtVolume(value) {
    if (value == null || Number.isNaN(Number(value))) return 'n/a';
    const number = Number(value);
    if (number >= 1e9) return `${(number / 1e9).toFixed(2)}B`;
    if (number >= 1e6) return `${(number / 1e6).toFixed(2)}M`;
    if (number >= 1e3) return `${(number / 1e3).toFixed(1)}K`;
    return String(number);
  }

  function fmtTime(value) {
    if (!value) return '--';
    return new Date(value).toLocaleTimeString('en-US', {
      hour: 'numeric',
      minute: '2-digit',
      second: '2-digit',
    });
  }

  function deltaClass(value) {
    if (value == null || Number.isNaN(Number(value))) return 'warn';
    return Number(value) >= 0 ? 'pos' : 'neg';
  }

  function sentimentClass(value) {
    const tone = String(value || '').toLowerCase();
    if (tone === 'bullish') return 'bullish';
    if (tone === 'bearish') return 'bearish';
    return 'neutral';
  }

  function chartSymbolFor(symbol) {
    return CHART_OVERRIDES[symbol] || symbol;
  }

  function chartSrc(symbol, interval) {
    return 'https://s.tradingview.com/widgetembed/?frameElementId=tv_chart'
      + '&symbol=' + encodeURIComponent(chartSymbolFor(symbol))
      + '&interval=' + interval
      + '&theme=dark&style=1&timezone=America%2FNew_York&withdateranges=1&hideideas=1&locale=en';
  }

  async function api(path) {
    let lastError = null;
    for (const base of API_BASE_CANDIDATES) {
      try {
        const response = await fetch(`${base}${path}`, { cache: 'no-store' });
        if (!response.ok) {
          lastError = new Error(`Request failed: ${path}`);
          continue;
        }
        return response.json();
      } catch (error) {
        lastError = error;
      }
    }
    throw lastError || new Error(`Request failed: ${path}`);
  }

  function setClock() {
    state.clock = new Date().toLocaleString('en-US', {
      weekday: 'short',
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      second: '2-digit',
    });
    const clockEl = document.querySelector('[data-clock]');
    if (clockEl) clockEl.textContent = state.clock;
  }

  function emptyState(message) {
    return `<div class="empty-state">${esc(message)}</div>`;
  }

  function errorState(message) {
    return `<div class="error-state">${esc(message)}</div>`;
  }

  function openChartDesk(symbol) {
    const clean = String(symbol || '').trim().toUpperCase();
    if (!clean) return;
    state.chartSymbol = clean;
    state.chartInput = clean;
    state.activeTab = 'chart';
    render();
    loadChartWorkspace(clean);
  }

  function getVisibleThemes() {
    if (!state.themes) return [];
    if (state.themeFilter === 'leaders') return state.themes.leaders || [];
    if (state.themeFilter === 'laggards') return state.themes.laggards || [];
    return state.themes.all || [];
  }

  function getSelectedTheme() {
    const visible = getVisibleThemes();
    if (!visible.length) return null;
    const current = visible.find((item) => item.theme === state.selectedTheme);
    if (current) return current;
    state.selectedTheme = visible[0].theme;
    return visible[0];
  }

  function createPlaceholderThemeStock(themeName, ticker, sortOrder) {
    return {
      ticker,
      company_name: ticker,
      curr_price: null,
      daily_pct: null,
      premarket_pct: null,
      premarket_price: null,
      prev_close: null,
      volume: null,
      avg_volume: null,
      rvol: null,
      market_cap: null,
      display_pct: null,
      themes: [themeName],
      quote_status: 'unavailable',
      sort_order: sortOrder,
    };
  }

  function compareThemeStocks(a, b) {
    const aValue = a?.display_pct;
    const bValue = b?.display_pct;
    if (aValue == null && bValue == null) return (a?.sort_order ?? 0) - (b?.sort_order ?? 0);
    if (aValue == null) return 1;
    if (bValue == null) return -1;
    if (bValue !== aValue) return bValue - aValue;
    return (a?.sort_order ?? 0) - (b?.sort_order ?? 0);
  }

  function normalizeThemePayload(payload) {
    const sourceThemes = Array.isArray(payload?.all) ? payload.all : [];
    const sourceMap = new Map(sourceThemes.map((item) => [item.theme, item]));
    const allThemes = Object.entries(THEME_FALLBACKS).map(([themeName, tickers]) => {
      const sourceTheme = sourceMap.get(themeName) || {};
      const sourceConstituents = Array.isArray(sourceTheme.constituents) ? sourceTheme.constituents : [];
      const liveMap = new Map(sourceConstituents.map((stock) => [String(stock?.ticker || '').toUpperCase(), stock]));
      const constituents = tickers
        .map((ticker, index) => {
          const live = liveMap.get(ticker);
          const fallback = createPlaceholderThemeStock(themeName, ticker, index);
          if (!live) return fallback;
          return {
            ...fallback,
            ...live,
            ticker,
            company_name: live.company_name || ticker,
            themes: Array.isArray(live.themes) && live.themes.length ? live.themes : [themeName],
            quote_status: live.quote_status || 'available',
            sort_order: index,
          };
        })
        .sort(compareThemeStocks);

      const validMoves = constituents
        .map((stock) => Number(stock.display_pct))
        .filter((value) => Number.isFinite(value));
      const upCount = validMoves.filter((value) => value > 0).length;
      const downCount = validMoves.filter((value) => value < 0).length;
      const avgPct = validMoves.length
        ? Number((validMoves.reduce((sum, value) => sum + value, 0) / validMoves.length).toFixed(2))
        : 0;
      const laggards = constituents.filter((stock) => stock.display_pct != null).slice(-5).reverse();

      return {
        theme: themeName,
        avg_pct: avgPct,
        up_count: upCount,
        down_count: downCount,
        stock_count: constituents.length,
        leaders: constituents.slice(0, 5),
        laggards: laggards.length ? laggards : constituents.slice(-5).reverse(),
        constituents,
      };
    }).sort((a, b) => {
      if ((b.avg_pct || 0) !== (a.avg_pct || 0)) return (b.avg_pct || 0) - (a.avg_pct || 0);
      return String(a.theme).localeCompare(String(b.theme));
    });

    const positive = allThemes.filter((theme) => (theme.avg_pct || 0) > 0).length;
    const negative = allThemes.filter((theme) => (theme.avg_pct || 0) < 0).length;
    const laggards = [...allThemes].sort((a, b) => (a.avg_pct || 0) - (b.avg_pct || 0)).slice(0, 6);

    return {
      updated_at: payload?.updated_at || new Date().toISOString(),
      summary: {
        total_themes: allThemes.length,
        positive,
        negative,
        flat: allThemes.length - positive - negative,
        best_theme: allThemes[0]?.theme || null,
        worst_theme: laggards[0]?.theme || null,
      },
      leaders: allThemes.slice(0, 6),
      laggards,
      all: allThemes,
    };
  }
  function renderHeader() {
    const hint = window.location.protocol === 'file:'
      ? 'Opened as a file. Live data is fetched from the hosted Render API first, with localhost as fallback.'
      : 'Served by the backend. Live data is fetched from the local API.';

    return `
      <header class="panel hero">
        <div class="hero-top">
          <div>
            <div class="kicker">HTML Market Dashboard</div>
            <h1>Live market dashboard with full theme constituents, ETF rotation, and AI chart context.</h1>
            <p>Track the tape, inspect every stock inside a theme, monitor ETF capital-flow proxies, and open a chart desk with AI commentary beside the TradingView chart.</p>
          </div>
          <div class="clock-card">
            <div class="label">Current time</div>
            <div class="value" data-clock>${esc(state.clock || '--')}</div>
            <div class="hint">${esc(hint)}</div>
          </div>
        </div>
        <div class="tabs">
          ${TAB_ORDER.map((tab) => `
            <button class="tab-btn ${state.activeTab === tab ? 'active' : ''}" data-tab="${tab}">${TAB_LABELS[tab]}</button>
          `).join('')}
        </div>
      </header>
    `;
  }

  function renderOverviewTab() {
    const brief = state.briefData?.brief;
    const overview = state.overview;
    const summary = overview?.summary || {};
    const bestTheme = state.themes?.summary?.best_theme || '--';
    const bestGroup = state.etfs?.summary?.best_group || '--';

    const summaryHtml = state.loading.overview && !overview
      ? emptyState('Loading market overview...')
      : state.errors.overview
        ? errorState(state.errors.overview)
        : `
          <div class="metrics-4">
            <div class="metric-card"><div class="metric-label">Advancing</div><div class="metric-value pos">${summary.positive ?? '--'}</div><div class="metric-copy">Core quotes with a positive session move.</div></div>
            <div class="metric-card"><div class="metric-label">Declining</div><div class="metric-value neg">${summary.negative ?? '--'}</div><div class="metric-copy">Core quotes trading below the prior close.</div></div>
            <div class="metric-card"><div class="metric-label">Best Theme</div><div class="metric-value">${esc(bestTheme)}</div><div class="metric-copy">Current theme leader by average move.</div></div>
            <div class="metric-card"><div class="metric-label">ETF Leadership</div><div class="metric-value">${esc(bestGroup)}</div><div class="metric-copy">Strongest ETF group by flow proxy.</div></div>
          </div>
        `;

    const paragraphsHtml = state.loading.brief && !brief
      ? emptyState('Building the market brief...')
      : state.errors.brief
        ? errorState(state.errors.brief)
        : brief
          ? `
            <div class="brief-card">
              <div class="desk-head">
                <div>
                  <div class="section-kicker">Market Brief</div>
                  <h2>${esc(brief.title || 'AI Market Brief')}</h2>
                  <p>Six short paragraphs on the tape, leadership, laggards, and where capital appears to be rotating.</p>
                </div>
                <div class="soft-pill ${sentimentClass(brief.sentiment) === 'bullish' ? 'pos' : sentimentClass(brief.sentiment) === 'bearish' ? 'neg' : 'warn'}">${esc(brief.sentiment || 'Neutral')}</div>
              </div>
              <div class="paragraphs">${(brief.paragraphs || []).map((paragraph) => `<div class="paragraph">${esc(paragraph)}</div>`).join('')}</div>
              <div class="bullet-list">${(brief.bullets || []).map((bullet) => `
                <div class="bullet-row">
                  <div class="tone-pill ${sentimentClass(bullet.tone)}">${esc(bullet.tone)}</div>
                  <div>${esc(bullet.text)}</div>
                </div>
              `).join('')}</div>
            </div>
          `
          : emptyState('The market brief is not available yet.');

    const groupOrder = ['Major Indexes', 'Risk Assets', 'Macro', 'Digital Assets'];
    const buckets = new Map();
    (overview?.items || []).forEach((item) => {
      if (!buckets.has(item.group)) buckets.set(item.group, []);
      buckets.get(item.group).push(item);
    });

    const groupsHtml = state.loading.overview && !overview
      ? emptyState('Loading grouped quotes...')
      : state.errors.overview
        ? errorState(state.errors.overview)
        : groupOrder.filter((group) => buckets.has(group)).map((group) => `
          <div class="section-gap">
            <div>
              <div class="section-kicker">${esc(group)}</div>
              <h2 style="margin:10px 0 0;font-size:1.35rem;">${esc(group)}</h2>
            </div>
            <div class="quote-grid">
              ${buckets.get(group).map((item) => `
                <div class="quote-card">
                  <div class="quote-head">
                    <div>
                      <div class="symbol">${esc(item.symbol)}</div>
                      <div class="title">${esc(item.label)}</div>
                    </div>
                    <button class="small-btn" data-open-desk="${esc(item.symbol)}">Open Desk</button>
                  </div>
                  <div>
                    <div class="big-price">${item.symbol.startsWith('^') ? fmtPrice(item.price, false) : fmtPrice(item.price)}</div>
                    <div class="${deltaClass(item.change_pct)}" style="margin-top:8px;font-weight:700;">${fmtPercent(item.change_pct)}</div>
                  </div>
                  <div class="pills">
                    ${item.extended_price != null ? `<span class="soft-pill ${deltaClass(item.extended_change_pct)}">${item.extended_session === 'pre' ? 'Pre' : 'Post'} ${fmtPrice(item.extended_price)} ${fmtPercent(item.extended_change_pct)}</span>` : ''}
                  </div>
                  <div class="quote-footer">
                    <div class="mini-card"><div class="mini-label">Day range</div><div style="margin-top:8px;">${item.day_low != null && item.day_high != null ? `${fmtNumber(item.day_low)} - ${fmtNumber(item.day_high)}` : 'n/a'}</div></div>
                    <div class="mini-card"><div class="mini-label">Volume</div><div style="margin-top:8px;">${fmtVolume(item.volume)}</div></div>
                  </div>
                </div>
              `).join('')}
            </div>
          </div>
        `).join('') || emptyState('No overview quotes are available right now.');

    return `
      <div class="main-stack">
        <div class="split">
          <section class="panel section">
            <div class="section-head">
              <div>
                <div class="section-kicker">Tape</div>
                <h2>Live market snapshot</h2>
                <p>Indexes, risk assets, macro gauges, and digital assets are refreshed from Yahoo Finance.</p>
              </div>
              <button class="action-btn" data-refresh="overview">Refresh Overview</button>
            </div>
            ${summaryHtml}
          </section>
          <section class="panel section">
            <div class="section-head">
              <div>
                <div class="section-kicker">Workflow</div>
                <h2>Jump to detail views</h2>
                <p>Move from the big picture into theme internals, ETF rotation, or the chart desk without leaving the dashboard.</p>
              </div>
            </div>
            <div class="summary-flow">
              <button class="metric-card" data-tab="themes" style="text-align:left;cursor:pointer;">
                <div class="metric-label" style="color:#9df3d0;">Themes</div>
                <div class="metric-value" style="font-size:1.55rem;">Full constituent view</div>
                <div class="metric-copy">See every stock inside each tracked theme.</div>
              </button>
              <button class="metric-card" data-tab="flows" style="text-align:left;cursor:pointer;">
                <div class="metric-label" style="color:#bfe7ff;">ETF Flows</div>
                <div class="metric-value" style="font-size:1.55rem;">Capital rotation proxy</div>
                <div class="metric-copy">Broad ETF board for sectors, rates, commodities, and more.</div>
              </button>
            </div>
          </section>
        </div>
        <section class="panel section">${paragraphsHtml}</section>
        <section class="panel section">
          <div class="section-head">
            <div>
              <div class="section-kicker">Grouped Quotes</div>
              <h2>Market groups</h2>
              <p>Open any symbol in the chart desk to inspect the TradingView chart and AI commentary beside it.</p>
            </div>
          </div>
          ${groupsHtml}
        </section>
      </div>
    `;
  }
  function renderThemesTab() {
    const visibleThemes = getVisibleThemes();
    const selectedTheme = getSelectedTheme();
    const summary = state.themes?.summary || {};

    const headerHtml = state.loading.themes && !state.themes
      ? emptyState('Loading theme dashboard...')
      : state.errors.themes
        ? errorState(state.errors.themes)
        : `
          <div class="metrics-4">
            <div class="metric-card"><div class="metric-label">Total themes</div><div class="metric-value">${summary.total_themes ?? '--'}</div><div class="metric-copy">Configured theme baskets currently tracked.</div></div>
            <div class="metric-card"><div class="metric-label">Positive breadth</div><div class="metric-value pos">${summary.positive ?? '--'}</div><div class="metric-copy">Themes trading with a positive average move.</div></div>
            <div class="metric-card"><div class="metric-label">Negative breadth</div><div class="metric-value neg">${summary.negative ?? '--'}</div><div class="metric-copy">Themes losing ground on average.</div></div>
            <div class="metric-card"><div class="metric-label">Best / Worst</div><div class="metric-value" style="font-size:1.35rem;">${esc(summary.best_theme || '--')}</div><div class="metric-copy">${esc(summary.worst_theme || '--')} | Updated ${fmtTime(state.themes?.updated_at)}</div></div>
          </div>
        `;

    const cardsHtml = state.loading.themes && !state.themes
      ? emptyState('Loading theme cards...')
      : state.errors.themes
        ? errorState(state.errors.themes)
        : visibleThemes.map((theme) => {
          const width = Math.min(Math.abs(theme.avg_pct || 0) * 10, 100);
          const barStyle = (theme.avg_pct || 0) >= 0
            ? 'linear-gradient(90deg,#1dd1a1,#6ee7b7)'
            : 'linear-gradient(90deg,#fb7185,#fda4af)';
          return `
            <button class="theme-card" data-theme-select="${esc(theme.theme)}" style="text-align:left;cursor:pointer;${selectedTheme && selectedTheme.theme === theme.theme ? 'outline:1px solid rgba(103,198,255,0.35);' : ''}">
              <div class="theme-head">
                <div>
                  <div class="section-kicker">Theme</div>
                  <div class="title">${esc(theme.theme)}</div>
                </div>
                <div class="${deltaClass(theme.avg_pct)}" style="font-size:22px;font-weight:700;">${fmtPercent(theme.avg_pct)}</div>
              </div>
              <div class="bar"><span style="width:${width}%;background:${barStyle}"></span></div>
              <div class="pills">
                <span class="soft-pill">${theme.stock_count} stocks</span>
                <span class="soft-pill pos">${theme.up_count} up</span>
                <span class="soft-pill neg">${theme.down_count} down</span>
              </div>
            </button>
          `;
        }).join('') || emptyState('No theme cards are available.');

    const detailHtml = !selectedTheme
      ? emptyState('Select a theme to inspect every stock inside it.')
      : `
        <div class="panel section">
          <div class="section-head">
            <div>
              <div class="section-kicker">Comprehensive Theme View</div>
              <h2>${esc(selectedTheme.theme)}</h2>
              <p>Every stock inside the selected theme, sorted by the best available displayed move.</p>
            </div>
            <div class="pills">
              <span class="soft-pill ${deltaClass(selectedTheme.avg_pct)}">Average ${fmtPercent(selectedTheme.avg_pct)}</span>
              <span class="soft-pill">${selectedTheme.stock_count} constituents</span>
            </div>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Ticker</th>
                  <th>Company</th>
                  <th>Price</th>
                  <th>Daily %</th>
                  <th>Pre %</th>
                  <th>Volume</th>
                  <th>RVol</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                ${(selectedTheme.constituents || []).map((stock) => `
                  <tr>
                    <td><button class="chip-btn clicker mono" data-open-desk="${esc(stock.ticker)}">${esc(stock.ticker)}</button></td>
                    <td>${esc(stock.company_name || stock.ticker)}</td>
                    <td>${fmtPrice(stock.curr_price)}</td>
                    <td class="${deltaClass(stock.daily_pct)}">${fmtPercent(stock.daily_pct)}</td>
                    <td class="${deltaClass(stock.premarket_pct)}">${fmtPercent(stock.premarket_pct)}</td>
                    <td>${fmtVolume(stock.volume)}</td>
                    <td>${stock.rvol != null ? `${stock.rvol}x` : 'n/a'}</td>
                    <td><button class="small-btn" data-open-desk="${esc(stock.ticker)}">Chart Desk</button></td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        </div>
      `;

    return `
      <div class="main-stack">
        <section class="panel section">
          <div class="section-head">
            <div>
              <div class="section-kicker">Theme Board</div>
              <h2>All stocks inside each theme</h2>
              <p>Use the theme cards to move between leadership groups and inspect every constituent in a full table.</p>
            </div>
            <button class="action-btn" data-refresh="themes">Refresh Themes</button>
          </div>
          ${headerHtml}
          <div class="filters" style="margin-top:18px;">
            <button class="filter-btn ${state.themeFilter === 'all' ? 'active' : ''}" data-theme-filter="all">All Themes</button>
            <button class="filter-btn ${state.themeFilter === 'leaders' ? 'active' : ''}" data-theme-filter="leaders">Best Themes</button>
            <button class="filter-btn ${state.themeFilter === 'laggards' ? 'active' : ''}" data-theme-filter="laggards">Worst Themes</button>
          </div>
        </section>
        <section class="theme-grid">${cardsHtml}</section>
        ${detailHtml}
      </div>
    `;
  }

  function renderFlowsTab() {
    const etfs = state.etfs;
    const summary = etfs?.summary || {};

    const headerHtml = state.loading.etfs && !etfs
      ? emptyState('Loading ETF flow board...')
      : state.errors.etfs
        ? errorState(state.errors.etfs)
        : `
          <div class="metrics-4">
            <div class="metric-card"><div class="metric-label">Total ETFs</div><div class="metric-value">${summary.total_etfs ?? '--'}</div><div class="metric-copy">Broad ETF set across market, style, macro, and thematic buckets.</div></div>
            <div class="metric-card"><div class="metric-label">Best Group</div><div class="metric-value">${esc(summary.best_group || '--')}</div><div class="metric-copy">Strongest average flow-proxy group right now.</div></div>
            <div class="metric-card"><div class="metric-label">Risk-On Proxy</div><div class="metric-value ${deltaClass(summary.risk_on_proxy)}">${fmtPercent(summary.risk_on_proxy)}</div><div class="metric-copy">QQQ daily move as a growth/risk proxy.</div></div>
            <div class="metric-card"><div class="metric-label">Defensive Proxy</div><div class="metric-value ${deltaClass(summary.defensive_proxy)}">${fmtPercent(summary.defensive_proxy)}</div><div class="metric-copy">TLT daily move as a defensive/rates proxy.</div></div>
          </div>
        `;

    const leadersHtml = (etfs?.leaders || []).map((item) => `
      <div class="mini-card">
        <div class="mini-label">${esc(item.group)}</div>
        <div style="margin-top:8px;display:flex;justify-content:space-between;gap:8px;align-items:center;">
          <button class="chip-btn clicker mono" data-open-desk="${esc(item.symbol)}">${esc(item.symbol)}</button>
          <span class="${deltaClass(item.change_pct)}">${fmtPercent(item.change_pct)}</span>
        </div>
        <div class="tiny-copy" style="margin-top:6px;">Flow proxy ${fmtNumber(item.flow_proxy || 0)}</div>
      </div>
    `).join('');

    const laggardsHtml = (etfs?.laggards || []).map((item) => `
      <div class="mini-card">
        <div class="mini-label">${esc(item.group)}</div>
        <div style="margin-top:8px;display:flex;justify-content:space-between;gap:8px;align-items:center;">
          <button class="chip-btn clicker mono" data-open-desk="${esc(item.symbol)}">${esc(item.symbol)}</button>
          <span class="${deltaClass(item.change_pct)}">${fmtPercent(item.change_pct)}</span>
        </div>
        <div class="tiny-copy" style="margin-top:6px;">Flow proxy ${fmtNumber(item.flow_proxy || 0)}</div>
      </div>
    `).join('');

    const groupsHtml = state.loading.etfs && !etfs
      ? emptyState('Loading ETF groups...')
      : state.errors.etfs
        ? errorState(state.errors.etfs)
        : (etfs?.groups || []).map((group) => `
          <section class="panel section table-section">
            <div class="section-head">
              <div>
                <div class="section-kicker">${esc(group.group)}</div>
                <h2>${esc(group.group)}</h2>
              </div>
              <span class="soft-pill ${deltaClass(group.avg_change_pct)}">Avg ${fmtPercent(group.avg_change_pct)}</span>
            </div>
            <div class="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>ETF</th>
                    <th>Name</th>
                    <th>Price</th>
                    <th>1D %</th>
                    <th>Ext %</th>
                    <th>Volume</th>
                    <th>RVol</th>
                    <th>Flow Proxy</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  ${group.items.map((item) => `
                    <tr>
                      <td><button class="chip-btn clicker mono" data-open-desk="${esc(item.symbol)}">${esc(item.symbol)}</button></td>
                      <td>${esc(item.label)}</td>
                      <td>${fmtPrice(item.price)}</td>
                      <td class="${deltaClass(item.change_pct)}">${fmtPercent(item.change_pct)}</td>
                      <td class="${deltaClass(item.extended_change_pct)}">${fmtPercent(item.extended_change_pct)}</td>
                      <td>${fmtVolume(item.volume)}</td>
                      <td>${item.rvol != null ? `${item.rvol}x` : 'n/a'}</td>
                      <td class="${deltaClass(item.flow_proxy)}">${fmtNumber(item.flow_proxy || 0)}</td>
                      <td><button class="small-btn" data-open-desk="${esc(item.symbol)}">Chart Desk</button></td>
                    </tr>
                  `).join('')}
                </tbody>
              </table>
            </div>
          </section>
        `).join('') || emptyState('No ETF flow data is available.');

    return `
      <div class="main-stack">
        <section class="panel section">
          <div class="section-head">
            <div>
              <div class="section-kicker">ETF Flow Board</div>
              <h2>ETF capital-flow proxies across the market</h2>
              <p>This is a broad ETF board so you can see where capital appears to be rotating across sectors, styles, rates, commodities, international markets, and digital-asset proxies.</p>
            </div>
            <button class="action-btn" data-refresh="flows">Refresh ETF Board</button>
          </div>
          ${headerHtml}
        </section>
        <section class="panel section">
          <div class="detail-grid">
            <div>
              <div class="section-kicker">Leaders</div>
              <h2 style="margin:10px 0 0;font-size:1.4rem;">Strongest flow proxies</h2>
              <div class="metrics-5" style="margin-top:14px;">${leadersHtml || emptyState('No leaders available.')}</div>
            </div>
            <div>
              <div class="section-kicker">Laggards</div>
              <h2 style="margin:10px 0 0;font-size:1.4rem;">Weakest flow proxies</h2>
              <div class="metrics-5" style="margin-top:14px;">${laggardsHtml || emptyState('No laggards available.')}</div>
            </div>
          </div>
        </section>
        ${groupsHtml}
      </div>
    `;
  }

  function buildRrgSvg(items) {
    if (!items.length) return emptyState('No ETF RRG points are available right now.');
    const width = 920;
    const height = 560;
    const padding = 72;
    const plotWidth = width - padding * 2;
    const plotHeight = height - padding * 2;
    const xValues = items.flatMap((item) => item.trail.map((point) => point.rs_ratio));
    const yValues = items.flatMap((item) => item.trail.map((point) => point.rs_momentum));
    const minX = Math.min(98, ...xValues) - 0.4;
    const maxX = Math.max(102, ...xValues) + 0.4;
    const minY = Math.min(98, ...yValues) - 0.4;
    const maxY = Math.max(102, ...yValues) + 0.4;
    const xScale = (value) => padding + ((value - minX) / (maxX - minX || 1)) * plotWidth;
    const yScale = (value) => height - padding - ((value - minY) / (maxY - minY || 1)) * plotHeight;
    const centerX = xScale(100);
    const centerY = yScale(100);

    const gridLines = Array.from({ length: 5 }, (_, index) => {
      const x = padding + (plotWidth / 4) * index;
      const y = padding + (plotHeight / 4) * index;
      return `
        <line x1="${x}" x2="${x}" y1="${padding}" y2="${height - padding}" stroke="rgba(148,163,184,0.12)" stroke-width="1" />
        <line x1="${padding}" x2="${width - padding}" y1="${y}" y2="${y}" stroke="rgba(148,163,184,0.12)" stroke-width="1" />
      `;
    }).join('');

    const trails = items.map((item, index) => {
      const color = QUADRANT_COLORS[item.quadrant] || '#dbeafe';
      const points = item.trail.map((point) => `${xScale(point.rs_ratio)},${yScale(point.rs_momentum)}`).join(' ');
      const latest = item.trail[item.trail.length - 1];
      const latestX = xScale(latest.rs_ratio);
      const latestY = yScale(latest.rs_momentum);
      const labelOffset = index % 2 === 0 ? -14 : 18;
      const dots = item.trail.map((point, pointIndex) => `
        <circle cx="${xScale(point.rs_ratio)}" cy="${yScale(point.rs_momentum)}" r="${pointIndex === item.trail.length - 1 ? 7 : 4}" fill="${color}" fill-opacity="${pointIndex === item.trail.length - 1 ? 1 : 0.35}" stroke="#07111f" stroke-width="2"></circle>
      `).join('');
      return `
        <g>
          <polyline fill="none" points="${points}" stroke="${color}" stroke-opacity="0.45" stroke-width="2.5"></polyline>
          ${dots}
          <text x="${latestX + 10}" y="${latestY + labelOffset}" fill="${color}" font-size="16" font-weight="700">${esc(item.symbol)}</text>
        </g>
      `;
    }).join('');

    return `
      <div class="rrg-shell">
        <svg viewBox="0 0 ${width} ${height}">
          <rect x="0" y="0" width="${width}" height="${height}" fill="#07111f"></rect>
          <rect x="${padding}" y="${padding}" width="${centerX - padding}" height="${centerY - padding}" fill="rgba(96,165,250,0.08)"></rect>
          <rect x="${centerX}" y="${padding}" width="${width - padding - centerX}" height="${centerY - padding}" fill="rgba(52,211,153,0.08)"></rect>
          <rect x="${padding}" y="${centerY}" width="${centerX - padding}" height="${height - padding - centerY}" fill="rgba(251,113,133,0.08)"></rect>
          <rect x="${centerX}" y="${centerY}" width="${width - padding - centerX}" height="${height - padding - centerY}" fill="rgba(245,158,11,0.08)"></rect>
          ${gridLines}
          <line x1="${centerX}" x2="${centerX}" y1="${padding}" y2="${height - padding}" stroke="rgba(226,232,240,0.45)" stroke-width="1.5" stroke-dasharray="6 8"></line>
          <line x1="${padding}" x2="${width - padding}" y1="${centerY}" y2="${centerY}" stroke="rgba(226,232,240,0.45)" stroke-width="1.5" stroke-dasharray="6 8"></line>
          <text x="${padding + 16}" y="${padding + 28}" fill="rgba(191,219,254,0.95)" font-size="20" font-weight="700">Improving</text>
          <text x="${width - padding - 140}" y="${padding + 28}" fill="rgba(167,243,208,0.95)" font-size="20" font-weight="700">Leading</text>
          <text x="${padding + 16}" y="${height - padding - 18}" fill="rgba(254,202,202,0.95)" font-size="20" font-weight="700">Lagging</text>
          <text x="${width - padding - 166}" y="${height - padding - 18}" fill="rgba(253,230,138,0.95)" font-size="20" font-weight="700">Weakening</text>
          ${trails}
        </svg>
      </div>
    `;
  }
  function renderRrgTab() {
    const rrg = state.rrg;
    const items = rrg?.items || [];
    const counts = items.reduce((accumulator, item) => {
      accumulator[item.quadrant] = (accumulator[item.quadrant] || 0) + 1;
      return accumulator;
    }, { Leading: 0, Weakening: 0, Lagging: 0, Improving: 0 });

    const summaryHtml = state.loading.rrg && !rrg
      ? emptyState('Loading ETF relative rotation data...')
      : state.errors.rrg
        ? errorState(state.errors.rrg)
        : `
          <div class="metrics-5">
            <div class="metric-card"><div class="metric-label">Benchmark</div><div class="metric-value">${esc(rrg?.benchmark?.symbol || 'SPY')}</div><div class="metric-copy">${fmtPrice(rrg?.benchmark?.price)} / ${fmtPercent(rrg?.benchmark?.change_pct)}</div></div>
            <div class="metric-card"><div class="metric-label">Leading</div><div class="metric-value" style="color:${QUADRANT_COLORS.Leading}">${counts.Leading}</div><div class="metric-copy">Strong ratio and strong momentum.</div></div>
            <div class="metric-card"><div class="metric-label">Improving</div><div class="metric-value" style="color:${QUADRANT_COLORS.Improving}">${counts.Improving}</div><div class="metric-copy">Momentum is improving.</div></div>
            <div class="metric-card"><div class="metric-label">Weakening</div><div class="metric-value" style="color:${QUADRANT_COLORS.Weakening}">${counts.Weakening}</div><div class="metric-copy">Still above 100 on ratio, but cooling.</div></div>
            <div class="metric-card"><div class="metric-label">Lagging</div><div class="metric-value" style="color:${QUADRANT_COLORS.Lagging}">${counts.Lagging}</div><div class="metric-copy">Updated ${fmtTime(rrg?.updated_at)}</div></div>
          </div>
        `;

    const chartHtml = state.loading.rrg && !rrg
      ? emptyState('Loading RRG chart...')
      : state.errors.rrg
        ? errorState(state.errors.rrg)
        : (rrg?.error ? errorState(rrg.error) : buildRrgSvg(items));

    const tableHtml = items.length
      ? `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ETF</th>
                <th>Sector</th>
                <th>Price</th>
                <th>1D %</th>
                <th>Weekly %</th>
                <th>RS-Ratio</th>
                <th>RS-Momentum</th>
                <th>Quadrant</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              ${items.map((item) => `
                <tr>
                  <td><button class="chip-btn clicker mono" data-open-desk="${esc(item.symbol)}">${esc(item.symbol)}</button></td>
                  <td>${esc(item.label)}</td>
                  <td>${fmtPrice(item.price)}</td>
                  <td class="${deltaClass(item.change_pct)}">${fmtPercent(item.change_pct)}</td>
                  <td class="${deltaClass(item.weekly_change_pct)}">${fmtPercent(item.weekly_change_pct)}</td>
                  <td>${item.rs_ratio != null ? Number(item.rs_ratio).toFixed(2) : 'n/a'}</td>
                  <td>${item.rs_momentum != null ? Number(item.rs_momentum).toFixed(2) : 'n/a'}</td>
                  <td><span class="soft-pill" style="color:${QUADRANT_COLORS[item.quadrant]};border-color:${QUADRANT_COLORS[item.quadrant]}55;">${esc(item.quadrant)}</span></td>
                  <td><button class="small-btn" data-open-desk="${esc(item.symbol)}">Chart Desk</button></td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      ` : emptyState('No ETF RRG rows were returned.');

    return `
      <div class="main-stack">
        <section class="panel section">
          <div class="section-head">
            <div>
              <div class="section-kicker">ETF RRG</div>
              <h2>Sector ETF relative rotation</h2>
              <p>Relative Rotation Graph style view for the SPDR sector set versus SPY.</p>
            </div>
            <button class="action-btn" data-refresh="rrg">Refresh RRG</button>
          </div>
          ${summaryHtml}
        </section>
        <section class="panel section">${chartHtml}</section>
        <section class="panel section">
          <div class="section-head">
            <div>
              <div class="section-kicker">Constituents</div>
              <h2>Sector ETF breakdown</h2>
            </div>
            <div class="legend">${Object.entries(QUADRANT_COLORS).map(([label, color]) => `<span class="soft-pill" style="color:${color};border-color:${color}55;">${esc(label)}</span>`).join('')}</div>
          </div>
          ${tableHtml}
        </section>
      </div>
    `;
  }

  function renderSessionMoversTab(kind) {
    const payload = state[kind];
    const rows = payload?.all || [];
    const label = kind === 'premarket' ? 'Pre-market' : 'Post-market';
    const summary = payload?.summary || {};

    const summaryHtml = state.loading[kind] && !payload
      ? emptyState(`Loading ${label.toLowerCase()} movers...`)
      : state.errors[kind]
        ? errorState(state.errors[kind])
        : `
          <div class="metrics-5">
            <div class="metric-card"><div class="metric-label">Matched</div><div class="metric-value">${summary.matched_count ?? '--'}</div><div class="metric-copy">Tracked names with a live ${label.toLowerCase()} move above the filter.</div></div>
            <div class="metric-card"><div class="metric-label">Up</div><div class="metric-value pos">${summary.leaders_count ?? '--'}</div><div class="metric-copy">Names trading green in the ${label.toLowerCase()} tape.</div></div>
            <div class="metric-card"><div class="metric-label">Down</div><div class="metric-value neg">${summary.laggards_count ?? '--'}</div><div class="metric-copy">Names trading red in the ${label.toLowerCase()} tape.</div></div>
            <div class="metric-card"><div class="metric-label">Biggest Up</div><div class="metric-value" style="font-size:1.25rem;">${esc(summary.biggest_up || '--')}</div><div class="metric-copy">Top upside mover returned by the live board.</div></div>
            <div class="metric-card"><div class="metric-label">Biggest Down</div><div class="metric-value" style="font-size:1.25rem;">${esc(summary.biggest_down || '--')}</div><div class="metric-copy">Top downside mover returned by the live board.</div></div>
          </div>
        `;

    const boardHtml = rows.length
      ? `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Ticker</th>
                <th>Company</th>
                <th>${label} %</th>
                <th>${label} Price</th>
                <th>1D %</th>
                <th>Event</th>
                <th>Perception Before</th>
                <th>What Changed / Why It Matters</th>
                <th>Market View Now</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              ${rows.map((item) => `
                <tr>
                  <td><button class="chip-btn clicker mono" data-open-desk="${esc(item.ticker)}">${esc(item.ticker)}</button></td>
                  <td>${esc(item.company_name || item.ticker)}</td>
                  <td class="${deltaClass(item.session_pct)}">${fmtPercent(item.session_pct)}</td>
                  <td>${fmtPrice(item.session_price)}</td>
                  <td class="${deltaClass(item.change_pct)}">${fmtPercent(item.change_pct)}</td>
                  <td>
                    <div>${esc(item.event_label || 'Narrative')}</div>
                    <div class="tiny-copy">${esc(item.headline_title || 'No fresh headline')}</div>
                  </td>
                  <td style="min-width:230px; color: var(--muted);">${esc(item.perception_before || 'n/a')}</td>
                  <td style="min-width:320px; color: var(--muted);">${esc(item.what_changed || item.reasoning || 'n/a')}</td>
                  <td style="min-width:240px; color: var(--muted);">${esc(item.market_view || item.analyst_view || 'n/a')}</td>
                  <td><button class="small-btn" data-open-desk="${esc(item.ticker)}">Chart Desk</button></td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      `
      : emptyState(`No ${label.toLowerCase()} movers were returned yet. If the feed is quiet, try again after the next session update.`);

    return `
      <div class="main-stack">
        <section class="panel section">
          <div class="section-head">
            <div>
              <div class="section-kicker">${label} Movers</div>
              <h2>${label} movers and rerating context</h2>
              <p>Track the names actually moving in the ${label.toLowerCase()} tape, why they are moving, what the market thought before the catalyst, and whether the setup now looks like a rerating or a fade.</p>
            </div>
            <button class="action-btn" data-refresh="${kind}">Refresh ${label}</button>
          </div>
          ${summaryHtml}
        </section>
        <section class="panel section">
          <div class="section-head">
            <div>
              <div class="section-kicker">Live Board</div>
              <h2>${label} move board</h2>
              <p>Open any ticker in the chart desk for deeper context on key events, expectations, and the current market perception.</p>
            </div>
          </div>
          ${boardHtml}
        </section>
      </div>
    `;
  }

  function renderPremarketTab() {
    return renderSessionMoversTab('premarket');
  }

  function renderPostmarketTab() {
    return renderSessionMoversTab('postmarket');
  }
  function renderEarningsTab() {
    const earnings = state.earnings;
    const items = earnings?.items || [];
    const brief = earnings?.brief || {};
    const coverage = earnings?.summary?.coverage_universe ?? '--';

    const summaryHtml = state.loading.earnings && !earnings
      ? emptyState('Loading earnings tracker...')
      : state.errors.earnings
        ? errorState(state.errors.earnings)
        : `
          <div class="metrics-5">
            <div class="metric-card"><div class="metric-label">Recent</div><div class="metric-value">${earnings?.summary?.recent_count ?? '--'}</div><div class="metric-copy">Results that landed in the recent lookback window.</div></div>
            <div class="metric-card"><div class="metric-label">Upcoming</div><div class="metric-value">${earnings?.summary?.upcoming_count ?? '--'}</div><div class="metric-copy">Names with scheduled reports still ahead.</div></div>
            <div class="metric-card"><div class="metric-label">Today</div><div class="metric-value">${earnings?.summary?.today_count ?? '--'}</div><div class="metric-copy">Names scheduled to report today.</div></div>
            <div class="metric-card"><div class="metric-label">Next 7 Days</div><div class="metric-value">${earnings?.summary?.next_7_days ?? '--'}</div><div class="metric-copy">Reports due over the next trading week.</div></div>
            <div class="metric-card"><div class="metric-label">Top Theme</div><div class="metric-value" style="font-size:1.2rem;">${esc(earnings?.summary?.top_theme || '--')}</div><div class="metric-copy">Most represented tracked theme in the current slate.</div></div>
          </div>
        `;

    const briefHtml = state.loading.earnings && !earnings
      ? emptyState('Building the earnings brief...')
      : state.errors.earnings
        ? errorState(state.errors.earnings)
        : `
          <div class="brief-card">
            <div class="desk-head">
              <div>
                <div class="section-kicker">AI Earnings Brief</div>
                <h2>${esc(brief.headline || 'Recent and upcoming earnings brief')}</h2>
                <p>Watching ${coverage} liquid U.S. stocks for fresh prints, near-term reports, and the places where earnings can spill into themes.</p>
              </div>
              <span class="soft-pill warn">Updated ${fmtTime(earnings?.updated_at)}</span>
            </div>
            <div class="paragraphs">
              ${[brief.summary, brief.focus, brief.themes, brief.risk].filter(Boolean).map((text) => `<div class="paragraph">${esc(text)}</div>`).join('') || emptyState('The earnings brief is not available yet.')}
            </div>
          </div>
        `;

    const tableHtml = items.length
      ? `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Ticker</th>
                <th>Company</th>
                <th>Date</th>
                <th>Status</th>
                <th>EPS Est</th>
                <th>Reported</th>
                <th>Surprise</th>
                <th>1D %</th>
                <th>Themes</th>
                <th>AI / Event Read</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              ${items.map((item) => `
                <tr>
                  <td><button class="chip-btn clicker mono" data-open-desk="${esc(item.ticker)}">${esc(item.ticker)}</button></td>
                  <td>${esc(item.company_name || item.ticker)}</td>
                  <td>
                    <div>${esc(item.earnings_date_display || 'n/a')}</div>
                    <div class="tiny-copy">${esc(item.event_source_label || 'Yahoo earnings feed')}</div>
                  </td>
                  <td><span class="soft-pill ${item.status === 'Today' ? 'warn' : item.status === 'Upcoming' ? 'pos' : ''}">${esc(item.status || 'Scheduled')}</span></td>
                  <td>${item.eps_estimate != null ? item.eps_estimate.toFixed(2) : 'n/a'}</td>
                  <td>${item.reported_eps != null ? item.reported_eps.toFixed(2) : 'n/a'}</td>
                  <td class="${deltaClass(item.surprise_pct)}">${fmtPercent(item.surprise_pct)}</td>
                  <td class="${deltaClass(item.change_pct)}">${fmtPercent(item.change_pct)}</td>
                  <td>${esc((item.themes || []).join(', ') || 'None')}</td>
                  <td style="min-width:320px; color: var(--muted);">${esc(item.reasoning || 'n/a')}</td>
                  <td><button class="small-btn" data-open-desk="${esc(item.ticker)}">Chart Desk</button></td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      `
      : emptyState('No recent or upcoming earnings rows were returned yet. If the provider is late, try refresh again in a few minutes.');

    return `
      <div class="main-stack">
        <section class="panel section">
          <div class="section-head">
            <div>
              <div class="section-kicker">Earnings Tracker</div>
              <h2>Recent and upcoming U.S. earnings</h2>
              <p>Watch the tracked liquid-stock universe for fresh prints and near-term report dates, then open any name in the chart desk to pair price action with news and AI context.</p>
            </div>
            <button class="action-btn" data-refresh="earnings">Refresh Earnings</button>
          </div>
          ${summaryHtml}
        </section>
        <section class="panel section">${briefHtml}</section>
        <section class="panel section">
          <div class="section-head">
            <div>
              <div class="section-kicker">Calendar</div>
              <h2>Earnings slate</h2>
              <p>Sorted by the nearest earnings event returned inside the recent-plus-upcoming window.</p>
            </div>
          </div>
          ${tableHtml}
        </section>
      </div>
    `;
  }
  function renderChartTab() {
    const workspace = state.chartWorkspace;
    const detail = workspace?.detail || {};
    const metrics = workspace?.snapshot?.metrics || {};
    const reasoning = workspace?.reasoning || {};
    const headlineImpacts = Array.isArray(reasoning.headline_impacts) ? reasoning.headline_impacts : [];

    const statsHtml = workspace ? `
      <div class="chart-stats">
        <div class="metric-card"><div class="metric-label">Trend</div><div class="metric-value" style="font-size:1.4rem;">${esc(metrics.trend_state || '--')}</div><div class="metric-copy">Chart state from the daily snapshot.</div></div>
        <div class="metric-card"><div class="metric-label">1M Return</div><div class="metric-value ${deltaClass(metrics.return_1m)}">${fmtPercent(metrics.return_1m)}</div><div class="metric-copy">Performance over the last month.</div></div>
        <div class="metric-card"><div class="metric-label">RVol 20</div><div class="metric-value">${metrics.relative_volume20 != null ? `${metrics.relative_volume20}x` : 'n/a'}</div><div class="metric-copy">Latest volume versus the 20-day average.</div></div>
        <div class="metric-card"><div class="metric-label">RSI 14</div><div class="metric-value ${metrics.rsi14 >= 70 ? 'neg' : metrics.rsi14 <= 35 ? 'pos' : ''}">${metrics.rsi14 != null ? metrics.rsi14.toFixed(1) : 'n/a'}</div><div class="metric-copy">Momentum stretch gauge.</div></div>
      </div>
    ` : (state.loading.chart ? emptyState('Loading chart desk context...') : state.errors.chart ? errorState(state.errors.chart) : emptyState('Open a symbol from the dashboard or type one below.'));

    const reasoningHtml = workspace ? `
      <div class="reason-stack">
        <div class="reason-card">
          <div class="desk-head">
            <div>
              <div class="section-kicker">AI Chart Reasoning</div>
              <h3>${esc(reasoning.headline || `${state.chartSymbol} chart context`)}</h3>
            </div>
            <span class="soft-pill ${sentimentClass(reasoning.bias) === 'bullish' ? 'pos' : sentimentClass(reasoning.bias) === 'bearish' ? 'neg' : 'warn'}">${esc(reasoning.bias || 'Neutral')}</span>
          </div>
          <div class="reason-copy">${esc(reasoning.summary || 'No AI summary yet.')}</div>
        </div>
        ${[
          ['Trend', reasoning.trend],
          ['Levels', reasoning.levels],
          ['Volume', reasoning.volume],
          ['Latest News', reasoning.news_summary],
          ['News Impact', reasoning.news_reasoning],
          ['Key Events', reasoning.key_events],
          ['Market Perception', reasoning.market_perception],
          ['Expectation', reasoning.expectation],
          ['Rerating Trigger', reasoning.rerating_trigger],
          ['Risk', reasoning.risk],
        ].map(([label, text]) => `
          <div class="reason-card">
            <div class="mini-label">${esc(label)}</div>
            <div class="reason-copy">${esc(text || 'n/a')}</div>
          </div>
        `).join('')}
        <div class="reason-card">
          <div class="mini-label">AI News Breakdown</div>
          <div class="headline-list">
            ${headlineImpacts.length ? headlineImpacts.map((item) => `
              <div class="headline-item">
                <div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;">
                  <div style="font-weight:600;">${esc(item.headline || 'Headline')}</div>
                  <div class="tone-pill ${sentimentClass(item.tone)}">${esc(item.tone || 'Neutral')}</div>
                </div>
                <div class="tiny-copy" style="margin-top:6px;">${esc(item.source || 'News feed')}</div>
                <div style="margin-top:8px;color:var(--muted);">${esc(item.summary || 'No summary from feed.')}</div>
                <div style="margin-top:10px;">${esc(item.impact || 'No impact read available.')}</div>
              </div>
            `).join('') : '<div class="headline-item">No headline-by-headline impact notes were returned.</div>'}
          </div>
        </div>
        <div class="reason-card">
          <div class="mini-label">Company Context</div>
          <div class="reason-copy">${esc(detail.company_name || state.chartSymbol)}${detail.sector ? ` | ${esc(detail.sector)}` : ''}${detail.themes?.length ? ` | Themes: ${esc(detail.themes.join(', '))}` : ''}</div>
        </div>
        <div class="reason-card">
          <div class="mini-label">Recent Headlines</div>
          <div class="headline-list">${(workspace.headlines || []).map((item) => `
            <a class="headline-item" href="${esc(item.url || '#')}" target="_blank" rel="noreferrer">
              <div style="font-weight:600;">${esc(item.title || 'Headline')}</div>
              <div class="tiny-copy">${esc(item.source || item.ticker || '')}</div>
              <div style="margin-top:8px;color:var(--muted);">${esc(item.summary || 'No summary from feed.')}</div>
            </a>
          `).join('') || '<div class="headline-item">No recent headlines were returned.</div>'}</div>
        </div>
      </div>
    ` : '';

    return `
      <div class="main-stack">
        <section class="panel section">
          <div class="section-head">
            <div>
              <div class="section-kicker">Chart Desk</div>
              <h2>TradingView chart plus AI reasoning</h2>
              <p>Type any symbol, or click one anywhere in the dashboard, to load a chart and a side-by-side AI explanation of what the structure is doing and what the latest news likely means.</p>
            </div>
          </div>
          <div class="chart-controls">
            <div class="intervals">
              ${INTERVALS.map((item) => `<button class="interval-btn ${state.chartInterval === item.value ? 'active' : ''}" data-chart-interval="${item.value}">${item.label}</button>`).join('')}
            </div>
            <div class="chart-input-row">
              <input id="chart-symbol-input" class="chart-input" value="${esc(state.chartInput)}" maxlength="20" />
              <button class="action-btn" data-chart-go>Load Desk</button>
              <button class="action-btn" data-open-external="${esc(chartSymbolFor(state.chartSymbol))}">Open in TradingView</button>
            </div>
          </div>
          ${statsHtml}
        </section>
        <section class="chart-layout">
          <div class="panel section">
            <div class="chart-frame">
              <iframe title="TradingView chart" src="${chartSrc(state.chartSymbol, state.chartInterval)}"></iframe>
            </div>
          </div>
          <div>${reasoningHtml || emptyState('AI reasoning will appear here once a symbol is loaded.')}</div>
        </section>
      </div>
    `;
  }

  function renderActiveTab() {
    if (state.activeTab === 'premarket') return renderPremarketTab();
    if (state.activeTab === 'postmarket') return renderPostmarketTab();
    if (state.activeTab === 'themes') return renderThemesTab();
    if (state.activeTab === 'flows') return renderFlowsTab();
    if (state.activeTab === 'rrg') return renderRrgTab();
    if (state.activeTab === 'earnings') return renderEarningsTab();
    if (state.activeTab === 'chart') return renderChartTab();
    return renderOverviewTab();
  }

  function render() {
    const root = document.getElementById('app');
    if (!root) return;
    root.innerHTML = `
      <div class="app-shell">
        ${renderHeader()}
        ${renderActiveTab()}
      </div>
    `;
  }

  async function loadOverviewBundle(force = false) {
    if (state.loading.overview && !force) return;
    state.loading.overview = true;
    state.errors.overview = '';
    render();
    try {
      state.overview = await api('/api/market-overview');
    } catch (error) {
      console.error(error);
      state.errors.overview = 'Unable to load the market overview right now.';
    } finally {
      state.loading.overview = false;
      render();
    }
  }

  async function loadBrief(force = false) {
    if (state.loading.brief && !force) return;
    state.loading.brief = true;
    state.errors.brief = '';
    render();
    try {
      state.briefData = await api('/api/market-brief');
    } catch (error) {
      console.error(error);
      state.errors.brief = 'Unable to build the market brief right now.';
    } finally {
      state.loading.brief = false;
      render();
    }
  }

  async function loadThemes(force = false) {
    if (state.loading.themes && !force) return;
    state.loading.themes = true;
    state.errors.themes = '';
    render();
    try {
      state.themes = normalizeThemePayload(await api('/api/theme-dashboard'));
      if (!state.selectedTheme && state.themes?.all?.length) {
        state.selectedTheme = state.themes.all[0].theme;
      }
    } catch (error) {
      console.error(error);
      state.themes = normalizeThemePayload(null);
      if (!state.selectedTheme && state.themes?.all?.length) {
        state.selectedTheme = state.themes.all[0].theme;
      }
      state.errors.themes = 'Live theme pricing is unavailable right now. Showing the full configured theme membership instead.';
    } finally {
      state.loading.themes = false;
      render();
    }
  }
  async function loadEtfs(force = false) {
    if (state.loading.etfs && !force) return;
    state.loading.etfs = true;
    state.errors.etfs = '';
    render();
    try {
      state.etfs = await api('/api/etf-dashboard');
    } catch (error) {
      console.error(error);
      state.errors.etfs = 'Unable to load the ETF flow board right now.';
    } finally {
      state.loading.etfs = false;
      render();
    }
  }

  async function loadRrg(force = false) {
    if (state.loading.rrg && !force) return;
    state.loading.rrg = true;
    state.errors.rrg = '';
    render();
    try {
      state.rrg = await api('/api/etf-rrg');
    } catch (error) {
      console.error(error);
      state.errors.rrg = 'Unable to load the ETF RRG right now.';
    } finally {
      state.loading.rrg = false;
      render();
    }
  }

  async function loadPremarket(force = false) {
    if (state.loading.premarket && !force) return;
    state.loading.premarket = true;
    state.errors.premarket = '';
    render();
    try {
      state.premarket = await api('/api/session-movers/pre?min_move=0.5&limit=15');
    } catch (error) {
      console.error(error);
      state.errors.premarket = 'Unable to load the pre-market movers right now.';
    } finally {
      state.loading.premarket = false;
      render();
    }
  }

  async function loadPostmarket(force = false) {
    if (state.loading.postmarket && !force) return;
    state.loading.postmarket = true;
    state.errors.postmarket = '';
    render();
    try {
      state.postmarket = await api('/api/session-movers/post?min_move=0.5&limit=15');
    } catch (error) {
      console.error(error);
      state.errors.postmarket = 'Unable to load the post-market movers right now.';
    } finally {
      state.loading.postmarket = false;
      render();
    }
  }
  async function loadEarnings(force = false) {
    if (state.loading.earnings && !force) return;
    state.loading.earnings = true;
    state.errors.earnings = '';
    render();
    try {
      state.earnings = await api('/api/earnings-tracker');
    } catch (error) {
      console.error(error);
      state.errors.earnings = 'Unable to load the earnings tracker right now.';
    } finally {
      state.loading.earnings = false;
      render();
    }
  }
  async function loadChartWorkspace(symbol, force = false) {
    const clean = String(symbol || state.chartSymbol || '').trim().toUpperCase();
    if (!clean) return;
    if (state.loading.chart && !force) return;
    state.chartSymbol = clean;
    state.chartInput = clean;
    state.chartWorkspace = null;
    state.loading.chart = true;
    state.errors.chart = '';
    render();
    try {
      state.chartWorkspace = await api(`/api/chart-workspace/${encodeURIComponent(clean)}`);
    } catch (error) {
      console.error(error);
      state.errors.chart = 'Unable to load the chart workspace right now.';
    } finally {
      state.loading.chart = false;
      render();
    }
  }

  function handleClick(event) {
    const tabButton = event.target.closest('[data-tab]');
    if (tabButton) {
      state.activeTab = tabButton.dataset.tab;
      render();
      if (state.activeTab === 'premarket' && !state.premarket) loadPremarket();
      if (state.activeTab === 'postmarket' && !state.postmarket) loadPostmarket();
      if (state.activeTab === 'chart' && !state.chartWorkspace) loadChartWorkspace(state.chartSymbol);
      if (state.activeTab === 'earnings' && !state.earnings) loadEarnings();
      return;
    }

    const refreshButton = event.target.closest('[data-refresh]');
    if (refreshButton) {
      const target = refreshButton.dataset.refresh;
      if (target === 'overview') { loadOverviewBundle(true); loadBrief(true); }
      if (target === 'premarket') loadPremarket(true);
      if (target === 'postmarket') loadPostmarket(true);
      if (target === 'themes') loadThemes(true);
      if (target === 'flows') loadEtfs(true);
      if (target === 'rrg') loadRrg(true);
      if (target === 'earnings') loadEarnings(true);
      return;
    }

    const filterButton = event.target.closest('[data-theme-filter]');
    if (filterButton) {
      state.themeFilter = filterButton.dataset.themeFilter;
      render();
      return;
    }

    const themeButton = event.target.closest('[data-theme-select]');
    if (themeButton) {
      state.selectedTheme = themeButton.dataset.themeSelect;
      render();
      return;
    }

    const deskButton = event.target.closest('[data-open-desk]');
    if (deskButton) {
      openChartDesk(deskButton.dataset.openDesk);
      return;
    }

    const intervalButton = event.target.closest('[data-chart-interval]');
    if (intervalButton) {
      state.chartInterval = intervalButton.dataset.chartInterval;
      render();
      return;
    }

    if (event.target.closest('[data-chart-go]')) {
      openChartDesk(state.chartInput || state.chartSymbol);
      return;
    }

    const externalButton = event.target.closest('[data-open-external]');
    if (externalButton) {
      window.open(`https://www.tradingview.com/chart/?symbol=${encodeURIComponent(externalButton.dataset.openExternal)}`, '_blank', 'noopener');
    }
  }

  function handleInput(event) {
    if (event.target.id === 'chart-symbol-input') {
      state.chartInput = event.target.value.toUpperCase();
    }
  }

  function handleKeydown(event) {
    if (event.target.id === 'chart-symbol-input' && event.key === 'Enter') {
      openChartDesk(state.chartInput || state.chartSymbol);
    }
  }

  function boot() {
    setClock();
    render();
    loadOverviewBundle();
    loadBrief();
    loadPremarket();
    loadPostmarket();
    loadThemes();
    loadEtfs();
    loadRrg();
    loadChartWorkspace(state.chartSymbol);
    document.addEventListener('click', handleClick);
    document.addEventListener('input', handleInput);
    document.addEventListener('keydown', handleKeydown);
    window.setInterval(setClock, 1000);
    window.setInterval(() => loadOverviewBundle(true), AUTO_REFRESH_MS);
    window.setInterval(() => loadPremarket(true), AUTO_REFRESH_MS);
    window.setInterval(() => loadPostmarket(true), AUTO_REFRESH_MS);
    window.setInterval(() => loadThemes(true), AUTO_REFRESH_MS);
    window.setInterval(() => loadEtfs(true), AUTO_REFRESH_MS);
    window.setInterval(() => loadRrg(true), AUTO_REFRESH_MS);
    window.setInterval(() => { if (state.earnings) loadEarnings(true); }, AUTO_REFRESH_MS);
    window.setInterval(() => loadBrief(true), AUTO_REFRESH_MS);
  }

  boot();
})();



















