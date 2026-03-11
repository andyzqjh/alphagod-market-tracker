import { useEffect, useMemo, useState } from 'react'
import axios from 'axios'

const REFRESH_MS = 60000
const GROUP_ORDER = ['Major Indexes', 'Risk Assets', 'Macro', 'Digital Assets']

function formatNumber(value, digits = 2) {
  if (value == null) return 'n/a'
  return Number(value).toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

function formatPrice(item) {
  if (item.price == null) return 'n/a'
  const prefix = item.symbol.startsWith('^') ? '' : '$'
  return `${prefix}${formatNumber(item.price, item.price >= 1000 ? 0 : 2)}`
}

function formatPercent(value) {
  if (value == null) return 'n/a'
  return `${value > 0 ? '+' : ''}${Number(value).toFixed(2)}%`
}

function formatVolume(value) {
  if (value == null) return 'n/a'
  if (value >= 1e9) return `${(value / 1e9).toFixed(2)}B`
  if (value >= 1e6) return `${(value / 1e6).toFixed(2)}M`
  if (value >= 1e3) return `${(value / 1e3).toFixed(1)}K`
  return String(value)
}

function QuoteCard({ item, onTickerClick }) {
  const positive = (item.change_pct || 0) >= 0
  const extendedPositive = (item.extended_change_pct || 0) >= 0

  return (
    <article className="dashboard-card rounded-3xl p-5">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-xs uppercase tracking-[0.25em] text-slate-500">{item.symbol}</div>
          <h3 className="mt-2 text-lg font-semibold text-white">{item.label}</h3>
        </div>
        <button
          onClick={() => onTickerClick(item.symbol.replace('^', ''))}
          className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-300 transition hover:border-sky-400/40 hover:text-white"
        >
          Chart
        </button>
      </div>

      <div className="mt-6 flex items-end justify-between gap-4">
        <div>
          <div className="text-3xl font-semibold text-white">{formatPrice(item)}</div>
          <div className={`mt-2 text-sm font-medium ${positive ? 'pos' : 'neg'}`}>
            {formatPercent(item.change_pct)}
            {item.change != null && (
              <span className="ml-2 text-xs text-slate-400">{item.change > 0 ? '+' : ''}{formatNumber(item.change)}</span>
            )}
          </div>
        </div>

        {item.extended_price != null && (
          <div className={`rounded-2xl border px-3 py-2 text-right text-xs ${extendedPositive ? 'border-emerald-500/30 bg-emerald-500/10 text-emerald-200' : 'border-rose-500/30 bg-rose-500/10 text-rose-200'}`}>
            <div className="uppercase tracking-[0.2em] text-[10px] opacity-80">{item.extended_session === 'pre' ? 'Pre market' : 'After hours'}</div>
            <div className="mt-1 font-semibold">${formatNumber(item.extended_price)}</div>
            <div>{formatPercent(item.extended_change_pct)}</div>
          </div>
        )}
      </div>

      <div className="mt-5 grid grid-cols-2 gap-3 text-sm text-slate-400">
        <div className="rounded-2xl bg-slate-950/40 px-3 py-2">
          <div className="text-[10px] uppercase tracking-[0.25em] text-slate-500">Day range</div>
          <div className="mt-2 text-slate-200">
            {item.day_low != null && item.day_high != null
              ? `${formatNumber(item.day_low)} - ${formatNumber(item.day_high)}`
              : 'n/a'}
          </div>
        </div>
        <div className="rounded-2xl bg-slate-950/40 px-3 py-2">
          <div className="text-[10px] uppercase tracking-[0.25em] text-slate-500">Volume</div>
          <div className="mt-2 text-slate-200">{formatVolume(item.volume)}</div>
        </div>
      </div>
    </article>
  )
}

export default function MarketOverview({ onTickerClick, onOpenThemes, onOpenRrg }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    let mounted = true

    async function load(isRefresh = false) {
      if (!mounted) return
      if (isRefresh) {
        setRefreshing(true)
      } else {
        setLoading(true)
      }

      try {
        const response = await axios.get('/api/market-overview')
        if (!mounted) return
        setData(response.data)
        setError('')
      } catch (err) {
        if (!mounted) return
        console.error(err)
        setError('Unable to load live market prices right now.')
      } finally {
        if (!mounted) return
        setLoading(false)
        setRefreshing(false)
      }
    }

    load()
    const timer = window.setInterval(() => load(true), REFRESH_MS)

    return () => {
      mounted = false
      window.clearInterval(timer)
    }
  }, [])

  const groupedItems = useMemo(() => {
    const buckets = new Map()
    ;(data?.items || []).forEach((item) => {
      if (!buckets.has(item.group)) {
        buckets.set(item.group, [])
      }
      buckets.get(item.group).push(item)
    })

    return GROUP_ORDER.filter((group) => buckets.has(group)).map((group) => ({
      group,
      items: buckets.get(group),
    }))
  }, [data])

  const lastUpdated = data?.updated_at
    ? new Date(data.updated_at).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', second: '2-digit' })
    : null

  return (
    <div className="space-y-6">
      <section className="grid gap-4 xl:grid-cols-[1.3fr,0.7fr]">
        <div className="panel rounded-[28px] p-6 md:p-7">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <div className="section-kicker">Market Overview</div>
              <h2 className="mt-3 text-2xl font-semibold text-white">Live market snapshot from Yahoo Finance</h2>
              <p className="mt-2 max-w-3xl text-sm text-slate-400">
                Major indexes, risk assets, macro gauges, and digital assets update on an automatic refresh cycle.
              </p>
            </div>
            <button
              onClick={() => window.location.reload()}
              className="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-300 transition hover:border-sky-400/40 hover:text-white"
            >
              {refreshing ? 'Refreshing...' : 'Hard refresh'}
            </button>
          </div>

          <div className="mt-6 grid gap-4 sm:grid-cols-3">
            <div className="summary-card">
              <div className="summary-label">Advancing</div>
              <div className="summary-value pos">{data?.summary?.positive ?? '--'}</div>
            </div>
            <div className="summary-card">
              <div className="summary-label">Declining</div>
              <div className="summary-value neg">{data?.summary?.negative ?? '--'}</div>
            </div>
            <div className="summary-card">
              <div className="summary-label">Updated</div>
              <div className="summary-value text-white">{lastUpdated || '--'}</div>
            </div>
          </div>
        </div>

        <div className="panel rounded-[28px] p-6 md:p-7">
          <div className="section-kicker">Jump To</div>
          <h2 className="mt-3 text-2xl font-semibold text-white">Themes and ETF rotation</h2>
          <p className="mt-2 text-sm text-slate-400">
            Use the theme tab for best, worst, and all theme cards. Use the ETF RRG tab for the full sector rotation map relative to SPY.
          </p>

          <div className="mt-6 grid gap-3">
            <button onClick={onOpenThemes} className="cta-card text-left transition hover:border-emerald-400/40">
              <div className="text-xs uppercase tracking-[0.25em] text-emerald-200/80">Themes</div>
              <div className="mt-2 text-lg font-semibold text-white">Best, worst, and all themes</div>
              <div className="mt-1 text-sm text-slate-400">Breadth, average performance, and ticker-level leaders inside each theme.</div>
            </button>
            <button onClick={onOpenRrg} className="cta-card text-left transition hover:border-sky-400/40">
              <div className="text-xs uppercase tracking-[0.25em] text-sky-200/80">ETF RRG</div>
              <div className="mt-2 text-lg font-semibold text-white">All sector ETFs like XLK, XLF, and XLE</div>
              <div className="mt-1 text-sm text-slate-400">Relative strength ratio and momentum quadrants with recent RRG trails.</div>
            </button>
          </div>

          {error && <div className="mt-5 rounded-2xl border border-rose-500/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">{error}</div>}
        </div>
      </section>

      {loading && !data ? (
        <div className="panel rounded-[28px] px-6 py-16 text-center text-slate-400">Loading live market overview...</div>
      ) : (
        groupedItems.map(({ group, items }) => (
          <section key={group} className="panel rounded-[28px] p-6 md:p-7">
            <div className="flex items-center justify-between gap-4">
              <div>
                <div className="section-kicker">{group}</div>
                <h3 className="mt-3 text-xl font-semibold text-white">{group}</h3>
              </div>
              <div className="text-xs text-slate-500">Click any card to jump into a chart.</div>
            </div>
            <div className="mt-6 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
              {items.map((item) => (
                <QuoteCard key={item.symbol} item={item} onTickerClick={onTickerClick} />
              ))}
            </div>
          </section>
        ))
      )}
    </div>
  )
}
