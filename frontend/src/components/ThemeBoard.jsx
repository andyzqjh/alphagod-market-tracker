import { useEffect, useMemo, useState } from 'react'
import axios from 'axios'

const REFRESH_MS = 120000
const FILTERS = [
  { id: 'leaders', label: 'Best Themes' },
  { id: 'laggards', label: 'Worst Themes' },
  { id: 'all', label: 'All Themes' },
]

function formatPercent(value) {
  if (value == null) return 'n/a'
  return `${value > 0 ? '+' : ''}${Number(value).toFixed(2)}%`
}

function PctBar({ pct }) {
  const width = Math.min(Math.abs(pct || 0) * 10, 100)
  const positive = (pct || 0) >= 0

  return (
    <div className="theme-bar mt-3">
      <div
        className="theme-bar__fill"
        style={{
          width: `${width}%`,
          background: positive ? 'linear-gradient(90deg, #1dd1a1, #6ee7b7)' : 'linear-gradient(90deg, #fb7185, #fda4af)',
        }}
      />
    </div>
  )
}

function StockList({ label, items, onTickerClick }) {
  if (!items?.length) return null

  return (
    <div className="rounded-3xl border border-white/8 bg-slate-950/35 p-4">
      <div className="text-[10px] uppercase tracking-[0.28em] text-slate-500">{label}</div>
      <div className="mt-3 space-y-2">
        {items.map((stock) => (
          <div key={`${label}-${stock.ticker}`} className="flex items-center justify-between gap-3">
            <button
              onClick={() => onTickerClick(stock.ticker)}
              className="font-mono text-sm text-sky-300 transition hover:text-white"
            >
              {stock.ticker}
            </button>
            <span className={`text-sm font-medium ${(stock.display_pct || 0) >= 0 ? 'pos' : 'neg'}`}>
              {formatPercent(stock.display_pct)}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}

function ThemeCard({ theme, onTickerClick }) {
  const positive = (theme.avg_pct || 0) >= 0

  return (
    <article className="dashboard-card rounded-[28px] p-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="text-xs uppercase tracking-[0.25em] text-slate-500">Theme</div>
          <h3 className="mt-2 text-xl font-semibold text-white">{theme.theme}</h3>
        </div>
        <div className={`text-right text-lg font-semibold ${positive ? 'pos' : 'neg'}`}>{formatPercent(theme.avg_pct)}</div>
      </div>

      <PctBar pct={theme.avg_pct} />

      <div className="mt-4 flex flex-wrap gap-2 text-xs text-slate-300">
        <span className="status-pill">{theme.stock_count} stocks</span>
        <span className="status-pill pos">{theme.up_count} up</span>
        <span className="status-pill neg">{theme.down_count} down</span>
      </div>

      <div className="mt-5 grid gap-3 lg:grid-cols-2">
        <StockList label="Leadership" items={theme.leaders} onTickerClick={onTickerClick} />
        <StockList label="Pressure" items={theme.laggards} onTickerClick={onTickerClick} />
      </div>
    </article>
  )
}

export default function ThemeBoard({ onTickerClick }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState('')
  const [filter, setFilter] = useState('all')

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
        const response = await axios.get('/api/theme-dashboard')
        if (!mounted) return
        setData(response.data)
        setError('')
      } catch (err) {
        if (!mounted) return
        console.error(err)
        setError('Unable to load theme performance right now.')
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

  const visibleThemes = useMemo(() => {
    if (!data) return []
    if (filter === 'leaders') return data.leaders || []
    if (filter === 'laggards') return data.laggards || []
    return data.all || []
  }, [data, filter])

  const lastUpdated = data?.updated_at
    ? new Date(data.updated_at).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', second: '2-digit' })
    : null

  return (
    <div className="space-y-6">
      <section className="panel rounded-[28px] p-6 md:p-7">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <div className="section-kicker">Theme Board</div>
            <h2 className="mt-3 text-2xl font-semibold text-white">Best themes, worst themes, and every configured theme</h2>
            <p className="mt-2 max-w-3xl text-sm text-slate-400">
              Theme cards are ranked by average move across their components using the best available live or extended Yahoo Finance quote.
            </p>
          </div>
          <div className="text-right text-sm text-slate-400">
            <div>Updated {lastUpdated || '--'}</div>
            <div className="mt-2">{refreshing ? 'Refreshing...' : 'Auto refresh every 2 minutes'}</div>
          </div>
        </div>

        <div className="mt-6 grid gap-4 md:grid-cols-4">
          <div className="summary-card">
            <div className="summary-label">Total themes</div>
            <div className="summary-value text-white">{data?.summary?.total_themes ?? '--'}</div>
          </div>
          <div className="summary-card">
            <div className="summary-label">Positive breadth</div>
            <div className="summary-value pos">{data?.summary?.positive ?? '--'}</div>
          </div>
          <div className="summary-card">
            <div className="summary-label">Negative breadth</div>
            <div className="summary-value neg">{data?.summary?.negative ?? '--'}</div>
          </div>
          <div className="summary-card">
            <div className="summary-label">Best / Worst</div>
            <div className="mt-2 text-sm text-white">{data?.summary?.best_theme || '--'}</div>
            <div className="mt-1 text-sm text-slate-400">{data?.summary?.worst_theme || '--'}</div>
          </div>
        </div>

        <div className="mt-6 flex flex-wrap gap-2">
          {FILTERS.map((item) => (
            <button
              key={item.id}
              onClick={() => setFilter(item.id)}
              className={`tab-pill ${filter === item.id ? 'active' : ''}`}
            >
              {item.label}
            </button>
          ))}
        </div>

        {error && <div className="mt-5 rounded-2xl border border-rose-500/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">{error}</div>}
      </section>

      {loading && !data ? (
        <div className="panel rounded-[28px] px-6 py-16 text-center text-slate-400">Loading theme performance...</div>
      ) : (
        <section className="grid gap-4 xl:grid-cols-2 2xl:grid-cols-3">
          {visibleThemes.map((theme) => (
            <ThemeCard key={theme.theme} theme={theme} onTickerClick={onTickerClick} />
          ))}
        </section>
      )}
    </div>
  )
}
