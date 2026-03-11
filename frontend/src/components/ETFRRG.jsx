import { useEffect, useMemo, useState } from 'react'
import axios from 'axios'

const REFRESH_MS = 180000
const QUADRANT_COLORS = {
  Leading: '#34d399',
  Weakening: '#f59e0b',
  Lagging: '#f87171',
  Improving: '#60a5fa',
}

function formatPercent(value) {
  if (value == null) return 'n/a'
  return `${value > 0 ? '+' : ''}${Number(value).toFixed(2)}%`
}

function formatPrice(value) {
  if (value == null) return 'n/a'
  return `$${Number(value).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}

function RRGChart({ items }) {
  const width = 920
  const height = 560
  const padding = 72
  const plotWidth = width - padding * 2
  const plotHeight = height - padding * 2

  const xValues = items.flatMap((item) => item.trail.map((point) => point.rs_ratio))
  const yValues = items.flatMap((item) => item.trail.map((point) => point.rs_momentum))
  const minX = Math.min(98, ...xValues) - 0.4
  const maxX = Math.max(102, ...xValues) + 0.4
  const minY = Math.min(98, ...yValues) - 0.4
  const maxY = Math.max(102, ...yValues) + 0.4

  const xScale = (value) => padding + ((value - minX) / (maxX - minX || 1)) * plotWidth
  const yScale = (value) => height - padding - ((value - minY) / (maxY - minY || 1)) * plotHeight
  const centerX = xScale(100)
  const centerY = yScale(100)

  return (
    <div className="overflow-hidden rounded-[28px] border border-white/10 bg-slate-950/40">
      <svg viewBox={`0 0 ${width} ${height}`} className="w-full">
        <rect x="0" y="0" width={width} height={height} fill="#07111f" />
        <rect x={padding} y={padding} width={centerX - padding} height={centerY - padding} fill="rgba(96, 165, 250, 0.08)" />
        <rect x={centerX} y={padding} width={width - padding - centerX} height={centerY - padding} fill="rgba(52, 211, 153, 0.08)" />
        <rect x={padding} y={centerY} width={centerX - padding} height={height - padding - centerY} fill="rgba(248, 113, 113, 0.08)" />
        <rect x={centerX} y={centerY} width={width - padding - centerX} height={height - padding - centerY} fill="rgba(245, 158, 11, 0.08)" />

        {[...Array(5)].map((_, index) => {
          const x = padding + (plotWidth / 4) * index
          const y = padding + (plotHeight / 4) * index
          return (
            <g key={index}>
              <line x1={x} x2={x} y1={padding} y2={height - padding} stroke="rgba(148, 163, 184, 0.12)" strokeWidth="1" />
              <line x1={padding} x2={width - padding} y1={y} y2={y} stroke="rgba(148, 163, 184, 0.12)" strokeWidth="1" />
            </g>
          )
        })}

        <line x1={centerX} x2={centerX} y1={padding} y2={height - padding} stroke="rgba(226, 232, 240, 0.45)" strokeWidth="1.5" strokeDasharray="6 8" />
        <line x1={padding} x2={width - padding} y1={centerY} y2={centerY} stroke="rgba(226, 232, 240, 0.45)" strokeWidth="1.5" strokeDasharray="6 8" />

        <text x={padding + 16} y={padding + 28} fill="rgba(191, 219, 254, 0.9)" fontSize="20" fontWeight="700">Improving</text>
        <text x={width - padding - 140} y={padding + 28} fill="rgba(167, 243, 208, 0.9)" fontSize="20" fontWeight="700">Leading</text>
        <text x={padding + 16} y={height - padding - 18} fill="rgba(254, 202, 202, 0.9)" fontSize="20" fontWeight="700">Lagging</text>
        <text x={width - padding - 164} y={height - padding - 18} fill="rgba(253, 230, 138, 0.9)" fontSize="20" fontWeight="700">Weakening</text>

        <text x={width / 2} y={height - 20} textAnchor="middle" fill="rgba(148, 163, 184, 0.9)" fontSize="16">JdK RS-Ratio style axis</text>
        <text x="24" y={height / 2} textAnchor="middle" transform={`rotate(-90 24 ${height / 2})`} fill="rgba(148, 163, 184, 0.9)" fontSize="16">JdK RS-Momentum style axis</text>

        {items.map((item, index) => {
          const color = QUADRANT_COLORS[item.quadrant] || '#cbd5f5'
          const points = item.trail.map((point) => `${xScale(point.rs_ratio)},${yScale(point.rs_momentum)}`).join(' ')
          const latest = item.trail[item.trail.length - 1]
          const latestX = xScale(latest.rs_ratio)
          const latestY = yScale(latest.rs_momentum)
          const labelOffset = index % 2 === 0 ? -14 : 18

          return (
            <g key={item.symbol}>
              <polyline fill="none" points={points} stroke={color} strokeOpacity="0.45" strokeWidth="2.5" />
              {item.trail.map((point, trailIndex) => (
                <circle
                  key={`${item.symbol}-${point.date}`}
                  cx={xScale(point.rs_ratio)}
                  cy={yScale(point.rs_momentum)}
                  r={trailIndex === item.trail.length - 1 ? 7 : 4}
                  fill={color}
                  fillOpacity={trailIndex === item.trail.length - 1 ? 1 : 0.35}
                  stroke="#07111f"
                  strokeWidth="2"
                />
              ))}
              <text x={latestX + 10} y={latestY + labelOffset} fill={color} fontSize="16" fontWeight="700">{item.symbol}</text>
            </g>
          )
        })}
      </svg>
    </div>
  )
}

export default function ETFRRG({ onTickerClick }) {
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
        const response = await axios.get('/api/etf-rrg')
        if (!mounted) return
        setData(response.data)
        setError(response.data?.error || '')
      } catch (err) {
        if (!mounted) return
        console.error(err)
        setError('Unable to load ETF RRG data right now.')
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

  const counts = useMemo(() => {
    return (data?.items || []).reduce(
      (accumulator, item) => {
        accumulator[item.quadrant] = (accumulator[item.quadrant] || 0) + 1
        return accumulator
      },
      { Leading: 0, Weakening: 0, Lagging: 0, Improving: 0 },
    )
  }, [data])

  const lastUpdated = data?.updated_at
    ? new Date(data.updated_at).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', second: '2-digit' })
    : null

  return (
    <div className="space-y-6">
      <section className="panel rounded-[28px] p-6 md:p-7">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <div className="section-kicker">ETF RRG</div>
            <h2 className="mt-3 text-2xl font-semibold text-white">All sector ETFs like XLK, XLF, XLE, and more</h2>
            <p className="mt-2 max-w-3xl text-sm text-slate-400">
              Relative Rotation Graph style view of sector ETF strength versus {data?.benchmark?.symbol || 'SPY'} using Yahoo Finance history.
            </p>
          </div>
          <button
            onClick={() => window.location.reload()}
            className="rounded-full border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-300 transition hover:border-sky-400/40 hover:text-white"
          >
            {refreshing ? 'Refreshing...' : 'Hard refresh'}
          </button>
        </div>

        <div className="mt-6 grid gap-4 md:grid-cols-5">
          <div className="summary-card">
            <div className="summary-label">Benchmark</div>
            <div className="summary-value text-white">{data?.benchmark?.symbol || 'SPY'}</div>
            <div className="mt-2 text-sm text-slate-400">{formatPrice(data?.benchmark?.price)} / {formatPercent(data?.benchmark?.change_pct)}</div>
          </div>
          <div className="summary-card">
            <div className="summary-label">Leading</div>
            <div className="summary-value" style={{ color: QUADRANT_COLORS.Leading }}>{counts.Leading}</div>
          </div>
          <div className="summary-card">
            <div className="summary-label">Improving</div>
            <div className="summary-value" style={{ color: QUADRANT_COLORS.Improving }}>{counts.Improving}</div>
          </div>
          <div className="summary-card">
            <div className="summary-label">Weakening</div>
            <div className="summary-value" style={{ color: QUADRANT_COLORS.Weakening }}>{counts.Weakening}</div>
          </div>
          <div className="summary-card">
            <div className="summary-label">Lagging</div>
            <div className="summary-value" style={{ color: QUADRANT_COLORS.Lagging }}>{counts.Lagging}</div>
            <div className="mt-2 text-sm text-slate-400">Updated {lastUpdated || '--'}</div>
          </div>
        </div>
      </section>

      {loading && !data ? (
        <div className="panel rounded-[28px] px-6 py-16 text-center text-slate-400">Calculating ETF relative rotation graph...</div>
      ) : (
        <>
          <section className="panel rounded-[28px] p-6 md:p-7">
            {error ? (
              <div className="rounded-2xl border border-rose-500/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">{error}</div>
            ) : (
              <RRGChart items={data?.items || []} />
            )}
          </section>

          <section className="panel rounded-[28px] p-6 md:p-7">
            <div className="flex items-center justify-between gap-4">
              <div>
                <div className="section-kicker">Constituents</div>
                <h3 className="mt-3 text-xl font-semibold text-white">Sector ETF breakdown</h3>
              </div>
              <div className="flex flex-wrap gap-2 text-xs text-slate-300">
                {Object.entries(QUADRANT_COLORS).map(([quadrant, color]) => (
                  <span key={quadrant} className="status-pill" style={{ borderColor: `${color}55`, color }}>
                    {quadrant}
                  </span>
                ))}
              </div>
            </div>

            <div className="mt-6 overflow-x-auto">
              <table className="w-full min-w-[780px] text-left">
                <thead>
                  <tr className="border-b border-white/10 text-xs uppercase tracking-[0.2em] text-slate-500">
                    <th className="px-3 py-3">ETF</th>
                    <th className="px-3 py-3">Sector</th>
                    <th className="px-3 py-3">Price</th>
                    <th className="px-3 py-3">1D %</th>
                    <th className="px-3 py-3">Weekly %</th>
                    <th className="px-3 py-3">RS-Ratio</th>
                    <th className="px-3 py-3">RS-Momentum</th>
                    <th className="px-3 py-3">Quadrant</th>
                    <th className="px-3 py-3"></th>
                  </tr>
                </thead>
                <tbody>
                  {(data?.items || []).map((item) => (
                    <tr key={item.symbol} className="border-b border-white/5 text-sm text-slate-300">
                      <td className="px-3 py-4 font-mono font-semibold text-white">{item.symbol}</td>
                      <td className="px-3 py-4">{item.label}</td>
                      <td className="px-3 py-4">{formatPrice(item.price)}</td>
                      <td className={`px-3 py-4 ${(item.change_pct || 0) >= 0 ? 'pos' : 'neg'}`}>{formatPercent(item.change_pct)}</td>
                      <td className={`px-3 py-4 ${(item.weekly_change_pct || 0) >= 0 ? 'pos' : 'neg'}`}>{formatPercent(item.weekly_change_pct)}</td>
                      <td className="px-3 py-4">{item.rs_ratio?.toFixed(2)}</td>
                      <td className="px-3 py-4">{item.rs_momentum?.toFixed(2)}</td>
                      <td className="px-3 py-4">
                        <span className="status-pill" style={{ borderColor: `${QUADRANT_COLORS[item.quadrant]}55`, color: QUADRANT_COLORS[item.quadrant] }}>
                          {item.quadrant}
                        </span>
                      </td>
                      <td className="px-3 py-4 text-right">
                        <button
                          onClick={() => onTickerClick(item.symbol)}
                          className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-300 transition hover:border-sky-400/40 hover:text-white"
                        >
                          Chart
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      )}
    </div>
  )
}
