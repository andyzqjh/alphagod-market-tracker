import { useEffect, useState } from 'react'
import ETFRRG from './components/ETFRRG'
import MarketOverview from './components/MarketOverview'
import Screener from './components/Screener'
import StockChart from './components/StockChart'
import ThemeBoard from './components/ThemeBoard'

const TABS = ['Overview', 'Themes', 'ETF RRG', 'Pre-Market Screener', 'Chart']

function formatClock(date) {
  return date.toLocaleString('en-US', {
    weekday: 'short',
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
  })
}

export default function App() {
  const [tab, setTab] = useState('Overview')
  const [chartTicker, setChartTicker] = useState('SPY')
  const [now, setNow] = useState(new Date())

  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 1000)
    return () => window.clearInterval(timer)
  }, [])

  function openChart(ticker) {
    setChartTicker(ticker)
    setTab('Chart')
  }

  return (
    <div className="min-h-screen px-4 py-5 md:px-6 lg:px-8">
      <div className="mx-auto max-w-[1600px] space-y-6">
        <header className="panel rounded-[28px] p-6 md:p-8">
          <div className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
            <div className="space-y-3">
              <div className="flex flex-wrap items-center gap-3 text-xs uppercase tracking-[0.35em] text-sky-200/75">
                <span className="status-pill">Live Dashboard</span>
                <span>Yahoo Finance stream</span>
                <span>Auto refresh enabled</span>
              </div>
              <div>
                <h1 className="text-3xl font-semibold tracking-tight text-white md:text-5xl">
                  Market dashboard with live rotation, themes, and sector ETF RRG.
                </h1>
                <p className="mt-3 max-w-4xl text-sm text-slate-300 md:text-base">
                  Track major indexes, scan best and worst performing themes, and review sector ETF relative rotation in one place.
                </p>
              </div>
            </div>

            <div className="rounded-3xl border border-white/10 bg-white/5 px-5 py-4 text-right shadow-[0_20px_70px_rgba(4,18,35,0.35)] backdrop-blur">
              <div className="text-xs uppercase tracking-[0.25em] text-slate-400">Local time</div>
              <div className="mt-2 text-lg font-semibold text-white md:text-xl">{formatClock(now)}</div>
              <div className="mt-2 text-sm text-slate-400">Tabs refresh independently so quotes stay current without freezing the screen.</div>
            </div>
          </div>

          <div className="mt-6 flex flex-wrap gap-2">
            {TABS.map((item) => (
              <button
                key={item}
                onClick={() => setTab(item)}
                className={`tab-pill ${tab === item ? 'active' : ''}`}
              >
                {item}
              </button>
            ))}
          </div>
        </header>

        <main>
          {tab === 'Overview' && (
            <MarketOverview
              onTickerClick={openChart}
              onOpenThemes={() => setTab('Themes')}
              onOpenRrg={() => setTab('ETF RRG')}
            />
          )}
          {tab === 'Themes' && <ThemeBoard onTickerClick={openChart} />}
          {tab === 'ETF RRG' && <ETFRRG onTickerClick={openChart} />}
          {tab === 'Pre-Market Screener' && <Screener onTickerClick={openChart} />}
          {tab === 'Chart' && <StockChart ticker={chartTicker} onTickerChange={setChartTicker} />}
        </main>
      </div>
    </div>
  )
}
