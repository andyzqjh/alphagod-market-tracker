import { useState, useEffect } from 'react'
import axios from 'axios'

const CAT_CLASS = {
  Earnings: 'cat-earnings',
  'New Contracts Partnerships': 'cat-contracts',
  FDA: 'cat-fda',
  'Themes Narratives': 'cat-themes',
  Others: 'cat-others',
}

function fmt(n) {
  if (n == null) return 'n/a'
  if (n >= 1e9) return (n/1e9).toFixed(1)+'B'
  if (n >= 1e6) return (n/1e6).toFixed(1)+'M'
  if (n >= 1e3) return (n/1e3).toFixed(0)+'K'
  return n
}

function AnalysisPanel({ a }) {
  if (!a) return <p className='text-gray-500 text-sm p-4'>No analysis yet</p>
  const items = [
    ['Impact', a.impact],
    ['Explosiveness', a.explosiveness],
    ['Statistical Edge', a.statistical_edge],
    ['Risk Factors', a.risk_factors],
  ]
  return (
    <div className='p-4 space-y-3 text-sm text-gray-300'>
      {items.map(([label, text]) => text ? (
        <div key={label}>
          <div className='text-gray-500 text-xs uppercase tracking-widest mb-1'>{label}</div>
          <div>{text}</div>
        </div>
      ) : null)}
    </div>
  )
}

function Row({ s, onChart }) {
  const [open, setOpen] = useState(false)
  const [analysis, setAnalysis] = useState(null)
  const [busy, setBusy] = useState(false)
  const [news, setNews] = useState('')
  const [showNews, setShowNews] = useState(false)

  async function analyze(newsText) {
    setBusy(true)
    try {
      const params = newsText ? { news: newsText } : {}
      const r = await axios.get('/api/analyze/' + s.ticker, { params })
      setAnalysis(r.data.analysis)
    } catch(e) { console.error(e) }
    finally { setBusy(false) }
  }

  function toggle() {
    if (!open && !analysis) analyze()
    setOpen(!open)
  }

  const pct = s.premarket_pct
  const day = s.daily_pct
  const gradeClass = analysis ? 'grade-' + analysis.grade?.toLowerCase() : ''
  const catClass = analysis ? (CAT_CLASS[analysis.category] || 'cat-others') : ''

  return (
    <>
      <tr className='border-b border-[#1e2530] hover:bg-[#141820] cursor-pointer' onClick={toggle}>
        <td className='px-3 py-2.5'>
          <div className='flex gap-2 items-center'>
            <span className='text-blue-400 font-mono font-bold text-sm'>{s.ticker}</span>
            {analysis && <span className={'text-xs px-1.5 py-0.5 rounded font-bold ' + gradeClass}>{analysis.grade}</span>}
          </div>
        </td>
        <td className='px-3 py-2.5'>
          <span className={'font-bold text-sm ' + (pct >= 0 ? 'pos' : 'neg')}>
            {pct >= 0 ? '+' : ''}{pct?.toFixed(2)}%
          </span>
        </td>
        <td className='px-3 py-2.5 text-gray-400 text-sm'>{fmt(s.volume)}</td>
        <td className='px-3 py-2.5 text-gray-400 text-sm'>{s.rvol}x</td>
        <td className='px-3 py-2.5'>
          {day != null && (
            <span className={'text-sm ' + (day >= 0 ? 'pos' : 'neg')}>
              {day >= 0 ? '+' : ''}{day?.toFixed(2)}%
            </span>
          )}
        </td>
        <td className='px-3 py-2.5 text-gray-400 text-sm'>
          {s.curr_price ? '$' + s.curr_price : '\u2014'}
        </td>
        <td className='px-3 py-2.5'>
          {analysis && <span className={'text-xs px-2 py-0.5 rounded-full ' + catClass}>{analysis.category}</span>}
        </td>
        <td className='px-3 py-2.5 text-gray-400 text-xs max-w-xs truncate'>
          {analysis?.brief_reasoning || (busy ? 'Analyzing...' : '\u2014 click to analyze')}
        </td>
        <td className='px-3 py-2.5'>
          <button onClick={e => { e.stopPropagation(); onChart(s.ticker) }}
            className='text-xs text-gray-500 hover:text-white border border-[#1e2530] rounded px-2 py-0.5'>
            Chart
          </button>
        </td>
      </tr>
      {open && (
        <tr className='bg-[#0f1319] border-b border-[#1e2530]'>
          <td colSpan={9} className='px-4 py-2'>
            {busy
              ? <p className='text-gray-500 text-sm p-4'>Analyzing with Claude AI...</p>
              : <AnalysisPanel a={analysis} />
            }
            <div className='px-4 pb-3'>
              {!showNews
                ? <button onClick={() => setShowNews(true)}
                    className='text-xs text-gray-500 hover:text-white underline'>
                    + Add news catalyst for better analysis
                  </button>
                : (
                  <div className='flex gap-2'>
                    <input
                      className='flex-1 bg-[#1e2530] border border-[#2d3748] rounded px-3 py-1.5 text-sm text-white outline-none'
                      placeholder='Paste news headline...'
                      value={news}
                      onChange={e => setNews(e.target.value)}
                      onClick={e => e.stopPropagation()}
                    />
                    <button onClick={e => { e.stopPropagation(); analyze(news); setShowNews(false) }}
                      className='px-3 py-1.5 bg-blue-600 text-white rounded text-sm'>
                      Re-analyze
                    </button>
                  </div>
                )
              }
            </div>
          </td>
        </tr>
      )}
    </>
  )
}

export default function Screener({ onTickerClick }) {
  const [stocks, setStocks] = useState([])
  const [loading, setLoading] = useState(false)
  const [minPct, setMinPct] = useState(3)
  const [updated, setUpdated] = useState(null)
  const [manual, setManual] = useState('')
  const [manualRes, setManualRes] = useState(null)
  const [manualBusy, setManualBusy] = useState(false)

  async function scan() {
    setLoading(true)
    try {
      const r = await axios.get('/api/screener?min_pct=' + minPct + '&limit=30')
      setStocks(r.data)
      setUpdated(new Date().toLocaleTimeString())
    } catch(e) { console.error(e) }
    finally { setLoading(false) }
  }

  async function analyzeManual() {
    if (!manual) return
    setManualBusy(true)
    try {
      const r = await axios.get('/api/analyze/' + manual.toUpperCase())
      setManualRes(r.data)
    } catch(e) { console.error(e) }
    finally { setManualBusy(false) }
  }

  useEffect(() => { scan() }, [])

  return (
    <div>
      <div className='flex flex-wrap items-center gap-4 mb-4'>
        <div>
          <h2 className='text-white font-bold text-lg'>Pre-Market Screener</h2>
          {updated && <span className='text-gray-500 text-xs'>Updated {updated}</span>}
        </div>
        <div className='flex items-center gap-2 ml-auto'>
          <span className='text-gray-500 text-sm'>Min gap %</span>
          <input type='number' value={minPct}
            onChange={e => setMinPct(Number(e.target.value))}
            className='w-16 bg-[#1e2530] border border-[#2d3748] rounded px-2 py-1 text-white text-sm outline-none' />
          <button onClick={scan} disabled={loading}
            className='px-4 py-1.5 bg-[#1e2530] text-white rounded text-sm hover:bg-[#2d3748] disabled:opacity-50'>
            {loading ? 'Scanning...' : 'Scan'}
          </button>
        </div>
      </div>

      <div className='bg-[#141820] border border-[#1e2530] rounded-lg p-4 mb-4'>
        <div className='text-gray-400 text-sm mb-2'>Analyze any ticker manually</div>
        <div className='flex gap-2'>
          <input
            className='w-32 bg-[#0b0e11] border border-[#2d3748] rounded px-3 py-1.5 text-white text-sm font-mono uppercase outline-none focus:border-blue-500'
            placeholder='AAPL' value={manual}
            onChange={e => setManual(e.target.value.toUpperCase())}
            onKeyDown={e => e.key === 'Enter' && analyzeManual()} />
          <button onClick={analyzeManual} disabled={manualBusy || !manual}
            className='px-4 py-1.5 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 disabled:opacity-50'>
            {manualBusy ? 'Analyzing...' : 'Analyze'}
          </button>
          {manualRes && (
            <button onClick={() => onTickerClick(manualRes.ticker)}
              className='px-4 py-1.5 bg-[#1e2530] text-white rounded text-sm hover:bg-[#2d3748]'>
              View Chart
            </button>
          )}
        </div>
        {manualRes?.analysis && (
          <div className='mt-3 grid grid-cols-2 gap-4'>
            <div>
              <div className='flex items-center gap-2 mb-2'>
                <span className='text-white font-bold'>{manualRes.ticker}</span>
                <span className={'text-sm px-2 py-0.5 rounded grade-' + manualRes.analysis.grade?.toLowerCase()}>
                  Grade {manualRes.analysis.grade}
                </span>
                <span className={'text-xs px-2 py-0.5 rounded-full ' + (CAT_CLASS[manualRes.analysis.category] || 'cat-others')}>
                  {manualRes.analysis.category}
                </span>
              </div>
              <div className='text-gray-400 text-sm'>{manualRes.analysis.brief_reasoning}</div>
              {manualRes.detail && (
                <div className='flex gap-4 mt-2 text-xs text-gray-500'>
                  {manualRes.detail.premarket_pct != null && (
                    <span>Pre-mkt: <span className={manualRes.detail.premarket_pct >= 0 ? 'pos' : 'neg'}>
                      {manualRes.detail.premarket_pct >= 0 ? '+' : ''}{manualRes.detail.premarket_pct}%
                    </span></span>
                  )}
                  {manualRes.detail.rvol && <span>RVol: {manualRes.detail.rvol}x</span>}
                  {manualRes.detail.industry && <span>{manualRes.detail.industry}</span>}
                </div>
              )}
            </div>
            <AnalysisPanel a={manualRes.analysis} />
          </div>
        )}
      </div>

      {loading && stocks.length === 0 ? (
        <div className='text-center text-gray-500 py-20'>
          <p className='text-lg mb-2'>Scanning stocks for pre-market gappers...</p>
          <p className='text-xs'>This takes ~60 seconds on first load</p>
        </div>
      ) : stocks.length === 0 ? (
        <div className='text-center text-gray-500 py-20'>
          <p>No stocks gapping over {minPct}% pre-market right now</p>
          <p className='text-xs mt-1'>Try lowering the min % or check market hours (4am–9:30am ET)</p>
        </div>
      ) : (
        <div className='bg-[#141820] border border-[#1e2530] rounded-lg overflow-hidden'>
          <table className='w-full'>
            <thead>
              <tr className='border-b border-[#1e2530]'>
                {['Ticker','Premkt %','Volume','RVol','Daily %','Price','Category','Reasoning',''].map(h => (
                  <th key={h} className='px-3 py-2.5 text-left text-xs text-gray-500 font-medium'>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {stocks.map(s => <Row key={s.ticker} s={s} onChart={onTickerClick} />)}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
