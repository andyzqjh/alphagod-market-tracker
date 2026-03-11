import { useState, useEffect } from 'react'

const INTERVALS = [
  { label: '1m', value: '1' },
  { label: '5m', value: '5' },
  { label: '15m', value: '15' },
  { label: '1h', value: '60' },
  { label: '4h', value: '240' },
  { label: '1D', value: 'D' },
  { label: '1W', value: 'W' },
]

export default function StockChart({ ticker, onTickerChange }) {
  const [input, setInput] = useState(ticker)
  const [interval, setIv] = useState('D')

  useEffect(() => { setInput(ticker) }, [ticker])

  const src =
    'https://s.tradingview.com/widgetembed/?frameElementId=tv_chart' +
    '&symbol=' + encodeURIComponent(ticker) +
    '&interval=' + interval +
    '&theme=dark&style=1&timezone=America%2FNew_York' +
    '&withdateranges=1&hideideas=1&locale=en'

  return (
    <div>
      <div className='flex items-center gap-3 mb-4 flex-wrap'>
        <h2 className='text-white font-bold text-lg'>Chart</h2>
        <div className='flex gap-1'>
          {INTERVALS.map(iv => (
            <button key={iv.value} onClick={() => setIv(iv.value)}
              className={'px-3 py-1 rounded text-xs font-medium transition-colors ' +
                (interval === iv.value ? 'bg-blue-600 text-white' : 'bg-[#1e2530] text-gray-400 hover:text-white')}>
              {iv.label}
            </button>
          ))}
        </div>
        <div className='flex gap-2 ml-auto'>
          <input
            className='w-28 bg-[#1e2530] border border-[#2d3748] rounded px-3 py-1.5 text-white text-sm font-mono uppercase outline-none focus:border-blue-500'
            value={input}
            onChange={e => setInput(e.target.value.toUpperCase())}
            onKeyDown={e => e.key === 'Enter' && onTickerChange(input)}
            placeholder='Ticker'
          />
          <button onClick={() => onTickerChange(input)}
            className='px-4 py-1.5 bg-[#1e2530] text-white rounded text-sm hover:bg-[#2d3748]'>
            Go
          </button>
        </div>
      </div>
      <div className='bg-[#141820] border border-[#1e2530] rounded-lg overflow-hidden' style={{ height: 600 }}>
        <iframe src={src} style={{ width: '100%', height: '100%', border: 'none' }}
          allowFullScreen title='TradingView Chart' />
      </div>
      <p className='text-gray-600 text-xs mt-2'>Chart powered by TradingView</p>
    </div>
  )
}
