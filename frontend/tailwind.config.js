/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        bg: '#0b0e11',
        card: '#141820',
        border: '#1e2530',
        accent: '#00c805',
        red: '#ff3b3b',
        muted: '#6b7280',
      }
    },
  },
  plugins: [],
}
