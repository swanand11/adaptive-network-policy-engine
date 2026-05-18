/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        'dark': '#0f111a',
        'panel': 'rgba(255, 255, 255, 0.03)',
        'border': 'rgba(255, 255, 255, 0.08)',
        'accent-primary': '#00f0ff',
        'accent-secondary': '#7000ff',
        'accent-tertiary': '#ff007f',
        'text-muted': '#8b949e',
        'good': '#00e676',
        'warn': '#ffea00',
        'bad': '#ff3d00',
      },
      fontFamily: {
        'outfit': ['Outfit', 'sans-serif'],
        'inter': ['Inter', 'sans-serif'],
      },
      backdropBlur: {
        'glass': '12px',
      }
    },
  },
  plugins: [
    require('@tailwindcss/forms'),
  ],
}
