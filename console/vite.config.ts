import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// GitHub Pages serves the site from /downrange/. Local builds use relative paths.
export default defineConfig({
  base: process.env.PAGES_BASE ?? './',
  plugins: [react()],
  server: { fs: { allow: ['..'] } },
  test: { environment: 'node' },
} as never)
