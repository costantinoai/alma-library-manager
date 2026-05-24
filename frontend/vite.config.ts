import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'path'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  build: {
    rollupOptions: {
      output: {
        manualChunks: {
          'vendor-react': ['react', 'react-dom'],
          'vendor-query': ['@tanstack/react-query'],
          'vendor-graph': ['react-force-graph-2d'],
        },
      },
    },
  },
  server: {
    // FRONTEND_PORT / BACKEND_PORT let `scripts/start-dev.sh` run a dev
    // stack on non-default ports (e.g. alongside a prod container already
    // holding 8000). The proxy must target the *dev* backend, not a
    // hardcoded :8000 which may be the production container.
    port: Number(process.env.FRONTEND_PORT) || 5173,
    proxy: {
      '/api': `http://localhost:${Number(process.env.BACKEND_PORT) || 8000}`,
    },
  },
})
