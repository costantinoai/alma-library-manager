/// <reference types="vitest/config" />
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
  optimizeDeps: {
    // Pre-bundle the graph renderer at dev-server start. It is reached ONLY via
    // lazy import() (the author neighbourhood dialog + the Insights graph), so
    // without this Vite discovers it at runtime on first open, re-optimizes,
    // and forces a full-page reload — which reads as the view "failing".
    include: ['react-force-graph-2d'],
  },
  test: {
    // jsdom so React Testing Library can mount components; globals so tests
    // read like the backend pytest suite (describe/it/expect without imports).
    environment: 'jsdom',
    globals: true,
    setupFiles: './src/test/setup.ts',
    css: false,
    include: ['src/**/*.{test,spec}.{ts,tsx}'],
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
