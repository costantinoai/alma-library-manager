/// <reference types="vitest/config" />
import { defineConfig, type Plugin } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'path'
import { vendorChunk } from './src/build/chunkGroups'

const MAX_CHUNK_BYTES = 500 * 1024

function enforceChunkBudget(maxBytes: number): Plugin {
  return {
    name: 'enforce-chunk-budget',
    generateBundle(_options, bundle) {
      const oversized = Object.values(bundle).flatMap((entry) => {
        if (entry.type !== 'chunk') return []
        const bytes = Buffer.byteLength(entry.code)
        return bytes > maxBytes ? [`${entry.fileName} (${(bytes / 1024).toFixed(1)} kB)`] : []
      })

      if (oversized.length > 0) {
        this.error(`JavaScript chunk budget exceeded (${maxBytes / 1024} kB): ${oversized.join(', ')}`)
      }
    },
  }
}

export default defineConfig({
  plugins: [react(), tailwindcss(), enforceChunkBudget(MAX_CHUNK_BYTES)],
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
        manualChunks: vendorChunk,
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
