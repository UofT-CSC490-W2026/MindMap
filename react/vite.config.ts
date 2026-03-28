import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = fileURLToPath(new URL('.', import.meta.url))

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      react: path.resolve(here, 'node_modules/react'),
      'react/jsx-runtime': path.resolve(here, 'node_modules/react/jsx-runtime.js'),
      'react/jsx-dev-runtime': path.resolve(here, 'node_modules/react/jsx-dev-runtime.js'),
      '@testing-library/react': path.resolve(here, 'node_modules/@testing-library/react/dist/index.js'),
    },
  },
  server: {
    fs: {
      allow: ['..'],
    },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./vitest.setup.ts'],
    include: ['../tests/frontend/**/*.test.{ts,tsx}'],
    deps: {
      moduleDirectories: [
        path.resolve(here, 'node_modules'),
        'node_modules',
      ],
    },
    coverage: {
      provider: 'v8',
      reporter: ['text', 'html', 'json-summary'],
      include: [
        'src/components/**/*.tsx',
        'src/hooks/**/*.ts',
        'src/services/**/*.ts',
        'src/utils/**/*.ts',
      ],
      exclude: [
        '**/*.test.*',
        'src/App.tsx',
        'src/main.tsx',
      ],
    },
  },
})
