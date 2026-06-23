import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    react(),
    tailwindcss(),
  ],
  server: {
    proxy: {
      '/cluster': 'http://localhost:8080',
      '/metrics': 'http://localhost:8080',
      '/node/': 'http://localhost:8080',
      '/command': 'http://localhost:8080',
      '/api': 'http://localhost:8080',
    }
  }
})
