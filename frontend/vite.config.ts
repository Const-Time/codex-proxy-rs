import { fileURLToPath, URL } from 'node:url'
import tailwindcss from '@tailwindcss/vite'
import vue from '@vitejs/plugin-vue'
import { defineConfig } from 'vite'
import { pluginBridge } from './build/plugin-bridge.ts'

export default defineConfig({
  base: '/',
  plugins: [vue(), tailwindcss(), pluginBridge()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/dev': {
        target: 'http://127.0.0.1:8080',
        changeOrigin: true,
        rewrite: path => path.replace(/^\/dev/, ''),
      },
    },
  },
  build: {
    outDir: 'dist',
    assetsDir: 'assets',
    chunkSizeWarningLimit: 600,
    rolldownOptions: {
      checks: {
        invalidAnnotation: false,
      },
      output: {
        codeSplitting: {
          minSize: 16_384,
          groups: [
            {
              name: 'zrender',
              priority: 30,
              test: /[\\/]node_modules[\\/](?:.*[/\\])?zrender[\\/]/,
            },
            {
              name: 'echarts',
              priority: 20,
              test: /[\\/]node_modules[\\/](?:.*[/\\])?echarts[\\/]/,
            },
            {
              name: 'plugin-sandbox',
              priority: 20,
              test: /[\\/]node_modules[\\/](?:.*[/\\])?css-tree[\\/]/,
            },
            {
              name: 'icons',
              priority: 15,
              test: /[\\/]node_modules[\\/](?:.*[/\\])?@lucide[\\/]vue[\\/]/,
            },
            {
              name: 'vendor',
              priority: 10,
              test: /[\\/]node_modules[\\/](?:.*[/\\])?(?:vue|@vue|vue-router|pinia|pinia-plugin-persistedstate|@vueuse|axios)[\\/]/,
            },
            {
              name: 'base-ui',
              priority: 8,
              minShareCount: 2,
              test: /[\\/]src[\\/]components[\\/](?:base[\\/](?!BaseTable[\\/]BaseTableColumnSettingsPanel|BaseColorPicker)|icons[\\/])/,
            },
            {
              name: 'app-shared',
              priority: 5,
              minShareCount: 2,
              test: /[\\/]src[\\/](?:composables|utils|api)[\\/]/,
            },
          ],
        },
      },
    },
  },
})
