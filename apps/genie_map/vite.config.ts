import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

export default defineConfig(({ mode }) => {
  // Load ALL env vars from project root (not just VITE_* prefixed).
  // This lets us map server-side vars into client bundle constants.
  const env = loadEnv(mode, process.cwd(), '');

  return {
    plugins: [react()],
    root: 'client',
    // Vite's automatic VITE_* injection into import.meta.env uses `envDir`
    // (defaults to `root`). Since root='client', without this it would look for
    // .env in kepler-demo/client/ instead of kepler-demo/.
    envDir: __dirname,
    define: {
      // Shim process.env for browser
      'process.env': JSON.stringify({}),
      // Inject DATABRICKS_SERVING_ENDPOINT_NAME as a compile-time constant so
      // client code can use the same model name without a separate VITE_ var.
      '__LLM_MODEL__': JSON.stringify(env.DATABRICKS_SERVING_ENDPOINT_NAME ?? ''),
      // Human-readable Genie Space name, shown in the assistant panel so users
      // can see which space it is configured to query.
      '__GENIE_SPACE_NAME__': JSON.stringify(env.VITE_GENIE_SPACE_NAME ?? ''),
    },
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './client/src'),
        '@shared': path.resolve(__dirname, './shared'),
        // Dedupe packages that cause "multiple copies" warnings
        'styled-components': path.resolve(__dirname, 'node_modules/styled-components'),
        'react': path.resolve(__dirname, 'node_modules/react'),
        'react-dom': path.resolve(__dirname, 'node_modules/react-dom'),
        'redux': path.resolve(__dirname, 'node_modules/redux'),
        'react-redux': path.resolve(__dirname, 'node_modules/react-redux'),
        // Polyfill Node.js assert for kepler.gl
        'assert': path.resolve(__dirname, 'node_modules/assert'),
        'util': path.resolve(__dirname, 'node_modules/util'),
        'process': path.resolve(__dirname, 'node_modules/process/browser.js'),
        // Fix react-audio-voice-recorder exports issue
        'react-audio-voice-recorder': path.resolve(__dirname, 'node_modules/react-audio-voice-recorder/dist/react-audio-voice-recorder.es.js'),
        // Fix @openassistant/echarts CSS export issue
        '@openassistant/echarts/dist/index.esm.css': path.resolve(__dirname, 'node_modules/@openassistant/echarts/dist/index.esm.css'),
        '@openassistant/ui/dist/index.esm.css': path.resolve(__dirname, 'node_modules/@openassistant/ui/dist/index.esm.css'),
      },
      dedupe: [
        'styled-components',
        'react',
        'react-dom',
        'redux',
        'react-redux',
      ]
    },
    optimizeDeps: {
      include: [
        '@kepler.gl/components',
        '@kepler.gl/reducers',
        '@kepler.gl/actions',
        '@kepler.gl/ai-assistant',
        '@kepler.gl/styles',
        '@kepler.gl/processors',
        '@openassistant/ui',
        '@openassistant/core',
        'react-virtualized',
        'styled-components',
        'react-redux',
        '@emotion/is-prop-valid',
        'react-resizable-panels',
        'viewport-mercator-project',
        'assert',
        'util',
        'process',
      ],
      exclude: [],
      esbuildOptions: {
        // Node.js global to browser globalThis
        define: {
          global: 'globalThis',
          'process.env': '{}',
        }
      }
    },
    server: {
      port: 5173,
      proxy: {
        '/api': {
          // Proxy to the local AppKit server. Reads PORT from env so local dev can avoid
          // colliding with other :3000 servers (e.g. the docs dev server); defaults to 3000.
          target: `http://localhost:${env.PORT || '3000'}`,
          changeOrigin: true,
        },
      },
    },
    build: {
      outDir: '../dist/client',
      emptyOutDir: true,
      commonjsOptions: {
        transformMixedEsModules: true
      }
    },
  };
});
