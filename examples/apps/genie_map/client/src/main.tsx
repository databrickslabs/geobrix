import React from 'react';
import ReactDOM from 'react-dom/client';
import { Provider } from 'react-redux';
import { store } from './store';
import App from './App';
import { ErrorBoundary } from './ErrorBoundary';
import { initApplicationConfig } from '@kepler.gl/utils';
import { DuckDBWasmAdapter } from '@kepler.gl/duckdb';
import { loader as monacoLoader } from '@monaco-editor/react';
import './index.css';
// Mapbox GL CSS - required for map to display correctly
import 'mapbox-gl/dist/mapbox-gl.css';
// MapLibre GL CSS - bundled locally (avoids CDN dependency at runtime)
import 'maplibre-gl/dist/maplibre-gl.css';

// Tell @monaco-editor/react to load Monaco from CDN instead of Vite's bundled
// worker files. Without this, Monaco workers fail in Vite's dev environment:
// "Uncaught TypeError: Cannot read properties of undefined (reading 'get')".
// CDN loading is self-contained — workers are blob-URLed from the CDN bundle.
monacoLoader.config({
  paths: { vs: 'https://cdn.jsdelivr.net/npm/monaco-editor@0.52.2/min/vs' }
});

// Initialize the DuckDB WASM adapter so SqlPanel can execute queries against
// locally-loaded kepler.gl datasets. Must run before ReactDOM.createRoot so
// getApplicationConfig().database is populated when SqlPanel first mounts.
initApplicationConfig({
  database: new DuckDBWasmAdapter({
    config: { query: { castBigIntToDouble: true } }
  })
});

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ErrorBoundary>
      <Provider store={store}>
        <App />
      </Provider>
    </ErrorBoundary>
  </React.StrictMode>
);
