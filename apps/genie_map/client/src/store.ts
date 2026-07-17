import { configureStore, combineReducers } from '@reduxjs/toolkit';
import keplerGlReducer, { uiStateUpdaters, enhanceReduxMiddleware } from '@kepler.gl/reducers';
import { aiAssistantReducer } from '@kepler.gl/ai-assistant';
import type { KeplerGlState } from '@kepler.gl/reducers';
import type { AiAssistantState } from '@kepler.gl/ai-assistant';
import { getActiveDataset } from './config/datasets';

const { DEFAULT_MAP_CONTROLS } = uiStateUpdaters;

// Initial map viewport comes from the active dataset registry (Task 5).
const { longitude, latitude, zoom } = getActiveDataset().defaultViewport;

// Default map controls with AI Assistant and SQL Panel
const CUSTOM_MAP_CONTROLS = {
  ...DEFAULT_MAP_CONTROLS,
  aiAssistant: {
    active: false,
    activeMapIndex: 0,
    disableClose: false,
    show: true
  },
  sqlPanel: {
    active: false,
    activeMapIndex: 0,
    disableClose: false,
    show: true
  }
};

// Enhance the kepler.gl reducer with initial state
const customizedKeplerGlReducer = keplerGlReducer.initialState({
  uiState: {
    currentModal: null,
    activeSidePanel: 'layer',
    readOnly: false,
    mapControls: CUSTOM_MAP_CONTROLS,
  },
  mapStyle: {
    // Use 'dark-matter' (free CARTO style) instead of 'dark' (Uber-specific Mapbox style)
    styleType: 'dark-matter',
  },
  mapState: {
    latitude,
    longitude,
    zoom,
  },
});

// App-specific reducer for local state
const appReducer = (state = { loaded: true }, _action: unknown) => state;

// Combine into "demo" namespace to match kepler.gl ai-assistant expectations
const demoReducer = combineReducers({
  keplerGl: customizedKeplerGlReducer,
  app: appReducer,
  aiAssistant: aiAssistantReducer,
});

// Root reducer with demo wrapper
const rootReducer = combineReducers({
  demo: demoReducer,
});

// Kepler.gl task middleware (for async operations like loading map styles)
const keplerGlMiddleware = enhanceReduxMiddleware([]);

export const store = configureStore({
  reducer: rootReducer,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  middleware: (getDefaultMiddleware) => [
    ...getDefaultMiddleware({
      serializableCheck: false,
      immutableCheck: false,
    }),
    ...keplerGlMiddleware,
  ] as any,
});

export type RootState = {
  demo: {
    keplerGl: {
      map?: KeplerGlState;
    };
    app: { loaded: boolean };
    aiAssistant: AiAssistantState;
  };
};

export type AppDispatch = typeof store.dispatch;

// Kepler.gl state getter for component injection - points to demo.keplerGl
export const keplerGlGetState = (state: RootState) => state.demo.keplerGl;
