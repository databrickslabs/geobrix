/// <reference types="vite/client" />

// Compile-time constant injected by vite.config.ts from DATABRICKS_SERVING_ENDPOINT_NAME.
declare const __LLM_MODEL__: string;

// Compile-time constant injected by vite.config.ts from VITE_GENIE_SPACE_NAME —
// the human-readable Genie Space name shown in the assistant panel.
declare const __GENIE_SPACE_NAME__: string;

// Dataset configuration env vars (VITE_* are baked into the client bundle at build time).
interface ImportMetaEnv {
  readonly VITE_MAPBOX_TOKEN: string;
  readonly VITE_DATASET_TABLE: string;
  readonly VITE_POINT_ZOOM_THRESHOLD: string;
  readonly VITE_H3_RESOLUTIONS: string;
  readonly VITE_H3_ZOOM_BREAKS: string;
  readonly VITE_DATASET_LABEL: string;
  readonly VITE_METRIC_1_LABEL: string;
  readonly VITE_METRIC_2_LABEL: string;
  readonly VITE_CATEGORY_FILTER_LABEL: string;
  readonly VITE_CATEGORY_FILTER_OPTIONS: string;
  readonly VITE_GROUP_FILTER_LABEL: string;
  readonly VITE_H3_HEIGHT_MULTIPLIER: string;
  readonly VITE_POINT_RADIUS: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
