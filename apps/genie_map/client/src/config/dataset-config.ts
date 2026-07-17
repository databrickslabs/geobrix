/**
 * Dataset configuration — all values driven by VITE_ environment variables
 * so that switching datasets only requires a .env change + Vite restart.
 *
 * See .env.example and notebooks/data_engineering.ipynb for instructions on
 * how to plug in a different dataset (e.g. NYC taxi trips).
 */

/** Fully-qualified Databricks table name (sent as IDENTIFIER() SQL param).
 *  Falls back to the wells canonical demo table if VITE_DATASET_TABLE is not set. */
export const DATASET_TABLE: string =
  import.meta.env.VITE_DATASET_TABLE;

/** Zoom level at which the view switches from H3 hexagons to individual points. */
export const POINT_ZOOM_THRESHOLD: number =
  Number(import.meta.env.VITE_POINT_ZOOM_THRESHOLD) || 12;

/**
 * Five actual H3 resolution numbers to use, indexed low-detail → high-detail.
 * Wells (USA scale): [4, 5, 6, 7, 8]
 * Taxi (NYC scale):  [8, 9, 10, 11, 12]
 */
export const H3_RESOLUTIONS: [number, number, number, number, number] = (() => {
  const raw = import.meta.env.VITE_H3_RESOLUTIONS ?? '4,5,6,7,8';
  const parts = raw.split(',').map(Number);
  if (parts.length !== 5) {
    console.warn('[dataset-config] VITE_H3_RESOLUTIONS must have exactly 5 values; using defaults');
    return [4, 5, 6, 7, 8];
  }
  return parts as [number, number, number, number, number];
})();

/**
 * Four map-zoom breakpoints separating the 5 H3 resolution tiers.
 * Wells: [5, 7, 9, 11]  → res[0] shown at zoom≤5, res[1] at zoom≤7, …, res[4] otherwise
 * Taxi:  [9, 10, 11, 12]
 */
export const H3_ZOOM_BREAKS: [number, number, number, number] = (() => {
  const raw = import.meta.env.VITE_H3_ZOOM_BREAKS ?? '5,7,9,11';
  const parts = raw.split(',').map(Number);
  if (parts.length !== 4) {
    console.warn('[dataset-config] VITE_H3_ZOOM_BREAKS must have exactly 4 values; using defaults');
    return [5, 7, 9, 11];
  }
  return parts as [number, number, number, number];
})();

/** Singular display name for one record (e.g. "Well", "Trip"). */
export const DATASET_LABEL = import.meta.env.VITE_DATASET_LABEL ?? 'Record';

/** Plural lowercase name derived from DATASET_LABEL (e.g. "Trip" → "trips"). */
export const RECORD_LABEL = `${DATASET_LABEL.toLowerCase()}s`;

/** Label for the SUM / MAX / MIN aggregation metric (METRIC_1 column). */
export const METRIC_1_LABEL = import.meta.env.VITE_METRIC_1_LABEL ?? 'Metric 1';

/** Label for the AVG aggregation metric (METRIC_2 column). */
export const METRIC_2_LABEL = import.meta.env.VITE_METRIC_2_LABEL ?? 'Metric 2';

/** Label for the categorical dropdown filter. Empty string hides the filter. */
export const CATEGORY_FILTER_LABEL = import.meta.env.VITE_CATEGORY_FILTER_LABEL ?? '';

/**
 * Dropdown options for the categorical filter.
 * Parsed from the comma-separated VITE_CATEGORY_FILTER_OPTIONS env var.
 * The first option is always an "All …" catch-all.
 */
export const CATEGORY_FILTER_OPTIONS: Array<{ value: string; label: string }> = (() => {
  const raw = import.meta.env.VITE_CATEGORY_FILTER_OPTIONS ?? '';
  // 'All' catch-all always comes first (value='' means no filter applied in SQL)
  const all = { value: '', label: 'All' };
  if (!raw.trim()) return [all];
  const opts = raw
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((v) => ({ value: v, label: v }));
  return [all, ...opts];
})();

/** Label for the text-input group/operator filter. Empty string hides the filter. */
export const GROUP_FILTER_LABEL = import.meta.env.VITE_GROUP_FILTER_LABEL ?? '';

/** H3 hexagon height multiplier (kepler.gl elevationScale). Well=130, Taxi=3. */
export const H3_HEIGHT_MULTIPLIER: number =
  Number(import.meta.env.VITE_H3_HEIGHT_MULTIPLIER) || 130;

/** Point layer radius in pixels. Well=20, Taxi=5.5. */
export const POINT_RADIUS: number =
  Number(import.meta.env.VITE_POINT_RADIUS) || 20;
