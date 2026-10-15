import type { DatasetConfig } from '@shared/types';

// Fully-qualified gold table name. Catalog + schema are build-time configurable
// (VITE_GOLD_CATALOG / VITE_GOLD_SCHEMA) so the same app can point at a different
// workspace's vapor-eyes gold; defaults match the reference deployment.
const CATALOG = import.meta.env.VITE_GOLD_CATALOG ?? 'geospatial_docs';
const SCHEMA = import.meta.env.VITE_GOLD_SCHEMA ?? 'vapor_eyes_lf';
const T = (name: string) => `${CATALOG}.${SCHEMA}.${name}`;

export const vaporEyes: DatasetConfig = {
  id: 'vapor-eyes',
  displayName: 'Vapor-Eyes — Permian Basin Methane',
  genieSpaceAlias: 'default',
  // Delaware Basin (full AOI center, from SDP _config bbox -104.5,30.8,-101.0,33.0)
  defaultViewport: { longitude: -102.75, latitude: 31.9, zoom: 8 },
  layers: [
    // CH4 hexes: cell-sourced, coarsen-ONLY (never finer than S5P native res 6).
    // Wide-area context; hides at zoom 9 — a 1-level overlap with the wells point layer
    // (which appears at 8) so the H3 screen hands off cleanly to individual features.
    { id: 'ch4_hotspots', kind: 'h3', label: 'CH₄ Hotspots (latest)',
      queryName: 'hotspot_h3', hexField: 'hex', valueField: 'ch4_max',
      tooltipFields: ['hex', 'ch4_max', 'ch4_mean', 'n_obs'],
      palette: 'Global Warming', enable3d: true,
      h3: { source: 'cells', cellIdCol: 'h3_cellid', nativeRes: 6,
            minRes: 2, maxRes: 6, zoomResBreaks: [5, 7, 9, 11],
            resByBreak: [3, 4, 5, 6, 6], aggExpr: 'MAX(ch4_max)', targetCells: 300 },
      zoomVisible: { min: 0, max: 9 } },
    // Well density: point-sourced, refine (finer on zoom-in) AND coarsen (only if dense).
    // Owns low/mid zoom; hides at zoom 9 (same as CH4 hexes) — 1-level overlap with the
    // wells point layer appearing at 8.
    { id: 'well_density', kind: 'h3', label: 'Well Density (H3)',
      queryName: 'wells_h3', hexField: 'hex', valueField: 'well_count',
      tooltipFields: ['hex', 'well_count', 'operator_count'],
      palette: 'Uber Viz Sequential', enable3d: true,
      h3: { source: 'points', lonCol: 'longitude', latCol: 'latitude',
            minRes: 3, maxRes: 9, zoomResBreaks: [5, 7, 9, 11],
            resByBreak: [4, 5, 6, 7, 9], aggExpr: 'COUNT(*)', targetCells: 300 },
      zoomVisible: { min: 0, max: 9 } },
    // Wells points: shown from zoom 8 up. Deliberately gated (not shown at the widest
    // zooms) so the map isn't overwhelmed by well density at basin scale — a demo talking
    // point about configuring layer visibility to the right scale.
    { id: 'wells', kind: 'point', label: 'Wells',
      queryName: 'wells_points', valueField: 'well_count',
      lngField: 'longitude', latField: 'latitude',
      tooltipFields: ['record_id', 'operator', 'field', 'county', 'play_name'],
      zoomVisible: { min: 8, max: 24 } },
    // EMIT plumes: always shown — few in number and they visually guide the eye to the
    // methane sources across every zoom level.
    { id: 'plumes', kind: 'point', label: 'EMIT Plumes',
      queryName: 'plume_points', valueField: 'max_conc_ppmm',
      lngField: 'longitude', latField: 'latitude',
      tooltipFields: ['record_id', 'max_conc_ppmm', 'lead_operator', 'lead_county'],
      zoomVisible: { min: 0, max: 24 } },
  ],
};

export const VAPOR_EYES_TABLES = {
  hotspot: T('hotspot_latest'),
  wellsEnriched: T('wells_enriched_latest'),  // feeds BOTH well_density (H3) and wells (points)
  plumes: T('plume_leaderboard_latest'),
};
