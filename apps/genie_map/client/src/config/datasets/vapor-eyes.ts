import type { DatasetConfig } from '@shared/types';

const T = (name: string) => `geospatial_docs.vapor_eyes_lf.${name}`;

export const vaporEyes: DatasetConfig = {
  id: 'vapor-eyes',
  displayName: 'Vapor-Eyes — Permian Basin Methane',
  genieSpaceAlias: 'default',
  // Delaware Basin (full AOI center, from SDP _config bbox -104.5,30.8,-101.0,33.0)
  defaultViewport: { longitude: -102.75, latitude: 31.9, zoom: 8 },
  layers: [
    // CH4 hexes: cell-sourced, coarsen-ONLY (never finer than S5P native res 6).
    // Always-on wide-area context; density heuristic keeps it readable at low zoom.
    { id: 'ch4_hotspots', kind: 'h3', label: 'CH₄ Hotspots (latest)',
      queryName: 'hotspot_h3', hexField: 'hex', valueField: 'ch4_max',
      tooltipFields: ['hex', 'ch4_max', 'ch4_mean', 'n_obs'],
      palette: 'Global Warming', enable3d: true,
      h3: { source: 'cells', cellIdCol: 'h3_cellid', nativeRes: 6,
            minRes: 2, maxRes: 6, zoomResBreaks: [5, 7, 9, 11],
            resByBreak: [3, 4, 5, 6, 6], aggExpr: 'MAX(ch4_max)', targetCells: 300 },
      zoomVisible: { min: 0, max: 24 } },
    // Well density: point-sourced, refine (finer on zoom-in) AND coarsen (only if dense).
    // Owns low/mid zoom; fades out over [11,12] as the wells point layer fades in.
    { id: 'well_density', kind: 'h3', label: 'Well Density (H3)',
      queryName: 'wells_h3', hexField: 'hex', valueField: 'well_count',
      tooltipFields: ['hex', 'well_count', 'operator_count'],
      palette: 'Uber Viz Sequential', enable3d: true,
      h3: { source: 'points', lonCol: 'longitude', latCol: 'latitude',
            minRes: 3, maxRes: 9, zoomResBreaks: [5, 7, 9, 11],
            resByBreak: [4, 5, 6, 7, 9], aggExpr: 'COUNT(*)', targetCells: 300 },
      zoomVisible: { min: 0, max: 12 }, fadeBand: [11, 12] },
    // Wells points: fade in over [11,12] — ~1-level overlap with well_density.
    { id: 'wells', kind: 'point', label: 'Wells',
      queryName: 'wells_points', valueField: 'well_count',
      lngField: 'longitude', latField: 'latitude',
      tooltipFields: ['record_id', 'operator', 'field', 'county', 'play_name'],
      zoomVisible: { min: 11, max: 24 }, fadeBand: [11, 12] },
    // EMIT plumes: coexist with the CH4 hex screen; appear once zoomed in to resolve sources.
    { id: 'plumes', kind: 'point', label: 'EMIT Plumes',
      queryName: 'plume_points', valueField: 'max_conc_ppmm',
      lngField: 'longitude', latField: 'latitude',
      tooltipFields: ['record_id', 'max_conc_ppmm', 'lead_operator', 'lead_county'],
      zoomVisible: { min: 9, max: 24 } },
  ],
};

export const VAPOR_EYES_TABLES = {
  hotspot: T('hotspot_latest'),
  wellsEnriched: T('wells_enriched_latest'),  // feeds BOTH well_density (H3) and wells (points)
  plumes: T('plume_leaderboard_latest'),
};
