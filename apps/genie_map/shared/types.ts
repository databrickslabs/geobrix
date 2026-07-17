// Aggregation operation types
export type AggregationOperation = 'COUNT' | 'SUM' | 'AVG' | 'MAX' | 'MIN';

// Viewport bounds for spatial queries
export interface ViewportBounds {
  x_min: number;
  x_max: number;
  y_min: number;
  y_max: number;
  zoom_level: number;
}

// H3 hexagon data row (query result from h3_aggregation.sql)
export interface H3DataRow {
  hex: string;
  count: number;
}

// Generic point data row using canonical column names.
// The data-engineering notebook maps any source dataset to these names.
export interface PointDataRow {
  longitude: number;
  latitude: number;
  record_id: string;
  group_filter: string | null;
  category_filter: string | null;
  metric_1: number | null;
  metric_2: number | null;
}

export type LayerKind = 'h3' | 'point';
export type H3Source = 'cells' | 'points';

// Dynamic, density-aware H3 config (spec §4). Resolution is picked at query time
// from BOTH zoom (a max-res ceiling) AND density (target on-screen cell count).
export interface H3ResConfig {
  source: H3Source;         // 'cells' → coarsen-only via h3_toparent (from a stored cell col);
                            // 'points' → refine+coarsen via h3_longlatash3 (from lon/lat)
  cellIdCol?: string;       // source==='cells': the stored H3 cell column, e.g. 'h3_cellid'
  nativeRes?: number;       // source==='cells': stored resolution = hard finest ceiling
  lonCol?: string;          // source==='points'
  latCol?: string;          // source==='points'
  minRes: number;           // coarsest resolution ever rendered
  maxRes: number;           // finest resolution allowed (points may exceed a cell's nativeRes)
  zoomResBreaks: number[];  // exactly 4 ascending zoom thresholds
  resByBreak: number[];     // exactly 5 resolutions (per zoom band); each <= maxRes
  aggExpr: string;          // child aggregation, e.g. 'MAX(ch4_max)' or 'COUNT(*)'
  targetCells?: number;     // density target (default 300); coarsen when in-view count exceeds it
}

export interface LayerDef {
  id: string;                     // kepler dataId + layer key, e.g. 'ch4_hotspots'
  kind: LayerKind;
  label: string;
  queryName: string;              // key into config/queries (file name without .sql)
  hexField?: string;              // kind==='h3': the string hex column returned by SQL
  h3?: H3ResConfig;               // kind==='h3': dynamic-resolution config (required for h3)
  valueField: string;            // color/size metric column
  lngField?: string;              // kind==='point'
  latField?: string;             // kind==='point'
  tooltipFields: string[];
  palette?: string;               // kepler named colorRange, default 'Global Warming'
  enable3d?: boolean;
  zoomVisible: { min: number; max: number };   // hard visibility band: min <= z < max
  fadeBand?: [number, number];    // optional opacity ramp across [start,end] for smooth swap
}

export interface DatasetConfig {
  id: string;                     // matches VITE_ACTIVE_DATASET
  displayName: string;
  genieSpaceAlias: string;        // 'default' server-side; label only
  defaultViewport: { longitude: number; latitude: number; zoom: number };
  layers: LayerDef[];
}
