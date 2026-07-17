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
