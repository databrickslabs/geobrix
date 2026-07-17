import { useMemo } from 'react';
import { sql } from '@databricks/appkit-ui/js';
import { POINT_DATASET_ID, POINT_FIELDS, POINT_LAYER_CONFIG } from '../config/point-layer-config';
import { POINT_ZOOM_THRESHOLD, DATASET_TABLE } from '../config/dataset-config';
import { useKeplerDataset } from './useKeplerDataset';
import type { ViewportBounds, PointDataRow } from '@shared/types';

// ---------------------------------------------------------------------------
// Module-level transform functions (stable references — must not be closures)
// ---------------------------------------------------------------------------

function transformPointRows(raw: unknown[]): PointDataRow[] {
  return (raw as Record<string, unknown>[])
    .filter((row) => isFinite(Number(row.longitude)) && isFinite(Number(row.latitude)))
    .map((row) => ({
      longitude: Number(row.longitude),
      latitude: Number(row.latitude),
      record_id: String(row.record_id ?? ''),
      group_filter: row.group_filter != null ? String(row.group_filter) : null,
      category_filter: row.category_filter != null ? String(row.category_filter) : null,
      metric_1: row.metric_1 != null ? Number(row.metric_1) : null,
      metric_2: row.metric_2 != null ? Number(row.metric_2) : null,
    }));
}

function toPointKeplerRow(row: PointDataRow): unknown[] {
  return [
    row.longitude,
    row.latitude,
    row.record_id,
    row.group_filter,
    row.category_filter,
    row.metric_1,
    row.metric_2,
  ];
}

// ---------------------------------------------------------------------------

interface UsePointDataOptions {
  bounds: ViewportBounds | null;
  categoryFilter?: string;
  groupFilter?: string;
}

/**
 * Fetches individual point data for high-zoom viewports and pushes it into kepler.gl.
 * Active when zoom >= POINT_ZOOM_THRESHOLD.
 *
 * Delegates all query-execution and kepler dataset management to useKeplerDataset.
 * To add a polygon geometry layer at high zoom, create a parallel hook with a
 * different queryName and a polygon layer config.
 */
export function usePointData({
  bounds,
  categoryFilter = '',
  groupFilter = '',
}: UsePointDataOptions): { isLoading: boolean; pointData: PointDataRow[] } {
  const params = useMemo(() => {
    if (!bounds) return null;
    if (!DATASET_TABLE) return null;
    if (bounds.zoom_level < POINT_ZOOM_THRESHOLD) return null;

    return {
      x_min: sql.double(bounds.x_min),
      x_max: sql.double(bounds.x_max),
      y_min: sql.double(bounds.y_min),
      y_max: sql.double(bounds.y_max),
      table_name: sql.string(DATASET_TABLE),
      category_filter: sql.string(categoryFilter),
      group_filter: sql.string(groupFilter.toUpperCase()),
    };
  }, [bounds, categoryFilter, groupFilter]);

  const { data, isLoading } = useKeplerDataset<PointDataRow>({
    queryName: 'point_data',
    params,
    transformRows: transformPointRows,
    toKeplerRow: toPointKeplerRow,
    fields: POINT_FIELDS,
    datasetId: POINT_DATASET_ID,
    datasetLabel: 'Point Data',
    layerConfig: POINT_LAYER_CONFIG,
  });

  return { isLoading, pointData: data };
}
