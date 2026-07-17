import { useMemo } from 'react';
import { sql } from '@databricks/appkit-ui/js';
import { H3_DATASET_ID, H3_LAYER_CONFIG } from '../config/h3-layer-config';
import {
  POINT_ZOOM_THRESHOLD,
  DATASET_TABLE,
  H3_RESOLUTIONS,
  H3_ZOOM_BREAKS,
} from '../config/dataset-config';
import { useKeplerDataset } from './useKeplerDataset';
import type { AggregationOperation, H3DataRow, ViewportBounds } from '@shared/types';

// ---------------------------------------------------------------------------
// Module-level transform functions (stable references — must not be closures)
// ---------------------------------------------------------------------------

function transformH3Rows(raw: unknown[]): H3DataRow[] {
  return (raw as { hex: string | null; count: number | null }[])
    .filter(
      (row) =>
        row.hex != null && row.hex !== '' && row.count != null && !isNaN(Number(row.count)),
    )
    .map((row) => ({ hex: String(row.hex), count: Number(row.count) }));
}

function toH3KeplerRow(row: H3DataRow): unknown[] {
  return [row.hex, row.count];
}

const H3_FIELDS = [
  { name: 'hex', type: 'string' },
  { name: 'count', type: 'real' },
];

// ---------------------------------------------------------------------------

interface UseH3AggregationDataOptions {
  bounds: ViewportBounds | null;
  aggregation: AggregationOperation;
  categoryFilter?: string;
  groupFilter?: string;
}

interface UseH3AggregationDataResult {
  h3Data: H3DataRow[];
  isLoading: boolean;
  error: Error | null;
}

/**
 * Fetches H3 aggregation data for the current viewport and pushes it into kepler.gl.
 * Active when zoom < POINT_ZOOM_THRESHOLD.
 *
 * Delegates all query-execution and kepler dataset management to useKeplerDataset.
 * To add a different index type (S2, quadkey, etc.) create a parallel hook following
 * the same pattern: memo params with a zoom guard, call useKeplerDataset.
 */
export function useH3AggregationData({
  bounds,
  aggregation,
  categoryFilter = '',
  groupFilter = '',
}: UseH3AggregationDataOptions): UseH3AggregationDataResult {
  const params = useMemo(() => {
    if (!bounds) return null;
    if (!DATASET_TABLE) return null;
    if (bounds.zoom_level >= POINT_ZOOM_THRESHOLD) return null;

    return {
      x_min: sql.double(bounds.x_min),
      x_max: sql.double(bounds.x_max),
      y_min: sql.double(bounds.y_min),
      y_max: sql.double(bounds.y_max),
      zoom_level: sql.number(bounds.zoom_level),
      zoom_break_1: sql.number(H3_ZOOM_BREAKS[0]),
      zoom_break_2: sql.number(H3_ZOOM_BREAKS[1]),
      zoom_break_3: sql.number(H3_ZOOM_BREAKS[2]),
      zoom_break_4: sql.number(H3_ZOOM_BREAKS[3]),
      res_1: sql.number(H3_RESOLUTIONS[0]),
      res_2: sql.number(H3_RESOLUTIONS[1]),
      res_3: sql.number(H3_RESOLUTIONS[2]),
      res_4: sql.number(H3_RESOLUTIONS[3]),
      res_5: sql.number(H3_RESOLUTIONS[4]),
      agg_op: sql.string(aggregation),
      table_name: sql.string(DATASET_TABLE),
      category_filter: sql.string(categoryFilter),
      group_filter: sql.string(groupFilter.toUpperCase()),
    };
  }, [bounds, aggregation, categoryFilter, groupFilter]);

  const { data, isLoading, error } = useKeplerDataset<H3DataRow>({
    queryName: 'h3_aggregation',
    params,
    transformRows: transformH3Rows,
    toKeplerRow: toH3KeplerRow,
    fields: H3_FIELDS,
    datasetId: H3_DATASET_ID,
    datasetLabel: 'H3 Aggregation',
    layerConfig: H3_LAYER_CONFIG,
  });

  return { h3Data: data, isLoading, error };
}
