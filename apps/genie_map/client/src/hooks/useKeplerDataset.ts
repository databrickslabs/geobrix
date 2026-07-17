import { useCallback, useEffect, useMemo, useRef } from 'react';
import { useDispatch } from 'react-redux';
import { addDataToMap, replaceDataInMap } from '@kepler.gl/actions';
import { useAnalyticsQuery } from '@databricks/appkit-ui/react';

/**
 * Configuration for a single kepler.gl dataset driven by an analytics query.
 *
 * @template TRow  The typed row shape returned by `transformRows`.
 */
export interface KeplerDatasetConfig<TRow> {
  /** Analytics query name — maps to config/queries/<queryName>.sql */
  queryName: string;

  /**
   * SQL-typed parameter object passed to the query.
   * Pass `null` to suppress the query for the current render cycle
   * (e.g. while zoom is in the wrong tier, or the table name is missing).
   */
  params: Record<string, unknown> | null;

  /**
   * Transform raw API rows to the typed shape used by the rest of the app.
   * Must be a **module-level constant** (not an inline closure) so its
   * reference is stable across renders and does not re-trigger the memo.
   */
  transformRows: (raw: unknown[]) => TRow[];

  /**
   * Convert one typed row into the ordered array kepler.gl expects.
   * Order must match the `fields` array.
   * Must be a **module-level constant** for the same reason as `transformRows`.
   */
  toKeplerRow: (row: TRow) => unknown[];

  /** Kepler.gl field schema — name + type for every column in `toKeplerRow`. */
  fields: { name: string; type: string }[];

  /** Dataset identifier used inside kepler.gl. Must be unique per layer. */
  datasetId: string;

  /** Human-readable label shown in the kepler.gl data panel. */
  datasetLabel: string;

  /**
   * Optional kepler.gl config applied on the *first* `addDataToMap` call only.
   * Subsequent updates use `replaceDataInMap` which preserves all layer settings.
   * If omitted, kepler will auto-create a layer using its own defaults.
   */
  layerConfig?: unknown;
}

export interface KeplerDatasetResult<TRow> {
  data: TRow[];
  isLoading: boolean;
  error: Error | null;
}

/**
 * Generic hook: run an analytics query → transform rows → push to kepler.gl.
 *
 * This is the single source of truth for the "query + kepler dataset" pattern.
 * All layer-specific hooks (`useViewportData`, `usePointData`, future A5 / polygon
 * hooks, etc.) are thin wrappers that provide their own `params` and transform
 * functions and delegate everything else here.
 *
 * Extension pattern:
 *   1. Add a SQL file in config/queries/<newQuery>.sql
 *   2. Create a ~35-line hook that memos its params and calls `useKeplerDataset`
 *   3. Add a `{ layerId, activeWhen }` rule to the `useLayerVisibility` call in App.tsx
 *
 * @template TRow  The typed row shape for this layer.
 */
export function useKeplerDataset<TRow>(
  config: KeplerDatasetConfig<TRow>,
): KeplerDatasetResult<TRow> {
  const {
    queryName,
    params,
    transformRows,
    toKeplerRow,
    fields,
    datasetId,
    datasetLabel,
    layerConfig,
  } = config;

  const dispatch = useDispatch();

  // Tracks whether we have already called addDataToMap for this dataset.
  // Once added we switch to replaceDataInMap so kepler preserves layer settings.
  const datasetAddedRef = useRef(false);

  const { data: rawData, loading, error } = useAnalyticsQuery(queryName, params);

  const data = useMemo((): TRow[] => {
    if (!rawData || !Array.isArray(rawData)) return [];
    return transformRows(rawData as unknown[]);
  }, [rawData, transformRows]);

  const updateKeplerData = useCallback(
    (rows: TRow[]) => {
      if (rows.length === 0) return;

      const dataset = {
        info: { id: datasetId, label: datasetLabel },
        data: {
          fields,
          rows: rows.map(toKeplerRow),
        },
      };

      if (!datasetAddedRef.current) {
        dispatch(
          addDataToMap({
            datasets: dataset,
            // keepExistingConfig: true is critical — without it addDataToMap calls
            // resetMapConfigUpdater which wipes all existing layers.
            options: { autoCreateLayers: false, centerMap: false, keepExistingConfig: true },
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            ...(layerConfig ? { config: layerConfig as any } : {}),
          }),
        );
        datasetAddedRef.current = true;
      } else {
        dispatch(
          replaceDataInMap({
            datasetToReplaceId: datasetId,
            datasetToUse: dataset,
            options: { centerMap: false },
          }),
        );
      }
    },
    [dispatch, datasetId, datasetLabel, fields, toKeplerRow, layerConfig],
  );

  useEffect(() => {
    if (data.length > 0) updateKeplerData(data);
  }, [data, updateKeplerData]);

  return {
    data,
    isLoading: loading,
    error: error ? new Error(error) : null,
  };
}
