import { useCallback, useMemo } from 'react';
import { sql } from '@databricks/appkit-ui/js';
import { useKeplerDataset } from './useKeplerDataset';
import { createH3LayerConfig } from '../config/h3-layer-config';
import { createPointLayerConfig } from '../config/point-layer-config';
import type { LayerDef, ViewportBounds } from '@shared/types';

// Module-level identity transform — stable reference so it never re-triggers
// the kepler-update memo in useKeplerDataset (see its transformRows contract).
const identityRows = (raw: unknown[]) => raw as Record<string, unknown>[];

export function buildLayerParams(
  layer: LayerDef, bounds: ViewportBounds | null, tableName: string,
): Record<string, unknown> | null {
  if (!bounds || !tableName) return null;
  const base: Record<string, unknown> = {
    x_min: sql.double(bounds.x_min), x_max: sql.double(bounds.x_max),
    y_min: sql.double(bounds.y_min), y_max: sql.double(bounds.y_max),
    table_name: sql.string(tableName),
  };
  if (layer.kind !== 'h3' || !layer.h3) return base;

  const h = layer.h3;
  const p: Record<string, unknown> = {
    ...base,
    zoom_level: sql.number(bounds.zoom_level),
    zoom_break_1: sql.number(h.zoomResBreaks[0]), zoom_break_2: sql.number(h.zoomResBreaks[1]),
    zoom_break_3: sql.number(h.zoomResBreaks[2]), zoom_break_4: sql.number(h.zoomResBreaks[3]),
    res_1: sql.number(h.resByBreak[0]), res_2: sql.number(h.resByBreak[1]),
    res_3: sql.number(h.resByBreak[2]), res_4: sql.number(h.resByBreak[3]),
    res_5: sql.number(h.resByBreak[4]),
    min_res: sql.number(h.minRes),
    target_cells: sql.number(h.targetCells ?? 300),
  };
  if (h.source === 'cells') p.native_res = sql.number(h.nativeRes ?? h.maxRes);
  else p.max_res = sql.number(h.maxRes);   // points source: finer allowed than any stored cell
  return p;
}

/**
 * Derive the kepler dataset field list (name + type) for a layer.
 *
 * The returned list is the single source of truth for BOTH the column set
 * fetched into the dataset AND the row order (`toKeplerRow` maps over it), so
 * hex/coords are kept first for readability. Names are de-duped: for h3, the
 * hex column and valueField may also appear in `tooltipFields`, and every
 * tooltip column must be registered or its tooltip renders empty.
 */
export function buildFields(layer: LayerDef): { name: string; type: string }[] {
  const out: { name: string; type: string }[] = [];
  const seen = new Set<string>();
  const push = (name: string, type: string) => {
    if (seen.has(name)) return;
    seen.add(name);
    out.push({ name, type });
  };
  if (layer.kind === 'h3') {
    push(layer.hexField ?? 'hex', 'string');
    push(layer.valueField, 'real');
    // Remaining tooltip columns are metrics (ch4_mean, n_obs, operator_count, …).
    layer.tooltipFields.forEach((f) => push(f, 'real'));
  } else {
    push(layer.lngField!, 'real');
    push(layer.latField!, 'real');
    layer.tooltipFields.forEach((f) => push(f, 'string'));
  }
  return out;
}

export function useLayerData(layer: LayerDef, bounds: ViewportBounds | null, tableName: string) {
  const params = useMemo(() => buildLayerParams(layer, bounds, tableName), [layer, bounds, tableName]);

  const layerConfig = useMemo(() => (
    layer.kind === 'h3'
      ? createH3LayerConfig({ datasetId: layer.id, hexField: layer.hexField ?? 'hex',
          valueField: layer.valueField, label: layer.label, enable3d: layer.enable3d ?? true,
          palette: layer.palette, tooltipFields: layer.tooltipFields })
      : createPointLayerConfig({ datasetId: layer.id, label: layer.label,
          latField: layer.latField!, lngField: layer.lngField!, tooltipFields: layer.tooltipFields })
  ), [layer]);

  const fields = useMemo(() => buildFields(layer), [layer]);

  // Stable reference keyed on `fields` — identity only changes when the field
  // list does, so the kepler-update effect does not re-fire every render.
  const toKeplerRow = useCallback(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (row: Record<string, unknown>) => fields.map((f) => (row as any)[f.name]),
    [fields],
  );

  return useKeplerDataset<Record<string, unknown>>({
    queryName: layer.queryName, params,
    transformRows: identityRows,
    toKeplerRow,
    fields, datasetId: layer.id, datasetLabel: layer.label, layerConfig,
  });
}
