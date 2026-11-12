import { useCallback, useMemo } from 'react';
import { sql } from '@databricks/appkit-ui/js';
import { useKeplerDataset } from './useKeplerDataset';
import { createH3LayerConfig } from '../config/h3-layer-config';
import { toKeplerValue } from './arrowRows';
import type { LayerDef, ViewportBounds } from '@shared/types';

export type LayerQueryFormat = 'JSON_ARRAY' | 'ARROW_STREAM';

/**
 * Result format for a layer's analytics query.
 *
 * AppKit delivers a JSON_ARRAY result as ONE server-sent event and rejects events over
 * 1 MiB, so a row-level point query at full density (12k+ wells in a zoom-8 view) cannot
 * ride it. ARROW_STREAM comes back as raw Arrow IPC in the HTTP response body, with no
 * SSE event and no 1 MiB cap. H3 layers are density-bounded (target_cells) and stay on
 * JSON_ARRAY, which also carries warehouse-startup progress.
 */
export function queryFormatFor(layer: LayerDef): LayerQueryFormat {
  return layer.kind === 'point' ? 'ARROW_STREAM' : 'JSON_ARRAY';
}

// Module-level identity transform — stable reference so it never re-triggers
// the kepler-update memo in useKeplerDataset (see its transformRows contract).
const identityRows = (raw: unknown[]) => raw as Record<string, unknown>[];

export function buildLayerParams(
  layer: LayerDef, bounds: ViewportBounds | null, tableName: string,
): Record<string, unknown> | null {
  if (!bounds || !tableName) return null;
  const base: Record<string, unknown> = {
    // NOTE: appkit's sql helper has no `double` — sql.number binds JS numbers
    // as DOUBLE. Using a non-existent sql.double left these params unbound.
    x_min: sql.number(bounds.x_min), x_max: sql.number(bounds.x_max),
    y_min: sql.number(bounds.y_min), y_max: sql.number(bounds.y_max),
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

  const fields = useMemo(() => buildFields(layer), [layer]);

  // H3 layers supply a hand-built config (works). POINT layers pass NO config so kepler
  // auto-creates the layer from the lat/lng fields — a hand-rolled point config produced a
  // layer that never rendered. undefined config → useKeplerDataset uses autoCreateLayers.
  const layerConfig = useMemo(() => (
    layer.kind === 'h3'
      ? createH3LayerConfig({ datasetId: layer.id, hexField: layer.hexField ?? 'hex',
          valueField: layer.valueField, label: layer.label, enable3d: layer.enable3d ?? true,
          palette: layer.palette, tooltipFields: layer.tooltipFields })
      : undefined
  ), [layer]);

  // Stable reference keyed on `fields` — identity only changes when the field
  // list does, so the kepler-update effect does not re-fire every render.
  // JSON_ARRAY cells arrive as STRINGS (e.g. "-103.74"); kepler/deck.gl need real numbers
  // to compute point positions, so a string coordinate yields NaN → the layer renders
  // nothing. toKeplerValue coerces real/integer fields to numbers, and renders typed
  // ARROW_STREAM numbers in string (tooltip) fields as strings.
  const toKeplerRow = useCallback(
    (row: Record<string, unknown>) => fields.map((f) => toKeplerValue(f.type, row[f.name])),
    [fields],
  );

  return useKeplerDataset<Record<string, unknown>>({
    queryName: layer.queryName, params, format: queryFormatFor(layer),
    transformRows: identityRows,
    toKeplerRow,
    fields, datasetId: layer.id, datasetLabel: layer.label, layerConfig,
  });
}
