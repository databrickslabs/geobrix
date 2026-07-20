import { describe, it, expect } from 'vitest';
import { buildLayerParams, buildFields } from '../useLayerData';
import type { LayerDef } from '@shared/types';

const bounds = { x_min: -103, x_max: -102, y_min: 31, y_max: 32, zoom_level: 8 };
const h3Layer: LayerDef = { id: 'ch4_hotspots', kind: 'h3', label: 'x',
  queryName: 'hotspot_h3', hexField: 'hex', valueField: 'ch4_max',
  tooltipFields: ['hex', 'ch4_max', 'ch4_mean', 'n_obs'],
  h3: { source: 'cells', cellIdCol: 'h3_cellid', nativeRes: 6, minRes: 2, maxRes: 6,
        zoomResBreaks: [5, 7, 9, 11], resByBreak: [3, 4, 5, 6, 6],
        aggExpr: 'MAX(ch4_max)', targetCells: 300 },
  zoomVisible: { min: 0, max: 24 } };
// point-sourced H3 (like well_density): refine on zoom-in, so carries max_res not native_res.
const h3PointsLayer: LayerDef = { id: 'well_density', kind: 'h3', label: 'Well Density (H3)',
  queryName: 'wells_h3', hexField: 'hex', valueField: 'well_count',
  tooltipFields: ['hex', 'well_count', 'operator_count'],
  h3: { source: 'points', lonCol: 'longitude', latCol: 'latitude',
        minRes: 3, maxRes: 9, zoomResBreaks: [5, 7, 9, 11],
        resByBreak: [4, 5, 6, 7, 9], aggExpr: 'COUNT(*)', targetCells: 300 },
  zoomVisible: { min: 0, max: 12 } };
const pointLayer: LayerDef = { id: 'plumes', kind: 'point', label: 'x',
  queryName: 'plume_points', valueField: 'max_conc_ppmm', lngField: 'longitude',
  latField: 'latitude', tooltipFields: ['record_id'], zoomVisible: { min: 9, max: 24 } };

// sql.number/sql.double/sql.string wrap the underlying value as a string on `.value`.
const val = (p: any) => (p == null ? p : p.value);

describe('buildLayerParams', () => {
  it('returns null when bounds are null', () => {
    expect(buildLayerParams(h3Layer, null, 't')).toBeNull();
  });
  it('returns null when tableName is empty', () => {
    expect(buildLayerParams(h3Layer, bounds, '')).toBeNull();
  });
  it('emits the dynamic-H3 params for a cells-source H3 layer with correct values', () => {
    const p = buildLayerParams(h3Layer, bounds, 'db.sch.hotspot_latest') as any;
    expect(val(p.table_name)).toBe('db.sch.hotspot_latest');
    expect(val(p.zoom_level)).toBe(String(bounds.zoom_level));
    expect(val(p.zoom_break_1)).toBe(String(h3Layer.h3!.zoomResBreaks[0]));
    // res_1..res_5 map to resByBreak[0..4]
    expect(val(p.res_1)).toBe(String(h3Layer.h3!.resByBreak[0]));
    expect(val(p.res_5)).toBe(String(h3Layer.h3!.resByBreak[4]));
    expect(val(p.min_res)).toBe(String(h3Layer.h3!.minRes));
    expect(val(p.target_cells)).toBe(String(h3Layer.h3!.targetCells));
    // cells source carries native_res, never max_res
    expect(val(p.native_res)).toBe(String(h3Layer.h3!.nativeRes));
    expect(p.max_res).toBeUndefined();
  });
  it('emits max_res (not native_res) for a points-source H3 layer', () => {
    const p = buildLayerParams(h3PointsLayer, bounds, 'db.sch.wells_enriched_latest') as any;
    expect(val(p.max_res)).toBe(String(h3PointsLayer.h3!.maxRes));
    expect(p.native_res).toBeUndefined();
    expect(val(p.res_5)).toBe(String(h3PointsLayer.h3!.resByBreak[4]));
  });
  it('emits only bbox+table for a point layer', () => {
    const p = buildLayerParams(pointLayer, bounds, 'db.sch.plume_leaderboard_latest') as any;
    expect(p.x_min).toBeDefined();
    expect(p.table_name).toBeDefined();
    expect(p.zoom_break_1).toBeUndefined();
  });
});

describe('buildFields', () => {
  it('includes hex, valueField, and all tooltipFields for an H3 layer without duplicates', () => {
    const fields = buildFields(h3Layer);
    const names = fields.map((f) => f.name);
    // every declared tooltip column is registered
    h3Layer.tooltipFields.forEach((t) => expect(names).toContain(t));
    expect(names).toContain('hex');
    expect(names).toContain('ch4_max');
    // no duplicates even though hex/ch4_max also appear in tooltipFields
    expect(new Set(names).size).toBe(names.length);
    // hex stays first; hex typed as string, everything else real
    expect(names[0]).toBe('hex');
    expect(fields.find((f) => f.name === 'hex')!.type).toBe('string');
    expect(fields.find((f) => f.name === 'ch4_mean')!.type).toBe('real');
  });
  it('derives point-layer coord fields from lngField/latField', () => {
    const fields = buildFields(pointLayer);
    const names = fields.map((f) => f.name);
    expect(names[0]).toBe('longitude');
    expect(names[1]).toBe('latitude');
    expect(names).toContain('record_id');
    expect(new Set(names).size).toBe(names.length);
  });
});
