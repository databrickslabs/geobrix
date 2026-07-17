import { describe, it, expect } from 'vitest';
import { buildLayerParams } from '../useLayerData';
import type { LayerDef } from '@shared/types';

const bounds = { x_min: -103, x_max: -102, y_min: 31, y_max: 32, zoom_level: 8 };
const h3Layer: LayerDef = { id: 'ch4_hotspots', kind: 'h3', label: 'x',
  queryName: 'hotspot_h3', hexField: 'hex', valueField: 'ch4_max', tooltipFields: [],
  h3: { source: 'cells', cellIdCol: 'h3_cellid', nativeRes: 6, minRes: 2, maxRes: 6,
        zoomResBreaks: [5, 7, 9, 11], resByBreak: [3, 4, 5, 6, 6],
        aggExpr: 'MAX(ch4_max)', targetCells: 300 },
  zoomVisible: { min: 0, max: 24 } };
const pointLayer: LayerDef = { id: 'plumes', kind: 'point', label: 'x',
  queryName: 'plume_points', valueField: 'max_conc_ppmm', lngField: 'longitude',
  latField: 'latitude', tooltipFields: ['record_id'], zoomVisible: { min: 9, max: 24 } };

describe('buildLayerParams', () => {
  it('returns null when bounds are null', () => {
    expect(buildLayerParams(h3Layer, null, 't')).toBeNull();
  });
  it('emits the dynamic-H3 params for an H3 layer', () => {
    const p = buildLayerParams(h3Layer, bounds, 'db.sch.hotspot_latest') as any;
    expect(p.x_min).toBeDefined();
    expect(p.table_name).toBeDefined();
    expect(p.zoom_level).toBeDefined();
    expect(p.zoom_break_1).toBeDefined();
    expect(p.res_1).toBeDefined();
    expect(p.res_5).toBeDefined();
    expect(p.native_res).toBeDefined();   // cells source carries native_res
    expect(p.target_cells).toBeDefined();
  });
  it('emits only bbox+table for a point layer', () => {
    const p = buildLayerParams(pointLayer, bounds, 'db.sch.plume_leaderboard_latest') as any;
    expect(p.x_min).toBeDefined();
    expect(p.table_name).toBeDefined();
    expect(p.zoom_break_1).toBeUndefined();
  });
});
