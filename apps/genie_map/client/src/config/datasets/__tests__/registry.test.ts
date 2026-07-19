import { describe, it, expect } from 'vitest';
import { vaporEyes } from '../vapor-eyes';
import { getActiveDataset, DATASETS } from '../index';

describe('vapor-eyes registry', () => {
  it('declares the four MVP layers', () => {
    const ids = vaporEyes.layers.map((l) => l.id);
    expect(ids).toEqual(['ch4_hotspots', 'well_density', 'wells', 'plumes']);
  });
  it('ch4_hotspots is a cell-sourced H3 layer capped at native res 6', () => {
    const l = vaporEyes.layers.find((x) => x.id === 'ch4_hotspots')!;
    expect(l.kind).toBe('h3');
    expect(l.queryName).toBe('hotspot_h3');
    expect(l.valueField).toBe('ch4_max');
    expect(l.h3!.source).toBe('cells');
    expect(l.h3!.nativeRes).toBe(6);
    expect(l.h3!.maxRes).toBe(6);          // never finer than S5P native footprint
    expect(l.h3!.zoomResBreaks).toHaveLength(4);
    expect(l.h3!.resByBreak).toHaveLength(5);
  });
  it('well_density is a point-sourced H3 layer that can refine past res 6', () => {
    const l = vaporEyes.layers.find((x) => x.id === 'well_density')!;
    expect(l.h3!.source).toBe('points');
    expect(l.h3!.lonCol).toBe('longitude');
    expect(l.h3!.maxRes).toBeGreaterThan(6);
    expect(l.queryName).toBe('wells_h3');
  });
  it('both H3 layers hand off to the wells point layer with a 1-level overlap', () => {
    const ch4 = vaporEyes.layers.find((x) => x.id === 'ch4_hotspots')!;
    const density = vaporEyes.layers.find((x) => x.id === 'well_density')!;
    const wells = vaporEyes.layers.find((x) => x.id === 'wells')!;
    // Wells appear at zoom 8; both H3 layers stay visible through 8 and hide at 9 — a
    // 1-level overlap (H3 max === wells min + 1) so the hex screen hands off to features.
    expect(ch4.zoomVisible.max).toBe(9);
    expect(density.zoomVisible.max).toBe(9);
    expect(ch4.zoomVisible.max).toBe(wells.zoomVisible.min + 1);
    expect(density.zoomVisible.max).toBe(wells.zoomVisible.min + 1);
  });
  it('plumes are always visible; wells are scale-gated', () => {
    const plumes = vaporEyes.layers.find((x) => x.id === 'plumes')!;
    const wells = vaporEyes.layers.find((x) => x.id === 'wells')!;
    expect(plumes.zoomVisible.min).toBe(0);   // plumes guide the eye at every zoom
    expect(plumes.zoomVisible.max).toBe(24);
    expect(wells.zoomVisible.min).toBe(8);    // wells gated to avoid basin-scale overload
  });
  it('getActiveDataset defaults to vapor-eyes', () => {
    expect(getActiveDataset().id).toBe('vapor-eyes');
    expect(DATASETS['vapor-eyes']).toBe(vaporEyes);
  });
});
