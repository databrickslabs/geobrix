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
  it('wells H3 and wells points share the wells_enriched source + overlap-swap', () => {
    const density = vaporEyes.layers.find((x) => x.id === 'well_density')!;
    const wells = vaporEyes.layers.find((x) => x.id === 'wells')!;
    // ~1-level overlap band: density fades out where wells fades in.
    expect(density.zoomVisible.max).toBeGreaterThan(wells.zoomVisible.min);
    expect(density.fadeBand).toBeDefined();
  });
  it('ch4 hexes and plumes coexist (plumes appear on zoom-in, hexes stay)', () => {
    const ch4 = vaporEyes.layers.find((x) => x.id === 'ch4_hotspots')!;
    const plumes = vaporEyes.layers.find((x) => x.id === 'plumes')!;
    expect(ch4.zoomVisible.max).toBe(24);   // ch4 stays visible at all zooms
    expect(plumes.zoomVisible.min).toBeGreaterThan(0); // plumes only on zoom-in
  });
  it('getActiveDataset defaults to vapor-eyes', () => {
    expect(getActiveDataset().id).toBe('vapor-eyes');
    expect(DATASETS['vapor-eyes']).toBe(vaporEyes);
  });
});
