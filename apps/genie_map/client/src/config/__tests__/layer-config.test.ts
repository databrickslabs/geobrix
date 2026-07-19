import { describe, it, expect } from 'vitest';
import { createH3LayerConfig } from '../h3-layer-config';

describe('layer config factories', () => {
  it('H3 factory honors a custom color field and dataset id', () => {
    const cfg = createH3LayerConfig({
      datasetId: 'ch4_hotspots', hexField: 'hex', valueField: 'ch4_max',
      label: 'CH4 Hotspots', enable3d: true,
    }) as any;
    const layer = cfg.config.visState.layers[0];
    expect(layer.config.dataId).toBe('ch4_hotspots');
    expect(layer.visualChannels.colorField.name).toBe('ch4_max');
    expect(layer.config.columns.hex_id).toBe('hex');
  });

  it('H3 factory resolves a registered non-default palette to a distinct colorRange', () => {
    const dflt = createH3LayerConfig({ datasetId: 'a', palette: 'Global Warming' }) as any;
    const cfg = createH3LayerConfig({ datasetId: 'b', palette: 'Uber Viz Sequential' }) as any;
    const dfltColors = dflt.config.visState.layers[0].config.visConfig.colorRange.colors;
    const colors = cfg.config.visState.layers[0].config.visConfig.colorRange.colors;
    expect(colors).not.toEqual(dfltColors);
  });

  // NOTE: point layers are no longer built from a hand-rolled config — kepler
  // auto-creates them from the lat/lng fields (a hand-rolled point config never bound
  // coordinates and rendered nothing). Their label/size/color/visibility are applied
  // post-create by usePointLayerFinalize. Hence no point-factory test here.
});
