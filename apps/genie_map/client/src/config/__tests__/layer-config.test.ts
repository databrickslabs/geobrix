import { describe, it, expect } from 'vitest';
import { createH3LayerConfig } from '../h3-layer-config';
import { createPointLayerConfig } from '../point-layer-config';

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

  it('point factory returns a point layer bound to the given dataset id + coords', () => {
    const cfg = createPointLayerConfig({
      datasetId: 'wells', label: 'Wells',
      latField: 'latitude', lngField: 'longitude',
      tooltipFields: ['record_id', 'operator'],
    }) as any;
    const layer = cfg.config.visState.layers[0];
    expect(layer.type).toBe('point');
    expect(layer.config.dataId).toBe('wells');
    expect(layer.config.columns.lat).toBe('latitude');
    expect(layer.config.columns.lng).toBe('longitude');
  });
});
