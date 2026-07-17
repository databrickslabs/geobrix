/**
 * Kepler.gl layer configuration for an individual point data layer.
 *
 * Provides a factory (`createPointLayerConfig`) analogous to
 * `createH3LayerConfig`, so styling and field bindings are per-dataset.
 */

export const POINT_DATASET_ID = 'point_dataset';
export const POINT_LAYER_ID = 'point-layer';

export const POINT_FIELDS = [
  { name: 'longitude',       type: 'real'   },
  { name: 'latitude',        type: 'real'   },
  { name: 'record_id',       type: 'string' },
  { name: 'group_filter',    type: 'string' },
  { name: 'category_filter', type: 'string' },
  { name: 'metric_1',        type: 'real'   },
  { name: 'metric_2',        type: 'real'   },
];

// "Global Warming" color palette — warm gradient matching the H3 layer.
const GLOBAL_WARMING = {
  name: 'Global Warming',
  type: 'sequential',
  category: 'Uber',
  colors: ['#5A1846', '#900C3F', '#C70039', '#E3611C', '#F1920E', '#FFC300'],
};

export interface PointLayerConfigOptions {
  datasetId: string;
  label: string;
  latField: string;
  lngField: string;
  tooltipFields: string[];
  radius?: number;
  color?: [number, number, number];
}

export function createPointLayerConfig(options: PointLayerConfigOptions) {
  const { datasetId, label, latField, lngField, tooltipFields,
          radius = 20, color = [255, 195, 0] } = options;
  return {
    version: 'v1',
    config: { visState: { filters: [], layers: [{
      id: `point-layer-${datasetId}`, type: 'point',
      config: { dataId: datasetId, label, color,
        columns: { lat: latField, lng: lngField, altitude: null },
        isVisible: true,
        visConfig: { radius, fixedRadius: false, opacity: 0.8, outline: false,
          thickness: 2, strokeColor: null, colorRange: GLOBAL_WARMING,
          strokeColorRange: GLOBAL_WARMING, radiusRange: [0, 50], filled: true },
        textLabel: [] },
      visualChannels: { colorField: null, colorScale: 'quantile',
        strokeColorField: null, strokeColorScale: 'quantile',
        sizeField: null, sizeScale: 'linear' } }],
      interactionConfig: { tooltip: { fieldsToShow: { [datasetId]: tooltipFields },
        enabled: true }, brush: { size: 0.5, enabled: false },
        geocoder: { enabled: false } },
      layerBlending: 'normal', splitMaps: [] } },
  };
}
