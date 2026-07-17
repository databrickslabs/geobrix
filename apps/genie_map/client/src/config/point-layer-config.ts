/**
 * Kepler.gl layer configuration for the individual point data layer.
 * Shown at zoom >= POINT_ZOOM_THRESHOLD; hidden otherwise.
 *
 * Column names match the canonical schema produced by notebooks/data_engineering.ipynb.
 */
import { DATASET_LABEL, POINT_RADIUS } from './dataset-config';

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

export const POINT_LAYER_CONFIG = {
  version: 'v1',
  config: {
    visState: {
      filters: [],
      layers: [
        {
          id: POINT_LAYER_ID,
          type: 'point',
          config: {
            dataId: POINT_DATASET_ID,
            label: `Point ${DATASET_LABEL}s Layer`,
            // Solid Global Warming orange — matches the H3 layer palette and
            // avoids invisible points that occur when coloring by a nullable field.
            color: [255, 195, 0] as unknown as [number, number, number],
            columns: {
              lat: 'latitude',
              lng: 'longitude',
              altitude: null,
            },
            isVisible: true,
            visConfig: {
              radius: POINT_RADIUS,  // driven by VITE_POINT_RADIUS
              fixedRadius: false,
              opacity: 0.8,
              outline: false,
              thickness: 2,
              strokeColor: null,
              colorRange: {
                name: 'Global Warming',
                type: 'sequential',
                category: 'Uber',
                colors: ['#5A1846', '#900C3F', '#C70039', '#E3611C', '#F1920E', '#FFC300'],
              },
              strokeColorRange: {
                name: 'Global Warming',
                type: 'sequential',
                category: 'Uber',
                colors: ['#5A1846', '#900C3F', '#C70039', '#E3611C', '#F1920E', '#FFC300'],
              },
              radiusRange: [0, 50],
              filled: true,
            },
            textLabel: [],
          },
          visualChannels: {
            // No colorField: all points use the solid colour above.
            colorField: null,
            colorScale: 'quantile',
            strokeColorField: null,
            strokeColorScale: 'quantile',
            sizeField: null,
            sizeScale: 'linear',
          },
        },
      ],
      interactionConfig: {
        tooltip: {
          fieldsToShow: {
            [POINT_DATASET_ID]: [
              'record_id',
              'group_filter',
              'category_filter',
              'metric_1',
              'metric_2',
            ],
          },
          enabled: true,
        },
        brush: { size: 0.5, enabled: false },
        geocoder: { enabled: false },
      },
      layerBlending: 'normal',
      splitMaps: [],
    },
  },
};
