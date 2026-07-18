/**
 * Kepler.gl AI Assistant Tool Utilities
 *
 * Extracted from kepler.gl/src/ai-assistant/src/tools/utils.ts
 * Provides context functions for accessing kepler.gl datasets and layers.
 */

import { Feature } from 'geojson';
import { Layer, VectorTileLayer } from '@kepler.gl/layers';
import { Datasets, KeplerTable } from '@kepler.gl/table';
import { SpatialJoinGeometries } from '@openassistant/geoda';
import { ALL_FIELD_TYPES, LAYER_TYPES } from '@kepler.gl/constants';
import { Field, ProtoDataset, ProtoDatasetField } from '@kepler.gl/types';
import { processFileData } from '@kepler.gl/processors';
import { createOrUpdateFilter, removeFilter } from '@kepler.gl/actions';

/**
 * Interpolate colors from the original colors with the given number of colors
 */
export function interpolateColor(originalColors: string[], numberOfColors: number) {
  if (originalColors.length === numberOfColors) {
    return originalColors;
  }
  // Simple linear interpolation
  const colors: string[] = [];
  for (let i = 0; i < numberOfColors; i++) {
    const t = i / (numberOfColors - 1);
    const index = t * (originalColors.length - 1);
    const lower = Math.floor(index);
    const upper = Math.ceil(index);
    const frac = index - lower;

    if (lower === upper || frac === 0) {
      colors.push(originalColors[lower]);
    } else {
      // Parse hex colors and interpolate
      const c1 = originalColors[lower];
      const c2 = originalColors[upper];
      const r1 = parseInt(c1.slice(1, 3), 16);
      const g1 = parseInt(c1.slice(3, 5), 16);
      const b1 = parseInt(c1.slice(5, 7), 16);
      const r2 = parseInt(c2.slice(1, 3), 16);
      const g2 = parseInt(c2.slice(3, 5), 16);
      const b2 = parseInt(c2.slice(5, 7), 16);
      const r = Math.round(r1 + frac * (r2 - r1));
      const g = Math.round(g1 + frac * (g2 - g1));
      const b = Math.round(b1 + frac * (b2 - b1));
      colors.push(`#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`);
    }
  }
  return colors;
}

/**
 * Get values from a dataset for a variable
 */
export function getValuesFromDataset(
  datasets: Datasets,
  layers: Layer[],
  datasetName: string,
  variableName: string
): unknown[] {
  const datasetId = Object.keys(datasets).find(dataId => datasets[dataId].label === datasetName);
  if (!datasetId) {
    throw new Error(`Dataset ${datasetName} not found`);
  }
  const dataset = datasets[datasetId];
  if (dataset) {
    const field = dataset.fields.find(f => f.name === variableName);
    if (!field) {
      throw new Error(`Field ${variableName} not found in dataset ${datasetName}`);
    }
    // for vector-tile, getting values from layerData
    if (dataset.type === 'vector-tile') {
      const vectorField = dataset.fields.find(f => f.name === variableName);
      if (vectorField) {
        return getValuesFromVectorTileLayer(datasetId, layers, vectorField);
      }
    }
    return Array.from({ length: dataset.length }, (_, i) => dataset.getValue(variableName, i));
  }
  return [];
}

function isVectorTileLayer(layer: Layer): layer is VectorTileLayer {
  return layer.type === LAYER_TYPES.vectorTile;
}

export function getValuesFromVectorTileLayer(datasetId: string, layers: Layer[], field: Field) {
  const layerIndex = layers.findIndex(layer => layer.config.dataId === datasetId);
  if (layerIndex === -1) return [];
  const layer = layers[layerIndex];
  if (!isVectorTileLayer(layer)) return [];
  const accessor = layer.accessRowValue(field);
  const values: unknown[] = [];
  // @ts-expect-error TODO fix this later in the vector-tile layer
  for (const row of layer.tileDataset.tileSet) {
    const value = accessor(field, row);
    if (value === null) break;
    values.push(value);
  }
  return values;
}

// Preference order for the NUMERIC measure to range-filter on when a chart selection
// comes in. A histogram/box-plot brush is a numeric range, and kepler applies a range
// filter on a numeric column reliably (a multi-select on a high-cardinality id column does
// not move the map). The first of these that exists in the selected dataset wins — these
// are the measures the app's charts are built on (see the layer configs' valueField).
const CHART_MEASURE_COLUMNS = [
  'max_conc_ppmm', 'ch4_max', 'ch4_mean', 'well_count', 'operator_count',
  'plume_count', 'n_obs', 'well_density', 'dist_m',
];

// Deterministic per-dataset filter id so repeated selections UPDATE the same cross-filter
// (via createOrUpdateFilter) rather than stacking new filters.
const crossFilterId = (datasetId: string) => `ai-crossfilter-${datasetId}`;

function isNumeric(v: unknown): v is number {
  return typeof v === 'number' && Number.isFinite(v);
}

// Pick the numeric column to range-filter: first known measure present, else the first
// field whose values are numeric.
function findMeasureColumn(dataset: KeplerTable): string | null {
  const names = new Set(dataset.fields.map(f => f.name));
  const known = CHART_MEASURE_COLUMNS.find(c => names.has(c));
  if (known) return known;
  for (const f of dataset.fields) {
    if (dataset.length > 0 && isNumeric(dataset.getValue(f.name, 0))) return f.name;
  }
  return null;
}

/**
 * Cross-filter a map layer from a chart selection, PERSISTENTLY.
 *
 * A chart's brush selection arrives as row indices. We compute the min/max of a numeric
 * measure column over the selected rows and push a RANGE filter into kepler's Redux state
 * via `createOrUpdateFilter`. This is the critical difference from a direct
 * `dataset.filteredIndex = ...` mutation: kepler recomputes `filteredIndex` from
 * `visState.filters` on every render (KeplerTable.filterTable), so a manual mutation is
 * reverted on the next pointer event, whereas a dispatched range filter persists AND
 * actually moves the map (a range on a numeric field is what kepler applies — a
 * multi-select on a high-cardinality id column does not).
 *
 * An empty selection removes the cross-filter, returning the map to all rows.
 */
export function highlightRows(
  datasets: Datasets,
  layers: Layer[],
  datasetName: string,
  selectedRowIndices: number[],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  dispatch: (action: any) => void,
  // Current visState.filters — needed to resolve a filter's index for removeFilter (which
  // is index-based, not id-based). Defaults to empty (clear becomes a no-op if unknown).
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  filters: Array<{ id?: string }> = [],
) {
  const datasetId = Object.keys(datasets).find(dataId => datasets[dataId].label === datasetName);
  if (!datasetId) return;
  const dataset = datasets[datasetId];
  if (!dataset) return;

  const filterId = crossFilterId(dataset.id);

  // Empty selection → clear the cross-filter (show everything again). NOTE: kepler's
  // removeFilter takes a numeric INDEX into visState.filters, not a filter id — passing an
  // id crashes removeFilterUpdater ("Cannot read properties of undefined (reading
  // 'dataId')"). Resolve the index from the filters list we were handed; if the filter
  // isn't present there's nothing to remove.
  if (selectedRowIndices.length === 0) {
    const idx = filters.findIndex(f => f?.id === filterId);
    if (idx >= 0) dispatch(removeFilter(idx));
    return;
  }

  const measure = findMeasureColumn(dataset);
  if (!measure) {
    // No numeric column to range-filter — fall back to a best-effort direct mutation so
    // charts on measure-less datasets still respond (non-persistent, but better than nothing).
    dataset.filteredIndex = selectedRowIndices;
    layers
      .filter(layer => layer.config.dataId === dataset.id)
      .forEach(layer => layer.formatLayerData(datasets));
    return;
  }

  // Range of the measure over the selected rows.
  const values = selectedRowIndices
    .map(i => dataset.getValue(measure, i))
    .filter(isNumeric);
  if (values.length === 0) {
    dispatch(removeFilter(filterId));
    return;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);

  // createOrUpdateFilter(id, dataId, field, value) — a [min,max] range filter on the
  // measure; reusing filterId means repeated brushes update this one filter in place.
  dispatch(createOrUpdateFilter(filterId, dataset.id, measure, [min, max]));
}

/**
 * Get the dataset context for AI assistant prompts
 */
export function getDatasetContext(datasets?: Datasets, layers?: Layer[]) {
  if (!datasets || !layers) return '';
  const context =
    'Please remember the following datasets and layers for answering the user question:';
  const dataMeta = Object.values(datasets).map((dataset: KeplerTable) => ({
    datasetName: dataset.label,
    datasetId: dataset.id,
    fields: dataset.fields.map(field => ({ [field.name]: field.type })),
    layers: layers
      .filter(layer => layer.config.dataId === dataset.id)
      .map(layer => ({
        id: layer.id,
        label: layer.config.label,
        type: layer.type,
        geometryMode: layer.config.columnMode,
        geometryColumns: Object.fromEntries(
          Object.entries(layer.config.columns)
            .filter(([, value]) => value !== null)
            .map(([key, value]) => [
              key,
              typeof value === 'object' && value !== null
                ? Object.fromEntries(Object.entries(value).filter(([, v]) => v !== null))
                : value
            ])
        )
      }))
  }));
  return `${context}\n${JSON.stringify(dataMeta)}`;
}

/**
 * Get geometries from a dataset
 */
export function getGeometriesFromDataset(
  datasets: Datasets,
  layers: Layer[],
  layerData: unknown[],
  datasetName: string
): SpatialJoinGeometries {
  const datasetId = Object.keys(datasets).find(dataId => datasets[dataId].label === datasetName);
  if (!datasetId) {
    return [];
  }
  const dataset = datasets[datasetId];

  // if layer is vector-tile, get the geometries from the layer
  if (dataset.type === 'vector-tile') {
    const selected = layers.filter(layer => layer.config.dataId === dataset.id);
    const layer = selected.find(layer => layer.type === LAYER_TYPES.vectorTile);
    if (!layer) return [];

    const geometries: Feature[] = [];
    // @ts-expect-error TODO fix this later in the vector-tile layer
    for (const row of layer.tileDataset.tileSet) {
      geometries.push(row);
    }
    return geometries;
  }

  // for non-vector-tile dataset, get the geometries from the possible layer
  const selectedLayers = layers.filter(layer => layer.config.dataId === dataset.id);
  if (selectedLayers.length === 0) return [];

  // find geojson layer, then point layer, then other layers
  const geojsonLayer = selectedLayers.find(layer => layer.type === LAYER_TYPES.geojson);
  const pointLayer = selectedLayers.find(layer => layer.type === LAYER_TYPES.point);
  const otherLayers = selectedLayers.filter(
    layer => layer.type !== LAYER_TYPES.geojson && layer.type !== LAYER_TYPES.point
  );

  const validLayer = geojsonLayer || pointLayer || otherLayers[0];
  if (validLayer) {
    const layerIndex = layers.findIndex(layer => layer.id === validLayer.id);
    const geometries = layerData[layerIndex] as { data: Feature[] };
    return geometries?.data || [];
  }

  return [];
}

/**
 * Save data as a new dataset by joining it with the left dataset
 */
export function saveAsDataset(
  datasets: Datasets,
  layers: Layer[],
  datasetName: string,
  newDatasetName: string,
  data: Record<string, unknown[]>
) {
  const datasetId = Object.keys(datasets).find(dataId => datasets[dataId].label === datasetName);
  if (!datasetId) return;

  if (Object.keys(datasets).includes(newDatasetName)) return;

  const leftDataset = datasets[datasetId];
  let numRows = leftDataset.length;
  let geometries: Feature[] = [];

  if (leftDataset.type === 'vector-tile') {
    geometries = getFeaturesFromVectorTile(leftDataset, layers) || [];
    numRows = geometries.length;
  }

  const fields: ProtoDatasetField[] = [
    ...Object.keys(data).map((fieldName, index) => ({
      name: fieldName,
      id: `${fieldName}_${index}`,
      displayName: fieldName,
      type: determineFieldType(data[fieldName][0])
    })),
    ...leftDataset.fields.map((field, index) => ({
      name: field.name,
      id: field.id || `${field.name}_${index}`,
      displayName: field.displayName,
      type: field.type
    })),
    ...(leftDataset.type === 'vector-tile'
      ? [{ name: '_geojson', id: '_geojson', displayName: '_geojson', type: 'geojson' }]
      : [])
  ];

  const dataValues = Object.values(data);

  const rows = Array(numRows)
    .fill(null)
    .map((_, rowIdx) => [
      ...dataValues.map(col => col[rowIdx]),
      ...leftDataset.fields.map(field =>
        leftDataset.type === 'vector-tile'
          ? geometries[rowIdx]?.properties?.[field.name]
          : leftDataset.getValue(field.name, rowIdx)
      ),
      ...(leftDataset.type === 'vector-tile' ? [geometries[rowIdx]] : [])
    ]);

  const newDataset: ProtoDataset = {
    info: { id: newDatasetName, label: newDatasetName },
    data: { fields, rows }
  };

  return newDataset;
}

function determineFieldType(value: unknown): keyof typeof ALL_FIELD_TYPES {
  return typeof value === 'number'
    ? Number.isInteger(value)
      ? ALL_FIELD_TYPES.integer
      : ALL_FIELD_TYPES.real
    : ALL_FIELD_TYPES.string;
}

function getFeaturesFromVectorTile(leftDataset: KeplerTable, layers: Layer[]) {
  const layerIndex = layers.findIndex(layer => layer.config.dataId === leftDataset.id);
  if (layerIndex === -1) return;

  const layer = layers[layerIndex];
  if (!isVectorTileLayer(layer)) return;

  const features: Feature[] = [];
  // @ts-expect-error TODO fix this later in the vector-tile layer
  for (const row of layer.tileDataset.tileSet) {
    features.push(row);
  }
  return features;
}

export function highlightRowsByColumnValues(
  datasets: Datasets,
  layers: Layer[],
  datasetName: string,
  columnName: string,
  selectedValues: unknown[],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  dispatch: (action: any) => void,
  filters: Array<{ id?: string }> = [],
) {
  const datasetId = Object.keys(datasets).find(dataId => datasets[dataId].label === datasetName);
  if (!datasetId) return;
  const dataset = datasets[datasetId];
  if (dataset) {
    const values = Array.from({ length: dataset.length }, (_, i) => dataset.getValue(columnName, i));
    const valueDict = values.reduce((acc, value, index) => {
      acc[value as string | number] = index;
      return acc;
    }, {} as Record<string | number, number>);
    const selectedIndices = selectedValues.map(value => valueDict[value as string | number]);
    highlightRows(datasets, layers, datasetName, selectedIndices, dispatch, filters);
  }
}

export async function appendColumnsToDataset(
  datasets: Datasets,
  layers: Layer[],
  datasetName: string,
  result: Record<string, number>[],
  newDatasetName: string
) {
  const datasetId = Object.keys(datasets).find(dataId => datasets[dataId].label === datasetName);
  if (!datasetId) {
    throw new Error(`Dataset ${datasetName} not found`);
  }

  const originalDataset = datasets[datasetId];
  const fields = originalDataset.fields;
  const numRows = originalDataset.length || result.length;

  const rowObjects: Record<string, unknown>[] = [];

  if (originalDataset.type === 'vector-tile') {
    const columnData: Record<string, unknown[]> = {};
    for (const field of fields) {
      columnData[field.name] = getValuesFromVectorTileLayer(datasetId, layers, field);
    }
    for (let i = 0; i < numRows; i++) {
      const rowObject: Record<string, unknown> = {};
      for (const field of fields) {
        rowObject[field.name] = columnData[field.name][i];
      }
      rowObjects.push(rowObject);
    }
  } else {
    for (let i = 0; i < numRows; i++) {
      const rowObject: Record<string, unknown> = {};
      for (const field of fields) {
        const value = originalDataset.getValue(field.name, i);
        rowObject[field.name] = value;
      }
      rowObjects.push(rowObject);
    }
  }

  for (let i = 0; i < numRows; i++) {
    const queryRow = result[i];
    const rowObject = rowObjects[i];
    Object.keys(queryRow).forEach(key => {
      const value = queryRow[key];
      rowObject[key] = value;
    });
  }

  const processedData = await processFileData({
    content: { fileName: newDatasetName, data: rowObjects },
    fileCache: []
  });

  return processedData;
}
