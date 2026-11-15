// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import {Datasets} from '@kepler.gl/table';
import {Layer} from '@kepler.gl/layers';
import {Loader} from '@loaders.gl/loader-utils';

import {basemap} from './basemap-tool';
import {addLayer, AddLayerTool} from './layer-creation-tool';
import {updateLayerColor} from './layer-style-tool';
import {loadData, LoadDataTool, LoadDataToolComponent} from './loaddata-tool';
import {mapBoundary} from './boundary-tool';
import {saveToolResults} from './save-data-tool';

export {TableToolComponent} from './table-tool';

export interface KeplerToolsContext {
  datasets: Datasets;
  layers: Layer[];
  loaders?: Loader[];
  loadOptions?: object;
  mapBoundary?: {
    nw: [number, number];
    se: [number, number];
  };
}

export function getKeplerTools(context: KeplerToolsContext) {
  // context for tools
  const getDatasets = () => {
    return context.datasets;
  };

  const getLayers = () => {
    return context.layers;
  };

  const getLoaders = () => {
    return {
      loaders: context.loaders,
      loadOptions: context.loadOptions
    };
  };

  // tool: addLayer
  const addLayerTool: AddLayerTool = {
    ...addLayer,
    context: {
      getDatasets
    }
  };

  // tool: updateLayerColor
  const updateLayerColorTool = {
    ...updateLayerColor,
    context: {
      getLayers
    }
  };

  // tool: loadData
  const loadDataTool: LoadDataTool = {
    ...loadData,
    context: {
      getLoaders
    },
    component: LoadDataToolComponent
  };

  // tool: mapBoundary
  const mapBoundaryTool = {
    ...mapBoundary,
    context: {
      getMapBoundary: () => {
        return context.mapBoundary;
      }
    }
  };

  return {
    basemap,
    addLayer: addLayerTool,
    updateLayerColor: updateLayerColorTool,
    loadData: loadDataTool,
    mapBoundary: mapBoundaryTool,
    saveDataToMap: saveToolResults
  };
}

// Re-export types and components
export {basemap} from './basemap-tool';
export {addLayer, AddLayerToolComponent, guessDefaultLayer} from './layer-creation-tool';
export type {AddLayerTool} from './layer-creation-tool';
export {updateLayerColor} from './layer-style-tool';
export {loadData, LoadDataToolComponent} from './loaddata-tool';
export type {LoadDataTool} from './loaddata-tool';
export {mapBoundary} from './boundary-tool';
export {saveToolResults, SaveDataToMapToolComponent} from './save-data-tool';
