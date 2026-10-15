// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import {
  boxplot,
  BoxplotTool,
  bubbleChart,
  BubbleChartTool,
  histogram,
  HistogramTool,
  pcp,
  PCPTool,
  scatterplot,
  ScatterplotTool
} from '@openassistant/plots';
import {
  BoxplotComponent,
  BubbleChartComponent,
  HistogramPlotComponent,
  ParallelCoordinateComponent,
  ScatterplotComponent
} from '@openassistant/echarts';

import {Datasets} from '@kepler.gl/table';
import {Layer} from '@kepler.gl/layers';
import {Dispatch} from 'redux';

import {getValuesFromDataset, highlightRows} from './utils';

export function getEchartsTools(
  datasets: Datasets,
  layers: Layer[],
  dispatch: Dispatch,
  filters: Array<{ id?: string }> = [],
) {
  // context for tools
  const getValues = async (datasetName: string, variableName: string) => {
    const values = getValuesFromDataset(datasets, layers, datasetName, variableName);
    return values as number[];
  };

  const onSelected = (datasetName: string, selectedIndices: number[]) => {
    // Persist the selection as a kepler filter (via highlightRows -> createOrUpdateFilter)
    // so it survives re-renders, instead of a transient dataset.filteredIndex mutation.
    // filters is passed so an empty selection can resolve the filter INDEX for removeFilter.
    highlightRows(datasets, layers, datasetName, selectedIndices, dispatch, filters);
  };

  // Create the boxplot tool with the getValues implementation
  const boxplotTool: BoxplotTool = {
    ...boxplot,
    context: {
      ...boxplot.context,
      getValues,
      onSelected
    },
    component: BoxplotComponent
  };

  // Create the bubble chart tool with the getValues implementation
  const bubbleChartTool: BubbleChartTool = {
    ...bubbleChart,
    context: {
      ...bubbleChart.context,
      getValues,
      onSelected
    },
    component: BubbleChartComponent
  };

  const histogramTool: HistogramTool = {
    ...histogram,
    context: {
      ...histogram.context,
      getValues,
      onSelected
    },
    component: HistogramPlotComponent
  };

  const pcpTool: PCPTool = {
    ...pcp,
    context: {
      ...pcp.context,
      getValues,
      onSelected
    },
    component: ParallelCoordinateComponent
  };

  const scatterplotTool: ScatterplotTool = {
    ...scatterplot,
    context: {
      ...scatterplot.context,
      getValues,
      onSelected
    },
    component: ScatterplotComponent
  };

  return {
    boxplotTool,
    bubbleChartTool,
    histogramTool,
    pcpTool,
    scatterplotTool
  };
}
