/**
 * Databricks Tools Setup for kepler.gl AI Assistant
 *
 * Wires the Databricks Genie tool to the kepler.gl Redux dispatch so that
 * Genie query results containing geometry are added to the map.
 */
import { Dispatch } from 'redux';
import { addDataToMap } from '@kepler.gl/actions';
import { processGeojson } from '@kepler.gl/processors';
import type { FeatureCollection } from 'geojson';

import { databricksGenie } from './genie-tool';
import type { DatabricksSqlContext } from './types';

export interface DatabricksToolsOptions {
  /**
   * Redux dispatch function.
   */
  dispatch: Dispatch;
}

/**
 * Build an addDataToMap function compatible with kepler.gl from the Redux
 * dispatch.
 */
function createAddDataToMap(dispatch: Dispatch): DatabricksSqlContext['addDataToMap'] {
  return (geojson: FeatureCollection, datasetName: string, options) => {
    const processedData = processGeojson(geojson);
    if (!processedData) {
      console.warn(`Failed to process GeoJSON for dataset: ${datasetName}`);
      return;
    }

    dispatch(
      addDataToMap({
        datasets: {
          info: { label: datasetName, id: datasetName },
          data: processedData,
        },
        options: {
          centerMap: options?.centerMap ?? false,
          keepExistingConfig: true,
        },
      }),
    );
  };
}

/**
 * Get configured Databricks tools for the kepler.gl AI assistant.
 *
 * @example
 * ```typescript
 * const tools = {
 *   ...getDatabricksTools({ dispatch }),
 *   ...getKeplerTools(...),
 * };
 * ```
 */
export function getDatabricksTools(options: DatabricksToolsOptions) {
  const context: DatabricksSqlContext = {
    addDataToMap: createAddDataToMap(options.dispatch),
  };

  return {
    databricksGenie: {
      ...databricksGenie,
      context,
    },
  };
}

export default getDatabricksTools;
