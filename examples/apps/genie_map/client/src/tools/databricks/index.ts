/**
 * Databricks tools for the kepler.gl AI Assistant.
 *
 * Provides a single tool today:
 * - `databricksGenie` — natural language to SQL via the AppKit `genie` plugin,
 *   with automatic map updates for geometry results.
 */

export { getDatabricksTools, type DatabricksToolsOptions } from './setup';
export { databricksGenie } from './genie-tool';
export {
  type DatabricksSqlContext,
  type DatabricksSqlToolResult,
  parseGeoJsonFromRows,
  generateDatasetName,
} from './types';

export type {
  FeatureCollection as GeoJSONFeatureCollection,
  Feature as GeoJSONFeature,
} from 'geojson';
