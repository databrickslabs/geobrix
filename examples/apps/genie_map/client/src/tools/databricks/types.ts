/**
 * Databricks SQL Tools - Types
 *
 * Shared types for the Databricks Genie tool used by the kepler.gl
 * AI assistant.
 */

import type { Feature, FeatureCollection, Geometry } from 'geojson';

/**
 * Context provided to the Databricks Genie tool. Wraps the kepler.gl
 * dispatch needed to add query results to the map.
 */
export interface DatabricksSqlContext {
  /**
   * Add GeoJSON data to the kepler.gl map.
   */
  addDataToMap: (
    geojson: FeatureCollection,
    datasetName: string,
    options?: {
      autoCreateLayers?: boolean;
      centerMap?: boolean;
    },
  ) => void;
}

/**
 * Result returned by Databricks SQL tools.
 */
export interface DatabricksSqlToolResult<T = unknown> {
  /** Result sent back to the LLM */
  llmResult: {
    success: boolean;
    message: string;
    details?: T;
  };
  /** Additional data for UI components */
  additionalData?: {
    geojson?: FeatureCollection;
    datasetName?: string;
    rowCount?: number;
    columns?: string[];
    sql?: string;
    rows?: Record<string, unknown>[];
    description?: string;
  };
}

/**
 * Parse a GeoJSON column out of tabular rows into a FeatureCollection.
 */
export function parseGeoJsonFromRows(
  rows: unknown[],
  geojsonColumn: string = 'geojson',
): FeatureCollection {
  const features: Feature[] = [];

  for (const row of rows) {
    const record = row as Record<string, unknown>;
    const geojsonValue = record[geojsonColumn];
    if (!geojsonValue) continue;

    let geometry: Geometry;
    try {
      if (typeof geojsonValue === 'string') {
        geometry = JSON.parse(geojsonValue);
      } else if (typeof geojsonValue === 'object') {
        geometry = geojsonValue as Geometry;
      } else {
        continue;
      }
    } catch {
      continue;
    }

    const properties: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(record)) {
      if (key !== geojsonColumn) properties[key] = value;
    }

    features.push({ type: 'Feature', geometry, properties });
  }

  return { type: 'FeatureCollection', features };
}

/**
 * Generate a unique dataset name with timestamp.
 */
export function generateDatasetName(prefix: string): string {
  const timestamp = Date.now().toString(36);
  return `${prefix}_${timestamp}`;
}
