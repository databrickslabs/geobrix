/**
 * Databricks Genie Tool
 *
 * Text-to-SQL tool backed by the @databricks/appkit `genie` plugin.
 * Streams `POST /api/genie/:alias/messages` via AppKit's `connectSSE` helper,
 * collects events, and maps the result into the openassistant tool shape.
 * GeoJSON results are added to the kepler.gl map automatically.
 */
import { z } from 'zod';
import { extendedTool } from '@openassistant/utils';
import { connectSSE } from '@databricks/appkit-ui/js';
import type { GenieStreamEvent } from '@databricks/appkit-ui/react';
import type {
  DatabricksSqlContext,
  DatabricksSqlToolResult,
} from './types';
import { generateDatasetName, parseGeoJsonFromRows } from './types';

const GENIE_ALIAS = 'default';
const GENIE_ENDPOINT = `/api/genie/${GENIE_ALIAS}/messages`;

const genieParameters = z.object({
  question: z
    .string()
    .describe(
      'Natural language question to ask Genie (e.g., "Show me all taxi trips in New York")',
    ),
  addToMap: z
    .boolean()
    .default(true)
    .describe(
      'Whether to add results to the map if they contain geometry (default: true)',
    ),
  outputDatasetName: z
    .string()
    .optional()
    .describe('Optional name for the output dataset in kepler.gl'),
});

interface GenieSummary {
  sql?: string;
  description?: string;
  text?: string;
  suggestedQuestions?: string[];
  columns: string[];
  rows: Record<string, unknown>[];
  error?: string;
}

/** Open the Genie SSE stream and collect every event until it closes. */
async function collectGenieEvents(question: string): Promise<GenieStreamEvent[]> {
  const events: GenieStreamEvent[] = [];
  let connectionError: unknown;

  await connectSSE<{ content: string }>({
    url: GENIE_ENDPOINT,
    payload: { content: question },
    maxRetries: 0,
    onMessage: async (msg) => {
      if (!msg.data) return;
      try {
        events.push(JSON.parse(msg.data) as GenieStreamEvent);
      } catch {
        // Skip malformed events
      }
    },
    onError: (err) => {
      connectionError = err;
    },
  });

  if (connectionError && events.length === 0) {
    throw connectionError instanceof Error
      ? connectionError
      : new Error(String(connectionError));
  }
  return events;
}

/** Reduce the SSE event stream into a single flat summary. */
function summarizeGenieEvents(events: GenieStreamEvent[]): GenieSummary {
  const errorEvent = events.find((e) => e.type === 'error');
  const messageResult = events.find((e) => e.type === 'message_result');
  const queryResult = events.find((e) => e.type === 'query_result');

  const attachments = messageResult?.message.attachments ?? [];
  const queryAttachment = attachments.find((a) => a.query);
  const textAttachment = attachments.find((a) => a.text?.content);
  const suggestionsAttachment = attachments.find((a) => a.suggestedQuestions);

  const columns =
    queryResult?.data.manifest.schema.columns.map((c) => c.name) ?? [];
  const rows = (queryResult?.data.result.data_array ?? []).map((row) => {
    const obj: Record<string, unknown> = {};
    columns.forEach((col, i) => {
      obj[col] = row[i];
    });
    return obj;
  });

  return {
    sql: queryAttachment?.query?.query,
    description: queryAttachment?.query?.description,
    text: textAttachment?.text?.content ?? messageResult?.message.content,
    suggestedQuestions: suggestionsAttachment?.suggestedQuestions,
    columns,
    rows,
    error: errorEvent?.error ?? messageResult?.message.error,
  };
}

/** Find the first column likely to contain a GeoJSON geometry value. */
export function findGeometryColumn(columns: string[]): string | undefined {
  return columns.find((col) => {
    const lower = col.toLowerCase();
    return lower.includes('geojson') || lower === 'geometry' || lower === 'geom';
  });
}

/**
 * Databricks Genie Tool — natural language → SQL via the AppKit genie plugin.
 * Geometry results are added to the kepler.gl map automatically.
 */
export const databricksGenie = extendedTool<
  typeof genieParameters,
  DatabricksSqlToolResult['llmResult'],
  DatabricksSqlToolResult['additionalData'],
  DatabricksSqlContext
>({
  description: `Ask natural language questions about your Databricks data using Genie.
Genie converts your question to SQL, executes it, and returns results.
Results containing geometry will automatically be added to the map.
Examples:
- "Show me all records in the current map area"
- "What are the top 10 groups by record count?"
- "Find all records where the primary metric is above average"`,
  parameters: genieParameters,
  execute: async (args, options) => {
    const context = options?.context as DatabricksSqlContext | undefined;

    try {
      const events = await collectGenieEvents(args.question);
      const result = summarizeGenieEvents(events);

      if (result.error) {
        return {
          llmResult: {
            success: false,
            message: result.error,
            details: {
              text: result.text,
              suggestedQuestions: result.suggestedQuestions,
            },
          },
        };
      }

      // Pure-text response (no SQL was generated)
      if (!result.sql && result.rows.length === 0) {
        return {
          llmResult: {
            success: true,
            message:
              result.text || `Genie returned no data for: "${args.question}"`,
            details: {
              text: result.text,
              suggestedQuestions: result.suggestedQuestions,
            },
          },
        };
      }

      const geojsonColumn = findGeometryColumn(result.columns);

      // Geometry results → push to the map
      if (geojsonColumn && args.addToMap && context?.addDataToMap) {
        const geojson = parseGeoJsonFromRows(result.rows, geojsonColumn);
        const datasetName =
          args.outputDatasetName || generateDatasetName('genie_result');

        context.addDataToMap(geojson, datasetName, {
          autoCreateLayers: true,
          centerMap: true,
        });

        return {
          llmResult: {
            success: true,
            message: `Genie found ${geojson.features.length} features for: "${args.question}"`,
            details: {
              featureCount: geojson.features.length,
              columns: result.columns,
              sql: result.sql,
              description: result.description,
              datasetName,
            },
          },
          additionalData: {
            geojson,
            datasetName,
            rowCount: result.rows.length,
            sql: result.sql,
            description: result.description,
          },
        };
      }

      // Tabular result without geometry
      return {
        llmResult: {
          success: true,
          message: `Genie returned ${result.rows.length} rows for: "${args.question}"`,
          details: {
            rowCount: result.rows.length,
            columns: result.columns,
            sql: result.sql,
            description: result.description,
            sampleRows: result.rows.slice(0, 5),
          },
        },
        additionalData: {
          rows: result.rows,
          columns: result.columns,
          sql: result.sql,
          description: result.description,
          rowCount: result.rows.length,
        },
      };
    } catch (error) {
      return {
        llmResult: {
          success: false,
          message: `Genie query failed: ${
            error instanceof Error ? error.message : 'Unknown error'
          }`,
        },
      };
    }
  },
});
