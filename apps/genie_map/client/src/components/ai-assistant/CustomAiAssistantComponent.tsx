/**
 * Custom AI Assistant Component
 *
 * This component extends kepler.gl's AI assistant with:
 * - Databricks Genie (Text-to-SQL) for natural language queries
 * - DuckDB WASM for fast local SQL queries
 * - GeoDa WASM for spatial statistics (LISA, buffer, centroid, etc.)
 * - ECharts for interactive visualizations
 * - kepler.gl map tools (layers, basemap, data loading)
 *
 * Uses a hybrid approach:
 * - Client-side: DuckDB/GeoDa for fast local operations
 * - Server-side: Databricks SQL for Unity Catalog cross-dataset queries
 */
import React, { useEffect, useRef, useState, useMemo } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import styled from 'styled-components';
import { textColorLT, theme } from '@kepler.gl/styles';
import { MessageModel, useAssistant } from '@openassistant/core';
import { AiAssistant } from '@openassistant/ui';
import '@openassistant/echarts/dist/index.esm.css';
import '@openassistant/ui/dist/index.esm.css';
import {
  setScreenCaptured,
  setStartScreenCapture,
  updateAiAssistantMessages,
  type AiAssistantState,
} from '@kepler.gl/ai-assistant';
import type { VisState } from '@kepler.gl/schemas';
import type { MapStyle } from '@kepler.gl/reducers';
import type { Dispatch } from 'redux';

import { getDatabricksTools } from '../../tools/databricks';
import { getKeplerTools, getQueryTool, getGeoTools, getEchartsTools, getDatasetContext } from '../../tools/kepler';

// Constants
const ASSISTANT_NAME = 'Kepler.gl AI Assistant';
const ASSISTANT_DESCRIPTION = 'AI-powered geospatial analysis assistant with Databricks SQL';
const ASSISTANT_VERSION = '1.0.0';
// __GENIE_SPACE_NAME__ is injected at build time (vite define) from
// VITE_GENIE_SPACE_NAME; fall back to a generic phrase if it wasn't set.
const GENIE_SPACE_LABEL = __GENIE_SPACE_NAME__ || 'the configured Databricks Genie Space';

const WELCOME_MESSAGE = `Hello! I'm your geospatial AI assistant powered by Databricks. I can help you analyze spatial data.

Natural-language questions are answered by **${GENIE_SPACE_LABEL}**.

**Capabilities:**
- **Natural Language Queries**: Ask questions about your data via Databricks Genie
- **Local SQL**: Fast local queries with DuckDB (filtering, aggregation)
- **Spatial Statistics**: LISA, spatial weights, regression via GeoDa
- **Spatial Operations**: Buffer, centroid, dissolve, spatial join
- **Visualizations**: Boxplot, histogram, scatterplot, PCP
- **Map Control**: Create layers, change basemap, load data

Try asking something like "Which operators have wells near the strongest plumes?", "Show me methane hotspots in Loving County", or "How many wells are in the Delaware Basin?"`;

const INSTRUCTIONS = `You are a geospatial data analysis assistant integrated with kepler.gl and Databricks.

## Operating principles (READ FIRST — these override everything below)
1. **Act, don't ask.** Default to producing an answer, not a clarifying question or a
   menu of options. Never reply with "Option A / Option B", "do you want 1, 2, or 3?",
   or "if you confirm, I'll…". Pick the best approach, state your choice and any
   assumption in one short sentence, and deliver the result. Only ask the user when the
   request is genuinely ambiguous in a way that changes the answer AND you cannot pick a
   reasonable default — or when an action is destructive. "Which of these did you mean"
   is a last resort, not an opening move.
2. **One Genie call beats a local tool chain.** For any question that involves the
   underlying data — especially multi-step analysis (join + aggregate + statistics +
   regression/residuals) — send the WHOLE task to \`databricksGenie\` in a single
   natural-language request and let it do the SQL server-side. Do NOT try to assemble it
   from local DuckDB/GeoDa tools (genericQuery, tableTool, mergeTablesTool, regressionTool)
   unless every input dataset is already loaded into the map. The local tools cannot
   reliably reference each other's intermediate outputs (e.g. a \`merge_…\` result is not
   addressable by a later query), so composing them for data that lives in Databricks
   dead-ends. When a local chain fails or a merged/derived dataset "is not found", STOP
   retrying locally and re-issue the entire request to \`databricksGenie\`.
3. **Never interrogate the map layers' internals.** The on-map layers (e.g. "Well
   Density (H3)") are rendering aids: their H3 resolution is dynamic (changes with zoom
   and density) and their hex ids are not a stable analytical grid. NEVER ask the user
   what resolution a layer is, and never try to join to a layer's hexes. For any gridded
   or spatial-join analysis, have Genie compute cells server-side at a fixed resolution
   (default H3 res 7 for this data; res 6 if you need more overlapping cells) — Genie
   knows the coordinate columns and the h3 functions.
4. **Trust Genie with the data model.** The Genie Space knows every table, column, and
   join. Never ask the user for table names, catalog/schema, resolution of stored data,
   or "where the data lives", and never say you're "assuming" a table exists — just call
   \`databricksGenie\`.
5. **Deliver, then offer next steps.** Give the result (a summary, a stat, a map layer),
   THEN optionally suggest one concrete follow-up. Don't gate the first result behind a
   question.
6. **Phrase Genie calls with defaults baked in; don't relay Genie's hedging.** When you
   call \`databricksGenie\`, state the concrete choice in the question itself (e.g.
   "...using H3 res 7", "...within 1 km using the nearest-well table", "use permissive
   thresholds so results aren't empty") rather than leaving it open. If Genie answers with
   a clarifying question ("Would you prefer mean or max?"), do NOT forward that question to
   the user — pick the sensible default, re-ask Genie with it specified, and return the
   result. If a Genie result comes back empty because a threshold was too strict, retry
   once with a looser threshold before reporting "no results".
7. **Retry transient Genie errors once.** A Genie call can occasionally fail with a
   transient error ("Authentication is temporarily unavailable", or a generic failure
   right after the app starts). Silently retry once before surfacing any error to the user.

## Available Tool Categories

### Databricks Tools (server-side, Unity Catalog access)
- databricksGenie: Ask natural language questions about Unity Catalog data via Databricks AI/BI Genie. Geometry results are added to the map automatically.

### Local Query Tools (client-side, fast)
- genericQuery: Execute SQL queries locally with DuckDB
- filterDataset: Filter dataset and save as new dataset
- tableTool: Create/modify tables using SQL
- mergeTablesTool: Merge multiple datasets

### Visualization Tools (ECharts)
- boxplotTool: Create box plots
- bubbleChartTool: Create bubble charts
- histogramTool: Create histograms
- pcpTool: Parallel coordinate plots
- scatterplotTool: Create scatterplots

### Spatial Statistics Tools (GeoDa WASM)
- lisaTool: Local Indicators of Spatial Association
- classifyTool: Data classification (jenks, quantile, etc.)
- weightsTool: Create spatial weights matrices
- globalMoranTool: Global Moran's I statistic
- regressionTool: Spatial regression analysis
- spatialJoinTool: Join datasets spatially
- bufferTool: Create buffer zones
- centroidTool: Calculate centroids
- dissolveTool: Dissolve geometries
- areaTool, lengthTool, perimeterTool: Geometry measurements

### Map Control Tools
- basemap: Change map style
- addLayer: Create map layers from datasets
- updateLayerColor: Update layer colors
- loadData: Load data from URL
- saveDataToMap: Save tool results to map
- mapBoundary: Get current map viewport

## About the data (Genie Space)
The \`databricksGenie\` tool is connected to a curated Databricks AI/BI Genie Space
over the Permian Basin methane gold tables — CH₄ hotspots, EMIT plumes, well
inventory (with operator / shale play / county), and plume→well attribution. The
Genie Space already knows every table, column, and join. You do NOT know the table
names yourself, and you do NOT need to — Genie resolves them.

## Guidelines
1. **Any question about the data** (plumes, wells, operators, hotspots, methane,
   counties, basins, "show me…", "which…", "how many…") → call \`databricksGenie\`
   with the user's question. This is the default and primary path. If a Genie result
   lacks geometry, summarize the tabular answer; geometry results are added to the map
   automatically.
2. Use the local DuckDB/GeoDa tools ONLY for fast operations on datasets ALREADY loaded
   into the map — never to reconstruct a Databricks query (see principle 2 above).
3. Explain what you're doing in one brief sentence — don't narrate every tool call.
4. Use visualizations to help users understand patterns in their data.`;

const PROMPT_IDEAS = `Based on the currently loaded datasets, suggest 3 analysis ideas that
produce an INTERACTIVE COMPONENT on this map — not a plain text answer. Each idea's
"description" must be a ready-to-run natural-language prompt the user can click to send
as-is. Bias toward the two component types this app renders:
  1. A NEW MAP LAYER — phrased "Show/Map ..." over the methane gold data (plumes, wells,
     CH4 hotspots, operators, counties, shale plays), which draws a geometry layer.
  2. A CHART THAT CROSS-FILTERS THE MAP — phrased "Chart ... and let me filter the map",
     which returns one row per feature (geometry + attributes) so the chart's selection
     cross-filters the map layer.
Make each prompt specific to the loaded data (name real attributes like max_conc_ppmm,
well_count, operator, county, play). Prefer at least one cross-filter idea. Return
JSON only:
[{"title": "short title", "description": "the full clickable prompt"}]`;

// Fixed Databricks configuration - uses our local LLM proxy
const DATABRICKS_CONFIG = {
  provider: 'openai' as const, // OpenAI-compatible API format
  // Model name is injected at build time from DATABRICKS_SERVING_ENDPOINT_NAME (.env).
  // To change the model, update that env var and rebuild — no code change required.
  model: __LLM_MODEL__,
  baseUrl: '/api/custom/llm', // Our proxy endpoint (no /v1 suffix needed)
  apiKey: 'databricks-proxy', // Placeholder - actual auth handled by proxy
};

const StyledAiAssistantComponent = styled.div`
  height: 100%;
  padding-bottom: 4px;

  * {
    font-size: 11px;
  }
`;

// State type matching kepler.gl's expected structure
type State = {
  demo: {
    keplerGl: {
      map: {
        uiState: { locale: string };
        visState: VisState;
        mapStyle: MapStyle;
        mapState: { latitude: number; longitude: number; zoom: number };
      };
    };
    aiAssistant: AiAssistantState;
  };
};

export function CustomAiAssistantComponent() {
  const visState = useSelector((state: State) => state.demo?.keplerGl?.map?.visState);
  const aiAssistant = useSelector((state: State) => state.demo?.aiAssistant);
  // mapBoundary is written to Redux by App.tsx via setMapBoundary() on every
  // viewport change (WebMercatorViewport.unproject) — use it directly rather
  // than approximating bounds from the map centre.
  const mapBoundary = useSelector((state: State) => state.demo?.aiAssistant?.keplerGl?.mapBoundary);
  const dispatch = useDispatch() as Dispatch;

  // Get datasets and layers from visState
  const datasets = visState?.datasets || {};
  const layers = visState?.layers || [];
  const layerData = visState?.layerData || [];

  // Get Databricks tools with dispatch bound
  const databricksTools = useMemo(
    () => getDatabricksTools({ dispatch }),
    [dispatch]
  );

  // Get kepler.gl tools
  const keplerTools = useMemo(() => {
    return getKeplerTools({
      datasets,
      layers,
      loaders: visState?.loaders,
      loadOptions: visState?.loadOptions,
      mapBoundary,
    });
  }, [datasets, layers, mapBoundary, visState?.loaders, visState?.loadOptions]);

  // Get query tools (DuckDB WASM)
  const queryTools = useMemo(
    () => getQueryTool(datasets, layers),
    [datasets, layers]
  );

  // Get geo tools (GeoDa WASM)
  const geoTools = useMemo(
    () => getGeoTools(
      { mapboxToken: undefined }, // Add mapbox token if needed
      datasets,
      layers,
      layerData
    ),
    [datasets, layers, layerData]
  );

  // Get echarts tools
  const echartsTools = useMemo(
    () => getEchartsTools(datasets, layers, dispatch),
    [datasets, layers, dispatch]
  );

  // Combine all tools - hybrid approach
  const tools = useMemo(() => {
    return {
      // Databricks tools for server-side queries
      ...databricksTools,
      // Kepler map tools
      ...keplerTools,
      // Local query tools (DuckDB WASM)
      ...queryTools,
      // Geo/spatial statistics tools (GeoDa WASM)
      ...geoTools,
      // Visualization tools (ECharts)
      ...echartsTools,
    };
  }, [databricksTools, keplerTools, queryTools, geoTools, echartsTools]);

  // Enable voice and screen capture (supported with OpenAI-compatible API)
  const enableVoiceAndScreenCapture = true;

  // Assistant props - using fixed Databricks configuration
  const assistantProps = {
    name: ASSISTANT_NAME,
    description: ASSISTANT_DESCRIPTION,
    version: ASSISTANT_VERSION,
    modelProvider: DATABRICKS_CONFIG.provider,
    model: DATABRICKS_CONFIG.model,
    apiKey: DATABRICKS_CONFIG.apiKey,
    baseUrl: DATABRICKS_CONFIG.baseUrl,
    tools,
  };

  const [datasetMetaData, setDatasetMetaData] = useState<string>('');
  const [ideas, setIdeas] = useState<{ title: string; description: string }[]>([]);
  const [restartKey, setRestartKey] = useState<number>(0);

  // Update dataset metadata when datasets or layers change
  useEffect(() => {
    const metaData = getDatasetContext(datasets, layers);
    setDatasetMetaData(metaData);
  }, [datasets, layers]);

  // Full instructions with dataset context
  const instructions = `${INSTRUCTIONS}\n\n${datasetMetaData}`;

  // Generate ideas from LLM
  const { restartChat: libraryRestartChat } = useAssistant({
    ...assistantProps,
    instructions,
  });

  const restartChatRef = useRef(libraryRestartChat);
  restartChatRef.current = libraryRestartChat;

  // Call our LLM proxy directly (without tools) to get prompt suggestions.
  // Going through openassistant's `temporaryPrompt` would include all
  // registered tools in the request — gpt-5 then prefers to invoke a tool
  // instead of returning the requested JSON, leaving the response empty.
  const generateIdeas = async () => {
    try {
      const response = await fetch(
        `${DATABRICKS_CONFIG.baseUrl}/chat/completions`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${DATABRICKS_CONFIG.apiKey}`,
          },
          body: JSON.stringify({
            model: DATABRICKS_CONFIG.model,
            stream: false,
            temperature: 1.0,
            messages: [
              { role: 'system', content: instructions },
              { role: 'user', content: PROMPT_IDEAS },
            ],
          }),
        },
      );

      if (!response.ok) {
        console.error('Error generating ideas: HTTP', response.status, await response.text());
        return;
      }

      const data = await response.json();
      const text: string | undefined = data?.choices?.[0]?.message?.content;
      const match = text?.match(/\[\s*\{[\s\S]*?\}\s*\]/);
      if (match) {
        setIdeas(JSON.parse(match[0]));
      }
    } catch (error) {
      console.error('Error generating ideas', error);
    }
  };

  useEffect(() => {
    if (ideas.length === 0 && datasetMetaData.length > 0) {
      generateIdeas();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [datasetMetaData]);

  const onRestartAssistant = async () => {
    dispatch(updateAiAssistantMessages([]));
    try {
      await restartChatRef.current();
    } catch (e) {
      console.error('Error restarting chat:', e);
    }
    setRestartKey((prev) => prev + 1);
  };

  const onMessagesUpdated = (messages: MessageModel[]) => {
    dispatch(updateAiAssistantMessages(messages));
  };

  const onScreenshotClick = () => {
    dispatch(setStartScreenCapture(true));
  };

  const onRemoveScreenshot = () => {
    dispatch(setScreenCaptured(''));
  };

  return (
    <StyledAiAssistantComponent className="ai-assistant-component">
      <AiAssistant
        key={restartKey}
        {...assistantProps}
        instructions={instructions}
        theme={theme.textColor === textColorLT ? 'light' : 'dark'}
        welcomeMessage={WELCOME_MESSAGE}
        temperature={0.7}
        topP={1.0}
        initialMessages={aiAssistant?.messages || []}
        onMessagesUpdated={onMessagesUpdated}
        enableVoice={enableVoiceAndScreenCapture}
        enableScreenCapture={enableVoiceAndScreenCapture}
        onScreenshotClick={onScreenshotClick}
        screenCapturedBase64={aiAssistant?.screenshotToAsk?.screenCaptured || ''}
        onRemoveScreenshot={onRemoveScreenshot}
        onRestartChat={onRestartAssistant}
        fontSize={'text-tiny'}
        botMessageClassName={''}
        githubIssueLink={'https://github.com/keplergl/kepler.gl/issues'}
        ideas={ideas}
        onRefreshIdeas={generateIdeas}
      />
    </StyledAiAssistantComponent>
  );
}

export default CustomAiAssistantComponent;
