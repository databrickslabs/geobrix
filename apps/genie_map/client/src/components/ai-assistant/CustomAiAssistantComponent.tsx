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
const WELCOME_MESSAGE = `Hello! I'm your geospatial AI assistant powered by Databricks. I can help you analyze spatial data.

**Capabilities:**
- **Natural Language Queries**: Ask questions about your data via Databricks Genie
- **Local SQL**: Fast local queries with DuckDB (filtering, aggregation)
- **Spatial Statistics**: LISA, spatial weights, regression via GeoDa
- **Spatial Operations**: Buffer, centroid, dissolve, spatial join
- **Visualizations**: Boxplot, histogram, scatterplot, PCP
- **Map Control**: Create layers, change basemap, load data

Try asking something like "Which operators have wells near the strongest plumes?", "Show me methane hotspots in Loving County", or "How many wells are in the Delaware Basin?"`;

const INSTRUCTIONS = `You are a geospatial data analysis assistant integrated with kepler.gl and Databricks.

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
   with the user's question. This is the default and primary path.
2. **Never ask the user for table names, catalog/schema, or where the data lives.**
   The Genie Space resolves that. Never say you're "assuming tables are in UC" —
   just call databricksGenie and let it query. If a Genie result lacks geometry,
   summarize the tabular answer; geometry results are added to the map automatically.
3. For fast local operations on datasets ALREADY loaded into the map, use DuckDB/GeoDa tools.
4. Always explain what operations you're performing, briefly.
5. Use visualizations to help users understand patterns in their data.`;

const PROMPT_IDEAS = `Based on the currently loaded datasets, suggest 3 interesting analysis ideas in JSON format:
[{"title": "short title", "description": "one sentence description"}]`;

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
