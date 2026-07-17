import { useCallback } from 'react';
import AutoSizer from 'react-virtualized/dist/commonjs/AutoSizer';
import styled, { ThemeProvider, StyleSheetManager } from 'styled-components';
import { useDispatch } from 'react-redux';
import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels';
import isPropValid from '@emotion/is-prop-valid';
import { ScreenshotWrapper } from '@openassistant/ui';
import { setStartScreenCapture, setScreenCaptured } from '@kepler.gl/ai-assistant';
import { theme, panelBorderColor } from '@kepler.gl/styles';
import { injectComponents } from '@kepler.gl/components';
import { SqlPanel } from '@kepler.gl/duckdb/components';

import { keplerGlGetState } from './store';
import { replaceMapControl } from './factories/map-control';
import { AnalyticsDashboard } from './components/panels/AnalyticsDashboard';
import { CustomAiAssistantPanel as AiAssistantPanel } from './components/ai-assistant';
import { useH3AggregationData } from './hooks/useH3AggregationData';
import { usePointData } from './hooks/usePointData';
import { useViewportBounds } from './hooks/useViewportBounds';
import { useLayerVisibility } from './hooks/useLayerVisibility';
import { usePanelState } from './hooks/usePanelState';
import { useFilterState } from './hooks/useFilterState';
import { H3_LAYER_ID } from './config/h3-layer-config';
import { POINT_LAYER_ID } from './config/point-layer-config';
import { POINT_ZOOM_THRESHOLD } from './config/dataset-config';

// Implements the default prop-forwarding behaviour from styled-components v5
function shouldForwardProp(propName: string, target: unknown) {
  if (typeof target === 'string') return isPropValid(propName);
  return true;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const KeplerGl = injectComponents([replaceMapControl()] as any);

// ---------------------------------------------------------------------------
// Layout styled components
// ---------------------------------------------------------------------------

const GlobalStyle = styled.div`
  font-family: ff-clan-web-pro, 'Helvetica Neue', Helvetica, sans-serif;
  font-weight: 400;
  font-size: 0.875em;
  line-height: 1.71429;

  *,
  *:before,
  *:after {
    box-sizing: border-box;
  }

  ul { margin: 0; padding: 0; }
  li { margin: 0; }

  a {
    text-decoration: none;
    color: ${(props: { theme: { labelColor?: string } }) => props.theme.labelColor ?? '#fff'};
  }
`;

const CONTAINER_STYLE: React.CSSProperties = {
  transition: 'margin 1s, height 1s',
  position: 'absolute',
  width: '100%',
  height: '100%',
  left: 0,
  top: 0,
  display: 'flex',
  flexDirection: 'column',
  backgroundColor: '#333',
};

const StyledVerticalResizeHandle = styled(PanelResizeHandle)`
  background-color: ${panelBorderColor};
  width: 4px;
  height: 100%;
  cursor: col-resize;
  &:hover { background-color: #555; }
`;

const BiToolsPanelContainer = styled.div`
  height: 100%;
  background: #242730;
  display: flex;
  flex-direction: column;
  overflow: hidden;
`;

const BiToolsPanelHeader = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px;
  border-bottom: 1px solid #3a3f4b;
  background: #1a1c23;
`;

const BiToolsPanelTitle = styled.h2`
  font-size: 14px;
  font-weight: 600;
  color: #ffffff;
  margin: 0;
`;

const BiToolsPanelContent = styled.div`
  flex: 1;
  overflow-y: auto;
  padding: 16px;
`;

// ---------------------------------------------------------------------------
// Layer visibility rules — add new layer types here without touching hook logic
// ---------------------------------------------------------------------------

const LAYER_RULES = [
  { layerId: H3_LAYER_ID,    activeWhen: (z: number) => z < POINT_ZOOM_THRESHOLD },
  { layerId: POINT_LAYER_ID, activeWhen: (z: number) => z >= POINT_ZOOM_THRESHOLD },
];

// ---------------------------------------------------------------------------

function App() {
  const dispatch = useDispatch();

  const { isAiAssistantPanelOpen, isBiToolsPanelOpen, isSqlPanelOpen, hasSidePanelOpen, startScreenCapture } = usePanelState();
  const { bounds, onViewStateChange } = useViewportBounds();

  // Filter/aggregation state — shared between data hooks (always running) and
  // the analytics panel UI (only mounted when the BI panel is open).
  const filterState = useFilterState();
  const { aggregation, categoryFilter, groupFilter } = filterState;

  // Data hooks always run regardless of panel visibility so kepler.gl receives
  // data from the first map interaction, not only after opening the panel.
  const { h3Data, isLoading: h3Loading } = useH3AggregationData({
    bounds,
    aggregation,
    categoryFilter,
    groupFilter,
  });

  const { pointData, isLoading: pointsLoading } = usePointData({
    bounds,
    categoryFilter,
    groupFilter,
  });

  useLayerVisibility(bounds?.zoom_level ?? null, LAYER_RULES);

  const _setStartScreenCapture = useCallback(
    (flag: boolean) => dispatch(setStartScreenCapture(flag)),
    [dispatch],
  );
  const _setScreenCaptured = useCallback(
    (screenshot: string) => dispatch(setScreenCaptured(screenshot)),
    [dispatch],
  );

  return (
    <StyleSheetManager shouldForwardProp={shouldForwardProp}>
      <ThemeProvider theme={theme}>
        <GlobalStyle>
          <ScreenshotWrapper
            startScreenCapture={startScreenCapture}
            setScreenCaptured={_setScreenCaptured}
            setStartScreenCapture={_setStartScreenCapture}
            className="h-screen"
          >
            <div style={CONTAINER_STYLE}>
              <PanelGroup direction="horizontal">
                <Panel defaultSize={hasSidePanelOpen ? 70 : 100}>
                  <AutoSizer>
                    {({ height, width }: { height: number; width: number }) => (
                      <KeplerGl
                        mapboxApiAccessToken={import.meta.env.VITE_MAPBOX_TOKEN}
                        id="map"
                        getState={keplerGlGetState}
                        width={width}
                        height={height}
                        onViewStateChange={onViewStateChange}
                      />
                    )}
                  </AutoSizer>
                </Panel>

                {isAiAssistantPanelOpen && (
                  <>
                    <StyledVerticalResizeHandle />
                    <Panel defaultSize={30} minSize={20}>
                      <AiAssistantPanel />
                    </Panel>
                  </>
                )}

                {isBiToolsPanelOpen && !isAiAssistantPanelOpen && !isSqlPanelOpen && (
                  <>
                    <StyledVerticalResizeHandle />
                    <Panel defaultSize={30} minSize={20}>
                      <BiToolsPanelContainer>
                        <BiToolsPanelHeader>
                          <BiToolsPanelTitle>Analytics Dashboard</BiToolsPanelTitle>
                        </BiToolsPanelHeader>
                        <BiToolsPanelContent>
                          <AnalyticsDashboard
                            bounds={bounds}
                            h3Data={h3Data}
                            h3Loading={h3Loading}
                            pointData={pointData}
                            pointsLoading={pointsLoading}
                            filterState={filterState}
                          />
                        </BiToolsPanelContent>
                      </BiToolsPanelContainer>
                    </Panel>
                  </>
                )}

                {isSqlPanelOpen && !isAiAssistantPanelOpen && (
                  <>
                    <StyledVerticalResizeHandle />
                    <Panel defaultSize={40} minSize={25}>
                      <SqlPanel />
                    </Panel>
                  </>
                )}
              </PanelGroup>
            </div>
          </ScreenshotWrapper>
        </GlobalStyle>
      </ThemeProvider>
    </StyleSheetManager>
  );
}

export default App;
