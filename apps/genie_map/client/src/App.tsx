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
import { CustomAiAssistantPanel as AiAssistantPanel } from './components/ai-assistant';
import { useViewportBounds } from './hooks/useViewportBounds';
import { useLayerVisibility } from './hooks/useLayerVisibility';
import type { LayerRule } from './hooks/useLayerVisibility';
import { usePanelState } from './hooks/usePanelState';
import { useLayerData } from './hooks/useLayerData';
import { getActiveDataset, VAPOR_EYES_TABLES } from './config/datasets';

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

// ---------------------------------------------------------------------------
// Registry-driven layer wiring
//
// The active dataset (Task 5 registry) is the single source of truth for which
// map layers exist, which source table feeds each, and their zoom-visibility
// bands. Adding/removing a layer is a registry edit — no changes here.
// ---------------------------------------------------------------------------

const DATASET = getActiveDataset();

// Each registry layer id → the source table its query reads. well_density (H3)
// and wells (points) intentionally share the enriched-wells source.
const TABLE_BY_LAYER: Record<string, string> = {
  ch4_hotspots: VAPOR_EYES_TABLES.hotspot,
  well_density: VAPOR_EYES_TABLES.wellsEnriched, // well_density H3 aggregates well points
  wells:        VAPOR_EYES_TABLES.wellsEnriched, // ...same source as the wells point layer
  plumes:       VAPOR_EYES_TABLES.plumes,
};

// Layer ids MUST be derived exactly as the layer factories produce them
// (`h3-layer-${id}` / `point-layer-${id}`), not the stale singular constants.
const LAYER_RULES: LayerRule[] = DATASET.layers.map((l) => ({
  layerId: l.kind === 'h3' ? `h3-layer-${l.id}` : `point-layer-${l.id}`,
  activeWhen: (z: number) => z >= l.zoomVisible.min && z < l.zoomVisible.max,
}));

// ---------------------------------------------------------------------------

function App() {
  const dispatch = useDispatch();

  const { isAiAssistantPanelOpen, isSqlPanelOpen, hasSidePanelOpen, startScreenCapture } = usePanelState();
  const { bounds, onViewStateChange } = useViewportBounds();

  // Registry-driven data loading. The registry is a fixed-length (4-layer)
  // compile-time constant, so the per-layer hook calls below are unrolled to
  // keep React's rules-of-hooks contract (stable call order every render).
  // Each hook loads its layer's viewport-scoped rows into kepler.gl regardless
  // of which side panel is open.
  useLayerData(DATASET.layers[0], bounds, TABLE_BY_LAYER[DATASET.layers[0].id]);
  useLayerData(DATASET.layers[1], bounds, TABLE_BY_LAYER[DATASET.layers[1].id]);
  useLayerData(DATASET.layers[2], bounds, TABLE_BY_LAYER[DATASET.layers[2].id]);
  useLayerData(DATASET.layers[3], bounds, TABLE_BY_LAYER[DATASET.layers[3].id]);

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
