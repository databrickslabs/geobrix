// Map control factory - injects AI Assistant and SQL Panel controls into kepler.gl

import React, { useCallback } from 'react';
import styled from 'styled-components';
import { withState, MapControlFactory, MapControlTooltipFactory, MapControlButton } from '@kepler.gl/components';
import { AiAssistantControlFactory } from '@kepler.gl/ai-assistant';
import type { MapControls } from '@kepler.gl/types';

const StyledMapControlPanel = styled.div`
  position: relative;
`;

const StyledMapControlOverlay = styled.div<{ $top: number }>`
  position: absolute;
  display: flex;
  top: ${(props) => props.$top}px;
  right: 0;
  z-index: 1;
  pointer-events: none !important;
  & > * {
    pointer-events: all;
  }
`;

// Extended map-controls type for our custom sqlPanel control.
interface CustomMapControl {
  show?: boolean;
  active?: boolean;
}

interface ExtendedMapControls extends MapControls {
  sqlPanel?: CustomMapControl;
}

interface PanelControlProps {
  mapControls: ExtendedMapControls;
  onToggleMapControl: (control: string) => void;
}

// SQL Panel icon (database symbol)
const SqlPanelIcon: React.FC<{ height?: string }> = ({ height = '16px' }) => (
  <svg viewBox="0 0 24 24" fill="currentColor" style={{ height, width: height }}>
    <path d="M12 3C7.58 3 4 4.79 4 7v10c0 2.21 3.58 4 8 4s8-1.79 8-4V7c0-2.21-3.58-4-8-4zm0 2c3.87 0 6 1.5 6 2s-2.13 2-6 2-6-1.5-6-2 2.13-2 6-2zm6 12c0 .5-2.13 2-6 2s-6-1.5-6-2v-2.23c1.61.78 3.72 1.23 6 1.23s4.39-.45 6-1.23V17zm0-4c0 .5-2.13 2-6 2s-6-1.5-6-2v-2.23C7.61 11.55 9.72 12 12 12s4.39-.45 6-1.23V13z"/>
  </svg>
);

// SqlPanelControlFactory - follows the kepler.gl map-control pattern.
SqlPanelControlFactory.deps = [MapControlTooltipFactory];
function SqlPanelControlFactory(
  MapControlTooltip: ReturnType<typeof MapControlTooltipFactory>
): React.FC<PanelControlProps> {
  const SqlPanelControl: React.FC<PanelControlProps> = ({
    mapControls,
    onToggleMapControl
  }) => {
    const onClick = useCallback(
      (event: React.MouseEvent) => {
        event.preventDefault();
        onToggleMapControl('sqlPanel');
      },
      [onToggleMapControl]
    );

    const showControl = mapControls?.sqlPanel?.show;
    if (!showControl) return null;

    const active = mapControls?.sqlPanel?.active;
    return (
      <MapControlTooltip
        id="show-sql-panel"
        message={active ? 'Hide SQL Panel' : 'Show SQL Panel'}
      >
        <MapControlButton
          className="map-control-button toggle-sql-panel"
          onClick={onClick}
          active={active}
        >
          <SqlPanelIcon height="18px" />
        </MapControlButton>
      </MapControlTooltip>
    );
  };

  SqlPanelControl.displayName = 'SqlPanelControl';
  return React.memo(SqlPanelControl);
}

CustomMapControlFactory.deps = [
  AiAssistantControlFactory,
  SqlPanelControlFactory,
  ...MapControlFactory.deps
];

function CustomMapControlFactory(
  AiAssistantControl: ReturnType<typeof AiAssistantControlFactory>,
  SqlPanelControlComponent: ReturnType<typeof SqlPanelControlFactory>,
  ...deps: Parameters<typeof MapControlFactory>
) {
  const MapControl = MapControlFactory(...deps);

  const actionComponents = [
    ...(MapControl.defaultActionComponents ?? []),
    SqlPanelControlComponent,
    AiAssistantControl
  ];

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const CustomMapControl = (props: any) => {
    // These panels are mutually exclusive: only one can be active at a time.
    // kepler.gl's built-in toggleMapControlUpdater only knows about its own
    // pairs (effect<->aiAssistant), so we handle our custom panels here.
    const MUTUALLY_EXCLUSIVE_PANELS = ['aiAssistant', 'sqlPanel'] as const;

    // Blur the currently focused element before toggling any map control.
    // MapControlButton CSS has &:focus { background: highlighted } which keeps
    // the button visually active after clicking — blur() removes that pseudo-state.
    const patchedToggle = useCallback((panelId: string) => {
      (document.activeElement as HTMLElement)?.blur?.();
      // Close any other currently-active panel in the exclusive group first,
      // so only one panel is active at a time.
      MUTUALLY_EXCLUSIVE_PANELS
        .filter(p => p !== panelId && (props.mapControls as any)?.[p]?.active)
        .forEach(p => props.onToggleMapControl?.(p));
      props.onToggleMapControl?.(panelId);
    }, [props.onToggleMapControl, props.mapControls]);

    return (
      <StyledMapControlOverlay $top={props.top ?? 0}>
        <StyledMapControlPanel>
          <MapControl {...props} top={0} actionComponents={actionComponents} onToggleMapControl={patchedToggle} />
        </StyledMapControlPanel>
      </StyledMapControlOverlay>
    );
  };

  return withState([], (state: { demo?: { app?: Record<string, unknown> } }) => ({
    ...state.demo?.app
  }))(CustomMapControl);
}

export function replaceMapControl() {
  return [MapControlFactory, CustomMapControlFactory];
}
