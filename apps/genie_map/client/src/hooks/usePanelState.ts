import { useSelector } from 'react-redux';
import type { RootState } from '../store';

interface PanelState {
  isAiAssistantPanelOpen: boolean;
  isSqlPanelOpen: boolean;
  /** True when any side panel is open — drives the kepler map panel size. */
  hasSidePanelOpen: boolean;
  /** Drives the AI-assistant screenshot capture overlay. */
  startScreenCapture: boolean;
}

/**
 * Reads kepler.gl ui state to determine which side panels are open.
 * Extracted from App.tsx so the component body stays focused on layout.
 */
export function usePanelState(): PanelState {
  const isAiAssistantPanelOpen = useSelector(
    (state: RootState) =>
      state.demo?.keplerGl?.map?.uiState?.mapControls?.aiAssistant?.active ?? false,
  );
  const isSqlPanelOpen = useSelector(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (state: RootState) =>
      (state.demo?.keplerGl?.map?.uiState?.mapControls as any)?.sqlPanel?.active ?? false,
  );
  const startScreenCapture = useSelector(
    (state: RootState) =>
      state.demo?.aiAssistant?.screenshotToAsk?.startScreenCapture ?? false,
  );

  return {
    isAiAssistantPanelOpen,
    isSqlPanelOpen,
    hasSidePanelOpen: isAiAssistantPanelOpen || isSqlPanelOpen,
    startScreenCapture,
  };
}
