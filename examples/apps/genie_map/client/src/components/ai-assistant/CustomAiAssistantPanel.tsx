/**
 * Custom AI Assistant Panel
 *
 * This panel wraps the custom AI assistant component and provides
 * the same UI structure as kepler.gl's AiAssistantPanel, but uses
 * our custom component with Databricks tools.
 *
 * Unlike kepler's panel, this uses a fixed Databricks configuration
 * so no config UI is needed.
 */
import React, { useMemo } from 'react';
import styled from 'styled-components';
import { useSelector } from 'react-redux';
import { IntlProvider } from 'react-intl';
import { flattenMessages } from '@kepler.gl/utils';
import { messages as keplerGlMessages } from '@kepler.gl/localization';
import { SidePanelTitleFactory } from '@kepler.gl/components';
import type { AiAssistantState } from '@kepler.gl/ai-assistant';
import type { VisState } from '@kepler.gl/schemas';
import type { MapStyle } from '@kepler.gl/reducers';

import { CustomAiAssistantComponent } from './CustomAiAssistantComponent';

// Create the side panel title component
const SidePanelTitle = SidePanelTitleFactory();

// Type assertion for kepler.gl theme properties
interface KeplerTheme {
  sidePanelBg: string;
  borderColor: string;
  subtextColorActive: string;
  sidePanelScrollBar: string;
}

// Styled components matching kepler.gl's AI assistant panel
const StyledAiAssistantPanelContainer = styled.div`
  display: flex;
  flex-direction: column;
  pointer-events: none !important;
  flex-grow: 1;
  justify-content: space-between;
  overflow: hidden;
  height: 100%;
  width: 100%;
  & > * {
    pointer-events: all;
  }
`;

const StyledAiAssistantPanel = styled.div`
  top: 0;
  background-color: ${(props) => (props.theme as unknown as KeplerTheme).sidePanelBg};
  display: flex;
  flex-direction: column;
  flex-grow: 1;
  overflow: hidden;
`;

const StyledAiAssistantPanelHeader = styled.div`
  padding: 16px 16px 4px 16px;
  border-bottom: 1px solid ${(props) => (props.theme as unknown as KeplerTheme).borderColor};
  color: ${(props) => (props.theme as unknown as KeplerTheme).subtextColorActive};
`;

const StyledAiAssistantPanelContent = styled.div`
  ${(props) => (props.theme as unknown as KeplerTheme).sidePanelScrollBar};
  color: ${(props) => (props.theme as unknown as KeplerTheme).subtextColorActive};
  padding: 10px 0px 10px 0px;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  height: 100%;
`;

// State type
type State = {
  demo: {
    keplerGl: {
      map: {
        uiState: { locale: string };
        visState: VisState;
        mapStyle: MapStyle;
      };
    };
    aiAssistant: AiAssistantState;
  };
};

// Localization messages for the AI assistant
const aiAssistantMessages: Record<string, Record<string, string>> = {
  en: {
    'aiAssistant.title': 'AI Assistant',
  },
};

export function CustomAiAssistantPanel() {
  const locale = useSelector(
    (state: State) => state.demo.keplerGl.map?.uiState?.locale || 'en'
  );

  // Combine kepler.gl messages with our AI assistant messages
  const combinedMessages = useMemo(() => {
    return Object.keys(aiAssistantMessages).reduce(
      (acc, language) => ({
        ...acc,
        [language]: {
          ...(aiAssistantMessages[language] || {}),
          ...(keplerGlMessages[language] || {}),
        },
      }),
      {} as Record<string, Record<string, string>>
    );
  }, []);

  const flattenedMessages = useMemo(
    () => flattenMessages(combinedMessages[locale] || combinedMessages['en']),
    [combinedMessages, locale]
  );

  return (
    <IntlProvider locale={locale} messages={flattenedMessages}>
      <StyledAiAssistantPanelContainer className="ai-assistant-manager">
        <StyledAiAssistantPanel>
          <StyledAiAssistantPanelHeader>
            <SidePanelTitle className="ai-assistant-manager-title" title="AI Assistant" />
          </StyledAiAssistantPanelHeader>

          <StyledAiAssistantPanelContent>
            <CustomAiAssistantComponent />
          </StyledAiAssistantPanelContent>
        </StyledAiAssistantPanel>
      </StyledAiAssistantPanelContainer>
    </IntlProvider>
  );
}

export default CustomAiAssistantPanel;
