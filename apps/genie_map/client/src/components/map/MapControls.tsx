import styled from 'styled-components';

interface ControlButtonProps {
  isActive: boolean;
  onClick: () => void;
}

const ControlButton = styled.button<{ $isActive: boolean }>`
  width: 36px;
  height: 36px;
  background: ${(props) => (props.$isActive ? '#3a86ff' : '#242730')};
  border: 1px solid #3a3f4b;
  border-radius: 4px;
  color: ${(props) => (props.$isActive ? '#ffffff' : '#a0a7b4')};
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s ease;

  &:hover {
    background: ${(props) => (props.$isActive ? '#3a86ff' : '#3a3f4b')};
    color: #ffffff;
  }

  svg {
    width: 20px;
    height: 20px;
  }
`;

export function ChartControlButton({ isActive, onClick }: ControlButtonProps) {
  return (
    <ControlButton $isActive={isActive} onClick={onClick} title="Toggle Charts Panel">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <path d="M18 20V10" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M12 20V4" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M6 20v-6" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    </ControlButton>
  );
}

export function AiControlButton({ isActive, onClick }: ControlButtonProps) {
  return (
    <ControlButton $isActive={isActive} onClick={onClick} title="Toggle AI Assistant">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <path
          d="M12 2L15.09 8.26L22 9.27L17 14.14L18.18 21.02L12 17.77L5.82 21.02L7 14.14L2 9.27L8.91 8.26L12 2Z"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </ControlButton>
  );
}
