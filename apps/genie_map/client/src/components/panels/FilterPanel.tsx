import { useState } from 'react';
import styled from 'styled-components';
import type { AggregationOperation } from '@shared/types';
import {
  DATASET_LABEL,
  METRIC_1_LABEL,
  METRIC_2_LABEL,
  CATEGORY_FILTER_LABEL,
  CATEGORY_FILTER_OPTIONS,
  GROUP_FILTER_LABEL,
} from '../../config/dataset-config';

interface FilterPanelProps {
  categoryFilter: string;
  groupFilter: string;
  selectedAggregation: AggregationOperation;
  onCategoryFilterChange: (value: string) => void;
  onGroupFilterChange: (value: string) => void;
  onAggregationChange: (aggregation: AggregationOperation) => void;
  onApplyFilters?: () => void;
}

const FilterContainer = styled.div`
  display: flex;
  flex-direction: column;
  gap: 16px;
`;

const FilterGroup = styled.div`
  display: flex;
  flex-direction: column;
  gap: 8px;
`;

const Label = styled.label`
  font-size: 12px;
  font-weight: 500;
  color: #a0a7b4;
  text-transform: uppercase;
  letter-spacing: 0.5px;
`;

const Select = styled.select`
  background: #1a1c23;
  border: 1px solid #3a3f4b;
  border-radius: 4px;
  color: #ffffff;
  padding: 8px 12px;
  font-size: 13px;
  cursor: pointer;
  transition: border-color 0.2s;

  &:hover {
    border-color: #4a5568;
  }

  &:focus {
    outline: none;
    border-color: #3a86ff;
  }

  option {
    background: #1a1c23;
    color: #ffffff;
  }
`;

const Input = styled.input`
  background: #1a1c23;
  border: 1px solid #3a3f4b;
  border-radius: 4px;
  color: #ffffff;
  padding: 8px 12px;
  font-size: 13px;
  transition: border-color 0.2s;

  &::placeholder {
    color: #6b7280;
  }

  &:hover {
    border-color: #4a5568;
  }

  &:focus {
    outline: none;
    border-color: #3a86ff;
  }
`;

const ApplyButton = styled.button`
  background: #3a86ff;
  border: none;
  border-radius: 4px;
  color: #ffffff;
  padding: 10px 16px;
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: background 0.2s;
  margin-top: 8px;

  &:hover {
    background: #2563eb;
  }

  &:active {
    background: #1d4ed8;
  }
`;

const AGGREGATION_OPTIONS: { value: AggregationOperation; label: string }[] = [
  { value: 'COUNT', label: `Total ${DATASET_LABEL}s` },
  { value: 'SUM',   label: `Total ${METRIC_1_LABEL}` },
  { value: 'AVG',   label: `Avg ${METRIC_2_LABEL}` },
  { value: 'MAX',   label: `Max ${METRIC_1_LABEL}` },
  { value: 'MIN',   label: `Min ${METRIC_1_LABEL}` },
];

export function FilterPanel({
  categoryFilter,
  groupFilter,
  selectedAggregation,
  onCategoryFilterChange,
  onGroupFilterChange,
  onAggregationChange,
  onApplyFilters,
}: FilterPanelProps) {
  const [localGroupFilter, setLocalGroupFilter] = useState(groupFilter);

  const handleApply = () => {
    onGroupFilterChange(localGroupFilter);
    onApplyFilters?.();
  };

  return (
    <FilterContainer>
      <FilterGroup>
        <Label>Aggregation</Label>
        <Select
          value={selectedAggregation}
          onChange={(e) => onAggregationChange(e.target.value as AggregationOperation)}
        >
          {AGGREGATION_OPTIONS.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </Select>
      </FilterGroup>

      {CATEGORY_FILTER_LABEL && (
        <FilterGroup>
          <Label>{CATEGORY_FILTER_LABEL}</Label>
          <Select
            value={categoryFilter}
            onChange={(e) => onCategoryFilterChange(e.target.value)}
          >
            {CATEGORY_FILTER_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </Select>
        </FilterGroup>
      )}

      {GROUP_FILTER_LABEL && (
        <FilterGroup>
          <Label>{GROUP_FILTER_LABEL}</Label>
          <Input
            type="text"
            placeholder={`Enter ${GROUP_FILTER_LABEL.toLowerCase()}...`}
            value={localGroupFilter}
            onChange={(e) => setLocalGroupFilter(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') handleApply();
            }}
          />
        </FilterGroup>
      )}

      <ApplyButton onClick={handleApply}>Apply Filters</ApplyButton>
    </FilterContainer>
  );
}
