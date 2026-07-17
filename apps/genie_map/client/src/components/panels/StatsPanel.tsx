import { useMemo } from 'react';
import styled from 'styled-components';
import type { AggregationOperation, H3DataRow, PointDataRow } from '@shared/types';
import { POINT_ZOOM_THRESHOLD } from '../../config/dataset-config';
import { DATASET_LABEL, RECORD_LABEL, METRIC_1_LABEL, METRIC_2_LABEL } from '../../config/dataset-config';

interface StatsPanelProps {
  h3Data: H3DataRow[];
  selectedAggregation: AggregationOperation;
  pointData?: PointDataRow[];
  zoomLevel?: number;
  isLoading?: boolean;
}

const StatsContainer = styled.div`
  margin-bottom: 20px;
`;

const MetricBox = styled.div`
  background: linear-gradient(135deg, #1a1c23 0%, #242730 100%);
  border: 1px solid #3a3f4b;
  border-radius: 8px;
  padding: 20px;
  text-align: center;
`;

const MetricLabel = styled.h4`
  font-size: 12px;
  font-weight: 500;
  color: #a0a7b4;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin: 0 0 8px 0;
`;

const MetricValue = styled.p`
  font-size: 32px;
  font-weight: 700;
  color: #ffffff;
  margin: 0;
  background: linear-gradient(90deg, #FFC300, #FF5733);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
`;

const LoadingValue = styled.div`
  height: 40px;
  width: 120px;
  margin: 0 auto;
  background: linear-gradient(90deg, #2a2d38 25%, #3a3f4b 50%, #2a2d38 75%);
  background-size: 200% 100%;
  animation: shimmer 1.5s infinite;
  border-radius: 4px;

  @keyframes shimmer {
    0% { background-position: -200% 0; }
    100% { background-position: 200% 0; }
  }
`;

const DataPointCount = styled.div`
  font-size: 11px;
  color: #6b7280;
  margin-top: 8px;
`;

const LABEL_MAP: Record<AggregationOperation, string> = {
  COUNT: `Total ${DATASET_LABEL}s`,
  SUM:   `Total ${METRIC_1_LABEL}`,
  AVG:   `Avg ${METRIC_2_LABEL}`,
  MAX:   `Max ${METRIC_1_LABEL}`,
  MIN:   `Min ${METRIC_1_LABEL}`,
};

function computeFromH3(h3Data: H3DataRow[], op: AggregationOperation): number {
  if (h3Data.length === 0) return 0;
  switch (op) {
    case 'COUNT':
    case 'SUM':
      return h3Data.reduce((acc, d) => acc + d.count, 0);
    case 'AVG':
      return h3Data.reduce((acc, d) => acc + d.count, 0) / h3Data.length;
    case 'MAX':
      return Math.max(...h3Data.map((d) => d.count));
    case 'MIN':
      return Math.min(...h3Data.map((d) => d.count));
    default:
      return 0;
  }
}

function computeFromPoints(points: PointDataRow[], op: AggregationOperation): number {
  if (points.length === 0) return 0;
  switch (op) {
    case 'COUNT':
      return points.length;
    case 'SUM':
      return points.reduce((acc, p) => acc + (p.metric_1 ?? 0), 0);
    case 'AVG': {
      const valid = points.filter((p) => p.metric_2 != null);
      return valid.length === 0
        ? 0
        : valid.reduce((acc, p) => acc + (p.metric_2 as number), 0) / valid.length;
    }
    case 'MAX': {
      const values = points.map((p) => p.metric_1).filter((v): v is number => v != null);
      return values.length === 0 ? 0 : Math.max(...values);
    }
    case 'MIN': {
      const values = points.map((p) => p.metric_1).filter((v): v is number => v != null && v > 0);
      return values.length === 0 ? 0 : Math.min(...values);
    }
    default:
      return 0;
  }
}

export function StatsPanel({
  h3Data,
  pointData = [],
  zoomLevel = 0,
  selectedAggregation,
  isLoading = false,
}: StatsPanelProps) {
  const usePointMode = zoomLevel >= POINT_ZOOM_THRESHOLD;

  const computedValue = useMemo(
    () =>
      usePointMode
        ? computeFromPoints(pointData, selectedAggregation)
        : computeFromH3(h3Data, selectedAggregation),
    [usePointMode, pointData, h3Data, selectedAggregation],
  );

  const formattedValue = useMemo(() => {
    if (selectedAggregation === 'AVG') {
      return computedValue.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
    return Math.round(computedValue).toLocaleString();
  }, [computedValue, selectedAggregation]);

  const dataPointLabel = usePointMode
    ? `${pointData.length.toLocaleString()} individual ${RECORD_LABEL} in viewport`
    : `${h3Data.length.toLocaleString()} H3 cells in viewport`;

  return (
    <StatsContainer>
      <MetricBox>
        <MetricLabel>{LABEL_MAP[selectedAggregation]}</MetricLabel>
        {isLoading ? (
          <LoadingValue />
        ) : (
          <>
            <MetricValue>{formattedValue}</MetricValue>
            <DataPointCount>{dataPointLabel}</DataPointCount>
          </>
        )}
      </MetricBox>
    </StatsContainer>
  );
}
