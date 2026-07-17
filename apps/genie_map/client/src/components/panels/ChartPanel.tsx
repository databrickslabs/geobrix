import { useMemo } from 'react';
import styled from 'styled-components';
import ReactEChartsCore from 'echarts-for-react/lib/core';
import * as echarts from 'echarts/core';
import { BarChart as EChartsBarChart } from 'echarts/charts';
import {
  GridComponent,
  TooltipComponent,
  DatasetComponent,
} from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import { useAnalyticsQuery } from '@databricks/appkit-ui/react';
import { sql } from '@databricks/appkit-ui/js';
import type { ViewportBounds } from '@shared/types';
import { GROUP_FILTER_LABEL, RECORD_LABEL, DATASET_TABLE } from '../../config/dataset-config';

// Register ECharts components
echarts.use([
  EChartsBarChart,
  GridComponent,
  TooltipComponent,
  DatasetComponent,
  CanvasRenderer,
]);

interface ChartPanelProps {
  bounds: ViewportBounds | null;
  country?: string;
  operatorName?: string;
  isLoading: boolean;
}

const ChartContainer = styled.div`
  margin-top: 24px;
`;

const ChartTitle = styled.h3`
  font-size: 13px;
  font-weight: 600;
  color: #ffffff;
  margin: 0 0 16px 0;
`;

const LoadingPlaceholder = styled.div`
  height: 300px;
  background: linear-gradient(90deg, #2a2d38 25%, #3a3f4b 50%, #2a2d38 75%);
  background-size: 200% 100%;
  animation: shimmer 1.5s infinite;
  border-radius: 4px;

  @keyframes shimmer {
    0% {
      background-position: -200% 0;
    }
    100% {
      background-position: 200% 0;
    }
  }
`;

const EmptyState = styled.div`
  height: 200px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #a0a7b4;
  font-size: 13px;
  background: #1a1c23;
  border-radius: 4px;
`;

export function ChartPanel({ bounds, isLoading: externalLoading }: ChartPanelProps) {
  // Memoize query params to prevent unnecessary re-fetches
  const queryParams = useMemo(() => {
    // Suppress query if table isn't configured yet (prevents IDENTIFIER('') crash).
    if (!DATASET_TABLE) return null;
    return ({
    // sql.double() always binds as DOUBLE — correct for WGS84 degrees.
    // sql.number() would bind whole-number coords as INT, producing invalid WKT.
    x_min: sql.double(bounds?.x_min ?? -180),
    x_max: sql.double(bounds?.x_max ?? 180),
    y_min: sql.double(bounds?.y_min ?? -90),
    y_max: sql.double(bounds?.y_max ?? 90),
    table_name: sql.string(DATASET_TABLE),
  })}, [bounds?.x_min, bounds?.x_max, bounds?.y_min, bounds?.y_max]);

  // Use AppKit's useAnalyticsQuery to fetch data from config/queries/chart_groups.sql
  const { data, loading, error } = useAnalyticsQuery('chart_groups', queryParams);

  const chartData = data as Array<{ group_name: string; record_count: number }> | undefined;
  const isLoading = externalLoading || loading;

  const chartOption = useMemo(() => {
    if (!chartData || chartData.length === 0) return null;

    return {
      backgroundColor: 'transparent',
      dataset: {
        source: [
          ['record_count', 'group_name'],
          ...chartData.map((row) => [row.record_count, row.group_name]),
        ],
      },
      grid: {
        left: 16,
        right: 16,
        top: 16,
        bottom: 32,
        containLabel: true,
      },
      tooltip: {
        trigger: 'item' as const,
        backgroundColor: '#1a1c23',
        borderColor: '#3a3f4b',
        textStyle: {
          color: '#ffffff',
        },
        formatter: (params: { value: [number, string] }) => {
          return `${params.value[1]}: ${params.value[0].toLocaleString()} ${RECORD_LABEL}`;
        },
      },
      xAxis: {
        type: 'value' as const,
        axisLabel: {
          color: '#a0a7b4',
          fontSize: 11,
          formatter: (value: number) =>
            value >= 1000
              ? `${(value / 1000).toFixed(value % 1000 === 0 ? 0 : 1)}k`
              : value.toString(),
        },
        axisLine: {
          lineStyle: {
            color: '#3a3f4b',
          },
        },
        splitLine: {
          lineStyle: {
            color: '#2a2d38',
          },
        },
      },
      yAxis: {
        type: 'category' as const,
        inverse: true,
        axisLabel: {
          color: '#a0a7b4',
          fontSize: 11,
          width: 100,
          overflow: 'truncate' as const,
        },
        axisLine: {
          lineStyle: {
            color: '#3a3f4b',
          },
        },
      },
      series: [
        {
          type: 'bar' as const,
          encode: {
            x: 'record_count',
            y: 'group_name',
          },
          itemStyle: {
            borderRadius: [0, 4, 4, 0],
            color: {
              type: 'linear' as const,
              x: 0,
              y: 0,
              x2: 1,
              y2: 0,
              colorStops: [
                { offset: 0, color: '#FFC300' },
                { offset: 1, color: '#FF5733' },
              ],
            },
          },
          barWidth: 20,
        },
      ],
    };
  }, [chartData]);

  if (isLoading) {
    return (
      <ChartContainer>
        <ChartTitle>Top {GROUP_FILTER_LABEL || 'Groups'} by {RECORD_LABEL} Count</ChartTitle>
        <LoadingPlaceholder />
      </ChartContainer>
    );
  }

  if (error) {
    return (
      <ChartContainer>
        <ChartTitle>Top {GROUP_FILTER_LABEL || 'Groups'} by {RECORD_LABEL} Count</ChartTitle>
        <EmptyState>Error loading data: {error}</EmptyState>
      </ChartContainer>
    );
  }

  if (!chartOption) {
    return (
      <ChartContainer>
        <ChartTitle>Top {GROUP_FILTER_LABEL || 'Groups'} by {RECORD_LABEL} Count</ChartTitle>
        <EmptyState>No data available for current viewport</EmptyState>
      </ChartContainer>
    );
  }

  return (
    <ChartContainer>
      <ChartTitle>Top {GROUP_FILTER_LABEL || 'Groups'} by {RECORD_LABEL} Count</ChartTitle>
      <ReactEChartsCore
        echarts={echarts}
        option={chartOption}
        style={{ height: 300 }}
        notMerge={true}
        lazyUpdate={true}
      />
    </ChartContainer>
  );
}
