import { StatsPanel } from './StatsPanel';
import { FilterPanel } from './FilterPanel';
import { ChartPanel } from './ChartPanel';
import { POINT_ZOOM_THRESHOLD } from '../../config/dataset-config';
import type { FilterState } from '../../hooks/useFilterState';
import type { H3DataRow, PointDataRow, ViewportBounds } from '@shared/types';

interface AnalyticsDashboardProps {
  bounds: ViewportBounds | null;
  h3Data: H3DataRow[];
  h3Loading: boolean;
  pointData: PointDataRow[];
  pointsLoading: boolean;
  filterState: FilterState;
}

/**
 * Pure UI panel — renders StatsPanel, FilterPanel, and ChartPanel.
 *
 * All data fetching and kepler.gl push happens in App.tsx via useH3AggregationData
 * and usePointData (which must always be mounted). This component only handles
 * display and passes filter changes back via filterState setters.
 */
export function AnalyticsDashboard({
  bounds,
  h3Data,
  h3Loading,
  pointData,
  pointsLoading,
  filterState,
}: AnalyticsDashboardProps) {
  const { aggregation, setAggregation, categoryFilter, setCategoryFilter, groupFilter, setGroupFilter } = filterState;
  const isPointsMode = bounds !== null && bounds.zoom_level >= POINT_ZOOM_THRESHOLD;

  return (
    <>
      <StatsPanel
        h3Data={h3Data}
        pointData={pointData}
        zoomLevel={bounds?.zoom_level ?? 0}
        selectedAggregation={aggregation}
        isLoading={isPointsMode ? pointsLoading : h3Loading}
      />
      <FilterPanel
        categoryFilter={categoryFilter}
        groupFilter={groupFilter}
        selectedAggregation={aggregation}
        onCategoryFilterChange={setCategoryFilter}
        onGroupFilterChange={setGroupFilter}
        onAggregationChange={setAggregation}
      />
      <ChartPanel bounds={bounds} isLoading={false} />
    </>
  );
}
