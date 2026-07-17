import { useState } from 'react';
import type { AggregationOperation } from '@shared/types';

export interface FilterState {
  aggregation: AggregationOperation;
  categoryFilter: string;
  groupFilter: string;
  setAggregation: (v: AggregationOperation) => void;
  setCategoryFilter: (v: string) => void;
  setGroupFilter: (v: string) => void;
}

/**
 * Owns the filter + aggregation state shared between the data hooks
 * (which push to kepler.gl) and the analytics panel UI.
 *
 * Returned as a single object so it can be passed as one prop to
 * AnalyticsDashboard without spreading many individual values.
 */
export function useFilterState(): FilterState {
  const [aggregation, setAggregation] = useState<AggregationOperation>('COUNT');
  const [categoryFilter, setCategoryFilter] = useState('');
  const [groupFilter, setGroupFilter] = useState('');
  return { aggregation, setAggregation, categoryFilter, setCategoryFilter, groupFilter, setGroupFilter };
}
