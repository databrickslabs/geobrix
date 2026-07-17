import type { DatasetConfig } from '@shared/types';
import { vaporEyes } from './vapor-eyes';

export { VAPOR_EYES_TABLES } from './vapor-eyes';

export const DATASETS: Record<string, DatasetConfig> = { 'vapor-eyes': vaporEyes };

export function getActiveDataset(): DatasetConfig {
  const id = (import.meta.env.VITE_ACTIVE_DATASET as string) || 'vapor-eyes';
  const ds = DATASETS[id];
  if (!ds) throw new Error(`Unknown VITE_ACTIVE_DATASET '${id}'. Known: ${Object.keys(DATASETS).join(', ')}`);
  return ds;
}
