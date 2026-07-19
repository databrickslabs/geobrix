import { describe, it, expect } from 'vitest';
import { findGeometryColumn } from '../genie-tool';

describe('findGeometryColumn', () => {
  it('matches curated ST_ASGEOJSON alias names', () => {
    expect(findGeometryColumn(['operator', 'geojson'])).toBe('geojson');
    expect(findGeometryColumn(['hex_geojson', 'x'])).toBe('hex_geojson');
    expect(findGeometryColumn(['geometry'])).toBe('geometry');
    expect(findGeometryColumn(['geom_geojson'])).toBe('geom_geojson');
    expect(findGeometryColumn(['operator', 'ch4_max'])).toBeUndefined();
  });
});
