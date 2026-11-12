import { describe, it, expect } from 'vitest';
import { readdirSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { DATASETS } from '../datasets';
import { queryFormatFor } from '../../hooks/useLayerData';

// AppKit's analytics route delivers a JSON_ARRAY result as ONE server-sent event, and its
// StreamManager rejects any event over 1 MiB ("Event exceeds max size of 1048576 bytes";
// the cap is not configurable from the plugin). An ARROW_STREAM query instead comes back
// as raw Arrow IPC in the HTTP response body (no SSE event), so the 1 MiB cap does not
// apply; AppKit bounds an INLINE Arrow attachment at 25 MiB and otherwise streams
// EXTERNAL_LINKS chunks.
// Measured on the vapor-eyes gold tables: the widest row (wells_points) is 221 bytes
// of JSON; budget 240 for headroom (Arrow is smaller per row, so the budget is conservative).
const MAX_EVENT_BYTES = 1024 * 1024;
const MAX_ARROW_INLINE_BYTES = 25 * 1024 * 1024;
const BYTES_PER_ROW_BUDGET = 240;
// Wells in view at the default zoom-8 viewport of the full-AOI gold table (measured).
const ZOOM8_WELLS_IN_VIEW = 12242;

const QUERIES_DIR = fileURLToPath(new URL('../../../../config/queries/', import.meta.url));

const layers = Object.values(DATASETS).flatMap((d) => d.layers);
const formatByQuery = new Map(layers.map((l) => [l.queryName, queryFormatFor(l)]));
const limitOf = (file: string) => {
  const m = /\bLIMIT\s+(\d+)\s*;?\s*$/im.exec(readFileSync(QUERIES_DIR + file, 'utf8'));
  return m ? Number(m[1]) : null;
};

describe('point layers fetch through the Arrow path', () => {
  const points = layers.filter((l) => l.kind === 'point');

  it('finds the point layers', () => {
    expect(points.map((l) => l.queryName)).toEqual(
      expect.arrayContaining(['wells_points', 'plume_points']),
    );
  });

  for (const l of points) {
    it(`${l.id} (${l.queryName}) uses ARROW_STREAM`, () => {
      expect(queryFormatFor(l)).toBe('ARROW_STREAM');
    });
  }

  it('wells_points LIMIT covers every well in the default zoom-8 view', () => {
    expect(limitOf('wells_points.sql')).toBeGreaterThanOrEqual(ZOOM8_WELLS_IN_VIEW);
  });
});

describe('analytics query LIMITs fit their delivery path', () => {
  const files = readdirSync(QUERIES_DIR).filter((f) => f.endsWith('.sql'));

  it('finds the query files', () => {
    expect(files).toContain('wells_points.sql');
  });

  for (const f of files) {
    const limit = limitOf(f);
    if (limit == null) continue;
    // A query no layer references (e.g. chart_groups) is fetched with the default JSON_ARRAY.
    const format = formatByQuery.get(f.replace(/\.sql$/, '')) ?? 'JSON_ARRAY';
    const cap = format === 'ARROW_STREAM' ? MAX_ARROW_INLINE_BYTES : MAX_EVENT_BYTES;
    it(`${f} (${format}): LIMIT ${limit} x ${BYTES_PER_ROW_BUDGET} B stays under ${cap} B`, () => {
      expect(limit * BYTES_PER_ROW_BUDGET).toBeLessThanOrEqual(cap);
    });
  }
});
