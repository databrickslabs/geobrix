import { describe, it, expect } from 'vitest';
import { readdirSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

// AppKit's analytics route streams a JSON_ARRAY result as ONE server-sent event, and its
// StreamManager rejects any event over 1 MiB ("Event exceeds max size of 1048576 bytes";
// the cap is not configurable from the plugin). A row-level query whose LIMIT can
// produce a bigger result fails outright instead of returning a truncated set.
// Measured on the vapor-eyes gold tables: the widest row (wells_points) is 221 bytes
// of JSON; budget 240 for headroom.
const MAX_EVENT_BYTES = 1024 * 1024;
const BYTES_PER_ROW_BUDGET = 240;

const QUERIES_DIR = fileURLToPath(new URL('../../../../config/queries/', import.meta.url));

describe('analytics query LIMITs fit one AppKit SSE event', () => {
  const files = readdirSync(QUERIES_DIR).filter((f) => f.endsWith('.sql'));

  it('finds the query files', () => {
    expect(files).toContain('wells_points.sql');
  });

  for (const f of files) {
    const limit = /\bLIMIT\s+(\d+)\s*;?\s*$/im.exec(readFileSync(QUERIES_DIR + f, 'utf8'));
    if (!limit) continue;
    it(`${f}: LIMIT ${limit[1]} x ${BYTES_PER_ROW_BUDGET} B stays under ${MAX_EVENT_BYTES} B`, () => {
      expect(Number(limit[1]) * BYTES_PER_ROW_BUDGET).toBeLessThanOrEqual(MAX_EVENT_BYTES);
    });
  }
});
