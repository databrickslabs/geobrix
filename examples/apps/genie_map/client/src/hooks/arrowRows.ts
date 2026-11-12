/**
 * Helpers for analytics results delivered as ARROW_STREAM (an apache-arrow Table)
 * rather than JSON_ARRAY (an array of row objects).
 */

/** The slice of an apache-arrow Table the converter reads. */
export interface ArrowTableLike {
  numRows: number;
  schema: { fields: { name: string }[] };
  getChild(name: string): { get(i: number): unknown } | null;
}

/** Whether a query result is an Arrow Table (ARROW_STREAM) rather than a row array. */
export function isArrowTableLike(v: unknown): v is ArrowTableLike {
  return v !== null && typeof v === 'object' && 'schema' in v && 'numRows' in v
    && typeof (v as ArrowTableLike).getChild === 'function';
}

/**
 * Convert an Arrow Table into plain row objects keyed by column name, the same shape
 * a JSON_ARRAY result has, so the kepler wiring is shared by both formats.
 * BIGINT cells (bigint) become numbers; nulls stay null.
 */
export function arrowTableToRows(table: ArrowTableLike): Record<string, unknown>[] {
  const cols = table.schema.fields.map((f) => [f.name, table.getChild(f.name)] as const);
  const rows: Record<string, unknown>[] = new Array(table.numRows);
  for (let i = 0; i < table.numRows; i++) {
    const row: Record<string, unknown> = {};
    for (const [name, col] of cols) {
      const v = col ? col.get(i) : null;
      row[name] = typeof v === 'bigint' ? Number(v) : (v ?? null);
    }
    rows[i] = row;
  }
  return rows;
}

/**
 * Normalize one cell for a kepler field of type `type`.
 *
 * JSON_ARRAY delivers every cell as a string (e.g. "-103.74"); kepler/deck.gl need
 * real numbers for coordinates, so numeric strings in real/integer fields become
 * numbers. ARROW_STREAM delivers typed values, so a number in a string-typed
 * (tooltip) field is rendered as the string JSON_ARRAY would have carried.
 */
export function toKeplerValue(type: string, v: unknown): unknown {
  if ((type === 'real' || type === 'integer') && typeof v === 'string' && v.trim() !== '') {
    const n = Number(v);
    return Number.isFinite(n) ? n : v;
  }
  if (type === 'string' && typeof v === 'number') return String(v);
  return v;
}
