import { describe, it, expect } from 'vitest';
import { arrowTableToRows, isArrowTableLike, toKeplerValue } from '../arrowRows';

// Minimal stand-in for an apache-arrow Table: the converter only uses numRows,
// schema.fields[].name and getChild(name).get(i).
function fakeTable(cols: Record<string, unknown[]>) {
  const names = Object.keys(cols);
  return {
    numRows: names.length ? cols[names[0]].length : 0,
    schema: { fields: names.map((name) => ({ name })) },
    getChild: (name: string) => (name in cols ? { get: (i: number) => cols[name][i] } : null),
  };
}

describe('arrowTableToRows', () => {
  it('turns columns into plain row objects keyed by column name', () => {
    const rows = arrowTableToRows(fakeTable({
      longitude: [-103.5, -102.25], latitude: [31.5, 32], record_id: ['a', 'b'],
    }));
    expect(rows).toEqual([
      { longitude: -103.5, latitude: 31.5, record_id: 'a' },
      { longitude: -102.25, latitude: 32, record_id: 'b' },
    ]);
  });
  it('maps BIGINT cells to numbers and keeps nulls', () => {
    const rows = arrowTableToRows(fakeTable({ n: [BigInt(7), null] }));
    expect(rows).toEqual([{ n: 7 }, { n: null }]);
  });
  it('returns no rows for an empty table', () => {
    expect(arrowTableToRows(fakeTable({}))).toEqual([]);
  });
});

describe('toKeplerValue', () => {
  it('coerces a numeric string to a number for a real field (JSON_ARRAY rows)', () => {
    expect(toKeplerValue('real', '-103.74')).toBe(-103.74);
  });
  it('keeps a real number for a real field (ARROW_STREAM rows)', () => {
    expect(toKeplerValue('real', -103.74)).toBe(-103.74);
  });
  it('renders a number as a string for a string field, as JSON_ARRAY delivered it', () => {
    expect(toKeplerValue('string', 1234.5)).toBe('1234.5');
  });
  it('passes strings and nulls through for a string field', () => {
    expect(toKeplerValue('string', 'OXY')).toBe('OXY');
    expect(toKeplerValue('string', null)).toBeNull();
  });
});

describe('isArrowTableLike', () => {
  it('accepts a Table-shaped value and rejects row arrays and null', () => {
    expect(isArrowTableLike(fakeTable({ a: [1] }))).toBe(true);
    expect(isArrowTableLike([{ a: 1 }])).toBe(false);
    expect(isArrowTableLike(null)).toBe(false);
  });
});
