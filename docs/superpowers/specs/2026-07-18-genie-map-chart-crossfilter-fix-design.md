# Genie Map: make chart→map cross-filter selections persist

## Problem

In the Genie Map AI panel, a chart built on a map-backed dataset (e.g. a histogram of
`max_conc_ppmm` on the EMIT-plumes layer) is supposed to cross-filter the map when you
brush-select on it. The chart renders, but **selections don't persist** — they revert as
soon as the pointer leaves the chart, so the map never stays filtered.

## Root cause

The selection path is: echarts brush → `onSelected(datasetName, indices)`
(`echarts-tools.tsx`) → `highlightRows(...)` (`client/src/tools/kepler/utils.ts:107`).

`highlightRows` mutates the kepler dataset **directly** —
`dataset.filteredIndex = selectedRowIndices` — and only dispatches `layerSetIsValid`. It
never writes to kepler's Redux `visState.filters`. But kepler recomputes `filteredIndex`
from `visState.filters` via `KeplerTable.filterTable(filters, layers)` on every normal
state update (hover, mouse-move, re-render). Since no filter exists in state, the next
cycle recomputes `filteredIndex` back to all rows — reverting the selection. Classic
"mutation outside the store gets overwritten."

Confirmed from kepler internals: `kepler-table` documents `filterTable(...)` as the method
that produces `filteredIndex` from `Filter[]`; base-layer data-update triggers key off the
table's `filteredIndex`. Our manual assignment is not durable.

## Fix

Route the chart selection through a real kepler filter so it lives in `visState.filters`
and survives re-render. In the `onSelected` handler (or a rewritten `highlightRows`):

- Maintain a dedicated **cross-filter** on the dataset keyed by a stable id column
  (`plume_id` for plumes, `api` for wells, `geoid` for counties, `hex` for H3 — fall back
  to a synthetic row-index column if none present).
- On selection: map the selected row indices → their id values, then
  `dispatch(createOrUpdateFilter(<stable filter id>, datasetId, <idColumn>, <selectedIds>))`
  (a multi-select filter). Reuse the same filter id per dataset so repeated selections
  update rather than stack.
- On empty selection: clear that filter (remove it, or set an empty/allow-all value) so
  the map returns to showing all rows.
- Remove the direct `dataset.filteredIndex = ...` mutation as the source of truth; let
  kepler's `filterTable` derive it from the dispatched filter.

`createOrUpdateFilter(id?, dataId, field, value)` is the one-shot action for this
(create-or-update semantics avoid managing filter indices by hand).

## Scope

- `client/src/tools/kepler/utils.ts` — `highlightRows` (and its
  `highlightRowsByColumnValues` caller) reworked to dispatch a filter instead of mutating
  `filteredIndex`. This needs a `dispatch` handle — thread it through `getEchartsTools`
  (which already receives `dispatch`) rather than the current `layerSetIsValid` callback.
- `client/src/tools/kepler/echarts-tools.tsx` — `onSelected` wiring updated accordingly.
- Blast radius is contained: `highlightRows` is only used by the echarts `onSelected`
  path (verified — two references, both in this flow).

## Verification

Live only (a brush-interaction/render bug — not unit-testable here; the app has no
jsdom/RTL harness):
1. Deploy to FEVM, open the app, ask *"Chart plume concentration and let me filter the
   map"*.
2. Brush a range on the histogram → the map filters to those plumes **and stays filtered**
   when the pointer leaves the chart.
3. Clear the brush → map returns to all plumes.
4. Confirm a filter appears in kepler's Filters panel for the dataset (evidence it went
   through Redux state, not a transient mutation).

## Risks / open questions

- Kepler multi-select filter on a high-cardinality id column (e.g. 72 plume_ids) should be
  fine at this scale; if a range brush maps more naturally to a *range* filter on the
  charted numeric column, that's an alternative encoding — decide during implementation
  based on what `onSelected` returns (row indices vs. a value range).
- If echarts' brush is itself transient (clears visually on mouseout even with a filter
  set), we may also need to persist the brush region in the chart component; assess after
  the Redux-filter fix, since that may be sufficient on its own.
