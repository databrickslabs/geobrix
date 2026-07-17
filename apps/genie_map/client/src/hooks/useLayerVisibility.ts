import { useEffect, useRef } from 'react';
import { useDispatch } from 'react-redux';
import { layerToggleVisibility } from '@kepler.gl/actions';

/**
 * A rule that maps a kepler.gl layer to a zoom-level predicate.
 * The layer will be shown when `activeWhen(zoom)` returns true.
 */
export interface LayerRule {
  layerId: string;
  activeWhen: (zoom: number) => boolean;
}

/**
 * Toggles kepler.gl layer visibility based on zoom level and a set of rules.
 *
 * Dispatches `layerToggleVisibility` only when the active/inactive composition
 * actually changes — i.e. when zoom crosses a tier boundary. Panning or tilting
 * within the same tier produces no dispatches.
 *
 * Extension pattern:
 *   Adding a new layer (A5, polygon, etc.) requires only a new rule entry here.
 *   No changes to this hook or to App.tsx logic beyond the new rule.
 *
 * @example
 * useLayerVisibility(bounds?.zoom_level ?? null, [
 *   { layerId: H3_LAYER_ID,    activeWhen: z => z < POINT_ZOOM_THRESHOLD },
 *   { layerId: POINT_LAYER_ID, activeWhen: z => z >= POINT_ZOOM_THRESHOLD },
 * ]);
 */
export function useLayerVisibility(zoom: number | null, rules: LayerRule[]): void {
  const dispatch = useDispatch();

  // Track the last dispatched zone key to avoid repeat dispatches within the same tier.
  const prevZoneKeyRef = useRef<string | null>(null);

  useEffect(() => {
    if (zoom === null) return;

    // Build a compact string key representing which layers are currently active.
    // e.g. for 2 rules with zoom=4: "10" means rule[0] active, rule[1] inactive.
    const zoneKey = rules.map((r) => (r.activeWhen(zoom) ? '1' : '0')).join('');

    if (zoneKey === prevZoneKeyRef.current) return;
    prevZoneKeyRef.current = zoneKey;

    rules.forEach((rule, i) => {
      dispatch(layerToggleVisibility(rule.layerId, zoneKey[i] === '1'));
    });
    // rules is intentionally excluded from deps — callers should define rules
    // as a stable constant outside the component or inside useMemo.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [zoom, dispatch]);
}
