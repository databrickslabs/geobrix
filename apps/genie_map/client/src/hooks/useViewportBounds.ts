import { useCallback, useEffect, useMemo, useState } from 'react';
import { useDispatch } from 'react-redux';
import { WebMercatorViewport } from '@math.gl/web-mercator';
import debounce from 'lodash.debounce';
import { setMapBoundary } from '@kepler.gl/ai-assistant';
import type { ViewportBounds } from '@shared/types';

interface UseViewportBoundsResult {
  /** Debounced viewport bounds — null until the first map interaction. */
  bounds: ViewportBounds | null;
  /**
   * Pass this directly to kepler.gl's `onViewStateChange` prop.
   * It projects the WebMercator view state to WGS84 bounds, dispatches
   * `setMapBoundary` for the AI assistant, and schedules a debounced
   * `bounds` state update that drives the analytics queries.
   */
  onViewStateChange: (viewState: {
    width: number;
    height: number;
    longitude: number;
    latitude: number;
    zoom: number;
  }) => void;
}

/**
 * Owns viewport bounds state for the kepler.gl map.
 *
 * Extracts from App.tsx: bounds state, debounce setup/cleanup, projection
 * from WebMercator → WGS84, and the `setMapBoundary` AI-assistant dispatch.
 *
 * @param debounceMs  How long to wait after the last view-state event before
 *                    committing the new bounds to state. Default 600 ms.
 */
export function useViewportBounds(debounceMs = 600): UseViewportBoundsResult {
  const dispatch = useDispatch();
  const [bounds, setBounds] = useState<ViewportBounds | null>(null);

  const debouncedSetBounds = useMemo(
    () => debounce((b: ViewportBounds) => setBounds(b), debounceMs),
    [debounceMs],
  );

  useEffect(() => () => debouncedSetBounds.cancel(), [debouncedSetBounds]);

  const onViewStateChange = useCallback(
    (viewState: {
      width: number;
      height: number;
      longitude: number;
      latitude: number;
      zoom: number;
    }) => {
      const viewport = new WebMercatorViewport(viewState);
      const nw = viewport.unproject([0, 0]) as [number, number];
      const se = viewport.unproject([viewport.width, viewport.height]) as [number, number];

      // Inform the kepler AI assistant of the current map extent.
      dispatch(setMapBoundary(nw, se));

      // Debounced update drives the analytics queries (H3 + points).
      debouncedSetBounds({
        x_min: nw[0],
        x_max: se[0],
        y_min: se[1],
        y_max: nw[1],
        zoom_level: Math.round(viewState.zoom),
      });
    },
    [dispatch, debouncedSetBounds],
  );

  return { bounds, onViewStateChange };
}
