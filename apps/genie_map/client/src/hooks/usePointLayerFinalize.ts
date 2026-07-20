import { useEffect, useRef } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import {
  layerConfigChange,
  layerToggleVisibility,
  layerVisConfigChange,
  layerVisualChannelConfigChange,
} from '@kepler.gl/actions';
import { keplerGlGetState } from '../store';

/**
 * Point layers are created by kepler's AUTO-CREATE path (a hand-rolled point config never
 * bound lat/lng and rendered nothing). Auto-created layers get a generic label ("Point")
 * and a kepler-generated id, so they miss the id-based rename + zoom-visibility logic used
 * for the H3 layers. This hook reconciles them BY DATASET ID (which we control):
 *
 *  - renames each point layer to its intended label (once), and
 *  - toggles its visibility from the current zoom against a [min,max) band.
 *
 * @param zoom       current map zoom (or null before first viewport)
 * @param pointDefs  one entry per point layer: its dataId (== LayerDef.id), label, and band
 */
export interface PointLayerDef {
  dataId: string;
  label: string;
  zoomMin: number;
  zoomMax: number;
  /** Optional fixed point radius override (kepler's auto-created default is small). */
  radius?: number;
  /** Optional: size points by this numeric field (data-driven radius). */
  sizeField?: string;
  /** [min,max] px radius range when sizeField is set. */
  radiusRange?: [number, number];
  /** Optional: color points by this field (categorical/quantile color channel). */
  colorField?: string;
}

interface KLayer {
  id: string;
  type: string;
  config: { dataId: string; label?: string; isVisible?: boolean };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type KField = { name: string; [k: string]: any };

export function usePointLayerFinalize(zoom: number | null, pointDefs: PointLayerDef[]): void {
  const dispatch = useDispatch();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const layers: KLayer[] = useSelector((s: any) => keplerGlGetState(s)?.map?.visState?.layers ?? []);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const datasets = useSelector((s: any) => keplerGlGetState(s)?.map?.visState?.datasets ?? {});
  const renamedRef = useRef<Set<string>>(new Set());
  const visibleRef = useRef<Map<string, boolean>>(new Map());

  useEffect(() => {
    pointDefs.forEach((def) => {
      const layer = layers.find((l) => l.config?.dataId === def.dataId);
      if (!layer) return;

      // (1) Rename once — auto-created layers default to "Point" — and apply optional
      // fixed radius / data-driven size in the same one-shot step.
      if (!renamedRef.current.has(def.dataId) && layer.config?.label !== def.label) {
        dispatch(layerConfigChange(layer, { label: def.label }));
        if (def.radius != null) {
          dispatch(layerVisConfigChange(layer, { radius: def.radius }));
        }
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const fields: KField[] = (datasets[def.dataId] as any)?.fields ?? [];
        const fieldObj = (name: string) => {
          const idx = fields.findIndex((f) => f.name === name);
          return idx > -1 ? { ...fields[idx], fieldIdx: idx } : null;
        };

        // Data-driven radius: bind the size visual channel to a numeric field so point
        // size encodes magnitude (e.g. plume size = concentration).
        if (def.sizeField) {
          const field = fieldObj(def.sizeField);
          if (field) {
            if (def.radiusRange) {
              dispatch(layerVisConfigChange(layer, { radiusRange: def.radiusRange }));
            }
            dispatch(layerVisualChannelConfigChange(layer, { sizeField: field }, 'size'));
          }
        }

        // Data-driven color: bind the color visual channel to a field (e.g. wells by
        // county) for categorical visual grouping.
        if (def.colorField) {
          const field = fieldObj(def.colorField);
          if (field) {
            dispatch(layerVisualChannelConfigChange(layer, { colorField: field }, 'color'));
          }
        }
        renamedRef.current.add(def.dataId);
      }

      // (2) Zoom-visibility by band. Only dispatch on an actual change.
      if (zoom !== null) {
        const shouldShow = zoom >= def.zoomMin && zoom < def.zoomMax;
        if (visibleRef.current.get(def.dataId) !== shouldShow) {
          visibleRef.current.set(def.dataId, shouldShow);
          if (layer.config?.isVisible !== shouldShow) {
            dispatch(layerToggleVisibility(layer.id, shouldShow));
          }
        }
      }
    });
    // layers identity changes as kepler updates; pointDefs is a stable module constant.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [layers, zoom, dispatch]);
}
