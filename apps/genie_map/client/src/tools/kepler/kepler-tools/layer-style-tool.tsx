// SPDX-License-Identifier: MIT
// Copyright contributors to the kepler.gl project

import {useEffect} from 'react';
import {extendedTool} from '@openassistant/utils';
import {layerVisualChannelConfigChange} from '@kepler.gl/actions';
import {Layer, LayerBaseConfig} from '@kepler.gl/layers';
import {LayerVisConfig} from '@kepler.gl/types';
import {z} from 'zod';
import {useDispatch} from 'react-redux';

function UpdateLayerColorToolComponent({layer, newConfig, channel, newVisConfig}: {
  layer: Layer;
  newConfig: Partial<LayerBaseConfig>;
  channel: string;
  newVisConfig: Partial<LayerVisConfig>;
}) {
  const dispatch = useDispatch();

  useEffect(() => {
    dispatch(layerVisualChannelConfigChange(layer, newConfig, channel, newVisConfig));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return null;
}

/**
 * Update the color of a layer
 * NOTE: this tool should be updated to updateLayerStyle including color, size, opacity, etc.
 */
export const updateLayerColor = extendedTool({
  description: 'Update the color of a layer',
  parameters: z.object({
    layerId: z.string(),
    numberOfColors: z.number(),
    customColors: z
      .array(z.string())
      .describe(
        'An array of hex color values. Please try to generate colors from user description like: van gogh starry night, water color etc.'
      )
  }),
  execute: executeUpdateLayerColor,
  context: {
    getLayers: () => {
      throw new Error('getLayers() not implemented.');
    },
    layerVisualChannelConfigChange: () => {
      throw new Error('layerVisualChannelConfigChange() not implemented.');
    }
  },
  component: UpdateLayerColorToolComponent
});

type UpdateLayerColorArgs = {
  layerId: string;
  numberOfColors: number;
  customColors: string[];
};

function isUpdateLayerColorArgs(args: unknown): args is UpdateLayerColorArgs {
  return (
    typeof args === 'object' &&
    args !== null &&
    typeof (args as UpdateLayerColorArgs).layerId === 'string' &&
    typeof (args as UpdateLayerColorArgs).numberOfColors === 'number' &&
    Array.isArray((args as UpdateLayerColorArgs).customColors) &&
    (args as UpdateLayerColorArgs).customColors.every(color => typeof color === 'string')
  );
}

type UpdateLayerColorFunctionContext = {
  getLayers: () => Layer[];
};

function isUpdateLayerColorFunctionContext(
  context: unknown
): context is UpdateLayerColorFunctionContext {
  return typeof context === 'object' && context !== null && typeof (context as UpdateLayerColorFunctionContext).getLayers === 'function';
}

type ExecuteUpdateLayerColorResult = {
  llmResult: {
    success: boolean;
    details?: string;
    error?: string;
    instruction?: string;
  };
  additionalData?: {
    layerId: string;
    layer: Layer;
    newConfig: Partial<LayerBaseConfig>;
    channel: string;
    newVisConfig: Partial<LayerVisConfig>;
  };
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function executeUpdateLayerColor(args: any, options: any): Promise<ExecuteUpdateLayerColorResult> {
  try {
    if (!isUpdateLayerColorArgs(args)) {
      throw new Error('Invalid updateLayerColor arguments');
    }
    if (!isUpdateLayerColorFunctionContext(options.context)) {
      throw new Error('Invalid updateLayerColor function context');
    }

    const {layerId, numberOfColors, customColors} = args;
    const {getLayers} = options.context;

    // get layer from visState by layerId
    const layers = getLayers();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const layer = layers.find((l: any) => l.id === layerId);
    if (!layer) {
      throw new Error(`Layer with id ${layerId} not found`);
    }

    // verify numberOfColors is equal to customColors.length
    if (numberOfColors !== customColors.length) {
      throw new Error(`customColors array must contain exactly ${numberOfColors} colors`);
    }

    const channel = 'color';

    const newConfig = {
      // colorScale: 'custom'
    } as Partial<LayerBaseConfig>;

    const oldColorRange = layer.config.visConfig.colorRange;
    const newColorRange = {
      ...oldColorRange,
      colors: customColors,
      ...(oldColorRange.colorMap
        ? {
            colorMap: [...oldColorRange.colorMap.map((c: [unknown, string], i: number) => [c[0], customColors[i]])]
          }
        : {})
    };

    const newVisConfig = {
      colorRange: newColorRange,
      strokeColorRange: newColorRange
    };

    return {
      llmResult: {
        success: true,
        details: `Color updated successfully to ${customColors.join(', ')} for layer ${layerId}`
      },
      additionalData: {
        layerId,
        layer,
        newConfig,
        channel,
        newVisConfig
      }
    };
  } catch (error) {
    return {
      llmResult: {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
        instruction: `Try to fix the error. If the error persists, pause the execution and ask the user to try with different prompt and context.`
      }
    };
  }
}
