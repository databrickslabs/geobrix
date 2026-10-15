/**
 * Custom Routes Plugin
 *
 * Thin OpenAI-compatible pass-through to the Databricks AI Gateway.
 * Required because openassistant's <AiAssistant> / useAssistant expects an
 * OpenAI-compatible streaming + tool-calling endpoint at a relative URL.
 *
 * Auth is obtained from AppKit's workspace client (reads DATABRICKS_HOST +
 * DATABRICKS_TOKEN via the Databricks SDK — no manual env reads here).
 *
 * Route mounted under /api/custom:
 * - POST /llm/chat/completions  → ${DATABRICKS_HOST}/ai-gateway/mlflow/v1/chat/completions
 *
 * Genie:   handled by the AppKit `genie()` plugin  (/api/genie/:alias/messages)
 * Serving: handled by the AppKit `serving()` plugin (/api/serving/:alias/invoke|stream)
 */
import type express from 'express';
import type { IAppRouter, PluginManifest } from '@databricks/appkit';
import { Plugin, toPlugin, getExecutionContext } from '@databricks/appkit';

const customRoutesManifest: PluginManifest<'custom'> = {
  name: 'custom',
  displayName: 'Custom Routes',
  description: 'OpenAI-compatible pass-through to the Databricks AI Gateway',
  resources: { required: [], optional: [] },
};

export interface CustomRoutesConfig {
  [key: string]: unknown;
}

class CustomRoutesPlugin extends Plugin<CustomRoutesConfig> {
  static manifest = customRoutesManifest;

  injectRoutes(router: IAppRouter) {
    this.route(router, {
      name: 'chatCompletions',
      method: 'post',
      path: '/llm/chat/completions',
      handler: async (req: express.Request, res: express.Response) => {
        try {
          const { client } = getExecutionContext();
          const upstreamHeaders = new Headers({ 'Content-Type': 'application/json' });
          await client.config.authenticate(upstreamHeaders);

          const host = client.config.host?.replace(/\/$/, '');
          const upstream = `${host}/ai-gateway/mlflow/v1/chat/completions`;

          const response = await fetch(upstream, {
            method: 'POST',
            headers: upstreamHeaders,
            body: JSON.stringify(req.body),
          });

          if (!response.ok) {
            const error = await response.text();
            console.error('[LLM Proxy] AI Gateway error:', response.status, error);
            res.status(response.status).json({ error: { message: error, type: 'api_error' } });
            return;
          }

          if (req.body.stream === true) {
            res.setHeader('Content-Type', 'text/event-stream');
            res.setHeader('Cache-Control', 'no-cache');
            res.setHeader('Connection', 'keep-alive');
            const reader = response.body!.getReader();
            const decoder = new TextDecoder();
            try {
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                res.write(decoder.decode(value, { stream: true }));
              }
            } finally {
              reader.releaseLock();
              res.end();
            }
            return;
          }

          res.json(await response.json());
        } catch (error) {
          console.error('[LLM Proxy]', error);
          res.status(500).json({
            error: {
              message: error instanceof Error ? error.message : 'Unknown error',
              type: 'internal_error',
            },
          });
        }
      },
    });
  }
}

export const customRoutes = toPlugin(CustomRoutesPlugin);
