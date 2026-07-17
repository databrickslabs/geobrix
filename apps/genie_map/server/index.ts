import { createApp, server, analytics, genie, serving } from '@databricks/appkit';
import { customRoutes } from './routes/custom-plugin';

// DATABRICKS_APP_PORT is injected by the Databricks Apps runtime in production.
// Fall back to PORT (set to 3000 in .env) for local development.
const PORT = Number(process.env.DATABRICKS_APP_PORT || process.env.PORT) || 3000;
const GENIE_SPACE_ID = process.env.DATABRICKS_GENIE_SPACE_ID;

if (!GENIE_SPACE_ID) {
  console.warn('[server] DATABRICKS_GENIE_SPACE_ID is not set; genie plugin will be disabled');
}

// Create AppKit server with plugins:
// - /api/analytics/query/:query_key      SQL queries from config/queries/
// - /api/genie/default/messages           Databricks AI/BI Genie (SSE)
// - /api/serving/default/invoke|stream    Databricks Model Serving (SDK-authenticated)
// - /api/custom/llm/chat/completions      OpenAI-compat pass-through to AI Gateway
//                                         (required for openassistant's useAssistant)
createApp({
  plugins: [
    // staticPath bypasses AppKit's built-in Vite in dev. Frontend runs on
    // :5173 (proxies /api here); in prod dist/client is served directly.
    server({ port: PORT, staticPath: 'dist/client' }),
    analytics({}),
    ...(GENIE_SPACE_ID ? [genie({ spaces: { default: GENIE_SPACE_ID } })] : []),
    serving({}),
    customRoutes({}),
  ],
}).then(() => {
  console.log(`🚀 AppKit server listening on http://localhost:${PORT}`);
  console.log(`   Analytics:  /api/analytics/query/:key`);
  console.log(`   Serving:    /api/serving/default/invoke|stream`);
  console.log(`   LLM Proxy:  /api/custom/llm/chat/completions`);
  if (GENIE_SPACE_ID) {
    console.log(`   Genie:      /api/genie/default/messages`);
  }
});
