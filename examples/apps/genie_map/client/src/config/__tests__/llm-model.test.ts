import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

// The AI Assistant's proxy calls the AI Gateway route /ai-gateway/mlflow/v1/chat/completions.
// That route only serves Unity Catalog model services, addressed as `system.ai.<model>` (no
// `databricks-` prefix). A legacy serving-endpoint name such as `databricks-gpt-5-2` is
// refused with "'databricks-gpt-5-2' is no longer available. Use Unity Catalog model services."
// The model is configured in three places that must agree: the app.yaml build env, the bundle
// variable that also grants the app EXECUTE on it, and the local env template.
const APP_ROOT = fileURLToPath(new URL('../../../../', import.meta.url));
const read = (file: string) => readFileSync(APP_ROOT + file, 'utf8');
const MODEL_SERVICE = /^system\.ai\.(?!databricks-)[a-z0-9][a-z0-9-]*$/;

const appYamlModel = () => {
  const m = /-\s*name:\s*DATABRICKS_LLM_MODEL\s*\n\s*value:\s*"?([^"\s]+)"?/.exec(read('app.yaml'));
  return m?.[1];
};
const bundleDefault = () => {
  const m = /\n {2}llm_model:\n(?: {4}.*\n)*? {4}default:\s*"?([^"\s]+)"?/.exec(read('databricks.yml'));
  return m?.[1];
};
const envTemplateModel = () => /^DATABRICKS_LLM_MODEL=(\S+)$/m.exec(read('databricks.env.example'))?.[1];

describe('AI Assistant model is a Unity Catalog model service', () => {
  it('app.yaml bakes a system.ai model into the build', () => {
    expect(appYamlModel()).toMatch(MODEL_SERVICE);
  });

  it('the bundle variable defaults to the same system.ai model', () => {
    expect(bundleDefault()).toMatch(MODEL_SERVICE);
    expect(bundleDefault()).toBe(appYamlModel());
  });

  it('the env template names the same system.ai model', () => {
    expect(envTemplateModel()).toMatch(MODEL_SERVICE);
    expect(envTemplateModel()).toBe(appYamlModel());
  });

  it('the app is granted EXECUTE on the model service, not CAN_QUERY on an endpoint', () => {
    const yml = read('databricks.yml');
    expect(yml).not.toMatch(/\bserving_endpoint:/);
    expect(yml).toMatch(
      /uc_securable:\s*\n\s*securable_type:\s*MODEL_SERVICE\s*\n\s*securable_full_name:\s*\$\{var\.llm_model\}\s*\n\s*permission:\s*EXECUTE/,
    );
  });

  it('rejects a legacy databricks- endpoint name', () => {
    expect('databricks-gpt-5-2').not.toMatch(MODEL_SERVICE);
    expect('system.ai.databricks-gpt-5-2').not.toMatch(MODEL_SERVICE);
    expect('system.ai.gpt-5-2').toMatch(MODEL_SERVICE);
  });
});
