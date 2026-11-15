// docs/src/components/FunctionExamples.js
import React from 'react';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeFromTest from '@site/src/components/CodeFromTest';
import functionInfo from '@site/../src/main/resources/com/databricks/labs/gbx/function-info.json';

// Tabs by surface, for a function whose function-info entry lists `surfaces` (generated from the function
// manifest). The Python tab shows the Python-level example until a function has its own Python example.
// Scala is the performance tier, not an API, so no Scala tab: the Surfaces tag links to the JAR page.
const SURFACE_TABS = [
  { key: 'session', label: 'SQL',               prop: 'sql',    lang: 'sql',    suffix: '_sql_example' },
  { key: 'uc',      label: 'Unity Catalog SQL', prop: 'ucSql',  lang: 'sql',    suffix: '_uc_sql_example' },
  { key: 'python',  label: 'Python',            prop: 'python', lang: 'python', suffix: '_python_example',
    fallback: 'pythonLight', fallbackSuffix: '_python_light_example' },
];

function surfacesFor(name) {
  const fns = functionInfo.functions || functionInfo;
  const entry = fns[name] || fns['gbx_' + name] || {};
  return entry.surfaces ? new Set(entry.surfaces) : null;
}

// Introduced-in version, e.g. "0.5.1" -> rendered as a "Since v0.5.1" badge so
// readers of the (single, latest) docs know which release first shipped this call.
function sinceFor(name) {
  const fns = functionInfo.functions || functionInfo;
  const entry = fns[name] || fns['gbx_' + name] || {};
  return entry.since || null;
}

export default function FunctionExamples(props) {
  const { name, testFile } = props;
  const surfaces = surfacesFor(name);
  const since = sinceFor(name);
  const tabs = surfaces
    ? SURFACE_TABS.filter((t) => surfaces.has(t.key)).map((t) => {
        const own = Boolean(props[t.prop]);
        return {
          key: t.key,
          label: t.label,
          lang: t.lang,
          code: own ? props[t.prop] : props[t.fallback],
          source: own ? props[t.prop + 'Source'] : props[t.fallback + 'Source'],
          suffix: own || !t.fallback ? t.suffix : t.fallbackSuffix,
        };
      })
      // A surface tab with neither its own example nor a fallback is left out, not shown as "Not available":
      // the function does run on that surface, the page just has no hand-written example for it.
      .filter((t) => Boolean(t.code))
    : [];
  return (
    <>
    {since && (
      <span
        className="gbx-since-badge"
        title={`Introduced in GeoBrix v${since}`}
        style={{
          display: 'inline-block',
          fontSize: '0.75rem',
          fontWeight: 600,
          padding: '0.1rem 0.5rem',
          marginBottom: '0.5rem',
          borderRadius: '0.75rem',
          border: '1px solid var(--ifm-color-emphasis-300)',
          color: 'var(--ifm-color-emphasis-700)',
          background: 'var(--ifm-color-emphasis-100)',
        }}
      >
        Since v{since}
      </span>
    )}
    {tabs.length > 0 && (
    <Tabs groupId="gbx-example-lang" className="gbx-example-lang-tabs">
      {tabs.map((t) => (
        <TabItem key={t.key} value={t.key} label={t.label} default={t.key === tabs[0].key}>
          <CodeFromTest
            language={t.lang}
            code={t.code}
            source={t.source}
            testFile={testFile}
            functionName={name + t.suffix}
            outputConstant={name + t.suffix + '_output'}
          />
        </TabItem>
      ))}
    </Tabs>
    )}
    </>
  );
}
