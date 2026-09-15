// docs/src/components/FunctionExamples.js
import React from 'react';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeFromTest from '@site/src/components/CodeFromTest';
import functionInfo from '@site/../src/main/resources/com/databricks/labs/gbx/function-info.json';

// Fixed tab order. label = tab text; key = binding label in function-info.json;
// lang = CodeFromTest language; suffix = example-function name suffix.
const TABS = [
  { key: 'sql',          label: 'SQL',            lang: 'sql',    suffix: '_sql_example' },
  { key: 'python-light', label: 'Python (light)', lang: 'python', suffix: '_python_light_example' },
  { key: 'python-heavy', label: 'Python (heavy)', lang: 'python', suffix: '_python_heavy_example' },
  { key: 'scala',        label: 'Scala',          lang: 'scala',  suffix: '_scala_example' },
];

function bindingsFor(name) {
  const fns = functionInfo.functions || functionInfo;
  const entry = fns[name] || fns['gbx_' + name] || {};
  return new Set(entry.bindings || []);
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
  const present = bindingsFor(name);
  const since = sinceFor(name);
  const codeByKey = {
    'sql': props.sql,
    'python-light': props.pythonLight,
    'python-heavy': props.pythonHeavy,
    'scala': props.scala,
  };
  const sourceByKey = {
    'sql': props.sqlSource,
    'python-light': props.pythonLightSource,
    'python-heavy': props.pythonHeavySource,
    'scala': props.scalaSource,
  };
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
    <Tabs groupId="gbx-example-lang" className="gbx-example-lang-tabs">
      {TABS.map((t) => (
        <TabItem key={t.key} value={t.key} label={t.label} default={t.key === 'sql'}>
          {present.has(t.key) && codeByKey[t.key] ? (
            <CodeFromTest
              language={t.lang}
              code={codeByKey[t.key]}
              source={sourceByKey[t.key]}
              testFile={testFile}
              functionName={name + t.suffix}
              outputConstant={name + t.suffix + '_output'}
            />
          ) : (
            <p><em>Not available in this tier.</em></p>
          )}
        </TabItem>
      ))}
    </Tabs>
    </>
  );
}
