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

export default function FunctionExamples(props) {
  const { name, testFile } = props;
  const present = bindingsFor(name);
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
  );
}
