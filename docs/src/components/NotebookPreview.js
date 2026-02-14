import React from 'react';
import CodeBlock from '@theme/CodeBlock';

/**
 * Renders a Jupyter notebook (.ipynb JSON) inline in the docs, nbdoc-style:
 * markdown cells as documentation, code cells with syntax highlighting.
 * Optional: show cell outputs if present in the notebook.
 *
 * Usage in MDX:
 *   import notebookRaw from '!!raw-loader!../../tests/python/setup/setup_sample_data.ipynb';
 *   <NotebookPreview notebookJson={notebookRaw} showOutputs={false} />
 */
export default function NotebookPreview({ notebookJson, showOutputs = false }) {
  const raw = typeof notebookJson === 'string' ? notebookJson : notebookJson?.default ?? '';
  if (!raw) {
    return <div className="margin-vert--md">No notebook content.</div>;
  }

  let nb;
  try {
    nb = JSON.parse(raw);
  } catch (e) {
    return (
      <div className="margin-vert--md alert alert--danger">
        Failed to parse notebook JSON: {e.message}
      </div>
    );
  }

  const cells = nb.cells || [];
  const language = nb.metadata?.kernelspec?.language || 'python';

  return (
    <div className="notebook-preview margin-vert--lg" data-notebook-preview>
      {cells.map((cell, idx) => {
        const source = (cell.source || []).join('');
        const trimmed = source.trim();
        if (!trimmed) return null;

        if (cell.cell_type === 'markdown') {
          return (
            <div key={idx} className="notebook-cell notebook-markdown margin-bottom--md">
              <MarkdownLike content={trimmed} />
            </div>
          );
        }

        if (cell.cell_type === 'code') {
          const hasMagic = trimmed.startsWith('%');
          const title = hasMagic && trimmed.startsWith('%pip') ? 'Install dependencies' : null;
          return (
            <div key={idx} className="notebook-cell notebook-code margin-bottom--lg">
              {title && (
                <div className="margin-bottom--sm" style={{ fontWeight: 600, fontSize: '0.9rem' }}>
                  {title}
                </div>
              )}
              <CodeBlock language={language} showLineNumbers={false}>
                {trimmed}
              </CodeBlock>
              {showOutputs && cell.outputs && cell.outputs.length > 0 && (
                <CellOutputs outputs={cell.outputs} />
              )}
            </div>
          );
        }

        return null;
      })}
    </div>
  );
}

/** Simple markdown-like rendering: headings, bold, lists, paragraphs. No extra deps. */
function MarkdownLike({ content }) {
  const lines = content.split('\n');
  const elements = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    const trimmed = line.trim();
    if (trimmed.startsWith('### ')) {
      elements.push(<h4 key={i}>{trimmed.slice(4)}</h4>);
      i++;
      continue;
    }
    if (trimmed.startsWith('## ')) {
      elements.push(<h3 key={i}>{trimmed.slice(3)}</h3>);
      i++;
      continue;
    }
    if (trimmed.startsWith('# ')) {
      elements.push(<h2 key={i}>{trimmed.slice(2)}</h2>);
      i++;
      continue;
    }
    if (trimmed.startsWith('- ') || trimmed.startsWith('* ')) {
      const listItems = [trimmed];
      i++;
      while (i < lines.length && (lines[i].trim().startsWith('- ') || lines[i].trim().startsWith('* '))) {
        listItems.push(lines[i].trim().slice(2));
        i++;
      }
      elements.push(
        <ul key={i}>
          {listItems.map((item, j) => (
            <li key={j}>{inlineMarkdown(item)}</li>
          ))}
        </ul>
      );
      continue;
    }
    if (trimmed === '') {
      i++;
      continue;
    }
    elements.push(
      <p key={i} style={{ marginBottom: '0.5em' }}>
        {inlineMarkdown(trimmed)}
      </p>
    );
    i++;
  }
  return <div className="markdown">{elements}</div>;
}

function inlineMarkdown(text) {
  const parts = [];
  let rest = text;
  let key = 0;
  while (rest.length > 0) {
    const bold = rest.match(/\*\*([^*]+)\*\*/);
    const code = rest.match(/`([^`]+)`/);
    let match = null;
    let type = null;
    if (bold && (!code || bold.index <= code.index)) {
      match = bold;
      type = 'bold';
    } else if (code) {
      match = code;
      type = 'code';
    }
    if (match && type) {
      if (match.index > 0) {
        parts.push(<span key={key++}>{rest.slice(0, match.index)}</span>);
      }
      if (type === 'bold') {
        parts.push(<strong key={key++}>{match[1]}</strong>);
      } else {
        parts.push(<code key={key++}>{match[1]}</code>);
      }
      rest = rest.slice(match.index + match[0].length);
    } else {
      parts.push(<span key={key++}>{rest}</span>);
      break;
    }
  }
  return parts.length === 1 ? parts[0] : <>{parts}</>;
}

function CellOutputs({ outputs }) {
  return (
    <div className="margin-top--sm" style={{ fontSize: '0.9em' }}>
      {outputs.map((out, i) => {
        if (out.output_type === 'stream' && out.name === 'stdout' && out.text) {
          const text = Array.isArray(out.text) ? out.text.join('') : out.text;
          return (
            <pre key={i} style={{ margin: 0, padding: '0.75rem', background: 'var(--ifm-code-background)', borderRadius: 4 }}>
              {text}
            </pre>
          );
        }
        if (out.output_type === 'execute_result' && out.data && out.data['text/plain']) {
          const text = Array.isArray(out.data['text/plain']) ? out.data['text/plain'].join('') : out.data['text/plain'];
          return (
            <pre key={i} style={{ margin: 0, padding: '0.75rem', background: 'var(--ifm-code-background)', borderRadius: 4 }}>
              {text}
            </pre>
          );
        }
        return null;
      })}
    </div>
  );
}
