import React from 'react';
import CodeBlock from '@theme/CodeBlock';
import ExecutionEnvironment from '@docusaurus/ExecutionEnvironment';

/**
 * CodeFromFile Component
 * 
 * Imports code from test files to ensure single source of truth.
 * 
 * Usage:
 *   <CodeFromFile 
 *     file="/tests/docs/python/setup/essential_bundle.py"
 *     language="python"
 *     title="Essential Bundle Setup"
 *   />
 * 
 * With line ranges:
 *   <CodeFromFile 
 *     file="/tests/docs/python/setup/essential_bundle.py"
 *     lines="10-20"
 *     language="python"
 *     title="Configuration Section"
 *   />
 */
export default function CodeFromFile({ file, language, title, lines, showLineNumbers = false }) {
  const [code, setCode] = React.useState('Loading...');
  const [error, setError] = React.useState(null);
  
  React.useEffect(() => {
    // Convert file path to GitHub raw URL for production
    // For local development, would need to set up a local file server
    const isProduction = typeof window !== 'undefined' && 
                         window.location.hostname !== 'localhost';
    
    let fileUrl;
    if (isProduction) {
      // GitHub raw URL for production
      const branch = 'main'; // or use a specific version tag
      fileUrl = `https://raw.githubusercontent.com/databrickslabs/geobrix/${branch}${file}`;
    } else {
      // For local development, use relative path from docs root
      // This requires the file to be accessible from the docs build
      fileUrl = `${file}`;
    }
    
    fetch(fileUrl)
      .then(res => {
        if (!res.ok) {
          throw new Error(`Failed to load ${file}: ${res.status} ${res.statusText}`);
        }
        return res.text();
      })
      .then(text => {
        if (lines) {
          // Extract specific line range
          const [start, end] = lines.split('-').map(Number);
          const codeLines = text.split('\n');
          const selectedLines = codeLines.slice(start - 1, end);
          setCode(selectedLines.join('\n'));
        } else {
          setCode(text);
        }
        setError(null);
      })
      .catch(err => {
        console.error('CodeFromFile error:', err);
        setError(err.message);
        setCode(`# Error loading code from ${file}\n# ${err.message}\n\n# This code should be imported from: ${file}`);
      });
  }, [file, lines]);

  if (error) {
    return (
      <div style={{ 
        padding: '1rem', 
        backgroundColor: '#fff3cd', 
        border: '1px solid #ffc107',
        borderRadius: '4px',
        marginBottom: '1rem'
      }}>
        <strong>⚠️ Code Import Error:</strong>
        <p>Could not load code from: <code>{file}</code></p>
        <p style={{ fontSize: '0.9em', color: '#666' }}>{error}</p>
        <p style={{ fontSize: '0.85em', marginTop: '0.5rem' }}>
          <strong>For local development:</strong> Copy the file to docs or use a file server.
          <br />
          <strong>For production:</strong> This will load from GitHub automatically.
        </p>
      </div>
    );
  }

  // Show indicator only when user has opted in (default off)
  const [showIndicator, setShowIndicator] = React.useState(false);

  React.useEffect(() => {
    if (ExecutionEnvironment.canUseDOM) {
      const show = localStorage.getItem('showCodeIndicators') === 'true';
      setShowIndicator(show);
    }
  }, []);
  
  const sourceNote = file && showIndicator ? (
    <div style={{ 
      fontSize: '0.85em', 
      color: '#155724', 
      marginBottom: '0.5rem',
      padding: '0.5rem',
      backgroundColor: '#d4edda',
      borderLeft: '3px solid #28a745',
      borderRadius: '4px',
      display: 'flex',
      alignItems: 'center',
      gap: '0.5rem'
    }}>
      <span style={{ fontSize: '1.2em' }}>🔗</span>
      <div style={{ flex: 1 }}>
        <strong>Fully Validated:</strong> <code>{file}</code>
      </div>
    </div>
  ) : null;

  return (
    <>
      {sourceNote}
      <CodeBlock 
        language={language} 
        title={title}
        showLineNumbers={showLineNumbers}
      >
        {code}
      </CodeBlock>
    </>
  );
}
