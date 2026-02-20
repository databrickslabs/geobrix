import React from 'react';
import CodeBlock from '@theme/CodeBlock';
import ExecutionEnvironment from '@docusaurus/ExecutionEnvironment';

/**
 * CodeFromTest Component - Single Source of Truth for Documentation Code
 * 
 * This component displays code imported from test files, ensuring that
 * documentation always shows tested, working code.
 * 
 * VALIDATION LEVELS (auto-detected):
 * - "integration" (🧪): Code from docs/tests/.../integration/ → Integration Example
 * - "tested" (green 🔗): Has source/testFile/functionName → fully validated
 * - "databricks" (blue ⚡): Code from tests-dbr/ → requires Databricks Runtime
 * - "compile" (gray 🔗): Explicitly set for compile-only validation
 * - "static" (gray 📄): No test props → reference snippets only
 *
 * Code indicators are hidden by default; users opt in via the bottom-right toggle.
 * 
 * Usage with raw-loader (recommended):
 *   import accessorCode from '!!raw-loader!../../../tests/python/rasterx/accessor_functions.py';
 * 
 *   <CodeFromTest 
 *     language="python"
 *     title="Get Raster Dimensions"
 *     source="docs/tests/python/rasterx/accessor_functions.py"
 *     testFile="docs/tests/python/rasterx/test_accessor_functions.py"
 *   >
 *     {accessorCode}
 *   </CodeFromTest>
 *   // → Auto-detected as "tested" (green)
 * 
 * With function extraction (shows specific function):
 *   <CodeFromTest 
 *     language="python"
 *     title="Get Raster Dimensions"
 *     source="docs/tests/python/rasterx/accessor_functions.py"
 *     testFile="docs/tests/python/rasterx/test_accessor_functions.py"
 *     functionName="get_raster_dimensions"
 *   >
 *     {accessorCode}
 *   </CodeFromTest>
 *   // → Auto-detected as "tested" (green)
 * 
 * Static reference code (no validation):
 *   <CodeFromTest 
 *     language="python"
 *     title="Configuration Example"
 *   >
 *     {`spark.conf.set("key", "value")`}
 *   </CodeFromTest>
 *   // → Auto-detected as "static" (gray)
 * 
 * Explicit validation level override:
 *   <CodeFromTest 
 *     language="scala"
 *     title="Compile Validated"
 *     validationLevel="compile"
 *   >
 *     {`val df = spark.read.format("gdal").load("...")`}
 *   </CodeFromTest>
 * 
 * Example output (show .show() results in docs): use outputConstant with a
 * constant in the same file (e.g. bng_aswkb_example_output = \"\"\"...\"\"\")
 * to render an "Example output" block below the code.
 *
 * Snippet display: For Python functions (def ...), only the runnable body is shown:
 * the def line, docstring, and trailing return are stripped so the example reads
 * as copy-paste code, not a wrapper.
 */
/**
 * Convert a Python function (def ...) to snippet-only: strip def line, docstring,
 * and trailing return(s), then dedent. So docs show the runnable example, not the wrapper.
 */
function pythonFunctionToSnippet(fullFunction) {
  if (!fullFunction || typeof fullFunction !== 'string') return fullFunction;
  const lines = fullFunction.split('\n');
  if (!lines[0].trim().startsWith('def ')) return fullFunction;

  let i = 1; // skip def line

  // Skip docstring (""" or ''' at start of body)
  while (i < lines.length && lines[i].trim() === '') i++;
  if (i < lines.length) {
    const trimmed = lines[i].trim();
    const quote = trimmed.startsWith('"""') ? '"""' : trimmed.startsWith("'''") ? "'''" : null;
    if (quote) {
      i++; // skip opening line
      // If closing quote not on same line, skip until we find it
      if (trimmed.indexOf(quote, quote.length) === -1) {
        while (i < lines.length) {
          if (lines[i].includes(quote)) { i++; break; }
          i++;
        }
      }
      while (i < lines.length && lines[i].trim() === '') i++;
    }
  }

  const bodyLines = lines.slice(i);
  if (bodyLines.length === 0) return fullFunction;

  // Remove from the end: blank lines, comment-only lines, then return statement(s)
  let end = bodyLines.length;
  while (end > 0) {
    const line = bodyLines[end - 1].trim();
    const isBlank = line === '';
    const isCommentOnly = line.startsWith('#');
    // Match "return", "return x", "return (x)", "return x  # comment", etc.
    const isReturn = line === 'return' || /^return\s/.test(line);
    if (isBlank || isCommentOnly || isReturn) {
      end--;
    } else break;
  }
  const withoutReturns = bodyLines.slice(0, end);

  // Dedent: use minimum non-zero indent
  const nonEmpty = withoutReturns.filter(l => l.trim() !== '');
  if (nonEmpty.length === 0) return withoutReturns.join('\n').trim();
  const minIndent = Math.min(...nonEmpty.map(l => l.match(/^\s*/)[0].length));
  const dedented = withoutReturns.map(l => l.length >= minIndent ? l.slice(minIndent) : l);

  return dedented.join('\n').trim();
}

/**
 * When a Python function is used only to hold SQL (or other) snippet content
 * (def name(): ... return """content"""), extract just the content so the
 * doc shows the snippet, not the wrapper.
 */
function pythonFunctionReturnValueToSnippet(fullFunction) {
  if (!fullFunction || typeof fullFunction !== 'string') return fullFunction;
  const normalized = fullFunction.replace(/\r\n/g, '\n').trim();
  const lines = normalized.split('\n');
  if (!lines[0].trim().startsWith('def ')) return fullFunction;

  // Regex fallback: return """...""" or return '''...''' (handles multi-line)
  const doubleMatch = normalized.match(/return\s+"""([\s\S]*?)"""/);
  if (doubleMatch && doubleMatch[1]) return doubleMatch[1].trim();
  const singleMatch = normalized.match(/return\s+'''([\s\S]*?)'''/);
  if (singleMatch && singleMatch[1]) return singleMatch[1].trim();

  let i = 1;
  // Skip blank and docstring
  while (i < lines.length && lines[i].trim() === '') i++;
  if (i < lines.length) {
    const trimmed = lines[i].trim();
    const quote = trimmed.startsWith('"""') ? '"""' : trimmed.startsWith("'''") ? "'''" : null;
    if (quote) {
      i++;
      if (trimmed.indexOf(quote, quote.length) === -1) {
        while (i < lines.length) {
          if (lines[i].includes(quote)) { i++; break; }
          i++;
        }
      }
      while (i < lines.length && lines[i].trim() === '') i++;
    }
  }
  // After docstring: expect return """ or return '''
  if (i >= lines.length) return fullFunction;
  const returnLine = lines[i].trim();
  const quoteType = returnLine.includes('"""') ? '"""' : returnLine.includes("'''") ? "'''" : null;
  if (!quoteType || !returnLine.startsWith('return ')) return fullFunction;

  // Single-line return """..."""
  const onSameLine = returnLine.slice(returnLine.indexOf(quoteType) + 3);
  const closeIdx = onSameLine.indexOf(quoteType);
  if (closeIdx !== -1) {
    return onSameLine.slice(0, closeIdx).trim();
  }
  // Multi-line: collect until closing """
  const contentLines = [];
  i++;
  while (i < lines.length) {
    if (lines[i].includes(quoteType)) break;
    contentLines.push(lines[i]);
    i++;
  }
  return contentLines.join('\n').trim();
}

/**
 * Extract SQL payload from Python that uses spark.sql("""...""").
 * Returns the string content inside the first spark.sql(""" or spark.sql('''.
 */
function pythonFunctionSparkSqlToSnippet(fullFunction) {
  if (!fullFunction || typeof fullFunction !== 'string') return '';
  const body = fullFunction.replace(/\r\n/g, '\n');
  const sqlDouble = body.match(/spark\.sql\s*\(\s*"""([\s\S]*?)"""\s*\)/);
  if (sqlDouble && sqlDouble[1]) return sqlDouble[1].trim();
  const sqlSingle = body.match(/spark\.sql\s*\(\s*'''([\s\S]*?)'''\s*\)/);
  if (sqlSingle && sqlSingle[1]) return sqlSingle[1].trim();
  return '';
}

/**
 * Extract a specific function or constant from Python/Scala code
 */
function extractFunction(code, functionName) {
  if (!functionName || typeof code !== 'string') {
    return code;
  }

  const lines = code.split('\n');

  // Try to find a Python function definition
  const defIndex = lines.findIndex(line =>
    line.trim().startsWith(`def ${functionName}(`)
  );
  // Try to find a Python constant/variable (for SQL blocks, etc.)
  const constantIndex = lines.findIndex(line =>
    line.trim().startsWith(`${functionName} =`)
  );
  // Try to find a Scala val constant
  const scalaIndex = lines.findIndex(line =>
    line.trim().startsWith(`val ${functionName}:`)
  );

  let functionStart = defIndex >= 0 ? defIndex : (constantIndex >= 0 ? constantIndex : scalaIndex);
  const isPythonDef = defIndex >= 0 && functionStart === defIndex;
  const isConstant = constantIndex >= 0 && functionStart === constantIndex;
  const isScalaVal = scalaIndex >= 0 && functionStart === scalaIndex;

  if (functionStart === -1) {
    console.warn(`Function or constant ${functionName} not found in code`);
    return code;
  }

  const firstLine = lines[functionStart];
  const secondLine = functionStart + 1 < lines.length ? lines[functionStart + 1] : '';
  const hasTripleQuote = (firstLine.includes('"""') || firstLine.includes("'''") ||
    secondLine.includes('"""') || secondLine.includes("'''"));

  // Only use triple-quote extraction for constants (CONSTANT = """ or val X: String = """)
  // Python def with docstring must use the regular function-body extraction
  if (hasTripleQuote && (isConstant || isScalaVal)) {
    const quoteType = (firstLine.includes('"""') || secondLine.includes('"""')) ? '"""' : "'''";

    // Find the starting quote (might be on first or second line)
    let quoteStartLine = functionStart;
    if (!firstLine.includes(quoteType) && secondLine.includes(quoteType)) {
      quoteStartLine = functionStart + 1;
    }

    // Find the closing triple quote
    let functionEnd = quoteStartLine + 1;
    for (let i = quoteStartLine + 1; i < lines.length; i++) {
      functionEnd = i + 1;
      if (lines[i].includes(quoteType) && i > quoteStartLine) {
        break;
      }
    }

    // Extract just the content inside the quotes
    const constantContent = lines.slice(functionStart, functionEnd).join('\n');

    // Try Python pattern: CONSTANT = """content"""
    let match = constantContent.match(new RegExp(`${functionName}\\s*=\\s*${quoteType}([\\s\\S]*?)${quoteType}`));

    // Try Scala pattern: val CONSTANT: String = """content"""
    if (!match) {
      match = constantContent.match(new RegExp(`val\\s+${functionName}\\s*:\\s*String\\s*=\\s*${quoteType}([\\s\\S]*?)${quoteType}`));
    }

    // Try Scala pattern with newline after equals: val CONSTANT: String =\n  """content"""
    if (!match) {
      match = constantContent.match(new RegExp(`val\\s+${functionName}\\s*:\\s*String\\s*=\\s*\\n\\s*${quoteType}([\\s\\S]*?)${quoteType}`));
    }

    // Fallback: line-by-line for Scala val with """ on next line (handles """.trim etc.)
    if (!match && isScalaVal) {
      let start = -1;
      let end = -1;
      for (let i = functionStart; i < lines.length; i++) {
        if (lines[i].includes(quoteType)) {
          if (start < 0) {
            start = i + 1; // content starts after opening """ line
          } else {
            end = i; // closing """ line; content is between start and end (exclusive of end)
            break;
          }
        }
      }
      if (start >= 0 && end >= 0) {
        const content = lines.slice(start, end).join('\n').trim();
        if (content.length > 0 && content.length < 10000) return content;
      }
    }

    return match ? match[1].trim() : constantContent;
  }

  // For regular functions, find the end (stop at next top-level def or name = constant)
  let functionEnd = functionStart + 1;
  const baseIndent = lines[functionStart].match(/^\s*/)[0].length;

  for (let i = functionStart + 1; i < lines.length; i++) {
    const line = lines[i];
    // Empty lines are part of the function
    if (line.trim() === '') {
      functionEnd = i + 1;
      continue;
    }
    const indent = line.match(/^\s*/)[0].length;
    const trimmed = line.trim();
    // Next function, or top-level constant (CONSTANT = or name_output =), not in-body vars like "df = "
    const isNextDef = trimmed.startsWith('def ');
    const isTopLevelConstant = /^[A-Z_][A-Z0-9_]*\s*=/.test(trimmed) || /^[a-z][a-z0-9_]*_output\s*=/.test(trimmed);
    if (indent <= baseIndent && (isNextDef || isTopLevelConstant)) {
      break;
    }
    functionEnd = i + 1;
  }

  return lines.slice(functionStart, functionEnd).join('\n');
}

export default function CodeFromTest({
  children,
  code: codeProp,
  language = 'python',
  title,
  source,
  testFile,
  lines,
  functionName,
  showLineNumbers = false,
  validationLevel,
  outputConstant,
  output
}) {
  // Accept code from either 'code' prop or 'children'
  let code = codeProp ?? children;

  // Some bundlers (e.g. MDX) pass module as { default: string }; normalize to string
  if (code != null && typeof code === 'object' && typeof code.default === 'string') {
    code = code.default;
  }
  if (typeof code !== 'string') {
    code = code != null ? String(code) : '';
  }
  // Normalize so extraction works: single line endings, no BOM
  if (typeof code === 'string') {
    code = code.replace(/\r\n/g, '\n').replace(/^\uFEFF/, '').trim();
  }

  // Auto-detect validation level if not explicitly set
  let effectiveValidationLevel = validationLevel;
  if (!effectiveValidationLevel) {
    // If source/testFile is from integration test folder → Integration Example
    if ((source && source.includes('integration/')) || (testFile && testFile.includes('integration/'))) {
      effectiveValidationLevel = "integration";
    }
    // If source/testFile is from tests-dbr/ → databricks validation level
    else if ((source && source.includes('tests-dbr/')) || (testFile && testFile.includes('tests-dbr/'))) {
      effectiveValidationLevel = "databricks";
    }
    // If we have source, testFile, or functionName → this is tested code
    else if (source || testFile || functionName) {
      effectiveValidationLevel = "tested";
    } else {
      // Otherwise, it's static reference code
      effectiveValidationLevel = "static";
    }
  }

  // Extract example output from same file (use full code before function extraction)
  const fullCode = code;
  let outputContent = (typeof output === 'string' && output.trim()) ? output : null;
  if (!outputContent && outputConstant && typeof fullCode === 'string') {
    const extracted = extractFunction(fullCode, outputConstant);
    // Only use if it looks like output (not the whole file when constant was missing)
    if (extracted && extracted !== fullCode && extracted.length < 10000) {
      outputContent = extracted;
    }
  }

  // If functionName specified, extract that function
  if (functionName && typeof code === 'string') {
    code = extractFunction(code, functionName);
    // For Python functions: show only the runnable snippet, not the wrapper
    if (code && code.trimStart().startsWith('def ')) {
      if (language === 'python') {
        code = pythonFunctionToSnippet(code);
      } else if (language === 'sql') {
        // SQL payload only: try return """...""" first, then spark.sql("""...""")
        let sqlPayload = pythonFunctionReturnValueToSnippet(code);
        if (!sqlPayload || sqlPayload.includes('spark.') || sqlPayload.includes('df.') || sqlPayload.trimStart().startsWith('def ')) {
          const fromSparkSql = pythonFunctionSparkSqlToSnippet(code);
          if (fromSparkSql) sqlPayload = fromSparkSql;
        }
        // Final fallback: direct regex for return """...""" in case helper returned full function
        if (!sqlPayload || sqlPayload.trimStart().startsWith('def ')) {
          const m = code.match(/return\s+"""([\s\S]*?)"""/);
          if (m && m[1]) sqlPayload = m[1].trim();
        }
        if (sqlPayload) code = sqlPayload;
      }
    }
  }

  // Ensure code is always a string for CodeBlock
  if (typeof code !== 'string') code = '';

  // Line continuation: raw-loader gives "\\" in file; display as "\" so copy-paste is correct (Python, bash)
  if (typeof code === 'string' && (language === 'python' || language === 'bash')) {
    code = code.replace(/(\s*)\\\\\s*$/gm, '$1\\');
  }

  // If lines specified, extract those lines
  if (lines && typeof code === 'string') {
    const [start, end] = lines.split('-').map(Number);
    const codeLines = code.split('\n');
    code = codeLines.slice(start - 1, end).join('\n');
  }

  // Show indicator only when user has opted in (default off); use showCodeIndicators so default is unambiguous
  const [showIndicator, setShowIndicator] = React.useState(false);

  React.useEffect(() => {
    if (ExecutionEnvironment.canUseDOM) {
      const show = localStorage.getItem('showCodeIndicators') === 'true';
      setShowIndicator(show);
    }
  }, []);

  // Determine indicator styling based on validation level
  const getIndicatorConfig = () => {
    switch (effectiveValidationLevel) {
      case "integration":
        // Integration test folder - distinct label (replaces Static/Compile/Fully Validated for this source)
        return {
          icon: '🧪',
          label: 'Integration Example',
          color: '#856404',
          bgColor: '#fff3cd',
          borderColor: '#ffc107'
        };
      case "tested":
        // Fully validated - green
        return {
          icon: '🔗',
          label: 'Fully Validated: tested at compile-time',
          color: '#155724',
          bgColor: '#d4edda',
          borderColor: '#28a745'
        };
      case "databricks":
        // Databricks Runtime required - blue
        return {
          icon: '⚡',
          label: 'Databricks Runtime Required',
          color: '#004085',
          bgColor: '#cce5ff',
          borderColor: '#004085'
        };
      case "compile":
        // Compile validated - green icon, gray background
        return {
          icon: '🔗',
          label: 'Compile Validated',
          color: '#555',
          bgColor: '#f5f5f5',
          borderColor: '#999'
        };
      case "static":
      default:
        // Static - gray
        return {
          icon: '📄',
          label: 'Static',
          color: '#555',
          bgColor: '#f5f5f5',
          borderColor: '#999'
        };
    }
  };

  const config = getIndicatorConfig();

  const sourceNote = showIndicator ? (
    <div style={{
      fontSize: '0.85em',
      color: config.color,
      marginBottom: '0.5rem',
      padding: '0.5rem',
      backgroundColor: config.bgColor,
      borderLeft: `3px solid ${config.borderColor}`,
      borderRadius: '4px',
      display: 'flex',
      alignItems: 'center',
      gap: '0.5rem'
    }}>
      <span style={{ fontSize: '1.2em' }}>{config.icon}</span>
      <div style={{ flex: 1 }}>
        <strong>{config.label}</strong>{source && (
          <>
            : <code>{source}</code>
            {testFile && <> • <code>{testFile}</code></>}
          </>
        )}
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
      {outputContent && (
        <CodeBlock
          language="text"
          title="Example output"
          showLineNumbers={false}
        >
          {outputContent.trim()}
        </CodeBlock>
      )}
    </>
  );
}
