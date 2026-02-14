#!/usr/bin/env python3
"""
Convert .md files to .mdx with proper CodeFromTest wrapping and escaping.
"""
import re
import sys
from pathlib import Path


def escape_for_jsx(code, language):
    """Escape code for JSX template literals."""
    escaped = code
    
    # 1. Python f-strings: {var} → \${var}
    if language == 'python':
        escaped = re.sub(r'(?<!\\)\{([^}]+)\}', r'\\${\1}', escaped)
    
    # 2. Backticks: ` → \\\`
    if '`' in escaped:
        escaped = escaped.replace('`', r'\\\`')
    
    # 3. Bash/Shell: $VAR → \$VAR, $(cmd) → \$(cmd)
    if language in ['bash', 'shell', 'sh']:
        escaped = re.sub(r'\$([A-Z_][A-Z0-9_]*)', r'\\$\1', escaped)
        escaped = re.sub(r'\$\(', r'\\$(', escaped)
    
    # 4. Scala: ${var} → \${var}, $var → \$var
    if language == 'scala':
        escaped = re.sub(r'\$\{', r'\\${', escaped)
        escaped = re.sub(r'\$([a-zA-Z_][a-zA-Z0-9_]*)', r'\\$\1', escaped)
    
    # 5. SQL: Usually no special escaping needed except backticks (already done)
    
    return escaped


def convert_md_to_mdx(input_path, output_path):
    """Convert .md to .mdx with CodeFromTest components."""
    
    with open(input_path, 'r') as f:
        content = f.read()
    
    # Add import after frontmatter
    if '---\n' in content:
        parts = content.split('---\n', 2)
        if len(parts) >= 3:
            frontmatter = parts[1]
            body = parts[2]
            content = f"---\n{frontmatter}---\n\nimport CodeFromTest from '@site/src/components/CodeFromTest';\n\n{body}"
    
    # Convert code blocks: ```language\ncode\n```
    pattern = r'```(\w+)\n(.*?)```'
    
    def replace_code_block(match):
        language = match.group(1)
        code = match.group(2).rstrip()
        escaped_code = escape_for_jsx(code, language)
        
        return f'\n<CodeFromTest language="{language}" validationLevel="static">\n{{`{escaped_code}`}}\n</CodeFromTest>\n'
    
    content = re.sub(pattern, replace_code_block, content, flags=re.DOTALL)
    
    # Fix inline code with curly braces: `{VAR}` → `\${'{'}VAR${'}'}`
    # But only outside of CodeFromTest blocks
    def fix_inline_curly_braces(text):
        # Simple pattern: `text with {something}` in markdown
        pattern = r'`([^`]*)\{([^}]+)\}([^`]*)`'
        return re.sub(pattern, r"`\1\\${'${'}\\2${'}'}\3`", text)
    
    # Apply to content outside CodeFromTest blocks
    # Split by CodeFromTest, fix inline, rejoin
    parts = re.split(r'(<CodeFromTest.*?</CodeFromTest>)', content, flags=re.DOTALL)
    for i in range(len(parts)):
        if not parts[i].startswith('<CodeFromTest'):
            parts[i] = fix_inline_curly_braces(parts[i])
    content = ''.join(parts)
    
    # Write output
    with open(output_path, 'w') as f:
        f.write(content)
    
    return len(re.findall(r'<CodeFromTest', content))


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python docs/scripts/convert_md_to_mdx.py input.md output.mdx  (from repo root)")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    count = convert_md_to_mdx(input_file, output_file)
    print(f"✅ Converted {input_file} → {output_file}: {count} code blocks")
