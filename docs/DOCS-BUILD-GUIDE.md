# Documentation Build Guide - Single Source of Truth Integration

This guide covers testing and deploying the GeoBrix documentation with automatically imported code from tests.

## 🎯 Overview

The documentation uses a **single source of truth** pattern:
1. Code lives in `docs/tests/python/` (tested by pytest)
2. MDX files import code using webpack raw-loader
3. `CodeFromTest` component displays tested code
4. Docusaurus builds static site with guaranteed-working examples

## 📋 Prerequisites

- Node.js 18+ installed
- npm installed
- Python 3.12+ (for running tests)
- Docker (for running tests in container)

---

## 🔧 Step 1: Install Dependencies

### Install Node Dependencies

```bash
cd docs/
npm install
```

This will install:
- Docusaurus core and preset
- React and MDX
- **raw-loader** (for importing .py files)

### Verify Installation

```bash
npm list raw-loader
```

Should show: `raw-loader@4.0.2`

---

## ✅ Step 2: Verify Tests Pass

**IMPORTANT**: Documentation code must pass all tests before building docs.

### Run All Documentation Tests

```bash
# From project root
./scripts/ci/run-doc-tests.sh local

# Or just Python tests
./scripts/ci/run-doc-tests.sh python

# Or direct pytest
cd docs/
pytest tests/python/ -v -m "not integration"
```

### Expected Results

```
91 tests total
55 passed
25 deselected (integration tests)
11 failed (expected - need Spark environment)
```

**Structure tests must pass** (imports, signatures, docstrings).

---

## 🏗️ Step 3: Test Local Build

### Clean Build

```bash
cd docs/
npm run clear  # Clear cache
npm run build  # Build static site
```

### Watch for Errors

**Expected Output**:
```
[SUCCESS] Generated static files in "build".
[SUCCESS] Use `npm run serve` to test your build locally.
```

**Common Issues**:

1. **Raw-loader not found**:
   ```bash
   npm install raw-loader --save-dev
   ```

2. **Import path errors**:
   - Check import path: `!!raw-loader!../../../tests/python/...`
   - Verify file exists relative to MDX file

3. **Function extraction fails**:
   - Check `functionName` matches exactly
   - Ensure function exists in imported file

### Serve Locally

```bash
npm run serve
```

Open http://localhost:3000 and navigate to:
- http://localhost:3000/docs/examples/overview

### Verify Integration

Check that:
1. ✅ Code blocks display correctly
2. ✅ "Single Source of Truth" badge shows
3. ✅ Source and test file paths are correct
4. ✅ Code syntax highlighting works
5. ✅ Function extraction shows only the requested function

### Function info (DESCRIBE FUNCTION EXTENDED)

SQL examples from `docs/tests/python/api/rasterx_functions_sql.py` and `gridx_functions_sql.py` are also used for Spark’s `DESCRIBE FUNCTION EXTENDED <name>` (one-copy pattern). After changing or adding SQL examples in those files, regenerate the function-info resource and commit it:

```bash
# From project root
python3 docs/scripts/generate-function-info.py
# Commit the updated file:
#   src/main/resources/com/databricks/labs/gbx/function-info.json
```

---

## 🧪 Step 4: Test the Integration

### Create a Test MDX File

Create `docs/docs/test-integration.mdx`:

```mdx
---
title: Integration Test
---

import CodeFromTest from '@site/src/components/CodeFromTest';
import testCode from '!!raw-loader!../../tests/python/setup/essential_bundle.py';

# Integration Test

<CodeFromTest 
  language="python"
  title="Test Import"
  source="docs/tests/python/setup/essential_bundle.py"
  lines="1-30"
>
  {testCode}
</CodeFromTest>
```

### Build and Verify

```bash
npm run build
npm run serve
```

Navigate to http://localhost:3000/docs/test-integration

**Success Criteria**:
- ✅ Page loads without errors
- ✅ Code from `essential_bundle.py` is displayed
- ✅ Lines 1-30 are shown
- ✅ Badge indicates source file

### Clean Up

```bash
rm docs/docs/test-integration.mdx
```

---

## 📊 Step 5: Update Existing Documentation

### Identify Pages to Update

Pages that currently have hardcoded examples:

```bash
# Find MDX files with code blocks
grep -r "```python" docs/docs/api/*.md

# Target files:
# - api/rasterx-functions.md
# - api/vectorx-functions.md
# - api/gridx-functions.md
```

### Update Pattern

**Before** (hardcoded):
```mdx
## Get Raster Width

```python
from databricks.labs.gbx.rasterx import functions as rx

width = rasters.select(
    rx.rst_width("tile").alias("width")
)
\```
```

**After** (auto-imported):
```mdx
import CodeFromTest from '@site/src/components/CodeFromTest';
// Path from docs/docs/api/ to docs/tests/
import pythonApiCode from '!!raw-loader!../../tests/python/api/python_api.py';

## Get Raster Width

<CodeFromTest 
  language="python"
  title="Get Raster Width"
  source="docs/tests/python/api/python_api.py"
  testFile="docs/tests/python/api/test_python_api.py"
  functionName="rst_width_example"
>
  {pythonApiCode}
</CodeFromTest>
```

### Conversion Checklist

For each API documentation page:
1. ✅ Add import for CodeFromTest component
2. ✅ Add import for code file with raw-loader
3. ✅ Replace hardcoded code blocks with CodeFromTest
4. ✅ Specify source and testFile
5. ✅ Use functionName or lines to extract specific code
6. ✅ Build and verify

---

## 🚀 Step 6: Deployment

### Pre-Deployment Checklist

Before deploying, ensure:

- ✅ All documentation tests pass
  ```bash
  ./scripts/ci/run-doc-tests.sh local
  ```

- ✅ Local build succeeds
  ```bash
  cd docs/ && npm run build
  ```

- ✅ Serve locally and spot-check pages
  ```bash
  npm run serve
  ```

- ✅ All MDX imports are valid
  ```bash
  # Check for broken imports
  npm run build 2>&1 | grep "Error"
  ```

- ✅ Code extraction works correctly
  - Functions display properly
  - Line ranges are accurate
  - No empty code blocks

### Build for Production

```bash
cd docs/
npm run clear
npm run build
```

### Deployment Options

#### Option A: GitHub Pages

```bash
# Configure in docusaurus.config.js
organizationName: 'databrickslabs'
projectName: 'geobrix'

# Deploy
npm run deploy
```

#### Option B: Manual Deployment

```bash
# Build creates static files in docs/build/
npm run build

# Upload build/ directory to your hosting
# Examples: Netlify, Vercel, AWS S3, etc.
```

#### Option C: Static zip for offline / local viewing

Use this when you need a single zip that works when users unzip and open `index.html` from any folder (e.g. Downloads). Uses relative paths and hash router so the site loads without a server. Zip filename and path use the version from `docs/package.json`.

```bash
# From project root (creates zip in resources/static/ by default)
gbx:docs:static-build

# Or from docs/ and zip manually:
cd docs/
npm run build:static-zip
cd build-static-zip && zip -r ../../resources/static/geobrix-docs-$(node -e "console.log(require('./package.json').version)").zip . -x "*.DS_Store"
```

- `build:static-zip` uses `@someok/docusaurus-plugin-relative-paths` (HTML/CSS), baseUrl `./`, hash router, and a post-step to relativize JS and inject a file:// redirect; then restores `docs/build/` for serving.
- Normal `npm run build` keeps `baseUrl: '/'` for deployed sites.

#### Option D: CI/CD pipeline

Add to `.github/workflows/docs.yml`:

```yaml
name: Deploy Documentation

on:
  push:
    branches: [main]
    paths:
      - 'docs/**'
      - 'docs/tests/**'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: '18'
      
      - name: Run documentation tests
        run: |
          pytest docs/tests/python/ -v -m "not integration"
      
      - name: Install dependencies
        working-directory: docs
        run: npm install
      
      - name: Build documentation
        working-directory: docs
        run: npm run build
      
      - name: Deploy to GitHub Pages
        uses: peaceiris/actions-gh-pages@v3
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./docs/build
```

### Post-Deployment Verification

After deployment:

1. ✅ Visit deployed site
2. ✅ Check example pages load correctly
3. ✅ Verify code blocks display
4. ✅ Test "Single Source of Truth" badges appear
5. ✅ Confirm code syntax highlighting works
6. ✅ Check source file links (if clickable)

---

## 🔍 Troubleshooting

### Build Errors

#### "Cannot find module 'raw-loader'"

**Solution**:
```bash
cd docs/
npm install raw-loader --save-dev
```

#### "Module parse failed: Unexpected token"

**Cause**: Webpack not configured for .py files

**Solution**: Check `docusaurus.config.js` has:
```javascript
webpack: {
  configure: (config) => {
    config.module.rules.push({
      test: /\.(py|scala)$/,
      use: 'raw-loader',
    });
    return config;
  },
}
```

#### "Function 'xxx' not found"

**Cause**: `functionName` doesn't match function in file

**Solution**:
1. Check function name spelling
2. Verify function exists in imported file
3. Try without `functionName` to see full file

### Runtime Errors

#### Code block is empty

**Cause**: Import path incorrect or file doesn't exist

**Solution**:
```bash
# Verify file exists
ls docs/tests/python/api/python_api.py

# Check import path from MDX file location
# MDX in: docs/docs/api/python.mdx
# From: docs/docs/api/ up 2 levels (../../) to: docs/
# Then: tests/python/api/python_api.py
# Import:  ../../tests/python/api/python_api.py ✅
```

**Import Path Pattern**:
From any file in `docs/docs/` subdirectories, use `../../tests/` to reach test files.

#### Badge doesn't show

**Cause**: Missing `source` prop

**Solution**: Always include `source` prop:
```jsx
<CodeFromTest 
  source="docs/tests/python/api/python_api.py"
  ...
>
```

---

## 📈 Continuous Integration

### Add Documentation Quality Gates

Add to `.github/workflows/doc-tests.yml`:

```yaml
- name: Verify documentation builds
  run: |
    cd docs/
    npm install
    npm run build
  
- name: Check for code import errors
  run: |
    # Ensure no "Error loading" messages in built docs
    ! grep -r "Error loading code" docs/build/
```

### Pre-commit Hook

Create `.git/hooks/pre-commit`:

```bash
#!/bin/bash
# Run doc tests before allowing commit

echo "Running documentation tests..."
pytest docs/tests/python/ -v -m "not integration" --tb=short

if [ $? -ne 0 ]; then
    echo "❌ Documentation tests failed. Fix tests before committing."
    exit 1
fi

echo "✅ Documentation tests passed"
```

Make executable:
```bash
chmod +x .git/hooks/pre-commit
```

---

## 🎓 Best Practices

### 1. Always Test Before Building

```bash
# Test → Build → Deploy
pytest docs/tests/python/ -v
cd docs/ && npm run build
npm run serve  # Verify locally
```

### 2. Use Function Extraction

Instead of showing entire files, extract specific functions:

```jsx
<CodeFromTest 
  functionName="rst_width_example"  // Shows only this function
  ...
>
```

### 3. Include Source and Test Files

Always show users where code comes from:

```jsx
<CodeFromTest 
  source="docs/tests/python/api/python_api.py"
  testFile="docs/tests/python/api/test_python_api.py"
  ...
>
```

### 4. Keep Tests Passing

Documentation build should fail if tests fail:

```bash
# Add to package.json scripts
"prebuild": "pytest ../tests/python/ -v -m 'not integration'"
```

### 5. Version Your Documentation

Tag documentation releases with code versions:

```bash
git tag -a docs-v1.0.0 -m "Documentation for GeoBrix v1.0.0"
git push origin docs-v1.0.0
```

---

## 📚 Quick Reference

### Key Files

| File | Purpose |
|------|---------|
| `docs/docusaurus.config.js` | Webpack configuration for raw-loader |
| `docs/package.json` | Dependencies including raw-loader |
| `docs/src/components/CodeFromTest.js` | Component for displaying tested code |
| `docs/tests/python/` | Tested Python code for documentation |
| `docs/docs/examples/` | Example MDX pages using CodeFromTest |

### Key Commands

```bash
# Install dependencies
cd docs/ && npm install

# Run tests
./scripts/ci/run-doc-tests.sh python

# Build documentation
cd docs/ && npm run build

# Serve locally
npm run serve

# Deploy
npm run deploy
```

### Component Usage

```jsx
import CodeFromTest from '@site/src/components/CodeFromTest';
// Path from docs/docs/ to docs/tests/
import code from '!!raw-loader!../../tests/python/module/file.py';

<CodeFromTest 
  language="python"
  title="Function Title"
  source="docs/tests/python/module/file.py"
  testFile="docs/tests/python/module/test_file.py"
  functionName="function_name"
>
  {code}
</CodeFromTest>
```

---

## ✅ Success Criteria

Documentation integration is successful when:

1. ✅ `npm run build` completes without errors
2. ✅ All code examples are imported from test files
3. ✅ "Single Source of Truth" badges display
4. ✅ pytest passes for all structure tests
5. ✅ Local serve shows code correctly
6. ✅ Production deployment works
7. ✅ No hardcoded examples remain
8. ✅ CI enforces test passing before build

---

## 🎉 Result

**You now have**:
- ✅ Zero drift between docs and tests
- ✅ Guaranteed working examples
- ✅ Single source of truth enforced
- ✅ CI validates quality
- ✅ Easy maintenance (update once)

**Users get**:
- ✅ Copy-paste safe code
- ✅ Always up-to-date examples
- ✅ Tested code they can trust

**Developers benefit from**:
- ✅ No manual synchronization
- ✅ Breaking changes caught early
- ✅ Clear single location for code
