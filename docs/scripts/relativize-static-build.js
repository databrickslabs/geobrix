/**
 * Post-process a Docusaurus build dir (from DOCS_STATIC_ZIP=1 build).
 * - Replaces "/./" with "./" in HTML and assets for file:// loading.
 * - Injects a script into root index.html so file:// opens redirect to #/ (hash router).
 *
 * Usage: node scripts/relativize-static-build.js [buildDir]
 * Default buildDir: docs/build
 */

const fs = require('fs');
const path = require('path');

const buildDir = process.argv[2]
  ? path.resolve(process.cwd(), process.argv[2])
  : path.join(__dirname, '..', 'build');
const pattern = /\/\.\//g;
const replacement = './';

// On file://: (1) empty, #, or #/ -> #/./. (2) #/docs/... (no dot) -> #/./docs/... so in-app links match. Run on load and on hashchange.
const FILE_PROTOCOL_SCRIPT =
  '<script>(function(){if(window.location.protocol!=="file:")return;function fix(){var h=window.location.hash;if(!h||h==="#"||h==="#/"){location.hash="#/./";return;}if(h.indexOf("#/./")===0)return;if(h.indexOf("#/docs/")===0){location.hash="#/./docs/"+h.slice(7);}}fix();window.addEventListener("hashchange",fix);})();</script>';

function relativize(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  const newContent = content.replace(pattern, replacement);
  if (newContent === content) return false;
  fs.writeFileSync(filePath, newContent, 'utf8');
  return true;
}

function injectFileProtocolScript(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  if (content.includes('FILE_PROTOCOL_SCRIPT') || content.includes('location.hash;if(!h)')) return false;
  const newContent = content.replace(/<head[^>]*>/, (head) => head + '\n' + FILE_PROTOCOL_SCRIPT);
  if (newContent === content) return false;
  fs.writeFileSync(filePath, newContent, 'utf8');
  return true;
}

function walk(dir, extRe, callback) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const e of entries) {
    const full = path.join(dir, e.name);
    if (e.isDirectory()) {
      walk(full, extRe, callback);
    } else if (extRe.test(e.name)) {
      callback(full);
    }
  }
}

if (!fs.existsSync(buildDir)) {
  console.error('Build directory not found:', buildDir);
  process.exit(1);
}

// Root to use for zipping: if hash-router build created nested build/, use that so zip has clean root.
const zipRootDir = (() => {
  const nested = path.join(buildDir, 'build');
  if (fs.existsSync(nested) && fs.statSync(nested).isDirectory()) {
    return nested;
  }
  return buildDir;
})();

function processHtmlDir(dir, injectRoot) {
  walk(dir, /\.html$/, (full) => {
    if (relativize(full)) {
      console.log('Relativized:', path.relative(buildDir, full));
    }
    if (injectRoot && path.basename(full) === 'index.html' && injectFileProtocolScript(full)) {
      console.log('Injected file:// hash normalization:', path.relative(buildDir, full));
    }
  });
}

// Relativize HTML at root and under docs/; inject file:// script into each root index.html
processHtmlDir(buildDir, true);
if (zipRootDir !== buildDir) {
  processHtmlDir(zipRootDir, true);
}

// Rewrite route paths in JS so pathname /./docs/... (from hash #/./docs/...) matches (static build emits path:"./docs/...").
function rewriteRoutePaths(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  const next = content.replace(/path:"\.\/docs/g, 'path:"/./docs').replace(/path:"\.\/"/g, 'path:"/./"');
  if (next === content) return false;
  fs.writeFileSync(filePath, next, 'utf8');
  return true;
}

// JS and CSS in assets (may contain baseUrl) – root and zip root
walk(path.join(buildDir, 'assets'), /\.(js|css)$/, (full) => {
  if (relativize(full)) {
    console.log('Relativized:', path.relative(buildDir, full));
  }
  if (/\.js$/.test(full) && rewriteRoutePaths(full)) {
    console.log('Route paths rewritten:', path.relative(buildDir, full));
  }
});
if (zipRootDir !== buildDir && fs.existsSync(path.join(zipRootDir, 'assets'))) {
  walk(path.join(zipRootDir, 'assets'), /\.(js|css)$/, (full) => {
    if (relativize(full)) {
      console.log('Relativized:', path.relative(buildDir, full));
    }
    if (/\.js$/.test(full) && rewriteRoutePaths(full)) {
      console.log('Route paths rewritten:', path.relative(buildDir, full));
    }
  });
}

// Write zip root path for the shell script (one line: absolute path to zipRootDir)
const markerPath = path.join(buildDir, '.static-zip-root');
fs.writeFileSync(markerPath, zipRootDir + '\n', 'utf8');
console.log('Zip root (package this folder):', path.relative(buildDir, zipRootDir) || '.');

console.log('Done. You can zip the build folder for the static docs bundle.');
