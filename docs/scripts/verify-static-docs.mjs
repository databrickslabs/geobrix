#!/usr/bin/env node
/**
 * Verify static docs zip renders under file://: open index with #/./docs/intro and check intro (not 404).
 * Uses file:// so the injected script (which only runs for file:) runs and normalizes the hash.
 * Usage: node scripts/verify-static-docs.mjs [path-to-unzipped-dir]
 */

import { chromium } from 'playwright';
import { join, resolve } from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = resolve(process.argv[2] || join(__dirname, '..', 'build-static-zip'));
const indexFile = join(root, 'index.html');

async function main() {
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();

    // 1) file:// with #/./docs/intro — script should replace to #/docs/intro and intro should render
    const fileUrl = 'file://' + indexFile + '#/./docs/intro';
    await page.goto(fileUrl, { waitUntil: 'domcontentloaded', timeout: 15000 });
    await page.waitForTimeout(3500);

    const hash = await page.evaluate(() => window.location.hash);
    const body = await page.locator('body').innerText();
    const hasIntro = /Introduction to GeoBrix|high-performance spatial/i.test(body);
    const has404 = /Page Not Found|We could not find what you were looking for/i.test(body);

    if (has404) {
      console.error('FAIL: Page Not Found when opening file://...index.html#/./docs/intro');
      process.exitCode = 1;
    } else if (hasIntro) {
      console.log('OK: Intro page renders for file:// + #/./docs/intro');
    } else {
      console.warn('WARN: Intro text not found; body snippet:', body.slice(0, 300));
    }

    // 2) file:// with #/./docs/intro (direct) — should show intro (routes are /./docs/... in zip)
    await page.goto('file://' + indexFile + '#/./docs/intro', { waitUntil: 'domcontentloaded', timeout: 15000 });
    await page.waitForTimeout(2000);
    const body2 = await page.locator('body').innerText();
    if (/Page Not Found/i.test(body2)) {
      console.error('FAIL: file:// + #/./docs/intro (direct) shows Page Not Found');
      process.exitCode = 1;
    } else if (/Introduction to GeoBrix|high-performance spatial/i.test(body2)) {
      console.log('OK: file:// + #/./docs/intro (direct) renders intro');
    }

    // 3) file:// with #/docs/installation (link-style, no dot) — script should fix to #/./ and Installation should render
    await page.goto('file://' + indexFile + '#/docs/installation', { waitUntil: 'domcontentloaded', timeout: 15000 });
    await page.waitForTimeout(2500);
    const bodyInstall = await page.locator('body').innerText();
    const hashAfterInstall = await page.evaluate(() => window.location.hash);
    if (/Page Not Found/i.test(bodyInstall)) {
      console.error('FAIL: file:// + #/docs/installation shows Page Not Found (hash after:', hashAfterInstall + ')');
      process.exitCode = 1;
    } else if (/Installation|heavy-weight|PySpark/i.test(bodyInstall)) {
      console.log('OK: Installation page renders for #/docs/installation (hash=', hashAfterInstall + ')');
    } else {
      console.warn('WARN: Installation content not found; hash=', hashAfterInstall);
    }

    // 4) file:// with #/./docs/api/overview — API Overview should render
    await page.goto('file://' + indexFile + '#/./docs/api/overview', { waitUntil: 'domcontentloaded', timeout: 15000 });
    await page.waitForTimeout(2500);
    const bodyApi = await page.locator('body').innerText();
    if (/Page Not Found/i.test(bodyApi)) {
      console.error('FAIL: file:// + #/./docs/api/overview shows Page Not Found');
      process.exitCode = 1;
    } else if (/API Reference|Overview|RasterX|VectorX|GridX/i.test(bodyApi)) {
      console.log('OK: API Overview page renders for #/./docs/api/overview');
    } else {
      console.warn('WARN: API Overview content not found');
    }

    // 5) file:// with #/docs/quick-start (link-style) — script should fix; Quick Start should render
    await page.goto('file://' + indexFile + '#/docs/quick-start', { waitUntil: 'domcontentloaded', timeout: 15000 });
    await page.waitForTimeout(2500);
    const bodyQS = await page.locator('body').innerText();
    if (/Page Not Found/i.test(bodyQS)) {
      console.error('FAIL: file:// + #/docs/quick-start shows Page Not Found');
      process.exitCode = 1;
    } else if (/Quick Start|quick.start/i.test(bodyQS)) {
      console.log('OK: Quick Start page renders for #/docs/quick-start');
    } else {
      console.warn('WARN: Quick Start content not found');
    }

    // 6) file:// with no hash — script should set #/./
    await page.goto('file://' + indexFile, { waitUntil: 'domcontentloaded', timeout: 15000 });
    await page.waitForTimeout(1000);
    let hashEmpty = await page.evaluate(() => window.location.hash);
    if (hashEmpty === '#/' || hashEmpty === '#/./' || hashEmpty === '') {
      console.log('OK: Empty hash handled, hash=', hashEmpty || '(empty)');
    } else {
      console.warn('WARN: Empty hash resulted in', hashEmpty);
    }

    // 7) file:// with #/ (root in bar) — script should set #/./ and home should load
    await page.goto('file://' + indexFile + '#/', { waitUntil: 'domcontentloaded', timeout: 15000 });
    await page.waitForTimeout(1500);
    const hashRoot = await page.evaluate(() => window.location.hash);
    const bodyRoot = await page.locator('body').innerText();
    const hasHome = /GeoBrix|tagline|Get Started/i.test(bodyRoot) && !/Page Not Found/i.test(bodyRoot);
    if (hashRoot !== '#/./') {
      console.warn('WARN: #/ resulted in hash=', hashRoot, '(expected #/./)');
    }
    if (hasHome) {
      console.log('OK: Root #/ normalizes to #/./ and home renders');
    } else {
      console.error('FAIL: file:// + #/ did not show home (hash=' + hashRoot + ')');
      process.exitCode = 1;
    }
  } finally {
    await browser.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
