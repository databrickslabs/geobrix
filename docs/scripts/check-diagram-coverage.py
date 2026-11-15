#!/usr/bin/env python3
"""Verify the RasterX function-categories diagram stays in sync with the registered function set.

The diagram's source is the generated HTML that the PNGs are exported from:
resources/images/diagrams/rasterx/rasterx-function-categories.html. Its generator
(resources/images/generators/rasterx-function-categories.py) builds it from the
manifest and the RasterX Function Reference page; gbx:docs:diagrams runs both steps.

Three checks run together:

  D4a - Coverage: the function pills in the diagram (one ``data-fn="rst_*"`` per pill)
        match the registered gbx_rst_* set exactly, each once. Reports:
          - registered functions MISSING from the diagram
          - diagram pills NOT in the registered set (stale / renamed)
          - pills that appear more than once
        Any non-empty -> fail.

  D4b - Counts: every "<N> functions" / "<N> RasterX functions" in the diagram's text
        equals the true count of registered rst_ functions, and each card's count badge
        equals the pills on that card. Any mismatch -> fail.

  D4c - Fresh: the generator, run now, writes the committed HTML byte for byte.
        A difference means the committed diagram (and its PNGs) is stale -> fail.

Exit code: 0 when all pass, 1 if any finds problems.

Run directly on the host -- pure stdlib, no Docker needed.
"""
from __future__ import annotations

import importlib.util
import json
import re
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_JSON = REPO_ROOT / "python/geobrix/src/databricks/labs/gbx/manifest/functions.json"
DIAGRAM_PY = REPO_ROOT / "resources/images/generators/rasterx-function-categories.py"
DIAGRAM_HTML = REPO_ROOT / "resources/images/diagrams/rasterx/rasterx-function-categories.html"

# One per pill: <g class="pill" data-fn="rst_name">.
PILL_RE = re.compile(r'data-fn="(rst_[a-z0-9_]+)"')

# Count mentions in the diagram's visible text, like "166 functions" or "166 RasterX functions".
COUNT_RE = re.compile(r"""(\d+)\s+(?:SQL\s+|RasterX\s+)?functions""")

CARD_RE = re.compile(r'<g class="node" data-category="([^"]*)">(.*?)</g></g>', re.S)
BADGE_RE = re.compile(r'<g class="badge">.*?>(\d+)</text></g>', re.S)

FIX = "run gbx:docs:diagrams (bash scripts/commands/gbx-docs-diagrams.sh), then commit the HTML and PNGs"


def canonical_rst() -> set[str]:
    """Return the set of registered rst_ names (without the gbx_ prefix): the manifest's."""
    names = {r["sql_name"] for r in json.loads(MANIFEST_JSON.read_text())["functions"]}
    return {n[len("gbx_"):] for n in names if n.startswith("gbx_rst_")}


def diagram_pills(html: str) -> list[str]:
    """Every pill's function name, in document order (duplicates kept)."""
    return PILL_RE.findall(html)


def visible_text(html: str) -> str:
    """The diagram's text: the <style> block, comments and tags removed."""
    html = re.sub(r"<style>.*?</style>|<!--.*?-->", " ", html, flags=re.S)
    return re.sub(r"<[^>]+>", " ", html)


def count_mentions(text: str) -> list[tuple[int, str]]:
    """Return [(number, matched_string), ...] for every count mention in *text*."""
    return [(int(m.group(1)), m.group(0)) for m in COUNT_RE.finditer(text)]


# ---------------------------------------------------------------------------
# Check D4a -- coverage
# ---------------------------------------------------------------------------


def check_d4a(registered: set[str], pills: list[str]) -> tuple[bool, str]:
    tokens = set(pills)
    missing = sorted(registered - tokens)
    stale = sorted(tokens - registered)
    twice = sorted(n for n, k in Counter(pills).items() if k > 1)

    lines = [f"D4a diagram-coverage: {len(registered)} registered, {len(pills)} pills in diagram"]
    if not registered:
        lines.append("FAIL D4a: the manifest gave 0 registered rst_ functions.")
        return False, "\n".join(lines)
    if not missing and not stale and not twice:
        lines.append(
            f"PASS D4a: diagram pills match the registered rst_ set exactly ({len(registered)} functions)."
        )
        return True, "\n".join(lines)
    if missing:
        lines.append(f"FAIL D4a: {len(missing)} registered function(s) MISSING from the diagram:")
        lines += [f"     - {name}  (registered as gbx_{name})" for name in missing]
    if stale:
        lines.append(f"FAIL D4a: {len(stale)} diagram pill(s) NOT in the registered set (stale/renamed):")
        lines += [f"     - {name}" for name in stale]
    if twice:
        lines.append(f"FAIL D4a: {len(twice)} pill(s) appear more than once:")
        lines += [f"     - {name}" for name in twice]
    return False, "\n".join(lines)


# ---------------------------------------------------------------------------
# Check D4b -- count strings and card badges
# ---------------------------------------------------------------------------


def check_d4b(true_count: int, html: str) -> tuple[bool, str]:
    mentions = count_mentions(visible_text(html))
    cards = CARD_RE.findall(html)
    lines = []
    ok = True

    if not mentions:
        ok = False
        lines.append(f"FAIL D4b: no count mention (e.g. '{true_count} functions') found in the diagram.")
    else:
        bad = [(n, s) for n, s in mentions if n != true_count]
        lines.append(
            f"D4b count-strings: true count is {true_count}; found {len(mentions)} mention(s), {len(bad)} disagree."
        )
        for n, s in bad:
            ok = False
            lines.append(f"     - '{s}' (has {n}, expected {true_count})")

    if not cards:
        ok = False
        lines.append("FAIL D4b: no category cards found in the diagram.")
    for title, body in cards:
        badge = BADGE_RE.search(body)
        n_pills = len(PILL_RE.findall(body))
        if not badge or int(badge.group(1)) != n_pills:
            ok = False
            lines.append(
                f"     - card '{title}': badge {badge.group(1) if badge else 'missing'}, {n_pills} pills"
            )
    if ok:
        lines.append(
            f"PASS D4b: every count mention equals {true_count}, and all {len(cards)} card badges match their pills."
        )
    else:
        lines.append("FAIL D4b: the diagram's counts disagree (see above).")
    return ok, "\n".join(lines)


# ---------------------------------------------------------------------------
# Check D4c -- the committed HTML is what the generator writes now
# ---------------------------------------------------------------------------


def generate() -> str:
    spec = importlib.util.spec_from_file_location("rasterx_function_categories", DIAGRAM_PY)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    try:
        spec.loader.exec_module(mod)
        reg = mod.registered()
        return mod.render(reg, mod.categories(reg))
    finally:
        sys.modules.pop(spec.name, None)


def check_d4c(html: str) -> tuple[bool, str]:
    try:
        fresh = generate()
    except SystemExit as e:  # the generator refuses a page/manifest mismatch
        return False, f"FAIL D4c: the generator refused to run: {e}"
    if fresh == html:
        return True, "PASS D4c: the committed diagram HTML is what the generator writes now."
    return False, f"FAIL D4c: the committed diagram HTML is stale -- {FIX}."


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main() -> int:
    for required in (MANIFEST_JSON, DIAGRAM_PY, DIAGRAM_HTML):
        if not required.exists():
            print(f"FAIL missing required file: {required}", file=sys.stderr)
            return 1

    registered = canonical_rst()
    html = DIAGRAM_HTML.read_text()

    print(f"Canonical registered rst_ functions: {len(registered)}")
    print()
    results = [
        check_d4a(registered, diagram_pills(html)),
        check_d4b(len(registered), html),
        check_d4c(html),
    ]
    for _, report in results:
        print(report)
        print()
    if all(ok for ok, _ in results):
        return 0
    print(f"diagram-coverage FAILED -- {FIX}.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
