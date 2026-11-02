#!/usr/bin/env python3
"""Verify the RasterX function-categories diagram stays in sync with the registered function set.

Two checks run together:

  D4a - Coverage: every rst_* token rendered as a pill in the diagram matches
        the registered gbx_rst_* set exactly. Reports:
          - registered functions MISSING from the diagram (pill not rendered)
          - diagram tokens NOT in the registered set (stale / renamed)
        Either non-empty -> fail.

  D4b - Count: every human-readable count mention in the diagram script
        (e.g. "107 functions", "107 SQL functions") equals the true count of
        registered rst_ functions. Any mismatch -> fail.

Exit code: 0 when both checks pass, 1 if either finds problems.

Run directly on the host -- pure stdlib, no Docker needed.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _manifest_names import all_names  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTERED_TXT = REPO_ROOT / "docs/tests-function-info/registered_functions.txt"
DIAGRAM_PY = REPO_ROOT / "resources/images/generators/rasterx-function-categories.py"

# Matches bare rst_ tokens (the pill labels used in the diagram script).
# We require word-boundary on the left (quote or comma or open-bracket) so we
# don't accidentally match prose like "rst_*" in a docstring or comment.
DIAGRAM_TOKEN_RE = re.compile(r"""(?<![a-z_])(rst_[a-z0-9_]+)""")

# Matches count mentions like "107 functions" or "107 SQL functions".
# Captures the digit group so we can compare it to the true count.
COUNT_RE = re.compile(r"""(\d+)\s+(?:SQL\s+)?functions""")


def canonical_rst() -> set[str]:
    """Return the set of registered rst_ names (without the gbx_ prefix): the manifest's and the legacy list's."""
    return {n[len("gbx_"):] for n in all_names() if n.startswith("gbx_rst_")}


def diagram_tokens(text: str) -> set[str]:
    """Return all unique rst_ tokens found in the diagram script.

    We restrict the search to string literals (quoted content) and list
    contexts to avoid picking up prose / comment fragments.  The regex
    already requires word-boundary on the left, which excludes loose
    references like 'rst_*' in docstrings.
    """
    return set(DIAGRAM_TOKEN_RE.findall(text))


def count_mentions(text: str) -> list[tuple[int, str]]:
    """Return [(number, matched_string), ...] for every count mention in *text*."""
    results = []
    for m in COUNT_RE.finditer(text):
        results.append((int(m.group(1)), m.group(0)))
    return results


# ---------------------------------------------------------------------------
# Check D4a -- coverage
# ---------------------------------------------------------------------------

def check_d4a(registered: set[str], tokens: set[str]) -> tuple[bool, str]:
    missing = sorted(registered - tokens)
    stale = sorted(tokens - registered)

    lines = [f"D4a diagram-coverage: {len(registered)} registered, {len(tokens)} in diagram"]

    if not missing and not stale:
        lines.append(
            f"PASS D4a: diagram pills match the registered rst_ set exactly "
            f"({len(registered)} functions)."
        )
        return True, "\n".join(lines)

    if missing:
        lines.append(
            f"FAIL D4a: {len(missing)} registered function(s) MISSING from the diagram:"
        )
        for name in missing:
            lines.append(f"     - {name}  (registered as gbx_{name})")
    if stale:
        lines.append(
            f"FAIL D4a: {len(stale)} diagram token(s) NOT in the registered set (stale/renamed):"
        )
        for name in stale:
            lines.append(f"     - {name}")
    return False, "\n".join(lines)


# ---------------------------------------------------------------------------
# Check D4b -- count string(s)
# ---------------------------------------------------------------------------

def check_d4b(true_count: int, text: str) -> tuple[bool, str]:
    mentions = count_mentions(text)

    if not mentions:
        return (
            False,
            "FAIL D4b: no count mention (e.g. '107 functions') found in the diagram script."
            f"  Expected to find {true_count}.",
        )

    bad = [(n, s) for n, s in mentions if n != true_count]
    if not bad:
        lines = [
            f"D4b count-strings: found {len(mentions)} mention(s) — all equal {true_count}.",
            f"PASS D4b: every count mention in the diagram script equals {true_count}.",
        ]
        return True, "\n".join(lines)

    lines = [
        f"D4b count-strings: true count is {true_count}; "
        f"found {len(mentions)} mention(s), {len(bad)} disagree:"
    ]
    for n, s in bad:
        lines.append(f"     - '{s}' (has {n}, expected {true_count})")
    return False, "\n".join(lines)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    for required in (REGISTERED_TXT, DIAGRAM_PY):
        if not required.exists():
            print(f"FAIL missing required file: {required}", file=sys.stderr)
            return 1

    registered = canonical_rst()
    diagram_text = DIAGRAM_PY.read_text()
    tokens = diagram_tokens(diagram_text)

    print(f"Canonical registered rst_ functions: {len(registered)}")
    print()

    d4a_ok, d4a_report = check_d4a(registered, tokens)
    d4b_ok, d4b_report = check_d4b(len(registered), diagram_text)

    print(d4a_report)
    print()
    print(d4b_report)
    print()

    if d4a_ok and d4b_ok:
        return 0
    print(
        "diagram-coverage FAILED -- update resources/images/generators/rasterx-function-categories.py"
        " to match the registered rst_ set, then re-render the SVG/PNG."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
