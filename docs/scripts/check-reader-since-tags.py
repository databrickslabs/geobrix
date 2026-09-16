#!/usr/bin/env python3
"""Guard: every reader/writer docs page carries a `_Since:_` version line.

Reader and writer pages under docs/docs/{readers,writers}/*.mdx each open with a
line of the form:

    _Since:_ lightweight `name_gbx` **vX.Y.Z** · <heavyweight … | light-only … | hybrid …>

This is hand-maintained (it is NOT generated — the function-info "Since v" badge
only covers registered functions, not DataSource reader/writer pages), so a newly
added reader/writer silently ships untagged unless something checks. This guard is
that check; it is wired into the QC judge so it runs on every push / PR.

Fails (exit 1) listing any page missing the line or whose line does not match the
canonical shape. Passes (exit 0) otherwise. Index/category/overview pages are skipped.
"""

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DOC_DIRS = [
    REPO_ROOT / "docs" / "docs" / "readers",
    REPO_ROOT / "docs" / "docs" / "writers",
]

# Non-reader/writer pages that legitimately have no Since line.
SKIP_STEMS = {"index", "readme", "intro", "overview", "_category_"}

# Strict prefix: `_Since:_ lightweight `<name>` **vX.Y.Z** ·` then any tier suffix.
# Allows heavyweight/light-only/hybrid variants after the middot without over-fitting.
SINCE_RE = re.compile(
    r"^_Since:_ lightweight `[a-z0-9_]+` \*\*v\d+\.\d+\.\d+\*\* · .+",
    re.MULTILINE,
)


def main() -> int:
    offenders = []
    scanned = 0
    for doc_dir in DOC_DIRS:
        if not doc_dir.is_dir():
            continue
        for mdx in sorted(doc_dir.glob("*.mdx")):
            if mdx.stem.lower() in SKIP_STEMS:
                continue
            scanned += 1
            text = mdx.read_text(encoding="utf-8")
            if not SINCE_RE.search(text):
                rel = mdx.relative_to(REPO_ROOT)
                reason = (
                    "malformed `_Since:_` line"
                    if "_Since:_" in text
                    else "missing `_Since:_` line"
                )
                offenders.append(f"{rel}: {reason}")

    print(f"check-reader-since-tags: scanned {scanned} reader/writer page(s).")
    if offenders:
        print("FAIL: reader/writer pages without a canonical `_Since:_` line:")
        for o in offenders:
            print(f"  - {o}")
        print(
            "\nExpected shape (hand-maintained, one per page):\n"
            "  _Since:_ lightweight `name_gbx` **vX.Y.Z** · "
            "<heavyweight `name` **vX.Y.Z** | _(light-only — no heavyweight tier)_>"
        )
        return 1

    print("PASS: every reader/writer page has a canonical `_Since:_` line.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
