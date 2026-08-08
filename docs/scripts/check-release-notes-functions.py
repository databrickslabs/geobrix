#!/usr/bin/env python3
"""Deterministic check: every function newly added to the registered list within
the QC git range must be mentioned in the release notes.

Reads:
  QC_RANGE (env)  -- git diff range, e.g. ``origin/beta/0.4.0..HEAD``.
                     If unset or empty, prints a notice and exits 0 (nothing to check).

Algorithm:
  1. ``git diff QC_RANGE -- docs/tests-function-info/registered_functions.txt``
     Collect lines matching ``^+gbx_[a-z0-9_]+`` (added registered functions;
     the ``+++`` file-header line is excluded by the regex).
  2. For each added name, check whether it appears (substring) anywhere in
     ``docs/docs/release-notes.mdx``.  Also accept the bare name (strip
     leading ``gbx_``) as a match -- some bullets reference the bare form.
     A match on either counts.
  3. Exit 1 listing every added function NOT mentioned in the release notes.
     Exit 0 if all added functions are mentioned (or none were added).

Graceful degradation:
  * If ``git diff`` fails (bad range, not a repo, etc.), print stderr and exit 0
    so a git plumbing issue does not hard-block the push.
  * Pure stdlib; host-only; no Docker needed.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTERED_TXT = REPO_ROOT / "docs/tests-function-info/registered_functions.txt"
RELEASE_NOTES = REPO_ROOT / "docs/docs/release-notes.mdx"

# Matches a newly added registered-function line: ``+gbx_foo_bar``
# The ``+++`` diff file-header lines are excluded because they contain a path,
# not a bare function name -- they will never match ``^[+]gbx_[a-z0-9_]+$``.
ADDED_LINE_RE = re.compile(r"^\+(?P<name>gbx_[a-z0-9_]+)\s*$", re.MULTILINE)


def added_functions(qc_range: str) -> list[str] | None:
    """Return names added to registered_functions.txt in ``qc_range``.

    Returns None on git error (caller should treat as advisory skip).
    """
    result = subprocess.run(
        ["git", "diff", qc_range, "--", str(REGISTERED_TXT.relative_to(REPO_ROOT))],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )
    if result.returncode != 0:
        print(
            f"git diff failed (exit {result.returncode}); treating as advisory skip.",
            file=sys.stderr,
        )
        if result.stderr.strip():
            print(result.stderr.strip(), file=sys.stderr)
        return None

    names = ADDED_LINE_RE.findall(result.stdout)
    return names


def mentioned_in_release_notes(name: str, notes_text: str) -> bool:
    """True if ``name`` or its bare form (without ``gbx_`` prefix) appears in notes.

    Also accepts brace-expansion shorthand used in this project's release notes, e.g.
    ``gbx_rst_quadbin_rastertogrid{avg,count,max,min,median}`` covers every suffixed form
    like ``gbx_rst_quadbin_rastertogridavg``.  We check whether the notes contain a
    brace-group starting with the function's stem (the longest prefix of ``name`` that
    ends at an underscore boundary and is followed by ``{`` in the notes).
    """
    if name in notes_text:
        return True
    bare = name[len("gbx_"):]
    if bare in notes_text:
        return True
    # Brace-expansion check: find the longest prefix of ``name`` that appears in the
    # notes followed immediately by ``{``.  This handles compound suffixes like
    # ``rastertogridavg`` where the brace group is written as:
    #   ``gbx_rst_quadbin_rastertogrid{avg,count,max,min,median}``
    # We walk from the full name backwards one character at a time to find the split.
    for split_at in range(len(name) - 1, len("gbx_"), -1):
        stem = name[:split_at]
        suffix = name[split_at:]
        if not suffix:
            continue
        search_key = stem + "{"
        if search_key in notes_text:
            idx = notes_text.find(search_key)
            close = notes_text.find("}", idx)
            if close != -1:
                brace_content = notes_text[idx + len(stem) + 1 : close]
                variants = [v.strip() for v in brace_content.split(",")]
                if suffix in variants:
                    return True
    return False


def main() -> int:
    qc_range = os.environ.get("QC_RANGE", "").strip()
    if not qc_range:
        print("QC_RANGE unset or empty; nothing to check -- skipping release-notes-functions.")
        return 0

    print(f"Range: {qc_range}")

    if not REGISTERED_TXT.exists():
        print(f"registered_functions.txt not found at {REGISTERED_TXT}; skipping.", file=sys.stderr)
        return 0

    if not RELEASE_NOTES.exists():
        print(f"Release notes not found at {RELEASE_NOTES}; skipping.", file=sys.stderr)
        return 0

    added = added_functions(qc_range)
    if added is None:
        # git error -- advisory skip
        return 0

    if not added:
        print("No functions added to registered_functions.txt in this range.")
        print("release-notes-functions: PASS")
        return 0

    print(f"Added functions detected ({len(added)}): {', '.join(added)}")

    notes_text = RELEASE_NOTES.read_text(encoding="utf-8")
    unmentioned = [n for n in added if not mentioned_in_release_notes(n, notes_text)]

    if unmentioned:
        print()
        print(f"FAIL: {len(unmentioned)} added function(s) not mentioned in release notes:")
        for name in sorted(unmentioned):
            print(f"  - {name}")
        print()
        print(f"Release notes path: {RELEASE_NOTES.relative_to(REPO_ROOT)}")
        print("Add a bullet (or inline reference) for each function above, then re-push.")
        return 1

    print(f"All {len(added)} added function(s) are mentioned in the release notes.")
    print("release-notes-functions: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
