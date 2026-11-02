#!/usr/bin/env python3
"""Deterministic check: every function newly added to the registered list within
the QC git range must be mentioned in the release notes.

Reads:
  QC_RANGE (env)  -- git diff range, e.g. ``origin/beta/0.4.0..HEAD``.
                     If unset or empty, prints a notice and exits 0 (nothing to check).

Algorithm:
  1. The added names: the registered names now (the stable rows of the function manifest,
     ``manifest/functions.json``, plus the legacy ``docs/tests-function-info/registered_functions.txt``)
     minus the names at the range's base (the same two files, read with ``git show <base>:<path>``).
  2. For each added name, check whether it appears (substring) anywhere in
     ``docs/docs/release-notes.mdx``, ``docs/docs/release-notes-0-6.mdx`` or
     ``docs/docs/release-notes-0-5-and-earlier.mdx`` (the release notes are split across
     the summary page and the series subpages).  Also accept the bare name (strip
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

import json
import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _manifest_names import all_names  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTERED_TXT = REPO_ROOT / "docs/tests-function-info/registered_functions.txt"
MANIFEST_JSON = REPO_ROOT / "python/geobrix/src/databricks/labs/gbx/manifest/functions.json"
RELEASE_NOTES = REPO_ROOT / "docs/docs/release-notes.mdx"
# The release notes are split across the summary page and the series subpages; a function
# mentioned on any of them counts.
RELEASE_NOTES_PAGES = [
    RELEASE_NOTES,
    REPO_ROOT / "docs/docs/release-notes-0-6.mdx",
    REPO_ROOT / "docs/docs/release-notes-0-5-and-earlier.mdx",
]

# A registered-function line of the legacy list: ``gbx_foo_bar``.
LISTED_RE = re.compile(r"^(gbx_[a-z0-9_]+)\s*$", re.MULTILINE)


def _show(rev: str, path: Path) -> str | None:
    """``path`` at ``rev`` ("" when the file did not exist there); None on any other git error."""
    rel = str(path.relative_to(REPO_ROOT))
    result = subprocess.run(
        ["git", "show", f"{rev}:{rel}"], capture_output=True, text=True, cwd=str(REPO_ROOT)
    )
    if result.returncode == 0:
        return result.stdout
    exists = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", f"{rev}^{{commit}}"],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )
    if exists.returncode == 0:
        return ""  # the revision exists; the file did not exist there
    print(f"git show {rev}:{rel} failed; treating as advisory skip.", file=sys.stderr)
    if result.stderr.strip():
        print(result.stderr.strip(), file=sys.stderr)
    return None


def names_at(rev: str) -> set[str] | None:
    """The registered names at ``rev``: the legacy list's and the manifest's stable rows."""
    listed, manifest = _show(rev, REGISTERED_TXT), _show(rev, MANIFEST_JSON)
    if listed is None or manifest is None:
        return None
    names = set(LISTED_RE.findall(listed))
    if manifest.strip():
        rows = json.loads(manifest).get("functions", [])
        names |= {r["sql_name"] for r in rows if r.get("status", "stable") == "stable"}
    return names


def added_functions(qc_range: str) -> list[str] | None:
    """Return the names registered now that were not registered at the base of ``qc_range``.

    Returns None on git error (caller should treat as advisory skip).
    """
    base = re.split(r"\.{2,3}", qc_range, maxsplit=1)[0].strip() or "HEAD"
    before = names_at(base)
    if before is None:
        return None
    return sorted(all_names() - before)


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
        print("No functions registered in this range.")
        print("release-notes-functions: PASS")
        return 0

    print(f"Added functions detected ({len(added)}): {', '.join(added)}")

    notes_text = "\n".join(
        page.read_text(encoding="utf-8") for page in RELEASE_NOTES_PAGES if page.exists()
    )
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
