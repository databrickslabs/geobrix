#!/usr/bin/env python3
"""Warn-level check: NEW prose sentences in user-facing docs must stay short.

Enforces the "Concise, clear prose (STE-lite)" rule in ``.claude/rules/docs-and-doctests.md``
(a description has at most 25 words, a procedure step at most 20) and item 8 of
``.claude/rules/gbx-commands.md``. Procedure steps and descriptions are not told apart: one
25-word cap applies to every sentence, so a 21-25 word procedure step passes.

Reads:
  QC_RANGE (env)  -- git diff range, e.g. ``origin/beta/0.6.0..HEAD``.
                     If unset or empty, prints a notice and exits 0 (nothing to check).

Scope: files changed in the range that match ``docs/docs/**/*.mdx``, ``docs/docs/**/*.md`` and
``scripts/commands/*.md``. Excluded: ``docs/docs/release-notes-0-5-and-earlier.mdx`` (history),
``docs/build*`` and ``docs/tests/``.

Algorithm:
  1. For each changed file, read the new version (the range end, or the working tree for a
     single-revision range) and the added line numbers from ``git diff -U0``.
  2. From the whole new file, find the lines that are not prose: frontmatter, code fences,
     headings, table rows, import/export/JSX/HTML lines, admonition markers and blanks.
  3. Join consecutive prose lines into paragraphs (a list item starts a new one), strip inline
     code, link targets and emphasis markers, split on ``.``, ``!``, ``?`` followed by space,
     and count words. A sentence over 25 words that touches an added line is a hit.

Output: ``path:line: <N> words: <first 12 words>...`` per hit, then a summary. Exit 1 on any
hit, else 0.

Graceful degradation: if ``git diff`` fails (bad range, not a repo), print a notice and exit 0.
Pure stdlib; host-only; no Docker needed.
"""
from __future__ import annotations

import fnmatch
import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MAX_WORDS = 25
PREVIEW_WORDS = 12

# fnmatch's ``*`` also crosses ``/``, so ``docs/docs/*.mdx`` covers nested pages.
INCLUDE = ("docs/docs/*.mdx", "docs/docs/*.md", "scripts/commands/*.md")
EXCLUDE = (
    "docs/docs/release-notes-0-5-and-earlier.mdx",
    "docs/build*",
    "docs/tests/*",
)

FENCE_RE = re.compile(r"^\s*(`{3,}|~{3,})")
LIST_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
HUNK_RE = re.compile(r"^@@ -\S+ \+(\d+)(?:,(\d+))? @@")
CODE_SPAN_RE = re.compile(r"(`+)(.+?)\1")
IMAGE_OR_LINK_RE = re.compile(r"!?\[([^\]]*)\]\([^)]*\)")
TAG_RE = re.compile(r"</?[A-Za-z][^>]*>")
EMPHASIS_RE = re.compile(r"\*+|(?<!\w)_+|_+(?!\w)")
SENTENCE_END_RE = re.compile(r"(?<=[.!?])\s+")
ABBREVIATIONS = ("e.g.", "i.e.", "etc.", "vs.", "approx.")


def git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], capture_output=True, text=True, cwd=str(REPO_ROOT))


def in_scope(path: str) -> bool:
    if any(fnmatch.fnmatch(path, pat) for pat in EXCLUDE):
        return False
    return any(fnmatch.fnmatch(path, pat) for pat in INCLUDE)


def range_end(qc_range: str) -> str | None:
    """The revision a range ends at, or None for a single revision (diff vs. the working tree)."""
    for sep in ("...", ".."):
        if sep in qc_range:
            return qc_range.split(sep, 1)[1] or "HEAD"
    return None


def new_text(path: str, end: str | None) -> str | None:
    if end is None:
        file = REPO_ROOT / path
        return file.read_text(encoding="utf-8") if file.exists() else None
    shown = git("show", f"{end}:{path}")
    return shown.stdout if shown.returncode == 0 else None


def added_lines(qc_range: str, path: str) -> set[int] | None:
    diff = git("diff", "-U0", qc_range, "--", path)
    if diff.returncode != 0:
        return None
    added: set[int] = set()
    new_no = 0
    for line in diff.stdout.splitlines():
        hunk = HUNK_RE.match(line)
        if hunk:
            new_no = int(hunk.group(1))
        elif line.startswith("+") and not line.startswith("+++"):
            added.add(new_no)
            new_no += 1
    return added


def non_prose_lines(lines: list[str]) -> set[int]:
    """1-based numbers of lines that are not prose, computed from the whole file."""
    skip: set[int] = set()
    start = 0
    if lines and lines[0].strip() == "---":
        for idx in range(1, len(lines)):
            if lines[idx].strip() == "---":
                skip.update(range(1, idx + 2))
                start = idx + 1
                break
    fence: tuple[str, int] | None = None
    for idx in range(start, len(lines)):
        no = idx + 1
        text = lines[idx]
        marker = FENCE_RE.match(text)
        if fence is not None:
            skip.add(no)
            if marker and marker.group(1)[0] == fence[0] and len(marker.group(1)) >= fence[1]:
                fence = None
            continue
        if marker:
            skip.add(no)
            fence = (marker.group(1)[0], len(marker.group(1)))
            continue
        stripped = text.strip()
        if not stripped or stripped.startswith(("#", "|", ":::", "<", "{", "import ", "export ")):
            skip.add(no)
    return skip


def clean(text: str) -> str:
    text = LIST_RE.sub("", text, count=1)
    text = CODE_SPAN_RE.sub("", text)
    text = IMAGE_OR_LINK_RE.sub(r"\1", text)
    text = TAG_RE.sub("", text)
    text = EMPHASIS_RE.sub("", text)
    return text.lstrip("> ")


def paragraphs(lines: list[str], skip: set[int]) -> list[list[tuple[int, str]]]:
    out: list[list[tuple[int, str]]] = []
    current: list[tuple[int, str]] = []
    for idx, text in enumerate(lines):
        no = idx + 1
        if no in skip:
            if current:
                out.append(current)
                current = []
            continue
        if LIST_RE.match(text) and current:
            out.append(current)
            current = []
        current.append((no, clean(text)))
    if current:
        out.append(current)
    return out


def sentences(paragraph: list[tuple[int, str]]):
    """Yield (words, set of source line numbers) for each sentence of a paragraph."""
    text = ""
    owner: list[int] = []
    for no, part in paragraph:
        piece = part.strip() + " "
        text += piece
        owner.extend([no] * len(piece))
    for abbr in ABBREVIATIONS:
        text = text.replace(abbr, abbr[:-1] + "\x00")
    pos = 0
    for chunk in SENTENCE_END_RE.split(text):
        start = text.find(chunk, pos)
        pos = start + len(chunk)
        words = [w for w in chunk.replace("\x00", ".").split() if re.search(r"[A-Za-z0-9]", w)]
        if words:
            yield words, set(owner[start:pos])


def check_file(path: str, qc_range: str, end: str | None) -> list[tuple[int, int, str]] | None:
    added = added_lines(qc_range, path)
    if added is None:
        return None
    body = new_text(path, end) if added else None
    if body is None:
        return []
    lines = body.splitlines()
    hits = []
    for paragraph in paragraphs(lines, non_prose_lines(lines)):
        for words, line_nos in sentences(paragraph):
            touched = sorted(line_nos & added)
            if len(words) > MAX_WORDS and touched:
                hits.append((touched[0], len(words), " ".join(words[:PREVIEW_WORDS])))
    return hits


def main() -> int:
    qc_range = os.environ.get("QC_RANGE", "").strip()
    if not qc_range:
        print("QC_RANGE unset or empty; nothing to check -- skipping prose-sentence-length.")
        return 0

    print(f"Range: {qc_range}")
    names = git("diff", "--name-only", "--diff-filter=AMR", qc_range)
    if names.returncode != 0:
        print(f"git diff failed (exit {names.returncode}); skipping prose-sentence-length.", file=sys.stderr)
        if names.stderr.strip():
            print(names.stderr.strip(), file=sys.stderr)
        return 0

    end = range_end(qc_range)
    total = 0
    for path in sorted(p for p in names.stdout.splitlines() if in_scope(p)):
        hits = check_file(path, qc_range, end)
        if hits is None:
            print(f"git diff failed for {path}; skipping it.", file=sys.stderr)
            continue
        for line_no, count, preview in sorted(hits):
            print(f"{path}:{line_no}: {count} words: {preview}...")
            total += 1

    print(f"prose-sentence-length: {total} sentence(s) over {MAX_WORDS} words in added lines.")
    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main())
