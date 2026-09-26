#!/usr/bin/env python3
"""check-centralized-primitives — flag ad-hoc reimplementation of centralizing primitives.

Reads docs/scripts/centralized-primitives.tsv (primitive, signature, allow, level, rule);
greps tracked .py/.ipynb source. `warn`-level hits print as non-blocking advisories (exit 0);
`fail`-level hits print as blocking violations (exit 1). Skips gracefully if the registry
is absent. Mirrors the other docs/scripts/check-*.py QC checks."""
from __future__ import annotations

import csv
import fnmatch
import re
import subprocess
import sys
from pathlib import Path

_SKIP_PARTS = {"target", "build-static-zip", "coverage-report", ".pytest_cache",
               "__pycache__", ".git", "m2"}


def _root() -> Path:
    return Path(subprocess.check_output(
        ["git", "rev-parse", "--show-toplevel"], text=True).strip())


def _registry_path() -> Path:
    return _root() / "docs/scripts/centralized-primitives.tsv"


def _load_registry() -> list[dict]:
    rows = []
    with open(_registry_path(), newline="") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if not r.get("primitive") or r["primitive"].lstrip().startswith("#"):
                continue
            rows.append(r)
    return rows


def _gather_files() -> dict[str, str]:
    root = _root()
    out = subprocess.check_output(
        ["git", "-C", str(root), "ls-files", "*.py", "*.ipynb"], text=True)
    files = {}
    for rel in out.splitlines():
        if any(p in _SKIP_PARTS for p in Path(rel).parts):
            continue
        try:
            files[rel] = (root / rel).read_text(encoding="utf-8", errors="replace")
        except OSError:
            pass
    return files


def _evaluate(rows: list[dict], files: dict[str, str]) -> tuple[list[str], list[str]]:
    """Pure core: return (advisories, violations) as 'primitive\\tpath:line\\ttext' strings.
    warn-level hits → advisories; fail-level hits → violations. `allow` globs suppress hits."""
    advisories: list[str] = []
    violations: list[str] = []
    for row in rows:
        pat = re.compile(row["signature"])
        allow = [g.strip() for g in (row.get("allow") or "").split(",") if g.strip()]
        level = (row.get("level") or "warn").strip()
        for rel, text in files.items():
            if any(fnmatch.fnmatch(rel, g) for g in allow):
                continue
            for i, line in enumerate(text.splitlines(), 1):
                if pat.search(line):
                    entry = f"{row['primitive']}\t{rel}:{i}\t{line.strip()[:100]}"
                    (violations if level == "fail" else advisories).append(entry)
    return advisories, violations


def main() -> int:
    if not _registry_path().exists():
        print("centralized-primitives: registry absent; skip")
        return 0
    rows = _load_registry()
    advisories, violations = _evaluate(rows, _gather_files())
    rule_by = {r["primitive"]: r.get("rule", "") for r in rows}
    if advisories:
        print(f"[advisory] {len(advisories)} warn-level hit(s) (non-blocking; use the canonical helper):")
        for a in advisories[:40]:
            prim = a.split("\t", 1)[0]
            print(f"    {a}    → see {rule_by.get(prim,'')}")
    if violations:
        print(f"[VIOLATION] {len(violations)} fail-level hit(s) — reinvented a centralized primitive:")
        for v in violations:
            prim = v.split("\t", 1)[0]
            print(f"    {v}    → use the canonical helper (see {rule_by.get(prim,'')})")
        print("centralized-primitives: FAIL-level violations found.")
        return 1
    print("centralized-primitives: OK (no fail-level violations; advisories are non-blocking).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
