#!/usr/bin/env python3
"""QC check `uc-integration-evidence`: a main-bound change needs a passing UC integration run.

The QC judge runs this with QC_RANGE set to the range being pushed or proposed:
  - `gh pr create`            -> origin/<base>..HEAD     (origin/main unless --base says otherwise)
  - push to main              -> origin/main..HEAD
  - push to another branch    -> origin/<branch>..HEAD   (not gated: this script exits 0)
  - push with no arguments    -> @{push}..HEAD           (resolved to the upstream branch here)
  - git pre-push hook         -> <sha>..<sha>            (carries no branch name: not gated)

For a range whose base is main, the evidence file written by `gbx:uc:tests`
(test-logs/uc-integration/evidence.json) must exist, say result "pass", and carry the sources_hash
of the current HEAD (scripts/ci/uc_sources_hash.py). Its scope must be "core" or "full", and "full"
when the range changes the release version (a release runs the full UC tests). Otherwise exit 1 with
one Fix line. With QC_RANGE unset the check runs as for main (a manual run answers "would a PR pass?").

The release version is read from the two files `gbx:versions:bump` edits as the library version:
`<version>` in pom.xml (the first one: the project's own) and `__version__` in
python/geobrix/src/databricks/labs/gbx/__init__.py. (`version` in python/geobrix/pyproject.toml is
dynamic: it reads `__version__`, so it needs no check of its own.) The range counts as a version change
when either value differs between the merge base of the range and HEAD.

Environment: GBX_UC_EVIDENCE overrides the evidence path; used by the tests.
"""

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import uc_sources_hash  # noqa: E402

REPO_ROOT = HERE.parents[1]
EVIDENCE = REPO_ROOT / "test-logs" / "uc-integration" / "evidence.json"
MAIN_REF = re.compile(r"^(?:[A-Za-z0-9._-]+/)?main$")
VERSION_FILES = ("pom.xml", "python/geobrix/src/databricks/labs/gbx/__init__.py")
VERSION_PATTERNS = (re.compile(r"<version>\s*([^<\s]+)\s*</version>"), re.compile(r'^__version__\s*=\s*["\']([^"\']+)["\']', re.M))
SCOPE_RANK = {"core": 1, "full": 2}


def fix_line(scope: str = "core") -> str:
    """The one Fix line; names the scope the range needs."""
    return f"Fix: run `bash scripts/commands/gbx-uc-tests.sh --profile <name> --scope {scope}` and retry."



def range_base(qc_range: str, root: Path = REPO_ROOT) -> Optional[str]:
    """The ref before `..` of the range, `@{push}` resolved to its upstream; None when it names no branch."""
    base = qc_range.split("..", 1)[0].strip()
    if base == "@{push}":
        res = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--abbrev-ref", "@{push}"], capture_output=True, text=True
        )
        base = res.stdout.strip() if res.returncode == 0 else ""
    return base or None


def is_main_bound(qc_range: Optional[str], root: Path = REPO_ROOT) -> bool:
    if not qc_range:
        return True
    base = range_base(qc_range, root)
    return bool(base and MAIN_REF.match(base))


def _versions(read) -> Tuple[Optional[str], ...]:
    out = []
    for rel, pat in zip(VERSION_FILES, VERSION_PATTERNS):
        text = read(rel)
        m = pat.search(text) if text else None
        out.append(m.group(1) if m else None)
    return tuple(out)


def version_changed(qc_range: Optional[str], root: Path = REPO_ROOT) -> bool:
    """True when the release version differs between the range's merge base and HEAD; False when it cannot tell (base missing)."""
    base = range_base(qc_range, root) if qc_range else "origin/main"
    if not base:
        return False

    def git_out(*args: str) -> Optional[str]:
        res = subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True)
        return res.stdout if res.returncode == 0 else None

    mb = git_out("merge-base", base, "HEAD")
    ref = mb.strip() if mb and mb.strip() else base
    if git_out("rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}") is None:
        return False
    old = _versions(lambda rel: git_out("show", f"{ref}:{rel}"))
    new = _versions(lambda rel: git_out("show", f"HEAD:{rel}"))
    return old != new


def evidence_scope(ev: dict) -> str:
    return str(ev.get("scope") or (ev.get("slice") or {}).get("scope") or "core")


def summary_line(ev: dict) -> str:
    """The one line the PR checklist asks for."""
    sl, ex = ev.get("slice", {}), ev.get("examples", {})
    return (
        f"gbx:uc:tests {ev.get('result', '?').upper()} at {str(ev.get('head_sha', '?'))[:12]}: "
        f"scope {evidence_scope(ev)}, slice {sl.get('passed', '?')} passed / {sl.get('failed', '?')} failed, "
        f"examples {ex.get('statements_ok', '?')}/{ex.get('total', '?')}, schema {ev.get('schema', '?')}, "
        f"finished {ev.get('finished', '?')}"
    )


def verdict(evidence_path: Path, root: Path = REPO_ROOT, need_full: bool = False) -> Tuple[bool, str]:
    """(ok, message) for the evidence file against the current sources hash and the scope the range needs."""
    fix = fix_line("full" if need_full else "core")
    if not evidence_path.is_file():
        return False, f"no UC integration evidence at {evidence_path}. {fix}"
    try:
        ev = json.loads(evidence_path.read_text())
    except ValueError as exc:
        return False, f"the UC integration evidence is not valid JSON ({exc}). {fix}"
    if ev.get("result") != "pass":
        return False, f"the last UC integration run did not pass ({summary_line(ev)}). {fix}"
    current = uc_sources_hash.sources_hash(root)
    if ev.get("sources_hash") != current:
        return False, (
            f"the UC integration evidence is stale: it covers sources {str(ev.get('sources_hash'))[:12]}, "
            f"HEAD has {current[:12]}. {fix}"
        )
    scope = evidence_scope(ev)
    if SCOPE_RANK.get(scope, 0) < SCOPE_RANK["full" if need_full else "core"]:
        why = "this range changes the release version, which needs the full UC tests" if need_full else "only a core or full run counts"
        return False, f"the UC integration evidence has scope {scope!r}; {why}. {fix}"
    return True, summary_line(ev)


def main() -> int:
    qc_range = os.environ.get("QC_RANGE")
    if not is_main_bound(qc_range):
        print(f"not a main-bound range ({qc_range}); skip")
        return 0
    evidence = Path(os.environ.get("GBX_UC_EVIDENCE") or EVIDENCE)
    ok, message = verdict(evidence, need_full=version_changed(qc_range))
    print(message, file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
