#!/usr/bin/env python3
"""QC check `uc-integration-evidence`: a main-bound change needs a passing UC integration run.

The QC judge runs this with QC_RANGE set to the range being pushed or proposed:
  - `gh pr create`            -> origin/<base>..HEAD     (origin/main unless --base says otherwise)
  - push to main              -> origin/main..HEAD
  - push to another branch    -> origin/<branch>..HEAD   (not gated: this script exits 0)
  - push with no arguments    -> @{push}..HEAD           (resolved to the upstream branch here)
  - git pre-push hook         -> <sha>..<sha>            (carries no branch name: not gated)

For a range whose base is main, the evidence file written by `gbx:test:uc-integration`
(test-logs/uc-integration/evidence.json) must exist, say result "pass", and carry the sources_hash
of the current HEAD (scripts/ci/uc_sources_hash.py). Otherwise exit 1 with one Fix line.
With QC_RANGE unset the check runs as for main (a manual run answers "would a PR pass?").

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
FIX = "Fix: run `bash scripts/commands/gbx-test-uc-integration.sh --profile <name>` and retry."


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


def summary_line(ev: dict) -> str:
    """The one line the PR checklist asks for."""
    sl, ex = ev.get("slice", {}), ev.get("examples", {})
    return (
        f"gbx:test:uc-integration {ev.get('result', '?').upper()} at {str(ev.get('head_sha', '?'))[:12]}: "
        f"slice {sl.get('passed', '?')} passed / {sl.get('failed', '?')} failed, "
        f"examples {ex.get('statements_ok', '?')}/{ex.get('total', '?')}, schema {ev.get('schema', '?')}, "
        f"finished {ev.get('finished', '?')}"
    )


def verdict(evidence_path: Path, root: Path = REPO_ROOT) -> Tuple[bool, str]:
    """(ok, message) for the evidence file against the current sources hash."""
    if not evidence_path.is_file():
        return False, f"no UC integration evidence at {evidence_path}. {FIX}"
    try:
        ev = json.loads(evidence_path.read_text())
    except ValueError as exc:
        return False, f"the UC integration evidence is not valid JSON ({exc}). {FIX}"
    if ev.get("result") != "pass":
        return False, f"the last UC integration run did not pass ({summary_line(ev)}). {FIX}"
    current = uc_sources_hash.sources_hash(root)
    if ev.get("sources_hash") != current:
        return False, (
            f"the UC integration evidence is stale: it covers sources {str(ev.get('sources_hash'))[:12]}, "
            f"HEAD has {current[:12]}. {FIX}"
        )
    return True, summary_line(ev)


def main() -> int:
    qc_range = os.environ.get("QC_RANGE")
    if not is_main_bound(qc_range):
        print(f"not a main-bound range ({qc_range}); skip")
        return 0
    evidence = Path(os.environ.get("GBX_UC_EVIDENCE") or EVIDENCE)
    ok, message = verdict(evidence)
    print(message, file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
