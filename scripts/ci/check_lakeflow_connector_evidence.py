#!/usr/bin/env python3
"""QC check `lakeflow-connector-evidence`: a main-bound change needs a passing Lakeflow connector run.

The QC judge runs this with QC_RANGE set to the range being pushed or proposed, as for
`uc-integration-evidence` (check_uc_integration_evidence.py): only a range whose base is main is gated.
With QC_RANGE unset the check runs as for main (a manual run answers "would a PR pass?").

For a main-bound range, the evidence file written by `gbx:test:lakeflow-connector`
(test-logs/lakeflow-connector/evidence.json) must exist, say result "pass", and carry the sources hash of
the current HEAD over LFC_PATHS. Otherwise exit 1 with one Fix line.

The hash is sha256 over `git ls-tree -r HEAD -- <LFC_PATHS>` (scripts/ci/uc_sources_hash.py): a later commit
that touches none of the paths (a docs page, a CI change) keeps the evidence valid.

Usage:
    python3 scripts/ci/check_lakeflow_connector_evidence.py            the QC check
    python3 scripts/ci/check_lakeflow_connector_evidence.py --hash     print the current sources hash

Environment: GBX_LFC_EVIDENCE overrides the evidence path; used by the tests.
"""

import json
import os
import sys
from pathlib import Path
from typing import Tuple

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import check_uc_integration_evidence as uc_check  # noqa: E402  (range rules shared with the UC gate)
import uc_sources_hash  # noqa: E402

REPO_ROOT = HERE.parents[1]
EVIDENCE = REPO_ROOT / "test-logs" / "lakeflow-connector" / "evidence.json"

# What a connector run proves, besides the workspace: the connector (its code, spec and pipeline source file),
# the GeoBrix read core it calls and the env wheel built from it, the documented wheel builds and dependency
# lines, and the test itself.
LFC_PATHS = (
    "connectors/lakeflow",
    "docs/tests/python/ingestion/lakeflow_connector_examples.py",
    "examples/tests/lakeflow_connector_it.py",
    "examples/tests/_lfc_expect.py",
    "python/geobrix/src",
    "python/geobrix/envs",
    "python/geobrix/pyproject.toml",
)


def sources_hash(root: Path = REPO_ROOT, rev: str = "HEAD") -> str:
    return uc_sources_hash.sources_hash(root, rev, paths=LFC_PATHS)


def fix_line() -> str:
    return "Fix: run `bash scripts/commands/gbx-test-lakeflow-connector.sh --profile <name>` and retry."


def summary_line(ev: dict) -> str:
    """The one line a PR checklist can quote."""
    checks = ev.get("checks") or []
    ok = sum(1 for c in checks if c.get("ok"))
    tables = ev.get("tables") or {}
    return (
        f"gbx:test:lakeflow-connector {str(ev.get('result', '?')).upper()} at {str(ev.get('head_sha', '?'))[:12]}: "
        f"checks {ok}/{len(checks)}, {len(tables)} table(s), pipeline {ev.get('pipeline_id', '?')}, "
        f"schema {ev.get('catalog', '?')}.{ev.get('schema', '?')}, finished {ev.get('finished', '?')}"
    )


def verdict(evidence_path: Path, root: Path = REPO_ROOT) -> Tuple[bool, str]:
    """(ok, message) for the evidence file against the current sources hash."""
    fix = fix_line()
    if not evidence_path.is_file():
        return False, f"no Lakeflow connector evidence at {evidence_path}. {fix}"
    try:
        ev = json.loads(evidence_path.read_text())
    except ValueError as exc:
        return False, f"the Lakeflow connector evidence is not valid JSON ({exc}). {fix}"
    if ev.get("result") != "pass":
        failed = [c.get("name") for c in ev.get("checks") or [] if not c.get("ok")]
        why = f"; failed checks: {', '.join(map(str, failed))}" if failed else ""
        return False, f"the last Lakeflow connector run did not pass ({summary_line(ev)}{why}). {fix}"
    current = sources_hash(root)
    if ev.get("sources_hash") != current:
        return False, (
            f"the Lakeflow connector evidence is stale: it covers sources {str(ev.get('sources_hash'))[:12]}, "
            f"HEAD has {current[:12]}. {fix}"
        )
    return True, summary_line(ev)


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv == ["--hash"]:
        print(sources_hash())
        return 0
    if argv:
        print(f"unknown arguments: {' '.join(argv)} (use --hash or none)", file=sys.stderr)
        return 2
    qc_range = os.environ.get("QC_RANGE")
    if not uc_check.is_main_bound(qc_range):
        print(f"not a main-bound range ({qc_range}); skip")
        return 0
    evidence = Path(os.environ.get("GBX_LFC_EVIDENCE") or EVIDENCE)
    ok, message = verdict(evidence)
    print(message, file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
