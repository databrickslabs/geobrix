#!/usr/bin/env python3
"""Param-name stability: the frozen canonical parameter names vs the function manifest.

The frozen fixture (docs/tests-function-info/canonical_param_names.txt) lists the parameter names
a function was published with. The function manifest (manifest/functions.json) is the live source of
truth for every surface, so this check holds the two together: for each fixture name, the stable
manifest row's parameter names must start with the fixture's names, in order (a row may add trailing
parameters). A renamed, reordered or dropped published parameter is a violation. Functions listed in
param_name_waiver.txt are exempt while a rename migration is underway.

Whether each surface (the generated Python entry, the lightweight API, the Scala wrappers, the
function-info usage) spells the manifest's names is checked by gbx:manifest:gen --check (the
parameter-name gate); this script does not re-read those surfaces.

An empty source is a failure, never a pass: no manifest rows, an empty fixture, or a fixture name with
no stable row means a source moved and nothing was verified.
Pure stdlib; runs on the host. Exit 0 on pass, 1 on any non-waived violation.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _manifest_names import stable_rows  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
FIXTURE = REPO / "docs/tests-function-info/canonical_param_names.txt"
WAIVER = REPO / "docs/tests-function-info/param_name_waiver.txt"


def _brackets_stripped(tokens: list[str]) -> list[str]:
    return [t.strip().strip("[]").strip() for t in tokens if t.strip()]


def load_fixture() -> dict[str, list[str]]:
    out = {}
    for line in FIXTURE.read_text().splitlines():
        line = line.rstrip()
        if not line or line.startswith("#"):
            continue
        name, _, args = line.partition("\t")
        out[name.strip()] = _brackets_stripped(args.split(","))
    return out

def load_waiver() -> set[str]:
    if not WAIVER.exists():
        return set()
    entries = set()
    for l in WAIVER.read_text().splitlines():
        stripped = l.strip()
        if not stripped or stripped.startswith("#"):
            continue
        # Strip inline comment (e.g. "gbx_foo  # reason")
        name = stripped.split("#")[0].strip()
        if name:
            entries.add(name)
    return entries

def check_against_manifest(report: bool = False) -> list[str]:
    """Violations: a fixture name whose manifest row does not start with the published parameter names."""
    fixture, waiver = load_fixture(), load_waiver()
    rows = stable_rows()
    violations = []
    if not rows:
        violations.append("[empty] the manifest has no stable rows; nothing was verified")
    if not fixture:
        violations.append(f"[empty] {FIXTURE.name} lists no functions; nothing was verified")
    for gbx_name, canon in fixture.items():
        if not report and gbx_name in waiver:
            continue
        row = rows.get(gbx_name)
        if row is None:
            violations.append(f"[A] {gbx_name}: in the fixture but not a stable manifest row")
            continue
        names = [p["name"] for p in row.get("params", [])]
        if names[: len(canon)] != canon:
            violations.append(
                f"[A] {gbx_name}: manifest params {names} do not start with the published {canon}"
            )
    return violations


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", action="store_true",
                    help="list ALL violations ignoring the waiver")
    args = ap.parse_args()
    violations = check_against_manifest(args.report)
    if violations:
        print("\n".join(sorted(violations)))
        print(f"\n{len(violations)} param-name violation(s).")
        return 1
    print(f"check-param-names: OK ({len(load_fixture())} published signatures held to the manifest)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
