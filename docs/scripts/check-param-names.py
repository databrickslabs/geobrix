#!/usr/bin/env python3
"""Invariant A (cross-tier Python param-name equality) + Invariant B (arity parity).

Compares live heavy-Python and light-Python signatures against the frozen
canonical fixture (docs/tests-function-info/canonical_param_names.txt). Functions
listed in param_name_waiver.txt are exempt while the rename migration is underway.
Pure stdlib; runs on the host. Exit 0 on pass, 1 on any non-waived violation.
"""
from __future__ import annotations
import argparse, re, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
FIXTURE = REPO / "docs/tests-function-info/canonical_param_names.txt"
WAIVER = REPO / "docs/tests-function-info/param_name_waiver.txt"
PY_SRC = REPO / "python/geobrix/src"

# Heavy shim files (databricks.labs.gbx.<pkg>) and light bindings (pyrx/pyvx/pygx).
# Exclude build/lib (build artifact).
HEAVY_GLOBS = ["databricks/labs/gbx/rasterx/functions.py",
               "databricks/labs/gbx/vectorx/functions.py",
               "databricks/labs/gbx/gridx/bng/functions.py",
               "databricks/labs/gbx/gridx/grid/functions.py",
               "databricks/labs/gbx/gridx/h3/functions.py"]
LIGHT_GLOBS = ["databricks/labs/gbx/pyrx/functions.py",
               "databricks/labs/gbx/pyvx/functions.py",
               "databricks/labs/gbx/pygx/functions.py"]

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
    return {l.strip() for l in WAIVER.read_text().splitlines()
            if l.strip() and not l.startswith("#")}

def _find_def(text: str, pyname: str) -> str | None:
    # Match `def <pyname>(` and capture the full parenthesized arg list across newlines.
    m = re.search(rf"def\s+{re.escape(pyname)}\s*\((.*?)\)\s*(->|:)", text, re.S)
    return m.group(1) if m else None

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def extract_py_params(path: Path, gbx_name: str) -> list[str] | None:
    pyname = gbx_name[len("gbx_"):]
    if not path.exists():
        return None
    arglist = _find_def(path.read_text(), pyname)
    if arglist is None:
        return None
    params = []
    for raw in arglist.split(","):
        tok = raw.strip()
        if not tok or tok.startswith("*"):
            continue
        pname = tok.split(":")[0].split("=")[0].strip()
        # Skip malformed tokens from Union[T, None] comma-splits (e.g. "None]")
        if pname and pname != "self" and _IDENT_RE.match(pname):
            params.append(pname)
    return params

def _first_existing(globs: list[str], gbx_name: str) -> list[str] | None:
    for g in globs:
        params = extract_py_params(PY_SRC / g, gbx_name)
        if params is not None:
            return params
    return None

def check_invariant_a(report: bool = False) -> list[str]:
    fixture, waiver = load_fixture(), load_waiver()
    violations = []
    for gbx_name, canon in fixture.items():
        heavy = _first_existing(HEAVY_GLOBS, gbx_name)
        light = _first_existing(LIGHT_GLOBS, gbx_name)
        # Only compare surfaces that exist (not every fn has both tiers).
        for tier, params in (("heavy", heavy), ("light", light)):
            if params is None:
                continue
            if params != canon:
                msg = (f"[A] {gbx_name}: {tier} params {params} != canonical {canon}")
                if report or gbx_name not in waiver:
                    violations.append(msg)
    return violations

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", action="store_true",
                    help="list ALL violations ignoring the waiver")
    args = ap.parse_args()
    violations = check_invariant_a(report=args.report)
    if violations:
        print("\n".join(sorted(violations)))
        print(f"\n{len(violations)} param-name violation(s).")
        return 1
    print("check-param-names: OK")
    return 0

if __name__ == "__main__":
    sys.exit(main())
