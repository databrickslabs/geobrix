"""Manifest names for the transition-era checks (stdlib only). A stable manifest row is covered by
gbx:manifest:gen --check; these scripts keep checking the names not yet in the manifest."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_JSON = ROOT / "python/geobrix/src/databricks/labs/gbx/manifest/functions.json"
_LEGACY = ROOT / "docs/tests-function-info/registered_functions.txt"


def stable_names() -> set:
    raw = json.loads(_JSON.read_text())
    return {r["sql_name"] for r in raw["functions"] if r.get("status", "stable") == "stable"}


def all_names() -> set:
    legacy = set()
    if _LEGACY.exists():
        legacy = {ln.strip() for ln in _LEGACY.read_text().splitlines() if ln.strip() and not ln.startswith("#")}
    return legacy | stable_names()
