"""The row seeder (a migration tool): its self-check over the committed manifest finds a light registration for every
function the manifest holds."""

import csv
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "manifest"))

import seed_rows  # noqa: E402


def test_the_seeder_finds_a_light_registration_for_every_committed_row(tmp_path):
    rows = json.loads((seed_rows.PKG / "manifest/functions.json").read_text())[
        "functions"
    ]
    rows = [r for r in rows if not r["sql_name"].startswith("gbx_mvp_")]
    klass = {}
    for cls, family in seed_rows.FAMILY_OF_CLASS.items():
        klass.setdefault(family, cls)
    path = tmp_path / "classification.csv"
    with path.open("w", newline="") as fh:
        out = csv.DictWriter(fh, ["name", "family", "kind"])
        out.writeheader()
        for r in rows:
            out.writerow(
                {"name": r["sql_name"], "family": klass[r["family"]], "kind": r["kind"]}
            )
    _, report = seed_rows.seed(sorted(set(klass.values())), path, include_existing=True)
    assert [line for line in report if line.startswith("MISSING")] == []
