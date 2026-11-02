"""The oracle tables are found on first use of the mapping, by any read, not only by a missing-key lookup."""

import subprocess
import sys

import pytest

PROBES = {
    "sorted": "sorted(oracles.TABLES)",
    "in": "'MVP_A' in oracles.TABLES",
    "len": "len(oracles.TABLES)",
    "get": "oracles.TABLES.get('MVP_A')",
    "keys": "list(oracles.TABLES.keys())",
    "items": "list(oracles.TABLES.items())",
    "values": "list(oracles.TABLES.values())",
    "iter": "list(iter(oracles.TABLES))",
}


@pytest.mark.parametrize("probe", sorted(PROBES))
def test_a_fresh_import_finds_the_tables_on_any_read(probe):
    """Each probe runs in a fresh interpreter, so nothing has looked a key up first."""
    code = (
        "from databricks.labs.gbx.bench import oracles; "
        f"r = {PROBES[probe]}; "
        "print(bool(r))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "True", probe
