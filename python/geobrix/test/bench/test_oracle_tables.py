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


def test_concurrent_first_reads_import_the_family_modules_once(monkeypatch):
    import threading
    import time

    from databricks.labs.gbx.bench import oracles

    calls = []

    def slow_tables():
        calls.append(1)
        time.sleep(0.2)
        return {"T": ("row",)}

    monkeypatch.setattr(oracles, "_tables", slow_tables)
    tables, seen = oracles._Tables(), []
    threads = [
        threading.Thread(target=lambda: seen.append(dict(tables.items())))
        for _ in range(6)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(calls) == 1 and seen == [{"T": ("row",)}] * 6  # none saw a partial dict


def test_a_builder_a_family_module_registers_on_import_is_found_before_any_table_read(
    monkeypatch,
):
    """`_build` finds the tables first, which imports the family modules and so registers their builders."""
    from databricks.labs.gbx.bench import oracles

    def family_import():
        oracles.BUILDERS["late_fixture"] = lambda tmpdir: "late"
        return {}

    monkeypatch.setattr(oracles, "_tables", family_import)
    monkeypatch.setattr(oracles, "TABLES", oracles._Tables())
    monkeypatch.delitem(oracles.BUILDERS, "late_fixture", raising=False)
    try:
        assert oracles.resolve_arg(oracles.Build("late_fixture"), "/x") == "late"
    finally:
        oracles.BUILDERS.pop("late_fixture", None)


def test_the_aggregate_view_is_typed_from_the_manifest_params():
    import dataclasses

    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.bench import oracles

    fn = manifest.load().functions["gbx_rst_bng_rasterize_agg"]
    case = next(c for c in getattr(oracles, fn.oracle) if c.fn == fn.sql_name)
    assert oracles.argument_schema(case, []) == "c0 STRING, c1 DOUBLE"
    wide = dataclasses.replace(
        fn,
        params=(dataclasses.replace(fn.params[0], session=("BIGINT",)), *fn.params[1:]),
    )
    from types import SimpleNamespace

    man = SimpleNamespace(functions={fn.sql_name: wide})
    import unittest.mock

    with unittest.mock.patch.object(manifest, "load", lambda: man):
        assert oracles.argument_schema(case, []) == "c0 BIGINT, c1 DOUBLE"


def test_an_aggregate_verdict_checks_the_resolved_arguments(monkeypatch):
    import dataclasses

    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.bench import oracles

    fn = manifest.load().functions["gbx_rst_bng_rasterize_agg"]
    case = next(c for c in getattr(oracles, fn.oracle) if c.fn == fn.sql_name)
    case = dataclasses.replace(case, args=(oracles.Build("late_agg"), *case.args[1:]))
    monkeypatch.setitem(oracles.BUILDERS, "late_agg", lambda tmpdir: ["resolved"])
    seen = []
    monkeypatch.setattr(oracles, "check", lambda c, out, args: seen.append(args))
    oracles.verdict(case, ["out"], [], "/tmp")
    assert seen[0][0] == ["resolved"]


@pytest.mark.parametrize(
    "kind,expect,good,bad",
    [
        (
            "geom_equals",
            "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
            "POLYGON((1 0, 1 1, 0 1, 0 0, 1 0))",  # the same ring, another start vertex
            "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))",
        ),
        ("cell_set", ["TQ3080", "TQ3180"], ["TQ3180", "TQ3080"], ["TQ3080", "TQ3080"]),
        ("approx", (1.0, 2.0), (1.0 + 1e-12, 2.0), (1.0, 2.1)),
    ],
)
def test_a_new_check_kind_passes_its_value_and_fails_another(kind, expect, good, bad):
    from databricks.labs.gbx.bench import oracles

    case = oracles.Case("probe", "gbx_bng_aswkt", ("TQ3080",), kind, expect)
    assert oracles.check(case, good, case.args) is None
    assert oracles.check(case, bad, case.args) is not None
    assert oracles.check(case, None, case.args) is not None
