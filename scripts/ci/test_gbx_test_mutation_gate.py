"""Tests for gbx:test:mutation-gate and scripts/mutation/gate.py: the command's shell, the argument handling
and the verdict maths (no mutmut run: that is minutes of work, done by the command itself)."""

import importlib.util
import json
import subprocess
import sys

import pytest
from conftest import REPO_ROOT

_spec = importlib.util.spec_from_file_location("mutation_gate", REPO_ROOT / "scripts" / "mutation" / "gate.py")
gate = importlib.util.module_from_spec(_spec)
sys.modules["mutation_gate"] = gate
_spec.loader.exec_module(gate)

CMD = REPO_ROOT / "scripts" / "commands" / "gbx-test-mutation-gate.sh"
P = "databricks.labs.gbx.pygx"


def _sh(*args):
    return subprocess.run(["bash", str(CMD), *args], capture_output=True, text=True, cwd=REPO_ROOT)


def test_help_exits_zero_and_names_every_option():
    res = _sh("--help")
    assert res.returncode == 0
    for opt in ("--family", "--modules", "--tests", "--slim-tests", "--oracle-tables", "--out", "--plan", "--log"):
        assert opt in res.stdout


def test_an_unknown_option_is_refused():
    res = _sh("--bogus")
    assert res.returncode == 1
    assert "Unknown option" in res.stderr


def test_a_run_without_arguments_is_refused():
    assert _sh().returncode == 2


def test_the_command_has_its_registration_and_is_executable():
    assert (REPO_ROOT / "scripts" / "commands" / "gbx-test-mutation-gate.md").exists()
    assert CMD.stat().st_mode & 0o111


def test_plan_resolves_modules_and_tests_and_runs_nothing(tmp_path):
    res = _sh(
        "--family", "unit", "--modules", "pygx/_bng,pygx._custom",
        "--tests", "python/geobrix/test/pygx/test_bng_codec.py --ignore=python/geobrix/test/pygx/test_parity_bng.py",
        "--slim-tests", "test/manager/test_family_cross_tier.py::test_family_oracle_l3_callables",
        "--oracle-tables", "GRID_SCALAR", "--plan",
    )
    assert res.returncode == 0, res.stderr
    plan = json.loads(res.stdout[res.stdout.index("{"):])
    assert plan["modules"] == [
        "src/databricks/labs/gbx/pygx/_bng.py",
        "src/databricks/labs/gbx/pygx/_custom.py",
    ]
    assert plan["old"] == ["test/pygx/test_bng_codec.py", "--ignore=test/pygx/test_parity_bng.py"]
    assert plan["oracle_tables"] == "GRID_SCALAR"
    assert "/gbx-mut-unit-" in plan["work"] and "mutate_only_covered_lines = true" in plan["mutmut"]


def test_a_missing_module_or_test_is_refused():
    base = ["--family", "unit", "--tests", "test/pygx/test_bng_codec.py", "--slim-tests", "test/pygx/test_bng_codec.py", "--plan"]
    assert _sh("--modules", "pygx/_nope", *base).returncode != 0
    res = _sh("--modules", "pygx/_bng", "--family", "unit", "--tests", "test/pygx/test_nope.py", "--slim-tests", "test/pygx/test_bng_codec.py", "--plan")
    assert res.returncode != 0 and "do not exist" in res.stderr


def test_a_module_level_pyspark_import_is_reported(tmp_path):
    (tmp_path / "test_a.py").write_text("from pyspark.sql import SparkSession\n")
    (tmp_path / "test_b.py").write_text("def test_x():\n    import pyspark\n")
    files = gate.selected_files(["."], tmp_path)
    assert [f.name for f in gate.pyspark_importers(files)] == ["test_a.py"]


def _codes(spec):
    return {f"{P}.{m}.x_{f}__mutmut_{i}": c for (m, f, i), c in spec.items()}


def test_a_mutant_absent_from_the_slim_run_is_not_killed():
    old = _codes({("_bng", "f", 1): 1, ("_bng", "f", 2): 1, ("_bng", "f", 3): 0})
    new = _codes({("_bng", "f", 1): 1})  # mutant 2 sits on a line only an old test covered
    K, ok = gate.kept(old, new)
    assert len(K) == 2 and len(ok) == 1
    assert gate.judge(old, new, {"sample50": gate.sample(old, 50)})["pass"] is False


def test_the_verdict_passes_when_the_slim_run_keeps_the_old_kills_and_fails_on_a_module_drop():
    old = _codes({("_a", "f", i): 1 for i in range(100)} | {("_b", "g", i): 1 for i in range(3)})
    same = dict(old)
    v = gate.judge(old, same, {"sample50": gate.sample(old, 50), "sample100": gate.sample(old, 100)})
    assert v["pass"] and v["rows"]["all"]["pct"] == 100.0
    new = dict(old)
    for k in [k for k in old if k.split(".")[-2] == "_b"][:1]:  # 1 of 3 in _b: a 33 point drop, 1% overall
        new[k] = 0
    v = gate.judge(old, new, {"sample50": gate.sample(old, 50), "sample100": gate.sample(old, 100)})
    assert not v["pass"] and any("module _b" in f for f in v["failures"])


def test_the_sample_is_seeded_and_stratified_by_function():
    codes = _codes({("_m", f"fn{j}", i): i % 2 for j in range(10) for i in range(20)})
    a, b = gate.sample(codes, 10), gate.sample(codes, 10)
    assert a == b
    assert len({gate.mutant_function(k) for k in a}) == 10  # one mutant per function before any function repeats
    assert gate.sample(codes, 10, seed=1) != a
    assert len(gate.sample(codes, 1000)) == len(codes)


@pytest.mark.parametrize(
    "key,module,func",
    [
        (f"{P}._bng.x_cell_to_geom__mutmut_12", "_bng", "cell_to_geom"),
        (f"{P}._bng.x__helper__mutmut_3", "_bng", "_helper"),
        (f"{P}._custom.xǁGridǁcell__mutmut_1", "_custom", "cell"),
    ],
)
def test_mutant_keys_name_their_module_and_function(key, module, func):
    assert (gate.mutant_module(key), gate.mutant_function(key)) == (module, func)
