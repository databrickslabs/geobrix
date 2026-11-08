"""Guards for the sharded light CI job.

The light tier runs as parallel shards (the `light` job matrix in build_main.yml), always on the native bindings and the
Rust module (the `light-payload` job). These tests read the workflow and the CI actions and fail unless:

* the shards together run every test file of the light dirs of `_LIGHT_TEST_DIRS` (test/conftest.py) exactly once, so a
  new light dir or file that no shard runs, or that two shards run, goes red. Shards are lists of paths (a dir, or a
  file), optionally one hashed part of a dir (`part`: i/n, GBX_SHARD; the hash is conftest's `shard_of`) and optionally
  the oracle tables of test_family_cross_tier.py (`oracle_tables`: that one file runs once per table, every oracle
  table of the manifest in exactly one shard);
* the heavy build ignores the same dirs (`LIGHT_IGNORES`) and the unsharded fallback line of the light action lists them;
* `test/bench` runs as exactly three shards, `bench 1/3`, `bench 2/3` and `bench 3/3`, each on its own (bench relies on no
  state another dir leaves in the process): test_levels.py (with pyvx) and test_levels_legs.py, the two long files, are
  pinned to a shard each so the three stay balanced;
* the heavy build waits only for the native payload (it is no longer behind the light shards), and nothing can quietly
  drop a shard, a path or the native bindings: the matrix has no `exclude`/`include`, and the job and the action step
  carry no `if`, `continue-on-error` or skip input; the action fails a shard whose bindings do not load from the payload.
"""

import ast
import re
import sys
import zlib
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
PKG = REPO_ROOT / "python" / "geobrix"
CONFTEST = PKG / "test" / "conftest.py"
FAMILY = "test/manager/test_family_cross_tier.py"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "build_main.yml"
LIGHT_ACTION = REPO_ROOT / ".github" / "actions" / "pyrx_build" / "action.yml"
HEAVY_ACTION = REPO_ROOT / ".github" / "actions" / "python_build" / "action.yml"

HOW_TO_FIX = "Put it in exactly one shard of the `light` job in build_main.yml"


def _conftest_tree():
    return ast.parse(CONFTEST.read_text())


def _light_test_dirs():
    return ast.literal_eval(
        next(
            n.value
            for n in _conftest_tree().body
            if isinstance(n, ast.Assign)
            and any(getattr(t, "id", "") == "_LIGHT_TEST_DIRS" for t in n.targets)
        )
    )


def _shard_of():
    """conftest's own `shard_of`, compiled from its source (the conftest imports pytest and is not importable here)."""
    fn = next(
        n
        for n in _conftest_tree().body
        if isinstance(n, ast.FunctionDef) and n.name == "shard_of"
    )
    ns = {"zlib": zlib}
    exec(compile(ast.Module([fn], []), str(CONFTEST), "exec"), ns)
    return ns["shard_of"]


def _jobs():
    return yaml.safe_load(WORKFLOW.read_text())["jobs"]


def _shards():
    return _jobs()["light"]["strategy"]["matrix"]["shard"]


def _test_files(path):
    """The test files under a path of python/geobrix (a dir, or a file), as 'test/...' strings."""
    p = PKG / path
    assert p.exists(), f"{path} does not exist"
    files = [p] if p.is_file() else [*p.rglob("test_*.py"), *p.rglob("*_test.py")]
    return {f.relative_to(PKG).as_posix() for f in files}


def _files_of(shard):
    """The test files one shard runs: its paths, less its ignores, narrowed to its part."""
    files = set()
    for path in shard["paths"].split():
        files |= _test_files(path)
    for ignored in shard.get("ignore", "").split():
        files -= _test_files(ignored)
    if shard.get("part"):
        i, n = (int(x) for x in shard["part"].split("/"))
        assert 0 <= i < n
        shard_of = _shard_of()
        files = {f for f in files if shard_of(f, n) == i}
    return files


def _all_light_files():
    return set().union(*(_test_files(f"test/{d}") for d in _light_test_dirs()))


def _oracle_tables():
    """Every oracle table test_family_cross_tier.py runs: the table of every stable function."""
    sys.path.insert(0, str(PKG / "src"))
    from databricks.labs.gbx import manifest

    tables = {
        f.oracle
        for f in manifest.load().functions.values()
        if f.status == "stable"
    }
    # the helpers table has rows of rasterx.helpers and no manifest function
    return tables | {"MVP_H"}


def test_the_shards_run_every_light_test_file_exactly_once():
    runs = {}
    for shard in _shards():
        for f in _files_of(shard):
            runs.setdefault(f, []).append(shard["name"])
    everything = _all_light_files()
    outside = sorted(set(runs) - everything)
    assert not outside, f"run by a shard but in no light dir: {outside}"
    missing = sorted(everything - set(runs))
    assert not missing, f"in a light dir but in no shard: {missing}. {HOW_TO_FIX}"
    # the family file runs once per oracle table group (below); every other file in exactly one shard
    twice = {f: s for f, s in runs.items() if len(s) > 1 and f != FAMILY}
    assert not twice, f"in more than one shard: {twice}. {HOW_TO_FIX}"


def test_the_family_file_runs_every_oracle_table_in_exactly_one_shard():
    running = [s for s in _shards() if FAMILY in _files_of(s)]
    assert len(running) > 1
    groups = []
    for shard in running:
        # without tables a shard would run the whole file (GBX_ORACLE_TABLES unset)
        assert shard.get("oracle_tables"), f"{shard['name']} runs {FAMILY} with no oracle_tables"
        groups.append(shard["oracle_tables"].split(","))
    flat = [t for g in groups for t in g]
    assert sorted(set(flat) ^ _oracle_tables()) == [], (
        f"oracle tables in no shard: {sorted(_oracle_tables() - set(flat))}; "
        f"named but not in the manifest: {sorted(set(flat) - _oracle_tables())}"
    )
    assert len(flat) == len(set(flat)), f"in more than one shard: {sorted(t for t in set(flat) if flat.count(t) > 1)}"
    # only that file takes tables, and nothing else takes a part of it
    assert all(
        not s.get("oracle_tables") or s["paths"].split() == [FAMILY] for s in _shards()
    )


def test_the_heavy_build_ignores_the_light_dirs_and_the_fallback_lists_them():
    light = sorted(_light_test_dirs())
    heavy = re.search(r'LIGHT_IGNORES="([^"]*)"', HEAVY_ACTION.read_text()).group(1)
    ignored = [i.removeprefix("--ignore=test/") for i in heavy.split()]
    assert sorted(ignored) == light, "LIGHT_IGNORES (python_build) differs from _LIGHT_TEST_DIRS"
    # gbx:test:python and gbx:test:light read the unsharded fallback line
    fallback = re.search(
        r"^\s*pytest (test/\S+(?: test/\S+)*) -m ", LIGHT_ACTION.read_text(), re.M
    ).group(1)
    assert sorted(d.removeprefix("test/") for d in fallback.split()) == light


_BENCH_SHARDS = ["bench 1/3", "bench 2/3", "bench 3/3"]
_BENCH_PINNED = ["test/bench/test_levels.py", "test/bench/test_levels_legs.py"]


def test_bench_runs_as_three_self_sufficient_shards_with_the_long_files_pinned():
    shards = [s for s in _shards() if s["name"].startswith("bench")]
    assert [s["name"] for s in shards] == _BENCH_SHARDS
    first, second, rest = (_files_of(s) for s in shards)
    bench = _test_files("test/bench")
    # together they are all of bench; the shard-wide check above proves each file is in exactly one
    assert (first | second | rest) >= bench
    # each long file is alone in its shard (the first with pyvx); the third shard is the rest of the dir
    pinned_first, pinned_second = _BENCH_PINNED
    assert pinned_first in first and first & bench == {pinned_first}
    assert second == {pinned_second}
    assert rest == bench - set(_BENCH_PINNED)
    # self-sufficient: no bench shard needs a dir another shard leaves behind, so the other dirs of a shard are only
    # pyvx, and the shards without pyvx (the second and third) still hold all of bench but the first's pinned file
    assert {f.split("/")[1] for f in first | second | rest} == {"bench", "pyvx"}
    # a bench test that imports a pyvx test module would reintroduce a cross-dir dependency
    for f in bench:
        text = (PKG / f).read_text()
        assert not re.search(r"^\s*(from|import)\s+(test\.)?pyvx\.test_", text, re.M), f


def test_the_graph_and_the_labels():
    jobs = _jobs()
    # heavy and light run in parallel; both wait only for what they read
    assert jobs["build"]["needs"] == "native-payload"
    assert jobs["light-payload"]["needs"] == "native-payload"
    assert jobs["light"]["needs"] == "light-payload"
    assert jobs["build"]["name"] == "Heavy tests (Scala + JAR)"
    assert jobs["light"]["name"].startswith("Light tests - ${{ matrix.shard.name }} (py")
    # the heavy actions read matrix.python and matrix.gdal: no other dimension is left in the job
    assert set(jobs["build"]["strategy"]["matrix"]) == {"python", "gdal"}


def test_the_light_job_cannot_hide_a_shard_or_test_without_the_native_bindings():
    job = _jobs()["light"]
    assert job["strategy"]["fail-fast"] is False
    # Only these two dimensions: an `exclude` (or `include`) could remove or add a shard
    # that the guard above never sees, because it reads `shard` alone.
    assert set(job["strategy"]["matrix"]) == {"python", "shard"}
    # A job-level condition or continue-on-error would let a failing shard pass.
    assert not {"if", "continue-on-error"} & set(job)
    for shard in _shards():
        assert set(shard) <= {"name", "paths", "ignore", "part", "oracle_tables", "delta"}, shard
    (step,) = [s for s in job["steps"] if s.get("uses") == "./.github/actions/pyrx_build"]
    # no `native` or `run_tests` input: a light shard always tests on the payload
    assert step["with"] == {
        "python_version": "${{ matrix.python }}",
        "paths": "${{ matrix.shard.paths }}",
        "ignore": "${{ matrix.shard.ignore }}",
        "part": "${{ matrix.shard.part }}",
        "oracle_tables": "${{ matrix.shard.oracle_tables }}",
        "delta": "${{ matrix.shard.delta || 'false' }}",
    }
    assert not {"if", "continue-on-error"} & set(step)
    # the payload job feeds the shards, and the action's check fails the shard when a binding does not load from it
    action = yaml.safe_load(LIGHT_ACTION.read_text())["runs"]["steps"]
    (check,) = [s for s in action if "light_native.py check" in s.get("run", "")]
    assert check["if"] == "inputs.native == 'true'" and "continue-on-error" not in check
    assert any(
        s.get("with", {}).get("name") == "light-native-payload" and s.get("if") == "inputs.native == 'true'"
        for s in action
    )
    # the one job that skips the payload runs no light test
    (scripts,) = [
        s
        for s in _jobs()["light-scripts"]["steps"]
        if s.get("uses") == "./.github/actions/pyrx_build"
    ]
    assert scripts["with"]["native"] == "false" and scripts["with"]["run_tests"] == "false"
