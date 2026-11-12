"""Tests for gbx:test:databricks: the shards read from CI's files, the selection (`affected` included), the plan, the queue
and the runners that take shards from it, the notebooks and the result files. No network and no workspace."""

import contextlib
import importlib.util
import json
import multiprocessing
import os
import subprocess
import sys
import textwrap
import threading
import zlib
from pathlib import Path

import pytest
import yaml
from conftest import REPO_ROOT

CMD = REPO_ROOT / "scripts" / "commands" / "gbx-test-databricks.sh"
TESTS = REPO_ROOT / "examples" / "tests"
PKG = REPO_ROOT / "python" / "geobrix"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


sys.path.insert(0, str(TESTS))
ts = _load("_test_shards", TESTS / "_test_shards.py")
sr = _load("_shard_runner", TESTS / "_shard_runner.py")
tm = _load("_timings", TESTS / "_timings.py")
launcher = _load("push_and_run_tests_on_cluster", TESTS / "push_and_run_tests_on_cluster.py")


@pytest.fixture(scope="module")
def shards():
    return ts.load(REPO_ROOT)


def _sh(*args):
    return subprocess.run(["bash", str(CMD), *args], capture_output=True, text=True, cwd=REPO_ROOT)


# ---- the command's shell


def test_help_exits_zero_and_names_every_option():
    res = _sh("--help")
    assert res.returncode == 0
    for opt in ("--profile", "--purpose", "--shards", "--sha", "--wheel", "--max-parallel", "--serverless", "--explain-only", "--fresh-cluster", "--log"):
        assert opt in res.stdout


def test_an_unknown_option_is_refused():
    res = _sh("--bogus")
    assert res.returncode == 2 and "Unknown option" in res.stderr


def test_a_launch_without_a_purpose_is_refused():
    assert _sh("--profile", "p", "--shards", "all-light").returncode == 2
    res = _sh("--profile", "p", "--purpose", "  ")
    assert res.returncode == 2 and "--purpose" in res.stderr


def test_the_launcher_refuses_a_launch_without_a_purpose_or_a_profile(monkeypatch):
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    assert launcher.main(["--profile", "p"]) == launcher.EXIT_USAGE
    assert launcher.main(["--purpose", "x"]) == launcher.EXIT_USAGE


def test_the_command_has_its_registration_and_is_executable():
    assert (REPO_ROOT / "scripts" / "commands" / "gbx-test-databricks.md").exists()
    assert CMD.stat().st_mode & 0o111


# ---- the shards come from CI's files


def test_the_light_shards_are_the_workflows_matrix(shards):
    rows = yaml.safe_load((REPO_ROOT / ts.WORKFLOW).read_text())["jobs"]["light"]["strategy"]["matrix"]["shard"]
    light = [s for s in shards if s.kind == "light"]
    assert [s.name for s in light] == [r["name"] for r in rows]
    for s, r in zip(light, rows):
        assert s.paths == str(r["paths"]).split()
        assert s.part == str(r.get("part", "")) and s.oracle_tables == str(r.get("oracle_tables", ""))
        assert s.delta == bool(r.get("delta", False))


def test_the_scripts_shard_is_the_light_scripts_job(shards):
    assert [s.paths for s in shards if s.kind == "scripts"] == [["scripts/ci"]]


def test_heavy_shards_are_the_test_dirs_the_light_tier_leaves_out(shards):
    ignored = {Path(p).name for p in ts.heavy_ignores(REPO_ROOT)}
    assert ignored == set(ts.light_test_dirs(REPO_ROOT)), "the heavy action's ignore list is the conftest's light dirs"
    heavy_dirs = {Path(s.paths[0]).name for s in shards if s.kind == "heavy" and s.name != "heavy root files"}
    on_disk = {d.name for d in (PKG / "test").iterdir() if d.is_dir() and any(d.rglob("test_*.py"))}
    assert heavy_dirs == on_disk - ignored - ts._NOT_TESTS
    assert all(s.pool == ts.POOL_JAR for s in shards if s.kind == "heavy")
    assert all(s.pool == ts.POOL_LIGHT for s in shards if s.kind in ("light", "scripts"))


def test_the_heavy_shards_run_the_integration_tests_that_need_the_jar(shards):
    # the JAR-needing tests are marked integration; with CI's "not integration" the heavy vectorx shard collected nothing
    heavy = [s for s in shards if s.kind == "heavy"]
    assert heavy and all("not integration" not in ts.pytest_args(s) for s in heavy)
    light = [s for s in shards if s.kind == "light"]
    assert light and all(ts.pytest_args(s)[-2:] == ["-m", "not integration"] for s in light if not s.oracle_tables)


def test_scala_does_not_run_on_a_cluster(shards):
    scala = next(s for s in shards if s.kind == "scala")
    assert scala.local_only
    with pytest.raises(ValueError, match="does not run on a cluster"):
        ts.select(shards, "scala")
    assert scala not in ts.select(shards, "all-light") + ts.select(shards, "all-heavy")


def test_a_shard_added_to_the_matrix_is_a_shard_here(tmp_path):
    (tmp_path / ".github/workflows").mkdir(parents=True)
    (tmp_path / ".github/actions/python_build").mkdir(parents=True)
    (tmp_path / "python/geobrix/test/newdir").mkdir(parents=True)
    (tmp_path / "python/geobrix/test/newdir/test_x.py").write_text("def test_x(): pass\n")
    (tmp_path / ".github/workflows/build_main.yml").write_text(textwrap.dedent("""\
        jobs:
          light:
            strategy:
              matrix:
                shard:
                  - name: brand new
                    paths: test/newdir
          light-scripts:
            steps:
              - run: python -m pytest scripts/ci -v
        """))
    (tmp_path / ".github/actions/python_build/action.yml").write_text('LIGHT_IGNORES="--ignore=test/newdir"\n')
    (tmp_path / "python/geobrix/test/conftest.py").write_text('_LIGHT_TEST_DIRS = ["newdir"]\n')
    got = ts.load(tmp_path)
    assert [s.name for s in got if s.kind == "light"] == ["brand new"]
    assert not [s for s in got if s.kind == "heavy"]  # newdir is light, not heavy


# ---- selection


def test_selection_by_spec(shards):
    names = lambda spec: [s.name for s in ts.select(shards, spec)]  # noqa: E731
    assert names("pyrx") == ["pyrx 1/2", "pyrx 2/2"]
    assert names("zarr 3/4") == ["zarr 3/4"]
    assert names("manager") == ["manager"] and names("manager family") == ["manager family 1/3", "manager family 2/3", "manager family 3/3"]
    assert "scripts/ci" in names("all-light") and not [n for n in names("all-light") if n.startswith("heavy")]
    assert names("all-heavy") and all(n.startswith("heavy") for n in names("all-heavy"))
    assert names("ds,ds") == ["ds"]
    with pytest.raises(ValueError, match="unknown shard"):
        ts.select(shards, "nope")


def _first_test(dirname):
    return sorted((PKG / "test" / dirname).glob("test_*.py"))[0].name


def test_test_files_named_in_shards_make_one_shard_of_just_those_files(shards):
    got = ts.select(shards, "python/geobrix/test/pyrx/test_env.py,test/pyrx/test_compression.py,ds", [], REPO_ROOT)
    files = next(s for s in got if s.name == "files")
    assert files.paths == ["test/pyrx/test_compression.py", "test/pyrx/test_env.py"] and files.kind == "light" and files.pool == ts.POOL_LIGHT
    assert [s.name for s in got if s.name != "files"] == [s.name for s in shards if s.group == "ds"], "named shards keep working beside them"
    assert ts.pytest_args(files) == ["test/pyrx/test_compression.py", "test/pyrx/test_env.py", "-m", "not integration"]
    assert ts.files_of(files, REPO_ROOT) == files.paths
    with pytest.raises(ValueError, match="no test file"):
        ts.select(shards, "test/pyrx/test_nope.py", [], REPO_ROOT)


def test_a_named_file_runs_on_the_tier_of_the_ci_shard_that_owns_it(shards):
    got = {s.name: s for s in ts.select(shards, "test/vectorx/test_tin_conforming_mode.py,test/pyrx/test_env.py", [], REPO_ROOT)}
    heavy = got["files heavy"]
    assert heavy.kind == "heavy" and heavy.pool == ts.POOL_JAR and heavy.marker == ts.HEAVY_MARKER, "a heavy dir's file needs the JAR pool"
    assert "-m" not in ts.pytest_args(heavy), "and runs its integration tests, as its heavy shard does"
    assert got["files"].pool == ts.POOL_LIGHT and got["files"].paths == ["test/pyrx/test_env.py"]


def test_a_narrowed_family_file_is_planned_by_the_tables_it_runs(shards):
    fam = ts.select(shards, "test/manager/test_family_cross_tier.py", [], REPO_ROOT)
    assert [s.name for s in fam] == ["files family"] and set(fam[0].oracle_tables.split(",")) == set(af.family_tables(shards))
    secs = {"test/manager/test_family_cross_tier.py": 2941.0, "table:RASTER_ACCESSOR": 90.0, "table:*": 2.0}
    narrowed = ts.narrow_tables(fam, "RASTER_ACCESSOR")
    est = ts.estimate(narrowed[0], REPO_ROOT, secs)
    assert est == pytest.approx(ts.FIXED_MIN + 92.0 / 60.0), "the one table the env runs, not the whole file's old time (60 min)"
    assert ts.narrow_tables(shards, "")[0] is shards[0]
    plan = _plan(_args("--env", "GBX_ORACLE_TABLES=RASTER_ACCESSOR"), narrowed)
    assert plan["shards"]["files-family"]["env"]["GBX_ORACLE_TABLES"] == "RASTER_ACCESSOR"


def test_each_chunk_of_a_narrowed_family_file_runs_only_its_own_tables(shards):
    """--env GBX_ORACLE_TABLES=A,B,C narrows the family file, and the cut gives each chunk some of those tables: the chunk's own
    list must reach its env, not the whole --env list (2026-11-12: two chunks both ran all five tables and both overran)."""
    fam = ts.narrow_tables(ts.select(shards, "test/manager/test_family_cross_tier.py", [], REPO_ROOT), "WEB_TILES,RASTER_FILE_OP")
    parts = ts.split_tables(fam[0], 2, {"table:WEB_TILES": 60.0, "table:RASTER_FILE_OP": 60.0})
    assert len(parts) == 2 and {p.oracle_tables for p in parts} == {"WEB_TILES", "RASTER_FILE_OP"}
    plan = _plan(_args("--env", "GBX_ORACLE_TABLES=WEB_TILES,RASTER_FILE_OP", "--env", "X=1"), parts)
    envs = sorted(c["env"]["GBX_ORACLE_TABLES"] for k, c in plan["shards"].items() if k.startswith("files-family"))
    assert envs == ["RASTER_FILE_OP", "WEB_TILES"]
    assert all(c["env"].get("X") == "1" for c in plan["shards"].values()), "the other --env variables still reach every chunk"


def test_a_cut_with_no_units_keeps_the_shard_whole(monkeypatch):
    monkeypatch.setattr(ts, "_items", lambda *a, **k: ({}, 0, 0.0))
    s = ts.Shard(name="x", kind="light", paths=["test/x"])
    assert [c.unit_max_min for c in ts.cut(s, 4.0, REPO_ROOT, {})] == [0.0], "no ValueError from max() of nothing"


af = launcher.af  # the module the launcher calls (a second copy would not see a monkeypatch)
TIN = "python/geobrix/src/databricks/labs/gbx/pyvx/_tin.py"
RS = "rust/gbxrs/src/tin.rs"


@pytest.fixture(scope="module")
def graph(tmp_path_factory):
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True, check=True).stdout.strip()
    return af.Graph(af.build_graph(REPO_ROOT, sha, tmp_path_factory.mktemp("graph"))), sha


def _sel(graph, shards, changed, old=None, rust=None):
    """The selection for ``changed`` at HEAD: ``old`` maps a path to its text before the change (default: absent before)."""
    G, sha = graph
    head = lambda f: af.blobs(REPO_ROOT, sha, [f])[f]  # noqa: E731
    return af.select(G, list(changed), shards, old_text=(old or {}).get if isinstance(old, dict) else (old or (lambda f: None)),
                     new_text=head, rust=rust)


def _with_body_change(graph, path, sym):
    """HEAD's text of ``path`` with one statement more at the top of function ``sym``: a change to that symbol only."""
    text = af.blobs(REPO_ROOT, graph[1], [path])[path]
    i = text.index(f"\ndef {sym}(") + 1
    j = text.index(":\n", text.index(")", i)) + 2
    return {path: text[:j] + "    _ = 0\n" + text[j:]}


_TIN_WORDS = ("tin", "triangul", "conforming", "jmath", "interpolate", "elevation")


def _tin_related(f, why=()):
    """A pyvx or TIN test: by its path, or by its reason (it names a TIN function's row, or it imports the UC TIN forms)."""
    return f.startswith("test/pyvx/") or any(w in (f + " ".join(why)).lower() for w in _TIN_WORDS) or "uc.forms" in " ".join(why)


def test_a_test_file_selects_itself_and_its_ci_shard_runs_just_it(graph, shards):
    f = f"test/pyrx/{_first_test('pyrx')}"
    sel = _sel(graph, shards, [f"python/geobrix/{f}"], old={})
    assert f in sel.files and sel.files[f][0].endswith("changed")
    out, lost = af.to_shards(sel, shards, REPO_ROOT)
    pyrx = next(s for s in out if s.name == "pyrx affected")
    assert f in pyrx.paths and pyrx.part == "" and pyrx.subset and pyrx.marker == "not integration" and not lost
    assert ts.files_of(pyrx, REPO_ROOT) == sorted(pyrx.paths), "the shard is cut from the selected files, not the whole CI shard"


def test_a_change_to_one_pyvx_module_selects_only_the_pyvx_and_tin_tests(graph, shards):
    sel = _sel(graph, shards, [TIN], old=_with_body_change(graph, TIN, "_delaunay"))
    assert "test/pyvx/test_tin_core.py" in sel.files and "test/pyvx/test_conforming.py" in sel.files
    assert sel.files and all(_tin_related(f, w) for f, w in sel.files.items()), sorted(sel.files)
    assert not [f for f in sel.files if f.startswith(("test/zarr/", "test/pyrx/", "test/ds/", "test/bench/test_levels"))]
    assert set(sel.tables) == {"VECTOR_GENERATOR"}, "the family rows of the TIN functions, no other table"
    why = sel.files["test/pyvx/test_tin_mode_arg.py"][0]
    assert "_tin.py (_delaunay)" in why and "pyvx.functions -> pyvx._tin" in why, why
    units, picked = af.light_units(shards, REPO_ROOT), af.light_selected(sel, shards, REPO_ROOT)
    assert picked < 0.05 * units, (picked, units)


def test_a_changed_symbol_nobody_uses_selects_nothing(graph, shards):
    text = af.blobs(REPO_ROOT, graph[1], [TIN])[TIN]
    old = {TIN: text + "\n\ndef _gone():\n    return 0\n"}  # the commit removed a function no module calls
    sel = _sel(graph, shards, [TIN], old=old)
    assert not sel.files and not sel.tables


def test_a_rust_kernel_change_selects_the_tests_of_its_callers(graph, shards):
    G, sha = graph
    crate = {p: (af.blobs(REPO_ROOT, sha, [p])[p], {-1}) for p in af.tracked(REPO_ROOT, sha, "rust/gbxrs/src") if p.endswith(".rs")}
    text = crate[RS][0]
    kd = [i for i in af.rust_items(text) if i["name"] == "KdTree"][-1]
    crate[RS] = (text, {kd["start"] + 3})  # a line inside KdTree's impl
    exports, _ = af.rust_changed_exports(crate)
    assert exports == {"TinKdTree"}, "only the class that wraps the KdTree"
    sel = _sel(graph, shards, [RS], rust=lambda fs: crate)
    assert "test/pyvx/test_conforming.py" in sel.files and "test/pyvx/test_tin_core.py" in sel.files
    assert "exports TinKdTree; called in " in sel.files["test/pyvx/test_tin_core.py"][0]
    assert all(_tin_related(f, w) for f, w in sel.files.items()), sorted(sel.files)
    assert not [f for f in sel.files if "bng" in f or f.startswith("test/pygx/")], "the BNG kernel did not change"


def test_a_rust_change_follows_calls_inside_the_crate_to_the_export():
    lib = "#[pyfunction]\nfn tin_delaunay() {\n    tin::delaunay(1)\n}\n#[pyfunction]\nfn other() {\n    1\n}\n#[pymodule]\nfn m(m: &M) {\n    m.add_function(wrap_pyfunction!(tin_delaunay, m)?)?;\n    m.add_function(wrap_pyfunction!(other, m)?)?;\n}\n"
    tin = "pub fn delaunay(x: i32) -> i32 {\n    helper(x)\n}\n\nfn helper(x: i32) -> i32 {\n    x\n}\n\n#[cfg(test)]\nmod tests {\n    fn t() {}\n}\n"
    got, _ = af.rust_changed_exports({"lib.rs": (lib, {-1}), "tin.rs": (tin, {6})})
    assert got == {"tin_delaunay"}, "helper -> delaunay -> tin_delaunay, not other"
    got, notes = af.rust_changed_exports({"lib.rs": (lib, {-1}), "tin.rs": (tin, {11})})
    assert got == set() and notes, "a change inside the crate's tests reaches no export"


_P = "python/geobrix/src/databricks/labs/gbx"
_T = "python/geobrix/test"


def _mini(files):
    """A graph of these files alone (path -> text), as build_graph makes one from a commit."""
    return af.Graph(af.graph_of(files))


def _mini_sel(files, changed, old, shards):
    G = _mini(files)
    return af.select(G, changed, shards, old_text=lambda f: old.get(f, files.get(f)), new_text=files.get)


def test_an_autouse_fixture_reached_through_a_conftest_selects_the_tests_under_it(shards):
    files = {
        f"{_P}/__init__.py": "", f"{_P}/helper.py": "def make():\n    return 1\n",
        f"{_T}/__init__.py": "", f"{_T}/zz/__init__.py": "",
        f"{_T}/zz/conftest.py": "import pytest\nfrom databricks.labs.gbx.helper import make\n\n\n@pytest.fixture(autouse=True)\n"
                                "def _setup():\n    make()\n\n\ndef pytest_configure(config):\n    pass\n",
        f"{_T}/zz/test_a.py": "def test_a():\n    assert 1\n",
    }
    sel = _mini_sel(files, [f"{_P}/helper.py"], {f"{_P}/helper.py": "def make():\n    return 0\n"}, shards)
    assert "test/zz/test_a.py" in sel.files, "the autouse fixture runs for every test under the conftest, named or not"


def test_an_aliased_import_through_a_hub_follows_the_name_it_was_given(shards):
    files = {
        f"{_P}/__init__.py": "", f"{_P}/core.py": "def f():\n    return 1\n\n\ndef g():\n    return 2\n",
        f"{_P}/hub/__init__.py": "from databricks.labs.gbx.core import f as _f\n\n\ndef api():\n    return _f()\n",
        f"{_T}/__init__.py": "", f"{_T}/test_x.py": "from databricks.labs.gbx.hub import api\n\n\ndef test_x():\n    api()\n",
    }
    old = {f"{_P}/core.py": "def f():\n    return 0\n\n\ndef g():\n    return 2\n"}
    sel = _mini_sel(files, [f"{_P}/core.py"], old, shards)
    assert "test/test_x.py" in sel.files, "f changed; the hub calls it as _f in api; the test uses api"


def test_a_package_init_whose_module_code_changed_selects_the_importers_of_its_submodules(shards):
    files = {
        f"{_P}/__init__.py": "", f"{_P}/pkg/__init__.py": "X = 1\nprint(X)\n", f"{_P}/pkg/sub.py": "def s():\n    return 1\n",
        f"{_T}/__init__.py": "", f"{_T}/test_s.py": "from databricks.labs.gbx.pkg import sub\n\n\ndef test_s():\n    sub.s()\n",
    }
    sel = _mini_sel(files, [f"{_P}/pkg/__init__.py"], {f"{_P}/pkg/__init__.py": "X = 1\n"}, shards)
    assert "test/test_s.py" in sel.files, "importing pkg.sub runs pkg/__init__"


def test_a_deleted_module_selects_the_tests_that_still_import_it(shards):
    files = {
        f"{_P}/__init__.py": "", f"{_P}/pkg/__init__.py": "",
        f"{_T}/__init__.py": "", f"{_T}/test_a.py": "from databricks.labs.gbx.pkg import gone\n\n\ndef test_a():\n    gone.x()\n",
        f"{_T}/test_b.py": "import databricks.labs.gbx.pkg.gone as g\n\n\ndef test_b():\n    g.x()\n",
        f"{_T}/test_c.py": "def test_c():\n    pass\n",
    }
    gone = f"{_P}/pkg/gone.py"
    sel = _mini_sel(files, [gone], {gone: "def x():\n    return 1\n"}, shards)
    assert {"test/test_a.py", "test/test_b.py"} <= set(sel.files) and "test/test_c.py" not in sel.files, "they now fail to import"


def test_a_manifest_change_selects_the_tests_that_read_the_whole_manifest(graph, shards):
    G, sha = graph
    text = af.blobs(REPO_ROOT, sha, [af.MANIFEST_JSON])[af.MANIFEST_JSON]
    rows = json.loads(text)
    rows["functions"][0] = dict(rows["functions"][0], status="tweaked")
    sel = _sel(graph, shards, [af.MANIFEST_JSON], old={af.MANIFEST_JSON: json.dumps(rows)})
    assert {"test/manager/test_virtual_dispatch.py", "test/manager/test_surfaces.py"} <= set(sel.files)
    assert af.light_selected(sel, shards, REPO_ROOT) < af.BROAD_SHARE * af.light_units(shards, REPO_ROOT), "not the whole bank"


def test_a_rust_const_static_macro_or_attribute_change_reaches_the_export():
    lib = ("const SCALE: f64 = 2.0;\nstatic mut N: i32 = 0;\nmacro_rules! twice {\n    ($x:expr) => { $x * 2.0 };\n}\n\n"
           "#[pyfunction]\nfn scaled(x: f64) -> f64 {\n    twice!(x) * SCALE\n}\n#[pyfunction]\nfn other() {\n    1\n}\n"
           "#[pymodule]\nfn m(m: &M) {\n    m.add_function(wrap_pyfunction!(scaled, m)?)?;\n    m.add_function(wrap_pyfunction!(other, m)?)?;\n}\n")
    for line, why in ((1, "a const"), (4, "a macro"), (7, "the attribute above the fn")):
        got, _ = af.rust_changed_exports({"lib.rs": (lib, {line})})
        assert "scaled" in got, why
    got, _ = af.rust_changed_exports({"lib.rs": ("use x::y;\n" + lib, {1})})
    assert {"scaled", "other"} <= got, "a changed line outside every item: every item of the file"


def test_an_affected_subset_takes_its_share_of_its_ci_shards_minutes(graph, shards):
    f = f"test/pyrx/{_first_test('pyrx')}"
    sel = _sel(graph, shards, [f"python/geobrix/{f}"], old={})
    out, _ = af.to_shards(sel, shards, REPO_ROOT)
    sub = next(s for s in out if s.name == "pyrx affected")
    home = next(s for s in shards if s.name == "pyrx 1/2" and ts._covers(s, f, ts.shard_of(REPO_ROOT))) or \
        next(s for s in shards if s.name == "pyrx 2/2")
    per_home = ts.untimed_file_seconds(home, len(ts.files_of(home, REPO_ROOT)))
    assert ts.untimed_file_seconds(sub, len(sub.paths)) == pytest.approx(per_home), "a file's share of pyrx's 17 min, not of the 12 min default"


def test_a_guessed_subset_or_rerun_estimate_gives_no_contention_record():
    per = {"a.py": 400.0}
    for chunk in ({"name": "pyrx affected", "subset": True, "est_src": "ci", "solo_min": 2.0},
                  {"name": "rerun test/a.py", "est_src": "ci", "solo_min": 2.0},
                  {"name": "files", "est_src": "ci", "solo_min": 2.0}):
        assert tm.contention_records(chunk, 600.0, per, 2, 3) == [], chunk["name"]
    assert tm.contention_records({"name": "ds", "est_src": "ci", "solo_min": 2.0}, 600.0, per, 2, 3), "a whole CI shard's minutes are measured"


def test_an_oracle_table_module_maps_to_its_family_parts_only(graph, shards):
    f = "python/geobrix/src/databricks/labs/gbx/bench/oracle_tables/vector_scalar.py"
    sel = _sel(graph, shards, [f])
    assert "VECTOR_SCALAR" in sel.tables and "RASTER_STAT" not in sel.tables
    out, _ = af.to_shards(sel, shards, REPO_ROOT)
    fam = [s for s in out if s.oracle_tables]
    assert fam and all(s.name.startswith("manager family") and s.unsplittable and s.subset for s in fam)
    assert "VECTOR_SCALAR" in ",".join(s.oracle_tables for s in fam) and "RASTER_STAT" not in ",".join(s.oracle_tables for s in fam)
    assert ts.shard_env(fam[0])["GBX_ORACLE_TABLES"] == fam[0].oracle_tables


def test_a_manifest_row_change_maps_to_its_family_and_the_tests_that_name_it(graph, shards):
    G, sha = graph
    text = af.blobs(REPO_ROOT, sha, [af.MANIFEST_JSON])[af.MANIFEST_JSON]
    doc = json.loads(text)
    row = next(r for r in doc["functions"] if r["sql_name"] == "gbx_st_triangulate")
    row["since"] = "0.0.1"  # the commit changed one field of the row
    sel = _sel(graph, shards, [af.MANIFEST_JSON], old={af.MANIFEST_JSON: json.dumps(doc)})
    assert set(sel.tables) == {"VECTOR_GENERATOR"} and "row gbx_st_triangulate" in sel.tables["VECTOR_GENERATOR"][0]
    assert sel.files and all("gbx_st_triangulate" in w[0] or "reads the whole manifest" in w[0] for w in sel.files.values())
    assert any("gbx_st_triangulate" in w[0] for w in sel.files.values())


def test_the_family_file_selects_every_table_and_a_conftest_its_folder(graph, shards):
    sel = _sel(graph, shards, ["python/geobrix/test/manager/test_family_cross_tier.py"])
    assert set(sel.tables) == set(af.family_tables(shards)) and af.FAMILY_FILE not in sel.files
    sel = _sel(graph, shards, ["python/geobrix/test/zarr/conftest.py"])
    assert sel.files and all(f.startswith("test/zarr/") for f in sel.files)


def test_a_native_payload_input_reaches_the_whole_bank(graph, shards):
    sel = _sel(graph, shards, ["scripts/native/arch-gate.json"])
    assert sel.everything == ["scripts/native/arch-gate.json"], "the env wheels carry what scripts/native builds"


def test_a_script_maps_to_the_scripts_ci_tests_that_name_it(graph, shards):
    sel = _sel(graph, shards, ["examples/tests/_affected.py"])
    assert "scripts/ci/test_gbx_test_databricks.py" in sel.files
    assert not [f for f in sel.files if not f.startswith("scripts/ci/")]
    out, _ = af.to_shards(sel, shards, REPO_ROOT)
    assert [s.kind for s in out] == ["scripts"] and out[0].paths == sorted(sel.files)


def test_a_broad_change_trips_the_guard_and_needs_allow_broad(graph, shards, monkeypatch, capsys):
    for f in ("python/geobrix/test/conftest.py", ".github/workflows/build_main.yml"):
        sel = _sel(graph, shards, [f])
        assert af.light_selected(sel, shards, REPO_ROOT) > af.BROAD_SHARE * af.light_units(shards, REPO_ROOT), f
    sel = _sel(graph, shards, ["python/geobrix/test/conftest.py"])
    monkeypatch.setattr(ts, "changed_files", lambda *a, **k: ["python/geobrix/test/conftest.py"])
    monkeypatch.setattr(af, "affected", lambda *a, **k: (sel, graph[0]))
    assert launcher.main(["--explain-only"]) == launcher.EXIT_USAGE
    err = capsys.readouterr().err
    assert "!!! affected selects" in err and "--allow-broad" in err
    assert launcher.main(["--explain-only", "--allow-broad"]) == 0
    assert "WARNING" in capsys.readouterr().err


def test_explain_only_names_each_selected_file_with_its_reason(graph, shards, monkeypatch, capsys, tmp_path):
    sel = _sel(graph, shards, [TIN], old=_with_body_change(graph, TIN, "_delaunay"))
    monkeypatch.setattr(ts, "changed_files", lambda *a, **k: [TIN])
    monkeypatch.setattr(af, "affected", lambda *a, **k: (sel, graph[0]))
    assert launcher.main(["--explain-only", "--timings", str(tmp_path / "h.json")]) == 0
    out = capsys.readouterr().out
    assert "test/pyvx/test_tin_core.py: python/geobrix/src/databricks/labs/gbx/pyvx/_tin.py (_delaunay): test.pyvx.test_tin_core -> pyvx._tin" in out
    assert "table VECTOR_GENERATOR [only the rows of gbx_st_" in out and ": -k]: row gbx_st_" in out, "a row-level change runs its rows only"
    assert "manager family 2/3 affected rows" in out and ", -k gbx_st_" in out
    queues = [ln for ln in out.splitlines() if ln.strip().startswith("queue (")]
    assert queues and not [q for q in queues if "zarr" in q or "pyrx" in q], queues


def test_changed_symbols_are_the_top_level_code_that_differs():
    old = "import os\n\ndef a():\n    return 1\n\ndef b():\n    return 2\n"
    assert af.changed_symbols(old, old.replace("return 2", "return 3")) == {"b"}
    assert af.changed_symbols(old, old + "\ndef c():\n    pass\n") == {"c"}
    assert af.changed_symbols(old, old + "\nprint(1)\n") is None, "module-level code is not a symbol"
    assert af.changed_symbols(old, old + "\n# a comment\n") == set()


def test_affected_with_no_change_runs_nothing(shards):
    assert ts.select(shards, "affected", [], REPO_ROOT) == []


# ---- lanes and the plan


def test_lanes_hold_every_shard_once_and_balance_by_expected_time(shards):
    light = ts.select(shards, "all-light")
    got = ts.lanes(light, 5)
    assert len(got) == 5
    assert sorted(s.name for lane in got for s in lane) == sorted(s.name for s in light)
    loads = [sum(s.expected_min for s in lane) for lane in got]
    assert max(loads) - min(loads) <= max(s.expected_min for s in light)
    assert len(ts.lanes(light[:2], 5)) == 2 and len(ts.lanes(light, 1)) == 1


def _args(*extra):
    return launcher.parse(["--run-id", "t1", "--purpose", "unit test", *extra])


def _plan(a, picked, wheel=None, jar=None):
    return launcher.make_plan(a, "a" * 40, picked, wheel, jar, "/Volumes/c/s/v", REPO_ROOT)


def test_the_plan_splits_light_and_heavy_shards_over_their_pools(shards):
    a = _args()
    plan = _plan(a, ts.select(shards, "pyrx,heavy rasterx"), jar={"name": "j.jar", "sha256": "0" * 64, "volume": "/Volumes/c/s/v/test-pool-jars/0/j.jar"})
    assert set(plan["pools"]) == {"gbx-test-1n", "gbx-test-1n-jar"}
    assert plan["pools"]["gbx-test-1n"]["jar"] is False and plan["pools"]["gbx-test-1n-jar"]["jar"] is True
    assert plan["stage"] == "/Volumes/c/s/v/test-runs/t1"
    assert plan["shards"]["pyrx-1-2"]["env"] == {"GBX_SHARD": "0/2"}
    assert plan["shards"]["pyrx-1-2"]["args"] == ["test/pyrx", "-m", "not integration"]
    text = launcher.explain(plan, [s for s in shards if s.local_only], [])
    assert "gbx-test-1n" in text and "queue (longest first" in text and "runner(s)" in text and "local only: scala" in text and "expected wall about" in text
    assert plan["pools"]["gbx-test-1n"]["table"] == "`c`.`s`.`gbx_test_queue_t1_light`" and plan["pools"]["gbx-test-1n-jar"]["table"].endswith("_jar`")


def test_env_reaches_every_shard_after_its_own(shards):
    a = _args("--env", "GBX_CAP_ARROW_TILE_TRANSPORT=0", "--env", "GBX_SHARD=x")
    plan = _plan(a, ts.select(shards, "pyrx"))
    assert plan["shards"]["pyrx-1-2"]["env"] == {"GBX_SHARD": "x", "GBX_CAP_ARROW_TILE_TRANSPORT": "0"}
    assert "--env takes KEY=VAL, not 'no-equals'" in launcher.problems(_args("--env", "no-equals"))
    a = _args("--spark-conf", "spark.sql.x.enabled=false")
    plan = _plan(a, ts.select(shards, "pyrx"))
    assert plan["shards"]["pyrx-1-2"]["spark_conf"] == {"spark.sql.x.enabled": "false"}
    assert "spark.conf.set(_k, _v)" in launcher.run_source(plan)


def test_oracle_tables_and_ignores_reach_the_pytest_args(shards):
    plan = _plan(_args(), ts.select(shards, "manager family 1/3,manager,scripts/ci"))
    fam = plan["shards"]["manager-family-1-3"]
    assert fam["env"]["GBX_ORACLE_TABLES"].startswith("CELL_AGGREGATE")
    assert "--ignore=test/manager/test_family_cross_tier.py" in plan["shards"]["manager"]["args"]
    assert plan["shards"]["scripts-ci"]["cwd"] == "." and plan["shards"]["scripts-ci"]["args"] == ["scripts/ci"]


def test_serverless_refuses_heavy_shards_and_explain_only_needs_no_purpose(capsys):
    assert launcher.main(["--explain-only", "--serverless", "--shards", "all-heavy"]) == launcher.EXIT_USAGE
    assert launcher.main(["--explain-only", "--shards", "ds"]) == 0
    assert "pool gbx-test-1n" in capsys.readouterr().out


def test_max_parallel_caps_the_runners(shards):
    plan = _plan(_args("--max-parallel", "2"), ts.select(shards, "all-light"))
    assert plan["pools"]["gbx-test-1n"]["runners"] == 2
    assert _plan(_args("--max-parallel", "8"), ts.select(shards, "ds"))["pools"]["gbx-test-1n"]["runners"] == 1, "never more runners than shards"


# ---- the runner notebook


def test_the_notebook_opens_with_the_four_line_header_and_its_code_compiles(shards):
    plan = _plan(_args(), ts.select(shards, "ds"), wheel=Path("/x/geobrix-0.6.0+env6-py3-none-any.whl"))
    nb = launcher.build_notebook(plan, "gbx-test-1n")
    head = "".join(nb["cells"][0]["source"])
    for key in ("Output:", "Time:", "Monitoring:", "unit test"):
        assert key in head
    assert head.count("**") >= 8
    start = "".join(nb["cells"][1]["source"])
    compile(start.replace("dbutils.", "d."), "start", "exec")
    assert "child.json" in start and "safeToJson" in start, "the chunk names its own run before anything can fail"
    install = "".join(nb["cells"][2]["source"])
    wheel = plan["wheel"]["volume"]
    # the staged wheel is forced in first (a same-version geobrix on the cluster would make a plain install a no-op)
    assert install.startswith(f'%pip install --quiet --force-reinstall --no-deps "geobrix @ file://{wheel}"\n%pip install --quiet')
    assert "geobrix[stac,vizx,zarr,databricks] @ file://" in install
    # then the guard, before the checkout's package is laid over the installed one: driver and workers must import the staged wheel
    guard = "".join(nb["cells"][3]["source"])
    compile(guard, "guard", "exec")
    assert "def run_guard(" in guard and f"'wheel': '{wheel}'" in guard
    assert "WHEEL_GUARD = run_guard(" in guard and "spark=None" in guard  # driver only here: the workers are checked after the overlay
    compile("".join(nb["cells"][4]["source"]), "runner", "exec")  # no leftover format fields
    assert "pytest.main" in "".join(nb["cells"][4]["source"])
    # after the checkout is laid over the installed package, the checkout must be what the driver AND the workers import
    run = "".join(nb["cells"][4]["source"])
    overlay, check, tests = run.index("shutil.copytree(src_pkg, inst"), run.index('run_guard({"tree": src_pkg}'), run.index("pytest.main")
    assert overlay < check < tests and 'exclude=("_build_info.py",)' in run and "spark=spark" in run
    served = launcher.build_notebook(dict(plan, serverless=True), "gbx-test-1n")["cells"]
    assert len(served) == 4 and "def run_guard(" in "".join(served[2]["source"])  # no %pip cell on Serverless; the guard stays


# ---- the result files


JUNIT = """<?xml version="1.0"?><testsuites><testsuite name="pytest" errors="1" failures="1" skipped="2" tests="6" time="3.5">
<testcase classname="t.a" name="ok1" time="0.1"/>
<testcase classname="t.a" name="ok2" time="0.1"/>
<testcase classname="t.a" name="bad" time="0.1"><failure message="assert 1 == 2&#10;more">trace</failure></testcase>
<testcase classname="t.b" name="err" time="0.1"><error message="fixture boom">trace</error></testcase>
<testcase classname="t.b" name="s1"><skipped message="no corpus" type="pytest.skip">x</skipped></testcase>
<testcase classname="t.b" name="s2"><skipped message="no corpus" type="pytest.skip">x</skipped></testcase>
</testsuite></testsuites>"""


def test_junit_counts_failures_and_skip_reasons(tmp_path):
    f = tmp_path / "junit.xml"
    f.write_text(JUNIT)
    j = launcher.parse_junit(f)
    assert (j["tests"], j["passed"], j["failed"], j["errors"], j["skipped"]) == (6, 2, 1, 1, 2)
    assert j["failures"][0][0] == "t.a::bad" and "assert 1 == 2" in j["failures"][0][1]
    assert j["skip_reasons"]["no corpus"] == 2


def test_verdicts():
    v = launcher.shard_verdict
    assert v(None, None) == "INCOMPLETE"
    assert v({"rc": 0, "total": 10, "failed": 0, "errors": 0}, None) == "ok"
    assert v({"rc": 1, "total": 10, "failed": 2, "errors": 0}, None) == "FAILED"
    assert v({"rc": 5, "total": 0}, None) == "EMPTY"
    assert v({"rc": 130, "total": 10, "failed": 0, "errors": 0, "reason": "shard cap 45 min"}, None) == "INCOMPLETE"
    assert v({"rc": 2, "total": 0, "failed": 0, "errors": 0}, None) == "ERRORS", "a collection error is an error, not 'did not finish'"
    assert v({"rc": 130, "total": 3, "failed": 0, "errors": 0}, None) == "INCOMPLETE"


def test_the_summary_lists_the_table_the_failures_and_the_skips(tmp_path):
    f = tmp_path / "junit.xml"
    f.write_text(JUNIT)
    j = launcher.parse_junit(f)
    row = dict(name="ds", slug="ds", pool="gbx-test-1n", verdict="FAILED", expected_min=12, passed=j["passed"], failed=1, errors=1, skipped=2,
               wall_s=100, reason="", failures=j["failures"], skip_reasons=j["skip_reasons"], actual_s=780, est_src="history")
    plan = {"run_id": "t1", "sha": "a" * 40, "wheel": {"name": "w.whl", "built_from": "b" * 40}, "purpose": "p", "jar": None}
    md = launcher.summary_md(plan, [row], {"gbx-test-1n": {"cluster_id": "c1", "node_type": "m5d.8xlarge", "job_run": 9, "wall_s": 100}})
    assert "| ds | gbx-test-1n | FAILED | 2 | 1 | 1 | 2 | 100 | 720 | 780 | +8 | history |" in md, "predicted vs actual, and the error"
    assert "median abs error 8%" in md and "err %" in launcher.table_text([row])
    assert "`t.a::bad`" in md and "no corpus" in md
    assert "ds" in launcher.table_text([row])


def test_the_jar_is_copied_out_and_named_by_its_content(tmp_path):
    jar = tmp_path / "geobrix-0.6.0-jar-with-dependencies.jar"
    jar.write_bytes(b"jar bytes")
    got = launcher.stage_jar(jar, REPO_ROOT)
    assert Path(got["local"]) != jar and Path(got["local"]).read_bytes() == b"jar bytes"
    assert got["dir"] == f"test-pool-jars/{got['sha256'][:12]}"
    jar.write_bytes(b"replaced by another build")
    assert Path(got["local"]).read_bytes() == b"jar bytes"
    json.dumps(got)


# ---- finer shards, timings, slots


def test_a_split_shard_runs_every_file_of_the_shard_exactly_once(shards):
    ds = next(s for s in shards if s.name == "ds")
    files = ts.files_of(ds, REPO_ROOT)
    parts = ts.split_shard(ds, 3, REPO_ROOT, {})
    assert len(parts) == 3
    assert sorted(f for p in parts for f in p.paths) == files
    assert all(not p.part and not p.ignore and p.name.startswith("ds [") for p in parts)


def test_a_part_of_a_hashed_dir_splits_into_the_files_of_that_part_only(shards):
    pyrx = next(s for s in shards if s.name == "pyrx 1/2")
    files = ts.files_of(pyrx, REPO_ROOT)
    other = ts.files_of(next(s for s in shards if s.name == "pyrx 2/2"), REPO_ROOT)
    assert files and other and not set(files) & set(other)
    assert sorted(f for p in ts.split_shard(pyrx, 2, REPO_ROOT, {}) for f in p.paths) == files


def test_the_oracle_tables_of_one_file_are_split_by_table_not_by_file(shards):
    fam = next(s for s in shards if s.name == "manager family 1/3")
    assert ts.files_of(fam, REPO_ROOT) is None
    parts = ts.split_shard(fam, 4, REPO_ROOT, {})
    assert len(parts) == 4
    tables = fam.oracle_tables.split(",")
    got = [t for p in parts for t in p.oracle_tables.split(",")]
    assert sorted(got) == sorted(tables), "every table in exactly one part"
    assert all(p.paths == fam.paths and ts.shard_env(p)["GBX_ORACLE_TABLES"] for p in parts)
    assert ts.split_shard(next(s for s in shards if s.name == "pyrx 1/2"), 1, REPO_ROOT, {})[0].name == "pyrx 1/2"


def test_the_split_balances_by_the_measured_seconds_of_each_file(shards):
    ds = next(s for s in shards if s.name == "ds")
    files = ts.files_of(ds, REPO_ROOT)
    secs = {f: 10.0 for f in files}
    secs[files[0]] = 600.0  # one file as long as sixty others
    parts = ts.split_shard(ds, 2, REPO_ROOT, secs)
    heavy = next(p for p in parts if files[0] in p.paths)
    assert len(heavy.paths) < len(files) / 2, "the long file sits nearly alone"
    assert abs(parts[0].est_min - parts[1].est_min) < 11 / 60 * 2 + 0.5


def test_auto_split_cuts_long_shards_to_about_the_target_and_leaves_short_ones(shards):
    light = ts.select(shards, "all-light")
    out = ts.auto_split(light, REPO_ROOT, {}, 6.0)
    assert len(out) > len(light)
    assert all(s.est_min <= 6.0 + 1e-6 for s in out), [(s.name, s.est_min) for s in out if s.est_min > 6.0]
    assert not [s for s in out if s.name.startswith("manager family") and not s.name.endswith("]")], "the family oracle groups are cut as well"

    def all_files(items):  # a file cut into test parts is in each of its parts once
        return sorted(f for s in items if s.test_part in ("", "0/" + ts.test_part_n(s)) for f in (ts.files_of(s, REPO_ROOT) or []))

    assert all_files(out) == all_files(light), "no file lost or doubled by the split"


def test_timings_decide_which_shards_are_long(shards):
    light = [s for s in ts.select(shards, "all-light") if s.name in ("ds", "scripts/ci")]
    fast = {f: 0.5 for s in light for f in ts.files_of(s, REPO_ROOT)}
    out = ts.auto_split(light, REPO_ROOT, fast, 6.0)
    assert [s.name for s in out] == ["ds", "scripts/ci"], "files measured at half a second need no cutting"
    slow = {f: 30.0 for s in light for f in ts.files_of(s, REPO_ROOT)}
    assert len(ts.auto_split(light, REPO_ROOT, slow, 6.0)) > 2


def test_slots_follow_the_cores_of_the_node():
    assert ts.slots_for(32) == 4 and ts.slots_for(64) == 8 and ts.slots_for(2) == 1


def test_the_plan_sizes_its_runners_to_clusters_times_slots(shards):
    picked = ts.select(shards, "all-light")
    one = _plan(_args(), picked)["pools"]["gbx-test-1n"]
    assert one["slots"] == 4 and one["runners"] == 4 and one["clusters"] == 1
    two = _plan(_args("--clusters", "3", "--max-parallel", "4"), picked)["pools"]["gbx-test-1n"]
    assert two["slots"] == 4 and two["clusters"] == 3 and two["runners"] == 12
    assert one["longest_min"] <= one["expected_min"] <= one["serial_min"]


def test_relane_sizes_the_runners_to_the_clusters_that_were_free(shards):
    a = _args("--clusters", "3")
    plan = _plan(a, ts.select(shards, "all-light"))
    before = list(plan["pools"]["gbx-test-1n"]["queue"])
    launcher.relane(plan, "gbx-test-1n", a, 1, 16)  # only one cluster was free, a smaller node
    spec = plan["pools"]["gbx-test-1n"]
    assert spec["clusters"] == 1 and spec["slots"] == 2 and spec["runners"] == 2
    assert spec["queue"] == before, "the queue and its order stay as they were"


def _junit(path, cases):
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(f'<testcase classname="{c}" name="{n}" time="{t}"/>' for c, n, t in cases)
    path.write_text(f"<testsuites><testsuite tests='{len(cases)}'>{body}</testsuite></testsuites>")
    return path


def _ds_files(n=3):
    return sorted(p.relative_to(PKG).as_posix() for p in (PKG / "test" / "ds").glob("test_*.py"))[:n]


def _mod(f):
    return f[:-3].replace("/", ".")


def test_every_run_merges_its_samples_and_none_overwrites_another(tmp_path):
    path = tmp_path / "h.json"
    f = _ds_files(1)[0]
    for run, secs in (("r1", 10.0), ("r2", 30.0), ("r3", 20.0)):
        tm.merge(path, [{"key": f, "s": secs, "slots": 1}], {"run": run})
    tm.merge(path, [{"key": f, "s": 999.0, "slots": 1}], {"run": "r3"})  # the same run again adds nothing
    h = tm.load(path)
    assert [x["s"] for x in h["keys"][f]["samples"]] == [10.0, 30.0, 20.0] and [r["run"] for r in h["runs"]] == ["r1", "r2", "r3"]
    est = tm.estimates(h, 1)[f]
    assert est == {"s": 20.0, "n": 3, "lb": False, "solo": 20.0, "x": 1.0}, "the median of the last runs, with the count"
    assert tm.estimates(h, 8)[f]["s"] == round(20.0 * ts.slowdown(8), 2), "normalized to a chunk alone, then to the plan's slots"
    tm.merge(path, [{"key": f, "s": 20.0 * ts.slowdown(8), "slots": 8}], {"run": "r4"})
    assert tm.load(path)["keys"][f]["samples"][-1]["s"] == 20.0


MERGER = """
import sys
sys.path.insert(0, sys.argv[1])
import _timings as tm
sys.stdin.readline()
for i in range(40):
    tm.merge(sys.argv[2], [{"key": f"k{j}", "s": float(i), "slots": 1} for j in range(20)], {"run": f"{sys.argv[3]}-{i}"})
"""


def test_two_runs_merging_at_once_keep_every_sample(tmp_path):
    script = tmp_path / "merger.py"
    script.write_text(MERGER)
    path = tmp_path / "h.json"
    procs = [subprocess.Popen([sys.executable, str(script), str(TESTS), str(path), name], stdin=subprocess.PIPE, text=True) for name in ("a", "b")]
    for p in procs:
        p.stdin.write("go\n")
        p.stdin.flush()
    for p in procs:
        p.communicate(timeout=120)
        assert p.returncode == 0
    h = tm.load(path)
    assert len(h["runs"]) == 80, "no run of either writer lost"
    assert all(len(h["keys"][f"k{j}"]["samples"]) == tm.KEEP for j in range(20))
    assert {x["run"].split("-")[0] for x in h["keys"]["k0"]["samples"]} == {"a", "b"}


def test_a_version_one_file_becomes_one_sample_per_file(tmp_path):
    p = tmp_path / "t.json"
    p.write_text(json.dumps({"run": "old", "slots": 8, "files": {"test/x.py": 24.0}}))
    h = tm.load(p)
    assert h["version"] == tm.VERSION and tm.estimates(h, 8)["test/x.py"]["s"] == 24.0


def test_a_stopped_chunk_leaves_lower_bounds_and_gets_a_chunk_of_its_own_first_next_time(shards, tmp_path):
    files = _ds_files(4)
    chunk = {"files": files}
    junit = _junit(tmp_path / "junit.xml", [(_mod(files[0]), "a", 30), (_mod(files[1]), "b", 20)])  # stopped inside files[1]
    got = {x["key"]: x for x in tm.run_samples(chunk, "INCOMPLETE", 2742, junit, REPO_ROOT, 1)}  # the 45 min cap
    assert got[files[0]] == {"key": files[0], "s": 30.0, "slots": 1, "lb": False}
    assert all(got[f]["lb"] for f in files[1:]) and got[files[1]]["s"] > got[files[2]]["s"] > 800, got
    assert not tm.run_samples(chunk, "INCOMPLETE", 600, tmp_path / "absent.xml", REPO_ROOT, 1)[0]["s"] < 100, "no junit (a crash): the time is shared"
    path = tmp_path / "h.json"
    tm.merge(path, [{"key": f, "s": 10.0, "slots": 4} for f in files], {"run": "before"})
    tm.merge(path, list(got.values()), {"run": "stopped"})
    h = tm.load(path)
    assert tm.lower_bounds(h, 4) == set(files[1:]), "a bound newer than the last full sample counts"
    ds = next(s for s in shards if s.name == "ds")
    out = ts.auto_split([ds], REPO_ROOT, tm.seconds(h, 4), 5.0, tm.lower_bounds(h, 4))
    alone = [c for c in out if any(f in c.paths for f in files[1:])]
    assert all(len(c.paths) == 1 and c.est_src == "lower bound" and c.test_part for c in alone), [(c.name, c.paths) for c in alone]
    assert all(c.est_min <= 5.0 + 1e-6 for c in out), "the target is a hard cap: a long file is cut into test parts"
    for f in files[1:]:  # each bounded file is planned at its bound at least, over its parts
        assert sum(c.est_min - ts.FIXED_MIN for c in alone if f in c.paths) * 60 >= got[f]["s"] / ts.slowdown(1) * ts.slowdown(4) - 1
    tm.merge(path, [{"key": f, "s": 12.0, "slots": 4} for f in files], {"run": "after"})
    assert not tm.lower_bounds(tm.load(path), 4), "a later full run answers the bound"


def test_a_stopped_chunks_bounds_share_only_the_time_its_keys_did_not_take(tmp_path):
    files = _ds_files(4)
    junit = _junit(tmp_path / "junit.xml", [(_mod(files[0]), "a", 30), (_mod(files[1]), "b", 20)])  # stopped inside files[1]
    busy = 2742
    got = {x["key"]: x["s"] for x in tm.run_samples({"files": files}, "INCOMPLETE", busy, junit, REPO_ROOT, 1)}
    elapsed = busy - ts.FIXED_MIN * 60
    assert sum(got.values()) == pytest.approx(elapsed), "the stopped key's own 20 s is counted once"
    assert got[files[1]] - got[files[2]] == pytest.approx(20.0)


def test_a_stopped_chunks_bounds_grow_by_the_share_of_its_tests_it_did_not_reach(tmp_path):
    """A chunk stopped at 2x its estimate after 18 of its 64 tests (tst-sls-b5, RASTER_TO_GRID on Serverless) needs about 3.6x
    its elapsed time: a bound of the elapsed time alone plans the next run short again, and it is stopped again."""
    chunk = {"tables": ["RASTER_TO_GRID"], "test_part": "0/2"}
    absent, busy = tmp_path / "absent.xml", 474
    elapsed = busy - ts.FIXED_MIN * 60
    plain = sum(x["s"] for x in tm.run_samples(chunk, "INCOMPLETE", busy, absent, REPO_ROOT, 1))
    grown = tm.run_samples(chunk, "INCOMPLETE", busy, absent, REPO_ROOT, 1, progress=(18, 64))
    assert plain == pytest.approx(elapsed) and all(x["lb"] for x in grown)
    assert sum(x["s"] for x in grown) == pytest.approx(elapsed * 64 / 18)
    assert sum(x["s"] for x in tm.run_samples(chunk, "INCOMPLETE", busy, absent, REPO_ROOT, 1, progress=(1, 500))) == pytest.approx(
        elapsed * tm.PROGRESS_SCALE_MAX), "a chunk that barely started is not extrapolated without limit"
    assert sum(x["s"] for x in tm.run_samples(chunk, "INCOMPLETE", busy, absent, REPO_ROOT, 1, progress=(0, 64))) == pytest.approx(elapsed)


def test_a_chunks_progress_is_read_from_its_pytest_log(tmp_path):
    log = tmp_path / "pytest.log"
    log.write_text("collecting ... collected 131 items / 67 deselected / 64 selected\n"
                   "test/x.py::a \x1b[32mPASSED\x1b[0m\x1b[32m [  1%]\x1b[0m\n"
                   "test/x.py::b \x1b[31mFAILED\x1b[0m\x1b[31m [  3%]\x1b[0m\n"
                   "test/x.py::c SKIPPED (no corpus) [  4%]\ntest/x.py::d \n")
    assert launcher.progress_of(log) == (3, 64)
    log.write_text("collected 7 items\ntest/x.py::a PASSED [ 14%]\n")
    assert launcher.progress_of(log) == (1, 7)
    assert launcher.progress_of(tmp_path / "absent.log") is None


def test_a_chunk_that_never_ran_leaves_no_zero_second_bound(tmp_path):
    files = _ds_files(2)
    absent = tmp_path / "absent.xml"
    assert tm.run_samples({"files": files}, "INCOMPLETE", None, absent, REPO_ROOT, 1) == []
    assert tm.run_samples({"files": files}, "INCOMPLETE", 30, absent, REPO_ROOT, 1) == [], "under the fixed start cost"
    path = tmp_path / "h.json"
    tm.merge(path, tm.run_samples({"files": files}, "INCOMPLETE", None, absent, REPO_ROOT, 1), {"run": "lost"})
    assert not set(files) & set(tm.seconds(tm.load(path), 4)), "no 0-minute estimate: the planner keeps CI's share"


def test_a_failed_or_partial_run_still_leaves_its_samples(tmp_path):
    files = _ds_files(2)
    junit = _junit(tmp_path / "junit.xml", [(_mod(files[0]), "a", 3), (_mod(files[0]), "b", 4), (_mod(files[1]), "c", 5)])
    assert {x["key"]: x["s"] for x in tm.run_samples({"files": files}, "FAILED", 60, junit, REPO_ROOT, 4)} == {files[0]: 7.0, files[1]: 5.0}
    assert tm.run_samples({"files": files}, "EMPTY", 60, junit, REPO_ROOT, 4) == []
    assert tm.run_samples({"files": files}, "ERRORS", 60, junit, REPO_ROOT, 4) == [], "a collection error times nothing"


def test_the_family_file_is_timed_and_cut_by_oracle_table(shards, tmp_path):
    fam = next(s for s in shards if s.name == "manager family 1/3")
    tables = fam.oracle_tables.split(",")
    fn_table = tm.table_of_function(REPO_ROOT)
    fn = next(n for n, t in fn_table.items() if t == tables[0])
    cls = "test.manager.test_family_cross_tier"
    junit = _junit(tmp_path / "junit.xml", [(cls, f"test_family_oracle[{fn}-L3--sql]", 40), (cls, f"test_family_oracle_uc_handlers[{tables[1]}-x]", 7),
                                           (cls, "test_detect_sees_the_jar_and_gdal", 2)])
    got = {x["key"]: x["s"] for x in tm.run_samples({"tables": tables}, "ok", 100, junit, REPO_ROOT, 1)}
    assert got == {f"table:{tables[0]}": 40.0, f"table:{tables[1]}": 7.0, "table:*": 2.0}
    secs = {f"table:{t}": 30.0 for t in tables}
    secs[f"table:{tables[0]}"] = 900.0  # one heavy table
    out = ts.auto_split([fam], REPO_ROOT, secs, 5.0)
    heavy = [c for c in out if tables[0] in c.oracle_tables.split(",")]
    assert heavy and all(c.oracle_tables == tables[0] for c in heavy), "the heavy table has chunks of its own"
    assert sorted(c.test_part for c in heavy) == [f"{i}/{len(heavy)}" for i in range(len(heavy))], "cut into test parts: 15 min > 5 min"
    whole = [c for c in out if c not in heavy] + heavy[:1]
    assert sorted(t for c in whole for t in c.oracle_tables.split(",")) == sorted(tables) and all(c.est_src == "history" for c in out)
    assert max(c.est_min for c in out) <= 5.0 + 1e-6


def test_one_file_longer_than_the_target_gets_a_chunk_of_its_own():
    got = ts.pack({"a": 30.0, "b": 2.0, "c": 2.0, "d": 1.0, "e": 1.0}, 4.0)
    assert got[0] == ["a"] and sorted(x for g in got for x in g) == ["a", "b", "c", "d", "e"]
    assert ts.pack({"a": 1.0}, 4.0) == [["a"]]
    assert len(ts.pack({f"f{i}": 1.0 for i in range(10)}, 7.0)) == 2, "10 min of small files over a 6.3 min budget: two chunks, not one"


def test_history_wins_over_the_ci_minutes_from_its_first_sample(shards):
    ds = next(s for s in shards if s.name == "ds")
    files = ts.files_of(ds, REPO_ROOT)
    assert ts.estimate_src(ds, REPO_ROOT, {}) == (float(ds.expected_min), "ci")
    one = {files[0]: 600.0}
    est, src = ts.estimate_src(ds, REPO_ROOT, one)
    assert src == "partial" and est > ds.expected_min + 9, "one timed file already moves the estimate"
    assert ts.estimate_src(ds, REPO_ROOT, {f: 1.0 for f in files})[1] == "history"


def test_the_plan_shows_where_each_estimate_came_from(shards):
    ds = next(s for s in shards if s.name == "ds")
    files = ts.files_of(ds, REPO_ROOT)
    chunks = ts.auto_split([ds, next(s for s in shards if s.name == "zarr 1/4")], REPO_ROOT, {f: 40.0 for f in files}, 5.0)
    plan = _plan(_args(), chunks)
    text = launcher.explain(plan, [], [launcher.estimate_note(chunks, {"keys": {"a": 1}, "runs": [1]}, "h.json")])
    assert " h)" in text and " ci)" in text and "h = history" in text
    assert "from history" in text and "from CI minutes" in text and "holds 1 key(s) from 1 run(s)" in text
    assert plan["shards"][chunks[0].slug]["files"] == chunks[0].paths and plan["shards"][chunks[0].slug]["est_src"] == "history"


def test_the_backfill_reads_earlier_runs_once(tmp_path):
    files = _ds_files(2)
    run = tmp_path / "runs" / "tst-old"
    _junit(run / "ds-1-2" / "junit.xml", [(_mod(files[0]), "a", 8)])
    (run / "ds-1-2" / "result.json").write_text(json.dumps({"rc": 0, "reason": ""}))
    _junit(run / "ds-2-2" / "junit.xml", [(_mod(files[1]), "b", 9)])
    (run / "ds-2-2" / "result.json").write_text(json.dumps({"rc": 130, "reason": "shard cap 45 min"}))  # stopped: no full sample
    (run / "summary.md").write_text("schedule: 2 chunks on 2 lane(s) (8 slots per cluster)")
    path = tmp_path / "h.json"
    assert tm.backfill(path, tmp_path / "runs", REPO_ROOT) == {"runs": 1, "keys": 1}
    assert tm.backfill(path, tmp_path / "runs", REPO_ROOT) == {"runs": 1, "keys": 0}, "a run is merged once"
    assert tm.load(path)["keys"][files[0]]["samples"][0]["slots"] == 8


def test_the_summary_says_the_speedup_over_a_serial_run():
    line = launcher.speedup_line({"wall_s": 600, "longest_s": 500, "serial_s": 3000})
    assert "1.2x the longest task" in line and "3000 s" in line and "speedup 5.0x" in line
    assert launcher.speedup_line({"wall_s": "-"}) == "wall - s"


def test_the_target_gives_about_three_and_a_half_chunks_per_slot(shards):
    light = ts.select(shards, "all-light")
    for slots in (4, 12):
        target = ts.auto_target_min(light, REPO_ROOT, {}, slots)
        chunks = ts.auto_split(light, REPO_ROOT, {}, target)
        assert slots * 2.5 <= len(chunks) and all(c.est_min <= target + 1e-6 for c in chunks), (slots, target, len(chunks))
    assert ts.auto_target_min(light, REPO_ROOT, {}, 1000) == 3.0, "never below what a task's fixed cost allows"
    assert ts.auto_target_min(light, REPO_ROOT, {}, 1) == 5.0, "never over 5 min: a chunk stays bite-sized"
    assert ts.auto_target_min(light[:1], REPO_ROOT, {}, 1) == round(light[0].expected_min / 3.5, 2)


def _rec(who, start, shards, end):
    return {"runner": who, "start": start, "end": end, "shards": [{"slug": n, "start": a, "end": b} for n, a, b in shards]}


def test_pool_metrics_say_the_busy_waiting_and_idle_time_of_each_slot():
    recs = [_rec("r0", 1000, [("a", 1000, 1100), ("b", 1100, 1200)], 1201), _rec("r1", 1000, [("c", 1010, 1150)], 1151)]
    got = launcher.pool_metrics(recs)
    assert got["chunks"] == 3 and got["slots"] == 2 and got["task_longest_s"] == 140 and got["task_mean_s"] == 113
    assert got["span_s"] == 201 and got["tail_s"] == 50 and got["ideal_s"] == 170
    assert got["idle_slot_s"] == 2 * 201 - 340
    r0, r1 = got["per_slot"]
    assert (r0["busy_s"], r0["waited_s"], r0["idle_end_s"]) == (200, 0, 0)
    assert (r1["busy_s"], r1["waited_s"], r1["idle_end_s"]) == (140, 10, 50), "r1 claimed 10 s late while b was still queued"
    text = "\n".join(launcher.schedule_lines(got))
    assert "3 chunks on 2 slot(s)" in text and "slot r1: 1 chunk(s), busy 140 s, waited 10 s" in text and "idle 50 s at the end" in text
    assert launcher.pool_metrics([]) == {} and launcher.schedule_lines({}) == []


def test_a_slot_that_quit_while_shards_were_queued_counts_as_waiting():
    recs = [_rec("r0", 0, [("a", 0, 10)], 10), _rec("r1", 0, [("b", 0, 50), ("c", 50, 90)], 90)]
    got = launcher.pool_metrics(recs)
    assert got["per_slot"][0]["waited_s"] == 40, "r0 left at 10 s; c was claimed only at 50 s"


def test_the_default_is_the_pre_push_pass_of_the_affected_shards():
    assert launcher.parse([]).shards == "affected"
    assert launcher.parse(["--shards", "all-light"]).shards == "all-light"


def test_a_pre_push_pass_with_nothing_changed_runs_nothing(monkeypatch, capsys):
    monkeypatch.setattr(ts, "changed_files", lambda *a, **k: [])
    assert launcher.main(["--explain-only"]) == 0
    assert "nothing to run" in capsys.readouterr().out


def test_a_plan_for_more_clusters_has_more_lanes_and_a_shorter_expected_wall(shards):
    picked = ts.select(shards, "all-light")
    chunks = ts.auto_split(picked, REPO_ROOT, {}, 5.0)
    one = _plan(_args("--max-parallel", "4"), chunks)["pools"]["gbx-test-1n"]
    three = _plan(_args("--max-parallel", "4", "--clusters", "3"), chunks)["pools"]["gbx-test-1n"]
    assert three["runners"] == 3 * one["runners"] and three["expected_min"] < one["expected_min"] / 2


def test_seconds_measured_at_eight_slots_are_brought_to_the_slots_of_the_plan():
    assert ts.slowdown(2) == 1.0 and round(ts.slowdown(8), 1) == 2.4
    got = ts.rescale_seconds({"a": 100.0}, 8, 4)
    assert 45 < got["a"] < 55, "half of it: the slowdown of 8 slots over that of 4"
    assert ts.rescale_seconds({"a": 100.0}, 4, 4) == {"a": 100.0}


def test_the_tarball_carries_every_committed_file_the_tests_read():
    # a node once lacked .mvn (test_maven_wrapper, the container guards) and pom.xml (test/bench/test_spec.py finds the root by it)
    listed = subprocess.run(["git", "archive", "--format=tar", "HEAD", *launcher.SUBSET], cwd=REPO_ROOT, capture_output=True, check=True).stdout
    import io
    import tarfile

    names = set(tarfile.open(fileobj=io.BytesIO(listed)).getnames())
    for f in (".mvn/wrapper/maven-wrapper.properties", "pom.xml", "rust-toolchain.toml", ".github/workflows/build_main.yml"):
        assert f in names, f
    assert any(n.startswith("src/test/scala/") for n in names), "the heavy tests read the Scala test sources (TestL1.scala)"
    assert not any(n.startswith(("src/test/resources/", "resources/")) for n in names), "staged once per content, or read by no shard"


def test_the_node_checkout_is_a_repository_that_holds_every_file(shards):
    src = _cell_source(shards)
    assert '["add", "-A", "-f"]' in src and "--allow-empty" not in src, "git ls-files / ls-tree HEAD see the files, as in the commit under test"


def test_a_runner_unpacks_the_checkout_once_and_its_later_chunks_reuse_it(tmp_path):
    unpacked, resets = [], []

    def unpack(repo):
        os.makedirs(repo)
        (Path(repo) / "f.py").write_text("x")
        unpacked.append(repo)

    def reset(repo):
        resets.append(repo)
        return True

    want = {"tarball": "/Volumes/v/t1-src.tar.gz", "resources": "abc"}
    got = [sr.node_checkout(str(tmp_path), "r0", want, unpack, reset) for _ in range(3)]
    assert [g[0] for g in got] == ["unpacked", "reused", "reused"] and len(unpacked) == 1, "one unpack per runner"
    assert len({g[1] for g in got}) == 1 and len(resets) == 2, "each later chunk resets the same tree"
    assert sr.node_checkout(str(tmp_path), "r1", want, unpack, reset)[0] == "unpacked", "another runner has a tree of its own"
    assert sr.node_checkout(str(tmp_path), "r0", dict(want, tarball="/other"), unpack, reset)[::2] == ("unpacked", "another launch's tree")
    assert sr.node_checkout(str(tmp_path), "r0", dict(want, tarball="/other"), unpack, lambda repo: "git clean: rc 128")[::2] == \
        ("unpacked", "reset failed: git clean: rc 128"), "a tree that cannot be reset is unpacked afresh, and says why"


def test_the_shard_cells_git_reset_brings_a_used_tree_back_to_its_commit(shards, tmp_path):
    src = _cell_source(shards)
    head = src[src.index("def _unpack(repo):"): src.index("t_co = time.time()")]
    ns = {"os": os, "shutil": __import__("shutil"), "tarfile": __import__("tarfile"), "PLAN": {}}
    exec(compile(textwrap.dedent(head), "reset", "exec"), ns)  # noqa: S102
    repo = tmp_path / "repo"
    (repo / "python/geobrix/lib").mkdir(parents=True)
    (repo / "a.py").write_text("a")
    for cmd in (["init", "-q"], ["add", "-A", "-f"], ["-c", "user.name=t", "-c", "user.email=t@x", "commit", "-q", "-m", "c"]):
        assert ns["_git"](str(repo), *cmd).returncode == 0
    (repo / "a.py").write_text("changed by a chunk")
    (repo / "spark-warehouse").mkdir()
    (repo / "python/geobrix/lib/g.jar").write_text("jar")
    assert ns["_reset"](str(repo)) is True
    assert (repo / "a.py").read_text() == "a" and not (repo / "spark-warehouse").exists() and (repo / "python/geobrix/lib/g.jar").exists()
    assert "rc" in str(ns["_reset"](str(tmp_path / "absent"))), "a failed reset says what failed"


def test_the_shard_cell_takes_its_checkout_from_the_runner_and_says_which(shards):
    src = _cell_source(shards)
    assert "def node_checkout(" in src and "node_checkout(base," in src and '"clean", "-fdxq"' in src
    assert 'checkout=CHECKOUT' in src, "the result says whether the chunk unpacked or reused the checkout"
    assert src.index('"/checkout.json"') < src.index("run_guard("), "said before the wheel guard, which may fail the chunk"
    ns = {}
    exec(compile(src[: src.index("import faulthandler")], "head", "exec"), ns)  # noqa: S102 - the inlined helpers stand alone
    assert ns["chunk_failed"]({"rc": 1}) and callable(ns["node_checkout"])
    assert '"runner": WHO' in launcher.runner_source(_plan(_args(), ts.select(shards, "ds"))), "the runner names itself to its chunks"


def test_a_fresh_cluster_run_makes_one_pool_per_kind_so_light_and_jar_do_not_share_a_cluster():
    light, jar = launcher.fresh_pool_name("tst-1", False), launcher.fresh_pool_name("tst-1", True)
    assert light != jar and light.endswith("-light") and jar.endswith("-jar")
    assert len(launcher.fresh_pool_name("x" * 80, True)) <= 60
    a = launcher.parse(["--run-id", "tst-1", "--fresh-cluster"])
    assert launcher.pool_name_of(a, "gbx-test-1n") == light and launcher.pool_name_of(a, "gbx-test-1n-jar", True) == jar
    assert launcher.pool_name_of(launcher.parse([]), "gbx-test-1n") == "gbx-test-1n"


class _CancelW:
    def __init__(self):
        self.cancelled = []
        self.jobs = type("J", (), {"cancel_run": lambda _s, rid: self.cancelled.append(rid)})()


def test_a_failed_launch_cancels_the_runs_it_has_not_seen_finish():
    w = _CancelW()
    runs, finished = {"gbx-test-1n": 11, "gbx-test-1n-jar": 22}, {"gbx-test-1n"}
    with pytest.raises(KeyboardInterrupt):
        with launcher.cancel_on_error(w, runs, finished):
            raise KeyboardInterrupt
    assert w.cancelled == [22], "the run that finished is left alone"


def test_a_launch_that_ends_well_cancels_nothing():
    w = _CancelW()
    with launcher.cancel_on_error(w, {"a": 1}, {"a"}):
        pass
    assert w.cancelled == []
    assert launcher.cancel_unfinished(w, {"a": 1, "b": 2}, set()) == [1, 2] and w.cancelled == [1, 2]


def test_affected_diffs_from_the_merge_base_so_the_base_side_is_not_counted(tmp_path):
    def git(*args):
        subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.com", *args], cwd=tmp_path, check=True, capture_output=True)

    git("init", "-q", "-b", "main")
    (tmp_path / "a.txt").write_text("a")
    git("add", "."), git("commit", "-q", "-m", "a")
    git("checkout", "-q", "-b", "feature")
    (tmp_path / "mine.txt").write_text("m")
    git("add", "."), git("commit", "-q", "-m", "mine")
    git("checkout", "-q", "main")
    (tmp_path / "theirs.txt").write_text("t")  # the base moves on after the branch left it
    git("add", "."), git("commit", "-q", "-m", "theirs")
    assert ts.changed_files(tmp_path, "main", "feature") == ["mine.txt"]


def _cell_source(shards):
    plan = _plan(_args(), ts.select(shards, "ds"), wheel=Path("/x/geobrix-0.6.0+env6-py3-none-any.whl"))
    return launcher.run_source(plan)


def test_the_overlay_removes_modules_the_commit_deleted_and_keeps_the_payload(shards, tmp_path):
    src = _cell_source(shards)
    start, end = src.index("inst, src_pkg ="), src.index("sys.path.insert(0, pkg)")
    snippet = textwrap.dedent(src[start:end])
    # the sitecustomize lines sit between the overlay and the path insert: keep only the overlay's own lines
    snippet = snippet[: snippet.index("# A test that starts")] if "# A test that starts" in snippet else snippet
    sitedir, pkg = tmp_path / "site", tmp_path / "repo"
    inst = sitedir / "databricks/labs/gbx"
    (inst / "_vendor/x86_64").mkdir(parents=True)
    (inst / "_vendor/x86_64/rasterio.py").write_text("payload")
    (inst / "old_module.py").write_text("deleted at the commit")
    (inst / "ds").mkdir()
    (inst / "ds/gone.py").write_text("gone")
    (inst / "_build_info.py").write_text("GIT_SHA = 'x'")
    (inst / "keep.py").write_text("old")
    srcdir = pkg / "src/databricks/labs/gbx"
    (srcdir / "ds").mkdir(parents=True)
    (srcdir / "keep.py").write_text("new")
    ns = {"os": __import__("os"), "shutil": __import__("shutil"), "sitedir": str(sitedir), "pkg": str(pkg)}
    exec(compile(snippet, "overlay", "exec"), ns)  # noqa: S102
    assert not (inst / "old_module.py").exists() and not (inst / "ds/gone.py").exists()
    assert (inst / "_vendor/x86_64/rasterio.py").read_text() == "payload" and (inst / "_build_info.py").exists()
    assert (inst / "keep.py").read_text() == "new"


def test_a_timed_out_test_escapes_a_per_case_except_and_its_connect_operations_end(shards, capsys):
    """The cell's timeout, run: it reaches a test that catches Exception per case (the family oracles re-run a failed batch
    case by case), and it interrupts a Spark Connect session's operations; a classic session is left alone."""
    src = _cell_source(shards)
    start, end = src.index("class TestTimeout(BaseException):"), src.index("def watchdog():")
    calls = []

    class ConnectSession:  # the module a Spark Connect session's class lives in is what the helper reads
        def interruptAll(self):
            calls.append("interruptAll")
            return ["op-1", "op-2"]

    ConnectSession.__module__ = "pyspark.sql.connect.session"
    ran = {}

    def per_case_test():  # a harness that judges each case and goes on after any Exception
        ns["MAIN_THREAD"] = threading.get_ident()
        ready.set()
        try:
            for _ in range(400):
                try:
                    __import__("time").sleep(0.01)
                except Exception:  # noqa: BLE001 - what the oracle harness does with a failed case
                    ran["swallowed"] = True
            ran["outcome"] = "ran on"
        except BaseException as exc:  # noqa: BLE001 - the timeout
            ran["outcome"] = type(exc).__name__ + ": " + str(exc)

    ns = {"threading": threading, "state": {"timed_out": []}, "TEST_TIMEOUT": 120.0, "spark": ConnectSession(),
          "interrupt_session": sr.interrupt_session}
    exec(compile(textwrap.dedent(src[start:end]), "timeout", "exec"), ns)  # noqa: S102
    ready = threading.Event()
    t = threading.Thread(target=per_case_test)
    t.start()
    ready.wait(5)
    ns["time_out_test"]("t.py::t")
    t.join(10)
    assert ran["outcome"] == "TestTimeout: t.py::t ran past 120 s (--test-timeout)" and "swallowed" not in ran
    assert calls == ["interruptAll"] and ns["state"]["timed_out"] == ["t.py::t"]
    assert "Spark session: interrupted 2 operation(s)" in capsys.readouterr().out

    class ClassicSession:
        def interruptAll(self):  # pragma: no cover - never called: a pool node's context is shared
            calls.append("classic")

    assert sr.interrupt_session(ClassicSession()) == "not a Spark Connect session: left running" and calls == ["interruptAll"]

    class Refusing:
        def interruptAll(self):
            raise RuntimeError("session closed")

    Refusing.__module__ = "pyspark.sql.connect.session"
    assert sr.interrupt_session(Refusing()) == "interruptAll failed: RuntimeError: session closed"


def test_a_serverless_chunk_works_in_a_temp_folder_its_own_user_makes(shards, tmp_path, monkeypatch):
    """The cell's scratch block, run: on Serverless a folder of the chunk's own under the temp dir (another chunk's user's folder
    can go with that user's files there), elsewhere the launch's shared folder on the scratch disk."""
    import shutil
    import tempfile
    import time

    src = _cell_source(shards)
    start, end = src.index("try:  # the node's local disk; Serverless has none to write"), src.index("try:  # the runner that runs this chunk")
    (tmp_path / "not-a-dir").write_text("x")  # the node's disk this host lacks: makedirs under a file fails as it does there
    snippet = src[start:end].replace("/local_disk0/gbxtest", str(tmp_path / "not-a-dir" / "gbxtest"))
    temp = tmp_path / "temp"
    temp.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(temp))

    def scratch_of(plan):
        ns = {"os": os, "shutil": shutil, "time": time, "PLAN": plan, "SLUG": "files-family-1-2"}
        exec(compile(snippet, "scratch", "exec"), ns)  # noqa: S102
        return ns

    pool = scratch_of({"run_id": "t1", "attempt": "0"})
    assert pool["OWN_BASE"] is False and pool["base"] == str(temp / "gbxtest" / "t1-0")
    sls = scratch_of({"run_id": "t1", "attempt": "0", "serverless": True})
    assert sls["OWN_BASE"] is True and os.path.dirname(sls["base"]) == str(temp) and os.path.basename(sls["base"]).startswith("gbxtest-")
    assert os.path.isdir(sls["root"] + "/out") and sls["root"] == sls["base"] + "/files-family-1-2"
    assert os.stat(sls["base"]).st_uid == os.getuid(), "made by this chunk's own user"


def test_a_lost_scratch_is_named_with_what_is_left_also_after_a_test_it_broke(shards, tmp_path, capsys):
    src = _cell_source(shards)
    start = src.index("LOST = None")
    snippet = src[start:src.index("result = dict(shard=", start)]
    base = tmp_path / "gbxtest-x"
    base.mkdir()
    root = base / "files-family-1-2"  # its out/ is gone

    def run(reason):
        ns = {"os": os, "root": str(root), "base": str(base), "scratch": str(tmp_path), "OWN_BASE": True, "state": {"reason": reason}}
        exec(compile(snippet, "lost", "exec"), ns)  # noqa: S102
        return ns

    ns = run("FileNotFoundError: [Errno 2] No such file or directory: '/tmp/x/repo'")
    lost = {"root": False, "base": True, "scratch": True, "uid": os.getuid(), "own_base": True}
    assert ns["LOST"] == lost and (root / "out").is_dir(), "the result can be written"
    assert ns["state"]["reason"].startswith("FileNotFoundError: [Errno 2] No such file or directory: '/tmp/x/repo'; the chunk's scratch ")
    assert str(lost) in ns["state"]["reason"] and "went during its run" in capsys.readouterr().out
    assert run("")["LOST"] is None and run("")["state"]["reason"] == "", "a scratch still there is no finding"
    assert "lost=LOST)" in src


def test_a_serverless_launch_refuses_a_wheel_whose_package_code_differs_from_the_sha(monkeypatch):
    import subprocess as sp

    calls = []

    def fake_git(*cmd):
        calls.append(cmd)
        if cmd[0] == "rev-parse":
            if cmd[2].startswith("gone"):
                raise sp.CalledProcessError(128, "git")
            return cmd[2].split("^")[0] + "\n"
        return {"same": "", "moved": "python/geobrix/src/databricks/labs/gbx/bench/oracles.py\n"}[cmd[2]]

    monkeypatch.setattr(launcher, "git", fake_git)
    assert launcher.serverless_wheel_problem("", "s" * 40) == "", "an unknown build commit is not checked"
    assert launcher.serverless_wheel_problem("same", "s" * 40) == ""
    assert calls[-1] == ("diff", "--name-only", "same", "s" * 40, "--", "python/geobrix/src")
    got = launcher.serverless_wheel_problem("moved", "s" * 40)
    assert got.startswith("the wheel was built from moved, and 1 file(s) of python/geobrix/src differ at ssssssssss")
    assert "bench/oracles.py" in got and "Fix: rebuild with gbx:build:wheel --env 6" in got
    assert "is not in this repository" in launcher.serverless_wheel_problem("gone1", "s" * 40)


def test_a_shard_leaves_its_log_and_a_locked_heartbeat_while_it_runs(shards):
    src = _cell_source(shards)
    for needle in ("beat_lock = threading.Lock()", "def keep_log()", "pytest_runtest_logstart", "keep_log()"):
        assert needle in src
    compile(src, "runner", "exec")


def test_a_run_is_stuck_only_when_every_running_shard_has_stopped_beating():
    stuck = launcher.run_is_stuck
    changed = {}
    beats = {"a": {"ts": 1.0}, "b": {"ts": 1.0}}
    assert not stuck(["a", "b"], beats, changed, 0.0, 900.0), "first sight: the clock starts now"
    assert stuck(["a", "b"], beats, changed, 1000.0, 900.0)
    beats["b"] = {"ts": 2.0}  # b beat again
    assert not stuck(["a", "b"], beats, changed, 1100.0, 900.0)
    assert not stuck([], beats, changed, 9999.0, 900.0), "nothing running is not stuck"


def test_a_half_written_runner_record_never_makes_a_healthy_run_look_unstarted_or_stuck():
    pw = launcher.PoolWatch()
    rec0 = {"runner": "r0", "current": "a", "shards": [{"slug": "z", "start": 0, "end": 5}]}
    rec1 = {"runner": "r1", "current": "b", "shards": []}
    assert [r["runner"] for r in pw.records({"r0": rec0, "r1": rec1})] == ["r0", "r1"] and pw.started
    recs = pw.records({"r0": None, "r1": None})  # both copies caught mid-write
    assert pw.started, "a pool that started stays started: the start cap never cancels it"
    current = [r["current"] for r in recs if r.get("current") and not r.get("done")]
    assert current == ["a", "b"], "the shards the runners were on stay in the stuck check"
    assert pw.beats({"a": {"ts": 1.0}}) == {"a": {"ts": 1.0}} and pw.beats({"a": None}) == {"a": {"ts": 1.0}}
    changed = {}
    launcher.run_is_stuck(current, {"a": {"ts": 1.0}, "b": {"ts": 1.0}}, changed, 0.0, 60.0)
    assert not launcher.run_is_stuck(current, {"a": {"ts": 1.0}, "b": {"ts": 2.0}}, changed, 100.0, 60.0), "b moved: not stuck"
    fresh = launcher.PoolWatch()
    fresh.records({"r0": {"runner": "r0", "current": "a", "shards": []}})
    assert not fresh.started, "a claim with no beat and no finished shard is not a start (it may hang in its install)"


def test_a_shard_that_has_not_started_or_one_that_died_earlier_does_not_decide():
    changed = {}
    beats = {"killed": {"ts": 1.0}, "live": {"ts": 1.0}}
    launcher.run_is_stuck(["live"], beats, changed, 0.0, 900.0)
    assert not launcher.run_is_stuck(["live", "next"], beats, changed, 5000.0, 900.0), "next is installing: no heartbeat yet"
    assert launcher.run_is_stuck(["live"], beats, changed, 5000.0, 900.0), "the killed shard's old beat is not counted in either direction"
    beats["live"]["done"] = True
    assert not launcher.run_is_stuck(["live"], beats, changed, 9000.0, 900.0), "a finished shard is not a hung one"


def test_a_split_part_that_holds_only_deselected_tests_does_not_fail_the_run():
    part = dict(name="ds [3/3]", verdict="EMPTY")
    whole = dict(name="ds", verdict="EMPTY")
    assert not launcher.is_failure(part) and launcher.is_failure(whole)
    assert not launcher.is_failure(dict(name="ds", verdict="ok"))
    for v in ("FAILED", "ERRORS", "INCOMPLETE"):
        assert launcher.is_failure(dict(name="ds [1/2]", verdict=v))


def test_two_shards_with_the_same_task_key_are_refused(shards):
    a = ts.Shard(name="ds [1/2]", kind="light")
    b = ts.Shard(name="DS [1/2]", kind="light")
    with pytest.raises(ValueError, match="same task key"):
        _plan(_args(), [a, b])


def test_the_pool_gets_the_jar_when_any_of_its_shards_needs_it(shards):
    mixed = ts.select(shards, "pyrx") + ts.select(shards, "heavy rasterx")
    plan = _plan(_args("--pool-jar", "gbx-test-1n"), mixed)
    assert plan["pools"]["gbx-test-1n"]["jar"] is True, "light and heavy shards in one pool: the cluster carries the JAR"


def test_each_pools_notebook_header_gives_the_times_of_its_own_shards(shards):
    plan = _plan(_args(), ts.select(shards, "pyrx,heavy rasterx"))
    plan["shards"]["pyrx-1-2"]["expected_min"] = 17.0
    plan["shards"]["heavy-rasterx"]["expected_min"] = 2.0
    light_head = launcher.header_md(plan, "gbx-test-1n")
    heavy_head = launcher.header_md(plan, "gbx-test-1n-jar")
    assert "about 17-17 min" in light_head and "about 2-2 min" in heavy_head


def test_the_estimate_and_the_split_count_an_untimed_file_the_same(shards):
    ds = next(s for s in shards if s.name == "ds")
    files = ts.files_of(ds, REPO_ROOT)
    secs = {f: 6.0 for f in files[: len(files) // 2 + 1]}  # a bit over half the files timed
    est = ts.estimate(ds, REPO_ROOT, secs)
    parts = ts.split_shard(ds, 2, REPO_ROOT, secs)
    total_parts = sum(p.est_min - ts.FIXED_MIN for p in parts)
    assert abs((est - ts.FIXED_MIN) - total_parts) < 0.05


def test_a_missing_base_is_a_usage_error_not_a_traceback(capsys):
    assert launcher.main(["--explain-only", "--shards", "affected", "--base", "no-such-rev/at-all"]) == launcher.EXIT_USAGE
    assert "cannot diff no-such-rev/at-all" in capsys.readouterr().err


def test_a_missing_wheel_does_not_stop_a_plan_but_does_stop_a_launch(monkeypatch, capsys):
    def no_wheel(explicit):
        raise SystemExit("PRECONDITION MISSING: expected one wheel, found 2; wheel stage not run.")

    monkeypatch.setattr(launcher.mut, "find_wheel", no_wheel)
    monkeypatch.setattr(launcher, "planned_clusters", lambda a: 1)
    assert launcher.main(["--explain-only", "--shards", "ds"]) == 0
    assert "no usable env6 wheel" in capsys.readouterr().out
    assert launcher.main(["--profile", "p", "--purpose", "x", "--shards", "ds"]) == launcher.EXIT_USAGE
    assert "PRECONDITION MISSING" in capsys.readouterr().err


def test_a_plan_that_only_explains_makes_no_copy_of_the_jar(tmp_path):
    jar = tmp_path / "geobrix-0.6.0-jar-with-dependencies.jar"
    jar.write_bytes(b"jar")
    got = launcher.stage_jar(jar, REPO_ROOT, copy=False)
    assert got["local"] == str(jar) and got["sha256"] and got["dir"].startswith("test-pool-jars/")


def test_wheel_takes_a_file_or_the_folder_that_holds_the_newest_env6_wheel(tmp_path):
    import os

    old = tmp_path / "geobrix-0.6.0+env6-py3-none-any.whl"
    old.write_bytes(b"PK")
    (tmp_path / "geobrix-0.6.0+env5-py3-none-any.whl").write_bytes(b"PK")  # the other family is not the one
    assert launcher.mut.find_wheel(str(tmp_path)) == old
    assert launcher.mut.find_wheel(str(tmp_path) + "/") == old
    assert launcher.mut.find_wheel(str(old)) == old
    newer = tmp_path / "geobrix-0.10.0+env6-py3-none-any.whl"
    newer.write_bytes(b"PK")
    os.utime(old, (1, 1))  # an older build, though its name sorts after the newer one
    assert launcher.mut.find_wheel(str(tmp_path)) == newer
    os.utime(old, (5, 5))
    os.utime(newer, (5, 5))  # equal mtimes (a copied folder): the higher version wins, not the later name
    assert launcher.mut.find_wheel(str(tmp_path)) == newer
    (tmp_path / "none").mkdir()
    with pytest.raises(SystemExit, match="no geobrix-.*env6.*in .*none"):
        launcher.mut.find_wheel(str(tmp_path / "none"), required=False)  # a folder the user named must hold a wheel


def test_jar_path_takes_a_file_or_the_folder_that_holds_the_jar(tmp_path):
    jar = tmp_path / "geobrix-0.6.0-jar-with-dependencies.jar"
    jar.write_bytes(b"PK")
    (tmp_path / "geobrix-0.6.0.jar").write_bytes(b"PK")  # the thin JAR is not the one
    assert launcher.jar_source(str(tmp_path)) == jar
    assert launcher.jar_source(str(tmp_path) + "/") == jar
    assert launcher.jar_source(str(jar)) == jar
    assert launcher.jar_source(str(tmp_path / "empty")) == tmp_path / "empty"  # no such file: refused by the caller
    (tmp_path / "none").mkdir()
    assert launcher.jar_source(str(tmp_path / "none")) is None
    import os

    newer = tmp_path / "geobrix-0.10.0-jar-with-dependencies.jar"
    newer.write_bytes(b"PK")
    os.utime(jar, (1, 1))  # an older build, though its name sorts after the newer one
    assert launcher.jar_source(str(tmp_path)) == newer
    os.utime(jar, (5, 5))
    os.utime(newer, (5, 5))  # equal mtimes (a copied folder): the higher version wins, not the later name
    assert launcher.jar_source(str(tmp_path)) == newer


# ---- the queue and the runners that take shards from it


def _picked_plan(shards, *extra):
    return _plan(_args(*extra), ts.select(shards, "pyrx,ds,zarr,scripts/ci"))


def test_the_planner_builds_runner_tasks_not_chained_shard_tasks(shards):
    plan = _picked_plan(shards, "--clusters", "2", "--max-parallel", "2", "--no-smoke")
    spec = plan["pools"]["gbx-test-1n"]
    assert spec["runners"] == 4 and "lanes" not in spec
    tasks = launcher.job_tasks(plan, "gbx-test-1n", {"runner": "/w/runner.ipynb", "shard": "/w/tests.ipynb"}, ["c1", "c2"])
    assert [t["task_key"] for t in tasks] == ["q_seed", "r_0", "r_1", "r_2", "r_3"]
    assert tasks[0]["depends_on"] == [] and tasks[0]["base_parameters"]["role"] == "seed"
    for i, t in enumerate(tasks[1:]):
        assert t["depends_on"] == ["q_seed"], "a runner waits for the queue only, never for another runner"
        assert t["existing_cluster_id"] == ["c1", "c2"][i % 2]
        assert t["base_parameters"] == {"pool": "gbx-test-1n", "role": "run", "runner": f"r{i}", "index": str(i), "shard_nb": "/w/tests.ipynb"}
        assert t["notebook_path"] == "/w/runner.ipynb" and t["timeout_seconds"] == int(spec["cap_min"] * 60)
    serverless = launcher.job_tasks(plan, "gbx-test-1n", {"runner": "/r", "shard": "/s"}, [])
    assert all(t.get("environment_key") == "gbx_env6" and "existing_cluster_id" not in t for t in serverless)


def test_the_queue_holds_every_shard_once_longest_first(shards):
    plan = _picked_plan(shards, "--no-smoke")
    spec = plan["pools"]["gbx-test-1n"]
    assert sorted(spec["queue"]) == sorted(plan["shards"]) and len(set(spec["queue"])) == len(spec["queue"])
    mins = [plan["shards"][s]["expected_min"] for s in spec["queue"]]
    assert mins == sorted(mins, reverse=True)
    assert sr.queue_order({"b": 2, "a": 2, "c": 9}) == ["c", "a", "b"], "ties by name, so the order is stable"
    sql = sr.DeltaQueue.create_sql("`c`.`s`.`q`", spec["queue"], {s: plan["shards"][s]["expected_min"] for s in spec["queue"]})
    assert sql.startswith("CREATE OR REPLACE TABLE `c`.`s`.`q` AS SELECT") and sql.count("('") == len(spec["queue"])
    fake = _FakeDelta()
    fake.sql(sql)
    assert [r[0] for r in fake.sql("SELECT slug FROM `c`.`s`.`q` WHERE owner IS NULL ORDER BY rank")] == spec["queue"]


class SimClock:
    """Virtual time for ``n`` threads: ``sleep(d)`` returns when every live thread sleeps and its wake time is the earliest, so a
    simulated hour takes milliseconds and the order of events is exact."""

    def __init__(self, n):
        self.now, self.live, self.sleepers, self.cv = 0.0, n, {}, threading.Condition()

    def time(self):
        return self.now

    def _advance(self):
        if self.sleepers and len(self.sleepers) == self.live:
            ident, wake = min(self.sleepers.items(), key=lambda kv: (kv[1], kv[0]))
            self.now = max(self.now, wake)
            del self.sleepers[ident]
            self.cv.notify_all()

    def sleep(self, d):
        me = threading.get_ident()
        with self.cv:
            self.sleepers[me] = self.now + d
            self._advance()
            while me in self.sleepers:
                self.cv.wait(5)

    def exit(self):
        with self.cv:
            self.live -= 1
            self._advance()


def _simulate(queue, n, actual):
    clock = SimClock(n)
    recs = [None] * n

    def runner(i):
        try:
            recs[i] = sr.run_loop(queue, f"r{i}", lambda slug: (clock.sleep(actual[slug]), {"rc": 0})[1], clock=clock.time, offset=i)
        finally:
            clock.exit()

    threads = [threading.Thread(target=runner, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert not any(t.is_alive() for t in threads), "the simulation hung"
    return recs


def test_no_slot_idles_while_the_queue_holds_a_shard_when_actual_times_differ_from_the_estimates(tmp_path):
    # 32 shards estimated alike, 4 slots. The old static lanes balanced the estimates; the actual times then differed by lane,
    # as on run 455320846339987 (lane busy times 42.9 / 41.4 / 30.2 / 19.9 min). Give each old lane's shards that lane's factor.
    est = {f"s{i:02d}": 5.0 + (i % 3) * 0.1 for i in range(32)}
    n = 4
    old_lanes = ts.lanes([ts.Shard(name=s, kind="light", est_min=m) for s, m in est.items()], n)
    factor = [42.9 / 33.6, 41.4 / 33.6, 30.2 / 33.6, 19.9 / 33.6]  # each lane's actual over the mean
    actual = {s.name: est[s.name] * factor[k] * 60 for k, lane in enumerate(old_lanes) for s in lane}
    static = [sum(actual[s.name] for s in lane) for lane in old_lanes]
    recs = _simulate(sr.DirQueue(str(tmp_path / "q"), sr.queue_order(est)), n, actual)
    ran = [x["slug"] for r in recs for x in r["shards"]]
    assert sorted(ran) == sorted(est), "every shard ran exactly once"
    m = sr.slot_metrics(recs)
    assert all(p["waited_s"] == 0 for p in m["per_slot"]), m["per_slot"]
    last_claim = max(x["start"] for r in recs for x in r["shards"])
    assert all(r["end"] >= last_claim for r in recs), "no runner left while a shard was still queued"
    dynamic = max(r["end"] for r in recs)
    assert dynamic <= sum(actual.values()) / n + max(actual.values()), "list scheduling's bound: the mean load plus one shard"
    assert max(static) - min(static) > 3 * max(actual.values()), ("the old lanes were uneven", static)
    assert dynamic < max(static) * 0.85, ("the queue finishes well before the slowest old lane", dynamic, static)


def test_the_claim_is_exclusive_under_concurrent_threads(tmp_path):
    order = [f"s{i:03d}" for i in range(200)]
    q = sr.DirQueue(str(tmp_path / "q"), order)
    got, lock = [], threading.Lock()

    def claimer(i):
        while True:
            slug = q.claim(f"t{i}", offset=i)
            if slug is None:
                return
            with lock:
                got.append(slug)

    threads = [threading.Thread(target=claimer, args=(i,)) for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert sorted(got) == order


CLAIMER = """
import sys
sys.path.insert(0, sys.argv[1])
import _shard_runner as sr
q = sr.DirQueue(sys.argv[2], sys.argv[3].split(","))
sys.stdin.readline()  # start together
while True:
    s = q.claim(sys.argv[4], offset=int(sys.argv[5]))
    if s is None:
        break
    print(s, flush=True)
"""


def test_the_claim_is_exclusive_under_concurrent_processes(tmp_path):
    order = [f"s{i:03d}" for i in range(120)]
    script = tmp_path / "claimer.py"
    script.write_text(CLAIMER)
    procs = [subprocess.Popen([sys.executable, str(script), str(TESTS), str(tmp_path / "q"), ",".join(order), f"p{i}", str(i)],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True) for i in range(8)]
    for p in procs:
        p.stdin.write("go\n")
        p.stdin.flush()
    got = []
    for p in procs:
        out, _ = p.communicate(timeout=60)
        assert p.returncode == 0
        got += out.split()
    assert sorted(got) == order, "each shard claimed by exactly one process"


class _Conflict(Exception):
    pass


class _FakeDelta:
    """A queue table with Delta's commit rule for UPDATE: the statement reads a snapshot, and its commit fails with a
    Concurrent*Exception when the row changed after that snapshot (a pause between the two widens the race)."""

    def __init__(self, pause=0.0):
        self.rows, self.version, self.lock, self.pause, self.commits = {}, 0, threading.Lock(), pause, 0

    def sql(self, q):
        import random
        import re
        import time

        q = " ".join(q.split())
        if q.startswith("CREATE OR REPLACE TABLE"):
            vals = re.findall(r"\('((?:[^'\\]|\\.)*)', (\d+), ([0-9.eE+-]+)\)", q)
            with self.lock:
                self.rows = {s: {"rank": int(r), "owner": None, "changed": 0} for s, r, _ in vals}
            return []
        if q.startswith("SELECT count(*)"):
            return [(len(self.rows),)]
        if q.startswith("SELECT slug"):
            with self.lock:
                return [(s,) for s, r in sorted(self.rows.items(), key=lambda kv: kv[1]["rank"]) if r["owner"] is None]
        if q.startswith("SELECT owner"):
            slug = re.search(r"slug = '([^']*)'", q).group(1)
            return [(self.rows[slug]["owner"],)]
        if q.startswith("UPDATE"):
            who, slug = re.search(r"owner = '([^']*)'.*slug = '([^']*)'", q).groups()
            with self.lock:
                v0, owner0 = self.version, self.rows[slug]["owner"]
            time.sleep(self.pause * random.random())
            with self.lock:
                if self.rows[slug]["changed"] > v0:
                    raise _Conflict("[DELTA_CONCURRENT_DELETE_READ] ConcurrentDeleteReadException: a concurrent update changed the row")
                if owner0 is not None:
                    return [(0,)]
                self.version += 1
                self.commits += 1
                self.rows[slug].update(owner=who, changed=self.version)
                return [(1,)]
        raise AssertionError(q)


def test_the_delta_claim_is_exclusive_under_concurrent_runners_and_retries_a_lost_commit():
    order = [f"s{i:02d}" for i in range(40)]
    fake = _FakeDelta(pause=0.002)
    fake.sql(sr.DeltaQueue.create_sql("t", order, {s: 1.0 for s in order}))
    got, lock, queues = [], threading.Lock(), []

    def claimer(i):
        q = sr.DeltaQueue(fake.sql, "t", sleep=lambda s: None, attempts=10000)
        queues.append(q)
        first = True
        while True:
            slug = q.claim(f"r{i}", offset=i if first else 0)
            first = False
            if slug is None:
                return
            with lock:
                got.append(slug)

    threads = [threading.Thread(target=claimer, args=(i,)) for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert sorted(got) == order and fake.commits == len(order)
    assert sum(q.conflicts for q in queues) > 0, "the race was exercised: some commits lost and were retried"


def test_the_delta_claim_raises_after_repeated_errors_that_are_not_conflicts():
    calls = []

    def sql(q):
        calls.append(q)
        if q.startswith("SELECT slug"):
            return [("a",)]
        raise RuntimeError("PERMISSION_DENIED")

    with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
        sr.DeltaQueue(sql, "t", sleep=lambda s: None).claim("r0")
    assert sum(q.startswith("UPDATE") for q in calls) == 4


def test_a_stalled_or_capped_shard_is_recorded_and_the_runner_goes_on(tmp_path):
    def nb(slug):
        if slug == "b":
            raise Exception("com.databricks.WorkflowException: com.databricks.NotebookExecutionException: Notebook run timed out after 3300 seconds")
        if slug == "c":
            return json.dumps({"rc": 130, "reason": "no test finished for 10 min (last: test/x.py::t)", "pid": 7})
        if slug == "d":
            raise RuntimeError("The Python process exited with exit code 137 (SIGKILL: Killed)")
        if slug == "e":
            raise ValueError("a harness bug outside run_shard")
        return json.dumps({"rc": 0, "reason": "", "pid": 1, "passed": 3})

    def execute(slug):
        if slug == "e":
            return nb(slug)  # an exception that reaches the loop itself
        return sr.run_shard(lambda: nb(slug), 3300)

    rec = sr.run_loop(sr.DirQueue(str(tmp_path / "q"), ["a", "b", "c", "d", "e", "f"]), "r0", execute)
    by = {x["slug"]: x for x in rec["shards"]}
    assert [x["slug"] for x in rec["shards"]] == ["a", "b", "c", "d", "e", "f"], "the runner went on after each stop"
    assert by["a"]["rc"] == 0 and by["a"]["passed"] == 3 and by["f"]["rc"] == 0
    assert "no result within 3300 s" in by["b"]["reason"] and by["b"]["rc"] is None
    assert by["c"]["rc"] == 130 and "no test finished" in by["c"]["reason"]
    assert "ended without a result" in by["d"]["reason"] and "137" in by["d"]["reason"]
    assert by["e"]["reason"].startswith("ValueError") and rec["done"] and not rec["error"]


def test_a_shard_without_a_result_takes_its_runners_reason_and_one_nobody_ran_says_so(shards, monkeypatch, tmp_path):
    plan = _picked_plan(shards)
    queue = plan["pools"]["gbx-test-1n"]["queue"]
    monkeypatch.setattr(launcher.mut, "download_dir", lambda w, remote, local: [])
    recs = {"gbx-test-1n": [{"runner": "r0", "shards": [{"slug": queue[0], "start": 0, "end": 9, "reason": "no result within 3300 s (outer timeout)"}]}]}
    rows = {r["slug"]: r for r in launcher.collect(None, plan, {}, tmp_path, recs)}
    assert rows[queue[0]]["verdict"] == "INCOMPLETE" and "outer timeout" in rows[queue[0]]["reason"] and rows[queue[0]]["claims"] == 1
    assert rows[queue[1]]["reason"].startswith("never claimed") and rows[queue[1]]["claims"] == 0
    recs["gbx-test-1n"].append({"runner": "r1", "shards": [{"slug": queue[0], "start": 0, "end": 9}]})
    assert "claimed 2 times" in {r["slug"]: r for r in launcher.collect(None, plan, {}, tmp_path, recs)}[queue[0]]["reason"]


# ---- the watchdog's label: a kill by the watchdog is never read as an out-of-memory


def test_a_shard_the_watchdog_killed_says_so_not_what_databricks_guesses(tmp_path):
    hb = tmp_path / "hb"
    hb.mkdir()
    oom = "com.databricks.NotebookExecutionException: FAILED: Execution ran out of memory or the kernel became unresponsive (exit code 137)"

    def killed():
        raise RuntimeError(oom)

    (hb / "a.json").write_text(json.dumps({"passed": 3, "reason": "killed by watchdog: shard cap 45 min", "watchdog": "shard cap 45 min"}))
    (hb / "b.json").write_text(json.dumps({"passed": 3, "reason": "", "watchdog": ""}))
    (hb / "c.json").write_text("{not json")
    got = sr.run_shard(killed, 3300, watchdog=lambda: sr.heartbeat_watchdog(str(hb / "a.json")))
    assert got["rc"] is None and got["reason"] == "killed by watchdog: shard cap 45 min"
    assert "out of memory" in got["platform_message"] and "out of memory" not in got["reason"]
    for name in ("b.json", "c.json", "absent.json"):  # the watchdog did not fire (or the heartbeat is unreadable): the platform's message stands
        other = sr.run_shard(killed, 3300, watchdog=lambda n=name: sr.heartbeat_watchdog(str(hb / n)))
        assert "ended without a result" in other["reason"] and "out of memory" in other["reason"]
    assert sr.watchdog_reason("killed by watchdog: x") == sr.watchdog_reason("x") == "killed by watchdog: x", "the label is added once"
    # a notebook that returned nothing, after a stall kill, is the same kill
    assert sr.run_shard(lambda: "", 3300, watchdog=lambda: "no test finished for 10 min (last: t)")["reason"] == "killed by watchdog: no test finished for 10 min (last: t)"
    assert sr.run_shard(lambda: json.dumps({"rc": 0, "passed": 1}), 3300, watchdog=lambda: "shard cap 45 min")["rc"] == 0, "a result is the result"
    rec = sr.run_loop(sr.DirQueue(str(tmp_path / "q"), ["a"]), "r0", lambda slug: sr.run_shard(killed, 3300, watchdog=lambda: "shard cap 45 min"))
    assert rec["shards"][0]["reason"] == "killed by watchdog: shard cap 45 min", "the runner's record carries it to the summary"


def test_the_shard_notebook_labels_what_its_watchdog_does_and_scales_the_stall_cap(shards):
    plan = _plan(_args(), ts.select(shards, "ds"))
    src = launcher.run_source(plan)
    assert 'state["watchdog"] = why' in src and 'state["reason"] = "killed by watchdog: " + why' in src
    assert 'STALL_MIN = S.get("stall_min") or CAPS["stall_min"]' in src and "STALL_MIN * 60" in src and 'CAPS["stall_min"] * 60' not in src
    ast = __import__("ast")
    ast.parse(src.replace("dbutils.", "d."))  # the cell is valid Python once formatted
    runner = launcher.runner_source(plan)
    assert "heartbeat_watchdog(STAGE" in runner and "HeavyGate(" in runner


def test_a_stalled_summary_names_the_kill():
    reason = "killed by watchdog: no test finished for 10 min (last: t.py::t)"
    row = dict(name="ds", slug="ds", pool="gbx-test-1n", verdict="INCOMPLETE", expected_min=12, passed=3, failed=0, errors=0, skipped=0,
               wall_s="-", reason=reason, failures=[], skip_reasons={}, est_src="ci")
    plan = {"run_id": "t1", "sha": "a" * 40, "wheel": {"name": "w.whl", "built_from": "b" * 40}, "purpose": "p", "jar": None}
    md = launcher.summary_md(plan, [row], {})
    assert "| ds | gbx-test-1n | INCOMPLETE |" in md and f"- {reason}" in md and "out of memory" not in md


# ---- heavy chunks: one at a time per cluster, and a stall cap that follows the recorded time


def test_the_gdal_engine_tables_are_heavy_and_packed_apart_from_the_light_ones(shards):
    fam = next(s for s in shards if s.name == "manager family 3/3")
    tables = fam.oracle_tables.split(",")
    assert {"SPEC_CALC", "SPEC_GDAL", "SPEC_SE"} <= set(tables) and {"SPEC_CALC", "SPEC_GDAL", "SPEC_SE"} <= ts.HEAVY_TABLES
    secs = {f"table:{t}": 60.0 for t in tables}  # a minute each: all fit one chunk of 12 min
    out = ts.auto_split([fam], REPO_ROOT, secs, 3.0)
    heavy = [c for c in out if c.heavy]
    assert heavy and all(set(c.oracle_tables.split(",")) <= ts.HEAVY_TABLES for c in heavy), "a chunk is heavy as a whole: no light table waits beside one"
    assert [c for c in out if not c.heavy] and not any(set(c.oracle_tables.split(",")) & ts.HEAVY_TABLES for c in out if not c.heavy)
    assert sorted(t for c in out for t in c.oracle_tables.split(",")) == sorted(tables)
    # a whole shard is judged the same way (annotate), and a file recorded long is heavy by the history alone
    whole = ts.annotate(fam, REPO_ROOT, secs)
    assert whole.heavy and whole.unit_max_min == 1.0
    ds = next(s for s in shards if s.name == "ds")
    f = ts.files_of(ds, REPO_ROOT)[0]
    assert not ts.annotate(ds, REPO_ROOT, {f: 60.0 * (ts.HEAVY_UNIT_MIN - 1)}).heavy
    assert ts.annotate(ds, REPO_ROOT, {f: 60.0 * ts.HEAVY_UNIT_MIN}).heavy


def test_the_stall_cap_follows_the_longest_unit_of_a_chunk_up_to_the_shard_cap():
    assert ts.stall_min_for(0.0, 10.0, 45.0) == 10.0 and ts.stall_min_for(8.0, 10.0, 45.0) == 10.0, "never below the flat cap"
    assert ts.stall_min_for(30.0, 10.0, 45.0) == 15.0 and ts.stall_min_for(200.0, 10.0, 45.0) == 45.0, "half the unit, never past the shard cap"


def test_the_plan_marks_heavy_chunks_and_scales_stall_caps_and_explain_shows_both(shards):
    fam = next(s for s in shards if s.name == "manager family 3/3")
    tables = fam.oracle_tables.split(",")
    secs = {f"table:{t}": 60.0 for t in tables}
    secs["table:SPEC_GDAL"] = 3000.0  # 50 min alone
    chunks = ts.auto_split([fam, next(s for s in shards if s.name == "ds")], REPO_ROOT, secs, 8.0)
    plan = _plan(_args("--clusters", "2"), chunks)
    spec = plan["pools"]["gbx-test-1n"]
    heavy = {s for s, v in plan["shards"].items() if v["heavy"]}
    assert heavy and spec["heavy"] == [s for s in spec["queue"] if s in heavy]
    gdal = [v for v in plan["shards"].values() if v["tables"] == ["SPEC_GDAL"]]
    assert len(gdal) == 7 and all(v["heavy"] and v["test_part"] and v["expected_min"] <= 8.0 for v in gdal), "50 min alone: 7 test parts under 8 min"
    assert gdal[0]["unit_max_min"] == round(50.0 / 7, 1) and all(v["stall_min"] == 10.0 for v in plan["shards"].values() if v["unit_max_min"] <= 20)
    assert ts.stall_min_for(50.0, 10.0, 45.0) == 25.0, "a long unit still scales the stall cap"
    text = launcher.explain(plan, [], [])
    assert ", heavy, part 0/7" in text and "at most one runs on a cluster at a time" in text


def test_the_expected_wall_waits_for_heavy_chunks_one_at_a_time_per_cluster():
    exp = {"h1": 20.0, "h2": 20.0, "h3": 20.0, "l1": 5.0, "l2": 5.0}
    queue = sr.queue_order(exp)
    free = launcher.schedule_wall(exp, queue, set(), runners=4, n_clusters=1)
    one = launcher.schedule_wall(exp, queue, {"h1", "h2", "h3"}, runners=4, n_clusters=1)
    two = launcher.schedule_wall(exp, queue, {"h1", "h2", "h3"}, runners=4, n_clusters=2)
    assert free == 20.0 and one == 60.0, "three heavy chunks of 20 min on one cluster run one after the other"
    assert two == 40.0, "two clusters: one runs two of them, the other one"
    spec = {}
    launcher.size_pool(spec, exp, 4, 1, 45.0, {"h1", "h2", "h3"})
    assert spec["expected_min"] == 60.0 and spec["heavy"] == ["h1", "h2", "h3"]
    plain = {}
    launcher.size_pool(plain, exp, 4, 1, 45.0)
    assert plain["heavy"] == [] and plain["expected_min"] == 20.0 and spec["cap_min"] > plain["cap_min"], "the runner timeout allows for the waits"


def _run_runners(queue_for, n, execute, heavy, gate_path, tmp_path):
    """n runner threads of one cluster, each with its own gate object on the cluster's one lock file."""
    recs, lock = [], threading.Lock()

    def runner(i):
        rec = sr.run_loop(queue_for(), f"r{i}", execute, offset=i, heavy=heavy, gate=sr.HeavyGate(gate_path), wait_s=0.01)
        with lock:
            recs.append(rec)

    threads = [threading.Thread(target=runner, args=(i,)) for i in range(n)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(30)
        assert not th.is_alive(), "a runner never ended: a waiting runner must wake when the heavy chunk ends"
    return recs


def test_runners_of_a_cluster_never_run_two_heavy_chunks_at_once_and_light_ones_go_on(tmp_path):
    import time

    order = ["h1", "l1", "h2", "l2", "h3", "l3", "l4", "l5"]
    heavy = {"h1", "h2", "h3"}
    live, peak, ran, started = set(), [0], [], threading.Event()
    seen = {}
    lock = threading.Lock()

    def execute(slug):
        with lock:
            live.add(slug)
            peak[0] = max(peak[0], len(live & heavy))
            ran.append(slug)
        if slug in heavy:
            started.set()
            time.sleep(0.3)
        elif slug == "l1":
            # a light chunk is taken while a heavy one is running: it waits for the heavy one to start, so a runner that could not
            # take light chunks beside a heavy one would stall here (and fail the check below)
            seen["heavy_started"] = started.wait(10)
            with lock:
                seen["heavy_live"] = bool(live & heavy)
        else:
            time.sleep(0.02)
        with lock:
            live.discard(slug)
        return {"rc": 0}

    q = tmp_path / "q"
    recs = _run_runners(lambda: sr.DirQueue(str(q), order, heavy), 4, execute, heavy, str(tmp_path / "node" / "heavy.lock"), tmp_path)
    assert sorted(ran) == sorted(order), "every chunk ran once"
    assert peak[0] == 1, "never two heavy chunks at once"
    assert seen == {"heavy_started": True, "heavy_live": True}, "the other slots took light chunks while a heavy one ran"
    assert all(r["done"] and not r["error"] for r in recs) and {s["slug"] for r in recs for s in r["shards"] if s["heavy"]} == heavy


def test_a_runner_waits_when_only_heavy_chunks_are_left_and_the_gate_is_taken(tmp_path):
    gate_path = str(tmp_path / "heavy.lock")
    other = sr.HeavyGate(gate_path)
    assert other.try_acquire() and not sr.HeavyGate(gate_path).try_acquire(), "one holder per lock file"
    q = sr.DirQueue(str(tmp_path / "q"), ["h", "l"], {"h"})
    assert q.claim("r0", heavy_ok=False) == "l" and q.claim("r0", heavy_ok=False) is sr.WAIT
    assert q.claim("r0", heavy_ok=True) == "h" and q.claim("r0", heavy_ok=False) is None
    # run_loop: while the other holder has the gate it waits; once released it takes the heavy chunk
    waits = []

    def sleep(s):
        waits.append(s)
        if len(waits) == 3:
            other.release()

    q2 = sr.DirQueue(str(tmp_path / "q2"), ["l", "h"], {"h"})
    rec = sr.run_loop(q2, "r1", lambda slug: {"rc": 0}, heavy={"h"}, gate=sr.HeavyGate(gate_path), wait_s=7.0, sleep=sleep)
    assert [s["slug"] for s in rec["shards"]] == ["l", "h"] and waits == [7.0] * 3
    assert sr.HeavyGate(gate_path).try_acquire(), "the runner let the gate go"
    # a runner killed with the gate held does not keep it: the lock goes with the descriptor
    holder = sr.HeavyGate(str(tmp_path / "g2"))
    assert holder.try_acquire()
    holder.release()
    assert sr.HeavyGate(str(tmp_path / "g2")).try_acquire()


def test_lost_commits_spread_over_the_free_rows_and_never_spend_the_claim_budget():
    tried, n = [], {"conflicts": 0}

    def sql(q):
        if q.startswith("SELECT slug"):
            return [("a",), ("b",), ("c",), ("d",)]
        if q.startswith("UPDATE"):
            tried.append(q.split("slug = '")[1].split("'")[0])
            if n["conflicts"] < 150:  # more lost commits than the 60 attempts: every other runner's claim won
                n["conflicts"] += 1
                raise RuntimeError("ConcurrentAppendException: files were added")
            return [(1,)]
        return [("r1",)]  # the read-back

    import random

    q = sr.DeltaQueue(sql, "t", sleep=lambda s: None, rng=random.Random(7).random)
    assert q.claim("r1") in {"a", "b", "c", "d"} and q.conflicts == 150
    assert len(set(tried)) > 1, "a retry takes another free row, not the first one every runner fights for"


def test_a_claim_gives_up_only_when_conflicts_outlast_the_claim_budget():
    now = [0.0]

    def sql(q):
        if q.startswith("SELECT slug"):
            return [("a",)]
        raise RuntimeError("DELTA_CONCURRENT_WRITE")

    q = sr.DeltaQueue(sql, "t", sleep=lambda s: now.__setitem__(0, now[0] + s), clock=lambda: now[0], budget_s=120.0)
    with pytest.raises(RuntimeError, match="no claim"):
        q.claim("r1")
    assert now[0] >= 120.0


def test_the_gate_is_released_whenever_the_runner_leaves_its_loop(tmp_path):
    gate_path = str(tmp_path / "heavy.lock")
    q = sr.DirQueue(str(tmp_path / "q"), ["h", "l"], heavy={"h"})
    calls = {"n": 0}

    def note(rec):  # the record's copy to the Volume fails while the heavy chunk is current
        calls["n"] += 1
        if rec.get("current") == "h":
            raise OSError("Volume write failed")

    rec = sr.run_loop(q, "r0", lambda slug: {"rc": 0}, heavy={"h"}, gate=sr.HeavyGate(gate_path), on_change=note)
    assert [x["slug"] for x in rec["shards"]] == ["h", "l"], "a record that cannot be saved stops nothing"
    assert sr.HeavyGate(gate_path).try_acquire(), "and the gate is free again"

    class Boom:
        def claim(self, who, offset=0, heavy_ok=True):
            raise KeyboardInterrupt

    gate = sr.HeavyGate(gate_path + "2")
    with pytest.raises(KeyboardInterrupt):
        sr.run_loop(Boom(), "r0", lambda slug: {}, heavy={"h"}, gate=gate)
    assert sr.HeavyGate(gate_path + "2").try_acquire(), "an interrupt leaves no lock behind"


def test_a_gate_that_cannot_make_its_lock_lets_the_runner_go_on_ungated(tmp_path):
    (tmp_path / "file").write_text("x")
    gate = sr.HeavyGate(str(tmp_path / "file" / "heavy.lock"))  # a file where its folder should be: makedirs fails
    assert gate.try_acquire() and "Error" in gate.error, "it cannot gate: it says why and does not block the runner"
    q = sr.DirQueue(str(tmp_path / "q"), ["h", "l"], heavy={"h"})
    rec = sr.run_loop(q, "r0", lambda slug: {"rc": 0}, heavy={"h"}, gate=sr.HeavyGate(str(tmp_path / "file" / "x.lock")))
    assert [x["slug"] for x in rec["shards"]] == ["h", "l"] and rec["gate_error"] and not rec["error"]


def test_a_runner_that_loses_the_light_rows_waits_for_the_heavy_ones(tmp_path):
    q = sr.DirQueue(str(tmp_path / "q"), ["h", "l"], heavy={"h"})
    real_open = os.open

    def racing_open(path, flags, mode=0o777):  # another runner claims "l" between the scan and this runner's create
        if path.endswith("l.claim") and not os.path.exists(path):
            fd = real_open(path, flags, mode)
            os.close(fd)
        return real_open(path, flags, mode)

    sr.os.open = racing_open
    try:
        assert q.claim("r1", heavy_ok=False) is sr.WAIT, "a heavy chunk is still queued: wait, do not quit"
    finally:
        sr.os.open = real_open


def test_a_result_that_cannot_be_written_does_not_fail_the_runner(tmp_path):
    q = sr.DirQueue(str(tmp_path / "q"), ["a"])

    def on_result(slug, res, start, end):
        raise OSError("Volume write failed")

    rec = sr.run_loop(q, "r0", lambda slug: {"rc": 0}, on_result=on_result)
    assert not rec["error"] and rec["result_errors"] and "a" in rec["result_errors"][0]


def test_the_delta_queue_leaves_heavy_chunks_to_a_runner_that_holds_the_gate():
    fake = _FakeDelta()
    fake.sql(sr.DeltaQueue.create_sql("t", ["h", "l"], {"h": 9.0, "l": 1.0}))
    q = sr.DeltaQueue(fake.sql, "t", sleep=lambda s: None, heavy={"h"})
    assert q.claim("r0", heavy_ok=False) == "l" and q.claim("r0", heavy_ok=False) is sr.WAIT
    assert q.claim("r1", heavy_ok=True) == "h" and q.claim("r1", heavy_ok=False) is None


class _Exit(Exception):
    pass


def test_the_runner_notebook_seeds_the_queue_then_runs_every_shard_once(shards, tmp_path):
    a = _args("--max-parallel", "2")
    plan = launcher.make_plan(a, "a" * 40, ts.select(shards, "pyrx,ds"), None, None, str(tmp_path), REPO_ROOT)
    stage = Path(plan["stage"])
    stage.mkdir(parents=True)
    (stage / "plan.json").write_text(json.dumps(plan))
    nb = launcher.build_runner_notebook(plan, "gbx-test-1n")
    assert "Monitoring:" in "".join(nb["cells"][0]["source"]) and len(nb["cells"]) == 2, "a header and the runner; no %pip"
    src = "".join(nb["cells"][1]["source"])
    assert "__STAGE__" not in src and "def run_loop" in src
    fake = _FakeDelta()
    ran, t_calls = [], []

    class Spark:
        def sql(self, q):
            rows = fake.sql(q)
            return type("DF", (), {"collect": lambda _self: rows})()

    def dbutils(widgets):
        def run(path, timeout, args):
            ran.append((widgets["runner"], args["shard"]))
            t_calls.append(args["t_call"])
            return json.dumps({"rc": 0, "pid": 100 + len(ran)})

        def exit_(v):
            raise _Exit(v)

        nbk = type("N", (), {"run": staticmethod(run), "exit": staticmethod(exit_)})
        return type("D", (), {"widgets": type("W", (), {"get": staticmethod(widgets.__getitem__)}), "notebook": nbk})

    def run_cell(widgets):
        with pytest.raises(_Exit) as got:
            exec(compile(src, "runner", "exec"), {"spark": Spark(), "dbutils": dbutils(widgets)})  # noqa: S102
        return json.loads(str(got.value))

    assert run_cell({"role": "seed", "pool": "gbx-test-1n"}) == {"queued": len(plan["shards"])}
    out = [run_cell({"role": "run", "pool": "gbx-test-1n", "runner": f"r{i}", "index": str(i), "shard_nb": "/w/tests.ipynb"}) for i in range(2)]
    assert sorted(s for _, s in ran) == sorted(plan["shards"]) and sum(o["shards"] for o in out) == len(plan["shards"])
    rec = json.loads((stage / "runners" / "gbx-test-1n" / "r0.json").read_text())
    assert rec["done"] and rec["pool"] == "gbx-test-1n" and rec["shards"], "the record reached the Volume folder"
    assert len(t_calls) == len(ran) and all(float(t) > 0 for t in t_calls), "each call passes its time to the chunk"


# ---- setup phases: where a chunk's minutes go before its first test


def test_the_runner_passes_the_time_of_its_call_to_the_chunk():
    src = launcher.runner_source({"stage": "/x"})
    assert '"t_call": "%.3f" % t_call' in src, "the chunk measures its own start against the runner's call"


def test_the_chunk_records_its_setup_phases_in_order(shards):
    plan = _plan(_args("--serverless"), ts.select(shards, "ds"), wheel=Path("/x/geobrix-0.6.0+env6-py3-none-any.whl"))
    nb = launcher.build_notebook(plan, "serverless")
    start = "".join(nb["cells"][1]["source"])
    ns = {}

    class W:
        @staticmethod
        def get(k):
            return {"shard": "s", "t_call": "100.5"}[k]

    d = type("D", (), {"widgets": W, "notebook": type("N", (), {"entry_point": None})})
    snippet = start[: start.index("_out = ")] + start[start.index('print("phase=nb_start'):]
    exec(compile(snippet, "start", "exec"), {"dbutils": d, **ns}, ns)  # noqa: S102
    assert ns["_GBX_PHASES"]["call"] == 100.5 and ns["_GBX_PHASES"]["nb_start"] > 100.5 and "start_cell_end" in ns["_GBX_PHASES"]
    run = "".join(nb["cells"][-1]["source"])
    marks = ["scratch", "checkout", "overlay", "import_gbx", "guard_tree", "import_pytest", "note_child", "probe", "pytest_start", "pytest_end"]
    at = [run.index('phase("%s")' % m) for m in marks]
    assert at == sorted(at), "the marks follow the chunk's own order"
    for hook in ("collected", "first_test_start", "first_test_end"):
        assert 'phase("%s")' % hook in run
    assert "phases=PHASES_OUT()" in run, "the result carries the phases"
    assert run.index('print("phases so far:"') > run.index("sys.stderr = Tee("), "a killed chunk's log holds its early phases"
    write = run.index('json.dump(result, open(root + "/out/result.json", "w"))')
    assert run.index('os.makedirs(root + "/out", exist_ok=True)', run.index("result = dict(shard=") - 600) < write, "a lost scratch is named, not a crash"


def test_a_serverless_chunk_checks_its_workers_while_it_unpacks(shards):
    """On Serverless the workers run the wheel, not the checkout, so their check needs no overlay: it starts before the
    unpack, in a thread, and the guard after the overlay takes what it found. On a pool node it stays after the overlay."""
    run = launcher.run_source({"stage": "/x"})
    early, unpack, guard = run.index("_WORKER_CHECK.start()"), run.index("CHECKOUT, repo, CHECKOUT_WHY = node_checkout("), run.index('run_guard({"tree": src_pkg}')
    assert early < unpack < guard
    assert "EARLY_WORKERS = bool(PLAN.get(\"serverless\"))" in run[:unpack] and "workers=_early_workers" in run[guard:]
    ns = {"time": __import__("time"), "threading": threading, "PLAN": {"serverless": True}, "spark": object()}
    seen = []

    def collect_workers(spark, tasks=8, exclude=()):
        seen.append(exclude)
        return [{"host": "w1"}]

    ns["collect_workers"] = collect_workers
    head = run[run.index("_EARLY = {}"): run.index("EARLY_WORKERS = bool(")] + "_WORKER_CHECK.start()\n"
    tail = run[run.index("def _early_workers"): run.index("if EARLY_WORKERS:\n    WHEEL_GUARD_TREE")]
    exec(compile(head + tail, "early", "exec"), ns)  # noqa: S102
    assert ns["_early_workers"]() == [{"host": "w1"}] and seen == [("_build_info.py",)]
    ns2 = dict(ns, collect_workers=lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no workers")))
    exec(compile(head + tail, "early", "exec"), ns2)  # noqa: S102
    with pytest.raises(RuntimeError, match="no workers"):
        ns2["_early_workers"]()


def test_the_summary_breaks_each_chunk_s_setup_into_phases():
    rows = [{"name": "a", "phases": {"call": 0.0, "nb_start": 30.0, "start_cell_end": 31.0, "run_cell": 45.0, "checkout": 57.0,
                                     "first_test_start": 80.0, "pytest_end": 200.0}},
            {"name": "b", "phases": None}]
    text = "\n".join(launcher.phase_lines(rows))
    assert "| a |" in text and "| b |" not in text
    head = [ln for ln in text.splitlines() if ln.startswith("| chunk")][0]
    assert head.index("nb_start") < head.index("run_cell") < head.index("checkout") < head.index("first_test_start")
    row = [ln for ln in text.splitlines() if ln.startswith("| a |")][0]
    assert "| 30 | 1 | 14 | 12 | 23 | 120 | 80 |" in row, "each phase is the time since the one before; the last column is the time to the first test"
    assert launcher.phase_lines([{"name": "b", "phases": None}]) == []


# ---- contention: what a unit takes beside other chunks, against what it takes alone
def _shard_rec(slug, start, end, **kw):
    return {"slug": slug, "start": start, "end": end, "rc": 0, "reason": "", **kw}


def test_neighbours_are_the_chunks_that_overlapped_on_the_same_cluster():
    records = {"p": [
        {"runner": "r0", "shards": [_shard_rec("a", 0, 100)]},  # cluster 0
        {"runner": "r1", "shards": [_shard_rec("b", 0, 100)]},  # cluster 1: alone
        {"runner": "r2", "shards": [_shard_rec("c", 10, 50), _shard_rec("d", 60, 120)]},  # cluster 0
        {"runner": "r4", "shards": [_shard_rec("e", 110, 300)]},  # cluster 0: 10 s beside d (a hand-off), 0 beside a
    ]}
    per, peaks = tm.neighbours(records, {"p": 2})
    assert per["a"]["nb"] == 2 and per["a"]["with"] == ["c", "d"] and per["a"]["peak"] == 2
    assert per["b"]["nb"] == 0 and per["b"]["peak"] == 1 and per["b"]["cluster"] == 1
    assert per["e"]["nb"] == 0, "an overlap under OVERLAP_MIN_S is a hand-off, not a neighbour"
    assert peaks == {"p": {0: 2, 1: 1}}


def test_the_contention_factor_is_the_median_ratio_beside_neighbours_and_marks_heavy_units(tmp_path):
    path = tmp_path / "h.json"
    for i, x in enumerate((5.0, 4.0, 6.0)):
        tm.merge(path, [], {"run": f"r{i}"}, [{"key": "test/zarr/test_x.py", "x": x, "nb": 3, "peak": 4},
                                              {"key": "test/ds/test_y.py", "x": 1.2, "nb": 2, "peak": 4},
                                              {"key": "test/ds/test_z.py", "x": 3.0, "nb": 0, "peak": 1}])
    tm.merge(path, [], {"run": "r0"}, [{"key": "test/zarr/test_x.py", "x": 99.0, "nb": 3}])  # the same run again adds nothing
    h = tm.load(path)
    assert tm.contention(h) == {"test/zarr/test_x.py": 5.0, "test/ds/test_y.py": 1.2}, "a run alone (nb 0) is no contention"
    assert tm.contended(h) == {"test/zarr/test_x.py"}


def test_a_sample_taken_beside_neighbours_is_deflated_so_the_factor_counts_once(tmp_path):
    path = tmp_path / "h.json"
    k = "test/zarr/test_x.py"
    tm.merge(path, [{"key": k, "s": 600.0, "slots": 1}], {"run": "r1"}, [{"key": k, "x": 5.0, "nb": 3}])
    e = tm.estimate_of(tm.load(path)["keys"][k], 1)
    assert e["solo"] == 120.0 and e["x"] == 5.0 and e["s"] == 600.0, "solo 600/5; planned at solo x factor"
    assert tm.seconds(tm.load(path), 1, adjust=False) == {k: 120.0}


def test_the_solo_estimate_follows_runs_alone_not_the_plan_a_contended_run_was_judged_by(tmp_path):
    path = tmp_path / "h.json"
    k = "test/zarr/test_x.py"
    tm.merge(path, [{"key": k, "s": 100.0, "slots": 1}], {"run": "r1"})
    tm.merge(path, [{"key": k, "s": 500.0, "slots": 1}], {"run": "r2"}, [{"key": k, "x": 5.0, "nb": 3}])  # judged against 100
    tm.merge(path, [{"key": k, "s": 300.0, "slots": 1}], {"run": "r3"})  # the unit itself got slower
    assert tm.estimate_of(tm.load(path)["keys"][k], 1)["solo"] == 200.0, "the runs alone move it (deflating r2 by its own x pins it at 100)"


def test_one_run_keeps_one_sample_per_key_the_median_of_its_chunks(tmp_path):
    path = tmp_path / "h.json"
    smp = [{"key": tm.REST, "s": s, "slots": 1} for s in (2.0, 90.0, 3.0)] + [{"key": "a.py", "s": 50.0, "slots": 1, "lb": True},
                                                                               {"key": "a.py", "s": 10.0, "slots": 1}]
    tm.merge(path, smp, {"run": "r1"}, [{"key": tm.REST, "x": x, "nb": 2, "peak": p} for x, p in ((1.0, 2), (60.0, 4), (2.0, 3))])
    keys = tm.load(path)["keys"]
    assert [x["s"] for x in keys[tm.REST]["samples"]] == [3.0], "not the first chunk's: the median of every family chunk's rest"
    assert keys["a.py"]["samples"] == [{"s": 10.0, "run": "r1", "slots": 1, "lb": False}], "a full sample wins over a lower bound"
    assert keys[tm.REST]["contention"] == [{"run": "r1", "x": 2.0, "nb": 2, "peak": 4}]


def test_a_runners_first_chunk_is_flagged_cold_and_warm_samples_plan_the_key(tmp_path):
    files = _ds_files(2)
    picked = [ts.Shard(name=f"ds [{i}/2]", kind="light", paths=[f], est_min=3.0, solo_min=3.0, est_src="history") for i, f in enumerate(files, 1)]
    plan = _plan(_args("--serverless"), picked)
    out = tmp_path / "out"
    _junit(out / "ds-1-2" / "junit.xml", [(_mod(files[0]), "a", 200)])
    _junit(out / "ds-2-2" / "junit.xml", [(_mod(files[1]), "b", 30)])
    records = {"serverless": [{"runner": "r0", "shards": [_shard_rec("ds-1-2", 0, 260), _shard_rec("ds-2-2", 260, 300)]}]}
    rows = [{"name": c.name, "slug": c.slug, "pool": "serverless", "verdict": "ok", "expected_min": 3.0, "passed": 1, "failed": 0, "errors": 0,
             "skipped": 0, "wall_s": 100, "reason": "", "claims": 1, "est_src": "history", "actual_s": 100, "failures": [], "skip_reasons": {}}
            for c in picked]
    launcher.record_timings(tmp_path / "h.json", plan, rows, out, records)
    keys = tm.load(tmp_path / "h.json")["keys"]
    assert keys[tm.SLS + files[0]]["samples"][0]["first"] is True and "first" not in keys[tm.SLS + files[1]]["samples"][0]
    k = tm.SLS + files[0]
    for run, s in (("w1", 60.0), ("w2", 70.0)):
        tm.merge(tmp_path / "h.json", [{"key": k, "s": s, "slots": 1}], {"run": run})
    assert tm.estimate_of(tm.load(tmp_path / "h.json")["keys"][k], 1)["s"] == 65.0, "the cold 200 s does not swing the plan"


def test_contention_records_compare_each_unit_with_its_solo_expected_time():
    chunk = {"unit_solo_s": {"a.py": 100.0, "b.py": 100.0, "c.py": 10.0}, "solo_min": 4.0}
    got = tm.contention_records(chunk, 900.0, {"a.py": 500.0, "b.py": 110.0, "c.py": 50.0}, nb=3, peak=4)
    assert {r["key"]: round(r["x"], 2) for r in got} == {"a.py": 5.0, "b.py": 1.1}, "c.py ran under CONTENTION_MIN_S: too short to judge"
    assert tm.contention_records(dict(chunk, est_src="lower bound"), 900.0, {"a.py": 500.0}, 3, 4) == []
    plain = {"solo_min": 2.0}  # an older plan: the chunk's ratio goes to the units that took most of it
    assert {r["key"] for r in tm.contention_records(plain, 600.0, {"a.py": 400.0, "b.py": 30.0}, 2, 3)} == {"a.py"}
    assert tm.contention_records(plain, 600.0, {"a.py": 400.0}, 2, 3, incomplete=True)[0]["x"] == 5.0, "a capped chunk: x is a lower bound"
    assert tm.contention_records({"solo_min": 9.0}, 600.0, {"a.py": 400.0}, 2, 3, incomplete=True) == [], "below the heavy mark it says nothing"


def test_contention_heavy_units_join_the_heavy_gate_and_the_plan_carries_their_factor(shards):
    ds = next(s for s in shards if s.name == "ds")
    files = ts.files_of(ds, REPO_ROOT)
    secs = {f: 60.0 for f in files}
    secs[files[0]] = 300.0  # 5 min: not long enough to be heavy by its time alone
    out = ts.auto_split([ds], REPO_ROOT, secs, 8.0, contended={files[0]})
    hot = [c for c in out if c.heavy]
    assert len(hot) == 1 and hot[0].paths == [files[0]], "the contended file is heavy and packed apart from the light ones"
    picked = [ts.Shard(**{**c.__dict__, "solo_min": round(c.est_min / (5.0 if c.heavy else 1.0), 2)}) for c in out]
    plan = _plan(_args(), picked)
    v = plan["shards"][hot[0].slug]
    assert v["heavy"] and v["contention"] == 5.0 and plan["pools"]["gbx-test-1n"]["heavy"] == [hot[0].slug]
    text = launcher.explain(plan, [], [])
    assert ", heavy, x5)" in text and "contention-heavy unit" in text


def test_a_run_records_contention_and_the_summary_prints_it_with_the_peak(tmp_path, shards):
    files = _ds_files(2)
    picked = [ts.Shard(name="ds [1/2]", kind="light", paths=[files[0]], est_min=3.0, solo_min=3.0, est_src="history"),
              ts.Shard(name="ds [2/2]", kind="light", paths=[files[1]], est_min=3.0, solo_min=3.0, est_src="history")]
    plan = _plan(_args(), picked)
    out = tmp_path / "out"
    _junit(out / "ds-1-2" / "junit.xml", [(_mod(files[0]), "a", 500)])
    _junit(out / "ds-2-2" / "junit.xml", [(_mod(files[1]), "b", 150)])
    for slug in ("ds-1-2", "ds-2-2"):
        plan["shards"][slug]["unit_solo_s"] = {plan["shards"][slug]["files"][0]: 100.0}
    records = {"gbx-test-1n": [{"runner": "r0", "shards": [_shard_rec("ds-1-2", 0, 540)]},
                               {"runner": "r1", "shards": [_shard_rec("ds-2-2", 0, 180)]}]}
    rows = [{"name": n, "slug": s, "pool": "gbx-test-1n", "verdict": "ok", "expected_min": 3.0, "passed": 1, "failed": 0, "errors": 0,
             "skipped": 0, "wall_s": 100, "reason": "", "claims": 1, "est_src": "history", "actual_s": a, "failures": [],
             "skip_reasons": {}} for n, s, a in (("ds [1/2]", "ds-1-2", 540), ("ds [2/2]", "ds-2-2", 180))]
    got = launcher.record_timings(tmp_path / "h.json", plan, rows, out, records)
    assert got["contention"] == 2 and got["peaks"] == {"gbx-test-1n": {0: 2}}
    assert rows[0]["x"] == 3.0 and rows[0]["nb"] == 1 and rows[1]["x"] == 1.0
    h = tm.load(tmp_path / "h.json")
    assert tm.contention(h) == {files[0]: 5.0, files[1]: 1.5} and tm.contended(h) == {files[0]}
    md = launcher.summary_md(plan, rows, {"gbx-test-1n": {"cluster_id": "c1", "peaks": got["peaks"]["gbx-test-1n"]}})
    assert "| contention | beside |" in md and "| 3x (heavy) | 1 |" in md and "peak concurrency: cluster 0: 2 chunk(s) at once" in md
    assert "3x" in launcher.table_text(rows)


def test_the_contention_backfill_reads_runner_records_and_queue_expectations(tmp_path):
    files = _ds_files(2)
    run = tmp_path / "runs" / "tst-x"
    _junit(run / "zz-1" / "junit.xml", [(_mod(files[0]), "a", 1200)])
    _junit(run / "zz-2" / "junit.xml", [(_mod(files[1]), "b", 200)])
    for slug in ("zz-1", "zz-2"):
        (run / slug / "result.json").write_text(json.dumps({"rc": 0, "reason": ""}))
    (run / "_runners" / "p").mkdir(parents=True)
    (run / "_runners" / "p" / "r0.json").write_text(json.dumps({"runner": "r0", "shards": [_shard_rec("zz-1", 0, 1260)]}))
    (run / "_runners" / "p" / "r1.json").write_text(json.dumps({"runner": "r1", "shards": [_shard_rec("zz-2", 0, 240)]}))
    path = tmp_path / "h.json"
    got = tm.backfill_contention(path, tmp_path / "runs", REPO_ROOT, expected={"tst-x": {"zz-1": 4.0, "zz-2": 4.0}})
    assert got == {"runs": 1, "records": 2}
    assert tm.backfill_contention(path, tmp_path / "runs", REPO_ROOT, expected={"tst-x": {"zz-1": 4.0, "zz-2": 4.0}})["records"] == 0
    assert tm.contended(tm.load(path)) == {files[0]}, "1260 s against 4 min beside a neighbour: 5.25x"


def test_a_split_chunk_estimated_by_its_share_of_ci_minutes_gives_no_contention_record():
    split = {"solo_min": 3.0, "est_src": "ci", "name": "zarr 2/4 [1/5]"}
    assert tm.contention_records(split, 900.0, {"a.py": 800.0}, 3, 4) == [], "a share of CI's minutes is a guess, not a solo time"
    whole = dict(split, name="zarr 4/4")
    assert tm.contention_records(whole, 900.0, {"a.py": 800.0}, 3, 4)[0]["x"] == 5.0, "CI's minutes for a whole shard are a solo time"


# ---- fail fast: results as they come, the stop rules, the smoke chunk, the ramp, the caps, and Serverless chunks


def _line(slug, end, *, failed=0, errors=0, passed=5, rc=None, reason="", smoke=False, overrun=False):
    rc = (1 if failed or errors else 0) if rc is None else rc
    res = {"rc": rc, "reason": reason, "passed": passed, "failed": failed, "errors": errors, "skipped": 0, "total": passed + failed,
           "first_failure": {"id": f"test/x.py::t_{slug}", "msg": "AssertionError: boom"} if failed or errors else {}, "smoke": smoke,
           "overrun": overrun, "child": {"job_id": "7", "run_id": f"9{len(slug)}"}}
    return sr.result_line(slug, res, end - 60, end, "r0")


def test_the_run_stops_when_the_first_two_chunks_to_finish_both_failed():
    first = [_line("a", 30, failed=1), _line("b", 10, failed=2), _line("c", 20, errors=1)]
    assert sr.stop_rule(first[:1]) == "", "one chunk: not yet"
    assert sr.stop_rule(first[1:]) == "the first 2 chunks to finish all failed"
    mixed = [_line("a", 30, failed=1), _line("b", 10), _line("c", 20, failed=1)]
    assert sr.stop_rule(mixed) == "", "one of the first two to END passed: the K rule never fires, whatever the order of the files"
    assert sr.stop_rule(first[1:], {"first_k": 0}) == "", "0 turns the rule off"
    assert sr.stop_rule(first, {"off": True}) == "", "--no-fail-fast"


def test_the_run_stops_at_three_failed_chunks_or_twenty_failed_tests():
    lines = [_line("ok1", 1), _line("ok2", 2)] + [_line(f"f{i}", 10 + i, failed=1) for i in range(3)]
    assert sr.stop_rule(lines[:4]) == "", "2 failed chunks, under the default 3"
    assert sr.stop_rule(lines) == "3 chunks failed (--max-failed-chunks 3)"
    many = [_line("ok", 1), _line("big", 2, failed=12), _line("big2", 3, errors=9)]
    assert sr.stop_rule(many) == "21 tests failed or errored (--max-failures 20)"
    assert sr.stop_rule(many, {"max_failures": 0}) == ""
    # what counts as a failed chunk: a failure, an error, a stop without a result, a selection error; not a part that collected
    # nothing, and not a chunk stopped at 2x its estimate with no test failed (a timing miss, recorded as a lower bound)
    assert sr.chunk_failed({"rc": 5, "failed": 0}) is False and sr.chunk_failed({"rc": 0, "passed": 3}) is False
    assert sr.chunk_failed({"rc": 2, "errors": 1}) and sr.chunk_failed({"rc": None, "reason": "killed by watchdog: shard cap 45 min"})
    assert sr.chunk_failed({"rc": 5, "reason": "selection error: none of the 3 fastest chunks collects a test"})
    assert sr.chunk_failed({"rc": 130, "reason": "killed by watchdog: ran past 6 min", "overrun": True}) is False
    assert sr.chunk_failed({"rc": 130, "reason": "killed by watchdog: ran past 6 min", "overrun": True, "failed": 1})


def test_a_failed_smoke_chunk_stops_the_run_whatever_the_policy():
    assert sr.stop_rule([_line("s", 5, errors=1, smoke=True)], {"off": True}) == "the smoke chunk s failed"
    assert sr.stop_rule([_line("s", 5, smoke=True)]) == "", "a smoke chunk that passed lets the queue go"


def test_runners_claim_nothing_more_once_the_rule_fires_and_the_queue_keeps_the_rest(tmp_path):
    results = tmp_path / "results"
    results.mkdir()
    order = [f"c{i}" for i in range(12)]

    def execute(slug):
        return {"rc": 1, "failed": 1, "passed": 2, "total": 3, "first_failure": {"id": f"test/x.py::{slug}", "msg": "ImportError: no osgeo"}}

    def on_result(slug, res, start, end):
        (results / f"{slug}.json").write_text(json.dumps(sr.result_line(slug, res, start, end)))

    q = sr.DirQueue(str(tmp_path / "q"), order)
    rec = sr.run_loop(q, "r0", execute, stop=lambda: sr.stop_rule(sr.read_results(str(results))), on_result=on_result)
    assert [x["slug"] for x in rec["shards"]] == ["c0", "c1"], "the 3rd claim never happens"
    assert rec["stopped"] == "the first 2 chunks to finish all failed" and rec["done"]
    assert q.claim("r1") == "c2", "the rest is still queued: nothing claimed it"
    line = json.loads((results / "c0.json").read_text())
    assert line["failed_chunk"] and line["first_failure"] == {"id": "test/x.py::c0", "msg": "ImportError: no osgeo"}


def test_the_ramp_holds_the_later_runners_until_two_chunks_passed(tmp_path):
    results = []
    assert not sr.ramp_open(results, 2, 5)
    results += [_line("a", 1, failed=1), _line("b", 2)]
    assert not sr.ramp_open(results, 2, 5), "a failed chunk is not a clean result"
    results.append(_line("c", 3))
    assert sr.ramp_open(results, 2, 5) and sr.ramp_open([], 2, 0), "two passed, or nothing left to wait for"
    waits, holds = [], iter([True, True, False])
    q = sr.DirQueue(str(tmp_path / "q"), ["x", "y"])
    rec = sr.run_loop(q, "r3", lambda slug: {"rc": 0}, hold=lambda: next(holds), hold_s=7.0, sleep=waits.append)
    assert waits == [7.0, 7.0] and [x["slug"] for x in rec["shards"]] == ["x", "y"]
    stopped = sr.run_loop(sr.DirQueue(str(tmp_path / "q2"), ["x"]), "r4", lambda slug: {"rc": 0}, hold=lambda: True,
                          stop=lambda: "the smoke chunk s failed", sleep=waits.append)
    assert stopped["stopped"] == "the smoke chunk s failed" and not stopped["shards"], "a held runner that sees a stop never claims"


def test_the_plan_starts_two_runners_and_waits_for_them_in_its_wall(shards):
    plan = _plan(_args("--max-parallel", "4", "--clusters", "2", "--no-smoke"), ts.select(shards, "pyrx,ds,zarr,scripts/ci"))
    spec = plan["pools"]["gbx-test-1n"]
    assert spec["start"] == 2 and plan["fail_fast"]["start_chunks"] == 2
    wide = _plan(_args("--max-parallel", "4", "--clusters", "2", "--no-smoke", "--start-chunks", "0"), ts.select(shards, "pyrx,ds,zarr,scripts/ci"))
    assert spec["expected_min"] >= wide["pools"]["gbx-test-1n"]["expected_min"], "holding runners back never shortens the wall"
    assert launcher.ramp_wall({"a": 4, "b": 4, "c": 4, "d": 4}, ["a", "b", "c", "d"], 4, 2, 2) == 8.0, "two run, then all four"
    assert launcher.ramp_wall({"a": 4, "b": 4, "c": 4, "d": 4}, ["a", "b", "c", "d"], 4, 0, 2) == 4.0


def _picked_plan_ff(shards, *extra):
    return _plan(_args(*extra), ts.select(shards, "pyrx,ds,zarr,scripts/ci"))


def test_the_smoke_collects_every_file_then_runs_the_first_tests_of_the_fastest_chunks(shards):
    plan = _picked_plan_ff(shards, "--clusters", "2", "--max-parallel", "2", "--env", "PYTEST_ADDOPTS=-k align_to")
    spec = plan["pools"]["gbx-test-1n"]
    smoke = plan["shards"][spec["smoke"]]
    assert spec["smoke"] == "smoke-light" and spec["queue"][0] == "smoke-light" and smoke["kind"] == "smoke"
    others = [s for s in plan["shards"] if s != "smoke-light"]
    assert sorted(spec["queue"][1:]) == sorted(others) and spec["queue"][1:] == sr.queue_order({s: plan["shards"][s]["expected_min"] for s in others})
    fastest = sorted((plan["shards"][s]["expected_min"], s) for s in others if not plan["shards"][s]["heavy"])
    assert smoke["candidates"] == [s for _, s in fastest[:3]], "the run's own fastest chunks, not fixed rows"
    files = {f for s in others for f in plan["shards"][s]["files"] if not f.startswith("scripts/")}
    assert set(smoke["collect"][2:]) == files and smoke["collect"][:2] == ["-m", "not integration"]
    assert smoke["env"] == {"PYTEST_ADDOPTS": "-k align_to"} and smoke["args"] == [], "the run's --env; no tests of its own"
    tasks = launcher.job_tasks(plan, "gbx-test-1n", {"runner": "/w/runner.ipynb", "shard": "/w/tests.ipynb"}, ["c1", "c2"])
    by = {t["task_key"]: t for t in tasks}
    assert by["smoke"]["depends_on"] == ["q_seed"] and by["smoke"]["base_parameters"]["role"] == "smoke"
    assert all(by[f"r_{i}"]["depends_on"] == ["smoke"] for i in range(spec["runners"])), "the runners wait for the smoke to pass"
    plain = _picked_plan_ff(shards, "--no-smoke")
    assert plain["pools"]["gbx-test-1n"]["smoke"] == "" and "smoke-light" not in plain["shards"]
    assert "smoke (gbx-test-1n, ~" in launcher.explain(plan, [], []) and "fail fast: stop the run at the first 2" in launcher.explain(plan, [], [])
    src = launcher.run_source(plan)
    assert "def smoke_pick():" in src and "selection error: none of the" in src and "fresh_tests()" in src
    assert launcher.smoke_chunk([ts.Shard(name="scripts/ci", kind="scripts", paths=["scripts/ci"])], "light", "a" * 40, REPO_ROOT, _args(), "p") is None


def test_the_plan_carries_the_policy_and_each_chunk_gets_its_caps(shards):
    plan = _plan(_args(), ts.select(shards, "ds"))
    ff = plan["fail_fast"]
    assert {k: ff[k] for k in ("first_k", "max_failed_chunks", "max_failures", "maxfail", "start_chunks", "test_timeout_s", "overrun_x", "max_wall_min")} == {
        "first_k": 2, "max_failed_chunks": 3, "max_failures": 20, "maxfail": 3, "start_chunks": 2, "test_timeout_s": 120.0, "overrun_x": 2.0,
        "max_wall_min": 20.0}
    src = launcher.run_source(plan)
    for needle in ('["--maxfail=" + str(MAXFAIL)] if MAXFAIL', "def pytest_collectreport", "maxfail_hit", "PyThreadState_SetAsyncExc",
                   "OVERRUN_S and time.time() - t0 > OVERRUN_S", "class TestPart:", 'raise RuntimeError("chunk %s: rc %s', "note_child()"):
        assert needle in src, needle
    off = launcher.fail_fast(_args("--no-fail-fast"))
    assert off["off"] and off["maxfail"] == 0 and off["smoke"], "--no-fail-fast runs every chunk out; the smoke stays"
    assert launcher.fail_fast(_args("--rerun-failed", "tst-x"))["maxfail"] == 1, "a re-run stops a chunk at its first failure"
    assert launcher.fail_fast(_args("--no-fail-fast", "--maxfail", "7"))["maxfail"] == 7
    assert launcher.problems(_args("--max-failed-chunks", "-1")) and launcher.problems(_args("--rerun-failed", "../x"))
    res = _sh("--help")
    for opt in ("--first-failed", "--max-failed-chunks", "--max-failures", "--maxfail", "--no-fail-fast", "--no-smoke", "--start-chunks",
                "--test-timeout", "--overrun-x", "--max-wall-min", "--rerun-failed"):
        assert opt in res.stdout, opt


def test_a_chunk_that_fails_fails_its_own_run_and_the_runner_still_gets_its_result(tmp_path):
    out = tmp_path / "result.json"
    out.write_text(json.dumps({"rc": 1, "failed": 9, "passed": 46, "child": {"run_id": "980"}}))

    def child_failed():
        raise RuntimeError("com.databricks.WorkflowException: NotebookExecutionException: FAILED: chunk zarr: rc 1, 9 failed")

    got = sr.run_shard(child_failed, 3300, result=lambda: sr.read_json_file(str(out)))
    assert got["failed"] == 9 and got["child"] == {"run_id": "980"} and "9 failed" in got["child_failed"]
    assert "ended without a result" in sr.run_shard(child_failed, 3300, result=lambda: sr.read_json_file(str(tmp_path / "absent.json")))["reason"]
    rec = sr.run_loop(sr.DirQueue(str(tmp_path / "q"), ["a"]), "r0", lambda slug: got)
    assert rec["shards"][0]["child"] == {"run_id": "980"}, "the runner's record names the chunk's run"


class _VolW:
    """A workspace whose Volume is a local folder: the Files calls Live and stop_early make, the cancels, and the runs' outputs."""

    def __init__(self, root):
        self.root, self.cancelled, self.outputs = Path(root), [], {}
        w = self

        class Files:
            def list_directory_contents(self, path):
                d = w._p(path)
                if not d.is_dir():
                    raise FileNotFoundError(path)
                return [type("E", (), {"path": f"{path}/{p.name}", "name": p.name, "is_directory": p.is_dir()})() for p in sorted(d.iterdir())]

            def download(self, path):
                data = w._p(path).read_bytes()
                return type("R", (), {"contents": type("C", (), {"read": lambda _s: data})()})()

            def upload(self, path, stream, overwrite=True):
                w._p(path).parent.mkdir(parents=True, exist_ok=True)
                w._p(path).write_bytes(stream.read())

        class Jobs:
            def cancel_run(self, rid):
                w.cancelled.append(rid)

            def get_run_output(self, rid):
                return type("O", (), {"error": w.outputs.get(rid, ""), "error_trace": ""})()

        self.files, self.jobs = Files(), Jobs()
        self.config = type("Cfg", (), {"host": "https://ws.example.com"})()

    def _p(self, path):
        return self.root / path.lstrip("/")


def test_live_results_print_a_row_per_chunk_with_its_run_and_fetch_its_files(shards, tmp_path):
    plan = _plan(_args(), ts.select(shards, "pyrx,ds"))
    w = _VolW(tmp_path / "vol")
    stage = w._p(plan["stage"])
    printed = []
    live = launcher.Live(w, plan, tmp_path / "local", t0=0.0, out=printed.append)
    assert live.poll(5.0) == [] and printed == [], "no results folder yet"
    slugs = [s for s in plan["shards"] if not s.startswith("smoke")]
    (stage / "results").mkdir(parents=True)
    (stage / "out" / slugs[0]).mkdir(parents=True)
    (stage / "out" / slugs[0] / "junit.xml").write_text(JUNIT)
    (stage / "results" / f"{slugs[0]}.json").write_text(json.dumps(_line(slugs[0], 100, failed=1)))
    (stage / "results" / f"{slugs[1]}.json.tmp").write_text("{")  # not a result file
    got = live.poll(125.0)
    assert [x["slug"] for x in got] == [slugs[0]]
    assert printed[0].startswith("  live results") and "+  2m05s  FAILED" in printed[1] and f"first: test/x.py::t_{slugs[0]} AssertionError: boom" in printed[1]
    assert "https://ws.example.com/#job/7/run/9" in printed[1], "the row names the chunk's own run"
    assert (tmp_path / "local" / slugs[0] / "junit.xml").exists() and slugs[0] in live.fetched, "its files came down at once"
    assert live.poll(130.0) == [], "a result is read once"
    (stage / "results" / f"{slugs[1]}.json").write_text(json.dumps(_line(slugs[1], 140)))
    assert [x["slug"] for x in live.poll(150.0)] == [slugs[1]] and " ok " in printed[-1]
    assert live.check() == "" and live.counts() == (2, 1)
    assert live.status() == f"2 chunk(s) done, 1 failed; first failure: test/x.py::t_{slugs[0]}"


def test_a_chunk_that_died_before_pytest_says_why_from_its_own_run(shards, tmp_path):
    plan = _plan(_args(), ts.select(shards, "pyrx,ds"))
    w = _VolW(tmp_path / "vol")
    slug = next(s for s in plan["shards"] if not s.startswith("smoke"))
    res = w._p(plan["stage"]) / "results"
    res.mkdir(parents=True)
    line = sr.result_line(slug, {"rc": None, "reason": "the shard's notebook run ended without a result: Py4JJavaError: Workload failed",
                                 "child": {"job_id": "1", "run_id": "42"}}, 0, 40)
    (res / f"{slug}.json").write_text(json.dumps(line))
    w.outputs[42] = "SyntaxError: unterminated string literal (detected at line 153)"
    printed = []
    launcher.Live(w, plan, None, t0=0.0, out=printed.append).poll(50.0)
    assert "SyntaxError: unterminated string literal" in printed[-1], "the run's own error, not the platform's 'Workload failed'"


def test_a_rule_that_fires_stops_the_whole_run_and_says_why(shards, tmp_path, capsys):
    plan = _plan(_args(), ts.select(shards, "pyrx,ds,zarr"))
    w = _VolW(tmp_path / "vol")
    res = w._p(plan["stage"]) / "results"
    res.mkdir(parents=True)
    for i, slug in enumerate([s for s in plan["shards"] if not s.startswith("smoke")][:2]):
        (res / f"{slug}.json").write_text(json.dumps(_line(slug, 10 + i, errors=1)))
    live = launcher.Live(w, plan, None, t0=0.0, out=lambda s: None)
    live.poll(60.0)
    rule = live.check()
    assert rule == "the first 2 chunks to finish all failed"
    launcher.stop_early(w, {"light": 11, "jar": 22}, plan, live, rule)
    err = capsys.readouterr().err
    assert "stopped early: the first 2 chunks to finish all failed" in err and err.count("  FAIL [") == 2
    assert sorted(w.cancelled) == [11, 22], "every pool's job is cancelled: the queue and the running chunks"
    assert json.loads(w._p(plan["stage"] + "/stop.json").read_text())["rule"] == rule, "the runners' stop marker"
    assert plan["stopped_early"] == rule
    md = launcher.summary_md(plan, [], {})
    assert "**stopped early: the first 2 chunks to finish all failed**" in md and "- fail fast: stop the run at" in md


def test_the_wall_budget_stops_the_run_and_lists_what_did_not_run(shards, tmp_path):
    plan = _plan(_args("--max-wall-min", "20"), ts.select(shards, "pyrx,ds"))
    assert launcher.wall_rule(plan, 19 * 60) == "" and launcher.wall_rule(plan, 20 * 60 + 1) == "the wall budget of 20 min is spent (--max-wall-min)"
    assert launcher.wall_rule(_plan(_args("--max-wall-min", "0"), ts.select(shards, "ds")), 10 ** 6) == "", "0: no budget"
    plan["stopped_early"] = launcher.wall_rule(plan, 1300)
    rows = [{"name": "a", "verdict": "INCOMPLETE", "reason": f"not run: stopped early ({plan['stopped_early']})"}, {"name": "b", "verdict": "ok", "reason": ""}]
    assert launcher.not_run_line(rows) == "not run (1 chunk(s)): a"
    big = _plan(_args("--max-wall-min", "1"), ts.select(shards, "pyrx,ds"))
    assert "passes --max-wall-min 1" in launcher.explain(big, [], []), "the plan warns when its expected wall passes the budget"


def test_a_failed_smoke_stops_the_job_before_the_runners_and_a_stop_marker_stops_a_runner(shards, tmp_path):
    a = _args("--max-parallel", "2")
    plan = launcher.make_plan(a, "a" * 40, ts.select(shards, "pyrx,ds"), None, None, str(tmp_path), REPO_ROOT)
    stage = Path(plan["stage"])
    stage.mkdir(parents=True)
    (stage / "plan.json").write_text(json.dumps(plan))
    src = "".join(launcher.build_runner_notebook(plan, "gbx-test-1n")["cells"][1]["source"])
    fake = _FakeDelta()
    ran = []

    class Spark:
        def sql(self, q):
            if q.startswith("SELECT count(*) FROM") and "owner IS NULL" in q:
                return type("DF", (), {"collect": lambda _self: [(sum(1 for r in fake.rows.values() if r["owner"] is None),)]})()
            rows = fake.sql(q)
            return type("DF", (), {"collect": lambda _self: rows})()

    def dbutils(widgets, result):
        def run(path, timeout, args):
            ran.append((widgets["runner"], args["shard"]))
            return json.dumps(result)

        def exit_(v):
            raise _Exit(v)

        nbk = type("N", (), {"run": staticmethod(run), "exit": staticmethod(exit_)})
        return type("D", (), {"widgets": type("W", (), {"get": staticmethod(widgets.__getitem__)}), "notebook": nbk})

    def run_cell(widgets, result=None):
        exec(compile(src, "runner", "exec"), {"spark": Spark(), "dbutils": dbutils(widgets, result or {"rc": 0, "passed": 1})})  # noqa: S102

    with pytest.raises(_Exit):
        run_cell({"role": "seed", "pool": "gbx-test-1n"})
    smoke = plan["pools"]["gbx-test-1n"]["smoke"]
    bad = {"rc": 2, "errors": 1, "total": 0, "first_failure": {"id": "test/pyrx/test_env.py", "msg": "ImportError: libgdal.so"}}
    with pytest.raises(RuntimeError, match=r"stopped early: the smoke chunk .* failed .*ImportError: libgdal.so"):
        run_cell({"role": "smoke", "pool": "gbx-test-1n", "runner": "smoke", "index": "0", "shard_nb": "/w/t"}, bad)
    assert ran == [("smoke", smoke)], "the smoke task ran the smoke chunk, and only it"
    line = json.loads((stage / "results" / f"{smoke}.json").read_text())
    assert line["smoke"] and line["failed_chunk"] and sr.stop_rule([line]) == f"the smoke chunk {smoke} failed"
    (stage / "stop.json").write_text(json.dumps({"rule": "the smoke chunk failed"}))
    with pytest.raises(_Exit) as got:
        run_cell({"role": "run", "pool": "gbx-test-1n", "runner": "r0", "index": "0", "shard_nb": "/w/t"})
    assert json.loads(str(got.value)) == {"runner": "r0", "shards": 0, "stopped": "stopped early: the smoke chunk failed"} and len(ran) == 1
    (stage / "stop.json").unlink()
    for f in (stage / "results").iterdir():
        f.unlink()
    # a runner whose chunk failed fails its own task, so the job's status tells the truth
    with pytest.raises(RuntimeError, match=r"2 of 2 chunk\(s\) this runner ran failed"):
        run_cell({"role": "run", "pool": "gbx-test-1n", "runner": "r1", "index": "1", "shard_nb": "/w/t"}, {"rc": 1, "failed": 2, "passed": 3})


def test_serverless_cuts_small_chunks_from_its_own_history_without_the_pools_contention(shards, tmp_path, capsys):
    ds = next(s for s in shards if s.name == "ds")
    files = ts.files_of(ds, REPO_ROOT)
    hist = tmp_path / "h.json"
    tm.merge(hist, [{"key": f, "s": 120.0, "slots": 1} for f in files], {"run": "pool1"}, [{"key": f, "x": 5.0, "nb": 3} for f in files])
    tm.merge(hist, [{"key": tm.SLS + files[0], "s": 900.0, "slots": 1}], {"run": "sls1"})
    h = tm.load(hist)
    sv, pv = tm.view(h, serverless=True), tm.view(h)
    assert tm.seconds(sv, 1, adjust=False)[files[0]] == 900.0 and tm.seconds(sv, 1, adjust=False)[files[1]] == 24.0, "its own, else the pool's solo time"
    assert files[0] in tm.contended(pv) and not any(k.startswith(tm.SLS) for k in pv["keys"]), "a pool plan never reads Serverless keys"
    assert launcher.main(["--explain-only", "--serverless", "--shards", "ds", "--timings", str(hist)]) == 0
    out = capsys.readouterr().out
    assert "x 8 slots" in out and ", heavy" not in out and "smoke (serverless," in out, "8 runners by default, no heavy gate"
    m = __import__("re").search(r"cut to about ([\d.]+) min", out)
    assert m and launcher.SERVERLESS_TARGET[0] <= float(m.group(1)) <= launcher.SERVERLESS_TARGET[1], out
    plan = _plan(_args("--serverless"), ts.select(shards, "ds"))
    assert set(plan["pools"]) == {"serverless"} and plan["pools"]["serverless"]["slots"] == launcher.SERVERLESS_SLOTS
    assert launcher.job_tasks(plan, "serverless", {"runner": "/r", "shard": "/s"}, [])[1]["environment_key"] == "gbx_env6"


def test_a_unit_over_the_target_is_cut_into_test_parts_and_its_samples_count_whole(tmp_path):
    files = _ds_files(2)
    shard = ts.Shard(name="files", kind="light", paths=files)
    out = ts.auto_split([shard], REPO_ROOT, {files[0]: 600.0, files[1]: 30.0}, 4.0)
    parts = [c for c in out if c.paths == [files[0]]]
    assert sorted(c.test_part for c in parts) == ["0/4", "1/4", "2/4", "3/4"] and all(c.est_min <= 4.0 for c in out), [(c.name, c.est_min) for c in out]
    assert "TestPart(S[\"test_part\"])" in launcher.run_source({"stage": "/x"}), "the shard keeps the tests of its part only"
    plan = _plan(_args(), out)
    slug = parts[0].slug
    junit = tmp_path / "out" / slug / "junit.xml"
    _junit(junit, [(_mod(files[0]), "a", 200)])
    rows = [{"name": parts[0].name, "slug": slug, "pool": "gbx-test-1n", "verdict": "ok", "expected_min": parts[0].est_min, "passed": 1, "failed": 0,
             "errors": 0, "skipped": 0, "wall_s": 230, "reason": "", "claims": 1, "est_src": "history", "actual_s": 250, "failures": [], "skip_reasons": {}}]
    launcher.record_timings(tmp_path / "h.json", plan, rows, tmp_path / "out", {})
    keys = tm.load(tmp_path / "h.json")["keys"]
    assert files[0] not in keys, "one part never sets its whole unit's history"
    assert keys[f"{files[0]}#{parts[0].test_part}"]["samples"][0]["s"] == round(200.0 / ts.slowdown(4), 2), "the part times its own key"
    # every part ran in one run: the unit gets their sum, and the next cut times each part by its own key
    rows = []
    for c, secs in zip(sorted(parts, key=lambda c: c.test_part), (100, 150, 200, 250)):
        _junit(tmp_path / "out2" / c.slug / "junit.xml", [(_mod(files[0]), "a", secs)])
        rows.append({"name": c.name, "slug": c.slug, "pool": "gbx-test-1n", "verdict": "ok", "expected_min": c.est_min, "passed": 1, "failed": 0,
                     "errors": 0, "skipped": 0, "wall_s": secs, "reason": "", "claims": 1, "est_src": "history", "actual_s": secs + 30,
                     "failures": [], "skip_reasons": {}})
    launcher.record_timings(tmp_path / "h2.json", dict(plan, run_id="t2"), rows, tmp_path / "out2", {})
    h = tm.load(tmp_path / "h2.json")
    assert h["keys"][files[0]]["samples"][0]["s"] == round(700.0 / ts.slowdown(4), 2), "all four parts: the unit is their sum"
    fsec = tm.seconds(h, 4)
    again = {c.test_part: c for c in ts.cut(shard, 4.0, REPO_ROOT, dict(fsec, **{files[0]: 600.0}), set()) if c.paths == [files[0]]}
    assert again["3/4"].est_min == round(ts.FIXED_MIN + fsec[f"{files[0]}#3/4"] / 60.0, 2) > again["0/4"].est_min, "a part's own time plans it"


def test_a_serverless_run_records_its_own_keys_and_a_stopped_run_flags_its_lower_bounds(tmp_path):
    files = _ds_files(2)
    picked = [ts.Shard(name="ds [1/2]", kind="light", paths=[files[0]], est_min=3.0, solo_min=3.0, est_src="history"),
              ts.Shard(name="ds [2/2]", kind="light", paths=[files[1]], est_min=3.0, solo_min=3.0, est_src="history")]
    plan = _plan(_args("--serverless"), picked)
    plan["stopped_early"] = "the first 2 chunks to finish all failed"
    out = tmp_path / "out"
    _junit(out / "ds-1-2" / "junit.xml", [(_mod(files[0]), "a", 200)])
    _junit(out / "ds-2-2" / "junit.xml", [(_mod(files[1]), "b", 30)])
    records = {"serverless": [{"runner": "r0", "shards": [_shard_rec("ds-1-2", 0, 260)]}, {"runner": "r1", "shards": [_shard_rec("ds-2-2", 0, 100)]}]}
    rows = [{"name": n, "slug": s, "pool": "serverless", "verdict": v, "expected_min": 3.0, "passed": 1, "failed": 1, "errors": 0,
             "skipped": 0, "wall_s": 100, "reason": "", "claims": 1, "est_src": "history", "actual_s": act, "failures": [],
             "skip_reasons": {}, "maxfail_hit": hit} for n, s, v, act, hit in (("ds [1/2]", "ds-1-2", "FAILED", 260, False),
                                                                             ("ds [2/2]", "ds-2-2", "FAILED", 100, True))]
    got = launcher.record_timings(tmp_path / "h.json", plan, rows, out, records)
    h = tm.load(tmp_path / "h.json")
    assert got["contention"] == 0 and rows[0]["nb"] == 0, "Serverless runners have no neighbours"
    assert set(h["keys"]) == {tm.SLS + files[0], tm.SLS + files[1]}, "Serverless samples under their own keys"
    assert h["keys"][tm.SLS + files[0]]["samples"][0] == {"s": 200.0, "run": "t1", "slots": 1, "lb": False, "first": True}, \
        "a failed chunk that ran out is a full sample (its runner's first: a cold start)"
    lb = h["keys"][tm.SLS + files[1]]["samples"][0]
    assert lb["lb"] and lb["stop"] == "the first 2 chunks to finish all failed", "--maxfail cut the chunk short: a flagged lower bound"
    assert h["runs"][-1]["stopped_early"] == "the first 2 chunks to finish all failed" and h["runs"][-1]["serverless"]


def test_collect_says_which_chunks_the_stop_left_unrun(shards, monkeypatch, tmp_path):
    plan = _picked_plan(shards, "--no-smoke")
    plan["stopped_early"] = "3 chunks failed (--max-failed-chunks 3)"
    queue = plan["pools"]["gbx-test-1n"]["queue"]
    asked = []
    monkeypatch.setattr(launcher.mut, "download_dir", lambda w, remote, local: asked.append(remote.rsplit("/", 1)[-1]) or [])
    recs = {"gbx-test-1n": [{"runner": "r0", "shards": [{"slug": queue[0], "start": 0, "end": 9}]}]}
    recs["gbx-test-1n"].append({"runner": "r1", "current": queue[2], "done": False, "shards": []})  # cancelled while it ran
    rows = {r["slug"]: r for r in launcher.collect(None, plan, {}, tmp_path, recs)}
    assert sorted(asked) == sorted([queue[0], queue[2]]), "a chunk the stop left unrun has no files to fetch"
    assert rows[queue[2]]["reason"].endswith("while it ran") and rows[queue[2]]["claims"] == 1, "a chunk that ran at the stop is not 'not run'"
    assert launcher.fail_fast(_args("--serverless"))["overrun_floor_min"] == 5.0 and launcher.fail_fast(_args())["overrun_floor_min"] == 3.0
    assert rows[queue[0]]["reason"] == "stopped early (3 chunks failed (--max-failed-chunks 3)) while it ran"
    assert rows[queue[1]]["reason"] == "not run: stopped early (3 chunks failed (--max-failed-chunks 3))" and rows[queue[1]]["verdict"] == "INCOMPLETE"


def test_rerun_failed_runs_only_the_test_ids_that_failed_in_that_run(tmp_path, monkeypatch):
    files = _ds_files(2)
    run = tmp_path / "test-logs" / "databricks-tests" / "tst-old"
    _junit(run / "ds-1-2" / "junit.xml", [(_mod(files[0]), "test_a", 1)])
    (run / "ds-1-2" / "junit.xml").write_text((run / "ds-1-2" / "junit.xml").read_text().replace(
        '<testcase classname="' + _mod(files[0]) + '" name="test_a" time="1"/>',
        '<testcase classname="' + _mod(files[0]) + '" name="test_a[x-1]" time="1"><failure message="boom"/></testcase>'
        '<testcase classname="' + _mod(files[0]) + '.TestK" name="test_b" time="1"><error message="bad"/></testcase>'
        '<testcase classname="' + _mod(files[0]) + '" name="test_ok" time="1"/>'))
    _junit(run / "ds-2-2" / "junit.xml", [(_mod(files[1]), "test_c", 1)])
    got = launcher.rerun_shards(run, REPO_ROOT)
    assert [s.name for s in got] == [f"rerun {files[0]}"] and got[0].paths == [f"{files[0]}::test_a[x-1]", f"{files[0]}::TestK::test_b"]
    assert launcher.rerun_shards(tmp_path / "absent", REPO_ROOT) == []


def _failed_junit(path, cases):
    """A junit file whose cases (classname, name) all failed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(f'<testcase classname="{c}" name="{n}" time="1"><failure message="boom"/></testcase>' for c, n in cases)
    path.write_text(f"<testsuites><testsuite tests='{len(cases)}'>{body}</testsuite></testsuites>")


def test_a_heavy_rerun_runs_on_the_jar_pool_with_its_integration_tests_and_the_family_env(shards, tmp_path):
    run = tmp_path / "tst-old"
    vx = "test.vectorx.test_tin_conforming_mode"
    _failed_junit(run / "heavy-vectorx" / "junit.xml", [(vx, "test_conforming_mode_answers_on_the_jvm[gbx_st_triangulate]")])
    _failed_junit(run / "manager-family-2-3-1-2" / "junit.xml", [("test.manager.test_family_cross_tier", "test_rows[RASTER_ACCESSOR-x]")])
    (run / "plan.json").write_text(json.dumps({"shards": {"manager-family-2-3-1-2": {"env": {"GBX_ORACLE_TABLES": "RASTER_ACCESSOR,SPEC_DEM"}}}}))
    got = {s.name: s for s in launcher.rerun_shards(run, REPO_ROOT, shards)}
    heavy = got["rerun test/vectorx/test_tin_conforming_mode.py"]
    assert heavy.kind == "heavy" and heavy.pool == ts.POOL_JAR and "-m" not in ts.pytest_args(heavy), "not deselected, and with the JAR"
    fam = got["rerun test/manager/test_family_cross_tier.py"]
    assert fam.oracle_tables == "RASTER_ACCESSOR,SPEC_DEM" and ts.shard_env(fam)["GBX_ORACLE_TABLES"] == "RASTER_ACCESSOR,SPEC_DEM"
    assert not heavy.subset and not fam.subset, "a rerun that collects nothing is a selection error, not a pass"


def test_rerun_failed_also_reruns_a_test_the_timeout_stopped(shards, tmp_path):
    """A test past --test-timeout that does not stop is killed with its chunk: the junit then holds no failure for it (2026-11-12,
    tst-sls-b4: gbx_rst_color_relief-L3engine-c0-sql), but the chunk's result names it in timed_out."""
    run = tmp_path / "tst-old"
    nid = "test/manager/test_family_cross_tier.py::test_family_oracle[gbx_rst_color_relief-L3engine-c0-sql]"
    (run / "smoke-serverless").mkdir(parents=True)
    (run / "smoke-serverless" / "junit.xml").write_text("<testsuites><testsuite tests='1'><testcase classname='test.manager."
                                                         "test_family_cross_tier' name='test_family_oracle[gbx_rst_aspect-L3engine-c0-sql]' time='1'/></testsuite></testsuites>")
    (run / "smoke-serverless" / "result.json").write_text(json.dumps({"timed_out": [nid]}))
    (run / "plan.json").write_text(json.dumps({"shards": {"smoke-serverless": {"env": {"GBX_ORACLE_TABLES": "SPEC_DEM"}}}}))
    got = launcher.rerun_shards(run, REPO_ROOT, shards)
    assert [s.paths for s in got] == [[nid]] and got[0].oracle_tables == "SPEC_DEM"


def test_an_overrun_chunk_is_stopped_and_reported_as_overrun_not_as_a_failed_test():
    res = {"rc": 130, "reason": "killed by watchdog: ran past 6 min, 2x its estimate", "overrun": True, "failed": 0, "errors": 0, "passed": 4}
    assert not sr.chunk_failed(res), "its own run and its runner task do not fail"
    assert launcher.shard_verdict(res, None) == "OVERRUN"
    assert launcher.verdict_of(sr.result_line("a", res, 0, 1)) == "OVERRUN"
    assert launcher.shard_verdict(dict(res, failed=1), None) != "OVERRUN", "a test that failed first is a failure"
    src = launcher.run_source({"stage": "/x"})
    assert "if chunk_failed(result):" in src and "def chunk_failed(" in src, "the shard raises by the same rule"
    assert 'OVERRUN_S = 0 if S.get("smoke")' in src, "the smoke has no estimate to overrun"
    row = {"name": "a [1/2]", "verdict": "OVERRUN", "reason": res["reason"]}
    assert launcher.is_failure(row), "the run is not green: the tests after the stop did not run"
    rec = sr.run_loop(sr.DirQueue("/tmp/gbx-overrun-q-" + str(os.getpid()), ["a"]), "r0", lambda slug: res)
    assert rec["shards"][0]["overrun"] and not [x for x in rec["shards"] if sr.chunk_failed(x)], "the runner's bad list skips it"


def test_empty_test_parts_say_empty_and_fail_their_unit_only_when_no_part_collected_a_test(shards):
    files = _ds_files(1)
    plan = {"shards": {f"p{i}": {"test_part": f"{i}/3", "files": files, "tables": []} for i in range(3)}}
    rows = [{"slug": f"p{i}", "name": f"x [{i + 1}/3]", "verdict": v, "reason": ""} for i, v in enumerate(("EMPTY", "ok", "ok"))]
    launcher.mark_empty_units(plan, rows)
    assert rows[0]["verdict"] == "EMPTY" and not launcher.is_failure(rows[0]), "one empty part of a unit whose others ran"
    rows = [dict(r, verdict="EMPTY") for r in rows]
    launcher.mark_empty_units(plan, rows)
    assert all(launcher.is_failure(r) for r in rows) and "no part" in rows[0]["reason"], "a unit that collected nothing fails"
    assert launcher.is_failure({"name": "rerun test/x.py", "verdict": "EMPTY", "subset": False}), "a rerun that collects nothing fails"


def test_an_unintended_empty_chunk_fails_its_own_run_its_runner_and_the_job(shards):
    empty = {"rc": 5, "total": 0, "passed": 0, "reason": ""}
    assert not sr.chunk_failed(empty), "a part that may be empty (the default) is no failure"
    assert sr.chunk_failed(dict(empty, empty_ok=False)), "a rerun or a whole shard that collects nothing fails"
    line = sr.result_line("a", dict(empty, empty_ok=False), 0, 1)
    assert line["failed_chunk"] and launcher.verdict_of(line) == "EMPTY", "a failed chunk, shown as EMPTY"
    rerun = ts.Shard(name="rerun test/x.py", kind="light", paths=["test/x.py::t"])
    part = ts.Shard(name="ds [1/2]", kind="light", paths=_ds_files(1))
    plan = _plan(_args(), [rerun, part])
    assert plan["shards"][rerun.slug]["empty_ok"] is False and plan["shards"][part.slug]["empty_ok"] is True
    assert '"empty_ok": S.get("empty_ok", True)' in launcher.run_source(plan) or "empty_ok=S.get(" in launcher.run_source(plan)
    rec = sr.run_loop(sr.DirQueue("/tmp/gbx-empty-q-" + str(os.getpid()), ["a"]), "r0", lambda slug: dict(empty, empty_ok=False))
    assert [x for x in rec["shards"] if sr.chunk_failed(x)], "the runner's bad list (and so the job) sees it"


def test_relane_rescales_the_solo_expectations_to_the_slots_it_leased(shards):
    a = _args("--max-parallel", "0")
    picked = [ts.Shard(name="ds", kind="light", paths=["test/ds"], est_min=10.0, solo_min=8.0, est_src="history")]
    plan = launcher.make_plan(a, "a" * 40, picked, None, None, "/Volumes/c/s/v", REPO_ROOT, fsolo={})
    pool = next(iter(plan["pools"]))
    plan["shards"]["ds"]["unit_solo_s"] = {"test/ds/test_x.py": 100.0}
    before = plan["pools"][pool]["slots"]
    launcher.relane(plan, pool, a, 1, 64)  # a 64 vCPU node: 8 slots, not the plan's 4
    k = ts.slowdown(8) / ts.slowdown(before)
    c = plan["shards"]["ds"]
    assert plan["pools"][pool]["slots"] == 8 and k > 1
    assert c["solo_min"] == pytest.approx(8.0 * k, rel=0.02) and c["unit_solo_s"]["test/ds/test_x.py"] == pytest.approx(100.0 * k, rel=0.02)
    assert c["expected_min"] == pytest.approx(10.0 * k, rel=0.02), "the estimate at the slots the node runs"
    launcher.relane(plan, pool, a, 1, 64)
    assert c["solo_min"] == pytest.approx(8.0 * k, rel=0.02), "a second relane at the same slots changes nothing"


def test_a_chunk_removes_its_scratch_and_a_runner_its_checkout(shards):
    src = launcher.run_source(_plan(_args(), ts.select(shards, "ds")))
    assert "OWN_BASE = True" in src and "shutil.rmtree(base if OWN_BASE else root" in src, "a chunk's own temp folder goes, not just its files"
    assert src.index("shutil.rmtree(base if OWN_BASE else root") > src.index('shutil.copy(root + "/out/" + f, OUT + "/" + f)')
    runner = launcher.runner_source(_plan(_args(), ts.select(shards, "ds")))
    assert '"/_checkout-" + WHO' in runner, "the runner removes its checkout when its loop ends"
    assert "STALE_SCRATCH_S" in src and "os.path.getmtime(_p) > STALE_SCRATCH_S" in src, "and a chunk sweeps old launches' scratch off the node"


def test_a_spark_conf_the_session_refuses_fails_the_chunk_with_the_reason(shards):
    src = launcher.run_source(_plan(_args("--spark-conf", "spark.x=1"), ts.select(shards, "ds")))
    start, end = src.index("CONF_ERR = "), src.index('print("shard", S["name"]')
    snippet = textwrap.dedent(src[start:end])

    class Conf:
        def set(self, k, v):
            raise RuntimeError("[CONFIG_NOT_AVAILABLE] Configuration spark.x is not available.")

    ns = {"S": {"spark_conf": {"spark.x": "1"}}, "spark": type("Sp", (), {"conf": Conf()})()}
    exec(compile(snippet, "conf", "exec"), ns)  # noqa: S102
    assert "spark.x=1" in ns["CONF_ERR"] and "CONFIG_NOT_AVAILABLE" in ns["CONF_ERR"]
    assert "if CONF_ERR:" in src and src.index("if CONF_ERR:") < src.index("rc = int(pytest.main(args, plugins=plugins))")


def test_the_wall_budget_starts_after_the_lease_wait_when_the_pool_is_submitted(shards, monkeypatch, tmp_path):
    plan = _plan(_args("--max-wall-min", "10"), ts.select(shards, "ds,pyrx"))
    pool = next(iter(plan["pools"]))
    now = [1000.0]
    seen = []
    monkeypatch.setattr(launcher.time, "time", lambda: now[0])

    def lease_many(*a, **k):
        now[0] += 25 * 60  # a 25 min wait for a free cluster
        return [type("L", (), {"cluster_id": "c1", "job_run": lambda self, r: None})()]

    monkeypatch.setattr(launcher, "lease_many", lease_many)
    monkeypatch.setattr(launcher, "relane", lambda *a, **k: None)
    monkeypatch.setattr(launcher, "put_text", lambda w, path, text: seen.append(json.loads(text)) if path.endswith("plan.json") else None)
    monkeypatch.setattr(launcher, "submit_pool_job", lambda *a, **k: 77)
    w = type("W", (), {"workspace": type("WS", (), {"import_": lambda *a, **k: None})(),
                       "clusters": type("C", (), {"get": lambda self, cid: type("CI", (), {"node_type_id": "m5d.8xlarge"})()})(),
                       "jobs": type("J", (), {"get_run": lambda self, rid: type("R", (), {"run_page_url": ""})()})()})()
    runs, info, nbs, t_submit = {}, {}, [], {}
    launcher.start_pool(w, _args("--max-wall-min", "10"), plan, pool, contextlib.ExitStack(), volroot="/Volumes/c/s/v", me="me",
                        base_dir="/x", nbs=nbs, runs=runs, pool_info=info, t_submit=t_submit)
    assert runs == {pool: 77}
    assert plan["pools"][pool]["deadline"] == 1000.0 + 25 * 60 + 10 * 60, "the budget counts from the submit, not from before the lease"
    assert seen[-1]["pools"][pool]["deadline"] == plan["pools"][pool]["deadline"], "the runners read it from the plan"
    src = launcher.runner_source(plan)
    assert 'SPEC.get("deadline")' in src


def test_affected_runs_the_family_rows_of_the_changed_functions_only_with_k(shards):
    sel = af.Selection()
    sel.add_table("RASTER_ACCESSOR", "row gbx_rst_width: changed")
    sel.functions.setdefault("RASTER_ACCESSOR", set()).update({"gbx_rst_width", "gbx_rst_height"})
    sel.add_table("SPEC_GDAL", "oracle_tables.spec_gdal defines it")
    sel.whole.add("SPEC_GDAL")
    out, lost = af.to_shards(sel, shards, REPO_ROOT)
    rows = [s for s in out if s.select_k]
    assert len(rows) == 1 and rows[0].oracle_tables == "RASTER_ACCESSOR" and rows[0].select_k == "gbx_rst_height or gbx_rst_width"
    assert 0 < rows[0].share < 1, "the estimate is the rows' share of the table"
    whole = [s for s in out if "SPEC_GDAL" in s.oracle_tables]
    assert whole and not whole[0].select_k, "a table whose own module changed runs whole"
    assert ts.pytest_args(rows[0])[-2:] == ["-k", "gbx_rst_height or gbx_rst_width"]
    plan = _plan(_args(), out)
    assert "-k gbx_rst_height or gbx_rst_width" in launcher.explain(plan, [], [])
