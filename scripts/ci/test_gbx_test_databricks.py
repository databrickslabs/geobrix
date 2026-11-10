"""Tests for gbx:test:databricks: the shards read from CI's files, the selection (`affected` included), the plan, the queue
and the runners that take shards from it, the notebooks and the result files. No network and no workspace."""

import importlib.util
import json
import multiprocessing
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


def _affected(shards, *files):
    return [s.name for s in ts.affected([s for s in shards if not s.local_only], files, REPO_ROOT)]


def test_affected_picks_the_part_that_holds_a_test_file(shards):
    f = _first_test("pyrx")
    rel = f"test/pyrx/{f}"
    want = f"pyrx {zlib.crc32(rel.encode()) % 2 + 1}/2"
    assert _affected(shards, f"python/geobrix/{rel}") == [want]


def test_affected_follows_a_family_file_to_its_three_shards_and_a_dir_to_its_shard(shards):
    assert _affected(shards, "python/geobrix/test/manager/test_family_cross_tier.py") == ["manager family 1/3", "manager family 2/3", "manager family 3/3"]
    assert _affected(shards, f"python/geobrix/test/ds/{_first_test('ds')}") == ["ds"]
    assert _affected(shards, f"python/geobrix/test/bench/{_first_test('bench')}")[0].startswith("bench")


def test_affected_follows_source_to_the_tests_of_its_package(shards):
    assert _affected(shards, "python/geobrix/src/databricks/labs/gbx/ds/reader.py") == ["ds"]
    assert _affected(shards, "python/geobrix/src/databricks/labs/gbx/rasterx/functions.py") == ["heavy rasterx"]


def test_a_change_to_shared_code_selects_the_heavy_shards_too(shards):
    every = [s.name for s in shards if s.kind in ("light", "heavy")]
    assert any(n.startswith("heavy") for n in every) and any(n.startswith("pyrx") for n in every)
    for f in (
        "python/geobrix/src/databricks/labs/gbx/core/crs.py",  # a shared package
        "python/geobrix/src/databricks/labs/gbx/_api.py",  # a top-level module
        "python/geobrix/test/conftest.py",
        "python/geobrix/test/_provided_spark.py",  # a helper at the top of test/
        "python/geobrix/pyproject.toml",
    ):
        assert _affected(shards, f) == every, f
    ci = _affected(shards, ".github/workflows/build_main.yml")
    assert ci == [s.name for s in shards if s.kind in ("light", "heavy", "scripts")]


def test_a_helper_of_a_test_dir_selects_every_shard_that_runs_part_of_it(shards):
    got = _affected(shards, "python/geobrix/test/pyrx/_helpers.py")
    assert got == ["pyrx 1/2", "pyrx 2/2"], "both halves import it, whichever half its own path hashes to"
    assert _affected(shards, "python/geobrix/test/zarr/conftest.py") == ["zarr 1/4", "zarr 2/4", "zarr 3/4", "zarr 4/4"]


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
    install = "".join(nb["cells"][1]["source"])
    assert install.startswith("%pip install") and "geobrix[stac,vizx,zarr,databricks] @ file://" in install
    compile("".join(nb["cells"][2]["source"]), "runner", "exec")  # no leftover format fields
    assert "pytest.main" in "".join(nb["cells"][2]["source"])
    assert len(launcher.build_notebook(dict(plan, serverless=True), "gbx-test-1n")["cells"]) == 2  # no %pip cell on Serverless


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
    assert all(s.est_min <= 6.0 * 1.6 or len(s.paths) == 1 for s in out), [(s.name, s.est_min) for s in out if s.est_min > 9.6]
    assert not [s for s in out if s.name.startswith("manager family") and not s.name.endswith("]")], "the family oracle groups are cut as well"

    def all_files(items):
        return sorted(f for s in items for f in (ts.files_of(s, REPO_ROOT) or []))

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
    assert est == {"s": 20.0, "n": 3, "lb": False}, "the median of the last runs, with the count"
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
    assert all(len(c.paths) == 1 and c.est_src == "lower bound" for c in alone), [(c.name, c.paths) for c in alone]
    assert sr.queue_order({c.slug: c.expected_min for c in out})[:3] == sorted([c.slug for c in alone], key=lambda x: -next(c.expected_min for c in out if c.slug == x)), "first in the queue"
    tm.merge(path, [{"key": f, "s": 12.0, "slots": 4} for f in files], {"run": "after"})
    assert not tm.lower_bounds(tm.load(path), 4), "a later full run answers the bound"


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
    assert len(heavy) == 1 and heavy[0].oracle_tables == tables[0], "the heavy table has a chunk of its own"
    assert sorted(t for c in out for t in c.oracle_tables.split(",")) == sorted(tables) and all(c.est_src == "history" for c in out)
    assert max(c.est_min for c in out if c is not heavy[0]) <= 5.0 * 1.6


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
        assert slots * 2.5 <= len(chunks) <= slots * 5, (slots, target, len(chunks))
    assert ts.auto_target_min(light, REPO_ROOT, {}, 1000) == 3.5, "never below what a task's fixed cost allows"
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
    assert "git add -A -f" in src and "--allow-empty" not in src, "git ls-files / ls-tree HEAD see the files, as in the commit under test"


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
    plan = _picked_plan(shards, "--clusters", "2", "--max-parallel", "2")
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
    plan = _picked_plan(shards)
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
    ran = []

    class Spark:
        def sql(self, q):
            rows = fake.sql(q)
            return type("DF", (), {"collect": lambda _self: rows})()

    def dbutils(widgets):
        def run(path, timeout, args):
            ran.append((widgets["runner"], args["shard"]))
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
