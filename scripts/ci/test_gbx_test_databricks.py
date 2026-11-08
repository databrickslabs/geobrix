"""Tests for gbx:test:databricks: the shards read from CI's files, the selection (`affected` included), the lanes, the plan,
the runner notebook and the result files. No network and no workspace."""

import importlib.util
import json
import subprocess
import sys
import textwrap
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
    every_light = [s.name for s in shards if s.kind == "light"]
    assert _affected(shards, "python/geobrix/src/databricks/labs/gbx/core/crs.py") == every_light
    assert _affected(shards, "python/geobrix/src/databricks/labs/gbx/_api.py") == every_light  # a top-level module
    assert _affected(shards, "python/geobrix/test/conftest.py") == every_light
    assert _affected(shards, "python/geobrix/pyproject.toml") == every_light


def test_affected_follows_scripts_and_ignores_the_rest(shards):
    assert _affected(shards, "scripts/ci/test_light_native.py") == ["scripts/ci"]
    assert _affected(shards, ".github/workflows/build_main.yml") == ["scripts/ci"]
    assert _affected(shards, "docs/docs/intro.mdx", "src/main/scala/X.scala", "README.md") == []


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
    assert "gbx-test-1n" in text and "lane 1" in text and "local only: scala" in text and "expected wall about" in text


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


def test_max_parallel_caps_the_lanes(shards):
    plan = _plan(_args("--max-parallel", "2"), ts.select(shards, "all-light"))
    assert len(plan["pools"]["gbx-test-1n"]["lanes"]) == 2


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


def test_the_summary_lists_the_table_the_failures_and_the_skips(tmp_path):
    f = tmp_path / "junit.xml"
    f.write_text(JUNIT)
    j = launcher.parse_junit(f)
    row = dict(name="ds", slug="ds", pool="gbx-test-1n", verdict="FAILED", expected_min=12, passed=j["passed"], failed=1, errors=1, skipped=2,
               wall_s=100, reason="", failures=j["failures"], skip_reasons=j["skip_reasons"])
    plan = {"run_id": "t1", "sha": "a" * 40, "wheel": {"name": "w.whl", "built_from": "b" * 40}, "purpose": "p", "jar": None}
    md = launcher.summary_md(plan, [row], {"gbx-test-1n": {"cluster_id": "c1", "node_type": "m5d.8xlarge", "job_run": 9, "wall_s": 100}})
    assert "| ds | gbx-test-1n | FAILED | 2 | 1 | 1 | 2 | 100 | 12 |" in md
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


def test_the_plan_sizes_its_lanes_to_clusters_times_slots(shards):
    picked = ts.select(shards, "all-light")
    one = _plan(_args(), picked)["pools"]["gbx-test-1n"]
    assert one["slots"] == 4 and len(one["lanes"]) == 4 and one["clusters"] == 1
    two = _plan(_args("--clusters", "3", "--max-parallel", "4"), picked)["pools"]["gbx-test-1n"]
    assert two["slots"] == 4 and two["clusters"] == 3 and len(two["lanes"]) == 12
    assert one["longest_min"] <= one["expected_min"] <= one["serial_min"]


def test_relane_respreads_the_shards_over_the_clusters_that_were_free(shards):
    a = _args("--clusters", "3")
    plan = _plan(a, ts.select(shards, "all-light"))
    before = sorted(s for lane in plan["pools"]["gbx-test-1n"]["lanes"] for s in lane)
    launcher.relane(plan, "gbx-test-1n", a, 1, 16)  # only one cluster was free, a smaller node
    spec = plan["pools"]["gbx-test-1n"]
    assert spec["clusters"] == 1 and spec["slots"] == 2 and len(spec["lanes"]) == 2
    assert sorted(s for lane in spec["lanes"] for s in lane) == before


def test_the_timings_file_keeps_the_seconds_per_test_file(tmp_path):
    out = tmp_path / "out"
    (out / "ds").mkdir(parents=True)
    f = next((PKG / "test" / "ds").glob("test_*.py"))
    mod = f"test.ds.{f.stem}"
    (out / "ds" / "junit.xml").write_text(
        f'<testsuites><testsuite tests="2"><testcase classname="{mod}" name="a" time="1.5"/>'
        f'<testcase classname="{mod}.TestK" name="b" time="2.0"/></testsuite></testsuites>'
    )
    rows = [dict(slug="ds", name="ds", verdict="ok", wall_s=100)]
    path = tmp_path / "timings.json"
    path.write_text(json.dumps({"files": {"test/other.py": 9.0}, "shards": {}}))
    launcher.write_timings(path, {"run_id": "r", "sha": "a" * 40}, rows, out)
    got = json.loads(path.read_text())
    assert got["files"][f"test/ds/{f.name}"] == 3.5 and got["files"]["test/other.py"] == 9.0
    assert got["shards"]["ds"] == 100
    assert launcher.file_seconds_of(launcher.load_timings(path))[f"test/ds/{f.name}"] == 3.5
    rows[0]["verdict"] = "INCOMPLETE"  # a shard that did not finish adds nothing
    launcher.write_timings(path, {"run_id": "r2", "sha": "b" * 40}, rows, out)
    assert json.loads(path.read_text())["run"] == "r2"


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


class _Task:
    def __init__(self, key, start, end):
        self.task_key, self.start_time, self.end_time = key, start * 1000, end * 1000


class _Jobs:
    def __init__(self, tasks):
        self._t = tasks

    def get_run(self, rid):
        return type("R", (), {"tasks": self._t})()


def test_pool_metrics_say_the_tail_and_the_idle_slot_time():
    spec = {"lanes": [["a", "b"], ["c"]], "slots": 2}
    tasks = [_Task("s_a", 1000, 1100), _Task("s_b", 1100, 1200), _Task("s_c", 1000, 1150)]
    got = launcher.pool_metrics(type("W", (), {"jobs": _Jobs(tasks)})(), 1, spec)
    assert got["chunks"] == 3 and got["task_longest_s"] == 150 and got["task_mean_s"] == 116
    assert got["span_s"] == 200 and got["tail_s"] == 50 and got["ideal_s"] == 175
    assert got["idle_slot_s"] == 2 * 200 - 350
    text = "\n".join(launcher.schedule_lines(got))
    assert "3 chunks on 2 lane(s)" in text and "tail" in text and "ideal" in text
    assert launcher.pool_metrics(type("W", (), {"jobs": _Jobs([])})(), 1, spec) == {}


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
    assert len(three["lanes"]) == 3 * len(one["lanes"]) and three["expected_min"] < one["expected_min"] / 2


def test_seconds_measured_at_eight_slots_are_brought_to_the_slots_of_the_plan():
    assert ts.slowdown(2) == 1.0 and round(ts.slowdown(8), 1) == 2.4
    got = ts.rescale_seconds({"a": 100.0}, 8, 4)
    assert 45 < got["a"] < 55, "half of it: the slowdown of 8 slots over that of 4"
    assert ts.rescale_seconds({"a": 100.0}, 4, 4) == {"a": 100.0}


def test_the_tarball_carries_the_ci_workflow_the_scripts_shard_parses():
    assert ".github" in launcher.SUBSET
    listed = subprocess.run(["git", "ls-tree", "-r", "--name-only", "HEAD", "--", ".github/workflows/build_main.yml"], cwd=REPO_ROOT, capture_output=True, text=True).stdout
    assert ".github/workflows/build_main.yml" in listed, "the file the guards read is in the tree the subset archives"


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
