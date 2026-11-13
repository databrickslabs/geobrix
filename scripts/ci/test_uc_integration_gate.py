"""Host tests for the UC integration gate: uc_sources_hash.py and check_uc_integration_evidence.py.

Each test builds a scratch git repository with copies of the two scripts, so nothing here touches
the real checkout, the workspace or Docker.
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from conftest import REPO_ROOT

CI = REPO_ROOT / "scripts" / "ci"


def git(root, *args):
    subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True, text=True)


POM = "<project>\n  <groupId>g</groupId>\n  <version>{v}</version>\n  <dependencies><dependency><version>9.9</version></dependency></dependencies>\n</project>\n"
VERSION_INIT = "python/geobrix/src/databricks/labs/gbx/__init__.py"
COMMIT = ("-c", "user.email=t@t", "-c", "user.name=t")


def bump(root, version, where="both"):
    """Commit a release version change in pom.xml and/or __init__.py."""
    if where in ("both", "pom"):
        (root / "pom.xml").write_text(POM.format(v=version))
    if where in ("both", "init"):
        (root / VERSION_INIT).write_text(f'__version__ = "{version}"\n')
    git(root, *COMMIT, "commit", "-q", "-am", f"bump {version}")


@pytest.fixture
def repo(tmp_path):
    root = tmp_path / "repo"
    (root / "scripts" / "ci").mkdir(parents=True)
    for name in ("uc_sources_hash.py", "check_uc_integration_evidence.py"):
        shutil.copy(CI / name, root / "scripts" / "ci" / name)
    for rel in ("python/geobrix/src/pkg/mod.py", "examples/uc-functions/00.sql", "src/main/scala/A.scala", "docs/a.md"):
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("v1\n")
    (root / "pom.xml").write_text(POM.format(v="0.6.0"))
    init = root / VERSION_INIT
    init.parent.mkdir(parents=True, exist_ok=True)
    init.write_text('__version__ = "0.6.0"\n')
    git(root, "init", "-q", "-b", "main")
    git(root, "-c", "user.email=t@t", "-c", "user.name=t", "add", "-A")
    git(root, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "init")
    git(root, "update-ref", "refs/remotes/origin/main", "HEAD")  # the base a main-bound range is measured from
    return root


def run(root, script, *args, env=None):
    e = {k: v for k, v in os.environ.items() if k not in ("QC_RANGE", "GBX_UC_EVIDENCE")}
    e.update(env or {})
    return subprocess.run(
        [sys.executable, str(root / "scripts" / "ci" / script), *args], capture_output=True, text=True, env=e, cwd=root
    )


def write_evidence(root, sources_hash, result="pass", slice_failed=0, scope="core"):
    ev = {
        "head_sha": "a" * 40,
        "sources_hash": sources_hash,
        "result": result,
        "scope": scope,
        "schema": "geobrix_uc_it_aaaaaaaaaaaa",
        "finished": "2026-11-05T12:00:00Z",
        "slice": {"passed": 10, "failed": slice_failed, "failed_ids": []},
        "examples": {"statements_ok": 156, "total": 156},
    }
    path = root / "test-logs" / "uc-integration" / "evidence.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ev))
    return path


def check(root, rng="origin/main..HEAD"):
    return run(root, "check_uc_integration_evidence.py", env={"QC_RANGE": rng})


def current_hash(root):
    return run(root, "uc_sources_hash.py").stdout.strip()


# ---- uc_sources_hash ----------------------------------------------------------------------------------------------


def test_hash_is_stable_and_ignores_paths_outside_the_list(repo):
    h = current_hash(repo)
    assert len(h) == 64
    (repo / "docs" / "a.md").write_text("v2\n")
    git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-am", "docs")
    assert current_hash(repo) == h


@pytest.mark.parametrize(
    "rel", ["python/geobrix/src/pkg/mod.py", "examples/uc-functions/00.sql", "src/main/scala/A.scala", "pom.xml"]
)
def test_hash_changes_when_a_uc_source_is_committed(repo, rel):
    h = current_hash(repo)
    (repo / rel).write_text("v2\n")
    git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-am", "change")
    assert current_hash(repo) != h


def test_check_clean_passes_then_fails_on_a_tracked_edit_and_ignores_untracked(repo):
    assert run(repo, "uc_sources_hash.py", "--check-clean").returncode == 0
    (repo / "python/geobrix/src/pkg/new.py").write_text("x\n")  # untracked: not a difference from HEAD
    assert run(repo, "uc_sources_hash.py", "--check-clean").returncode == 0
    (repo / "pom.xml").write_text("edited\n")
    res = run(repo, "uc_sources_hash.py", "--check-clean")
    assert res.returncode == 1 and "pom.xml" in res.stderr
    (repo / "pom.xml").write_text(POM.format(v="0.6.0"))
    assert run(repo, "uc_sources_hash.py", "--check-clean").returncode == 0


def test_check_clean_sees_staged_edits_and_ignores_edits_outside_the_list(repo):
    (repo / "docs" / "a.md").write_text("edited\n")
    assert run(repo, "uc_sources_hash.py", "--check-clean").returncode == 0
    (repo / "src/main/scala/A.scala").write_text("staged\n")
    git(repo, "add", "src/main/scala/A.scala")
    assert run(repo, "uc_sources_hash.py", "--check-clean").returncode == 1


def test_the_hash_covers_the_documented_paths():
    sys.path.insert(0, str(CI))
    import uc_sources_hash

    assert uc_sources_hash.UC_PATHS == ("python/geobrix/src", "examples/uc-functions", "src/main/scala", "pom.xml")


# ---- check_uc_integration_evidence --------------------------------------------------------------------------------

MAIN_RANGES = ["origin/main..HEAD", "main..HEAD", "upstream/main..HEAD"]
OTHER_RANGES = ["origin/streamline/0.6.0..HEAD", "origin/beta/0.6.0..HEAD", "origin/HEAD..HEAD", "abc123..def456", "origin/mainline..HEAD"]


@pytest.mark.parametrize("rng", OTHER_RANGES)
def test_a_range_not_based_on_main_is_skipped_even_without_evidence(repo, rng):
    res = run(repo, "check_uc_integration_evidence.py", env={"QC_RANGE": rng})
    assert res.returncode == 0 and "not a main-bound range" in res.stdout


@pytest.mark.parametrize("rng", MAIN_RANGES)
def test_main_bound_without_evidence_fails_with_a_fix(repo, rng):
    res = run(repo, "check_uc_integration_evidence.py", env={"QC_RANGE": rng})
    assert res.returncode == 1
    assert "no UC integration evidence" in res.stderr and "gbx-uc-tests.sh --profile" in res.stderr and "--scope core" in res.stderr


@pytest.mark.parametrize("rng", MAIN_RANGES)
def test_main_bound_with_fresh_passing_evidence_passes_and_prints_the_summary(repo, rng):
    write_evidence(repo, current_hash(repo))
    res = run(repo, "check_uc_integration_evidence.py", env={"QC_RANGE": rng})
    assert res.returncode == 0, res.stderr
    assert "PASS" in res.stdout and "scope core" in res.stdout and "slice 10 passed / 0 failed" in res.stdout and "examples 156/156" in res.stdout


def test_stale_evidence_fails_after_a_uc_source_commit_but_not_after_a_docs_commit(repo):
    write_evidence(repo, current_hash(repo))
    (repo / "docs" / "a.md").write_text("v2\n")
    git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-am", "docs")
    assert run(repo, "check_uc_integration_evidence.py", env={"QC_RANGE": "origin/main..HEAD"}).returncode == 0
    (repo / "python/geobrix/src/pkg/mod.py").write_text("v2\n")
    git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-am", "src")
    res = run(repo, "check_uc_integration_evidence.py", env={"QC_RANGE": "origin/main..HEAD"})
    assert res.returncode == 1 and "stale" in res.stderr


def test_a_failed_run_fails_the_check(repo):
    write_evidence(repo, current_hash(repo), result="fail", slice_failed=3)
    res = run(repo, "check_uc_integration_evidence.py", env={"QC_RANGE": "origin/main..HEAD"})
    assert res.returncode == 1 and "did not pass" in res.stderr and "FAIL" in res.stderr


def test_an_unreadable_evidence_file_fails(repo):
    path = write_evidence(repo, current_hash(repo))
    path.write_text("{not json")
    assert run(repo, "check_uc_integration_evidence.py", env={"QC_RANGE": "origin/main..HEAD"}).returncode == 1


def test_no_qc_range_checks_like_main(repo):
    assert run(repo, "check_uc_integration_evidence.py").returncode == 1
    write_evidence(repo, current_hash(repo))
    assert run(repo, "check_uc_integration_evidence.py").returncode == 0


def test_push_without_arguments_uses_the_upstream_branch(repo, tmp_path):
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", str(remote)], check=True)
    git(repo, "remote", "add", "origin", str(remote))
    git(repo, "push", "-q", "-u", "origin", "main")
    assert run(repo, "check_uc_integration_evidence.py", env={"QC_RANGE": "@{push}..HEAD"}).returncode == 1
    git(repo, "checkout", "-q", "-b", "streamline/x")
    git(repo, "push", "-q", "-u", "origin", "streamline/x")
    assert run(repo, "check_uc_integration_evidence.py", env={"QC_RANGE": "@{push}..HEAD"}).returncode == 0


def test_evidence_path_can_be_overridden(repo, tmp_path):
    other = tmp_path / "ev.json"
    other.write_text(json.dumps({"result": "pass", "sources_hash": current_hash(repo)}))
    res = run(repo, "check_uc_integration_evidence.py", env={"QC_RANGE": "origin/main..HEAD", "GBX_UC_EVIDENCE": str(other)})
    assert res.returncode == 0


def test_fail_on_revert_a_check_that_ignores_the_hash_would_pass_stale_evidence(repo):
    """Mutation guard: with the hash comparison removed the stale case passes, so the stale test above is not vacuous."""
    write_evidence(repo, "0" * 64)
    env = {"QC_RANGE": "origin/main..HEAD"}
    assert run(repo, "check_uc_integration_evidence.py", env=env).returncode == 1
    script = repo / "scripts" / "ci" / "check_uc_integration_evidence.py"
    text = script.read_text()
    mutated = text.replace('if ev.get("sources_hash") != current:', "if False:")
    assert mutated != text
    script.write_text(mutated)
    assert run(repo, "check_uc_integration_evidence.py", env=env).returncode == 0


# ---- the release rule: a version change needs the full scope ----------------------------------------------------


@pytest.mark.parametrize("where", ["both", "pom", "init"])
def test_a_version_change_rejects_core_evidence_and_names_the_full_scope(repo, where):
    bump(repo, "0.6.1", where)
    write_evidence(repo, current_hash(repo), scope="core")
    res = check(repo)
    assert res.returncode == 1
    assert "scope 'core'" in res.stderr and "full UC tests" in res.stderr and "--scope full" in res.stderr


def test_a_version_change_accepts_full_evidence(repo):
    bump(repo, "0.6.1")
    write_evidence(repo, current_hash(repo), scope="full")
    res = check(repo)
    assert res.returncode == 0, res.stderr
    assert "scope full" in res.stdout


def test_without_a_version_change_core_and_full_both_pass(repo):
    (repo / "python/geobrix/src/pkg/mod.py").write_text("v2\n")
    git(repo, *COMMIT, "commit", "-q", "-am", "src")
    for scope in ("core", "full"):
        write_evidence(repo, current_hash(repo), scope=scope)
        assert check(repo).returncode == 0, scope


def test_a_dependency_version_in_pom_is_not_a_release_version_change(repo):
    (repo / "pom.xml").write_text(POM.format(v="0.6.0").replace("9.9", "9.10"))
    git(repo, *COMMIT, "commit", "-q", "-am", "dep")
    write_evidence(repo, current_hash(repo), scope="core")
    assert check(repo).returncode == 0


def test_the_version_is_compared_with_the_merge_base_not_the_tip_of_main(repo):
    git(repo, "checkout", "-q", "-b", "feature")
    git(repo, "checkout", "-q", "main")
    bump(repo, "0.6.1")  # main moved on; the feature branch did not touch the version
    git(repo, "checkout", "-q", "feature")
    write_evidence(repo, current_hash(repo), scope="core")
    assert check(repo, "main..HEAD").returncode == 0


def test_evidence_without_a_scope_counts_as_core(repo):
    path = write_evidence(repo, current_hash(repo))
    ev = json.loads(path.read_text())
    del ev["scope"]
    path.write_text(json.dumps(ev))
    assert check(repo).returncode == 0
    bump(repo, "0.6.1")
    ev["sources_hash"] = current_hash(repo)
    path.write_text(json.dumps(ev))
    assert check(repo).returncode == 1


def test_a_smoke_or_unknown_scope_never_counts(repo):
    write_evidence(repo, current_hash(repo), scope="smoke")
    assert check(repo).returncode == 1


def test_fail_on_revert_a_check_without_the_release_rule_would_pass_core_evidence_after_a_bump(repo):
    """Mutation guard: with the release rule removed the version-change case passes, so the tests above are not vacuous."""
    bump(repo, "0.6.1")
    write_evidence(repo, current_hash(repo), scope="core")
    assert check(repo).returncode == 1
    script = repo / "scripts" / "ci" / "check_uc_integration_evidence.py"
    text = script.read_text()
    mutated = text.replace("need_full=version_changed(qc_range)", "need_full=False")
    assert mutated != text
    script.write_text(mutated)
    assert check(repo).returncode == 0


def test_the_qc_config_registers_the_check_as_a_failing_command():
    cfg = json.loads((REPO_ROOT / ".claude" / "qc-judge" / "config.json").read_text())
    spec = cfg["checks"]["uc-integration-evidence"]
    assert spec["type"] == "command" and spec["severity"] == "fail" and spec["enabled"] is True
    assert spec["cmd"] == "python3 scripts/ci/check_uc_integration_evidence.py"
    assert spec["timeout_seconds"] <= 60


# ---- the driver and the command ----------------------------------------------------------------------------------


@pytest.fixture
def driver(monkeypatch):
    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    monkeypatch.syspath_prepend(str(CI))
    import importlib

    mod = importlib.import_module("uc_integration")
    # No workspace call: the warehouse looks fine unless a test says otherwise.
    monkeypatch.setattr(mod, "warehouse_problem", lambda *a, **k: None)
    monkeypatch.setattr(mod.Gate, "client", lambda self: object())
    return mod


def test_a_missing_warehouse_stops_the_run_before_any_step(driver, monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(driver, "OUT_DIR", tmp_path)
    monkeypatch.setattr(driver, "EVIDENCE", tmp_path / "evidence.json")
    monkeypatch.setattr(driver, "ADHOC_EVIDENCE", tmp_path / "evidence-adhoc.json")
    monkeypatch.setattr(driver.uc_sources_hash, "dirty_files", lambda root: [])
    monkeypatch.setattr(driver, "slice_tables", lambda s: ("T",))
    monkeypatch.setattr(driver, "warehouse_problem", lambda *a, **k: "the SQL warehouse w was deleted.\nFix: pass --warehouse-id.")
    for step in ("step_wheel", "step_stage", "step_install", "step_warehouse", "cleanup"):
        monkeypatch.setattr(driver.Gate, step, lambda self, *a: pytest.fail("a run with no warehouse must not start a step"))
    assert driver.main(["--profile", "p", "--scope", "core", "--warehouse-id", "w"]) == 1
    err = capsys.readouterr().err
    assert "refused" in err and "Fix:" in err


def test_slice_statements_come_from_the_container_when_the_host_has_no_osgeo(driver, monkeypatch, tmp_path):
    from databricks.labs.gbx.bench import t28

    class OsgeoUnavailable(Exception):
        pass

    def no_osgeo(*a, **k):
        raise OsgeoUnavailable("No module named 'osgeo'")

    calls = []

    def fake_run(argv, log, label, env=None, echo=True):
        calls.append(argv)
        out = tmp_path / "slice-full.json"
        out.write_text(json.dumps({"pairs": [["c1", "SELECT 1"], ["c2", "SELECT 2"]]}))
        return 0, ""

    monkeypatch.setattr(t28, "_slice_pairs", no_osgeo)
    monkeypatch.setattr(driver, "OUT_DIR", tmp_path)
    monkeypatch.setattr(driver, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(driver, "_CONTAINER_SLICE", {})
    monkeypatch.setattr(driver, "run_cmd", fake_run)
    from databricks.labs.gbx.bench import probes

    assert driver.slice_pairs("cat", "sch", probes.UC_SLICE_TABLES) == [("c1", "SELECT 1"), ("c2", "SELECT 2")]
    inner = calls[0][-1]
    assert calls[0][1].endswith("gbx-docker-exec.sh") and "--emit-slice slice-full.json" in inner
    assert "--scope full" in inner and "--against sch" in inner and "--catalog cat" in inner
    driver.slice_pairs("cat", "sch", probes.UC_SLICE_TABLES)
    assert len(calls) == 1  # built once per run


def test_a_failed_container_build_names_a_fix_and_other_errors_pass_through(driver, monkeypatch, tmp_path):
    from databricks.labs.gbx.bench import probes, t28

    class OsgeoUnavailable(Exception):
        pass

    monkeypatch.setattr(t28, "_slice_pairs", lambda *a, **k: (_ for _ in ()).throw(OsgeoUnavailable("x")))
    monkeypatch.setattr(driver, "OUT_DIR", tmp_path)
    monkeypatch.setattr(driver, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(driver, "_CONTAINER_SLICE", {})
    monkeypatch.setattr(driver, "run_cmd", lambda *a, **k: (1, "container is not running"))
    with pytest.raises(driver.StepFailed, match="Fix: start the container"):
        driver.slice_pairs("cat", "sch", probes.UC_SLICE_TABLES)
    monkeypatch.setattr(t28, "_slice_pairs", lambda *a, **k: (_ for _ in ()).throw(ValueError("real bug")))
    with pytest.raises(ValueError):
        driver.slice_pairs("cat", "sch", probes.UC_SLICE_TABLES)


def test_known_failures_need_a_reason_and_skip_comments(driver):
    assert driver.parse_known_failures("# c\n\nfoo: bar baz\n") == {"foo": "bar baz"}
    with pytest.raises(ValueError):
        driver.parse_known_failures("foo\n")
    with pytest.raises(ValueError):
        driver.parse_known_failures("foo:   \n")
    assert driver.parse_known_failures((REPO_ROOT / "scripts/ci/uc_integration_known_failures.txt").read_text()) == {}


def test_judge_slice_counts_unexpected_failures_only(driver):
    msgs = ["a: boom", "b: got 1, want 2", "a: second line of a"]
    r = driver.judge_slice(msgs, 10, {"b": "known cause"})
    assert r == {"total": 10, "passed": 8, "failed": 1, "failed_ids": ["a"], "tolerated_ids": ["b"]}
    ex = {"statements_ok": 5, "total": 5}
    assert driver.decide(r, ex) == "fail"
    assert driver.decide(driver.judge_slice(["b: x"], 10, {"b": "k"}), ex) == "pass"
    assert driver.decide(driver.judge_slice([], 10, {}), {"statements_ok": 4, "total": 5}) == "fail"
    assert driver.decide(driver.judge_slice([], 10, {}), {"statements_ok": 0, "total": 0}) == "fail"


def test_shards_cover_every_statement_once(driver):
    pairs = [(f"c{i}", "SELECT 1") for i in range(23)]
    parts = driver.shard(pairs, 8)
    assert len(parts) == 8 and sorted(c for p in parts for c, _ in p) == sorted(c for c, _ in pairs)
    assert len(driver.shard(pairs[:3], 8)) == 3 and driver.shard([], 4) == []


def test_install_notebook_installs_with_procedures_and_asserts_routines(driver):
    nb = driver.install_notebook("cat", "sch", "/Volumes/c/s/v/uc-it/abc")
    code = "".join(nb["cells"][0]["source"])
    compile(code, "nb", "exec")
    assert "procedures=True" in code and "'/Volumes/c/s/v/uc-it/abc'" in code and "assert cnt > 0" in code
    # Fail on revert: uc.install does not create the schema (first E2E run: SCHEMA_NOT_FOUND), so the notebook must, first.
    create = "CREATE SCHEMA IF NOT EXISTS `cat`.`sch`"
    assert create in code and code.index(create) < code.index("uc.install(")
    # Fail on revert: a wheel staged again under the same name could leave the functions on an older build; the notebook
    # makes one UC function call that reports the imported build, after the install.
    probe = "uc_build_probe.assert_installed_build(spark, 'cat', 'sch', '/Volumes/c/s/v/uc-it/abc'"
    assert probe in code and code.index("uc.install(") < code.index(probe)


def test_wheel_reuse_refuses_a_wheel_built_from_a_dirty_tree(driver):
    ok, why = driver.wheel_reusable({"dirty": True, "git_sha": "a" * 40})
    assert not ok and "dirty" in why


def test_child_commands_do_not_see_the_drivers_virtualenvs(driver, tmp_path):
    # Fail on revert: under `uv run --with ...` throwaway venvs (no pip) lead PATH, so gbx:build:wheel found no pip.
    venvs = []
    for name in ("throwaway", "layer"):
        (tmp_path / name / "bin").mkdir(parents=True)
        (tmp_path / name / "pyvenv.cfg").write_text("home = /usr\n")
        venvs.append(f"{tmp_path / name}/bin")
    env = driver.child_env({"PATH": ":".join(venvs + ["/usr/bin", "/bin"]), "VIRTUAL_ENV": venvs[0], "HOME": "/h"})
    assert env["PATH"] == "/usr/bin:/bin" and "VIRTUAL_ENV" not in env and env["HOME"] == "/h"


def test_the_help_names_the_options_and_a_missing_profile_exits_2():
    sh = str(REPO_ROOT / "scripts" / "commands" / "gbx-uc-tests.sh")
    res = subprocess.run(["bash", sh, "--help"], capture_output=True, text=True)
    assert res.returncode == 0
    for opt in ("--profile", "--keep", "--dry-run", "--scope", "--against", "--shards", "--warehouse-id", "--rebuild", "--run-part", "--drop-parts", "--install-into"):
        assert opt in res.stdout
    for gone in ("--slice", "--smoke"):
        assert gone not in res.stdout
    assert subprocess.run(["bash", sh, "--nope"], capture_output=True, text=True).returncode == 2
    assert subprocess.run(["bash", sh, "--profile"], capture_output=True, text=True).returncode == 2
    assert subprocess.run(["bash", sh, "--install-into"], capture_output=True, text=True).returncode == 2


def test_the_old_command_files_are_gone():
    cmds = REPO_ROOT / "scripts" / "commands"
    assert not (cmds / "gbx-test-uc-integration.sh").exists() and not (cmds / "gbx-test-uc-integration.md").exists()
    assert (cmds / "gbx-uc-tests.sh").exists() and (cmds / "gbx-uc-tests.md").exists()


def test_the_driver_requires_a_profile(driver, capsys):
    assert driver.main([]) == 2
    assert "--profile required" in capsys.readouterr().err


def test_the_driver_refuses_a_dirty_uc_source_and_runs_nothing(driver, monkeypatch, capsys):
    monkeypatch.setattr(driver.uc_sources_hash, "dirty_files", lambda root: ["pom.xml"])
    assert driver.main(["--profile", "p", "--dry-run"]) == 1
    assert "pom.xml" in capsys.readouterr().err


def test_the_driver_dry_run_reports_counts_without_a_workspace_call(driver, monkeypatch, capsys):
    monkeypatch.setattr(driver.uc_sources_hash, "dirty_files", lambda root: [])
    monkeypatch.setattr(driver, "slice_tables", lambda scope: ("T",))
    monkeypatch.setattr(driver, "slice_pairs", lambda c, s, t: [(f"c{i}", "SELECT 1") for i in range(20)])
    monkeypatch.setattr(driver, "slice_batchable", lambda t: set())
    monkeypatch.setattr(driver, "example_pairs", lambda c, s, prefixes=None: {Path("a.sql"): [("x", "SELECT 1")] * 3})
    assert driver.main(["--profile", "p", "--dry-run", "--shards", "4"]) == 0
    out = capsys.readouterr().out
    assert "scope core: slice 20 statements in 4 shard(s)" in out and "no workspace call" in out
    assert "20 warehouse statements, batch 25" in out  # nothing batchable here: the plan keeps every statement


def test_scope_selects_the_tables_and_the_smoke_sample(driver, monkeypatch, capsys):
    from databricks.labs.gbx.bench import probes

    assert driver.slice_tables("core") == probes.UC_CORE_TABLES
    assert driver.slice_tables("smoke") == probes.UC_CORE_TABLES
    assert driver.slice_tables("full") == probes.UC_SLICE_TABLES and len(probes.UC_SLICE_TABLES) > len(probes.UC_CORE_TABLES)
    monkeypatch.setattr(driver.uc_sources_hash, "dirty_files", lambda root: [])
    monkeypatch.setattr(driver, "slice_pairs", lambda c, s, t: [(f"c{i}", "SELECT 1") for i in range(40)])
    monkeypatch.setattr(driver, "slice_batchable", lambda t: set())
    seen = []
    monkeypatch.setattr(driver, "example_pairs", lambda c, s, prefixes=None: seen.append(prefixes) or {Path("a.sql"): [("x", "SELECT 1")]})
    assert driver.main(["--profile", "p", "--dry-run", "--scope", "smoke", "--against", "sch"]) == 0
    assert "scope smoke against sch: slice 12 statements" in capsys.readouterr().out and seen == ["00,01"]
    assert driver.main(["--profile", "p", "--dry-run", "--scope", "full"]) == 0
    assert "scope full: slice 40 statements" in capsys.readouterr().out and seen[-1] is None


def test_smoke_without_against_is_refused(driver, capsys):
    assert driver.main(["--profile", "p", "--scope", "smoke"]) == 2
    assert "--against" in capsys.readouterr().err


@pytest.mark.parametrize("scope", ["smoke", "core", "full"])
def test_an_against_run_writes_only_the_adhoc_file(driver, monkeypatch, tmp_path, scope):
    """--against writes evidence-adhoc.json (with the scope) and leaves evidence.json alone, whatever the verdict."""
    monkeypatch.setattr(driver, "OUT_DIR", tmp_path)
    monkeypatch.setattr(driver, "EVIDENCE", tmp_path / "evidence.json")
    monkeypatch.setattr(driver, "ADHOC_EVIDENCE", tmp_path / "evidence-adhoc.json")
    monkeypatch.setattr(driver, "slice_tables", lambda s: ("T",))
    ok = ({"total": 1, "passed": 1, "failed": 0, "failed_ids": [], "tolerated_ids": []}, {"statements_ok": 2, "total": 2, "failures": []})
    monkeypatch.setattr(driver.Gate, "step_warehouse", lambda self, tables, known: ok)
    for step in ("step_wheel", "step_stage", "step_install"):
        monkeypatch.setattr(driver.Gate, step, lambda self, *a: pytest.fail("an --against run builds, stages and installs nothing"))
    monkeypatch.setattr(driver.Gate, "cleanup", lambda self: pytest.fail("an --against run cleans up nothing"))
    (tmp_path / "evidence.json").write_text("{}")
    rc = driver.main(["--profile", "p", "--scope", scope, "--against", "some_schema", "--warehouse-id", "w"])
    assert rc == 0
    ev = json.loads((tmp_path / "evidence-adhoc.json").read_text())
    assert ev["scope"] == scope and ev["schema"] == "some_schema" and ev["adhoc"] is True and ev["result"] == "pass"
    assert (tmp_path / "evidence.json").read_text() == "{}"


def test_a_gated_run_records_its_scope_in_evidence_json(driver, monkeypatch, tmp_path):
    monkeypatch.setattr(driver, "OUT_DIR", tmp_path)
    monkeypatch.setattr(driver, "EVIDENCE", tmp_path / "evidence.json")
    monkeypatch.setattr(driver, "ADHOC_EVIDENCE", tmp_path / "evidence-adhoc.json")
    monkeypatch.setattr(driver.uc_sources_hash, "dirty_files", lambda root: [])
    monkeypatch.setattr(driver, "slice_tables", lambda s: ("T",))
    ok = ({"total": 1, "passed": 1, "failed": 0, "failed_ids": [], "tolerated_ids": []}, {"statements_ok": 2, "total": 2, "failures": []})
    monkeypatch.setattr(driver.Gate, "step_wheel", lambda self: Path("w.whl"))
    monkeypatch.setattr(driver, "wheel_info", lambda w: {"name": "w.whl"})
    monkeypatch.setattr(driver.Gate, "step_stage", lambda self, w, i: "/v/w.whl")
    monkeypatch.setattr(driver.Gate, "step_install", lambda self, w: None)
    monkeypatch.setattr(driver.Gate, "step_warehouse", lambda self, tables, known: ok)
    monkeypatch.setattr(driver.Gate, "cleanup", lambda self: "dropped")
    assert driver.main(["--profile", "p", "--scope", "full", "--warehouse-id", "w"]) == 0
    ev = json.loads((tmp_path / "evidence.json").read_text())
    assert ev["scope"] == "full" and ev["slice"]["scope"] == "full" and ev["result"] == "pass" and "adhoc" not in ev
    assert not (tmp_path / "evidence-adhoc.json").exists()


def test_the_driver_dry_run_counts_the_batched_warehouse_statements(driver, monkeypatch, capsys):
    pairs = [(f"v{i}", f"SELECT f({i}) AS out") for i in range(60)] + [(f"r{i}", f"SELECT bad({i}) AS out") for i in range(5)]
    monkeypatch.setattr(driver.uc_sources_hash, "dirty_files", lambda root: [])
    monkeypatch.setattr(driver, "slice_tables", lambda scope: ("T",))
    monkeypatch.setattr(driver, "slice_pairs", lambda c, s, t: pairs)
    monkeypatch.setattr(driver, "slice_batchable", lambda t: {c for c, _ in pairs if c.startswith("v")})
    monkeypatch.setattr(driver, "example_pairs", lambda c, s, prefixes=None: {Path("a.sql"): [("x", "SELECT 1")]})
    assert driver.main(["--profile", "p", "--dry-run", "--batch", "25"]) == 0
    assert "slice 65 statements in 1 shard(s) (8 warehouse statements, batch 25" in capsys.readouterr().out  # 25+25+10 values, 5 refusals
    assert driver.main(["--profile", "p", "--dry-run", "--batch", "1"]) == 0
    assert "(65 warehouse statements, batch 1" in capsys.readouterr().out


def test_the_driver_hands_the_runner_its_batch_and_concurrency(driver, monkeypatch):
    g = driver.Gate(driver.parse_args(["--profile", "p", "--warehouse-id", "w", "--batch", "7", "--concurrency", "5"]))
    argv = g.uc_sql_argv(Path("a.sql"), Path("a.json"), "sch", batch=True)
    assert argv[argv.index("--batch") + 1] == "7" and argv[argv.index("--concurrency") + 1] == "5"
    assert "--batch" not in g.uc_sql_argv(Path("a.sql"), Path("a.json"))  # cleanup and the like run alone
    assert driver.parse_args(["--profile", "p"]).batch == driver.DEFAULT_BATCH == 25


# ---- a gated run in parts (--run-part I/N) -------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["0/3", "4/3", "1/1", "x", "2", "2/x"])
def test_run_part_needs_i_of_n_with_n_at_least_2(driver, bad, capsys):
    with pytest.raises(SystemExit) as exc:
        driver.parse_args(["--profile", "p", "--run-part", bad])
    assert exc.value.code == 2
    assert driver.parse_args(["--profile", "p", "--run-part", "2/5"]).run_part == (2, 5)
    assert driver.parse_args(["--profile", "p"]).run_part is None


def test_parts_deal_the_slice_and_give_the_examples_to_the_last_part(driver):
    pairs = [(f"c{i}", "SELECT 1") for i in range(23)]
    work = [driver.part_work(pairs, i, 4) for i in range(1, 5)]
    assert [ex for _, ex in work] == [False, False, False, True]
    dealt = [c for sl, _ in work for c, _ in sl]
    assert sorted(dealt) == sorted(c for c, _ in pairs) and len(dealt) == len(pairs)
    assert work[-1][0] == []
    assert driver.part_work(pairs, 1, 1) == (pairs, True)  # an unsplit run: all of it


def test_merge_parts_judges_the_union_and_flags_a_case_no_part_ran(driver):
    recs = {
        "1": {"cids": ["a", "b"], "slice": {"total": 2, "passed": 1, "failed": 1, "failed_ids": ["b"], "tolerated_ids": []}, "examples": None},
        "2": {"cids": ["c"], "slice": {"total": 1, "passed": 0, "failed": 0, "failed_ids": [], "tolerated_ids": ["c"]}, "examples": None},
        "3": {"cids": [], "slice": {"total": 0, "passed": 0, "failed": 0, "failed_ids": [], "tolerated_ids": []},
              "examples": {"statements_ok": 4, "total": 4, "failures": []}},
    }
    sl, ex = driver.merge_parts(recs, ["a", "b", "c"], 3)
    assert sl == {"total": 3, "passed": 1, "failed": 1, "failed_ids": ["b"], "tolerated_ids": ["c"]}
    assert ex == {"statements_ok": 4, "total": 4, "failures": []}
    sl, _ = driver.merge_parts(recs, ["a", "b", "c", "d"], 3)  # fail on revert: a case no part ran must not pass
    assert sl["failed_ids"] == ["b", "d"] and sl["total"] == 4 and sl["passed"] == 1


def _parted(driver, monkeypatch, tmp_path):
    """A gated run with every workspace step faked; returns the call log."""
    monkeypatch.setattr(driver, "OUT_DIR", tmp_path)
    monkeypatch.setattr(driver, "EVIDENCE", tmp_path / "evidence.json")
    monkeypatch.setattr(driver, "ADHOC_EVIDENCE", tmp_path / "evidence-adhoc.json")
    monkeypatch.setattr(driver.uc_sources_hash, "dirty_files", lambda root: [])
    monkeypatch.setattr(driver, "slice_tables", lambda s: ("T",))
    calls = []
    monkeypatch.setattr(driver.Gate, "step_wheel", lambda self: calls.append("wheel") or Path("w.whl"))
    monkeypatch.setattr(driver, "wheel_info", lambda w: {"name": "w.whl"})
    monkeypatch.setattr(driver.Gate, "step_stage", lambda self, w, i: calls.append("stage") or "/v/w.whl")

    def install(self, w):
        calls.append("install")
        self.run_ids.append("111")
        self.ws_dir = "/Users/me/GeoBrix/uc-it-x"

    monkeypatch.setattr(driver.Gate, "step_install", install)
    monkeypatch.setattr(driver.Gate, "schema_exists", lambda self: True)

    def warehouse(self, tables, known, part=(1, 1)):
        i, n = part
        calls.append(f"warehouse {i}/{n}")
        self.slice_expected = ["a", "b", "c"]
        cids = {1: ["a", "b"], 2: ["c"], 3: []}[i]
        self.part_cids = cids
        sl = {"total": len(cids), "passed": len(cids), "failed": 0, "failed_ids": [], "tolerated_ids": []}
        ex = {"statements_ok": 5, "total": 5, "failures": []} if i == n else None
        return sl, ex

    monkeypatch.setattr(driver.Gate, "step_warehouse", warehouse)
    monkeypatch.setattr(driver.Gate, "cleanup", lambda self: calls.append(f"cleanup {self.schema} {self.ws_dir}") or "dropped")
    return calls


def test_a_parted_run_installs_once_resumes_and_writes_evidence_after_the_last_part(driver, monkeypatch, tmp_path):
    calls = _parted(driver, monkeypatch, tmp_path)
    base = ["--profile", "p", "--scope", "full", "--warehouse-id", "w", "--run-part"]
    assert driver.main(base + ["1/3"]) == 0
    assert calls == ["wheel", "stage", "install", "warehouse 1/3"]
    assert not (tmp_path / "evidence.json").exists()  # a part alone is not evidence, and nothing is dropped yet
    calls.clear()
    assert driver.main(base + ["3/3"]) == 0  # any order; a rerun or a later part resumes on the install
    assert driver.main(base + ["1/3"]) == 0
    assert calls == ["warehouse 3/3", "warehouse 1/3"] and not (tmp_path / "evidence.json").exists()
    calls.clear()
    assert driver.main(base + ["2/3"]) == 0
    assert calls[0] == "warehouse 2/3" and calls[1].startswith("cleanup geobrix_uc_it_") and calls[1].endswith("/Users/me/GeoBrix/uc-it-x")
    ev = json.loads((tmp_path / "evidence.json").read_text())
    assert ev["result"] == "pass" and ev["scope"] == "full" and ev["parts"] == 3 and ev["job_run_ids"] == ["111"]
    assert ev["slice"]["total"] == 3 and ev["slice"]["passed"] == 3 and ev["examples"]["total"] == 5
    assert ev["wheel"] == {"name": "w.whl"} and ev["cleanup"] == "dropped" and "adhoc" not in ev


def test_a_failed_part_keeps_the_install_and_its_rerun_completes_the_run(driver, monkeypatch, tmp_path):
    calls = _parted(driver, monkeypatch, tmp_path)
    base = ["--profile", "p", "--scope", "full", "--warehouse-id", "w", "--run-part"]
    good = driver.Gate.step_warehouse
    monkeypatch.setattr(driver.Gate, "step_warehouse", lambda self, t, k, part=(1, 1): (_ for _ in ()).throw(driver.StepFailed("slice_0 stopped")))
    assert driver.main(base + ["1/2"]) == 1
    assert not any(c.startswith("cleanup") for c in calls)  # the install stays for the rerun
    monkeypatch.setattr(driver.Gate, "step_warehouse", good)
    assert driver.main(base + ["2/2"]) == 0 and not (tmp_path / "evidence.json").exists()  # part 1 has no result yet
    assert driver.main(base + ["1/2"]) == 0
    assert calls.count("install") == 1 and json.loads((tmp_path / "evidence.json").read_text())["result"] == "pass"


def test_a_part_that_finds_a_failure_fails_and_so_does_the_merged_evidence(driver, monkeypatch, tmp_path):
    _parted(driver, monkeypatch, tmp_path)
    good = driver.Gate.step_warehouse

    def one_bad(self, tables, known, part=(1, 1)):
        sl, ex = good(self, tables, known, part)
        if part[0] == 1:
            sl = {**sl, "passed": 1, "failed": 1, "failed_ids": ["b"]}
        return sl, ex

    monkeypatch.setattr(driver.Gate, "step_warehouse", one_bad)
    base = ["--profile", "p", "--scope", "full", "--warehouse-id", "w", "--run-part"]
    assert driver.main(base + ["1/2"]) == 1
    assert driver.main(base + ["2/2"]) == 1
    ev = json.loads((tmp_path / "evidence.json").read_text())
    assert ev["result"] == "fail" and ev["slice"]["failed_ids"] == ["b"]


def test_a_lost_schema_is_installed_again(driver, monkeypatch, tmp_path):
    calls = _parted(driver, monkeypatch, tmp_path)
    base = ["--profile", "p", "--scope", "full", "--warehouse-id", "w", "--run-part"]
    assert driver.main(base + ["1/2"]) == 0
    monkeypatch.setattr(driver.Gate, "schema_exists", lambda self: False)
    assert driver.main(base + ["2/2"]) == 0
    assert calls.count("install") == 2


def test_run_part_takes_no_smoke_scope(driver, monkeypatch, capsys):
    monkeypatch.setattr(driver.uc_sources_hash, "dirty_files", lambda root: [])
    assert driver.main(["--profile", "p", "--warehouse-id", "w", "--run-part", "1/2", "--scope", "smoke", "--against", "sch"]) == 2
    assert "--run-part" in capsys.readouterr().err


def test_an_against_run_of_a_part_runs_its_share_and_writes_only_the_adhoc_file(driver, monkeypatch, tmp_path):
    monkeypatch.setattr(driver, "OUT_DIR", tmp_path)
    monkeypatch.setattr(driver, "EVIDENCE", tmp_path / "evidence.json")
    monkeypatch.setattr(driver, "ADHOC_EVIDENCE", tmp_path / "evidence-adhoc.json")
    monkeypatch.setattr(driver, "slice_tables", lambda s: ("T",))
    seen = []
    ok = {"total": 2, "passed": 2, "failed": 0, "failed_ids": [], "tolerated_ids": []}
    monkeypatch.setattr(driver.Gate, "step_warehouse", lambda self, tables, known, part=(1, 1): seen.append(part) or (ok, None))
    for step in ("step_wheel", "step_stage", "step_install", "cleanup"):
        monkeypatch.setattr(driver.Gate, step, lambda self, *a: pytest.fail("an --against run builds, installs and drops nothing"))
    assert driver.main(["--profile", "p", "--scope", "full", "--against", "sch", "--warehouse-id", "w", "--run-part", "2/3"]) == 0
    ev = json.loads((tmp_path / "evidence-adhoc.json").read_text())
    assert seen == [(2, 3)] and ev["part"] == "2/3" and ev["result"] == "pass" and ev["examples"] is None
    assert not (tmp_path / "evidence.json").exists() and not list(tmp_path.glob("*/parts-*.json"))
    bad = {**ok, "passed": 1, "failed": 1, "failed_ids": ["x"]}
    monkeypatch.setattr(driver.Gate, "step_warehouse", lambda self, tables, known, part=(1, 1): (bad, None))
    assert driver.main(["--profile", "p", "--scope", "full", "--against", "sch", "--warehouse-id", "w", "--run-part", "2/3"]) == 1


def test_drop_parts_cleans_up_an_unfinished_parted_run_of_another_commit(driver, monkeypatch, tmp_path):
    calls = _parted(driver, monkeypatch, tmp_path)
    assert driver.main(["--profile", "p", "--scope", "full", "--warehouse-id", "w", "--run-part", "1/2"]) == 0
    sha12 = next(p.parent.name for p in tmp_path.glob("*/parts-*.json"))
    calls.clear()
    assert driver.main(["--profile", "p", "--warehouse-id", "w", "--drop-parts", sha12]) == 0
    assert calls == [f"cleanup geobrix_uc_it_{sha12} /Users/me/GeoBrix/uc-it-x"]
    state = json.loads(next(tmp_path.glob("*/parts-*.json")).read_text())
    assert state["dropped"] == "dropped"
    assert driver.main(["--profile", "p", "--warehouse-id", "w", "--drop-parts", "000000000000"]) == 1


def test_the_dry_run_prints_the_plan_of_every_part(driver, monkeypatch, capsys):
    monkeypatch.setattr(driver.uc_sources_hash, "dirty_files", lambda root: [])
    monkeypatch.setattr(driver, "slice_tables", lambda scope: ("T",))
    monkeypatch.setattr(driver, "slice_pairs", lambda c, s, t: [(f"c{i}", "SELECT 1") for i in range(10)])
    monkeypatch.setattr(driver, "slice_batchable", lambda t: set())
    monkeypatch.setattr(driver, "example_pairs", lambda c, s, prefixes=None: {Path("a.sql"): [("x", "SELECT 1")] * 3})
    assert driver.main(["--profile", "p", "--dry-run", "--scope", "full", "--run-part", "1/3"]) == 0
    out = capsys.readouterr().out
    assert "part 1/3: slice 5 statements (5 warehouse statements)" in out
    assert "part 2/3: slice 5 statements" in out and "part 3/3: the example files, 3 statements" in out


def test_a_judgement_that_needs_osgeo_is_redone_in_the_container(driver, monkeypatch, tmp_path):
    """Fail on revert: the host has no osgeo, so the summary, polygonize and contour references could not judge a
    real result (the first full run: `the judge could not read the result (OsgeoUnavailable: ...)`)."""
    from databricks.labs.gbx.bench import oracles

    host = ["a: the judge could not read the result (OsgeoUnavailable: the summary reference needs osgeo)", "b: boom"]
    monkeypatch.setattr(oracles, "check_uc_results", lambda cases, results, d: list(host))
    monkeypatch.setattr(driver, "OUT_DIR", tmp_path)
    monkeypatch.setattr(driver, "REPO_ROOT", tmp_path)
    calls = []

    def fake_run(argv, log, label, env=None, echo=True):
        calls.append(argv)
        sent = oracles.load_uc_results(str(next(tmp_path.glob("judge-*-in.json"))))
        assert sorted(sent) == ["a"] and sent["a"]["value"] == b"\x01"  # only what needs osgeo, bytes intact
        next(tmp_path.glob("judge-*-in.json")).with_name(next(tmp_path.glob("judge-*-in.json")).name.replace("-in", "-out")).write_text(
            json.dumps({"failures": ["a: got 2, want 3"]})
        )
        return 0, ""

    monkeypatch.setattr(driver, "run_cmd", fake_run)
    got = driver.judge_uc([], {"a": {"value": b"\x01"}, "b": {"value": 1}}, "full")
    assert got == ["b: boom", "a: got 2, want 3"]
    assert calls[0][1].endswith("gbx-docker-exec.sh") and "--judge-uc" in calls[0][-1] and "--scope full" in calls[0][-1]
    monkeypatch.setattr(driver, "run_cmd", lambda *a, **k: (1, "container is not running"))
    with pytest.raises(driver.StepFailed, match="Fix: start the container"):
        driver.judge_uc([], {"a": {"value": b"\x01"}}, "full")
    host[:] = ["b: boom"]
    assert driver.judge_uc([], {}, "full") == ["b: boom"]  # nothing needs osgeo: no container call


def test_the_container_side_judges_the_results_it_is_given(driver, monkeypatch, tmp_path):
    from databricks.labs.gbx.bench import oracles, probes

    monkeypatch.setattr(probes, "uc_slice_cases", lambda tables: [type("C", (), {"cid": "a"})(), type("C", (), {"cid": "z"})()])
    seen = []
    monkeypatch.setattr(oracles, "check_uc_results", lambda cases, results, d: seen.append([c.cid for c in cases]) or ["a: x"])
    inp, out = tmp_path / "in.json", tmp_path / "out.json"
    oracles.dump_uc_results({"a": {"value": 1}}, str(inp))
    assert driver.main(["--judge-uc", str(inp), "--judge-out", str(out), "--scope", "full"]) == 0
    assert seen == [["a"]] and json.loads(out.read_text()) == {"failures": ["a: x"]}


def test_a_parted_run_resumes_across_a_commit_that_changes_no_uc_source_or_wheel_input(driver, monkeypatch, tmp_path):
    """Fail on revert: other agents committed docs and commands between parts, and each new HEAD started the parted
    run again with a fresh install (three installs in one run). The record now follows the UC sources, not HEAD."""
    calls = _parted(driver, monkeypatch, tmp_path)
    monkeypatch.setattr(driver.uc_sources_hash, "sources_hash", lambda root: "h" * 64)
    base = ["--profile", "p", "--scope", "full", "--warehouse-id", "w", "--run-part"]
    monkeypatch.setattr(driver.uc_sources_hash, "head_sha", lambda root: "a" * 40)
    assert driver.main(base + ["1/2"]) == 0
    monkeypatch.setattr(driver.uc_sources_hash, "head_sha", lambda root: "b" * 40)
    monkeypatch.setattr(driver, "inputs_changed", lambda old, root=None: [])
    assert driver.main(base + ["2/2"]) == 0
    assert calls.count("install") == 1
    ev = json.loads((tmp_path / "evidence.json").read_text())
    assert ev["head_sha"] == "b" * 40 and ev["install_head_sha"] == "a" * 40 and ev["schema"] == "geobrix_uc_it_" + "a" * 12
    assert [r["head_sha"] for r in ev["part_runs"].values()] == ["a" * 40, "b" * 40]
    assert ev["result"] == "pass" and calls[-1].startswith("cleanup geobrix_uc_it_" + "a" * 12)


def test_a_parted_run_starts_again_when_a_wheel_input_changed(driver, monkeypatch, tmp_path):
    calls = _parted(driver, monkeypatch, tmp_path)
    monkeypatch.setattr(driver.uc_sources_hash, "sources_hash", lambda root: "h" * 64)
    base = ["--profile", "p", "--scope", "full", "--warehouse-id", "w", "--run-part"]
    monkeypatch.setattr(driver.uc_sources_hash, "head_sha", lambda root: "a" * 40)
    assert driver.main(base + ["1/2"]) == 0
    monkeypatch.setattr(driver.uc_sources_hash, "head_sha", lambda root: "c" * 40)
    monkeypatch.setattr(driver, "inputs_changed", lambda old, root=None: ["python/geobrix/envs/env6.txt"])
    assert driver.main(base + ["2/2"]) == 0
    assert calls.count("install") == 2 and not (tmp_path / "evidence.json").exists()  # part 1 of the new run is due


def test_inputs_changed_lists_the_uc_and_wheel_inputs_a_commit_range_touched(driver):
    head = driver.uc_sources_hash.head_sha(REPO_ROOT)
    assert driver.inputs_changed(head) == []
    assert driver.inputs_changed("0" * 40) == ["<commit 000000000000 is not in this repository>"]


def test_install_into_stages_installs_and_keeps_without_a_warehouse_or_evidence(driver, monkeypatch, tmp_path):
    """--install-into is a kept demo install: its own folder by schema, no warehouse step, no cleanup, no evidence."""
    monkeypatch.setattr(driver, "OUT_DIR", tmp_path)
    monkeypatch.setattr(driver, "EVIDENCE", tmp_path / "evidence.json")
    monkeypatch.setattr(driver, "ADHOC_EVIDENCE", tmp_path / "evidence-adhoc.json")
    monkeypatch.setattr(driver, "warehouse_problem", lambda *a, **k: pytest.fail("an install looks up no warehouse"))
    seen = {}
    monkeypatch.setattr(driver.Gate, "step_wheel", lambda self: Path("w.whl"))
    monkeypatch.setattr(driver, "wheel_info", lambda w: {"name": "w.whl", "sha256": "ab" * 32, "git_sha": "c" * 40})
    monkeypatch.setattr(driver.Gate, "step_stage", lambda self, w, i: seen.setdefault("folder", self.folder) + "/w.whl")
    monkeypatch.setattr(driver.Gate, "step_install", lambda self, w: seen.update(schema=self.schema, wheel=w))
    for step in ("step_warehouse", "cleanup"):
        monkeypatch.setattr(driver.Gate, step, lambda self, *a: pytest.fail("an install runs no warehouse step and keeps everything"))
    assert driver.main(["--profile", "p", "--catalog", "c", "--volroot", "/Volumes/c/s/v", "--install-into", "demo"]) == 0
    assert seen == {"folder": "/Volumes/c/s/v/uc-install/demo", "schema": "demo", "wheel": "/Volumes/c/s/v/uc-install/demo/w.whl"}
    assert not (tmp_path / "evidence.json").exists() and not (tmp_path / "evidence-adhoc.json").exists()


def test_install_into_takes_no_against(driver, capsys):
    assert driver.main(["--profile", "p", "--install-into", "demo", "--against", "x"]) == 2
    assert "--against" in capsys.readouterr().err


def test_install_into_reuses_any_clean_wheel_but_not_a_dirty_one(driver, monkeypatch, tmp_path):
    dist = tmp_path / "dist" / "env6"
    dist.mkdir(parents=True)
    (dist / "geobrix-0.6.0+env6-py3-none-any.whl").write_text("")
    g = driver.Gate(driver.parse_args(["--profile", "p", "--install-into", "demo"]))
    monkeypatch.setattr(driver, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(driver, "wheel_reusable", lambda info: pytest.fail("an install does not ask whether the wheel fits HEAD"))
    monkeypatch.setattr(driver, "wheel_info", lambda w: {"dirty": False, "git_sha": "c" * 40, "sha256": "ab" * 32})
    assert g.step_wheel().name == "geobrix-0.6.0+env6-py3-none-any.whl"
    monkeypatch.setattr(driver, "wheel_info", lambda w: {"dirty": True, "git_sha": "c" * 40, "sha256": "ab" * 32})
    # A dirty wheel falls through to the gated check, which refuses it too, so the build path starts (jar_stale first).
    monkeypatch.setattr(driver, "wheel_reusable", lambda info: (False, "the wheel is dirty"))
    monkeypatch.setattr(driver, "jar_stale", lambda jar: pytest.fail("a dirty wheel is not reused: the run builds"))
    with pytest.raises(pytest.fail.Exception, match="a dirty wheel is not reused"):
        g.step_wheel()


def test_install_into_dry_run_makes_no_workspace_call(driver, monkeypatch, capsys):
    for step in ("step_wheel", "step_stage", "step_install"):
        monkeypatch.setattr(driver.Gate, step, lambda self, *a: pytest.fail("a dry run calls no step"))
    assert driver.main(["--profile", "p", "--catalog", "c", "--volroot", "/Volumes/c/s/v", "--install-into", "demo", "--dry-run"]) == 0
    assert "dry run: install into c.demo, staging folder /Volumes/c/s/v/uc-install/demo" in capsys.readouterr().out


def test_install_into_reports_a_failed_step_and_exits_1(driver, monkeypatch, capsys):
    monkeypatch.setattr(driver.Gate, "step_wheel", lambda self: Path("w.whl"))
    monkeypatch.setattr(driver, "wheel_info", lambda w: {"name": "w.whl", "sha256": "ab" * 32, "git_sha": "c" * 40})

    def boom(self, w, i):
        raise driver.StepFailed("stage refused")

    monkeypatch.setattr(driver.Gate, "step_stage", boom)
    monkeypatch.setattr(driver.Gate, "step_install", lambda self, w: pytest.fail("no install after a failed stage"))
    assert driver.main(["--profile", "p", "--catalog", "c", "--volroot", "/Volumes/c/s/v", "--install-into", "demo"]) == 1
    assert "FAILED: stage refused" in capsys.readouterr().err
