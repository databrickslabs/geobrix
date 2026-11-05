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


@pytest.fixture
def repo(tmp_path):
    root = tmp_path / "repo"
    (root / "scripts" / "ci").mkdir(parents=True)
    for name in ("uc_sources_hash.py", "check_uc_integration_evidence.py"):
        shutil.copy(CI / name, root / "scripts" / "ci" / name)
    for rel in ("python/geobrix/src/pkg/mod.py", "examples/uc-functions/00.sql", "src/main/scala/A.scala", "pom.xml", "docs/a.md"):
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("v1\n")
    git(root, "init", "-q", "-b", "main")
    git(root, "-c", "user.email=t@t", "-c", "user.name=t", "add", "-A")
    git(root, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "init")
    return root


def run(root, script, *args, env=None):
    e = {k: v for k, v in os.environ.items() if k not in ("QC_RANGE", "GBX_UC_EVIDENCE")}
    e.update(env or {})
    return subprocess.run(
        [sys.executable, str(root / "scripts" / "ci" / script), *args], capture_output=True, text=True, env=e, cwd=root
    )


def write_evidence(root, sources_hash, result="pass", slice_failed=0):
    ev = {
        "head_sha": "a" * 40,
        "sources_hash": sources_hash,
        "result": result,
        "schema": "geobrix_uc_it_aaaaaaaaaaaa",
        "finished": "2026-11-05T12:00:00Z",
        "slice": {"passed": 10, "failed": slice_failed, "failed_ids": []},
        "examples": {"statements_ok": 156, "total": 156},
    }
    path = root / "test-logs" / "uc-integration" / "evidence.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ev))
    return path


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
    (repo / "pom.xml").write_text("v1\n")
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
    assert "no UC integration evidence" in res.stderr and "gbx-test-uc-integration.sh --profile" in res.stderr


@pytest.mark.parametrize("rng", MAIN_RANGES)
def test_main_bound_with_fresh_passing_evidence_passes_and_prints_the_summary(repo, rng):
    write_evidence(repo, current_hash(repo))
    res = run(repo, "check_uc_integration_evidence.py", env={"QC_RANGE": rng})
    assert res.returncode == 0, res.stderr
    assert "PASS" in res.stdout and "slice 10 passed / 0 failed" in res.stdout and "examples 156/156" in res.stdout


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

    return importlib.import_module("uc_integration")


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


def test_wheel_reuse_refuses_a_wheel_built_from_a_dirty_tree(driver):
    ok, why = driver.wheel_reusable({"dirty": True, "git_sha": "a" * 40})
    assert not ok and "dirty" in why


def test_the_help_names_the_options_and_a_missing_profile_exits_2():
    sh = str(REPO_ROOT / "scripts" / "commands" / "gbx-test-uc-integration.sh")
    res = subprocess.run(["bash", sh, "--help"], capture_output=True, text=True)
    assert res.returncode == 0
    for opt in ("--profile", "--keep", "--dry-run", "--slice", "--shards", "--warehouse-id", "--rebuild", "--smoke"):
        assert opt in res.stdout
    assert subprocess.run(["bash", sh, "--nope"], capture_output=True, text=True).returncode == 2
    assert subprocess.run(["bash", sh, "--profile"], capture_output=True, text=True).returncode == 2


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
    monkeypatch.setattr(driver, "example_pairs", lambda c, s: {Path("a.sql"): [("x", "SELECT 1")] * 3})
    assert driver.main(["--profile", "p", "--dry-run", "--shards", "4"]) == 0
    out = capsys.readouterr().out
    assert "slice core: 20 statements in 4 shard(s)" in out and "no workspace call" in out


def test_a_smoke_run_is_never_evidence(driver, monkeypatch, tmp_path):
    """--smoke writes evidence-smoke.json and leaves evidence.json alone, whatever the verdict."""
    monkeypatch.setattr(driver, "OUT_DIR", tmp_path)
    monkeypatch.setattr(driver, "EVIDENCE", tmp_path / "evidence.json")
    monkeypatch.setattr(driver.uc_sources_hash, "dirty_files", lambda root: [])
    monkeypatch.setattr(driver, "slice_tables", lambda scope: ("T",))
    ok = ({"total": 1, "passed": 1, "failed": 0, "failed_ids": [], "tolerated_ids": []}, {"statements_ok": 2, "total": 2, "failures": []})
    monkeypatch.setattr(driver.Gate, "step_warehouse", lambda self, tables, known: ok)
    monkeypatch.setattr(driver.Gate, "cleanup", lambda self: pytest.fail("a smoke run cleans up nothing"))
    (tmp_path / "evidence.json").write_text("{}")
    rc = driver.main(["--profile", "p", "--smoke", "some_schema", "--warehouse-id", "w"])
    assert rc == 1  # not "pass"
    assert json.loads((tmp_path / "evidence-smoke.json").read_text())["result"] == "smoke"
    assert (tmp_path / "evidence.json").read_text() == "{}"
