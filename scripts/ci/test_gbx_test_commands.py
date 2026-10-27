"""Tests for fixes to the gbx:test:* commands: parity discovery, doc-test isolation, locale.

Each command runs from a scratch copy of scripts/commands/ against a fake `docker` (see
conftest.py's ContainerSandbox): nothing reaches a daemon, and the work a command would send
into the container (pytest, Maven) is recorded instead of run.
"""

import re
from pathlib import Path

import pytest
from conftest import REPO_ROOT

TEST_ROOT = REPO_ROOT / "python" / "geobrix" / "test"
PARITY_RE = re.compile(r"python3 -m pytest /root/geobrix/(\S+) ")

# Suites the parity gate deliberately does not run, and why (see gbx-test-parity.md).
EXCLUDED = {
    "python/geobrix/test/core/test_check_binding_parity.py",  # unit test of the checker
    "python/geobrix/test/ds/test_vector_writer_parity.py",  # light-only round trip
    "python/geobrix/test/ds/test_writer_parity.py",  # light-only round trip
    "python/geobrix/test/ds/test_reader_parity.py",  # skips when the heavy reader yields 0 rows
}


def _repo_rel(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def _parity_sandbox(sandbox, test_tree: Path):
    """A sandbox whose python/geobrix/test is `test_tree` and that has a staged JAR."""
    geobrix = sandbox.root / "python" / "geobrix"
    (geobrix / "lib").mkdir(parents=True)
    (geobrix / "lib" / "geobrix-0.0.0-jar-with-dependencies.jar").write_text("jar")
    if test_tree.is_symlink() or not test_tree.exists():
        raise AssertionError("pass a real directory")
    (geobrix / "test").symlink_to(test_tree)
    return sandbox


def _run_parity(sandbox, *args):
    res = sandbox.run(
        "gbx-test-parity.sh",
        "--skip-build",
        *args,
        FAKE_DOCKER_EXEC_STDOUT="== 3 passed in 1.00s ==",
    )
    ran = [m.group(1) for c in sandbox.execs() for m in [PARITY_RE.search(c[-1])] if m]
    return res, ran


# ------------------------------------------------------------------- parity: discovery


def test_the_default_run_covers_every_test_parity_file_in_the_repo(sandbox):
    _parity_sandbox(sandbox, TEST_ROOT)
    every = {_repo_rel(p) for p in TEST_ROOT.rglob("test_parity*.py")}
    assert len(every) >= 18  # the files the old fixed list missed are among them

    res, ran = _run_parity(sandbox)

    assert res.returncode == 0, res.output
    assert every - set(ran) == set(), "the default parity run skipped these suites"
    assert len(ran) == len(set(ran))  # each suite once


def test_the_default_run_keeps_the_suites_the_old_list_named(sandbox):
    _parity_sandbox(sandbox, TEST_ROOT)

    res, ran = _run_parity(sandbox)

    for old in (
        "python/geobrix/test/pyvx/test_crs_parity.py",
        "python/geobrix/test/pygx/test_gridx_error_parity.py",
        "python/geobrix/test/ds/test_vector_parity.py",
    ):
        assert old in ran, old


def test_the_default_run_skips_only_the_documented_exclusions(sandbox):
    _parity_sandbox(sandbox, TEST_ROOT)
    candidates = {
        _repo_rel(p)
        for pattern in ("test_parity*.py", "test_*_parity.py")
        for p in TEST_ROOT.rglob(pattern)
    }

    res, ran = _run_parity(sandbox)

    assert candidates - set(ran) == EXCLUDED
    assert all((REPO_ROOT / e).exists() for e in EXCLUDED)  # no stale exclusion


def test_a_new_parity_suite_is_picked_up_without_editing_the_command(sandbox, tmp_path):
    tree = tmp_path / "tests"
    (tree / "pyrx").mkdir(parents=True)
    (tree / "pyrx" / "test_parity_brand_new.py").write_text("def test_x(): pass\n")
    (tree / "pyrx" / "test_unrelated.py").write_text("def test_y(): pass\n")
    _parity_sandbox(sandbox, tree)

    res, ran = _run_parity(sandbox)

    assert ran == ["python/geobrix/test/pyrx/test_parity_brand_new.py"]


# ----------------------------------------------------------------- parity: --path


def test_path_is_repeatable_and_runs_exactly_those_suites_in_order(sandbox):
    _parity_sandbox(sandbox, TEST_ROOT)
    a = "python/geobrix/test/pyvx/test_parity_mvt.py"
    b = "python/geobrix/test/pygx/test_parity_bng.py"

    res, ran = _run_parity(sandbox, "--path", a, "--path", b)

    assert res.returncode == 0, res.output
    assert ran == [a, b]


def test_a_path_directory_runs_the_parity_suites_under_it(sandbox):
    _parity_sandbox(sandbox, TEST_ROOT)

    res, ran = _run_parity(sandbox, "--path", "python/geobrix/test/pyvx/")

    assert ran and all(r.startswith("python/geobrix/test/pyvx/") for r in ran)
    assert "python/geobrix/test/pyvx/test_parity_mvt.py" in ran


def test_a_repeated_path_runs_once(sandbox):
    _parity_sandbox(sandbox, TEST_ROOT)
    a = "python/geobrix/test/pyvx/test_parity_mvt.py"

    res, ran = _run_parity(sandbox, "--path", a, "--path", a)

    assert ran == [a]


def test_a_missing_path_fails_the_run(sandbox):
    _parity_sandbox(sandbox, TEST_ROOT)

    res, ran = _run_parity(sandbox, "--path", "python/geobrix/test/pyvx/test_nope.py")

    assert res.returncode == 1 and "No such suite file" in res.output and ran == []


# --------------------------------------------------- python-docs: single-file isolation


def _docs_run_cmd(sandbox, *args):
    sandbox.run("gbx-test-python-docs.sh", "--skip-build", *args)
    return [c[-1] for c in sandbox.execs() if "pytest" in c[-1]][-1]


@pytest.mark.parametrize(
    "args, doc_dir",
    [
        (["--path", "api/test_rasterx_functions_sql.py"], "api"),
        (["--test", "api/test_rasterx_functions_sql.py::test_x"], "api"),
        (["--path", "readers/test_one.py"], "readers"),
    ],
    ids=["path-file", "test-nodeid", "other-dir"],
)
def test_a_single_file_target_gets_its_directory_on_pythonpath(sandbox, args, doc_dir):
    run_cmd = _docs_run_cmd(sandbox, *args)

    base = "/root/geobrix/docs/tests/python"
    # The sibling example modules (`from _fixtures import ...`) resolve as they do when the
    # whole directory is collected: the file's directory first, then its parent.
    assert f"export PYTHONPATH={base}/{doc_dir}:{base}" in run_cmd


@pytest.mark.parametrize(
    "args",
    [["--suite", "api"], ["--path", "api/"], []],
    ids=["suite", "directory", "default"],
)
def test_a_directory_target_leaves_pythonpath_alone(sandbox, args):
    assert "PYTHONPATH" not in _docs_run_cmd(sandbox, *args)


# -------------------------------------------------------------- scala: non-ASCII paths


def test_scala_runs_pass_a_utf8_locale_to_the_container(sandbox):
    sandbox.run("gbx-test-scala.sh", "--suite", "com.databricks.labs.gbx.util.*")
    sandbox.run("gbx-test-scala.sh", "--by-package")

    maven = [
        (c, e)
        for c, e in zip(sandbox.execs(), sandbox.exec_envs())
        if "mvn test" in c[-1]
    ]
    assert len(maven) == 1 + 6  # the single run, then one per package
    # The JVM derives sun.jnu.encoding from the locale; without one, non-ASCII file names
    # cannot be encoded and the tests that use them are cancelled.
    assert all(e.get("LC_ALL") == "C.UTF-8" for _, e in maven), [e for _, e in maven]
