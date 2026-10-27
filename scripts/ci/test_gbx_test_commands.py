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


def test_the_default_run_covers_the_cross_tier_suites_that_need_the_jar(sandbox):
    _parity_sandbox(sandbox, TEST_ROOT)
    cross_tier = {_repo_rel(p) for p in TEST_ROOT.rglob("*_cross_tier.py")}
    assert len(cross_tier) >= 5  # netcdf, chm, binpoints, binpoints_agg, isoband

    res, ran = _run_parity(sandbox)

    assert cross_tier - set(ran) == set()


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
        for pattern in ("test_parity*.py", "test_*_parity.py", "*_cross_tier.py")
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


# ------------------------------------- light-tier tests run natively on the host (no container)


def _fake_venv(sandbox):
    """A `.venv-pyrx` whose python records the PySpark worker settings and its arguments."""
    bin_dir = sandbox.root / ".venv-pyrx" / "bin"
    bin_dir.mkdir(parents=True)
    log = sandbox.root / "venv-python.log"
    python = bin_dir / "python"
    python.write_text(
        "#!/bin/sh\n"
        f'printf "%s|%s|%s\\n" "$PYSPARK_PYTHON" "$PYSPARK_DRIVER_PYTHON" "$*" >> "{log}"\n'
        '[ "$1" = "-" ] && cat > /dev/null\n'
        "exit 0\n"
    )
    python.chmod(0o755)
    (bin_dir / "activate").write_text(f'export PATH="{bin_dir}:$PATH"\n')
    return python, log


def _last_python_call(log):
    workers, driver, argv = log.read_text().splitlines()[-1].split("|", 2)
    return workers, driver, argv


def test_the_host_run_starts_spark_workers_on_the_venv_python(sandbox):
    python, log = _fake_venv(sandbox)

    # A caller's shell may export another interpreter; workers on it fail the version check.
    res = sandbox.run(
        "gbx-test-pyrx.sh",
        "--path",
        "python/geobrix/test/zarr/test_write.py",
        PYSPARK_PYTHON="/usr/bin/python3",
    )

    assert res.returncode == 0, res.output
    workers, driver, argv = _last_python_call(log)
    assert Path(workers).resolve() == Path(driver).resolve() == python.resolve()
    assert "-m pytest python/geobrix/test/zarr/test_write.py" in argv
    assert sandbox.execs() == []  # the host run never reaches the container


def _light_sandbox(sandbox):
    python, log = _fake_venv(sandbox)
    test_dir = sandbox.root / "python" / "geobrix" / "test"
    test_dir.mkdir(parents=True)
    (test_dir / "conftest.py").write_text((TEST_ROOT / "conftest.py").read_text())
    return log


def _light_dirs_conftest_lists():
    block = re.search(
        r"_LIGHT_TEST_DIRS = \[(.*?)\]", (TEST_ROOT / "conftest.py").read_text(), re.S
    )
    return re.findall(r'"(\w+)"', block.group(1))


def test_the_light_command_runs_every_light_dir_when_no_path_is_given(sandbox):
    log = _light_sandbox(sandbox)

    res = sandbox.run("gbx-test-light.sh")

    assert res.returncode == 0, res.output
    _, _, argv = _last_python_call(log)
    ran = [a.rstrip("/") for a in argv.split() if a.startswith("python/geobrix/test/")]
    dirs = _light_dirs_conftest_lists()
    assert "zarr" in dirs and len(dirs) >= 13
    assert ran == [f"python/geobrix/test/{d}" for d in dirs]


def test_the_light_command_runs_only_the_paths_it_is_given(sandbox):
    log = _light_sandbox(sandbox)

    res = sandbox.run(
        "gbx-test-light.sh",
        "--path",
        "python/geobrix/test/zarr/test_keys.py",
        "--path",
        "python/geobrix/test/zarr/test_layout.py",
    )

    assert res.returncode == 0, res.output
    _, _, argv = _last_python_call(log)
    ran = [a for a in argv.split() if a.startswith("python/geobrix/test/")]
    assert ran == [
        "python/geobrix/test/zarr/test_keys.py",
        "python/geobrix/test/zarr/test_layout.py",
    ]
    assert sandbox.execs() == []


def _fake_uv(sandbox):
    log = sandbox.root / "uv.log"
    uv = sandbox.bin / "uv"
    uv.write_text(f'#!/bin/sh\necho "$*" >> "{log}"\n')
    uv.chmod(0o755)
    return log


def test_venv_sync_refreshes_an_existing_venv_with_the_zarr_extra(sandbox):
    # `uv venv` refuses an existing venv, so the sync must not call it when one is there.
    _fake_venv(sandbox)
    uv_log = _fake_uv(sandbox)

    res = sandbox.run("gbx-venv-sync.sh")

    assert res.returncode == 0, res.output
    calls = uv_log.read_text().splitlines()
    assert not [c for c in calls if c.startswith("venv")], calls
    (install,) = [c for c in calls if c.startswith("pip install")]
    assert "zarr_env6" in install and "light_env6" in install
    # The `test` extra pins mapbox-vector-tile<2.2, which light_env6 (>=2.2) cannot satisfy.
    assert "test]" not in install and ",test" not in install
    # The Delta 4.0.x test jars run on Spark 4.0.x only; the `test` extra leaves pyspark open.
    assert "pyspark>=4.0,<4.1" in install


def test_venv_sync_creates_the_venv_when_it_is_missing(sandbox):
    uv_log = _fake_uv(sandbox)

    # The run fails at the isolation check (the fake makes no venv); only the uv calls matter.
    sandbox.run("gbx-venv-sync.sh")

    calls = uv_log.read_text().splitlines()
    assert calls[0].startswith("venv ") and "--python 3.12" in calls[0], calls
