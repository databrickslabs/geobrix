"""Tests for fixes to the gbx:test:* commands: parity discovery, doc-test isolation, locale.

Each command runs from a scratch copy of scripts/commands/ against a fake `docker` (see
conftest.py's ContainerSandbox): nothing reaches a daemon, and the work a command would send
into the container (pytest, Maven) is recorded instead of run.
"""

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
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


def test_a_dot_slash_path_still_honours_the_documented_exclusions(sandbox):
    _parity_sandbox(sandbox, TEST_ROOT)

    res, ran = _run_parity(sandbox, "--path", "./python/geobrix/test/ds/")

    assert ran, res.output
    assert not set(ran) & EXCLUDED, "the exclusions were defeated by the ./ prefix"
    assert all(not r.startswith("./") for r in ran)


def test_a_parity_skip_names_missing_sample_data_when_that_is_why(sandbox):
    # Newly discovered suites skip wholesale when the container has no sample-data mounts; the
    # failure said only "0 tests passed", not why. The command runs pytest with --color=yes, so
    # the report it reads back is COLOURED (ESC[33mSKIPPED ESC[0m ...), and a '^SKIPPED' grep on
    # it matches nothing. So this test runs the REAL command string the script sends to the
    # container (the fake docker executes it, on a coloured report the test wrote): stripping
    # the colour anywhere but inside that command fails here.
    _parity_sandbox(sandbox, TEST_ROOT)
    path = ("--path", "python/geobrix/test/ds/test_vector_parity.py")
    report = sandbox.root / "target" / "parity-test_vector_parity.txt"
    report.parent.mkdir()
    yellow, bold, reset = "\x1b[33m", "\x1b[1m", "\x1b[0m"

    def run(reason):
        report.write_text(
            "python/geobrix/test/ds/test_vector_parity.py::test_a "
            f"{yellow}SKIPPED{reset}{yellow} (reason){reset}\n"
            f"{yellow}=== short test summary info ==={reset}\n"
            f"{yellow}SKIPPED{reset} [2] python/geobrix/test/ds/test_vector_parity.py:67: {reason}\n"
            f"{yellow}=============== {bold}2 skipped{reset}{yellow} in 1.00s ==============={reset}\n"
        )
        return sandbox.run(
            "gbx-test-parity.sh",
            "--skip-build",
            *path,
            FAKE_DOCKER_RUN_MATCHING="tail -6",
        )

    res = run("sample not mounted: /Volumes/x")
    assert res.returncode == 1
    assert "sample data missing" in res.output and "sample-data mounts" in res.output

    res = run("could not import 'mapbox_vector_tile' (sample of one)")
    assert res.returncode == 1 and "sample data missing" not in res.output
    assert "2 skipped" in res.output  # it still read the (coloured) summary


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


@pytest.mark.parametrize(
    "command, args",
    [
        ("gbx-test-scala-docs.sh", []),
        ("gbx-coverage-scala.sh", []),
        ("gbx-coverage-scala.sh", ["--by-package"]),
        ("gbx-coverage-scala-package.sh", ["rasterx"]),
        ("gbx-coverage-scala-docs.sh", []),
        ("gbx-bench-heavyweight.sh", ["--functions", "gbx_rst_x"]),
    ],
    ids=[
        "scala-docs",
        "coverage",
        "coverage-by-package",
        "coverage-package",
        "coverage-docs",
        "bench",
    ],
)
def test_every_maven_test_run_gets_the_utf8_locale(sandbox, command, args):
    # gbx:test:scala had it; the other Maven test runs cancelled the non-ASCII file-name tests
    # (NodeFilePathUtilTest) for want of a locale, so coverage and the suite disagreed.
    sandbox.run(command, *args)

    maven = [
        (c, e)
        for c, e in zip(sandbox.execs(), sandbox.exec_envs())
        if re.search(
            r"\bmvn (clean )?(scoverage:(test|integration-check)|test)\b", c[-1]
        )
    ]
    assert maven, sandbox.execs()
    for c, e in maven:
        assert e.get("LC_ALL") == "C.UTF-8" or "export LC_ALL=C.UTF-8" in c[-1], c[-1]


# ------------------------------------- light-tier tests run natively on the host (no container)


def _fake_venv(sandbox, venv=None):
    """A venv (default `.venv-pyrx`) whose python records the PySpark worker settings and args."""
    venv = venv or sandbox.root / ".venv-pyrx"
    bin_dir = venv / "bin"
    bin_dir.mkdir(parents=True)
    log = venv / "calls.log"
    python = bin_dir / "python"
    python.write_text(
        "#!/bin/sh\n"
        f'printf "%s|%s|%s\\n" "$PYSPARK_PYTHON" "$PYSPARK_DRIVER_PYTHON" "$*" >> "{log}"\n'
        f'printf "%s\\n" "$TZ" >> "{log}.tz"\n'
        f'printf "%s\\n" "$@" > "{log}.argv"\n'  # one word per line: the last call's arguments
        '[ "$1" = "-" ] && cat > /dev/null\n'
        'exit "${FAKE_PYTHON_RC:-0}"\n'
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


def _last_m(words):
    """The word after the last `-m` (the first is `python -m pytest`)."""
    return words[len(words) - words[::-1].index("-m")]


def test_markers_narrow_the_default_and_pytest_addopts_reaches_pytest(sandbox):
    # `-m "not delta"` must work through the commands, as one pytest argument, without
    # re-admitting the integration tests the default excludes.
    python, log = _fake_venv(sandbox)
    argv = Path(str(log) + ".argv")
    path = "python/geobrix/test/zarr/test_keys.py"

    sandbox.run("gbx-test-pyrx.sh", "--path", path, "--markers", "not delta")
    words = argv.read_text().splitlines()
    assert _last_m(words) == "((not integration) and (not perf)) and (not delta)"

    sandbox.run("gbx-test-light.sh", "--path", path, "--markers", "slow or fast")
    words = argv.read_text().splitlines()
    assert _last_m(words) == "((not integration) and (not perf)) and (slow or fast)"

    sandbox.run("gbx-test-pyrx.sh", "--path", path)  # no --markers: the unit loop skips perf
    words = argv.read_text().splitlines()
    assert _last_m(words) == "(not integration) and (not perf)"

    sandbox.run("gbx-test-pyrx.sh", "--path", path, "--perf")  # opt in to the perf cases
    words = argv.read_text().splitlines()
    assert _last_m(words) == "not integration"

    sandbox.run("gbx-test-pyrx.sh", "--path", path, "--with-integration")  # opt in to the network tests
    words = argv.read_text().splitlines()
    assert _last_m(words) == "not perf"

    # Both opt-ins leave nothing to exclude: pytest gets an empty -m, which selects everything.
    sandbox.run("gbx-test-pyrx.sh", "--path", path, "--perf", "--with-integration")
    words = argv.read_text().splitlines()
    assert _last_m(words) == ""

    # --docker: the marker is still one word in the container's command, and the caller's
    # PYTEST_ADDOPTS (--durations, --junitxml) is passed into the container.
    sandbox.run(
        "gbx-test-pyrx.sh",
        *("--docker", "--path", path, "--markers", "not delta"),
        PYTEST_ADDOPTS="--durations=5",
    )
    command, env = [
        (c, e)
        for c, e in zip(sandbox.execs(), sandbox.exec_envs())
        if "pytest" in c[-1]
    ][-1]
    words = shlex.split(command[-1])
    assert _last_m(words) == "((not integration) and (not perf)) and (not delta)"
    assert env["PYTEST_ADDOPTS"] == "--durations=5"


def test_a_native_payload_reaches_the_container_pytest_runs(sandbox):
    # GBX_NATIVE_ROOT names a payload under the checkout (relative or absolute): the container's pytest gets its
    # /root/geobrix path, on the light run and on the parity run; unset, nothing is passed.
    path = "python/geobrix/test/zarr/test_keys.py"

    def pytest_env():
        return [e for c, e in zip(sandbox.execs(), sandbox.exec_envs()) if "pytest" in c[-1]][-1]

    sandbox.run("gbx-test-pyrx.sh", "--docker", "--path", path, GBX_NATIVE_ROOT="build/env-wheel/env6/payload")
    assert pytest_env()["GBX_NATIVE_ROOT"] == "/root/geobrix/build/env-wheel/env6/payload"
    sandbox.run("gbx-test-pyrx.sh", "--docker", "--path", path)
    assert "GBX_NATIVE_ROOT" not in pytest_env()
    tree = sandbox.root / "t"
    tree.mkdir()
    (tree / "test_x_cross_tier.py").write_text("def test_x():\n    pass\n")
    _parity_sandbox(sandbox, tree)
    payload = str(sandbox.root / "build" / "env-wheel" / "env6" / "payload")
    res = sandbox.run(
        "gbx-test-parity.sh", "--skip-build", GBX_NATIVE_ROOT=payload,
        FAKE_DOCKER_EXEC_STDOUT="== 3 passed in 1.00s ==",
    )
    assert res.returncode == 0, res.output
    assert pytest_env()["GBX_NATIVE_ROOT"] == "/root/geobrix/build/env-wheel/env6/payload"


def test_a_native_payload_reaches_the_scala_test_jvm(sandbox):
    # The Scala run passes GBX_NATIVE_ROOT (as its /root/geobrix path) beside the locale; unset, nothing is passed.
    def maven_env():
        return [e for c, e in zip(sandbox.execs(), sandbox.exec_envs()) if "mvn test" in c[-1]][-1]

    sandbox.run("gbx-test-scala.sh", "--suite", "x.Y", GBX_NATIVE_ROOT="build/env-wheel/env6/payload")
    env = maven_env()
    assert env["GBX_NATIVE_ROOT"] == "/root/geobrix/build/env-wheel/env6/payload" and env["LC_ALL"] == "C.UTF-8"
    sandbox.run("gbx-test-scala.sh", "--suite", "x.Y")
    assert "GBX_NATIVE_ROOT" not in maven_env()
    # the per-package loop passes it to every Maven run
    sandbox.run("gbx-test-scala.sh", "--by-package", GBX_NATIVE_ROOT="build/env-wheel/env6/payload")
    by_package = [e for c, e in zip(sandbox.execs(), sandbox.exec_envs()) if "mvn test" in c[-1]][-6:]
    assert [e.get("GBX_NATIVE_ROOT") for e in by_package] == ["/root/geobrix/build/env-wheel/env6/payload"] * 6


def _markers_of_calls(log):
    """The `-m` expression of every pytest process the fake venv python was started for, in order."""
    calls = [line.split("|", 2)[2] for line in log.read_text().splitlines()]
    return [c.rsplit(" -m ", 1)[1] for c in calls]  # the log joins the words with spaces


def test_the_spark_connect_tests_run_in_a_pytest_process_of_their_own(sandbox):
    # A test that uses the local Connect session cannot share a process with classic-session tests. The conftest marks
    # such tests `connect`; a run whose paths hold any does the others first and the Connect tests second.
    _, log = _fake_venv(sandbox)
    test_dir = sandbox.root / "python" / "geobrix" / "test"
    (test_dir / "manager").mkdir(parents=True)
    (test_dir / "manager" / "test_with_connect.py").write_text(
        "def test_x(spark_connect_local):\n    pass\n"
    )
    (test_dir / "zarr").mkdir()
    (test_dir / "zarr" / "test_plain.py").write_text("def test_y(spark):\n    pass\n")

    res = sandbox.run(
        "gbx-test-pyrx.sh",
        *("--path", "python/geobrix/test/manager/test_with_connect.py"),
        *("--path", "python/geobrix/test/zarr/test_plain.py"),
    )
    assert res.returncode == 0, res.output
    assert _markers_of_calls(log) == [
        "((not integration) and (not perf)) and (not connect)",
        "((not integration) and (not perf)) and (connect)",
    ]
    # The second process collects only the file that uses the fixture: importing the others first would bind the
    # light UDFs to a classic session before the Connect one exists.
    first, second = [c.split("|", 2)[2] for c in log.read_text().splitlines()]
    assert "test_plain.py" in first and "test_with_connect.py" in first
    assert "test_with_connect.py" in second and "test_plain.py" not in second

    log.unlink()  # paths with no Connect test: one process, the markers as before
    res = sandbox.run(
        "gbx-test-pyrx.sh", "--path", "python/geobrix/test/zarr/test_plain.py"
    )
    assert res.returncode == 0, res.output
    assert _markers_of_calls(log) == ["(not integration) and (not perf)"]

    # Only Connect tests selected: the first process collects nothing (5), which is not a failure while the second ran.
    log.unlink()
    res = sandbox.run(
        "gbx-test-pyrx.sh",
        *("--path", "python/geobrix/test/manager/test_with_connect.py"),
        FAKE_PYTHON_RC="5",
    )
    assert res.returncode == 5  # both processes collected nothing here


def test_the_parity_run_leaves_the_spark_connect_tests_out(sandbox):
    # The container has no gRPC client and a parity file's process holds a classic session: a Connect test there can
    # only skip, and a skip fails the gate. Connect tests run in gbx:test:pyrx.
    tree = sandbox.root / "t"  # a one-file tree is enough to reach the pytest line
    tree.mkdir()
    (tree / "test_x_cross_tier.py").write_text("def test_x():\n    pass\n")
    _parity_sandbox(sandbox, tree)
    res, ran = _run_parity(sandbox)
    assert res.returncode == 0, res.output
    pytest_lines = [c[-1] for c in sandbox.execs() if PARITY_RE.search(c[-1])]
    assert pytest_lines and all("-m 'not connect'" in c for c in pytest_lines)


def test_a_run_that_selects_nothing_says_which_cases_are_skipped(sandbox):
    # A file whose only tests are perf- or integration-marked exits 5 (no tests collected) with no other sign why.
    _fake_venv(sandbox)
    path = "python/geobrix/test/pygx/test_geomk_golden.py"

    res = sandbox.run("gbx-test-pyrx.sh", "--path", path, FAKE_PYTHON_RC="5")
    assert res.returncode == 5 and "--perf" in res.output.split("exit code: 5")[-1]

    res = sandbox.run("gbx-test-pyrx.sh", "--path", path, "--perf", FAKE_PYTHON_RC="5")
    assert res.returncode == 5 and "--perf" not in res.output.split("exit code: 5")[-1]
    assert "--with-integration" in res.output.split("exit code: 5")[-1]  # integration still skipped

    res = sandbox.run(
        "gbx-test-pyrx.sh",
        *("--path", path, "--perf", "--with-integration"),
        FAKE_PYTHON_RC="5",
    )
    hint = res.output.split("exit code: 5")[-1]
    assert res.returncode == 5 and "--perf" not in hint and "--with-integration" not in hint


def test_the_host_run_pins_utc_as_the_container_and_ci_do(sandbox):
    # The container and the CI runners are UTC; a developer's local zone makes the timestamp
    # tests (e.g. test/zarr/test_read.py) fail natively.
    python, log = _fake_venv(sandbox)

    res = sandbox.run(
        "gbx-test-pyrx.sh",
        "--path",
        "python/geobrix/test/zarr/test_read.py",
        TZ="America/New_York",
    )

    assert res.returncode == 0, res.output
    assert Path(str(log) + ".tz").read_text().split() == ["UTC"]

    # GBX_TEST_TZ is the one way to another zone; the caller's TZ is still ignored.
    res = sandbox.run(
        "gbx-test-pyrx.sh",
        "--path",
        "python/geobrix/test/zarr/test_read.py",
        TZ="Asia/Tokyo",
        GBX_TEST_TZ="America/New_York",
    )

    assert res.returncode == 0, res.output
    # (the fake python appends one line per run)
    assert Path(str(log) + ".tz").read_text().split() == ["UTC", "America/New_York"]


CI_ACTION = REPO_ROOT / ".github" / "actions" / "pyrx_build" / "action.yml"


def _ci_light_dirs():
    """The dirs of the light CI phase's pytest line, in the order CI runs them."""
    line = next(
        x
        for x in CI_ACTION.read_text().splitlines()
        if x.strip().startswith("pytest test/")
    )
    return re.findall(r"\btest/(\w+)", line)


def _light_sandbox(sandbox):
    python, log = _fake_venv(sandbox)
    test_dir = sandbox.root / "python" / "geobrix" / "test"
    test_dir.mkdir(parents=True)
    (test_dir / "conftest.py").write_text((TEST_ROOT / "conftest.py").read_text())
    action = sandbox.root / ".github" / "actions" / "pyrx_build" / "action.yml"
    action.parent.mkdir(parents=True)
    action.write_text(CI_ACTION.read_text())
    return log


def _light_dirs_conftest_lists():
    block = re.search(
        r"_LIGHT_TEST_DIRS = \[(.*?)\]", (TEST_ROOT / "conftest.py").read_text(), re.S
    )
    return re.findall(r'"(\w+)"', block.group(1))


def test_the_light_command_runs_every_light_dir_in_the_ci_order(sandbox):
    log = _light_sandbox(sandbox)

    res = sandbox.run("gbx-test-light.sh")

    assert res.returncode == 0, res.output
    _, _, argv = _last_python_call(log)
    ran = [a.rstrip("/") for a in argv.split() if a.startswith("python/geobrix/test/")]
    ci = _ci_light_dirs()
    assert ran == [f"python/geobrix/test/{d}" for d in ci]
    # The order is deliberate: all dirs share one process, and pyvx registers the PROJ grids
    # that bench's raster builds on worker threads rely on (see the CI action's comment).
    assert ci.index("pyvx") < ci.index("bench")
    # ... and it covers exactly the dirs the heavy phase ignores.
    assert sorted(ci) == sorted(_light_dirs_conftest_lists())


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


def _fake_lock(sandbox):
    lock = sandbox.root / "python" / "geobrix" / "requirements-light-env6-ci.txt"
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text("pyspark==4.0.0 \\\n    --hash=sha256:aa\n")
    return lock


def test_venv_sync_installs_the_ci_lock_into_an_existing_venv(sandbox):
    # `uv venv` refuses an existing venv, so the sync must not call it when one is there.
    _fake_venv(sandbox)
    uv_log = _fake_uv(sandbox)
    lock = _fake_lock(sandbox)

    res = sandbox.run("gbx-venv-sync.sh")

    assert res.returncode == 0, res.output
    calls = uv_log.read_text().splitlines()
    assert not [c for c in calls if c.startswith("venv")], calls
    # What the light CI phase installs: the hash-pinned lock (CI's exact versions, so the
    # results match CI; an extras install floats and keeps a stale venv's versions), then the
    # package alone. The `test` extra is never used: it pins mapbox-vector-tile<2.2, which
    # the env6 lock's 2.2 cannot satisfy.
    installs = [c for c in calls if c.startswith("pip install")]
    assert len(installs) == 2, calls
    assert "--require-hashes" in installs[0] and f"-r {lock}" in installs[0]
    assert "--no-deps" in installs[1] and "-e ./python/geobrix" in installs[1]
    assert "[" not in installs[1]


def test_venv_sync_connect_adds_the_client_deps_for_the_venvs_own_pyspark(sandbox):
    # Opt-in: the CI lock has no gRPC client, so the local Connect tier needs `pyspark[connect]`
    # at the venv's pyspark version, constrained to what is installed (no locked version moves).
    bin_dir = sandbox.root / ".venv-pyrx" / "bin"
    bin_dir.mkdir(parents=True)
    python = bin_dir / "python"
    python.write_text(
        "#!/bin/sh\n"
        '[ "$1" = "-c" ] && case "$2" in "import pyspark"*) echo 4.0.0;; esac\n'
        '[ "$1" = "-" ] && cat > /dev/null\n'
        "exit 0\n"
    )
    python.chmod(0o755)
    uv_log = _fake_uv(sandbox)
    _fake_lock(sandbox)

    sandbox.run("gbx-venv-sync.sh")
    plain = uv_log.read_text().splitlines()
    assert not [
        c for c in plain if "pyspark[connect]" in c or c.startswith("pip freeze")
    ]

    uv_log.unlink()
    res = sandbox.run("gbx-venv-sync.sh", "--connect")

    assert res.returncode == 0, res.output
    calls = uv_log.read_text().splitlines()
    assert [c for c in calls if c.startswith("pip freeze")], calls
    extra = [c for c in calls if "pyspark[connect]" in c]
    assert len(extra) == 1 and "pyspark[connect]==4.0.0 zstandard" in extra[0], calls
    assert " -c " in extra[0] and "--require-hashes" not in extra[0]
    assert calls.index(extra[0]) > max(
        i for i, c in enumerate(calls) if "--require-hashes" in c
    )  # after the lock install


def test_python_option_on_an_existing_venv_fails_instead_of_being_ignored(sandbox):
    # `uv venv` is skipped for an existing venv, so --python used to change nothing, silently.
    bin_dir = sandbox.root / ".venv-pyrx" / "bin"
    bin_dir.mkdir(parents=True)
    python = bin_dir / "python"
    python.write_text('#!/bin/sh\n[ "$1" = "-c" ] && echo 3.12.11\nexit 0\n')
    python.chmod(0o755)
    uv_log = _fake_uv(sandbox)
    _fake_lock(sandbox)

    res = sandbox.run("gbx-venv-sync.sh", "--python", "3.11")

    assert (
        res.returncode == 1 and "--recreate" in res.output and "3.12.11" in res.output
    )
    assert not uv_log.exists()  # nothing was installed
    sandbox.run("gbx-venv-sync.sh", "--python", "3.12")  # the venv's own minor is fine
    assert uv_log.exists()


def test_venv_sync_creates_the_venv_when_it_is_missing(sandbox):
    uv_log = _fake_uv(sandbox)
    _fake_lock(sandbox)

    # The run fails at the isolation check (the fake makes no venv); only the uv calls matter.
    sandbox.run("gbx-venv-sync.sh")

    calls = uv_log.read_text().splitlines()
    assert calls[0].startswith("venv ") and "--python 3.12" in calls[0], calls


def test_venv_sync_refuses_to_run_without_the_lock(sandbox):
    _fake_venv(sandbox)
    uv_log = _fake_uv(sandbox)

    res = sandbox.run("gbx-venv-sync.sh")

    assert res.returncode == 1 and "requirements-light-env6-ci.txt" in res.output
    assert not uv_log.exists()  # nothing was installed from an unpinned resolve


def test_the_venv_location_can_be_moved(sandbox, tmp_path):
    # So a new lock can be tried without disturbing a venv another test run is using.
    _, default_log = _fake_venv(sandbox)
    python, log = _fake_venv(sandbox, tmp_path / "other-venv")

    res = sandbox.run(
        "gbx-test-pyrx.sh",
        "--path",
        "python/geobrix/test/zarr/test_keys.py",
        GBX_PYRX_VENV=str(tmp_path / "other-venv"),
    )

    assert res.returncode == 0, res.output
    workers, _, _ = _last_python_call(log)
    assert Path(workers).resolve() == python.resolve()
    assert not default_log.exists()  # the default venv was not used


@pytest.mark.parametrize(
    "command, option",
    [
        ("gbx-test-light.sh", "--path"),
        ("gbx-test-light.sh", "--log"),
        ("gbx-test-pyrx.sh", "--path"),
        ("gbx-test-pyrx.sh", "--log"),
        ("gbx-venv-sync.sh", "--python"),
        ("gbx-venv-sync.sh", "--log"),
    ],
)
def test_an_option_without_its_value_is_refused_not_looped_on(sandbox, command, option):
    proc = sandbox.popen(command, option)
    try:
        out, _ = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        pytest.fail(f"{command} {option} with no value never exited")

    assert proc.returncode == 2 and option in out, out


# ------------------- workspace launchers: a Python that can import databricks-sdk (host)


# What the launchers' interpreter is asked: "is it Python 3.11+?" (version_info) and "can it
# import databricks.sdk?". The fakes answer the first with `new` and the second with `sdk`.
SDK_SPEC = re.search(
    r"GBX_DATABRICKS_SDK_SPEC:-([^}]+)\}",
    (REPO_ROOT / "scripts" / "commands" / "common.sh").read_text(),
).group(1)


def _venv_python(sandbox, *, sdk, venv=None, new=True):
    """A venv (default `.venv-pyrx`) whose python can or cannot import databricks.sdk.

    The CI-lock venv has no SDK: `python -c "import databricks.sdk"` fails there. Any other
    call (a launcher run) is recorded in calls.log next to the interpreter. `new=False` is a
    Python older than 3.11.
    """
    venv = venv or sandbox.root / ".venv-pyrx"
    (venv / "bin").mkdir(parents=True)
    python = venv / "bin" / "python"
    python.write_text(
        '#!/bin/sh\nif [ "$1" = "-c" ]; then case "$2" in *version_info*) exit %d;; esac; exit %d; fi\n'
        'echo "$*" >> "%s"\nexit 0\n'
        % (0 if new else 1, 0 if sdk else 1, venv / "calls.log")
    )
    python.chmod(0o755)
    return python, venv / "calls.log"


def _fake_python3(directory, *, sdk, new=True):
    """A `python3` that can or cannot import databricks.sdk; any other call is recorded."""
    directory.mkdir(parents=True, exist_ok=True)
    python3, log = directory / "python3", directory / "python3.log"
    python3.write_text(
        '#!/bin/sh\nif [ "$1" = "-c" ]; then case "$2" in *version_info*) exit %d;; esac; exit %d; fi\n'
        'echo "$*" >> "%s"\nexit 0\n' % (0 if new else 1, 0 if sdk else 1, log)
    )
    python3.chmod(0o755)
    return log


def _run_sdk_helper(sandbox, *packages, **env):
    """Run gbx_python_with_sdk (common.sh); returns the result and the chosen command's words."""
    res = subprocess.run(
        [
            "bash",
            "-c",
            f'PROJECT_ROOT="{sandbox.root}"; source "{sandbox.commands}/common.sh"; '
            f'gbx_python_with_sdk {" ".join(packages)} || exit 1; '
            'printf "%s\\n" "${GBX_SDK_PYTHON[@]}"',
        ],
        env=sandbox.env(**env),
        capture_output=True,
        text=True,
        timeout=60,
    )
    return res, res.stdout.split("\n")[:-1]


def _sdk_python(sandbox, *packages, **env):
    res, words = _run_sdk_helper(sandbox, *packages, **env)
    assert res.returncode == 0, res.stderr
    return words


def test_the_sdk_python_is_the_venv_only_when_the_venv_can_import_the_sdk(
    sandbox, tmp_path
):
    # With the SDK: the venv as it is. Without it (the CI-lock venv): uv layers the SDK, and any
    # extra package, over the venv's own interpreter, so the venv's packages stay importable.
    # No venv: uv on 3.12. No uv: system python3, if it can import the SDK.
    # The uv cases use the sandbox's fake uv: a CI runner may have no uv on PATH.
    _fake_uv(sandbox)
    uv = ["uv", "run", "--no-project", "--python"]
    with_sdk, _ = _venv_python(sandbox, sdk=True)
    assert _sdk_python(sandbox) == [str(with_sdk)]
    assert _sdk_python(sandbox, "build") == [
        str(with_sdk)
    ]  # extras are the venv's business

    other, _ = _venv_python(sandbox, sdk=False, venv=tmp_path / "bare")
    bare = {"GBX_PYRX_VENV": str(tmp_path / "bare")}
    assert _sdk_python(sandbox, **bare) == uv + [
        str(other),
        *("--with", SDK_SPEC, "python"),
    ]
    assert _sdk_python(sandbox, "build", **bare) == uv + [
        str(other),
        *("--with", SDK_SPEC, "--with", "build", "python"),
    ]

    nowhere = {"GBX_PYRX_VENV": str(tmp_path / "nowhere")}
    assert _sdk_python(sandbox, "build", **nowhere) == uv + [
        "3.12",
        *("--with", SDK_SPEC, "--with", "build", "python"),
    ]

    _fake_python3(tmp_path / "py3", sdk=True)
    no_uv = {**nowhere, "PATH": f"{tmp_path / 'py3'}:/usr/bin:/bin"}
    assert _sdk_python(sandbox, **no_uv) == ["python3"]


def test_without_uv_or_an_sdk_the_helper_stops_with_one_hint(sandbox, tmp_path):
    # A bare traceback ("No module named databricks") told the user nothing about the fix.
    _fake_python3(tmp_path / "py3", sdk=False)
    nowhere = str(tmp_path / "nowhere")

    res, words = _run_sdk_helper(
        sandbox, GBX_PYRX_VENV=nowhere, PATH=f"{tmp_path / 'py3'}:/usr/bin:/bin"
    )

    assert res.returncode == 1 and words == []
    assert "uv is not installed" in res.stderr and "Install uv" in res.stderr
    assert "pip install databricks-sdk" in res.stderr
    assert "Traceback" not in res.stderr


def test_the_sdk_is_pinned_and_an_interpreter_older_than_3_11_is_passed_over(
    sandbox, tmp_path
):
    # The serverless runner reads pyproject.toml with tomllib (3.11+), and `uv run --with
    # databricks-sdk` floated to whatever PyPI had that day.
    assert "==" in SDK_SPEC
    _fake_uv(sandbox)
    _venv_python(sandbox, sdk=True, new=False)  # a venv with the SDK, but Python 3.10

    words = _sdk_python(sandbox)

    assert words[:5] == ["uv", "run", "--no-project", "--python", "3.12"], words
    assert (
        SDK_SPEC in words
    )  # not the old venv's interpreter, which tomllib cannot serve

    _fake_python3(
        tmp_path / "py3", sdk=True, new=False
    )  # system python3 3.10 with the SDK
    old = {
        "GBX_PYRX_VENV": str(tmp_path / "nowhere"),
        "PATH": f"{tmp_path / 'py3'}:/usr/bin:/bin",
    }
    res, _ = _run_sdk_helper(sandbox, **old)
    assert res.returncode == 1 and "3.11" in res.stderr


@pytest.mark.parametrize(
    "command, args, env, launcher, extras",
    [
        (
            "gbx-data-stage-jar.sh",
            [],
            {"GBX_BUNDLE_SKIP_JAR": "1"},
            "stage_jar_to_volume",
            [],
        ),
        (
            "gbx-data-stage-wheel.sh",
            [],
            {"GBX_BUNDLE_SKIP_JAR": "1"},
            "stage_wheel_to_volume",
            [
                "build"
            ],  # without a venv it builds with `python -m build` on this interpreter
        ),
        ("gbx-bench-cluster.sh", [], {}, "push_and_run_bench_on_cluster", []),
        ("gbx-test-bundle-databricks.sh", [], {}, "push_and_run_bundle_on_cluster", []),
        (
            "gbx-test-bundle-databricks.sh",
            ["--local"],
            {},
            "test_bundle_on_databricks",
            ["pytest"],  # --local runs the bundle under pytest, which the SDK layer does not carry
        ),
        (
            "gbx-test-primitive-databricks.sh",
            [],
            {},
            "push_and_run_primitive_on_cluster",
            [],
        ),
        (
            "gbx-test-notebooks-serverless.sh",
            ["--notebook", "x.ipynb"],
            {},
            "run_notebooks_serverless",
            [],
        ),
        (
            "gbx-uc-sql.sh",
            ["--file", "x.sql", "--profile", "p", "--warehouse-id", "w"],
            {},
            "uc_sql",
            [],
        ),
    ],
)
def test_the_workspace_launchers_get_the_sdk_when_the_venv_has_none(
    sandbox, command, args, env, launcher, extras
):
    # The venv is the CI light lock and has no databricks-sdk: running a launcher on it died with
    # "No module named databricks.sdk" before anything reached the workspace.
    python, calls = _venv_python(sandbox, sdk=False)
    uv_log = _fake_uv(sandbox)
    config = sandbox.root / "examples" / "tests" / "databricks_cluster_config.env"
    config.parent.mkdir(parents=True)
    config.write_text("")  # gbx-bench-cluster.sh refuses to start without it

    res = sandbox.run(command, *args, **env)

    assert res.returncode == 0, res.output
    words = uv_log.read_text().splitlines()[-1].split()
    assert words[:4] == ["run", "--no-project", "--python", str(python)], words
    packages = ["--with", SDK_SPEC] + [x for p in extras for x in ("--with", p)]
    assert words[4 : 5 + len(packages)] == packages + ["python"], words
    assert any(w.endswith(f"examples/tests/{launcher}.py") for w in words), words
    assert not calls.exists()  # the SDK-less interpreter never ran the launcher itself


def test_a_fresh_checkout_builds_the_wheel_on_an_interpreter_that_has_build(sandbox):
    # No .venv-pyrx: stage_wheel_to_volume.py builds with `[sys.executable, -m, build]`, so the
    # ephemeral uv Python needs `build` next to the SDK or the stage dies on "No module named build".
    uv_log = _fake_uv(sandbox)
    assert not (sandbox.root / ".venv-pyrx").exists()

    res = sandbox.run("gbx-data-stage-wheel.sh", GBX_BUNDLE_SKIP_JAR="1")

    assert res.returncode == 0, res.output
    words = uv_log.read_text().splitlines()[-1].split()
    assert words[:9] == [
        *("run", "--no-project", "--python", "3.12"),
        *("--with", SDK_SPEC, "--with", "build", "python"),
    ], words
    assert words[9].endswith("examples/tests/stage_wheel_to_volume.py"), words


def test_a_local_only_wheel_build_needs_no_sdk_so_it_stays_on_python3(sandbox):
    # GBX_BUNDLE_SKIP_WHEEL_UPLOAD=1 imports no SDK: no uv, no network, and not the venv either.
    _, venv_calls = _venv_python(sandbox, sdk=False)
    uv_log = _fake_uv(sandbox)
    py3_log = _fake_python3(sandbox.bin, sdk=False)

    res = sandbox.run(
        "gbx-data-stage-wheel.sh",
        GBX_BUNDLE_SKIP_JAR="1",
        GBX_BUNDLE_SKIP_WHEEL_UPLOAD="1",
    )

    assert res.returncode == 0, res.output
    assert py3_log.read_text().split() == ["examples/tests/stage_wheel_to_volume.py"]
    assert not uv_log.exists() and not venv_calls.exists()


_FAILING_SDK = """
import os
class NotFound(Exception):
    pass
class _Files:
    def create_directory(self, path):
        pass
    def upload(self, path, fh, overwrite):
        if os.environ.get("FAKE_UPLOAD_FAILS"):
            raise NotFound("Volume %s does not exist" % path)
        self.size = len(fh.read())
    def get_metadata(self, path):
        return type("M", (), {"content_length": self.size})()
class WorkspaceClient:
    def __init__(self, **kw):
        self.files = _Files()
"""


@pytest.mark.parametrize("log_args", [[], ["--log", "stage.log"]], ids=["no-log", "tee-log"])
def test_stage_wheel_exits_nonzero_when_the_upload_raises(sandbox, tmp_path, log_args):
    # A chain that kept going after "NotFound: Volume ... does not exist" asked whether the command
    # swallows it. The real script runs on a stub SDK whose upload raises; with and without the
    # `| tee` of --log the command's own status is the script's, never tee's.
    stub = tmp_path / "stub"
    (stub / "databricks" / "sdk").mkdir(parents=True)
    (stub / "databricks" / "__init__.py").write_text("")
    (stub / "databricks" / "sdk" / "__init__.py").write_text(_FAILING_SDK)
    python, _ = _venv_python(sandbox, sdk=True)
    python.write_text(
        f'#!/bin/sh\nPYTHONPATH="{stub}" exec "{sys.executable}" "$@"\n'
    )  # a venv python that has the stub SDK
    tests = sandbox.root / "examples" / "tests"
    tests.mkdir(parents=True)
    for name in ("stage_wheel_to_volume.py", "_artifact_volume.py", "_cli.py"):
        shutil.copy(REPO_ROOT / "examples" / "tests" / name, tests / name)
    dist = sandbox.root / "dist" / "env6"
    dist.mkdir(parents=True)
    (dist / "geobrix-0.6.0+env6-py3-none-any.whl").write_bytes(b"wheel")
    args = ["--env", "6", "--profile", "p", *log_args]

    res = sandbox.run("gbx-data-stage-wheel.sh", *args, FAKE_UPLOAD_FAILS="1")

    assert res.returncode != 0, res.output
    assert "NotFound: Volume" in res.output and "does not exist" in res.output, res.output
    assert "Done:" not in res.output

    res = sandbox.run("gbx-data-stage-wheel.sh", *args)  # the same run with a working upload

    assert res.returncode == 0, res.output
    assert "Done: /Volumes/main/default/geobrix_samples/geobrix-0.6.0+env6-py3-none-any.whl" in res.output


def test_stage_wheel_profile_reaches_the_launcher_and_an_unknown_option_is_refused(sandbox):
    # --profile was dropped by a catch-all `*) shift`, so staging used the config file's profile
    # while the caller believed it chose one. It must set the launcher's profile, and anything the
    # command does not know must fail instead of being ignored.
    python3 = sandbox.bin / "python3"
    seen = sandbox.root / "profile.log"
    python3.write_text(
        '#!/bin/sh\necho "${DATABRICKS_CONFIG_PROFILE:-unset}" >> "%s"\nexit 0\n' % seen
    )
    python3.chmod(0o755)
    # a profile the host cannot have, so an ignored --profile cannot pass by luck
    env = dict(
        GBX_BUNDLE_SKIP_JAR="1",
        GBX_BUNDLE_SKIP_WHEEL_UPLOAD="1",
        DATABRICKS_CONFIG_PROFILE="from-env",
    )

    res = sandbox.run("gbx-data-stage-wheel.sh", "--profile", "oauth-fe", **env)

    assert res.returncode == 0, res.output
    assert seen.read_text().split() == ["oauth-fe"]  # --profile wins over the environment

    seen.unlink()
    res = sandbox.run("gbx-data-stage-wheel.sh", **env)

    assert res.returncode == 0, res.output
    assert seen.read_text().split() == ["from-env"]  # without --profile, the environment's

    for args in (["--not-an-option"], ["--profile"], ["--profile", "--no-wait"]):
        seen.unlink(missing_ok=True)
        res = sandbox.run("gbx-data-stage-wheel.sh", *args, **env)

        want = "Unknown option: --not-an-option" if args[0] != "--profile" else (
            "--profile needs a profile name"
        )
        assert res.returncode == 2 and want in res.output, (args, res.output)
        assert not seen.exists(), args  # the launcher never ran


def test_a_launcher_runs_on_the_venv_itself_when_it_has_the_sdk(sandbox):
    python, calls = _venv_python(sandbox, sdk=True)
    uv_log = _fake_uv(sandbox)

    res = sandbox.run("gbx-data-stage-jar.sh", GBX_BUNDLE_SKIP_JAR="1")

    assert res.returncode == 0, res.output
    assert calls.read_text().split() == ["examples/tests/stage_jar_to_volume.py"]
    assert not uv_log.exists()  # nothing was fetched


# ------------------------------------------------------- lint: a --path scope for --fix


def test_lint_path_scopes_the_run_to_the_files_named(sandbox):
    # An unscoped --fix reformats the whole tree, other people's files included.
    geobrix = sandbox.root / "python" / "geobrix"
    (geobrix / "src").mkdir(parents=True)
    (geobrix / "src" / "mine.py").write_text("x = 1\n")
    (geobrix / "test").mkdir()
    sources = sandbox.root / "connectors" / "lakeflow" / "sources"
    sources.mkdir(parents=True)
    (sources / "conn.py").write_text("y = 2\n")

    def lint(*args):
        res = sandbox.run("gbx-lint-python.sh", *args)
        envs = sandbox.exec_envs()
        return res, (envs[-1] if envs else {})

    res, env = lint("--fix", "--path", "python/geobrix/src/mine.py")
    assert res.returncode == 0, res.output
    assert env["LINT_PKG_PATHS"] == "src/mine.py\n" and env["LINT_LAKEFLOW_PATHS"] == ""
    assert env["ISORT_FLAGS"] == "" and env["BLACK_FLAGS"] == ""  # still a fix run

    res, env = lint("--check", "--path", "python/geobrix/test/", "--path", str(sources))
    assert env["LINT_PKG_PATHS"] == "test\n"  # a directory, trailing slash dropped
    assert env["LINT_LAKEFLOW_PATHS"] == "sources\n"
    assert env["BLACK_FLAGS"] == "--check"

    res, env = lint()  # no --path: the CI scopes, unchanged
    assert env["LINT_PKG_PATHS"] == "src\ntest\n"
    assert env["LINT_LAKEFLOW_PATHS"] == "sources\n"

    before = len(sandbox.execs())
    res, _ = lint("--fix", "--path", "scripts/commands/common.sh")
    assert res.returncode == 2 and "outside the lint scopes" in res.output
    assert len(sandbox.execs()) == before  # nothing was run


def test_stage_jar_writes_a_log_when_asked(sandbox):
    # The gbx convention is --log <path> on every command; stage-jar refused it as unknown.
    _fake_uv(sandbox)

    res = sandbox.run(
        "gbx-data-stage-jar.sh", "--log", "stage.log", GBX_BUNDLE_SKIP_JAR="1"
    )

    assert res.returncode == 0, res.output
    assert (
        "Logging to" in res.output
        and (sandbox.root / "test-logs" / "stage.log").exists()
    )


def _python3_logging_args(sandbox):
    """A python3 on PATH that records its arguments and the profile it sees; returns the log path."""
    log = sandbox.root / "py3-args.log"
    py3 = sandbox.bin / "python3"
    py3.write_text('#!/bin/sh\necho "profile=${DATABRICKS_CONFIG_PROFILE:-unset} $*" >> "%s"\nexit 0\n' % log)
    py3.chmod(0o755)
    return log


def test_stage_jar_from_wheel_takes_no_lock_and_passes_the_wheel_by_absolute_path(sandbox):
    log = _python3_logging_args(sandbox)
    wheel = sandbox.root / "dist" / "env6" / "geobrix-0.6.0+env6-py3-none-any.whl"
    wheel.parent.mkdir(parents=True)
    wheel.write_bytes(b"w")

    with sandbox.lock_held_by("pid=4242 cmd=gbx-test-scala.sh since=now"):  # a Maven build would wait or exit 75
        res = sandbox.run(
            "gbx-data-stage-jar.sh", "--no-wait", "--from-wheel", "dist/env6/../env6/" + wheel.name,
            "--profile", "oauth-fe", GBX_BUNDLE_SKIP_JAR_UPLOAD="1",
        )  # fmt: skip

    assert res.returncode == 0, res.output
    assert "container lock" not in res.output
    profile, script, flag, path = log.read_text().split()
    assert (profile, flag) == ("profile=oauth-fe", "--from-wheel")
    assert script.endswith("examples/tests/stage_jar_to_volume.py")
    assert Path(path).is_absolute() and Path(path).resolve() == wheel.resolve()  # not relative to the project root


@pytest.mark.parametrize("args", [["--from-wheel"], ["--from-wheel", "--no-wait"], ["--profile"], ["--profile", "--no-wait"]])
def test_stage_jar_options_that_need_a_value_are_refused(sandbox, args):
    log = _python3_logging_args(sandbox)
    res = sandbox.run("gbx-data-stage-jar.sh", *args, GBX_BUNDLE_SKIP_JAR_UPLOAD="1")
    assert res.returncode == 2 and "needs" in res.output and not log.exists()


def test_stage_wheel_help_wins_over_a_versioned_without_env(sandbox):
    log = _python3_logging_args(sandbox)
    res = sandbox.run("gbx-data-stage-wheel.sh", "--versioned", "--help")
    assert res.returncode == 0 and "Usage:" in res.output and "--versioned" in res.output
    assert "stages env wheels: pass" not in res.output and not log.exists()


def test_stage_jar_from_wheel_runs_the_launcher_even_with_skip_jar_set(sandbox):
    log = _python3_logging_args(sandbox)
    wheel = sandbox.root / "w" / "geobrix-0.6.0+env6-py3-none-any.whl"
    wheel.parent.mkdir()
    wheel.write_bytes(b"w")
    res = sandbox.run("gbx-data-stage-jar.sh", "--from-wheel", str(wheel),
                      GBX_BUNDLE_SKIP_JAR="1", GBX_BUNDLE_SKIP_JAR_UPLOAD="1")  # fmt: skip
    assert res.returncode == 0, res.output
    assert "--from-wheel" in log.read_text()


def test_stage_wheel_versioned_reaches_the_launcher_only_with_env(sandbox):
    log = _python3_logging_args(sandbox)
    env = dict(GBX_BUNDLE_SKIP_WHEEL_UPLOAD="1")

    res = sandbox.run("gbx-data-stage-wheel.sh", "--versioned", **env)
    assert res.returncode == 2 and "--versioned stages env wheels" in res.output and not log.exists()

    res = sandbox.run("gbx-data-stage-wheel.sh", "--env", "6", "--versioned", **env)
    assert res.returncode == 0, res.output
    assert log.read_text().split()[2:] == ["--env", "6", "--versioned"]


def test_ci_status_help_is_help_not_the_run_limit(tmp_path):
    # `--help` was taken as the LIMIT: `gh run list --limit --help` failed. gh must not be called.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    gh = bin_dir / "gh"
    gh.write_text(f'#!/bin/sh\necho "$*" >> "{tmp_path / "gh.log"}"\n')
    gh.chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}
    command = REPO_ROOT / "scripts" / "commands" / "gbx-ci-status.sh"

    for flag in ("--help", "-h"):
        res = subprocess.run(
            ["bash", str(command), flag], env=env, capture_output=True, text=True
        )
        assert res.returncode == 0 and "Usage" in res.stdout, (flag, res.stderr)
    bad = subprocess.run(
        ["bash", str(command), "--nope"], env=env, capture_output=True, text=True
    )

    assert bad.returncode == 1 and "Unknown option" in bad.stderr
    letters = subprocess.run(
        ["bash", str(command), "abc"], env=env, capture_output=True, text=True
    )
    assert letters.returncode == 1 and "abc" in letters.stderr  # a LIMIT must be a number
    assert not (tmp_path / "gh.log").exists()


def test_review_round_skips_the_generated_lock_files_unless_asked(sandbox):
    # The compiled lock files are hash-pinned output nobody reviews; Isaac loads every changed
    # file in full, so they only cost context. They are opted out through a layered config.
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@t", "-C", str(sandbox.root)]
    subprocess.run([*git, "init", "-q"], check=True)
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "base"], check=True)
    (sandbox.root / "change.txt").write_text("x\n")
    subprocess.run([*git, "add", "change.txt"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "change"], check=True)
    argv = sandbox.root / "dbexec.argv"
    dbexec = sandbox.bin / "dbexec"
    dbexec.write_text(f'#!/bin/sh\nprintf "%s\\n" "$@" > "{argv}"\necho reviewed\n')
    dbexec.chmod(0o755)

    def review(*args):
        res = sandbox.run("gbx-review-round.sh", "--base", "HEAD~1", *args)
        assert res.returncode == 0, res.output
        return argv.read_text().splitlines()

    words = review()
    config = Path(words[words.index("-c") + 1])
    assert config == sandbox.commands / "isaac-review-local.yaml"
    text = config.read_text()
    for pattern in ("**/requirements-dev-container*.txt", "**/requirements*-ci.txt"):
        assert f'"{pattern}"' in text
    assert (
        '"third_party/go/vendor/**"' in text
    )  # Isaac's own defaults are repeated: lists replace
    for severity in ("critical", "major", "minor", "info"):
        assert f"    {severity}: true" in text  # all severities are reported, not just the top two

    # --include-generated keeps the lock files in, and still reports every severity
    words = review("--include-generated")
    config = Path(words[words.index("-c") + 1])
    assert config == sandbox.commands / "isaac-review-severities.yaml"
    text = config.read_text()
    assert "\nopt_out:" not in text  # the lock files stay in
    for severity in ("critical", "major", "minor", "info"):
        assert f"    {severity}: true" in text
    assert (
        review("--", "-c", "mine.yaml").count("-c") == 1
    )  # the caller's config, not ours


@pytest.mark.parametrize(
    "output_args",
    [
        ["-o", "test-logs/r.md"],
        ["--output", "test-logs/r.md"],
        ["-o=test-logs/r.md"],
        ["--output=test-logs/r.md"],
    ],
    ids=["-o", "--output", "-o=", "--output="],
)
def test_review_round_keeps_a_relative_report_when_notebooks_are_stripped(
    sandbox, output_args
):
    # A changed .ipynb sends isaac into a scratch worktree that is deleted on exit; a relative
    # `-o` was written there and lost. The report must land under the caller's directory.
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@t", "-C", str(sandbox.root)]
    subprocess.run([*git, "init", "-q"], check=True)
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "base"], check=True)
    (sandbox.root / "nb.ipynb").write_text(json.dumps({"cells": [], "nbformat": 4}) + "\n")
    subprocess.run([*git, "add", "nb.ipynb"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "notebook"], check=True)
    dbexec = sandbox.bin / "dbexec"
    dbexec.write_text(
        '#!/bin/sh\nout=""\nwhile [ $# -gt 0 ]; do\n'
        '  case "$1" in -o|--output) out="$2";; -o=*|--output=*) out="${1#*=}";; esac\n'
        "  shift\ndone\n"
        'mkdir -p "$(dirname "$out")" && echo report > "$out"\necho reviewed\n'
    )
    dbexec.chmod(0o755)

    # No git identity anywhere (a CI runner): the scratch base must bring its own. useConfigOnly
    # stops git from guessing one from the user and host name, which a developer's machine can.
    no_identity = dict(
        HOME=str(sandbox.root / "empty-home"),
        GIT_CONFIG_GLOBAL="/dev/null",
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_COUNT="1",
        GIT_CONFIG_KEY_0="user.useConfigOnly",
        GIT_CONFIG_VALUE_0="true",
    )
    res = sandbox.run(
        "gbx-review-round.sh", "--base", "HEAD~1", "--", *output_args, **no_identity
    )

    assert res.returncode == 0, res.output
    assert (sandbox.root / "test-logs" / "r.md").read_text() == "report\n"


# ----------------------------------------- host venv curated to match the container (venv check)


def _env_json(python="3.12.3", natives=None, **packages):
    return json.dumps(
        {
            "python": python,
            "packages": packages,
            "natives": natives or {"gdal": "3.12.1", "proj": "9.5.1", "geos": "3.13.1"},
        }
    )


def _curated_sandbox(sandbox, host_json):
    """Sandbox with the report script and a host venv whose `python - collect` prints host_json.

    Any other call of that python (running the report's compare / match-requirements) runs on the
    real interpreter, so the comparison itself is the real code."""
    ci = sandbox.root / "scripts" / "ci"
    ci.mkdir(parents=True)
    shutil.copy(REPO_ROOT / "scripts" / "ci" / "venv_env_report.py", ci)
    venv = sandbox.root / ".venv-pyrx"
    (venv / "bin").mkdir(parents=True)
    python = venv / "bin" / "python"
    python.write_text(
        f'#!/bin/sh\nif [ "$1" = "-" ]; then cat > /dev/null; cat <<\'JSON\'\n{host_json}\nJSON\nexit 0; fi\n'
        f'exec "{sys.executable}" "$@"\n'
    )
    python.chmod(0o755)
    return venv


def test_venv_check_fails_on_drift_and_passes_on_a_match(sandbox):
    # The host cannot reach PROJ (pyproj missing or broken): that is a broken host, not a pass.
    host_natives = {"gdal": "3.12.1", "proj": None, "geos": "3.13.1"}
    host = _env_json(
        python="3.12.11",
        numpy="2.3.4",
        scipy="1.16.3",
        extra="1.0",
        natives=host_natives,
    )
    venv = _curated_sandbox(sandbox, host)

    # Python patch, numpy and natives drift; `extra` is host-only (not comparable).
    drifted = _env_json(
        python="3.12.3",
        numpy="2.1.3",
        scipy="1.16.3",
        natives={"gdal": "3.11.0", "proj": "9.5.1", "geos": "3.13.1"},
    )
    res = sandbox.run("gbx-venv-check.sh", FAKE_DOCKER_EXEC_STDOUT=drifted)
    assert res.returncode == 1, res.output
    assert re.search(r"python\s+3\.12\.11\s+3\.12\.3\s+DRIFT", res.output)
    assert re.search(r"numpy\s+2\.3\.4\s+2\.1\.3\s+DRIFT", res.output)
    assert re.search(r"gdal\s+3\.12\.1\s+3\.11\.0\s+DRIFT", res.output)
    assert re.search(
        r"proj\s+-\s+9\.5\.1\s+DRIFT", res.output
    )  # unreachable on the host
    assert (
        "scipy" not in res.output.split("item")[1].split("packages in both")[0]
    )  # equal: not listed

    same = _env_json(
        python="3.12.11", numpy="2.3.4", scipy="1.16.3", natives=host_natives
    )
    res = sandbox.run("gbx-venv-check.sh", FAKE_DOCKER_EXEC_STDOUT=same)
    assert res.returncode == 0, res.output
    assert "2 packages in both, 0 differ" in res.output

    # Packages `gbx:venv:sync --connect` added that the container lacks are declared, not drift.
    (venv / ".gbx-connect-extras").write_text("extra\n")
    res = sandbox.run("gbx-venv-check.sh", FAKE_DOCKER_EXEC_STDOUT=same)
    assert res.returncode == 0, res.output
    assert "declared connect-tier extras (host only): extra" in res.output
    assert (
        "1 packages only on the host" not in res.output
    )  # counted as declared, not stray


def test_venv_sync_match_container_builds_on_the_containers_python_and_versions(
    sandbox,
):
    container = _env_json(python="3.12.3", numpy="2.1.3", pyspark="4.0.0", trans="2.0")
    _curated_sandbox(sandbox, _env_json(python="3.12.3", numpy="2.1.3", trans="1.0"))
    shutil.rmtree(sandbox.root / ".venv-pyrx")  # a fresh checkout: uv creates the venv
    log = sandbox.root / "uv.calls"
    uv = sandbox.bin / "uv"
    uv.write_text(
        "#!/bin/sh\n"
        f'echo "$*" >> "{log}"\n'
        'case "$1" in\n'
        f'  venv) mkdir -p "$2/bin"; cp "{sandbox.root}/.venv-fake-python" "$2/bin/python";;\n'
        '  pip) while [ $# -gt 0 ]; do [ "$1" = "-r" ] && cp "$2" "$(mktemp "'
        + str(sandbox.root)
        + '/req.XXXXXX")"; shift; done;;\n'
        "esac\n"
    )
    uv.chmod(0o755)
    # the python the fake `uv venv` installs: prints the host environment for `python - collect`
    fake = sandbox.root / ".venv-fake-python"
    fake.write_text(
        '#!/bin/sh\nif [ "$1" = "-" ]; then cat > /dev/null; '
        f"echo '{_env_json(python='3.12.3', numpy='2.3.4', trans='1.0')}'; exit 0; fi\n"
        f'exec "{sys.executable}" "$@"\n'
    )
    fake.chmod(0o755)
    lock = _fake_lock(sandbox)
    lock.write_text(
        "numpy==2.3.4 \\\n    --hash=sha256:aa\nonlylock==9.9 \\\n    --hash=sha256:bb\n"
        "pyspark==4.0.0 \\\n    --hash=sha256:cc\n"
        'marked==1.0 ; python_version < "3.13" \\\n    --hash=sha256:dd\n'
    )

    res = sandbox.run(
        "gbx-venv-sync.sh", "--match-container", FAKE_DOCKER_EXEC_STDOUT=container
    )

    calls = log.read_text().splitlines()
    assert calls[0].startswith("venv ") and "--python 3.12.3" in calls[0], calls
    installs = [c for c in calls if c.startswith("pip install")]
    assert all(
        "--require-hashes" not in c for c in installs
    )  # CI's hashes are another platform
    reqs = [p.read_text() for p in sandbox.root.glob("req.*")]
    first = next(r for r in reqs if "onlylock" in r)
    assert (
        "numpy==2.1.3" in first and "numpy==2.3.4" not in first
    )  # the container's version
    assert (
        "onlylock==9.9" in first
    )  # a package the container lacks keeps the lock's version
    assert 'marked==1.0 ; python_version < "3.13"' in first  # environment markers are kept
    # Matching can pull in packages the lock does not name at their newest: they are pinned too.
    pins = next(r for r in reqs if "onlylock" not in r)
    assert pins.split() == [
        "numpy==2.1.3",
        "trans==2.0",
    ]  # what the host has at other versions
    # The fake never really installs, so the final check still sees drift: the sync must fail.
    assert res.returncode == 1 and "drifts from the container" in res.output, res.output


def test_no_command_hard_codes_a_package_index():
    # This is a public repository: a package index comes from PIP_INDEX_URL or pip's own
    # configuration, never from a literal URL in a command.
    commands = Path(__file__).resolve().parents[1] / "commands"
    literal_index = re.compile(
        r"(--(extra-)?index-url[ =]+|INDEX_URL=|index-url *= *)[\"']?https?://", re.IGNORECASE
    )
    offenders = [
        f"{path.name}:{n}"
        for path in sorted(commands.glob("*"))
        if path.suffix in {".sh", ".md", ".py"}
        for n, line in enumerate(path.read_text(errors="replace").splitlines(), 1)
        if literal_index.search(line)
    ]
    assert offenders == [], offenders


# ------------------------------------------------ gbx:data:stage-sample (the .sh around the launcher)


def _stage_sample_sandbox(sandbox):
    """The command with a stub launcher that records its argv, and a `uv` that records its own.

    The launcher itself is tested in test_stage_sample_to_volume.py; here only the script around it:
    which interpreter it picks, what it hands on, and that --help runs nothing."""
    launcher = sandbox.root / "examples" / "tests" / "stage_sample_to_volume.py"
    launcher.parent.mkdir(parents=True)
    launcher.write_text(
        "import json, os, sys\n"
        'open(os.environ["STUB_LOG"], "a").write(json.dumps(sys.argv[1:]) + "\\n")\n'
    )
    uv = sandbox.bin / "uv"
    uv.write_text('#!/bin/sh\necho "$@" >> "$UV_LOG"\nexit 1\n')
    uv.chmod(0o755)
    stub_log, uv_log = sandbox.root / "launcher.log", sandbox.root / "uv.log"
    env = dict(
        STUB_LOG=str(stub_log),
        UV_LOG=str(uv_log),
        GBX_PYRX_VENV=str(sandbox.root / "no-venv"),  # so the SDK python comes from uv, not a venv
    )
    return env, stub_log, uv_log


def test_stage_sample_help_exits_zero_and_runs_nothing(sandbox):
    env, stub_log, uv_log = _stage_sample_sandbox(sandbox)

    res = sandbox.run("gbx-data-stage-sample.sh", "--help", **env)

    assert res.returncode == 0, res.output
    assert "--dry-run" in res.output and "--profile" in res.output
    assert not stub_log.exists() and not uv_log.exists()


def test_stage_sample_dry_run_needs_neither_uv_nor_the_sdk_and_keeps_the_log_out_of_the_launcher(
    sandbox,
):
    env, stub_log, uv_log = _stage_sample_sandbox(sandbox)

    res = sandbox.run(
        "gbx-data-stage-sample.sh", "--dry-run", "--path", "x", "--log", "l.log", **env
    )

    # uv is present here and fails every call: a dry run must not call it
    assert res.returncode == 0, res.output
    assert not uv_log.exists(), uv_log.read_text()
    assert json.loads(stub_log.read_text()) == ["--dry-run", "--path", "x"]  # no --log
    assert (sandbox.root / "test-logs" / "l.log").exists()


def test_the_serverless_notebook_command_hands_the_runner_its_arguments_whole(sandbox):
    """The wrapper parses only --help and --log: every other argument, valued or not, reaches the runner as given."""
    env, _stub_log, uv_log = _stage_sample_sandbox(sandbox)
    (sandbox.bin / "uv").write_text('#!/bin/sh\nfor a in "$@"; do echo "$a"; done >> "$UV_LOG"\nexit 0\n')
    runner = sandbox.root / "examples" / "tests" / "run_notebooks_serverless.py"
    runner.parent.mkdir(parents=True, exist_ok=True)
    runner.write_text("")
    args = ["--notebook", "a.ipynb", "--dep-notebook", "cfg.ipynb", "--existing-cluster-id", "0101-x",
            "--new-cluster", "--jar", "/Volumes/c/s/v/j.jar", "--access-mode", "standard"]  # fmt: skip

    res = sandbox.run("gbx-test-notebooks-serverless.sh", *args, **env)

    assert res.returncode == 0, res.output
    seen = uv_log.read_text().splitlines()
    assert seen[seen.index("python") + 2 :] == args  # after the interpreter and the runner script


def test_stage_sample_without_dry_run_runs_the_launcher_on_the_sdk_python(sandbox):
    env, stub_log, uv_log = _stage_sample_sandbox(sandbox)

    res = sandbox.run("gbx-data-stage-sample.sh", "--profile", "p", **env)

    # the fake uv fails, so the launcher never runs and the command exits non-zero
    assert res.returncode != 0
    assert not stub_log.exists()
    call = uv_log.read_text().split()
    assert call[:2] == ["run", "--no-project"]
    assert call[-4] == "python" and call[-3].endswith("examples/tests/stage_sample_to_volume.py")
    assert call[-2:] == ["--profile", "p"]


# --- the env wheels in the workspace launchers: the Serverless runner's dependency list, stage-wheel --env ------

def _launcher(name, monkeypatch):
    import importlib
    from unittest import mock

    monkeypatch.syspath_prepend(str(REPO_ROOT / "examples" / "tests"))
    with mock.patch.dict(os.environ):  # stage_wheel_to_volume copies databricks_cluster_config.env in at import
        return importlib.import_module(name)


ENV6 = "/Volumes/c/s/v/geobrix-0.6.0+env6-py3-none-any.whl"


def _extra(env, name):
    lines = (REPO_ROOT / f"python/geobrix/envs/env{env}/extra-{name}.txt").read_text().splitlines()
    return [ln.split("#", 1)[0].strip() for ln in lines if ln.split("#", 1)[0].strip()]


@pytest.mark.parametrize(
    "wheel, extras, env_version, want",
    [
        (ENV6, ["stac", "vizx"], "6", [ENV6, *_extra(6, "stac"), *[r for r in _extra(6, "vizx") if r not in _extra(6, "stac")]]),
        (ENV6, [], "6", [ENV6]),  # the bare env wheel: its core is the wheel's own Requires-Dist
        (ENV6, ["light_env6"], "6", "needs no light extra"),
        (ENV6.replace("env6", "env5"), ["models_gpu_env5"], "5", "--extra-deps 'torch==2.9.0,torchvision==0.24.0,segment-geospatial,opencv-python-headless==4.11.0.86'"),
        ("/Volumes/c/s/v/geobrix-0.6.0-py3-none-any.whl", ["photogrammetry_gpu_env5"], "5", "--extra-deps 'pycolmap-cuda12==4.2.1'"),
        ("/Volumes/c/s/v/geobrix-0.6.0-py3-none-any.whl", ["light_env6"], "6", "it requires geobrix==0.6.0+env6"),
        (ENV6, ["overture"], "6", "has no [overture]"),
        (ENV6, ["vizx"], "5", "Fix: pass --env-version 6"),
        ("/Volumes/c/s/v/geobrix-0.6.0-py3-none-any.whl", ["vizx"], "6", None),  # the plain wheel: pyproject's [vizx]
    ],
)  # fmt: skip
def test_the_serverless_runner_installs_an_env_wheel_with_its_own_extras(monkeypatch, wheel, extras, env_version, want):
    runner = _launcher("run_notebooks_serverless", monkeypatch)
    if isinstance(want, str):
        with pytest.raises(SystemExit, match=re.escape(want)):
            runner._build_dep_list(wheel, extras, [], env_version)
        return
    deps = runner._build_dep_list(wheel, extras, [], env_version)
    if want is None:
        import tomllib

        vizx = tomllib.loads((REPO_ROOT / "python/geobrix/pyproject.toml").read_text())["project"]
        assert deps == [wheel, *vizx["optional-dependencies"]["vizx"]]
    else:
        assert deps == want
    assert runner.wheel_env(runner.DEFAULT_WHEEL) == int(runner.DEFAULT_ENV_VERSION) == 6
    assert runner._split_deps("rich, overturemaps>=1.0.2,<2") == ["rich", "overturemaps>=1.0.2,<2"]


@pytest.mark.parametrize(
    "wheel, extra, want",
    [
        # the PEP 685 spellings of an extra name (case, '-', '_' and '.' are one separator) reach the same refusal
        (ENV6, "Light-Env6", "needs no light extra"),
        (ENV6.replace("env6", "env5"), "models-gpu-env5", "a 0.5.x GPU extra, dropped in 0.6.0"),
        (ENV6.replace("env6", "env5"), "GPU.ENV5.ALL", "a 0.5.x GPU extra, dropped in 0.6.0"),
        ("/Volumes/c/s/v/geobrix-0.6.0-py3-none-any.whl", "models-gpu-env5", "a 0.5.x GPU extra, dropped in 0.6.0"),
        ("/Volumes/c/s/v/geobrix-0.6.0-py3-none-any.whl", "light-dbr19", "a 0.5.x name; it requires geobrix=="),
        ("/Volumes/c/s/v/geobrix-0.6.0-py3-none-any.whl", "Light.Env6", "it requires geobrix==0.6.0+env6"),
    ],
)  # fmt: skip
def test_the_serverless_runner_normalizes_extra_names_before_it_refuses_one(monkeypatch, wheel, extra, want):
    runner = _launcher("run_notebooks_serverless", monkeypatch)
    with pytest.raises(SystemExit, match=re.escape(want)):
        runner._build_dep_list(wheel, [extra], [], "5" if "env5" in wheel or "gpu" in extra.lower() else "6")


def test_the_serverless_runner_finds_an_extra_under_any_spelling(monkeypatch):
    runner = _launcher("run_notebooks_serverless", monkeypatch)
    assert runner._build_dep_list(ENV6, ["STAC"], [], "6") == runner._build_dep_list(ENV6, ["stac"], [], "6")
    plain = "/Volumes/c/s/v/geobrix-0.6.0-py3-none-any.whl"
    assert runner._build_dep_list(plain, ["Zarr-Env6"], [], "6") == runner._build_dep_list(plain, ["zarr_env6"], [], "6")
    assert runner._build_dep_list(plain, ["zarr-env6"], [], "6")[1:]  # it has packages: not "not found"


@pytest.mark.parametrize("wheel", [ENV6, "/Volumes/c/s/v/geobrix-0.6.0-py3-none-any.whl"])
def test_an_empty_extras_option_means_the_wheel_alone(monkeypatch, wheel):
    """--extras '' (the CSV is empty) must reach the job environment as the wheel and nothing else."""
    runner = _launcher("run_notebooks_serverless", monkeypatch)
    reached = {}

    def stop(**kw):
        raise RuntimeError("workspace client reached")

    sdk = type(sys)("databricks.sdk")
    sdk.WorkspaceClient = stop
    monkeypatch.setitem(sys.modules, "databricks.sdk", sdk)
    real = runner._build_dep_list
    monkeypatch.setattr(runner, "_build_dep_list", lambda *a: reached.setdefault("deps", real(*a)))
    nb = REPO_ROOT / "examples" / "tests" / "probe_arrow_empty_chunks.ipynb"
    monkeypatch.setattr(sys, "argv", ["run_notebooks_serverless.py", "--notebook", str(nb), "--wheel", wheel,
                                      "--extras", "", "--env-version", "6"])  # fmt: skip
    with pytest.raises(RuntimeError, match="workspace client reached"):  # no SystemExit from the option parsing
        runner.main()
    assert reached["deps"] == [wheel]


def test_the_serverless_runner_submits_one_jar_task_with_the_jar_as_a_java_dependency(monkeypatch):
    from types import SimpleNamespace

    jobs = pytest.importorskip("databricks.sdk.service.jobs")  # the launcher's own interpreter has it
    RunLifeCycleState, RunResultState = jobs.RunLifeCycleState, jobs.RunResultState
    runner = _launcher("run_notebooks_serverless", monkeypatch)
    sent = {}

    def fake_submit(w, **kw):
        sent.update(kw)
        return SimpleNamespace(run_id=7)

    state = SimpleNamespace(life_cycle_state=RunLifeCycleState.TERMINATED, result_state=RunResultState.SUCCESS,
                            state_message="")  # fmt: skip
    w = SimpleNamespace(jobs=SimpleNamespace(
        get_run=lambda run_id: SimpleNamespace(run_page_url="u", state=state, tasks=[]),
        get_run_output=lambda run_id: SimpleNamespace(logs="E-JARtask wall_s=1.0"),
    ))  # fmt: skip
    monkeypatch.setattr(runner, "submit_once", fake_submit)
    assert runner.run_jar_task(w, "a.b.Job", "/Volumes/c/s/v/j.jar", ["t1", "t2", "3"], "6", 0)
    (env,), (task,) = sent["environments"], sent["tasks"]
    assert env.spec.java_dependencies == ["/Volumes/c/s/v/j.jar"] and env.spec.environment_version == "6"
    assert task.environment_key == env.environment_key and task.max_retries == 0 and task.disable_auto_optimization
    assert (task.spark_jar_task.main_class_name, task.spark_jar_task.parameters) == ("a.b.Job", ["t1", "t2", "3"])


def test_the_warehouse_runner_records_a_statement_with_no_result_set(monkeypatch):
    """A CREATE FUNCTION answers SUCCEEDED with no columns: a result with no value, not a stopped run."""
    from types import SimpleNamespace as NS

    sql = pytest.importorskip("databricks.sdk.service.sql")
    runner = _launcher("uc_sql", monkeypatch)
    done = NS(status=NS(state=sql.StatementState.SUCCEEDED), result=None, manifest=NS(schema=NS(columns=None)))
    w = NS(statement_execution=NS(execute_statement=lambda **kw: done))
    assert runner.run_once(w, "CREATE FUNCTION f() RETURNS INT RETURN 1", "wh", None, None, 5) == (
        sql.StatementState.SUCCEEDED, None, None, None,
    )  # fmt: skip


class _FakeFiles:
    def __init__(self, listing):
        self.calls, self.listing, self.sizes = [], listing, {}

    def create_directory(self, path):
        self.calls.append(("mkdir", path))

    def list_directory_contents(self, path):
        from types import SimpleNamespace

        self.calls.append(("list", path))
        return [SimpleNamespace(is_directory=False, name=n, path=f"{path}/{n}") for n in self.listing]

    def delete(self, path):
        self.calls.append(("delete", path))

    def upload(self, path, fh, overwrite):
        self.sizes[path] = len(fh.read())
        self.calls.append(("upload", path))

    def get_metadata(self, path):
        from types import SimpleNamespace

        return SimpleNamespace(content_length=self.sizes[path])


@pytest.mark.parametrize("args", [["--env", "both"], ["--env", "6", "--no-prune"]])
def test_stage_wheel_env_puts_each_env_wheel_on_the_bundle_volroot_by_its_own_name(tmp_path, monkeypatch, args):
    # The artifact Volume dir's plain wheel (another lane's) is never listed, pruned or overwritten.
    stage = _launcher("stage_wheel_to_volume", monkeypatch)
    files = _FakeFiles(["geobrix-0.6.0-py3-none-any.whl"])
    sdk = type(sys)("databricks.sdk")
    sdk.WorkspaceClient = lambda **kw: type("W", (), {"files": files})()
    monkeypatch.setitem(sys.modules, "databricks.sdk", sdk)
    project = tmp_path / "repo"
    for env in (5, 6):
        (project / "dist" / f"env{env}").mkdir(parents=True)
        (project / "dist" / f"env{env}" / f"geobrix-0.6.0+env{env}-py3-none-any.whl").write_bytes(b"w" * env)
    monkeypatch.setattr(stage, "TESTS_DIR", project / "examples" / "tests")
    for k, v in {"DATABRICKS_CONFIG_PROFILE": "p", "GBX_BUNDLE_VOLUME_CATALOG": "c", "GBX_BUNDLE_VOLUME_SCHEMA": "s",
                 "GBX_BUNDLE_VOLUME_NAME": "v"}.items():  # fmt: skip
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("GBX_ARTIFACT_VOLUME", raising=False)  # not needed for an env wheel
    monkeypatch.delenv("GBX_BUNDLE_SKIP_WHEEL_UPLOAD", raising=False)
    monkeypatch.setattr(sys, "argv", ["stage_wheel_to_volume.py", *args])

    assert stage.main() == 0
    envs = (5, 6) if "both" in args else (6,)
    uploads = [c[1] for c in files.calls if c[0] == "upload"]
    assert uploads == [f"/Volumes/c/s/v/geobrix-0.6.0+env{e}-py3-none-any.whl" for e in envs]
    assert not [c for c in files.calls if c[0] in ("list", "delete")]



def _stage_env_wheels(tmp_path, monkeypatch, files, args, short=()):
    """main() of stage_wheel_to_volume for ``args`` against a scratch project with both env wheels built."""
    stage = _launcher("stage_wheel_to_volume", monkeypatch)
    sdk = type(sys)("databricks.sdk")
    sdk.WorkspaceClient = lambda **kw: type("W", (), {"files": files})()
    monkeypatch.setitem(sys.modules, "databricks.sdk", sdk)
    project = tmp_path / "repo"
    for env in (5, 6):
        (project / "dist" / f"env{env}").mkdir(parents=True)
        (project / "dist" / f"env{env}" / f"geobrix-0.6.0+env{env}-py3-none-any.whl").write_bytes(b"w" * env)
    monkeypatch.setattr(stage, "TESTS_DIR", project / "examples" / "tests")
    for k, v in {"DATABRICKS_CONFIG_PROFILE": "p", "GBX_BUNDLE_VOLUME_CATALOG": "c", "GBX_BUNDLE_VOLUME_SCHEMA": "s",
                 "GBX_BUNDLE_VOLUME_NAME": "v"}.items():  # fmt: skip
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("GBX_ARTIFACT_VOLUME", raising=False)
    monkeypatch.delenv("GBX_BUNDLE_SKIP_WHEEL_UPLOAD", raising=False)
    monkeypatch.setattr(sys, "argv", ["stage_wheel_to_volume.py", *args])
    return stage


def test_stage_wheel_env_versioned_gives_each_env_its_own_folder_and_keeps_only_that_wheel(tmp_path, monkeypatch):
    root = "/Volumes/c/s/v/geobrix/0.6.0"
    held = {  # what each folder holds before the stage
        f"{root}/env5": ["geobrix-0.5.2+env5-py3-none-any.whl", "geobrix-0.6.0+env5-py3-none-any.whl", "notes.txt"],
        f"{root}/env6": ["geobrix-0.6.0-py3-none-any.whl", "geobrix-0.6.0+env6-py3-none-any.whl"],
    }
    files = _FakeFiles([])
    real = files.list_directory_contents
    files.list_directory_contents = lambda path: (setattr(files, "listing", held[path]), real(path))[1]
    stage = _stage_env_wheels(tmp_path, monkeypatch, files, ["--env", "both", "--versioned"])

    assert stage.main() == 0
    calls = files.calls
    assert [c for c in calls if c[0] == "upload"] == [
        ("upload", f"{root}/env5/geobrix-0.6.0+env5-py3-none-any.whl"),
        ("upload", f"{root}/env6/geobrix-0.6.0+env6-py3-none-any.whl"),
    ]
    assert ("mkdir", f"{root}/env5") in calls and ("mkdir", f"{root}/env6") in calls
    # every other wheel of a folder goes, this env's own is overwritten, notes.txt is no wheel
    assert [c for c in calls if c[0] == "delete"] == [
        ("delete", f"{root}/env5/geobrix-0.5.2+env5-py3-none-any.whl"),
        ("delete", f"{root}/env6/geobrix-0.6.0-py3-none-any.whl"),
    ]
    assert not [c for c in calls if c[1].rstrip("/") == "/Volumes/c/s/v"]  # the volroot itself is never touched


def test_stage_wheel_env_versioned_stops_on_a_short_upload(tmp_path, monkeypatch, capsys):
    files = _FakeFiles([])
    files.get_metadata = lambda path: type("M", (), {"content_length": 0})()
    stage = _stage_env_wheels(tmp_path, monkeypatch, files, ["--env", "6", "--versioned"])

    assert stage.main() == 1
    assert "upload verification failed" in capsys.readouterr().err
    assert ("upload", "/Volumes/c/s/v/geobrix/0.6.0/env6/geobrix-0.6.0+env6-py3-none-any.whl") in files.calls


def test_stage_wheel_versioned_without_env_is_refused_before_anything_is_listed(tmp_path, monkeypatch, capsys):
    files = _FakeFiles([])
    stage = _stage_env_wheels(tmp_path, monkeypatch, files, ["--versioned"])

    assert stage.main() == 2
    assert "--versioned stages env wheels" in capsys.readouterr().err and files.calls == []


def test_the_plain_wheel_stage_prunes_only_other_plain_wheels(monkeypatch):
    # if GBX_ARTIFACT_VOLUME is the bundle volroot, a plain stage must not delete the env wheels staged there
    av = _launcher("_artifact_volume", monkeypatch)
    files = _FakeFiles(["geobrix-0.5.2-py3-none-any.whl", "geobrix-0.6.0+env5-py3-none-any.whl",
                        "geobrix-0.6.0+env6-py3-none-any.whl", "geobrix-0.6.0-py3-none-any.whl", "other.txt"])  # fmt: skip
    w = type("W", (), {"files": files})()
    removed = av.prune_stale(w, "/Volumes/c/s/v", "geobrix-", ".whl", "geobrix-0.6.0-py3-none-any.whl", exclude="+env")
    assert removed == ["/Volumes/c/s/v/geobrix-0.5.2-py3-none-any.whl"]
    text = (REPO_ROOT / "examples/tests/stage_wheel_to_volume.py").read_text()
    assert 'whl.name, exclude="+env")' in text  # the stage passes it


_SNAPSHOT_PROBE = """#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"
gbx_snapshot_exec "${BASH_SOURCE[0]}" "$@"
echo "running from $PROJECT_ROOT, source tree ${GBX_SOURCE_ROOT:-none}"
sleep 2
echo "version ONE"
"""


def test_a_launcher_command_runs_from_a_snapshot_so_an_edit_during_the_run_cannot_change_it(sandbox):
    """Editing a gbx script under a live runner once ran half a changed line (`dk: command not found`)."""
    sandbox.write_command("gbx-snapshot-probe.sh", _SNAPSHOT_PROBE)
    proc = sandbox.popen("gbx-snapshot-probe.sh")
    first = proc.stdout.readline()  # the command is now past the snapshot and sleeping
    script = sandbox.commands / "gbx-snapshot-probe.sh"
    script.write_text(script.read_text().replace("version ONE", "version TWO") + "\nrm -rf /nonexistent-dk\n")
    out, _ = proc.communicate(timeout=30)

    assert proc.returncode == 0, first + out
    assert "version ONE" in out and "version TWO" not in out, out
    m = re.search(r"running from (\S+), source tree (\S+)", first)
    assert m and Path(m.group(2)) == sandbox.root and Path(m.group(1)) != sandbox.root, first
    assert not Path(m.group(1)).exists()  # the snapshot is removed when the run ends


_SNAPSHOT_SIZE_PROBE = """#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"
gbx_snapshot_exec "${BASH_SOURCE[0]}" "$@"
echo "docker files: $(cd "$PROJECT_ROOT/scripts/docker" && find . -type f | sort | tr '\\n' ' ')"
echo "big files: $(find "$PROJECT_ROOT" -type f -size +1024k | wc -l | tr -d ' ')"
echo "m2 dirs: $(find "$PROJECT_ROOT" -name m2 | wc -l | tr -d ' ')"
"""


def test_the_snapshot_never_copies_the_maven_repo_or_any_large_data_dir(sandbox):
    """scripts/docker/m2 is a 1.1 GB persistent Maven repo: copying it cost 13 s per launch and filled /tmp."""
    docker = sandbox.root / "scripts" / "docker"
    (docker / "m2" / "repository").mkdir(parents=True)
    (docker / "m2" / "repository" / "big.jar").write_bytes(b"\0" * (3 * 1024 * 1024))
    (docker / "data").mkdir()
    (docker / "data" / "huge.bin").write_bytes(b"\0" * (3 * 1024 * 1024))
    sandbox.write_command("gbx-snapshot-size-probe.sh", _SNAPSHOT_SIZE_PROBE)

    res = sandbox.run("gbx-snapshot-size-probe.sh")

    assert res.returncode == 0, res.output
    assert "m2 dirs: 0" in res.output and "big files: 0" in res.output, res.output
    # what the snapshotted commands source or exec from scripts/docker is there
    assert "./gbx-container.sh" in res.output and "./gbx-lock.sh" in res.output, res.output


def test_help_runs_in_place_without_a_snapshot(sandbox):
    sandbox.write_command("gbx-snapshot-probe.sh", _SNAPSHOT_PROBE)

    res = sandbox.run("gbx-snapshot-probe.sh", "--help")

    assert "source tree none" in res.output and str(sandbox.root) in res.output
