"""Tests for fixes to the gbx:test:* commands: parity discovery, doc-test isolation, locale.

Each command runs from a scratch copy of scripts/commands/ against a fake `docker` (see
conftest.py's ContainerSandbox): nothing reaches a daemon, and the work a command would send
into the container (pytest, Maven) is recorded instead of run.
"""

import re
import subprocess
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
    # light_env6 (>=2.2) cannot satisfy.
    installs = [c for c in calls if c.startswith("pip install")]
    assert len(installs) == 2, calls
    assert "--require-hashes" in installs[0] and f"-r {lock}" in installs[0]
    assert "--no-deps" in installs[1] and "-e ./python/geobrix" in installs[1]
    assert "[" not in installs[1]


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


def _venv_python(sandbox, *, sdk, venv=None):
    """A venv (default `.venv-pyrx`) whose python can or cannot import databricks.sdk.

    The CI-lock venv has no SDK: `python -c "import databricks.sdk"` fails there. Any other
    call (a launcher run) is recorded in calls.log next to the interpreter.
    """
    venv = venv or sandbox.root / ".venv-pyrx"
    (venv / "bin").mkdir(parents=True)
    python = venv / "bin" / "python"
    python.write_text(
        '#!/bin/sh\n[ "$1" = "-c" ] && exit %d\n'
        'echo "$*" >> "%s"\nexit 0\n' % (0 if sdk else 1, venv / "calls.log")
    )
    python.chmod(0o755)
    return python, venv / "calls.log"


def _sdk_python(sandbox, **env):
    """What gbx_python_with_sdk (common.sh) picks, one word per line."""
    res = subprocess.run(
        [
            "bash",
            "-c",
            f'PROJECT_ROOT="{sandbox.root}"; source "{sandbox.commands}/common.sh"; '
            'gbx_python_with_sdk; printf "%s\\n" "${GBX_SDK_PYTHON[@]}"',
        ],
        env=sandbox.env(**env),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert res.returncode == 0, res.stderr
    return res.stdout.split("\n")[:-1]


def test_the_sdk_python_is_the_venv_only_when_the_venv_can_import_the_sdk(
    sandbox, tmp_path
):
    # With the SDK: the venv as it is. Without it (the CI-lock venv): uv layers the SDK over the
    # venv's own interpreter, so the venv's packages stay importable. No venv: uv on 3.12.
    # No venv and no uv: system python3.
    uv = ["run", "--no-project", "--python"]
    with_sdk, _ = _venv_python(sandbox, sdk=True)
    assert _sdk_python(sandbox) == [str(with_sdk)]

    other, _ = _venv_python(sandbox, sdk=False, venv=tmp_path / "bare")
    layered = _sdk_python(sandbox, GBX_PYRX_VENV=str(tmp_path / "bare"))
    assert layered == ["uv"] + uv + [str(other), "--with", "databricks-sdk", "python"]

    missing = _sdk_python(sandbox, GBX_PYRX_VENV=str(tmp_path / "nowhere"))
    assert missing == ["uv"] + uv + ["3.12", "--with", "databricks-sdk", "python"]

    assert _sdk_python(
        sandbox, GBX_PYRX_VENV=str(tmp_path / "nowhere"), PATH="/usr/bin:/bin"
    ) == ["python3"]


@pytest.mark.parametrize(
    "command, args, env, launcher",
    [
        (
            "gbx-data-stage-jar.sh",
            [],
            {"GBX_BUNDLE_SKIP_JAR": "1"},
            "stage_jar_to_volume",
        ),
        (
            "gbx-data-stage-wheel.sh",
            [],
            {"GBX_BUNDLE_SKIP_JAR": "1"},
            "stage_wheel_to_volume",
        ),
        ("gbx-bench-cluster.sh", [], {}, "push_and_run_bench_on_cluster"),
        (
            "gbx-test-notebooks-serverless.sh",
            ["--notebook", "x.ipynb"],
            {},
            "run_notebooks_serverless",
        ),
    ],
)
def test_the_workspace_launchers_get_the_sdk_when_the_venv_has_none(
    sandbox, command, args, env, launcher
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
    assert words[4:7] == ["--with", "databricks-sdk", "python"], words
    assert any(w.endswith(f"examples/tests/{launcher}.py") for w in words[7:]), words
    assert not calls.exists()  # the SDK-less interpreter never ran the launcher itself


def test_a_launcher_runs_on_the_venv_itself_when_it_has_the_sdk(sandbox):
    python, calls = _venv_python(sandbox, sdk=True)
    uv_log = _fake_uv(sandbox)

    res = sandbox.run("gbx-data-stage-jar.sh", GBX_BUNDLE_SKIP_JAR="1")

    assert res.returncode == 0, res.output
    assert calls.read_text().split() == ["examples/tests/stage_jar_to_volume.py"]
    assert not uv_log.exists()  # nothing was fetched
