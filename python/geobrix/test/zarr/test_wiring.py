"""The Zarr suite's wiring: extras, the three light-test-dir lists, and the CI locks.

Each of these has been missed before when a light feature was added, and nothing else
fails loudly when it is (a dir that never runs in CI, a dep missing from the CI lock).
"""

import ast
import importlib.util
import os
import re
import shlex
import tomllib
from pathlib import Path
from types import SimpleNamespace

PKG = Path(__file__).resolve().parents[2]  # python/geobrix
REPO = PKG.parents[1]
EXTRAS = tomllib.loads((PKG / "pyproject.toml").read_text())["project"][
    "optional-dependencies"
]
STACK = ("zarr", "numcodecs", "zarrs", "tensorstore", "obstore")


def _names(requirements):
    return {re.split(r"[<>=!~\[ ]", r, maxsplit=1)[0] for r in requirements}


def test_extras_are_per_runtime_with_an_alias_and_no_bare_zarr():
    assert set(STACK) <= _names(EXTRAS["zarr_dbr19"])
    assert all("<" in r for r in EXTRAS["zarr_dbr19"]), "every pin carries a cap"
    assert EXTRAS["zarr_env6"] == ["geobrix[zarr_dbr19]"]
    assert "zarr" not in EXTRAS
    # the env6 wheel's [zarr] is the same pins (gbx:build:wheel ships envs/env6/extra-zarr.txt as it)
    env6 = (PKG / "envs" / "env6" / "extra-zarr.txt").read_text().splitlines()
    pins = [ln.split("#", 1)[0].strip() for ln in env6]
    assert sorted(p for p in pins if p) == sorted(
        r.split("#", 1)[0].strip() for r in EXTRAS["zarr_dbr19"]
    )
    assert not (PKG / "envs" / "env5" / "extra-zarr.txt").exists()
    assert (
        ".[stac,vizx,zarr_dbr19]"
        in (PKG / "requirements-light-env6-all-ci.in").read_text().splitlines()
    )


def test_zarr_is_registered_in_all_three_light_test_dir_lists():
    tree = ast.parse((PKG / "test" / "conftest.py").read_text())
    dirs = next(
        ast.literal_eval(n.value)
        for n in tree.body
        if isinstance(n, ast.Assign)
        and any(getattr(t, "id", "") == "_LIGHT_TEST_DIRS" for t in n.targets)
    )
    assert "zarr" in dirs

    heavy = (REPO / ".github/actions/python_build/action.yml").read_text()
    ignores = re.search(r'LIGHT_IGNORES="([^"]*)"', heavy).group(1).split()
    assert sorted(i.removeprefix("--ignore=test/") for i in ignores) == sorted(dirs)

    light = (REPO / ".github/actions/pyrx_build/action.yml").read_text()
    run = re.search(r"^\s*pytest (test/\S+(?: test/\S+)*) -m ", light, re.M).group(1)
    assert sorted(d.removeprefix("test/") for d in run.split()) == sorted(dirs)


def test_ci_locks_pin_the_zarr_stack():
    for lock in (
        "requirements-light-env6-ci.txt",
        "requirements-light-env6-all-ci.txt",
    ):
        text = (PKG / lock).read_text()
        for name in STACK:
            assert re.search(rf"^{name}==\S+ \\$", text, re.M), f"{lock} lacks {name}"
    ci_in = (PKG / "requirements-light-env6-ci.in").read_text()
    for name in STACK:
        assert re.search(rf"^{name}[<>=]", ci_in, re.M), f"env6 .in lacks {name}"


def test_conftest_delta_version_matches_the_fetch_script_and_ci_runs_it():
    script = (REPO / "scripts/ci/fetch-delta-test-jars.sh").read_text()
    conftest = (PKG / "test/conftest.py").read_text()
    script_version = re.search(r'^DELTA_VERSION="([^"]+)"', script, re.M).group(1)
    conftest_version = re.search(r'^_DELTA_VERSION = "([^"]+)"', conftest, re.M).group(
        1
    )
    assert script_version == conftest_version
    # the Zarr doc tests find the same jars, in the same repos, at the same version
    docs = (REPO / "docs/tests/python/zarr_delta.py").read_text()
    docs_version = re.search(r'^_DELTA_VERSION = "([^"]+)"', docs, re.M).group(1)
    assert docs_version == script_version
    for found in ("io/delta/delta-spark_2.13", "io/delta/delta-storage", "GBX_M2_REPO"):
        assert found in docs and found in conftest, found
    assert '"scripts" / "docker" / "m2"' in conftest
    assert '"scripts" / "docker" / "m2"' in docs
    for pin in ("SHA256_DELTA_SPARK", "SHA256_DELTA_STORAGE"):
        assert re.search(rf'^{pin}="[0-9a-f]{{64}}"$', script, re.M), pin
    assert re.search(
        r"^MVN_ARGS=\(.*\s-C\b", script, re.M
    ), "Maven must run with strict checksums"
    light = (REPO / ".github/actions/pyrx_build/action.yml").read_text()
    assert "scripts/ci/fetch-delta-test-jars.sh" in light


def test_delta_jars_reach_the_classpath_only_when_zarr_tests_run(tmp_path, monkeypatch):
    """The root conftest hook: classpath (never spark.jars), quoted, zarr-gated, no override."""
    spec = importlib.util.spec_from_file_location(
        "gbx_root_conftest_under_test", PKG / "test" / "conftest.py"
    )
    conftest = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(conftest)

    repo = tmp_path / "a repo with spaces"
    v = conftest._DELTA_VERSION
    jars = [
        repo / f"io/delta/delta-spark_2.13/{v}/delta-spark_2.13-{v}.jar",
        repo / f"io/delta/delta-storage/{v}/delta-storage-{v}.jar",
    ]
    for jar in jars:
        jar.parent.mkdir(parents=True)
        jar.write_bytes(b"")
    monkeypatch.setenv("GBX_M2_REPO", str(repo))
    monkeypatch.setenv("PYSPARK_SUBMIT_ARGS", "x")  # records the original for teardown
    monkeypatch.delenv("PYSPARK_SUBMIT_ARGS")

    def item(path, delta=False):
        marker = SimpleNamespace(name="delta") if delta else None
        return SimpleNamespace(
            path=path,
            get_closest_marker=lambda name: marker if name == "delta" else None,
        )

    def session(*items):
        return SimpleNamespace(
            items=[i if isinstance(i, SimpleNamespace) else item(i) for i in items]
        )

    ds_test = PKG / "test" / "ds" / "test_x.py"
    zarr_test = PKG / "test" / "zarr" / ".." / "zarr" / "test_x.py"  # needs resolving

    conftest.pytest_collection_finish(session(ds_test))
    assert (
        "PYSPARK_SUBMIT_ARGS" not in os.environ
    ), "no zarr or delta-marked test collected"

    conftest.pytest_collection_finish(
        session(item(ds_test, delta=True))
    )  # a ds test marked delta
    assert (
        shlex.split(os.environ.pop("PYSPARK_SUBMIT_ARGS"))[0] == "--driver-class-path"
    )

    conftest.pytest_collection_finish(session(ds_test, zarr_test))
    args = shlex.split(os.environ["PYSPARK_SUBMIT_ARGS"])  # as PySpark splits it
    assert args[0] == "--driver-class-path" and args[2] == "pyspark-shell"
    assert len(args) == 3 and "--jars" not in args
    assert args[1].split(os.pathsep) == [str(j) for j in jars]

    os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 1g pyspark-shell"
    conftest.pytest_collection_finish(session(zarr_test))
    assert os.environ["PYSPARK_SUBMIT_ARGS"] == "--driver-memory 1g pyspark-shell"
