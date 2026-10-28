"""Guards for the sharded light CI job and its test/bench path filter.

The light tier runs as parallel shards (the `pyrx` job matrix in build_main.yml). These tests
read the workflow and the two CI actions and fail unless:

* the shards together hold exactly the dirs of `_LIGHT_TEST_DIRS` (test/conftest.py), each in
  one shard, so a new light dir that no shard runs, or that two shards run, goes red;
* the heavy build ignores the same set (`LIGHT_IGNORES`) and the unsharded fallback line of the
  light action lists the same set;
* `pyvx` runs before `bench` in the same shard (the PROJ state bench relies on);
* `build` waits for the whole sharded job, and the shards actually receive their `dirs`.

The path filter (`paths_changed.py`) is checked against the paths the workflow really passes
it, including the cases that must fail safe.
"""

import ast
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFTEST = REPO_ROOT / "python" / "geobrix" / "test" / "conftest.py"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "build_main.yml"
LIGHT_ACTION = REPO_ROOT / ".github" / "actions" / "pyrx_build" / "action.yml"
HEAVY_ACTION = REPO_ROOT / ".github" / "actions" / "python_build" / "action.yml"
PATHS_CHANGED = Path(__file__).with_name("paths_changed.py")

HOW_TO_FIX = (
    "Put every light dir in exactly one shard of the `pyrx` job in build_main.yml"
)


def _light_test_dirs():
    tree = ast.parse(CONFTEST.read_text())
    return ast.literal_eval(
        next(
            n.value
            for n in tree.body
            if isinstance(n, ast.Assign)
            and any(getattr(t, "id", "") == "_LIGHT_TEST_DIRS" for t in n.targets)
        )
    )


def _pyrx_job():
    return yaml.safe_load(WORKFLOW.read_text())["jobs"]["pyrx"]


def _shards():
    return {
        s["name"]: s["dirs"].split() for s in _pyrx_job()["strategy"]["matrix"]["shard"]
    }


def test_the_shards_cover_exactly_the_light_dirs_once_each():
    light = sorted(_light_test_dirs())
    shards = _shards()
    in_shards = [d for dirs in shards.values() for d in dirs]
    assert all(d.startswith("test/") for d in in_shards), in_shards
    names = [d.removeprefix("test/") for d in in_shards]

    twice = sorted({n for n in names if names.count(n) > 1})
    assert not twice, f"in more than one shard (or twice): {twice}. {HOW_TO_FIX}"
    assert sorted(names) == light, (
        f"in _LIGHT_TEST_DIRS but in no shard: {sorted(set(light) - set(names))}; "
        f"in a shard but not in _LIGHT_TEST_DIRS: {sorted(set(names) - set(light))}. "
        f"{HOW_TO_FIX}"
    )

    # The heavy build must ignore the same set ...
    heavy = re.search(r'LIGHT_IGNORES="([^"]*)"', HEAVY_ACTION.read_text()).group(1)
    ignored = [i.removeprefix("--ignore=test/") for i in heavy.split()]
    assert (
        sorted(ignored) == light
    ), "LIGHT_IGNORES (python_build) differs from _LIGHT_TEST_DIRS"

    # ... and so must the unsharded fallback line (gbx:test:python and gbx:test:light read it).
    fallback = re.search(
        r"^\s*pytest (test/\S+(?: test/\S+)*) -m ", LIGHT_ACTION.read_text(), re.M
    ).group(1)
    assert sorted(d.removeprefix("test/") for d in fallback.split()) == light


def test_pyvx_runs_before_bench_in_the_same_shard():
    # pyvx registers the PROJ grids that bench's raster builds on worker threads rely on.
    (dirs,) = [d for d in _shards().values() if "test/bench" in d]
    assert "test/pyvx" in dirs and dirs.index("test/pyvx") < dirs.index("test/bench")


def test_heavy_waits_for_every_shard_and_each_shard_gets_its_dirs():
    workflow = yaml.safe_load(WORKFLOW.read_text())
    needs = workflow["jobs"]["build"]["needs"]
    assert "pyrx" in ([needs] if isinstance(needs, str) else needs)
    job = _pyrx_job()
    assert job["strategy"]["fail-fast"] is False
    (step,) = [
        s for s in job["steps"] if s.get("uses") == "./.github/actions/pyrx_build"
    ]
    assert step["with"]["dirs"] == "${{ matrix.shard.dirs }}"
    assert "steps.bench.outputs.changed" in step["with"]["skip_dirs"]


def _watched_paths():
    """The paths the workflow's bench step hands to paths_changed.py."""
    (step,) = [s for s in _pyrx_job()["steps"] if s.get("id") == "bench"]
    words = shlex.split(step["run"].replace("\\\n", " "))
    return words[words.index("HEAD") + 1 :]


def _git(cwd, *args):
    # hooks off: a developer's global hooks would run on every commit of these scratch repos
    cmd = ["git", "-c", "user.name=ci", "-c", "user.email=ci@example.com"]
    done = subprocess.run(
        [*cmd, "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )
    return done.stdout.strip()


def _shallow_clone_after_a_change(tmp_path, changed_file):
    """A depth-1 clone at the commit that touches `changed_file`; returns (clone, base sha)."""
    origin = tmp_path / "origin"
    origin.mkdir()
    _git(origin, "init", "-q")
    _git(origin, "config", "uploadpack.allowAnySHA1InWant", "true")
    files = [
        "python/geobrix/test/bench/test_x.py",
        "python/geobrix/src/databricks/labs/gbx/bench/x.py",
        "python/geobrix/test/conftest.py",
        ".github/workflows/build_main.yml",
        "docs/a.md",
    ]
    for f in files:
        (origin / f).parent.mkdir(parents=True, exist_ok=True)
        (origin / f).write_text("v1\n")
    _git(origin, "add", ".")
    _git(origin, "commit", "-q", "-m", "base")
    base = _git(origin, "rev-parse", "HEAD")
    (origin / changed_file).write_text("v2\n")
    _git(origin, "commit", "-q", "-am", "head")
    clone = tmp_path / "clone"
    _git(tmp_path, "clone", "-q", "--depth=1", f"file://{origin}", str(clone))
    return clone, base


@pytest.mark.parametrize(
    "changed_file,base_kind,expected",
    [
        ("python/geobrix/test/bench/test_x.py", "real", "true"),
        ("python/geobrix/src/databricks/labs/gbx/bench/x.py", "real", "true"),
        ("python/geobrix/test/conftest.py", "real", "true"),
        (".github/workflows/build_main.yml", "real", "true"),
        ("docs/a.md", "real", "false"),
        # Fail safe: no usable base means the bench tests run, whatever changed.
        ("docs/a.md", "empty", "true"),
        ("docs/a.md", "zeros", "true"),
        ("docs/a.md", "unknown", "true"),
    ],
)
def test_the_bench_filter_runs_bench_unless_provably_unchanged(
    tmp_path, changed_file, base_kind, expected
):
    clone, base = _shallow_clone_after_a_change(tmp_path, changed_file)
    base = {"real": base, "empty": "", "zeros": "0" * 40, "unknown": "1" * 40}[
        base_kind
    ]
    out = tmp_path / "github_output"

    res = subprocess.run(
        [sys.executable, str(PATHS_CHANGED), base, "HEAD", *_watched_paths()],
        cwd=clone,
        env={**os.environ, "GITHUB_OUTPUT": str(out)},
        capture_output=True,
        text=True,
    )

    assert res.returncode == 0, res.stderr
    assert res.stdout.startswith(f"{expected}: "), res.stdout
    assert out.read_text() == f"changed={expected}\n"
