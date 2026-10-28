"""Guards for the sharded light CI job.

The light tier runs as parallel shards (the `pyrx` job matrix in build_main.yml). These tests
read the workflow and the two CI actions and fail unless:

* the shards together hold exactly the dirs of `_LIGHT_TEST_DIRS` (test/conftest.py), each in
  one shard, so a new light dir that no shard runs, or that two shards run, goes red;
* the heavy build ignores the same set (`LIGHT_IGNORES`) and the unsharded fallback line of the
  light action lists the same set;
* `pyvx` runs before `bench` in the same shard (the PROJ state bench relies on);
* `build` waits for the whole sharded job, the shards actually receive their `dirs`, and
  nothing can quietly drop a shard or a dir: the matrix has no `exclude`/`include`, and the job
  and the action step carry no `if`, `continue-on-error` or skip input.
"""

import ast
import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFTEST = REPO_ROOT / "python" / "geobrix" / "test" / "conftest.py"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "build_main.yml"
LIGHT_ACTION = REPO_ROOT / ".github" / "actions" / "pyrx_build" / "action.yml"
HEAVY_ACTION = REPO_ROOT / ".github" / "actions" / "python_build" / "action.yml"

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


def test_heavy_waits_for_every_shard_and_nothing_can_hide_one():
    workflow = yaml.safe_load(WORKFLOW.read_text())
    needs = workflow["jobs"]["build"]["needs"]
    assert "pyrx" in ([needs] if isinstance(needs, str) else needs)

    job = _pyrx_job()
    assert job["strategy"]["fail-fast"] is False
    # Only these two dimensions: an `exclude` (or `include`) could remove or add a shard
    # that the guard above never sees, because it reads `shard` alone.
    assert set(job["strategy"]["matrix"]) == {"python", "shard"}
    # A job-level condition or continue-on-error would let a failing shard pass the gate.
    assert not {"if", "continue-on-error"} & set(job)

    (step,) = [
        s for s in job["steps"] if s.get("uses") == "./.github/actions/pyrx_build"
    ]
    assert step["with"] == {
        "python_version": "${{ matrix.python }}",
        "dirs": "${{ matrix.shard.dirs }}",
    }
    assert not {"if", "continue-on-error"} & set(step)
