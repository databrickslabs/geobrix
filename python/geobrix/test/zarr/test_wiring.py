"""The Zarr suite's wiring: extras, the three light-test-dir lists, and the CI locks.

Each of these has been missed before when a light feature was added, and nothing else
fails loudly when it is (a dir that never runs in CI, a dep missing from the CI lock).
"""

import ast
import re
import tomllib
from pathlib import Path

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
    assert "geobrix[zarr_dbr19]" in EXTRAS["light_dbr19_all"]
    assert EXTRAS["light_env6_all"] == ["geobrix[light_dbr19_all]"]


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
