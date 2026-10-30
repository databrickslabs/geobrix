"""The drift test for a GDAL bump: every spec's algorithm, argument names and choices exist in the pinned GDAL.

Reads ``GetUsageAsJSON()`` of the GDAL the light tier runs (the env wheel payload's ``osgeo``). Where ``osgeo`` is
not that GDAL (the macOS host, a non-env wheel, the dev container without ``GBX_NATIVE_ROOT``) every row skips with
the reason the engine gives.
"""

import json

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import run, spec

MAN = manifest.load()
ALGS = sorted(MAN.gdal.allow)


@pytest.fixture(scope="module")
def usage():
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    return {alg: json.loads(run.usage_json(alg)) for alg in ALGS}


def _args(u):
    return {
        a["name"]: a
        for k in ("input_arguments", "output_arguments", "input_output_arguments")
        for a in u.get(k, [])
    }


def test_the_engine_runs_the_pinned_gdal(usage):
    assert run.gdal_release() == MAN.gdal.pin


@pytest.mark.parametrize("alg", ALGS)
def test_every_allowed_and_engine_argument_exists(usage, alg):
    names = set(_args(usage[alg]))
    want = set(MAN.gdal.allow[alg]) | {
        "input",
        "input-format",
        "output",
        "output-format",
        "creation-option",
        "quiet",
    }
    assert want <= names, sorted(want - names)
    assert ("num-threads" in names) == (alg in spec.THREADED)


@pytest.mark.parametrize(
    "fn", [f for f in MAN.functions.values() if f.gdal], ids=lambda f: f.sql_name
)
def test_every_spec_argument_is_canonical_and_its_choices_exist(usage, fn):
    """A spec names each argument by its canonical name, never by an alias (GDAL keeps old names such as
    reproject's --dst-crs only as hidden aliases, which a later release may drop), and every word it passes is
    one of that argument's choices."""
    args = _args(usage[fn.gdal.alg])
    aliases = {al for a in args.values() for al in a.get("aliases", ())}
    for a in fn.gdal.args:
        assert (
            a.arg in args and a.arg not in aliases
        ), f"{fn.sql_name}: '{a.arg}' is not a canonical argument of '{fn.gdal.alg}'"
        choices = args[a.arg].get("choices")
        words = (
            list(a.map.values())
            if a.type == "choice"
            else [a.const] if isinstance(a.const, str) else []
        )
        if choices:
            assert set(words) <= set(choices), (a.arg, words, choices)


def test_the_engines_exception_mode_is_its_own_threads(usage):
    """The engine turns osgeo's exceptions off for its own calls (``run._gdal_status``); in the pinned GDAL that mode
    is the calling thread's alone, so another thread using osgeo meanwhile keeps the process's mode.
    """
    import threading

    gdal = run._gdal()
    before = gdal.GetUseExceptions()
    gdal.UseExceptions()
    seen, inside, read = {}, threading.Event(), threading.Event()

    def other():
        assert inside.wait(10)
        seen["other"] = gdal.GetUseExceptions()
        read.set()

    t = threading.Thread(target=other)
    t.start()
    try:
        with run._gdal_status():
            seen["engine"] = gdal.GetUseExceptions()
            inside.set()
            assert read.wait(10)
    finally:
        t.join(10)
        if not before:
            gdal.DontUseExceptions()  # the process's mode as it was
    assert seen == {"engine": 0, "other": 1}
