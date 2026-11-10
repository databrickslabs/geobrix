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


# The arguments the engine itself writes for an algorithm, by the output kind of the row that uses it. A JSON row reads
# the text from `output-string`, and `raster info` has no output file, creation option or --quiet.
_ENGINE_ARGS_BY_KIND = {
    "raster": {
        "input",
        "input-format",
        "output",
        "output-format",
        "creation-option",
        "quiet",
    },
    "features": {"input", "input-format", "output", "output-format", "quiet"},
    "json": {"input", "input-format", "output-format", "output-string"},
}
_KIND_OF = {f.gdal.alg: f.gdal.out_kind for f in MAN.functions.values() if f.gdal}


@pytest.mark.parametrize("alg", ALGS)
def test_every_allowed_and_engine_argument_exists(usage, alg):
    names = set(_args(usage[alg]))
    want = set(MAN.gdal.allow[alg]) | _ENGINE_ARGS_BY_KIND[_KIND_OF[alg]]
    assert want <= names, sorted(want - names)
    # `raster viewshed` lists --num-threads, but GDAL refuses it outside its cumulative mode, which the engine does not use
    assert ("num-threads" in names) == (
        alg in spec.THREADED or alg == "raster viewshed"
    )


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
        if a.const == "input" and a.arg == "output-data-type":
            # the engine's own word (argv.py): the source's band type, written as that type's GDAL name when the
            # argv is built, so it is no choice of the spec to check
            words = []
        if choices:
            assert set(words) <= set(choices), (a.arg, words, choices)


# The kind of value each GDAL usage type takes in a `gbx_rst_gdal` command (`gdal.command_allow`). A string argument has
# no type of its own in the usage: it is a word, a list of words, a size, an expression, a CRS or a polygon.
_USAGE_KINDS = {
    "boolean": {"flag"},
    "integer": {"integer"},
    "real": {"number"},
    "integer_list": {"integers"},
    "real_list": {"numbers"},
    "string": {"word", "number", "expr", "crs", "wkt"},
    "string_list": {"words", "sizes", "expr"},
}
_COMMAND_STEPS = sorted(MAN.gdal.command_allow)


@pytest.fixture(scope="module")
def step_usage():
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    return {
        step: json.loads(run.usage_json(f"raster {step}")) for step in _COMMAND_STEPS
    }


@pytest.mark.parametrize("step", _COMMAND_STEPS)
def test_every_command_step_and_argument_exists_and_is_canonical(step_usage, step):
    """Each `gbx_rst_gdal` step is `raster <step>` in the pinned GDAL, each argument is its canonical name (never an
    alias), and its kind of value is one the argument's usage type takes. A dataset argument is never allowed (it would
    name a file); no argument is one the engine writes."""
    args = _args(step_usage[step])
    aliases = {al for a in args.values() for al in a.get("aliases", ())}
    for name, kind in MAN.gdal.command_allow[step].items():
        assert (
            name in args and name not in aliases
        ), f"{step}: '{name}' is not a canonical argument of 'raster {step}'"
        gtype = args[name]["type"]
        assert (
            gtype in _USAGE_KINDS and kind in _USAGE_KINDS[gtype]
        ), f"{step}: '{name}' is {gtype} in GDAL; kind '{kind}' does not take it"
        assert name not in spec.ENGINE_ARGS, (step, name)
        choices = args[name].get("choices")
        if choices and kind == "word":
            assert all(
                c.replace("-", "").replace("_", "").replace(".", "").isalnum()
                for c in choices
            ), (step, name, choices)


def test_the_crs_arguments_are_the_ones_whose_name_says_so():
    """`command_crs_args` is every CRS-kind argument of the allowed steps, and GDAL's CRS arguments are the `*-crs` ones."""
    named = {
        name
        for kinds in MAN.gdal.command_allow.values()
        for name in kinds
        if name.endswith("-crs")
    }
    assert (
        MAN.gdal.command_crs_args
        == named
        == {
            "input-crs",
            "output-crs",
            "bbox-crs",
            "geometry-crs",
        }
    )


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
