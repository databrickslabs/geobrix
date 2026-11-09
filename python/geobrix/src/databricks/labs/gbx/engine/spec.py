"""The engine's rules: what a spec may say, what a value may be, and how a call fails.

Pure Python. The engine core applies ``guard_tokens`` to every argv it runs, so a manifest typo or a forged call
hits the same rules a spec does.
"""

from __future__ import annotations

import re
from typing import Optional, Sequence

from databricks.labs.gbx.manifest import GdalSection, GdalSpec

# A `{param}` placeholder of a `calc_formula` template.
PLACEHOLDER = re.compile(r"\{([a-z_][a-z0-9_]*)\}")

# The argv tokens the engine writes itself; no spec and no caller sets them.
ENGINE_ARGS = frozenset(
    {
        "input",
        "input-format",
        "output",
        "output-format",
        "creation-option",
        "num-threads",
        "quiet",
    }
)
# The algorithms that take --num-threads (the engine sets it to 1: a Spark task is one core).
THREADED = frozenset({"raster reproject"})
# Binders whose value is a file the engine writes under the call's prefix: the one way a deny-listed file argument
# (color-map) may appear, and only with the engine's placeholder (``<text0>``, ...) as its value.
ENGINE_FILE_BINDERS = frozenset({"text_vsimem"})
# Output formats the engine writes: GTiff for rasters, MEM for features. Never VRT or GDALG (both can reference
# other files or run a stored pipeline when opened).
OUTPUT_FORMAT_OF = {"raster": "GTiff", "features": "MEM"}
OUTPUT_FORMATS = frozenset(OUTPUT_FORMAT_OF.values())


class SpecError(ValueError):
    """A spec, or an argv, the engine will not run (outside the allowlist or on the deny list)."""


class ParamError(ValueError):
    """A parameter value the function refuses: raised, naming the parameter and the value."""


class DataError(Exception):
    """A value or a tile the function cannot process: the result is an error tile, carrying this reason."""


def check_value(sql_name: str, param: str, value) -> None:
    """Refuse a string value that could name a file or a URL (GDAL reads files and URLs through some
    arguments: a dataset, an @file). Binder output is not checked: the engine writes it. A `crs` value is judged by
    the canonical CRS resolver's kinds instead (``core.crs.crs_text_kind``), which also refuses a URL, a file name
    and a grid named by path, but not the ``$schema`` URL of a PROJJSON.
    """
    if not isinstance(value, str):
        return
    v = value.strip()
    if v.startswith(("/", "\\", "@")) or "://" in v or ".." in v:
        raise ParamError(
            f"{sql_name}: {param} must not name a file or a URL (got '{value}')"
        )


def _alg_args(section: GdalSection, alg: str, sql_name: str) -> Sequence[str]:
    if alg not in section.allow:
        raise SpecError(f"{sql_name}: the algorithm '{alg}' is not on the allowlist")
    return section.allow[alg]


def validate(fn, section: GdalSection, block: Optional[GdalSpec] = None) -> None:
    """Raise SpecError unless `block` (default: the row's own) is one the engine runs for `fn`: its algorithm and
    every argument on the allowlist, none on the deny list, every param and input the function's own, and each
    both-or-neither (`with`) group symmetric."""
    block = block or fn.gdal
    name = fn.sql_name
    allowed = _alg_args(section, block.alg, name)
    params = {p.name for p in fn.params}
    for param, _kind in block.inputs:
        if param not in params:
            raise SpecError(f"{name}: the input '{param}' is not a parameter")
    by_arg = {a.arg: a for a in block.args}
    for a in block.args:
        if a.arg in ENGINE_ARGS or (
            a.arg in section.deny and a.type not in ENGINE_FILE_BINDERS
        ):
            raise SpecError(f"{name}: the argument '{a.arg}' is deny-listed")
        if a.arg not in allowed:
            raise SpecError(
                f"{name}: the argument '{a.arg}' of '{block.alg}' is not on the allowlist"
            )
        if a.source is not None and a.source not in params:
            raise SpecError(
                f"{name}: '{a.arg}' binds '{a.source}', which is not a parameter"
            )
        if a.formula is not None:
            for ph in sorted(set(PLACEHOLDER.findall(a.formula))):
                # `{nodata}` is the source's NoData value, written by the engine (a result of the source's type)
                if ph not in params and ph != "nodata":
                    raise SpecError(
                        f"{name}: the formula of '{a.arg}' has {{{ph}}}, which is not a parameter"
                    )
        for fname, (template, bands) in a.formulas.items():
            if (
                set(PLACEHOLDER.findall(template)) != set(bands)
                or fname != fname.lower()
            ):
                raise SpecError(
                    f"{name}: the formula '{fname}' of '{a.arg}' must be named in lower case and use exactly its bands"
                )
        if a.when_empty is not None and a.when_empty not in params:
            raise SpecError(
                f"{name}: '{a.arg}' reads '{a.when_empty}' (when_empty), which is not a parameter"
            )
        if a.type == "checked_name" and "*" not in a.map:
            raise SpecError(
                f"{name}: '{a.arg}' is a checked_name, which needs the fixed word GDAL gets under '*' in its map"
            )
        if a.and_ is not None and a.and_ not in params:
            raise SpecError(
                f"{name}: '{a.arg}' reads '{a.and_}' (and), which is not a parameter"
            )
        if a.with_ is not None:
            other = by_arg.get(a.with_)
            if other is None or other.with_ != a.arg:
                raise SpecError(
                    f"{name}: '{a.arg}' goes with '{a.with_}', which does not go with it"
                )


def guard_tokens(
    section: GdalSection,
    alg: str,
    tokens: Sequence[str],
    engine_files: frozenset = frozenset(),
) -> None:
    """The engine core's check of the argv a spec produced (the engine's own tokens are added after it): every
    token is `--name=value` or a bare `--name`, the name on the algorithm's allowlist and off the deny list. A
    deny-listed name passes only when it is in `engine_files` (an argument bound through ``ENGINE_FILE_BINDERS``) and
    its value is the engine's placeholder for a file it wrote.
    """
    allowed = _alg_args(section, alg, alg)
    for t in tokens:
        if not t.startswith("--"):
            raise SpecError(f"{alg}: the token '{t}' is not a long argument")
        name, _, value = t[2:].partition("=")
        engine_file = name in engine_files and value.startswith("<text")
        if name in ENGINE_ARGS or (name in section.deny and not engine_file):
            raise SpecError(f"{alg}: the argument '{name}' is deny-listed")
        if name not in allowed:
            raise SpecError(f"{alg}: the argument '{name}' is not on the allowlist")
