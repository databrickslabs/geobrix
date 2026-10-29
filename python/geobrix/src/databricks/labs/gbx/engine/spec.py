"""The engine's rules: what a spec may say, what a value may be, and how a call fails.

Pure Python. The engine core applies ``guard_tokens`` to every argv it runs, so a manifest typo or a forged call
hits the same rules a spec does. Mirrored by the JVM's ``GdalSpecArgv``.
"""

from __future__ import annotations

from typing import Optional, Sequence

from databricks.labs.gbx.manifest import GdalSection, GdalSpec

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
# Output formats the engine writes: GTiff for rasters, MEM for features. Never VRT or GDALG (both can reference
# other files or run a stored pipeline when opened).
OUTPUT_FORMATS = frozenset({"GTiff", "MEM"})


class SpecError(ValueError):
    """A spec, or an argv, the engine will not run (outside the allowlist or on the deny list)."""


class ParamError(ValueError):
    """A parameter value the function refuses: raised, naming the parameter and the value."""


class DataError(Exception):
    """A value or a tile the function cannot process: the result is an error tile, carrying this reason."""


def check_value(sql_name: str, param: str, value) -> None:
    """Refuse a string value that could name a file or a URL (GDAL reads files and URLs through some
    arguments: a CRS, a dataset, an @file). Binder output is not checked: the engine writes it.
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
        if a.arg in section.deny or a.arg in ENGINE_ARGS:
            raise SpecError(f"{name}: the argument '{a.arg}' is deny-listed")
        if a.arg not in allowed:
            raise SpecError(
                f"{name}: the argument '{a.arg}' of '{block.alg}' is not on the allowlist"
            )
        if a.source is not None and a.source not in params:
            raise SpecError(
                f"{name}: '{a.arg}' binds '{a.source}', which is not a parameter"
            )
        if a.with_ is not None:
            other = by_arg.get(a.with_)
            if other is None or other.with_ != a.arg:
                raise SpecError(
                    f"{name}: '{a.arg}' goes with '{a.with_}', which does not go with it"
                )


def guard_tokens(section: GdalSection, alg: str, tokens: Sequence[str]) -> None:
    """The engine core's check of the argv a spec produced (the engine's own tokens are added after it): every
    token is `--name=value` or a bare `--name`, the name on the algorithm's allowlist and off the deny list.
    """
    allowed = _alg_args(section, alg, alg)
    for t in tokens:
        if not t.startswith("--"):
            raise SpecError(f"{alg}: the token '{t}' is not a long argument")
        name = t[2:].split("=", 1)[0]
        if name in section.deny or name in ENGINE_ARGS:
            raise SpecError(f"{alg}: the argument '{name}' is deny-listed")
        if name not in allowed:
            raise SpecError(f"{alg}: the argument '{name}' is not on the allowlist")
