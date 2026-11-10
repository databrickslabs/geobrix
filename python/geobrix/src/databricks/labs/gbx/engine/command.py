"""gbx_rst_gdal: a ``gdal raster`` step or pipeline from a caller, checked against an allowlist.

The caller writes what follows ``gdal raster`` on the command line (``slope --unit=percent``, or steps joined by ``!``).
The engine adds the ``read`` and ``write`` steps itself, so the caller never names a file, a format, a process option or
a ``/vsi`` path. Four rules keep a command inside the tile it is given:

- a step is a word on the manifest's ``gdal.command_allow`` list, and nothing else runs (no ``read``, ``write``, ``tee``,
  ``concat``, ``mosaic``, ``calc`` or ``vector`` step);
- an argument is a long argument (``--name=value``) that the step lists, and it is not on the deny list (``--config``,
  ``--input``, ``--output``, ``--like``, ``--mask``, a ``--creation-option`` ...). ``--geometry`` is the one deny-listed
  name a step takes: its value is a WKT polygon that the engine writes again from the parsed geometry;
- a value has the grammar of its kind (a number, a word, a list of numbers, a mapping expression ...), so it holds no
  path, URL, ``@file``, quote or step separator;
- a CRS value goes through the canonical CRS resolver, which refuses a file name, a URL and a grid named by path.

Pure Python: the checks run on the host, and the pipeline's tokens are the only thing the engine hands to GDAL.
"""

from __future__ import annotations

import re
from typing import Callable, List, Mapping, Sequence

from databricks.labs.gbx.engine.spec import ENGINE_ARGS, ParamError

PIPELINE_ALG = "raster pipeline"
# The words that join two steps; a quoted one is a separator as well (the quotes are gone when the words are split).
SEPARATORS = ("!", "|")
# The largest command, the most steps in it, and the longest value of a word.
MAX_COMMAND = 64 * 1024
MAX_STEPS = 32
# The steps GDAL cannot stream: each holds its result in memory while the pipeline runs (the output estimate counts them).
MATERIALIZING = frozenset(
    {"sieve", "fill-nodata", "proximity", "viewshed", "neighbors"}
)

_NUM = r"(?:[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?|[+-]?(?:nan|inf|infinity))"
_INT = r"[+-]?\d+"
_WORD = r"[A-Za-z0-9_.+\-]{1,64}"
_SIZE = r"\d+%?"
# A reclassify mapping or a neighbors kernel: numbers, ranges, the words NO_DATA / DEFAULT / PASSTHROUGH, names. Never a
# slash, backslash, @, quote, `!` or `|`.
_EXPR = r"[A-Za-z0-9_.+\-,;=\[\]() ]{1,4096}"


def _list(item: str) -> "re.Pattern":
    return re.compile(rf"^{item}(?:,{item})*$", re.IGNORECASE)


_KIND = {
    "integer": re.compile(rf"^{_INT}$"),
    "number": re.compile(rf"^{_NUM}$", re.IGNORECASE),
    "integers": _list(_INT),
    "numbers": _list(_NUM),
    "word": re.compile(rf"^{_WORD}$"),
    "words": _list(_WORD),
    "sizes": _list(_SIZE),
    "expr": re.compile(rf"^{_EXPR}$"),
}
_KIND_NAME = {
    "integer": "a whole number",
    "number": "a number",
    "integers": "whole numbers separated by commas",
    "numbers": "numbers separated by commas",
    "word": "a single word of letters, digits and _ . + -",
    "words": "words separated by commas",
    "sizes": "whole numbers or percentages separated by commas",
    "expr": "an expression of letters, digits, spaces and _ . + - , ; = [ ] ( )",
}
_LONG = re.compile(r"^--([a-z][a-z0-9-]*)(?:=(.*))?$", re.DOTALL)


def _refuse(why: str, fix: str = "") -> ParamError:
    return ParamError(f"gbx_rst_gdal: {why}" + (f". Fix: {fix}" if fix else ""))


def tokenize(command: str) -> List[List[str]]:
    """The command's steps, each ``[name, --arg=value, ...]``, split quote-aware.

    Single and double quotes group a word; inside double quotes a backslash escapes the next character. Steps split on a bare
    ``!`` or ``|``. Raises ParamError for an unterminated quote, ``[`` or ``]`` as a word (a nested pipeline), an empty
    step, a command over ``MAX_COMMAND`` characters or ``MAX_STEPS`` steps, and any character outside printable ASCII
    and white space.
    """
    if not isinstance(command, str):
        raise _refuse(f"the command must be text (got {type(command).__name__})")
    if len(command) > MAX_COMMAND:
        raise _refuse(
            f"the command has {len(command)} characters, over the limit of {MAX_COMMAND}"
        )
    bad = next(
        (c for c in command if not (c in "\t\n\r" or " " <= c <= "~")),
        None,
    )
    if bad is not None:
        raise _refuse(
            f"the command has a character that is not printable ASCII ({bad!r})",
            "write the command with plain ASCII text",
        )
    words, cur, quote, started, i = [], [], None, False, 0
    while i < len(command):
        c = command[i]
        if quote:
            if c == quote:
                quote = None
            elif c == "\\" and quote == '"' and i + 1 < len(command):
                i += 1
                cur.append(command[i])
            else:
                cur.append(c)
        elif c in "\"'":
            quote, started = c, True
        elif c.isspace():
            if started:
                words.append("".join(cur))
                cur, started = [], False
        else:
            cur.append(c)
            started = True
        i += 1
    if quote:
        raise _refuse(
            f"the command has an unterminated quote ({command[:60]!r})",
            "close the quote",
        )
    if started:
        words.append("".join(cur))
    steps: List[List[str]] = []
    step: List[str] = []
    for w in words:
        if w in ("[", "]"):
            raise _refuse(
                f"nested pipelines ('{w}') are not supported",
                "write the steps in one line, joined by !",
            )
        if w in SEPARATORS:
            if not step:
                raise _refuse("the command has an empty step")
            steps.append(step)
            step = []
        else:
            step.append(w)
    if not step:
        raise _refuse("the command is empty or ends with a separator")
    steps.append(step)
    if len(steps) > MAX_STEPS:
        raise _refuse(
            f"the command has {len(steps)} steps, over the limit of {MAX_STEPS}"
        )
    return steps


def _polygon_wkt(value: str) -> str:
    """The WKT polygon `value` holds, written again from the parsed geometry: GDAL's own parser accepts text after the
    geometry (and a GeoJSON or a file name in its place), so what it gets is what shapely read, not what the caller wrote.
    """
    import math

    if not value.lstrip().upper().startswith(("POLYGON", "MULTIPOLYGON")):
        raise _refuse(
            "geometry must be a WKT POLYGON or MULTIPOLYGON, not a file or other text",
            'pass the polygon as WKT, e.g. --geometry="POLYGON((0 0,1 0,1 1,0 0))"',
        )
    try:
        import shapely

        geom = shapely.from_wkt(value)
    except Exception:  # noqa: BLE001 - any parse failure is this value's
        geom = None
    if (
        geom is None
        or geom.geom_type not in ("Polygon", "MultiPolygon")
        or geom.is_empty
        or not all(math.isfinite(v) for v in shapely.get_coordinates(geom).ravel())
    ):
        raise _refuse(
            "geometry must be a non-empty WKT POLYGON or MULTIPOLYGON with finite coordinates"
        )
    return shapely.to_wkt(geom, rounding_precision=-1, trim=True, output_dimension=2)


def _value(
    step: str, key: str, kind: str, value: str, canonical: Callable[[str], str]
) -> str:
    """One argument's value text, checked against its kind (a CRS is rewritten, a polygon written again)."""
    if kind == "crs":
        try:
            return canonical(value)
        except (
            Exception
        ) as exc:  # noqa: BLE001 - any refusal or parse failure is this argument's
            raise _refuse(
                f"{key} of step '{step}': {str(exc).removeprefix('resolve_crs: ')}"
            ) from None
    if kind == "wkt":
        return _polygon_wkt(value)
    if not _KIND[kind].match(value):
        raise _refuse(
            f"the value of '{key}' in step '{step}' must be {_KIND_NAME[kind]} (got {value[:60]!r})",
            "pass the value without a path, quote or separator",
        )
    return value


def check_step(
    step: Sequence[str],
    allow: Mapping[str, Mapping[str, str]],
    deny,
    canonical: Callable[[str], str],
) -> List[str]:
    """One step's tokens, refused unless its name and every argument are on the allowlist and every value has the
    grammar of its kind. `allow` maps a step to its arguments and their kinds."""
    name, args = step[0], step[1:]
    if name not in allow:
        raise _refuse(
            f"the step '{name}' is not one gbx_rst_gdal runs",
            f"use one of {', '.join(sorted(allow))}",
        )
    out, seen = [name], set()
    for t in args:
        m = _LONG.match(t)
        if m is None:
            raise _refuse(
                f"'{t}' in step '{name}' is not a long argument (--name=value); short flags and loose values are not accepted",
                "write each argument as --name=value",
            )
        key, value = m.group(1), m.group(2)
        kinds = allow[name]
        if (
            key in ENGINE_ARGS
            or (key in deny and kinds.get(key) != "wkt")
            or key not in kinds
        ):
            raise _refuse(
                f"the argument '{key}' of step '{name}' is not accepted",
                f"use one of {', '.join('--' + k for k in sorted(kinds))}",
            )
        if key in seen:
            raise _refuse(f"the argument '{key}' of step '{name}' is given twice")
        seen.add(key)
        kind = kinds[key]
        if kind == "flag":
            if value is None or value.lower() == "true":
                out.append(f"--{key}")
            elif value.lower() != "false":
                raise _refuse(
                    f"the flag '{key}' of step '{name}' takes true or false (got {value[:60]!r})"
                )
            continue
        if value is None or value == "":
            raise _refuse(
                f"the argument '{key}' of step '{name}' needs a value, written with = and no space ('--{key}=<value>')",
                "join the argument and its value with =",
            )
        out.append(f"--{key}={_value(name, key, kind, value, canonical)}")
    return out


def pipeline_tokens(
    command: str,
    section,
    canonical: Callable[[str], str],
    out_tokens: Sequence[str],
) -> List[str]:
    """The argv of ``gdal raster pipeline`` for `command`: the engine's ``read`` step, the caller's checked steps, and
    the engine's ``write`` step with `out_tokens` (``--output=...``, the format and the creation options). A
    reprojection runs on one thread: a Spark task is one core."""
    tokens = ["read", "--input=<input>", "--input-format=GTiff"]
    for step in tokenize(command):
        tokens += ["!"] + check_step(
            step, section.command_allow, section.deny, canonical
        )
        if step[0] == "reproject":
            tokens.append("--num-threads=1")
    return tokens + ["!", "write"] + list(out_tokens)


def steps_of(tokens: Sequence[str]) -> List[List[str]]:
    """The steps of a pipeline's tokens (``read``, the caller's steps, ``write``), split on the bare ``!`` the engine wrote."""
    steps: List[List[str]] = [[]]
    for t in tokens:
        if t == "!":
            steps.append([])
        else:
            steps[-1].append(t)
    return steps
