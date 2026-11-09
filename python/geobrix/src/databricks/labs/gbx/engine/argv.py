"""Binders and the argv: (a row's spec, the call's values, its input rasters) -> the tokens GDAL parses.

``test/engine/fixtures/argv_golden.json`` pins the tokens.
The order is fixed: the inputs, ``--input-format``, the spec's arguments in template order, then the output, its
format and creation options, ``--num-threads`` where the algorithm has it, and ``--quiet``. Every token is
``--name=value`` or a bare ``--name``, so a value can never be read as a flag or as a pipeline step.
"""

from __future__ import annotations

import functools
import json
import math
import re
import string
import threading
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine.spec import (
    ENGINE_FILE_BINDERS,
    PLACEHOLDER,
    THREADED,
    DataError,
    ParamError,
    check_value,
    guard_tokens,
)

_ITEMSIZE = {
    "Byte": 1,
    "UInt8": 1,
    "Int8": 1,
    "UInt16": 2,
    "Int16": 2,
    "Float16": 2,
    "UInt32": 4,
    "Int32": 4,
    "Float32": 4,
    "CInt16": 4,
    "UInt64": 8,
    "Int64": 8,
    "Float64": 8,
    "CInt32": 8,
    "CFloat32": 8,
    "CFloat64": 16,
}
_VARS = string.ascii_uppercase
# A map-algebra variable: one capital letter that is not part of a longer name, a number or an indexed V[n].
_VAR = re.compile(r"(?<![A-Za-z0-9_\]])([A-Z])(?![A-Za-z0-9_\[])")
# The envelope's closed vocabulary: calc, and per variable V_index / V_band. There is no dialect: GDAL's muparser
# grammar evaluates every expression.
_ENVELOPE_KEYS = frozenset(
    {"calc"} | {f"{v}_{k}" for v in _VARS for k in ("index", "band")}
)
_TEMPLATE_CACHE_SIZE = 512
_templates: "OrderedDict" = OrderedDict()
_templates_lock = threading.Lock()
_MISSING = object()


@dataclass(frozen=True)
class RasterProps:
    """What the engine reads from an input's header before it binds: size, band count and band-1 type."""

    width: int
    height: int
    bands: int
    dtype: str  # GDAL type name
    nodata: Optional[float] = (
        None  # band 1's declared NoData value, None when it declares none
    )


@dataclass(frozen=True)
class _Calc:
    expr: str
    variables: Tuple[
        Tuple[str, int], ...
    ]  # (variable, input index), sorted by variable


def fmt_double(x: float) -> str:
    """A double as the engine prints it: the shortest round-trip digits, plain notation, no trailing zeros."""
    if x == 0:
        return "0"
    return format(Decimal(repr(float(x))).normalize(), "f")


# The NoData value a source of a type gets when it declares none (``pyrx.core._nodata.fallback_nodata``, by GDAL type).
_NUMPY_OF = {
    "Byte": "uint8",
    "UInt16": "uint16",
    "Int16": "int16",
    "UInt32": "uint32",
    "Int32": "int32",
    "Float32": "float32",
    "Float64": "float64",
}


def source_nodata(props: "RasterProps") -> float:
    """The NoData value of a result that keeps the source's type: the source's declared value, else its type's
    fallback (0 for Byte, 65535 for UInt16, the type's minimum for the signed and float types).
    """
    if props.nodata is not None:
        return props.nodata
    from databricks.labs.gbx.pyrx.core._nodata import fallback_nodata

    return fallback_nodata(_NUMPY_OF.get(props.dtype, "uint8"))


def _nodata_text(x: float) -> str:
    return "nan" if math.isnan(x) else fmt_double(x)


def _fmt_const(v: Any) -> str:
    if isinstance(v, float):
        return fmt_double(v)
    return str(v)


def _is_null(v: Any) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))


def _fail(fn, arg, message: str):
    """The binder failure of `arg`: an error tile reason for a `bad: data` argument, else a parameter error."""
    return (DataError if arg.bad == "data" else ParamError)(f"{fn.sql_name}: {message}")


def _bind_value(fn, arg, value) -> Optional[str]:
    """One argument's value text, or None to leave a bare flag out (a false `bool`)."""
    t = arg.type
    if t == "double":
        x = float(value)
        if math.isinf(x):
            raise _fail(fn, arg, f"{arg.source} must be finite (got {value})")
        return fmt_double(x)
    if t == "int":
        return str(int(value))
    if t == "bool":
        return "" if bool(value) else None
    if t == "choice":
        word = str(value).strip().lower()
        choices = {k.lower(): v for k, v in arg.map.items()}
        if word not in choices:
            allowed = ", ".join(f"'{k}'" for k in arg.map)
            raise _fail(
                fn, arg, f"{arg.source} must be one of {allowed} (got '{value}')"
            )
        return choices[word]
    if t == "crs":
        # Gated by the canonical CRS resolver's kinds (``core.crs.crs_text_kind``, run by ``canonical_crs``), not by
        # ``check_value``: a value that IS a URL, a file name or a grid named by path is refused there, while a
        # PROJJSON's ``$schema`` URL or a WKT2 ``URI[...]`` (never fetched) does not refuse the CRS.
        try:
            return canonical_crs(str(value))
        except (
            Exception
        ) as exc:  # noqa: BLE001 - any refusal or parse failure is this value's
            raise _fail(
                fn, arg, f"{arg.source}: {str(exc).removeprefix('resolve_crs: ')}"
            ) from None
    raise ValueError(f"{fn.sql_name}: binder '{t}' is not built in this release")


@functools.lru_cache(maxsize=1)
def _osgeo():
    """osgeo (GDAL's own CRS parser), or None where this process has no osgeo."""
    from databricks.labs.gbx import _osgeo as osgeo

    try:
        return osgeo.bindings("the GDAL spec engine")
    except ImportError:
        return None


CRS_BUILD_FIX = (
    "Fix: pass EPSG:<code>, WKT or PROJJSON that GDAL can build; for a grid, register its directory with "
    "register_proj_grids, or mark it optional (@name)"
)


@functools.lru_cache(maxsize=256)
def canonical_crs(text: str) -> str:
    """The one form a target CRS takes in the argv: ``AUTHORITY:CODE`` when the parsed CRS carries
    that authority node exactly (no identification, no fuzzy match), else its WKT.

    The value must be a kind ``core.crs.crs_text_kind`` accepts (never a file, a URL or a grid named by path), and
    it is parsed by OSR with file and network access off, then exported by OSR: the calls the JVM's
    ``SpatialRefOps.resolveCrs`` / ``crsToCanonical`` make on the same GDAL, so the heavy and light functions bind the same text.
    The parse is judged as the JVM judges it, with osr's exceptions off: a failing return, or a failure GDAL
    reports during it (PROJ cannot build the CRS, e.g. a required grid it cannot find, which would otherwise
    leave a reprojection without its datum shift), refuses the value. Without osgeo (a host that cannot run the
    engine) the canonical resolver and the strict authority rule stand in.
    """
    from databricks.labs.gbx.core.crs import _is_intlike, crs_text_kind, resolve_crs

    v = text.strip()
    if not _is_intlike(v):
        crs_text_kind(v)
    b = _osgeo()
    if b is None:
        crs = resolve_crs(v)
        auth = crs.to_authority(confidence_threshold=100)
        return f"{auth[0]}:{auth[1]}" if auth else crs.to_wkt()
    gdal, osr = b.gdal, b.osr
    with osr.ExceptionMgr(useExceptions=False), gdal.ExceptionMgr(useExceptions=False):
        gdal.ErrorReset()
        sr = osr.SpatialReference()
        if _is_intlike(v):
            rc = sr.ImportFromEPSG(int(v))
        else:
            rc = sr.SetFromUserInput(
                v, options=["ALLOW_FILE_ACCESS=NO", "ALLOW_NETWORK_ACCESS=NO"]
            )
        if rc != 0 or gdal.GetLastErrorType() >= gdal.CE_Failure:
            why = gdal.GetLastErrorMsg() or f"OGR error {rc}"
            raise ValueError(f"GDAL cannot build the CRS ({why}). {CRS_BUILD_FIX}")
        name, code = sr.GetAuthorityName(None), sr.GetAuthorityCode(None)
        return f"{name}:{code}" if name and code else sr.ExportToWkt()


_TEXT_MAX = 64 * 1024
# The color names GDAL's color-relief reads in place of R G B (case-insensitive); any other name it turns into a
# transparent color without an error, so the engine refuses it here.
COLOR_NAMES = (
    "white",
    "black",
    "red",
    "green",
    "blue",
    "yellow",
    "magenta",
    "cyan",
    "grey",
    "gray",
    "orange",
    "purple",
    "brown",
)
_NUMBER = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?%?$")
_CHANNEL = re.compile(r"^\d{1,3}$")
_FILE_LIKE = re.compile(r"[/\\]|\.(txt|clr|csv)$", re.I)
COLOR_TABLE_FIX = (
    "Fix: pass the table's text, one 'value R G B [A]' line per entry (a color name may replace R G B, "
    "'nv R G B' colors NoData, 'N%' is a share of the value range), not a file path; read a file's text first"
)


def _color_error(sql_name: str, param: str, why: str) -> ParamError:
    return ParamError(f"{sql_name}: {param} {why}. {COLOR_TABLE_FIX}")


@functools.lru_cache(maxsize=256)
def _color_table(sql_name: str, param: str, text: str) -> Tuple[str, bool]:
    """(the table in the canonical form GDAL reads, whether its entries carry an alpha column).

    Every line is checked: ``value R G B [A]`` (``value`` a number or ``N%``), ``value <color name>``, or the NoData
    color ``nv R G B [A]``; blank lines and ``#`` comments are skipped, commas count as spaces. A table with no
    ``nv`` line gets one that is transparent (RGBA) or black (RGB), because GDAL otherwise colors a NoData pixel like
    the lowest value. Raises ParamError, naming the first bad line, with a Fix.
    """
    if len(text) > _TEXT_MAX or "\x00" in text:
        raise _color_error(
            sql_name, param, f"must be a color table of at most {_TEXT_MAX} characters"
        )
    if not text.strip():
        raise _color_error(
            sql_name, param, "is empty: it needs at least one color entry"
        )
    if (
        "\n" not in text.strip()
        and _FILE_LIKE.search(text.strip())
        and " " not in text.strip()
    ):
        raise _color_error(
            sql_name,
            param,
            f"is a file path ('{text.strip()[:60]}'), not the table's text",
        )
    out: List[str] = []
    has_nv = has_alpha = False
    entries = 0
    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [t for t in re.split(r"[\s,]+", line) if t]
        key, rest = parts[0], parts[1:]

        def bad(why: str, n=n, line=line):
            return _color_error(
                sql_name,
                param,
                f"has a line that is not a color entry (line {n}: '{line[:60]}'): {why}",
            )

        nv = key.lower() in ("nv", "nodata")
        if not nv and not _NUMBER.match(key):
            raise bad("it must start with a number, a percent such as 50%, or nv")
        if len(rest) == 1 and not rest[0].isdigit():
            if rest[0].lower() not in COLOR_NAMES:
                raise bad(
                    f"'{rest[0]}' is not a color name GDAL knows ({', '.join(COLOR_NAMES)})"
                )
            color = rest[0].lower()
        elif len(rest) in (3, 4) and all(
            _CHANNEL.match(t) and int(t) <= 255 for t in rest
        ):
            color = " ".join(str(int(t)) for t in rest)
            has_alpha = has_alpha or len(rest) == 4
        else:
            raise bad(
                "after the value it needs R G B, R G B A (each 0 to 255), or one color name"
            )
        if nv:
            has_nv = True
            out.append(f"nv {color}")
        else:
            entries += 1
            out.append(f"{key} {color}")
    if entries == 0:
        raise _color_error(
            sql_name, param, "has no color entries (only nv, comments or blank lines)"
        )
    if not has_nv:
        out.append("nv 0 0 0 0" if has_alpha else "nv 0 0 0")
    return "\n".join(out) + "\n", has_alpha


def check_color_table(sql_name: str, param: str, text) -> str:
    """The color table's text in the canonical form, or a ParamError naming the first bad line, with a Fix."""
    return _color_table(sql_name, param, str(text))[0]


def text_values(fn, values: Mapping[str, Any]) -> List[str]:
    """The texts the call's ``text_vsimem`` arguments write, in template order (the engine writes each to a file under
    the call's prefix and names it in ``build(..., texts=)``). A NULL value is not written: the result is NULL.
    """
    return [
        check_color_table(fn.sql_name, a.source, values.get(a.source))
        for a in fn.gdal.args
        if a.type == "text_vsimem" and not _is_null(values.get(a.source))
    ]


def _calc_binding(fn, arg, value, inputs: Sequence[RasterProps]) -> _Calc:
    """The map-algebra envelope -> GDAL `raster calc`'s muparser expression and one `--input=V=` per variable.

    Today's envelope (``{"calc": ..., "A_index": i, "A_band": b, ...}``) or a bare expression. Variable V reads
    band 1 of input V's ordinal unless the envelope says otherwise; every bare V in the expression is written
    V[b], so the output is one band. There is one grammar, GDAL's muparser, so the envelope takes no dialect.
    ``extra_options`` (gdal_calc flags) is refused.
    """
    s = str(value).strip()
    if len(inputs) > len(_VARS):
        raise ParamError(
            f"{fn.sql_name}: map algebra takes at most {len(_VARS)} rasters (A to Z); got {len(inputs)}"
        )
    bindings: Dict[str, Tuple[int, int]] = {}
    if s.startswith("{"):
        try:
            env = json.loads(s)
        except ValueError as exc:
            raise ParamError(
                f"{fn.sql_name}: the expression is not valid JSON ({exc})"
            ) from None
        if not isinstance(env, dict) or not isinstance(env.get("calc"), str):
            raise ParamError(
                f'{fn.sql_name}: the expression envelope needs a "calc" string, e.g. {{"calc": "A * 2"}}'
            )
        if "extra_options" in env:
            raise ParamError(
                f"{fn.sql_name}: extra_options is not supported (gdal_calc flags have no place in GDAL's "
                "raster calc). Fix: express the computation in calc"
            )
        if "dialect" in env:
            raise ParamError(
                f"{fn.sql_name}: the expression envelope takes no dialect: GeoBrix evaluates every expression with "
                'GDAL\'s muparser grammar. Fix: remove "dialect" and write the expression in that grammar '
                "(for example (A - B) / (A + B))"
            )
        unknown = sorted(set(env) - _ENVELOPE_KEYS)
        if unknown:
            raise ParamError(
                f"{fn.sql_name}: the expression envelope has unknown key(s) {', '.join(unknown)}; it takes calc, "
                "and V_index / V_band for a variable V from A to Z"
            )
        for ordinal, var in enumerate(_VARS):
            ik, bk = f"{var}_index", f"{var}_band"
            if ik not in env and bk not in env:
                continue
            idx, band = env.get(ik, ordinal), env.get(bk, 1)
            if type(idx) is not int or type(band) is not int:
                raise ParamError(
                    f"{fn.sql_name}: {ik} and {bk} must be integers (got {idx}, {band})"
                )
            if not 0 <= idx < len(inputs):
                raise ParamError(
                    f"{fn.sql_name}: {ik}={idx} is out of range for {len(inputs)} raster(s)"
                )
            if not 1 <= band <= inputs[idx].bands:
                raise ParamError(
                    f"{fn.sql_name}: {bk}={band} is out of range: raster {idx} has "
                    f"{inputs[idx].bands} band(s)"
                )
            bindings[var] = (idx, band)
        s = env["calc"]
    check_value(fn.sql_name, arg.source, s)
    positional = {_VARS[i]: (i, 1) for i in range(len(inputs))}
    resolved = {**positional, **bindings}
    unbound = sorted(set(_VAR.findall(s)) - set(resolved))
    if unbound:
        raise ParamError(
            f"{fn.sql_name}: the expression uses {', '.join(unbound)}, which no raster is bound to"
        )
    expr = _VAR.sub(lambda m: f"{m.group(1)}[{resolved[m.group(1)][1]}]", s)
    return _Calc(expr, tuple((v, i) for v, (i, _) in sorted(resolved.items())))


def _as_list(value) -> Optional[List[float]]:
    """The finite numbers of a list value, sorted and without repeats, or None when it holds none.

    The value is an ``ARRAY<DOUBLE>`` (a list, a tuple or an array) or a comma-separated STRING of numbers. NULL and
    NaN elements are dropped; an infinite element is refused by the caller. A list that is NULL, empty or holds only
    NULLs counts as NULL.
    """
    if value is None:
        return None
    if getattr(value, "ndim", 0) > 0:  # an array
        value = value.tolist()
    items = (
        value
        if isinstance(value, (list, tuple))
        else [t for t in str(value).replace(" ", "").split(",") if t != ""]
    )
    nums = sorted({float(v) for v in items if not _is_null(v) and str(v).strip() != ""})
    return nums or None


def level_snapper(fn, values: Mapping[str, Any]):
    """``(layer field, snap)`` for a row whose features carry a contour level, or None.

    GDAL's output layer holds each level rounded to six decimals (0.1 + 0.2 reads 0.3, and 0.1234567 reads 0.123457),
    so a field read from it is not the level the call asked for. ``snap(value)`` gives that level back: the nearest
    fixed level, or for an interval the nearest ``offset + k * interval``, when it lies within the rounding (1e-6).
    """
    by = {a.arg: a for a in fn.gdal.args}
    name, lv = by.get("elevation-name"), by.get("levels")
    if name is None or lv is None or lv.type != "double_list":
        return None
    nums = _as_list(values.get(lv.source))
    if nums is None:
        iv, off = by.get("interval"), by.get("offset")
        if iv is None or _is_null(values.get(iv.source)):
            return None
        step = float(values[iv.source])
        base = 0.0
        if off is not None and not _is_null(values.get(off.source)):
            base = float(values[off.source])

        def level_of(v: float) -> float:
            return base + round((v - base) / step) * step

    else:
        import bisect

        def level_of(v: float) -> float:
            i = bisect.bisect_left(nums, v)
            near = nums[max(i - 1, 0) : i + 1]
            return min(near, key=lambda x: abs(x - v))

    def snap(v):
        if v is None:
            return v
        level = level_of(v)
        return level if abs(level - v) <= 1e-6 else v

    return name.map["*"], snap


def _has_items(value) -> bool:
    """True when a list value holds a number (a value that is no list of numbers is judged where it is bound)."""
    try:
        return _as_list(value) is not None
    except ValueError:
        return False


def _bind_list(fn, arg, value) -> Optional[str]:
    """A ``double_list`` value as ``a,b,c``: sorted, without repeats and NULLs (GDAL refuses an unsorted list or a
    repeat); None when the list is NULL or empty. An infinite element is refused, naming it.
    """
    try:
        nums = _as_list(value)
    except ValueError:
        raise _fail(
            fn, arg, f"{arg.source} must be a list of numbers (got {value!r})"
        ) from None
    if nums is None:
        return None
    bad = next((x for x in nums if math.isinf(x)), None)
    if bad is not None:
        raise _fail(fn, arg, f"{arg.source} must be finite (got {bad})")
    return ",".join(fmt_double(x) for x in nums)


def _band(fn, param, v, props: RasterProps) -> int:
    """A band number of the input: a whole number from 1 to its band count."""
    n = int(v)
    if n != float(v) or n < 1:
        raise ParamError(f"{fn.sql_name}: {param}={v} is not a band number (1 or more)")
    if n > props.bands:
        raise ParamError(
            f"{fn.sql_name}: {param}={v} is out of range: the raster has {props.bands} band(s)"
        )
    return n


def _formula_binding(fn, arg, values, inputs: Sequence[RasterProps]) -> Optional[_Calc]:
    """A fixed formula with the call's band numbers, constants and operator written into it, over input A.

    ``calc_formula``: the arg's template, each ``{param}`` replaced by the call's value (an INT param is a band
    number, written ``A[n]`` where the template says so; a DOUBLE a finite number; a STRING one of the arg's ``map``
    words). ``calc_named``: the formula the ``from`` param names, with each of its band names replaced by ``A[n]``
    from the band map the ``and`` param holds. Only these values reach the expression, so no caller text does. None
    when a placeholder value is NULL: the arg's null rule then applies.
    """
    params = {p.name: p for p in fn.params}
    if arg.type == "calc_named":
        if _is_null(values.get(arg.source)):
            return None
        name = str(values.get(arg.source)).strip().lower()
        if name not in arg.formulas:
            known = ", ".join(sorted(arg.formulas))
            raise ParamError(
                f"{fn.sql_name}: {arg.source} '{values.get(arg.source)}' is not one of {known}"
            )
        template, bands = arg.formulas[name]
        if _is_null(values.get(arg.and_)):
            return None
        bmap = {str(k).lower(): v for k, v in dict(values.get(arg.and_)).items()}
        subs = {}
        for b in bands:
            if b not in bmap:
                have = ", ".join(sorted(bmap))
                raise ParamError(
                    f"{fn.sql_name}: the formula '{name}' needs the band '{b}' in {arg.and_}; got {have or 'none'}"
                )
            subs[b] = f"A[{_band(fn, f'{arg.and_}[{b}]', bmap[b], inputs[0])}]"
    else:
        template, subs = arg.formula, {"nodata": _nodata_text(source_nodata(inputs[0]))}
        for p in sorted(set(PLACEHOLDER.findall(template)) - {"nodata"}):
            v, t = values.get(p), params[p].session[0]
            if _is_null(v):
                return None
            if t == "INT":
                subs[p] = str(_band(fn, p, v, inputs[0]))
            elif t == "DOUBLE":
                if not math.isfinite(float(v)):
                    raise ParamError(f"{fn.sql_name}: {p} must be finite (got {v})")
                subs[p] = fmt_double(float(v))
            else:  # a STRING: one of the arg's words
                if str(v) not in arg.map:
                    allowed = ", ".join(f"'{k}'" for k in arg.map)
                    raise ParamError(
                        f"{fn.sql_name}: {p} must be one of {allowed} (got '{v}')"
                    )
                subs[p] = arg.map[str(v)]
    expr = PLACEHOLDER.sub(lambda m: subs[m.group(1)], template)
    return _Calc(expr, (("A", 0),))


def data_bound(fn, values: Mapping[str, Any]) -> bool:
    """True when this call binds a value to a ``bad: data`` argument: GDAL refusing the argv is then that value's
    fault, a data error (an error tile), not a parameter error."""
    return any(
        a.bad == "data" and not _is_null(values.get(a.source))
        for a in fn.gdal.args
        if a.source is not None
    )


# Binders that write one token of their own from one value (no template, no partner).
_SPECIAL = ("checked_name", "positive_double", "double_list")


def _const_token(a, inputs: Sequence[RasterProps]) -> Optional[str]:
    """The token of a fixed argument, or None when it writes none (a false flag).

    ``input`` is a result that keeps the source's band type and NoData value; ``declared`` is the band's declared
    NoData value, else the -9999 sentinel (neither is traced as data); True is a bare flag.
    """
    if a.const == "input" and a.arg in ("output-data-type", "nodata"):
        first = inputs[0]
        return (
            f"--{a.arg}={first.dtype}"
            if a.arg == "output-data-type"
            else f"--{a.arg}={_nodata_text(source_nodata(first))}"
        )
    if a.const == "declared" and a.arg == "input-nodata":
        nd = inputs[0].nodata
        return f"--{a.arg}={_nodata_text(nd if nd is not None else -9999.0)}"
    if a.const is True:
        return f"--{a.arg}"
    return None if a.const is False else f"--{a.arg}={_fmt_const(a.const)}"


def _special_token(fn, a, v) -> Optional[str]:
    """The token of a ``checked_name``, ``positive_double`` or ``double_list`` argument. A list that is NULL or empty
    follows the arg's null rule: raises, writes no token (the empty string) or makes the result NULL (None).

    - ``checked_name``: the value is accepted and not passed on (the function keeps it for compatibility): it must not
      be blank, and GDAL gets the fixed word the arg's map holds under '*'.
    - ``positive_double``: a number above 0 and finite, else a refusal naming the value (`when_empty` says why it is
      needed: the list that is empty).
    - ``double_list``: ``a,b,c``, sorted and without repeats and NULLs.
    """
    if a.type == "checked_name":
        if v is not None and str(v).strip() == "":
            raise _fail(fn, a, f"{a.source} must be non-empty (got '{v}')")
        return f"--{a.arg}={a.map['*']}"
    if a.type == "positive_double":
        if _is_null(v) or not 0.0 < float(v) < math.inf:
            why = f"{a.when_empty} is empty so " if a.when_empty else ""
            got = "NULL" if v is None else v
            raise _fail(fn, a, f"{why}{a.source} must be > 0 and finite; got {got}")
        return f"--{a.arg}={fmt_double(float(v))}"
    text = _bind_list(fn, a, v)
    if text is not None:
        return f"--{a.arg}={text}"
    if a.null == "raise":
        raise ParamError(f"{fn.sql_name}: {a.source} must not be NULL")
    return "" if a.null == "omit" else None  # an omitted list writes no token


def _check_together(fn, a, v, by_arg, values) -> None:
    """A ParamError unless both or neither of two arguments that go together (`with`) are supplied."""
    partner = by_arg[a.with_] if a.with_ else None
    if partner is not None and _is_null(v) != _is_null(values.get(partner.source)):
        raise ParamError(
            f"{fn.sql_name}: {a.source} and {partner.source} must be supplied together (both or neither). "
            f"Fix: pass both {a.source} and {partner.source}, or neither."
        )


def _output_tokens(fn, inputs: Sequence[RasterProps]) -> List[str]:
    """The output tokens of a spec's output kind: a GeoTIFF with the write profile's creation options, or for features
    GDAL's in-memory layer (it writes no file, and the output name is a label)."""
    block = fn.gdal
    if block.out_kind == "raster":
        from databricks.labs.gbx.pyrx.core.compression import engine_creation_options

        first = inputs[0]
        dtype = first.dtype if block.out_dtype == "input" else block.out_dtype
        decoded = sum(
            r.width * r.height * r.bands * _ITEMSIZE.get(r.dtype, 8) for r in inputs
        )
        creation = engine_creation_options(
            dtype, decoded, first.width, first.height, block.alg
        )
        return ["--output=<output>", "--output-format=GTiff"] + [
            f"--creation-option={o}" for o in creation
        ]
    if block.out_kind == "features":
        return ["--output=<output>", "--output-format=MEM"]
    raise ValueError(
        f"{fn.sql_name}: the {block.out_kind} output is not built in this release"
    )


def _plain_token(fn, a, v) -> Optional[str]:
    """The token of a plain binder (double, int, bool, choice, crs): a string value is refused when it could name a
    file or a URL (a `crs` is judged by the CRS resolver instead). None for a false bool, which writes no flag.
    """
    if a.type != "crs":
        check_value(fn.sql_name, a.source, v)
    text = _bind_value(fn, a, v)
    if text is None:
        return None
    return f"--{a.arg}" if a.type == "bool" else f"--{a.arg}={text}"


def _template(fn, values, inputs, section) -> Optional[List[str]]:
    """The argv with the placeholder input and output names: bound once per distinct (spec, values, input props),
    so a literal argument is resolved once, not per row."""
    block = fn.gdal
    in_kind = block.inputs[0][1]
    names = (
        ["<input>"]
        if in_kind == "raster"
        else [f"<input{i}>" for i in range(len(inputs))]
    )
    by_arg = {a.arg: a for a in block.args}
    calc: Optional[_Calc] = None
    tokens: List[str] = []
    n_texts = 0
    for a in block.args:
        if a.source is None:
            tok = _const_token(a, inputs)
            if tok:
                tokens.append(tok)
            continue
        if a.when_empty is not None and _has_items(values.get(a.when_empty)):
            continue  # the fixed values replace this argument (levels over an interval)
        if a.type in _SPECIAL:
            tok = _special_token(fn, a, values.get(a.source))
            if tok is None:
                return None
            if tok:
                tokens.append(tok)
            continue
        if a.type in ("calc_formula", "calc_named"):
            calc = _formula_binding(fn, a, values, inputs)
            if calc is None:
                if a.null == "raise":
                    raise ParamError(
                        f"{fn.sql_name}: {a.source} and the other values of the formula must not be NULL"
                    )
                return None
            tokens += [f"--{a.arg}={calc.expr}", "--dialect=muparser"]
            continue
        v = values.get(a.source)
        _check_together(fn, a, v, by_arg, values)
        if _is_null(v):
            if a.null == "omit":
                continue
            if a.null == "raise":
                raise ParamError(f"{fn.sql_name}: {a.source} must not be NULL")
            return None
        if a.type == "calc_envelope":
            calc = _calc_binding(fn, a, v, inputs)
            tokens += [f"--{a.arg}={calc.expr}", "--dialect=muparser"]
            continue
        if a.type == "text_vsimem":
            check_color_table(fn.sql_name, a.source, v)
            tokens.append(f"--{a.arg}=<text{n_texts}>")
            n_texts += 1
            continue
        if a.type == "table_alpha":
            if _color_table(fn.sql_name, a.source, str(v))[1]:
                tokens.append(f"--{a.arg}")
            continue
        tok = _plain_token(fn, a, v)
        if tok:
            tokens.append(tok)
    guard_tokens(
        section,
        block.alg,
        tokens,
        frozenset(a.arg for a in block.args if a.type in ENGINE_FILE_BINDERS),
    )
    if calc is not None:
        ins = [f"--input={v}={names[i]}" for v, i in calc.variables]
    else:
        ins = [f"--input={n}" for n in names]
    return (
        ins
        + ["--input-format=GTiff"]
        + tokens
        + _output_tokens(fn, inputs)
        + (["--num-threads=1"] if block.alg in THREADED else [])
        + ["--quiet"]
    )


def _hashable(v):
    """A list or array value as a tuple, so a call with the same levels shares one bound template."""
    if isinstance(v, list) or getattr(v, "ndim", 0) > 0:
        return tuple(v)
    return v


def build(
    fn,
    values: Mapping[str, Any],
    inputs: Sequence[RasterProps],
    names: Optional[Sequence[str]] = None,
    output: str = "<output>",
    section=None,
    texts: Optional[Sequence[str]] = None,
) -> Optional[List[str]]:
    """The argv for one call of `fn`, or None when a NULL value makes the result NULL.

    `values` maps the function's params to this call's values; `inputs` are the input rasters' header props;
    `names` the inputs' `/vsimem` names (default the placeholders ``<input>``, or ``<input0>``, ``<input1>``,
    ... for an array of rasters, the form ``last_command`` shows). `section` is the manifest's `gdal` section (the
    allowlist), by default the manifest's own. `texts` are the names of the files the engine wrote for the call's
    ``text_vsimem`` arguments (``text_values``), in order: they fill ``<text0>``, ``<text1>``, ... Raises ParamError (raise) or DataError (an error tile) when a binder
    refuses a value.
    """
    section = section if section is not None else manifest.load().gdal
    try:
        key = (
            id(fn),
            id(section),
            tuple(sorted((k, _hashable(v)) for k, v in values.items())),
            tuple(inputs),
        )
        hash(key)
    except TypeError:  # an unhashable value: bind it without the cache
        key = None
    with _templates_lock:
        cached = _templates.get(key, _MISSING) if key is not None else _MISSING
        if cached is not _MISSING:
            _templates.move_to_end(key)
    if cached is _MISSING:
        cached = _template(fn, values, inputs, section)
        if key is not None:
            with _templates_lock:
                _templates[key] = cached
                while len(_templates) > _TEMPLATE_CACHE_SIZE:
                    _templates.popitem(last=False)
    if cached is None or (names is None and output == "<output>" and texts is None):
        return None if cached is None else list(cached)
    in_kind = fn.gdal.inputs[0][1]
    placeholders = (
        ["<input>"]
        if in_kind == "raster"
        else [f"<input{i}>" for i in range(len(inputs))]
    )
    real = {**dict(zip(placeholders, names or placeholders)), "<output>": output}

    def fill(tok: str) -> str:
        if tok.startswith(("--input=", "--output=")):
            head, _, ph = tok.rpartition("=")
            return f"{head}={real.get(ph, ph)}"
        if texts is not None and "=<text" in tok:
            head, _, ph = tok.rpartition("=")
            return f"{head}={texts[int(ph[len('<text'):-1])]}"
        return tok

    return [fill(tok) for tok in cached]
