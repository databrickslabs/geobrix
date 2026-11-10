"""Binders and the argv: (a row's spec, the call's values, its input rasters) -> the tokens GDAL parses.

``test/engine/fixtures/argv_golden.json`` pins the tokens.
The order is fixed: the inputs, ``--input-format``, the spec's arguments in template order, then the output, its
format and creation options, ``--num-threads`` where the algorithm has it, and ``--quiet``. Every token is
``--name=value`` or a bare ``--name``, so a value can never be read as a flag or as a pipeline step.
"""

from __future__ import annotations

import dataclasses
import functools
import json
import math
import re
import string
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import command as command_mod
from databricks.labs.gbx.engine.spec import (
    ENGINE_FILE_BINDERS,
    PLACEHOLDER,
    THREADED,
    DataError,
    ParamError,
    check_value,
    guard_tokens,
)

# what a streamed polygonize holds for each column of the band, at most: two lines of values (8 bytes) and of region ids
# (4 bytes), a line of mask bytes and two lines of the polygonizer's arm records (under 100 bytes each)
_STREAM_BYTES_PER_COLUMN = 256

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
# The placeholder of the observer's position in a template: the point in the input's own CRS, written by the engine at run time.
OBSERVER = "<observer>"
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
    # The window's own size when the engine was handed a decimated read of it (a virtual tile read at its target size:
    # 0 when the input is read whole), and the input's pixel size in its CRS units (0 when unknown).
    src_width: int = 0
    src_height: int = 0
    px_x: float = 0.0
    px_y: float = 0.0
    # True when a reprojection of this input carries a mask band (``run._mask_to_carry``): the output estimate then holds
    # the mask's working memory. Not part of the template cache key.
    mask: bool = field(default=False, compare=False)


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


_COUNT_MAX = 2**31 - 1


def _count(fn, arg, value) -> int:
    """`value` as a whole number from 0 to 2^31 - 1 (a count of pixels or passes), else a refusal naming the parameter and
    the value: GDAL reads the argument as an integer, and a fraction cut to its whole part would run another search.
    """
    try:
        x = float(value)
        ok = math.isfinite(x) and x == int(x) and 0 <= x <= _COUNT_MAX
    except (TypeError, ValueError, OverflowError):
        ok = False
    if not ok:
        raise _fail(
            fn,
            arg,
            f"{arg.source} must be a whole number from 0 to {_COUNT_MAX} (got {value}). "
            "Fix: pass a whole number",
        )
    return int(x)


def _bind_value(fn, arg, value) -> Optional[str]:
    """One argument's value text, or None to leave a bare flag out (a false `bool`)."""
    t = arg.type
    if t == "double":
        x = float(value)
        if math.isinf(x):
            raise _fail(fn, arg, f"{arg.source} must be finite (got {value})")
        return fmt_double(x)
    if t == "nonneg_double":
        x = float(value)
        if not (math.isfinite(x) and x >= 0.0):
            raise _fail(fn, arg, f"{arg.source} must be >= 0 and finite; got {value}")
        return fmt_double(x)
    if t == "int":
        return str(int(value))
    if t == "bool":
        return "" if bool(value) else None
    if t == "count":
        return str(_count(fn, arg, value))
    if t == "flag_choice":
        # a choice among words that switch a bare flag on (true) or leave it out (false)
        word = str(value).strip().lower()
        choices = {k.lower(): v for k, v in arg.map.items()}
        if word not in choices:
            allowed = ", ".join(f"'{k}'" for k in arg.map)
            raise _fail(
                fn, arg, f"{arg.source} must be one of {allowed} (got '{value}')"
            )
        return "" if choices[word] else None
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
    return _export_crs(_osr_reference(v))


def _osr_reference(text: str):
    """The OSR reference of a CRS text (an integer code, ``AUTHORITY:CODE``, WKT or PROJJSON): the one parse of a CRS value in
    the engine. File and network access are off, and the parse is judged as the JVM judges it, with osr's exceptions off: a
    failing return, or a failure GDAL reports during it (PROJ cannot build the CRS, e.g. a required grid it cannot find,
    which would otherwise leave a reprojection without its datum shift), raises a ValueError. The caller has already
    judged the text's kind (``core.crs.crs_text_kind``).
    """
    from databricks.labs.gbx.core.crs import _is_intlike

    b = _osgeo()
    gdal, osr = b.gdal, b.osr
    v = text.strip()
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
        return sr


def _export_crs(sr) -> str:
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
    "aqua",
    "violet",
    "indigo",
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


def _names_a_file(text: str) -> bool:
    """True when the whole `text` (stripped) is one line that names a file: a path (with a slash, or with spaces in it) or
    a file name (``.txt``, ``.clr``, ``.csv``), not a table. A line that starts with a number, a percent or ``nv`` is a
    table entry, and a line that starts with ``#`` is a comment: neither is judged by its slashes.
    """
    if "\n" in text or text.startswith("#") or not _FILE_LIKE.search(text):
        return False
    first = re.split(r"[\s,]+", text)[0]
    return not (_NUMBER.match(first) or first.lower() in ("nv", "nodata"))


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
    if _names_a_file(text.strip()):
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


def _text_args(fn, values: Mapping[str, Any]) -> list:
    """The ``text_vsimem`` arguments that write a file for this call, in template order: the one list ``text_values`` and
    the template's ``<text{k}>`` names both read. An argument with a NULL value writes none, and neither does one that
    ``when_empty`` replaces."""
    return [
        a
        for a in fn.gdal.args
        if a.type == "text_vsimem"
        and not _is_null(values.get(a.source))
        and not (a.when_empty is not None and _has_items(values.get(a.when_empty)))
    ]


def text_values(fn, values: Mapping[str, Any]) -> List[str]:
    """The texts the call's ``text_vsimem`` arguments write, in template order (the engine writes each to a file under
    the call's prefix and names it in ``build(..., texts=)``). A NULL value is not written: the result is NULL.
    """
    return [
        check_color_table(fn.sql_name, a.source, values.get(a.source))
        for a in _text_args(fn, values)
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
# Binders that write one `a,b` token: a size from a factor, and a pair of whole numbers or of numbers.
_SIZED = ("size_from_factor", "int_pair", "double_pair")


def _positive(fn, arg, name: str, v, whole: bool):
    """`v` (the value of param `name`) as a number above 0 and finite, else a refusal naming the param and the value. A
    `whole` number is a count of pixels: a fractional value is refused, never cut to its integer part.
    """
    try:
        x = int(v) if whole else float(v)
        fractional = whole and float(v) != x
    except (TypeError, ValueError, OverflowError):  # int(inf) is an OverflowError
        x, fractional = math.nan, False
    if fractional:
        raise _fail(fn, arg, f"{name} must be a whole number of pixels; got {v}")
    if not (0 < x < math.inf):
        raise _fail(fn, arg, f"{name} must be positive and finite; got {v}")
    return x


def _sized_token(fn, a, values, first: "RasterProps") -> str:
    """The token of a ``size_from_factor`` (``--size=w,h`` from a factor above 0 and the first input's size, each side
    rounded half to even and at least 1), ``int_pair`` (the `from` and `and` params as whole numbers) or
    ``double_pair`` (as numbers) argument. Both members must be above 0 and finite, else a refusal naming the member.
    """
    if a.type == "size_from_factor":
        f = _positive(fn, a, a.source, values[a.source], False)
        w, h = first.src_width or first.width, first.src_height or first.height
        return f"--{a.arg}={max(1, round(w * f))},{max(1, round(h * f))}"
    whole = a.type == "int_pair"
    x = _positive(fn, a, a.source, values[a.source], whole)
    y = _positive(fn, a, a.and_, values[a.and_], whole)
    return (
        f"--{a.arg}={x},{y}" if whole else f"--{a.arg}={fmt_double(x)},{fmt_double(y)}"
    )


def observer_arg(fn):
    """The ``observer_xy`` argument of a row, or None."""
    return next((a for a in fn.gdal.args if a.type == "observer_xy"), None)


def _observer_source_crs(fn, a, geom, values: Mapping[str, Any]) -> str:
    """The canonical text of the CRS the observer is in, or "" when it has none (it is then in the raster's own CRS).

    Rule 1: a geometry's embedded SRID wins, else the row's ``crs`` parameter, else none. The text is bound as every
    target CRS is (``canonical_crs``), so a value that names a file or a URL is refused.
    """
    import shapely

    srid = int(shapely.get_srid(geom))
    text = str(srid) if srid > 0 else values.get("crs")
    if text is None or str(text).strip() == "":
        return ""
    try:
        return canonical_crs(str(text))
    except (
        Exception
    ) as exc:  # noqa: BLE001 - any refusal or parse failure is this value's
        raise ParamError(
            f"{fn.sql_name}: crs: {str(exc).removeprefix('resolve_crs: ')}"
        ) from None


def observer_of(
    fn, values: Mapping[str, Any]
) -> Optional[Tuple[float, float, float, str]]:
    """``(x, y, height, source CRS text)`` of the observer a ``observer_xy`` argument binds, or None for a NULL geometry (the
    result is NULL).

    The geometry is a POINT as WKB, EWKB, WKT or EWKT; any other geometry, and text that is no geometry, raises a
    ParamError naming the parameter. A NULL height is 0.0, and a height that is negative or not finite raises. The
    source CRS is "" when the observer has none; the engine puts the point in the input's own CRS at run time
    (``run._position``).
    """
    a = observer_arg(fn)
    geom_in, height = values.get(a.source), values.get(a.and_)
    if _is_null(geom_in):
        return None
    from databricks.labs.gbx._geom import parse_geom

    if isinstance(geom_in, (bytes, bytearray, memoryview)) and bytes(
        geom_in[:1]
    ) not in (
        b"\x00",
        b"\x01",
    ):
        # A WKB starts with its byte order (0 or 1). Text that came as bytes is WKT or EWKT: the batch form's SQL wrapper
        # types the geometry by its first session type (BINARY), which casts a STRING argument to its UTF-8 bytes.
        try:
            geom_in = bytes(geom_in).decode("utf-8")
        except UnicodeDecodeError:
            pass
    try:
        geom = parse_geom(geom_in)
    except Exception:  # noqa: BLE001 - text or bytes that are no geometry
        geom = None
    if geom is None:
        raise ParamError(
            f"{fn.sql_name}: {a.source} is not a geometry (WKB, EWKB, WKT or EWKT)"
        )
    if geom.geom_type != "Point":
        raise ParamError(
            f"{fn.sql_name}: {a.source} must be a POINT (got {geom.geom_type.upper()})"
        )
    if geom.is_empty or not (math.isfinite(geom.x) and math.isfinite(geom.y)):
        raise ParamError(
            f"{fn.sql_name}: {a.source} must be a POINT with finite coordinates"
        )
    h = 0.0 if _is_null(height) else float(height)
    if not (math.isfinite(h) and h >= 0.0):
        raise ParamError(
            f"{fn.sql_name}: {a.and_} must be >= 0 and finite; got {height}"
        )
    return float(geom.x), float(geom.y), h, _observer_source_crs(fn, a, geom, values)


# The bands of a result by algorithm, where they are not the input's: a terrain algorithm writes one band, a color
# relief up to four (RGB and an alpha band). A reprojection keeps the input's bands; a calc's come from its expression.
_OUT_BANDS = {
    "raster slope": 1,
    "raster aspect": 1,
    "raster hillshade": 1,
    "raster tri": 1,
    "raster tpi": 1,
    "raster roughness": 1,
    "raster viewshed": 1,
    "raster color-map": 4,
}


@dataclass(frozen=True)
class WarpGrid:
    """The grid GDAL's reprojection would pick for an input before any size or resolution is set
    (``run._warp_grid``): its size in pixels and its pixel size in the output CRS's units.
    """

    width: int
    height: int
    px_x: float
    px_y: float


def _token_value(tokens: Sequence[str], name: str) -> Optional[str]:
    return next(
        (t.split("=", 1)[1] for t in tokens if t.startswith(f"--{name}=")), None
    )


def _output_pixels(tokens: Sequence[str], first: "RasterProps", grid) -> int:
    """The pixels of the output grid: from ``--size``, else ``--resolution`` over the extent, else the grid GDAL picks
    (``grid``; without it the input's own size, with the extent taken from its north-up pixel size).
    """
    reach = _token_value(tokens, "max-distance")
    if (
        reach is not None
        and first.px_x
        and first.px_y
        and any(t.startswith("--position=") for t in tokens)
    ):
        # a viewshed with a distance limit writes the window around the observer: the distance each way, in pixels
        wx = 2 * math.ceil(float(reach) / first.px_x) + 1
        wy = 2 * math.ceil(float(reach) / first.px_y) + 1
        return min(first.width, wx) * min(first.height, wy)
    size = _token_value(tokens, "size")
    if size is not None:
        w, h = (int(v) for v in size.split(","))
        return w * h
    res = _token_value(tokens, "resolution")
    if res is not None:
        rx, ry = (float(v) for v in res.split(","))
        if grid is not None:
            ex, ey = grid.width * grid.px_x, grid.height * grid.px_y
        elif first.px_x and first.px_y:
            ex, ey = first.width * abs(first.px_x), first.height * abs(first.px_y)
        else:
            return first.width * first.height
        return math.ceil(ex / rx) * math.ceil(ey / ry)
    return grid.width * grid.height if grid is not None else first.width * first.height


@dataclass(frozen=True)
class PipeState:
    """What a command's output holds after a step, for the budget: size, pixel size and top-left corner in `crs` (a pixel
    size of 0 when the extent is unknown: a rotated or ground-control-point tile), bands and the bytes of a pixel.
    `fresh` is True while it is still the input's own header (run reads GCPs from the file then).
    """

    width: int
    height: int
    px_x: float
    px_y: float
    ox: float
    oy: float
    crs: Optional[str]
    bands: int
    item: int
    fresh: bool = False

    @property
    def nbytes(self) -> int:
        return self.width * self.height * self.bands * self.item

    @property
    def extent(self) -> Optional[Tuple[float, float]]:
        return (
            (self.width * self.px_x, self.height * self.px_y)
            if self.px_x > 0 and self.px_y > 0
            else None
        )


class NoGeo:
    """The geometry calls a command's estimate needs from GDAL: ``warp(state, target_crs)`` is
    ``(width, height, px_x, px_y, ox, oy)`` of the grid a reprojection picks (None when GDAL gives none), and
    ``envelope(bbox, from_crs, to_crs)`` is the (minx, miny, maxx, maxy) of a box moved between CRSs (None when it
    cannot be). ``run`` supplies them from GDAL; this one has none, so a step that needs one is not bounded.
    """

    def warp(self, state, target):
        return None

    def envelope(self, bbox, from_crs, to_crs):
        return None


UNBOUNDED_FIX = "Fix: give --size (absolute pixels) for the step, or cut the tile smaller first (gbx_rst_retile)"
# The steps that hold their whole result in memory while they run (GDAL cannot stream them).
_FLOAT_STEPS = frozenset(
    {"slope", "aspect", "tri", "tpi", "roughness", "proximity", "neighbors"}
)


def _unbounded(step: str, why: str) -> DataError:
    return DataError(
        f"gbx_rst_gdal: the output of the step '{step}' cannot be bounded ({why}). {UNBOUNDED_FIX}"
    )


def _windowed_viewshed(step: Sequence[str]) -> bool:
    """A viewshed step with a distance limit: it writes the window around the observer, not the grid it reads."""
    if step[0] != "viewshed":
        return False
    reach, at = _token_value(step, "max-distance"), _numbers(
        _token_value(step, "position")
    )
    return (
        reach is not None
        and math.isfinite(float(reach))
        and float(reach) > 0
        and len(at) >= 2
        and all(math.isfinite(v) for v in at)
    )


def window_mask(tokens: Sequence[str]) -> str:
    """How the footprint mask a command carries meets a window around the observer: ``"none"`` when no step writes one,
    ``"cut"`` when the mask, on the grid of the steps before the window, is cut to it (no reproject, resize or clip comes
    after it), ``"skip"`` when a step after the window changes the grid again, so the mask has no grid of the output.
    """
    steps = command_mod.steps_of(tokens)[1:-1]
    at = next((i for i, st in enumerate(steps) if _windowed_viewshed(st)), None)
    if at is None:
        return "none"
    later = (st[0] for st in steps[at + 1 :])
    return "skip" if any(n in ("reproject", "resize", "clip") for n in later) else "cut"


def _numbers(text: Optional[str]) -> List[float]:
    return [] if text is None else [float(v) for v in text.split(",")]


def _scaled(value: str, base: int) -> Optional[int]:
    """One `--size` value: a whole number of pixels, or a percentage of `base`; 0 (keep the aspect) is None."""
    if value.endswith("%"):
        return max(1, math.ceil(base * float(value[:-1]) / 100.0))
    n = int(value)
    return n or None


def _sized(step, st: PipeState, size: List[str], extent) -> Tuple[int, int]:
    """(width, height) of a ``--size`` of `st` (percentages of the current size, 0 keeps the aspect ratio)."""
    w = _scaled(size[0], st.width)
    h = _scaled(size[1], st.height) if len(size) > 1 else None
    if w is None and h is None:
        raise _unbounded(step[0], "a size of 0 and 0")
    if w is None:
        w = max(1, math.ceil(h * st.width / st.height))
    if h is None:
        h = max(1, math.ceil(w * st.height / st.width))
    return w, h


def _cut(step, st: PipeState, geo, box, box_crs) -> PipeState:
    """`st` cut to the box (xmin, ymin, xmax, ymax) given in `box_crs` (None: the state's own CRS)."""
    if st.extent is None:
        raise _unbounded(step[0], "the tile's pixel size is unknown")
    if box_crs is not None and st.crs is not None and box_crs != st.crs:
        env = geo.envelope(box, box_crs, st.crs)
        if env is None:
            raise _unbounded(step[0], "the box cannot be moved into the tile's CRS")
        box = env
    elif box_crs is not None and st.crs is None:
        raise _unbounded(step[0], "the tile has no CRS")
    dx, dy = box[2] - box[0], box[3] - box[1]
    w = max(1, math.ceil(dx / st.px_x))
    h = max(1, math.ceil(dy / st.px_y))
    if "--allow-bbox-outside-source" not in step:
        w, h = min(w, st.width), min(h, st.height)
    return dataclasses.replace(st, width=w, height=h, ox=box[0], oy=box[3], fresh=False)


def _target_pixel(st: PipeState, target: str, geo) -> Tuple[float, float]:
    """The pixel size of `st` in the units of `target`, where GDAL gives no output grid: the tile's extent moved into
    `target` over its pixel counts, as GDAL's own grid picks it. (0, 0), not bounded, when the extent is unknown or
    cannot be moved: the source's pixel size is in another CRS's units."""
    if st.extent is None:
        return 0.0, 0.0
    w, h = st.extent
    env = geo.envelope((st.ox, st.oy - h, st.ox + w, st.oy), st.crs, target)
    if env is None:
        return 0.0, 0.0
    return (env[2] - env[0]) / st.width, (env[3] - env[1]) / st.height


def _reproject(step, st: PipeState, geo) -> PipeState:
    if _token_value(step, "input-crs") is not None:
        st = dataclasses.replace(st, crs=_token_value(step, "input-crs"), fresh=False)
    target = _token_value(step, "output-crs") or st.crs
    if st.crs is None or target is None:
        raise DataError(
            "gbx_rst_gdal: the tile has no CRS, so it cannot be reprojected. Fix: set its CRS first with gbx_rst_setcrs"
        )
    g = geo.warp(st, target)
    size, res = _token_value(step, "size"), _token_value(step, "resolution")
    bbox = _numbers(_token_value(step, "bbox"))
    out = dataclasses.replace(st, crs=target, fresh=False)
    if g is not None:
        gw, gh, gpx, gpy, gox, goy = g
        out = dataclasses.replace(
            out, width=gw, height=gh, px_x=gpx, px_y=gpy, ox=gox, oy=goy
        )
    if bbox:
        box_crs = _token_value(step, "bbox-crs")
        env = (
            geo.envelope(tuple(bbox), box_crs, target)
            if box_crs not in (None, target)
            else tuple(bbox)
        )
        if env is None:
            raise _unbounded("reproject", "the box cannot be moved into the target CRS")
        out = dataclasses.replace(out, ox=env[0], oy=env[3])
        ext = (env[2] - env[0], env[3] - env[1])
    elif g is not None:
        ext = (g[0] * g[2], g[1] * g[3])
    else:
        ext = None
    if size is not None:
        sw, sh = _sized(step, out, size.split(","), ext)
        return dataclasses.replace(
            out,
            width=sw,
            height=sh,
            px_x=ext[0] / sw if ext else 0.0,
            px_y=ext[1] / sh if ext else 0.0,
        )
    if res is not None or bbox:
        if res is not None:
            rx, ry = (_numbers(res) * 2)[:2]
        elif g is not None or target == st.crs:
            rx, ry = out.px_x, out.px_y  # in the target CRS's units
        else:
            rx, ry = _target_pixel(st, target, geo)
        if ext is None or rx <= 0 or ry <= 0:
            raise _unbounded(
                "reproject", "the extent or pixel size after the CRS change is unknown"
            )
        return dataclasses.replace(
            out,
            width=max(1, math.ceil(ext[0] / rx)),
            height=max(1, math.ceil(ext[1] / ry)),
            px_x=rx,
            px_y=ry,
        )
    if g is None:
        raise _unbounded("reproject", "GDAL gives no output grid for the tile")
    return out


def _resize(step, st: PipeState) -> PipeState:
    size, res = _token_value(step, "size"), _token_value(step, "resolution")
    if size is not None:
        w, h = _sized(step, st, size.split(","), st.extent)
        ext = st.extent
        return dataclasses.replace(
            st,
            width=w,
            height=h,
            px_x=ext[0] / w if ext else 0.0,
            px_y=ext[1] / h if ext else 0.0,
            fresh=False,
        )
    rx, ry = (_numbers(res) * 2)[:2]
    if st.extent is None or rx <= 0 or ry <= 0:
        raise _unbounded("resize", "the tile's pixel size is unknown")
    return dataclasses.replace(
        st,
        width=max(1, math.ceil(st.extent[0] / rx)),
        height=max(1, math.ceil(st.extent[1] / ry)),
        px_x=rx,
        px_y=ry,
        fresh=False,
    )


def _clip(step, st: PipeState, geo) -> PipeState:
    window = _numbers(_token_value(step, "window"))
    bbox = _numbers(_token_value(step, "bbox"))
    geometry = _token_value(step, "geometry")
    if window:
        out = dataclasses.replace(
            st,
            width=max(1, int(window[2])),
            height=max(1, int(window[3])),
            ox=st.ox + window[0] * st.px_x,
            oy=st.oy - window[1] * st.px_y,
            fresh=False,
        )
    elif bbox:
        out = _cut(step, st, geo, tuple(bbox), _token_value(step, "bbox-crs"))
    elif geometry is not None:
        import shapely

        out = _cut(
            step,
            st,
            geo,
            tuple(shapely.from_wkt(geometry).bounds),
            _token_value(step, "geometry-crs"),
        )
    else:
        out = st
    return dataclasses.replace(out, bands=out.bands + ("--add-alpha" in step))


def _viewshed_window(step, st: PipeState) -> PipeState:
    """The state after a viewshed with a distance limit: one band, the window around the observer (the distance each way,
    in pixels, as ``_output_pixels`` sizes the row), placed by the observer's x and y in the state's CRS.
    """
    if st.extent is None:
        # no pixel size to measure the distance in: the whole grid bounds it, and the state stays the input's own header
        # (a later reprojection is sized from it; a state that is not the header has no pixel size to size one from)
        return st
    reach = float(_token_value(step, "max-distance"))
    x, y = _numbers(_token_value(step, "position"))[:2]
    w = max(1, min(st.width, 2 * math.ceil(reach / st.px_x) + 1))
    h = max(1, min(st.height, 2 * math.ceil(reach / st.px_y) + 1))
    return dataclasses.replace(
        st,
        width=w,
        height=h,
        ox=x - (w // 2) * st.px_x,
        oy=y + (h // 2) * st.px_y,
        bands=1,
        fresh=False,
    )


def _advance(step, st: PipeState, geo) -> PipeState:
    """The state after one step of a command."""
    name = step[0]
    named = _token_value(step, "output-data-type")
    if name == "reproject":
        out = _reproject(step, st, geo)
    elif name == "resize":
        out = _resize(step, st)
    elif name == "clip":
        out = _clip(step, st, geo)
    elif name == "nodata-to-alpha":
        out = dataclasses.replace(st, bands=st.bands + 1)
    elif _windowed_viewshed(step):
        out = _viewshed_window(step, st)
    elif name == "select":
        listed = len(_token_value(step, "band").split(","))
        out = dataclasses.replace(
            st, bands=max(1, st.bands - listed if "--exclude" in step else listed)
        )
    else:
        out = st
    if named is not None:
        item = _ITEMSIZE.get(named, 8)
    elif name == "unscale":
        item = 8
    elif name in _FLOAT_STEPS:
        item = max(out.item, 4)
    else:
        item = out.item
    return dataclasses.replace(out, item=item, fresh=out.fresh and out is st)


def pipeline_peak(tokens: Sequence[str], start: PipeState, geo) -> int:
    """The most decoded bytes a command holds at once, for the per-call budget. GDAL runs the steps lazily or in memory,
    so the bound is the largest state any step leaves (a step that holds its result in memory while it runs also holds
    its input), judged from the state before it: a reprojection's grid is the one GDAL picks for the previous step's
    output (``geo.warp``), a ``--resolution`` or box is read against the previous extent, a clip cuts it, a type
    step changes the bytes of a pixel. A step whose output cannot be bounded raises a DataError with a Fix.
    """
    steps = command_mod.steps_of(tokens)[1:-1]  # without the read and write steps
    state, peak = start, 0
    for step in steps:
        after = _advance(step, state, geo)
        held = after.nbytes + (
            state.nbytes if step[0] in command_mod.MATERIALIZING else 0
        )
        peak = max(peak, held)
        state = after
    return peak


def estimate_output(
    fn,
    tokens: Sequence[str],
    props: Sequence["RasterProps"],
    grid: Optional[WarpGrid] = None,
) -> int:
    """The decoded bytes the call's output will hold, for the per-call budget: the inputs plus this must fit.

    The pixels come from ``--size``, else ``--resolution`` over the extent, else the grid GDAL picks for a reprojection
    (``grid``, which accounts for a rotated or GCP-georeferenced tile and for a target CRS that changes the pixel
    count), else the input's own size. The type is the argv's ``--output-data-type`` when it has one, else the row's
    output type; the bands are the algorithm's (``_OUT_BANDS``), else the input's. A reprojection that carries a mask
    (``RasterProps.mask``) also holds the mask's working arrays: a byte per input pixel in four copies and a byte per
    output pixel in three. A JSON output holds nothing to count, and a streamed features output its line buffers.
    """
    first = props[0]
    if fn.gdal.out_kind == "json":
        return 0  # the text is small; the statistics read the pixels block by block
    if fn.gdal.out_kind == "features" and fn.gdal.out_stream:
        # a streamed features output writes no raster: GDAL writes the features to a file on local disk and reads the
        # band a line at a time, so what it holds is its line buffers (``_STREAM_BYTES_PER_COLUMN``). Its bookkeeping
        # grows with the regions it finds (about 30 bytes a region, test_polygonize_memory), which no header gives.
        return first.width * _STREAM_BYTES_PER_COLUMN
    if fn.gdal.alg == command_mod.PIPELINE_ALG:
        raise ValueError(
            "a command's output is judged by pipeline_peak (run._judge_budget)"
        )
    dtype = _token_value(tokens, "output-data-type") or (
        first.dtype if fn.gdal.out_dtype == "input" else fn.gdal.out_dtype
    )
    item = _ITEMSIZE.get(dtype, 8)
    bands = _OUT_BANDS.get(fn.gdal.alg, first.bands)
    if fn.gdal.alg == "raster calc" and "[" in (_token_value(tokens, "calc") or ""):
        bands = 1  # a band-indexed expression (A[1], the map algebra's V[b]) writes one band; a bare A writes every band
    # inputs that are not warped share one grid: the largest bounds what the algorithm writes
    pixels = _output_pixels(tokens, first, grid)
    if fn.gdal.alg not in ("raster reproject", "raster viewshed"):
        pixels = max(pixels, *(p.width * p.height for p in props))
    out = pixels * bands * item
    if fn.gdal.per_band and bands > 1:
        out += 2 * pixels * item  # one band's result file and the bytes read from it
    if fn.gdal.alg == "raster reproject" and first.mask:
        out += first.width * first.height * 4 + pixels * 3
    return out


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
        if _is_null(v) and a.null == "omit":
            return ""  # no limit: GDAL's own default
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


def _output_tokens(
    fn, inputs: Sequence[RasterProps], striped: bool = False
) -> List[str]:
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
            dtype,
            decoded,
            first.width,
            first.height,
            block.alg,
            predictor=block.write != "command",
            striped=striped,
        )
        return ["--output=<output>", "--output-format=GTiff"] + [
            f"--creation-option={o}" for o in creation
        ]
    if block.out_kind == "features" and block.out_stream:
        # a file on local disk with no spatial index and a commit every 10,000 features: what GDAL holds while it writes
        # stays small however many features there are
        return [
            "--output=<output>",
            "--output-format=GPKG",
            "--layer-creation-option=SPATIAL_INDEX=NO",
            "--commit-interval=10000",
        ]
    if block.out_kind == "features":
        return ["--output=<output>", "--output-format=MEM"]
    if block.out_kind == "json":
        return [
            "--output-format=json"
        ]  # the text comes back in the algorithm's `output-string`
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
    return f"--{a.arg}" if a.type in ("bool", "flag_choice") else f"--{a.arg}={text}"


def _is_null_value(a, v, values: Mapping[str, Any]) -> bool:
    """True when the argument `a` has no value: `v` is NULL, or for a pair (`and`) its partner is."""
    return _is_null(v) or (a.and_ is not None and _is_null(values.get(a.and_)))


def _null_argument(block, values: Mapping[str, Any]) -> bool:
    """True when an argument whose null rule is ``null_result`` has a NULL value: the result is NULL before any other
    argument is judged (a wrong word beside a NULL is NULL). NaN counts as NULL: an Arrow UDF hands a NULL DOUBLE over
    as NaN. The arguments with a rule or a binder of their own (a pair that goes together, a list, a formula) are
    judged where they are bound.
    """
    return any(
        a.source is not None
        and a.null == "null_result"
        and a.with_ is None
        and a.when_empty is None
        and a.type not in _SPECIAL
        and a.type != "observer_xy"
        and not a.type.startswith("calc_")
        and _is_null_value(a, values.get(a.source), values)
        for a in block.args
    )


def _bound_tokens(fn, a, v, values, inputs, n_texts: int):
    """(the tokens, the map-algebra binding or None) of argument `a` for its non-NULL value `v`: a pair or a size, a
    map-algebra envelope, a color table (the engine's ``<text{n_texts}>`` file) and its alpha flag, or a plain binder.
    """
    if a.type in _SIZED:
        return [_sized_token(fn, a, values, inputs[0])], None
    if a.type == "calc_envelope":
        calc = _calc_binding(fn, a, v, inputs)
        return [f"--{a.arg}={calc.expr}", "--dialect=muparser"], calc
    if a.type == "text_vsimem":
        check_color_table(fn.sql_name, a.source, v)
        return [f"--{a.arg}=<text{n_texts}>"], None
    if a.type == "table_alpha":
        alpha = _color_table(fn.sql_name, a.source, str(v))[1]
        return ([f"--{a.arg}"] if alpha else []), None
    if a.type == "band":
        return [f"--{a.arg}={_band(fn, a.source, v, inputs[0])}"], None
    tok = _plain_token(fn, a, v)
    return ([tok] if tok else []), None


def _pipeline_template(fn, values, inputs, section) -> Optional[List[str]]:
    """The argv of a ``command`` row (``gbx_rst_gdal``): the whole ``raster pipeline`` token list, which the
    caller's command fills between the engine's ``read`` and ``write`` steps. A NULL command makes the result NULL.
    A pipeline with a hillshade step is written striped (``engine_creation_options``).
    """
    a = next(x for x in fn.gdal.args if x.type == "command")
    v = values.get(a.source)
    if _is_null(v):
        if a.null == "raise":
            raise ParamError(f"{fn.sql_name}: {a.source} must not be NULL")
        return None
    steps = command_mod.tokenize(str(v))
    out = _output_tokens(fn, inputs, striped=any(s[0] == "hillshade" for s in steps))
    return command_mod.pipeline_tokens(str(v), section, canonical_crs, out)


def _template(fn, values, inputs, section) -> Optional[List[str]]:
    """The argv with the placeholder input and output names: bound once per distinct (spec, values, input props),
    so a literal argument is resolved once, not per row."""
    block = fn.gdal
    if block.alg == command_mod.PIPELINE_ALG:
        return _pipeline_template(fn, values, inputs, section)
    in_kind = block.inputs[0][1]
    names = (
        ["<input>"]
        if in_kind == "raster"
        else [f"<input{i}>" for i in range(len(inputs))]
    )
    by_arg = {a.arg: a for a in block.args}
    if _null_argument(block, values):
        return None
    calc: Optional[_Calc] = None
    tokens: List[str] = []
    text_index = {id(a): k for k, a in enumerate(_text_args(fn, values))}
    for a in block.args:
        if a.source is None:
            tok = _const_token(a, inputs)
            if tok:
                tokens.append(tok)
            continue
        if a.when_empty is not None and _has_items(values.get(a.when_empty)):
            continue  # the fixed values replace this argument (levels over an interval)
        if a.type == "observer_xy":
            if observer_of(fn, values) is None:
                return None
            tokens.append(
                f"--{a.arg}={OBSERVER}"
            )  # the engine writes the point at run time (run._position)
            continue
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
        if _is_null_value(a, v, values):
            if a.null == "raise":
                raise ParamError(f"{fn.sql_name}: {a.source} must not be NULL")
            if a.null == "omit":
                continue
            return None
        toks, bound = _bound_tokens(fn, a, v, values, inputs, text_index.get(id(a), 0))
        tokens += toks
        calc = bound or calc
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
        + (
            [] if block.out_kind == "json" else ["--quiet"]
        )  # `raster info` has no --quiet
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
