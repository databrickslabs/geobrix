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
        template, subs = arg.formula, {}
        for p in sorted(set(PLACEHOLDER.findall(template))):
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


def _template(fn, values, inputs, section) -> Optional[List[str]]:
    """The argv with the placeholder input and output names: bound once per distinct (spec, values, input props),
    so a literal argument is resolved once, not per row."""
    from databricks.labs.gbx.pyrx.core.compression import engine_creation_options

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
    for a in block.args:
        if a.source is None:
            if a.const is True:
                tokens.append(f"--{a.arg}")
            elif a.const is not False:
                tokens.append(f"--{a.arg}={_fmt_const(a.const)}")
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
        partner = by_arg[a.with_] if a.with_ else None
        if partner is not None and _is_null(v) != _is_null(values.get(partner.source)):
            raise ParamError(
                f"{fn.sql_name}: {a.source} and {partner.source} must be supplied together (both or neither). "
                f"Fix: pass both {a.source} and {partner.source}, or neither."
            )
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
        if a.type not in ("crs",):
            check_value(fn.sql_name, a.source, v)
        text = _bind_value(fn, a, v)
        if text is not None:
            tokens.append(f"--{a.arg}" if a.type == "bool" else f"--{a.arg}={text}")
    guard_tokens(section, block.alg, tokens)
    if calc is not None:
        ins = [f"--input={v}={names[i]}" for v, i in calc.variables]
    else:
        ins = [f"--input={n}" for n in names]
    first = inputs[0]
    dtype = first.dtype if block.out_dtype == "input" else block.out_dtype
    decoded = sum(
        r.width * r.height * r.bands * _ITEMSIZE.get(r.dtype, 8) for r in inputs
    )
    creation = engine_creation_options(
        dtype, decoded, first.width, first.height, block.alg
    )
    return (
        ins
        + ["--input-format=GTiff"]
        + tokens
        + ["--output=<output>", "--output-format=GTiff"]
        + [f"--creation-option={o}" for o in creation]
        + (["--num-threads=1"] if block.alg in THREADED else [])
        + ["--quiet"]
    )


def build(
    fn,
    values: Mapping[str, Any],
    inputs: Sequence[RasterProps],
    names: Optional[Sequence[str]] = None,
    output: str = "<output>",
    section=None,
) -> Optional[List[str]]:
    """The argv for one call of `fn`, or None when a NULL value makes the result NULL.

    `values` maps the function's params to this call's values; `inputs` are the input rasters' header props;
    `names` the inputs' `/vsimem` names (default the placeholders ``<input>``, or ``<input0>``, ``<input1>``,
    ... for an array of rasters, the form ``last_command`` shows). `section` is the manifest's `gdal` section (the
    allowlist), by default the manifest's own. Raises ParamError (raise) or DataError (an error tile) when a binder
    refuses a value.
    """
    section = section if section is not None else manifest.load().gdal
    try:
        key = (id(fn), id(section), tuple(sorted(values.items())), tuple(inputs))
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
    if cached is None or (names is None and output == "<output>"):
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
        return tok

    return [fill(tok) for tok in cached]
