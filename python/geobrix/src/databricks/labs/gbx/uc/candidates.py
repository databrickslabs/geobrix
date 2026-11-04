"""SQL bodies that are candidates for a function's UC form: data only, never installed by default.

A candidate is a SQL expression (and its private SQL helpers) that may replace a Scala or Python handler on Unity
Catalog if a measurement shows it faster and the oracle rows agree. It lives here, not on the manifest row, so the
default install does not change before the numbers are in: a row that gained an L4 ``sql`` key would be installed as a
SQL function at once. The M2 probe (``bench.uc_perf``) reads it to build the ``sql`` leg of the row's probe copies;
``bench`` and the host tests judge it against the row's oracle rows. When a candidate wins, its body moves onto the row
(L4 ``sql``) and the entry here is deleted.

Pure data: no pyspark, no manifest import at module level.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Dict, Mapping, Optional, Tuple


@dataclass(frozen=True)
class Candidate:
    """``body`` is the text after ``RETURN`` (``{q}`` marks a helper call, as in a manifest ``sql`` key) and ``helpers``
    the private SQL functions it calls (``gbx__<x>``, the manifest's helper shape). ``limits`` lists, in words, where the
    body is known to differ from the Python handler (so a decision to adopt it reads them).
    """

    function: str
    body: str
    helpers: Tuple[Mapping, ...] = ()
    summary: str = ""
    limits: Tuple[str, ...] = ()


# ---- gbx_bng_cellarea ---------------------------------------------------------------------------------------------
# The Python core reads the cell id STRING by its shape: a letter pair (112 are cells), an even run of digits (k per axis)
# and an optional quadrant suffix. The area follows from k and the suffix alone: (10^(5-k) m / 1000)^2 km2 for a square,
# (5 * 10^(4-k) m / 1000)^2 for a quadrant, 250000 for a single letter.
_PREFIX = "'^(?:[SN][A-HJ-Z]|H[F-HJ-Z]|[TO][ABCFGHLMNQRSVWX]|J[FGHLMNQRSVWX])$'"  # the pairs of BNG's letter grid
_QUADRANT = "(length(cellid) > 2 AND substring(cellid, length(cellid) - 1, 2) IN ('SW', 'NW', 'NE', 'SE'))"
_DIGITS = (
    f"IF({_QUADRANT}, substring(cellid, 3, length(cellid) - 4), substring(cellid, 3))"
)
_K = f"(length({_DIGITS}) DIV 2)"  # digits per axis
_CELLAREA = (
    "CASE WHEN cellid IS NULL THEN NULL "
    "WHEN length(cellid) = 1 THEN IF(cellid RLIKE '^[STNOHJ]$', 250000.0, NULL) "
    f"WHEN NOT (substring(cellid, 1, 2) RLIKE {_PREFIX}) THEN NULL "
    f"WHEN NOT ({_DIGITS} RLIKE '^[0-9]*$') THEN NULL "
    f"WHEN {_QUADRANT} THEN IF({_K} <= 4, power(5.0 * power(10, 4 - {_K}) / 1000.0, 2), NULL) "
    f"ELSE IF({_K} <= 5, power(power(10, 5 - {_K}) / 1000.0, 2), NULL) END"
)

# ---- gbx_bng_eastnorthasbng ---------------------------------------------------------------------------------------
# The Python core truncates both coordinates to whole metres, takes the 100 km letter indices from them, splits the
# remainder at the cell edge (10^(6-r) m, or 10^(7-|r|) m for a quadrant resolution r < -1) and writes the cell id as
# letters + eastings digits + northings digits + the quadrant suffix. A quadrant is the half of the cell each remainder
# falls in. Resolution -1 (500 km) writes one letter, S or T, from the easting alone (the core does not read the northing).
_LETTERS = (
    "SVSWSXSYSZTVTWTX" "SQSRSSSTSUTQTRTS" "SLSMSNSOSPTLTMTN" "SFSGSHSJSKTFTGTH" "SASBSCSDSETATBTC"
    "NVNWNXNYNZOVOWOX" "NQNRNSNTNUOQOROS" "NLNMNNNONPOLOMON" "NFNGNHNJNKOFOGOH" "NANBNCNDNEOAOBOC"
    "HVHWHXHYHZJVJWJX" "HQHRHSHTHUJQJRJS" "HLHMHNHOHPJLJMJN" "HFHGHHHJHKJFJGJH"
)  # fmt: skip
_RES_NAMES = (
    ("500km", -1), ("100km", 1), ("50km", -2), ("10km", 2), ("5km", -3), ("1km", 3),
    ("500m", -4), ("100m", 4), ("50m", -5), ("10m", 5), ("5m", -6), ("1m", 6),
)  # fmt: skip
_WHEN_NAMES = " ".join(f"WHEN '{n}' THEN {i}" for n, i in _RES_NAMES)
_INDEX = "regexp_extract(btrim(resolution), '^(-?[0-9]+)', 1)"
_RESINDEX = (
    f"CASE btrim(resolution) {_WHEN_NAMES} ELSE "
    "CASE WHEN btrim(resolution) RLIKE '^-?[0-9]+([.]0+)?$' THEN "
    f"CASE WHEN try_cast({_INDEX} AS BIGINT) IN (-6, -5, -4, -3, -2, -1, 1, 2, 3, 4, 5, 6) "
    f"THEN CAST(try_cast({_INDEX} AS BIGINT) AS INT) "
    "ELSE raise_error(concat('BNG resolution index must be one of [-6, -5, -4, -3, -2, -1, 1, 2, 3, 4, 5, 6] "
    f"(1=100km..6=1m, negatives=quadrants), got ', {_INDEX}, '. Metres-as-Int (e.g. 1000) is NOT a resolution.')) END "
    "ELSE raise_error(concat('BNG resolution not supported, found ', chr(39), btrim(resolution), chr(39))) END END"
)
_DIV = "CAST(power(10, IF(r > 0, 6 - r, 7 - abs(r))) AS BIGINT)"
_EL = "least(greatest(ei DIV 100000, 0), 7)"
_NL = "least(greatest(ni DIV 100000, 0), 13)"
_CELL = (
    f"IF(r = -1, substring('SSSSSTTT', {_EL} + 1, 1), "
    f"concat(substring('{_LETTERS}', ({_NL} * 8 + {_EL}) * 2 + 1, 2), "
    f"lpad(CAST(pmod(ei, 100000) DIV {_DIV} AS STRING), IF(r > 0, r - 1, abs(r) - 2), '0'), "
    f"lpad(CAST(pmod(ni, 100000) DIV {_DIV} AS STRING), IF(r > 0, r - 1, abs(r) - 2), '0'), "
    "IF(r > 0, '', IF(pmod(ei, "
    f"{_DIV}) * 2 < {_DIV}, IF(pmod(ni, {_DIV}) * 2 < {_DIV}, 'SW', 'NW'), "
    f"IF(pmod(ni, {_DIV}) * 2 < {_DIV}, 'SE', 'NE')))))"
)
_ENAB = (
    "CASE WHEN easting IS NULL OR northing IS NULL OR resolution IS NULL THEN NULL "
    "WHEN isnan(easting) OR isnan(northing) OR abs(easting) = CAST('Infinity' AS DOUBLE) "
    "OR abs(northing) = CAST('Infinity' AS DOUBLE) THEN NULL "
    "ELSE {q}gbx__bng_cell(CAST(easting AS BIGINT), CAST(northing AS BIGINT), {q}gbx__bng_resindex(resolution)) END"
)

CANDIDATES: Dict[str, Candidate] = {
    "gbx_bng_cellarea": Candidate(
        "gbx_bng_cellarea",
        _CELLAREA,
        summary="Area of a BNG cell in square kilometres, read from the shape of its id string.",
        limits=(
            "a cell finer than 1 m is NULL (the Python handler raises KeyError)",
            "a body that Python's int() accepts but [0-9] does not (a sign, spaces, underscores, non-ASCII digits) is NULL",
        ),
    ),
    "gbx_bng_eastnorthasbng": Candidate(
        "gbx_bng_eastnorthasbng",
        _ENAB,
        helpers=(
            {
                "name": "gbx__bng_resindex",
                "params": [["resolution", "STRING"]],
                "returns": "INT",
                "sql": _RESINDEX,
                "summary": "BNG resolution index of a size name or digit string, refusing one BNG does not define.",
            },
            {
                "name": "gbx__bng_cell",
                "params": [["ei", "BIGINT"], ["ni", "BIGINT"], ["r", "INT"]],
                "returns": "STRING",
                "sql": _CELL,
                "summary": "BNG cell id of whole-metre eastings and northings at resolution index r.",
            },
        ),
        summary="BNG cell id of an easting and northing, in SQL.",
        limits=(
            "a coordinate at or west/south of -100000 m, or at or past 10^7 m, is not emulated (the core's id arithmetic overflows its fields there)",
            "surrounding whitespace is trimmed to spaces only (Python strips every whitespace character)",
            "a refusal has a comma where the Python message has a semicolon (a probe statement file splits on semicolons)",
        ),
    ),
}


def get(name: str) -> Optional[Candidate]:
    """The candidate for the manifest function ``name``, or None."""
    return CANDIDATES.get(name)


def source(name: str) -> Optional[Tuple[str, tuple]]:
    """``(body, helpers)`` of the candidate for ``name`` (the shape ``sqlbody.l4_source`` returns), or None."""
    c = CANDIDATES.get(name)
    return None if c is None else (c.body, tuple(c.helpers))


def with_candidate(fn, cand: Optional[Candidate] = None):
    """``fn`` (a manifest ``Function``) with the candidate's SQL kernel as its L4 ``sql`` key: a copy to build probe
    functions from, never stored. A row that already has a SQL kernel is returned as it is (the row wins).
    """
    from databricks.labs.gbx.uc import sqlbody

    cand = cand or CANDIDATES.get(fn.sql_name)
    if cand is None or sqlbody.l4_source(fn) is not None:
        return fn
    l4 = {**fn.levels["L4"], "sql": cand.body}
    if cand.helpers:
        l4["sql_helpers"] = tuple(cand.helpers)
    return dataclasses.replace(fn, levels={**fn.levels, "L4": l4})
