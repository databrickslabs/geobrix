"""Host stand-ins for the product SQL functions a SQL kernel body calls (`h3_*`, `st_*`).

The host's local Spark has none of the Databricks built-ins, so a body that calls one cannot be created there. A stand-in is
a Python function registered under the product name for the length of one test, so the body's own logic (the array
folds, the map lookups, the CASE branches) runs against the oracle rows. It is NOT the product: whether a product function
takes these arguments, returns this type, and treats an empty or mismatched geometry this way is judged on the workspace
(the Task 28 pass). A body that calls a product function with no stand-in here still fails on the host, as before.

A GEOMETRY value is BINARY EWKB here (the SRID rides in the bytes); a body must therefore not spell the type GEOMETRY.

Two kinds. A UDF stand-in (:func:`register`) serves a call in a plain expression or a recipe statement. Spark refuses a
Python UDF, and a SQL function whose argument is a lambda variable, inside a lambda, so a call inside `transform`,
`filter` or `aggregate` cannot be a function: :func:`rewrite` expands it in the SQL text instead (only
`h3_kringdistances` has one). A product call inside a lambda that has no expansion here (the geometry folds) is not run
on the host at all; its body is judged on the workspace.
"""

from __future__ import annotations

import contextlib
import re
from typing import Dict, Iterable, List

_CALL = re.compile(r"\b([a-z][a-z0-9_]*)\s*\(")


def _ring_case(radius: int = 3):
    """The expansion of `h3_kringdistances(cell, k)`: a function of the two argument texts that returns a CASE over the
    cell and k, built with the h3 library for the cells the oracle rows use (the disk of ``radius`` around the table's
    base cell and around the first pentagon of its resolution). A cell or k outside the table raises (a NULL k is NULL, as the
    product's is), so a row cannot
    pass on a ring the stand-in never built."""
    import h3

    from databricks.labs.gbx.bench.oracle_tables.grid_scalar import H3_CELL

    res = h3.get_resolution(h3.int_to_str(H3_CELL))
    seeds = [h3.int_to_str(H3_CELL), h3.get_pentagons(res)[0]]
    universe = sorted(
        {c for seed in seeds for c in h3.grid_disk(seed, radius)}, key=h3.str_to_int
    )

    def literal(cell, k):
        items = ", ".join(  # hop distance d: the hollow ring at d
            f"named_struct('cellid', {h3.str_to_int(c)}L, 'distance', {d})"
            for d in range(k + 1)
            for c in sorted(h3.grid_ring(cell, d))
        )
        return f"array({items})"

    def expand(cell_text, k_text):
        whens = " ".join(
            f"WHEN {h3.str_to_int(c)} THEN CASE ({k_text}) "
            + " ".join(f"WHEN {k} THEN {literal(c, k)}" for k in range(radius + 1))
            + f" ELSE CASE WHEN ({k_text}) IS NULL THEN NULL ELSE raise_error(concat("
            f"'h3_kringdistances stand-in: no ring built for k = ', CAST(({k_text}) AS STRING), "
            f"' (built to {radius})')) END END"
            for c in universe
        )
        return (
            f"(CASE ({cell_text}) {whens} "
            "ELSE raise_error('h3_kringdistances stand-in: no ring built for this cell') END)"
        )

    return expand


def rewrite(text: str) -> str:
    """``text`` with each `h3_kringdistances(cell, k)` call expanded in place (see the module note)."""
    from databricks.labs.gbx.uc import sqlbody

    pat = re.compile(r"\bh3_kringdistances\s*\(")
    expand, out, pos = None, [], 0
    while True:
        m = pat.search(text, pos)
        if m is None:
            out.append(text[pos:])
            return "".join(out)
        args, end = sqlbody._call_end(text, m.end() - 1)
        if len(args) != 2:
            raise ValueError(
                f"h3_kringdistances takes 2 arguments, the body passes {len(args)}"
            )
        expand = expand or _ring_case()
        out.append(text[pos : m.start()])
        out.append(expand(rewrite(args[0]), rewrite(args[1])))
        pos = end


def _geometry_functions(spark):
    import shapely
    from pyspark.sql import functions as f
    from pyspark.sql.types import BinaryType, BooleanType, StringType

    def load(b):
        return None if b is None else shapely.from_wkb(bytes(b))

    def ewkb(g, srid):
        return shapely.to_wkb(shapely.set_srid(g, srid or 0), include_srid=True)

    def same_srid(a, b, name):
        sa, sb = shapely.get_srid(a), shapely.get_srid(b)
        if sa != sb:
            raise ValueError(f"{name}: the geometries have SRIDs {sa} and {sb}")
        return sa

    @f.udf(returnType=BinaryType())
    def st_geomfromewkb(b):
        return None if b is None else ewkb(load(b), shapely.get_srid(load(b)))

    @f.udf(returnType=BinaryType())
    def st_geomfromwkb(b, srid=None):
        if b is None:
            return None
        g = load(b)
        return ewkb(g, shapely.get_srid(g) if srid is None else int(srid))

    @f.udf(returnType=BinaryType())
    def st_geomfromwkt(w, srid=None):
        return None if w is None else ewkb(shapely.from_wkt(w), srid)

    @f.udf(returnType=BinaryType())
    def st_union(a, b):
        if a is None or b is None:
            return None
        ga, gb = load(a), load(b)
        return ewkb(ga.union(gb), same_srid(ga, gb, "st_union"))

    @f.udf(returnType=BinaryType())
    def st_intersection(a, b):
        if a is None or b is None:
            return None
        ga, gb = load(a), load(b)
        return ewkb(ga.intersection(gb), same_srid(ga, gb, "st_intersection"))

    @f.udf(returnType=BooleanType())
    def st_isempty(a):
        return None if a is None else bool(load(a).is_empty)

    @f.udf(returnType=BinaryType())
    def st_asbinary(a):
        return None if a is None else shapely.to_wkb(load(a))

    @f.udf(returnType=BinaryType())
    def st_asewkb(a):
        return None if a is None else ewkb(load(a), shapely.get_srid(load(a)))

    @f.udf(returnType=StringType())
    def st_astext(a):
        return None if a is None else shapely.to_wkt(load(a))

    for u in (
        st_geomfromewkb,
        st_geomfromwkb,
        st_geomfromwkt,
        st_union,
        st_intersection,
        st_isempty,
        st_asbinary,
        st_asewkb,
        st_astext,
    ):
        spark.udf.register(u.func.__name__, u)


def _union_agg(spark):
    import pandas as pd
    import shapely
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import BinaryType

    @pandas_udf(BinaryType())
    def st_union_agg(s: pd.Series) -> bytes:
        geoms = [shapely.from_wkb(bytes(b)) for b in s if b is not None]
        if not geoms:
            return None
        srids = {shapely.get_srid(g) for g in geoms}
        if len(srids) > 1:
            raise ValueError(f"st_union_agg: the geometries have SRIDs {sorted(srids)}")
        u = shapely.union_all(geoms)
        return shapely.to_wkb(shapely.set_srid(u, srids.pop()), include_srid=True)

    spark.udf.register("st_union_agg", st_union_agg)


_GEOMETRY = (
    "st_geomfromewkb",
    "st_geomfromwkb",
    "st_geomfromwkt",
    "st_union",
    "st_intersection",
    "st_isempty",
    "st_asbinary",
    "st_asewkb",
    "st_astext",
)
# name -> the registrar that installs it (and every name it installs together)
STAND_INS: Dict[str, object] = {n: _geometry_functions for n in _GEOMETRY}
STAND_INS["st_union_agg"] = _union_agg
_INSTALLS: Dict[object, List[str]] = {
    _geometry_functions: list(_GEOMETRY),
    _union_agg: ["st_union_agg"],
}


# SQL kernels whose product calls sit inside a lambda that has no expansion above (the geometry folds): their SQL runs
# on the workspace only. The function's session level (L3) and its oracle rows still run on the host, and a text test
# pins the body.
WORKSPACE_ONLY: Dict[str, str] = {
    "gbx_bng_cellunion_array": "folds with the product st_union inside aggregate()",
    "gbx_bng_cellintersection_array": "folds with the product st_intersection inside aggregate()",
}


def rewrite_source(source):
    """A kernel source ``(body, helpers)`` with :func:`rewrite` applied to the body and every helper."""
    body, helpers = source
    return rewrite(body), tuple({**h, "sql": rewrite(h["sql"])} for h in helpers)


def needed(*texts: str) -> List[str]:
    """The UDF stand-in names the SQL ``texts`` call, sorted (a name outside the stand-in table is not listed)."""
    called = {m.group(1) for t in texts for m in _CALL.finditer(t.lower())}
    return sorted(called & set(STAND_INS))


def register(spark, texts: Iterable[str]) -> List[str]:
    """Install the stand-ins that ``texts`` call; return every name installed (for :func:`drop`)."""
    registrars = {STAND_INS[n] for n in needed(*texts)}
    names: List[str] = []
    for r in registrars:
        r(spark)
        names += _INSTALLS[r]
    return names


def drop(spark, names: Iterable[str]) -> None:
    for n in names:
        spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")


@contextlib.contextmanager
def l0_stand_in(fn):
    """The session's L0 install of ``fn`` (``sqlwrap.sql_kernel``) with its body's product calls expanded, for one test."""
    from databricks.labs.gbx.uc import sqlbody

    real = sqlbody.l0_source
    sqlbody.l0_source = lambda f: (None if real(f) is None else rewrite_source(real(f)))
    try:
        yield
    finally:
        sqlbody.l0_source = real
