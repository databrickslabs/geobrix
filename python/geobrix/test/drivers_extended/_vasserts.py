"""Comparison of GeoBrix's vector rows with plain pyogrio's reading of the same dataset."""

import math
from test.drivers_extended._asserts import same_crs
from typing import List, Optional


def geom_column(columns: List[str]) -> str:
    """The geometry column of a GeoBrix vector row: the one with a ``<name>_srid`` companion."""
    cands = [
        c[: -len("_srid")]
        for c in columns
        if c.endswith("_srid") and c[: -len("_srid")] in columns
    ]
    assert len(cands) == 1, f"one geometry column expected, got {cands} in {columns}"
    return cands[0]


def _blank(v) -> bool:
    if v is None:
        return True
    try:
        import pandas as pd

        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def same_value(got, want) -> bool:
    if _blank(got) or _blank(want):
        return _blank(got) and _blank(want)
    if isinstance(want, float) or isinstance(got, float):
        return math.isclose(float(got), float(want), rel_tol=1e-9, abs_tol=1e-12)
    if hasattr(want, "isoformat") or hasattr(got, "isoformat"):
        return str(got)[:19] == str(want)[:19]
    return str(got) == str(want)


def crs_text(row, gname: str) -> Optional[str]:
    """The CRS a GeoBrix row declares for its geometry column: ``EPSG:<srid>``, else the PROJ string, else None."""
    srid = row[f"{gname}_srid"]
    if srid not in (None, "", "0"):
        return f"EPSG:{srid}"
    return row[f"{gname}_srid_proj"] or None


def assert_rows_are(rows, ref, what: str, ordering: Optional[List[str]] = None) -> None:
    """``rows`` (GeoBrix Rows of one layer) against ``ref`` (the pyogrio GeoDataFrame): the same number of
    features, the same geometry on each (WKB, compared exactly), the same attributes and the same CRS.
    """
    import shapely

    cols = list(rows[0].asDict()) if rows else []
    assert len(rows) == len(ref), f"{what}: {len(rows)} rows, pyogrio reads {len(ref)}"
    if not rows:
        return
    gname = geom_column(cols)
    attrs = [c for c in cols if c not in (gname, f"{gname}_srid", f"{gname}_srid_proj")]
    assert set(attrs) == {
        c for c in ref.columns if c != "geometry"
    }, f"{what}: columns {attrs} vs pyogrio {[c for c in ref.columns if c != 'geometry']}"
    want = [
        {**{c: ref.iloc[i][c] for c in attrs}, "__g": ref.geometry.iloc[i]}
        for i in range(len(ref))
    ]
    got = [{**r.asDict()} for r in rows]

    def sortkey(d):
        g = d["__g"] if "__g" in d else d[gname]
        wkb = (
            shapely.to_wkb(g)
            if not isinstance(g, (bytes, bytearray)) and g is not None
            else g
        )
        return (
            tuple("" if _blank(d.get(c)) else str(d.get(c)) for c in attrs),
            bytes(wkb) if wkb is not None else b"",
        )

    for d in got:
        d["__g"] = shapely.from_wkb(bytes(d[gname])) if d[gname] is not None else None
    got.sort(key=sortkey)
    want.sort(key=sortkey)
    for g, w in zip(got, want):
        for c in attrs:
            assert same_value(
                g[c], w[c]
            ), f"{what}: {c} is {g[c]!r}, pyogrio reads {w[c]!r}"
        if w["__g"] is None or g["__g"] is None:
            assert w["__g"] is None and g["__g"] is None, f"{what}: geometry"
        else:
            assert g["__g"].geom_type == w["__g"].geom_type, f"{what}: geometry type"
            assert shapely.equals_exact(
                g["__g"], w["__g"], 1e-9
            ), f"{what}: geometry {g['__g'].wkt} vs {w['__g'].wkt}"
    texts = {crs_text(r, gname) for r in rows}
    assert len(texts) == 1, f"{what}: CRS {texts}"
    text = texts.pop()
    assert same_crs(
        text, ref.crs.to_wkt() if ref.crs else None
    ), f"{what}: CRS {text} vs pyogrio {ref.crs}"
