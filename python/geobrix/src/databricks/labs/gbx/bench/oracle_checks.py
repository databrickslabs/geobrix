"""Checks of the raster generators and the raster-to-vector functions for the oracle tables (see `Case`).

Each takes the case, the rows (a tile generator's tile structs, or the structs of an array) and the case's
arguments, and returns None or a one-line reason. Expected values are computed here from the fixture with an
independent library (numpy, h3, quadbin, shapely), never from the function under test.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Optional


def _field(row, name):
    from databricks.labs.gbx.bench.oracles import _field as f

    return f(row, name)


def check_tile_rows(case, tiles, args) -> Optional[str]:
    """`tile_rows`: `expect` is {"pixels": [first-band pixels of each row, in order], "meta": a `tile_meta` dict held
    by every row}. The rows are exactly those tiles (a NoData pixel is a masked one, expected as None).
    """
    import numpy as np

    from databricks.labs.gbx.bench.oracles import _check_tile_meta, _masked_band

    want = case.expect["pixels"]
    if len(tiles) != len(want):
        return f"{case.cid}: {len(tiles)} rows, want {len(want)}"
    for i, (t, px) in enumerate(zip(tiles, want)):
        meta = replace(case, kind="tile_meta", expect=case.expect.get("meta", {}))
        why = _check_tile_meta(meta, t, args)
        if why:
            return f"row {i}: {why}"
        got = _masked_band(_field(t, "raster"))
        ref = np.ma.masked_invalid(np.array(px, dtype="float64"))
        if got.shape != ref.shape or not np.array_equal(
            np.ma.getmaskarray(got), np.ma.getmaskarray(ref)
        ):
            return f"{case.cid}: row {i} has the shape or mask {got.shape} {np.ma.getmaskarray(got).tolist()}, want {ref.shape}"
        if not np.allclose(got.filled(0.0), ref.filled(0.0)):
            return f"{case.cid}: row {i} pixels {got.filled(0).tolist()}, want {ref.filled(0).tolist()}"
    return None


def _pixel_cells(grid: str, res, cx, cy):
    """The cell key of each pixel centre, from the cell definitions: the h3 and quadbin libraries, the 1 km BNG
    squares (E, N) // size, and the custom grid's own numbering (1000 root columns of 1000 m from (0, 0), each split
    in two per resolution). A key is the cell id where one exists (h3, quadbin, custom).
    """
    import numpy as np

    if grid == "h3":
        import h3

        return [h3.str_to_int(h3.latlng_to_cell(y, x, res)) for x, y in zip(cx, cy)]
    if grid == "quadbin":
        import quadbin

        return [int(quadbin.point_to_cell(x, y, res)) for x, y in zip(cx, cy)]
    size = (
        {1: 100000, 2: 10000, 3: 1000, 4: 100, 5: 10, 6: 1}[res]
        if grid == "bng"
        else 1000 / 2**res
    )
    cols = np.floor(np.asarray(cx) / size).astype(int)
    rows = np.floor(np.asarray(cy) / size).astype(int)
    if grid == "bng":
        return list(zip(cols.tolist(), rows.tolist()))
    return [int(r) * 1000 * 2**res + int(c) | res << 56 for c, r in zip(cols, rows)]


def check_tessellation(case, tiles, args) -> Optional[str]:
    """`tessellation`: `expect` is {"grid": h3 | quadbin | bng | custom, "res": the resolution as an index, "complete":
    bool}. Each valid pixel belongs to the cell its centre is in, so the tile of a cell holds exactly the sum of the
    valid pixels there: the same cells (with `complete`, also cells with no pixel centre, empty) and the same sums.
    For BNG, whose id is not a number a library gives, the sums are compared as a list and the cells counted.
    """
    import numpy as np
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.bench.oracles import _masked_band, _raster_of

    e = case.expect
    with MemoryFile(_raster_of(args[0])) as mf, mf.open() as src:
        z = src.read(1, masked=True)
        ys, xs = np.mgrid[0 : src.height, 0 : src.width]
        cx, cy = src.transform * (xs.ravel() + 0.5, ys.ravel() + 0.5)
    keys = _pixel_cells(e["grid"], e["res"], cx, cy)
    want: dict = {}
    for k, v, m in zip(keys, z.data.ravel(), np.ma.getmaskarray(z).ravel()):
        want[k] = want.get(k, 0.0) + (0.0 if m else float(v))
    got: list = []
    for t in tiles:
        raster = _field(t, "raster")
        if raster is None:
            return f"{case.cid}: a tile with no raster ({_field(t, 'metadata')!r})"
        band = _masked_band(raster)
        got.append((_field(t, "cellid"), float(band.sum()) if band.count() else 0.0))
    if e["grid"] == "bng":
        if sorted(s for _, s in got) != sorted(want.values()):
            return f"{case.cid}: {len(got)} tiles with sums {sorted(s for _, s in got)}, want {sorted(want.values())}"
        return None
    ids = [c for c, _ in got]
    if len(set(ids)) != len(ids) or any(isinstance(c, float) for c in ids):
        return f"{case.cid}: a cell id twice, or through a float: {ids}"
    missing = set(want) - set(ids)
    extra = set(ids) - set(want)
    if missing or (extra and not e.get("complete")):
        return f"{case.cid}: missing cells {sorted(missing)}, unexpected cells {sorted(extra)}"
    sums = dict(got)
    wrong = {c: (sums[c], want[c]) for c in want if abs(sums[c] - want[c]) > 1e-6}
    wrong.update({c: (sums[c], 0.0) for c in extra if sums[c] != 0.0})
    return None if not wrong else f"{case.cid}: cell sums (got, want) differ: {wrong}"


def check_isobands(case, out, args) -> Optional[str]:
    """`isobands`: `expect` is [(band, lower, upper, area)] for the array of structs `out`, order aside."""
    from databricks.labs.gbx._geom import parse_geom

    if out is None:
        return f"{case.cid}: got NULL, want {len(case.expect)} polygons"
    got = sorted(
        (
            int(_field(r, "band")),
            float(_field(r, "lower")),
            float(_field(r, "upper")),
            round(parse_geom(bytes(_field(r, "geom_wkb"))).area, 6),
        )
        for r in out
    )
    want = sorted((b, lo, up, round(a, 6)) for b, lo, up, a in case.expect)
    return None if got == want else f"{case.cid}: polygons {got}, want {want}"


def check_contours(case, out, args) -> Optional[str]:
    """`contours`: `expect` is {value: x}. Every line of the array `out` is vertical at the x of its value (the data
    rise by one per pixel to the east, so a line of value v crosses the pixel centres' mid-line at x), and each value
    has a line."""
    from databricks.labs.gbx._geom import parse_geom

    if out is None:
        return f"{case.cid}: got NULL, want lines at {sorted(case.expect)}"
    seen = {}
    for r in out:
        v = float(_field(r, "value"))
        geom = parse_geom(bytes(_field(r, "geom_wkb")))
        xs = {
            round(c[0], 6)
            for part in getattr(geom, "geoms", [geom])
            for c in part.coords
        }
        seen.setdefault(v, set()).update(xs)
    # GDAL moves a line off a pixel-centre value by 1e-5 m, so the x of a line is held to a millimetre
    want = {float(v): x for v, x in case.expect.items()}
    ok = set(seen) == set(want) and all(
        abs(x - want[v]) < 1e-3 for v, xs in seen.items() for x in xs
    )
    return None if ok else f"{case.cid}: lines at {seen}, want {want}"


def check_polygons(case, tiles, args) -> Optional[str]:
    """`polygons`: `expect` is [(value, area)], one per row of a polygonize, order aside."""
    from databricks.labs.gbx._geom import parse_geom

    got = sorted(
        (
            float(_field(r, "value")),
            round(parse_geom(bytes(_field(r, "geom_wkb"))).area, 6),
        )
        for r in tiles
    )
    want = sorted((float(v), round(a, 6)) for v, a in case.expect)
    return None if got == want else f"{case.cid}: polygons {got}, want {want}"
