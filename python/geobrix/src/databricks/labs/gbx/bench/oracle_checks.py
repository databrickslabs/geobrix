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
        have, need = sorted(s for _, s in got), sorted(want.values())
        if e.get(
            "complete"
        ):  # a cell with no pixel centre is emitted empty (sum 0.0): extra zeros are allowed
            wanted_zeros = need.count(0.0)
            if have.count(0.0) >= wanted_zeros:
                have = sorted([s for s in have if s != 0.0] + [0.0] * wanted_zeros)
        if have != need:
            return f"{case.cid}: {len(got)} tiles with sums {sorted(s for _, s in got)}, want {need}"
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


def _bng_square(e: float, n: float, size: int) -> str:
    """The Ordnance Survey grid reference of the `size` m square holding (e, n): two letters for the 100 km square,
    then the easting and northing digits within it (the published OS algorithm, not the grid code under test).
    """
    e100, n100 = int(e // 100000), int(n // 100000)
    first = (19 - n100) - (19 - n100) % 5 + (e100 + 10) // 5
    second = ((19 - n100) * 5) % 25 + e100 % 5
    first += first > 7  # the alphabet has no I
    second += second > 7
    width = len(str(100000 // size)) - 1  # 0 digits for the 100 km square itself
    digits = (
        str(int(e % 100000 // size)).zfill(width)
        + str(int(n % 100000 // size)).zfill(width)
        if width
        else ""
    )
    return chr(first + 65) + chr(second + 65) + digits


def _grid_keys(grid: str, res, cx, cy) -> list:
    """The id of the cell each pixel centre is in: a number for h3, quadbin and custom, the OS reference for BNG."""
    if grid != "bng":
        return _pixel_cells(grid, res, cx, cy)
    sizes = {1: 100000, 2: 10000, 3: 1000, 4: 100, 5: 10, 6: 1}
    if res not in sizes:
        raise ValueError(
            f"the oracle computes BNG resolutions 1 to 6 only, not {res!r} (quadrant resolutions are unsupported in the oracle)"
        )
    size = sizes[res]
    return [_bng_square(x, y, size) for x, y in zip(cx, cy)]


def _reduce(stat: str, vals):
    """The statistic of the valid pixel values of one cell (population variance and standard deviation)."""
    import numpy as np

    return float(
        {
            "avg": np.mean,
            "count": len,
            "max": np.max,
            "median": np.median,
            "min": np.min,
            "stddev": np.std,
            "sum": np.sum,
            "variance": np.var,
        }[stat](vals)
    )


def _check_covering(case, e, bands, got, keys, geo) -> Optional[str]:
    """`assignment: "covering"`, judged for `count` only (the share of a pixel in a cell is the overlap area, which
    the oracle does not recompute): per band the counts add up to its valid pixels (each pixel is shared out whole),
    the cell each valid pixel's centre is in holds a positive share, and a cell with a positive share is the cell of
    some valid pixel's centre or corner (so a wrong cell id fails; a cell lying inside one pixel is not judged).
    """
    import numpy as np

    if e["stat"] != "count":
        return (
            f"{case.cid}: covering is judged for count only, not {e['stat']!r} "
            "(the oracle does not recompute overlap-weighted statistics)"
        )
    src, cx, cy = geo
    cols, rows = np.mgrid[0 : src.height, 0 : src.width]
    corners = [
        _grid_keys(
            e["grid"],
            e["res"],
            *(src.transform * (cols.ravel() + dc, rows.ravel() + dr)),
        )
        for dr in (0, 1)
        for dc in (0, 1)
    ]
    for b, band in enumerate(bands, start=1):
        have = sum(m for (bb, _), m in got.items() if bb == b and m is not None)
        if abs(have - band.count()) > 1e-6:
            return f"{case.cid}: band {b} counts add up to {have}, want its {band.count()} valid pixels"
        valid = ~np.ma.getmaskarray(band).ravel()
        centre = {k for k, v in zip(keys, valid) if v}
        near = centre | {k for ks in corners for k, v in zip(ks, valid) if v}
        share = {c for (bb, c), m in got.items() if bb == b and m is not None and m > 0}
        if centre - share:
            return f"{case.cid}: band {b} cells {sorted(map(str, centre - share))[:3]} hold a pixel centre but no share"
        if share - near:
            return f"{case.cid}: band {b} cells {sorted(map(str, share - near))[:3]} overlap no valid pixel's centre or corner"
    return None


def check_grid_stats(case, rows, args) -> Optional[str]:
    """`grid_stats`: `expect` is {"grid", "res" (an index), "stat"} and optionally "coverage" ("sparse": no cell
    but those with a valid pixel), "assignment" ("covering": the counts of a band add up to its valid pixels,
    each pixel shared out by area) and "past_2_53" (a cell id past 2**53 must come out exact, as an int).

    The rows are (band, cellID, measure). Each valid pixel of a band (its own NoData and mask) is in the cell its
    centre is in, and numpy reduces them per cell. With the default `complete` coverage a cell that holds no valid
    pixel may also come out, with a NULL measure (0.0 for a count).
    """
    import numpy as np
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.bench.oracles import _raster_of

    e = case.expect
    with MemoryFile(_raster_of(args[0])) as mf, mf.open() as src:
        bands = [src.read(b, masked=True) for b in range(1, src.count + 1)]
        ys, xs = np.mgrid[0 : src.height, 0 : src.width]
        cx, cy = src.transform * (xs.ravel() + 0.5, ys.ravel() + 0.5)
    keys = _grid_keys(e["grid"], e["res"], cx, cy)
    got: dict = {}
    for r in rows:
        k = (int(_field(r, "band")), _field(r, "cellID"))
        if k in got:
            return f"{case.cid}: band {k[0]} cell {k[1]} twice"
        got[k] = _field(r, "measure")
    if any(isinstance(c, float) or isinstance(c, bool) for _, c in got):
        return f"{case.cid}: a cell id is not an int or a string: {sorted(map(str, got))[:3]}"
    if e.get("assignment") == "covering":
        return _check_covering(case, e, bands, got, keys, (src, cx, cy))
    want: dict = {}
    for b, band in enumerate(bands, start=1):
        cells: dict = {}
        flat, mask = band.data.ravel(), np.ma.getmaskarray(band).ravel()
        for k, v, m in zip(keys, flat, mask):
            if not m:
                cells.setdefault(k, []).append(float(v))
        want.update({(b, k): _reduce(e["stat"], v) for k, v in cells.items()})
    if e.get("past_2_53") and not any(
        isinstance(c, int) and c > 2**53 for _, c in want
    ):
        return f"{case.cid}: the row expects no cell id past 2**53"
    missing = sorted(set(want) - set(got), key=str)
    if missing:
        return f"{case.cid}: {len(missing)} cells missing, e.g. {missing[:3]}"
    extra = sorted(set(got) - set(want), key=str)
    if extra and e.get("coverage") == "sparse":
        return f"{case.cid}: {len(extra)} cells without a valid pixel, e.g. {extra[:3]}"
    empty = 0.0 if e["stat"] == "count" else None
    bad_extra = [k for k in extra if got[k] != empty]
    if bad_extra:
        return f"{case.cid}: empty cells {bad_extra[:3]} have the measure {got[bad_extra[0]]}, want {empty}"
    off = {
        k: (got[k], v)
        for k, v in want.items()
        if got[k] is None or abs(got[k] - v) > 1e-9 * max(1.0, abs(v))
    }
    return (
        None
        if not off
        else f"{case.cid}: (got, want) differ: {dict(list(off.items())[:3])}"
    )
