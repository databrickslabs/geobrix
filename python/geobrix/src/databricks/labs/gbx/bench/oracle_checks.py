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

    from databricks.labs.gbx.bench.oracles import _masked_band, _ref_dataset

    e = case.expect
    if "covering" in e:
        # coverage "covering": a cell any pixel overlaps gets a tile. `covering` is a point in each such cell, so the
        # cells are the ones those points lie in, each once.
        px, py = zip(*e["covering"])
        want_ids = set(_pixel_cells(e["grid"], e["res"], np.array(px), np.array(py)))
        ids = [_field(t, "cellid") for t in tiles]
        if set(ids) == want_ids and len(ids) == len(want_ids):
            return None
        return f"{case.cid}: cells {sorted(ids)}, want each of {sorted(want_ids)} once"
    with _ref_dataset(args) as src:
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


def check_features_ref(case, out, args) -> Optional[str]:
    """`features_ref`: the lines of the array `out`, by level, against GDAL's older `ContourGenerateEx` on the same tile
    (`oracles._ref_contours`). `expect` names the rule:

    - ``exact``: the GDAL spec engine runs the algorithm the reference runs. The same levels, the same number of lines
      per level, and every level's lines within a millionth of a pixel of the reference (Hausdorff distance), with the
      same total length.
    - ``equivalent``: the earlier scikit-image lines, which are still the Unity Catalog kernel's. They trace the same
      crossings, but stop at the outermost pixel centres where GDAL's run to the tile's edge (half a pixel farther
      along each axis), and the two split a line at a NoData cell differently. The same levels, and each level's
      lines within one pixel of the reference (Hausdorff distance): measured at 0.5 to 0.86 pixel on a smooth surface
      and on noise with NoData holes. The line count and length may differ.
    """
    import shapely
    from shapely.ops import unary_union

    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.bench.oracles import _ref_contours

    if out is None:
        return f"{case.cid}: got NULL, want contour lines"
    got = {}
    for r in out:
        got.setdefault(float(_field(r, "value")), []).append(
            parse_geom(bytes(_field(r, "geom_wkb")))
        )
    want, px = _ref_contours(args)
    if sorted(got) != sorted(want):
        return f"{case.cid}: levels {sorted(got)}, reference {sorted(want)}"
    exact = case.expect == "exact"
    for lv in sorted(want):
        g, w = got[lv], want[lv]
        away = shapely.hausdorff_distance(unary_union(g), unary_union(w)) / px
        if not exact:
            if away > 1.0:
                return f"{case.cid}: level {lv} lies {away:.3g} px from the reference"
            continue
        lg, lw = sum(x.length for x in g), sum(x.length for x in w)
        if len(g) != len(w):
            return f"{case.cid}: level {lv} has {len(g)} lines, reference {len(w)}"
        if away > 1e-6 or abs(lg - lw) > 1e-9 * max(1.0, lw):
            return f"{case.cid}: level {lv} lies {away:.3g} px from the reference (length {lg}, reference {lw})"
    return None


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

    from databricks.labs.gbx.bench.oracles import _ref_dataset

    e = case.expect
    with _ref_dataset(args) as src:
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


def check_grid_stats_json(case, out, args) -> Optional[str]:
    """`grid_stats_json`: a recipe's one value, the JSON text of its (band, cellID, measure) rows, judged as
    `grid_stats` judges them. `args` are those of the session row the recipe stands for (the whole tile, which is
    what the tiles the recipe merges were cut from)."""
    import json

    if out is None:
        return f"{case.cid}: got NULL, want the rows of a grid reducer"
    return check_grid_stats(case, json.loads(out), args)


def _stats_reference(args):
    """Per band of the tile `args[0]`, from numpy over the masked pixels (not the function under test): (count, sum, M2,
    least, greatest, valid values); a band with no valid pixel is (0.0, None, None, None, None, []).
    """
    from databricks.labs.gbx.bench.oracles import _ref_dataset

    out = []
    with _ref_dataset(args) as src:
        for b in range(1, src.count + 1):
            band = src.read(b, masked=True)
            v = band.compressed().astype("float64")
            if v.size == 0:
                out.append((0.0, None, None, None, None, v))
                continue
            out.append(
                (
                    float(v.size),
                    float(v.sum()),
                    float(((v - v.sum() / v.size) ** 2).sum()),
                    float(v.min()),
                    float(v.max()),
                    v,
                )
            )
    return out


def _same_number(a, b) -> bool:
    import math

    if a is None or b is None:
        return a is None and b is None
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9)


def check_stats_partial(case, out, args) -> Optional[str]:
    """`stats_partial`: `out` is the array of structs (band, count, sum, m2, min, max), one per band of the tile in order.
    Each is recomputed with numpy over the valid pixels (count, sum, M2 from the mean, least, greatest); a band with no
    valid pixel is (0.0, NULL, NULL, NULL, NULL). A list in `expect` is held to as well (the fixture's own arithmetic).
    """
    if out is None:
        return f"{case.cid}: got NULL, want one state for each band"
    rows = list(out)
    want = _stats_reference(args)
    if [int(_field(r, "band")) for r in rows] != list(range(1, len(want) + 1)):
        return f"{case.cid}: the bands are {[_field(r, 'band') for r in rows]}, want 1..{len(want)}"
    fields = ("count", "sum", "m2", "min", "max")
    for r, w in zip(rows, want):
        got = tuple(_field(r, f) for f in fields)
        if not all(_same_number(g, x) for g, x in zip(got, w[:5])):
            return f"{case.cid}: band {_field(r, 'band')} is {got}, want {w[:5]}"
    if case.expect is not None:
        for r, e in zip(rows, case.expect):
            got = tuple(_field(r, f) for f in fields)
            if not all(_same_number(g, x) for g, x in zip(got, e)):
                return f"{case.cid}: band {_field(r, 'band')} is {got}, the fixture's arithmetic gives {e}"
        if len(rows) != len(case.expect):
            return f"{case.cid}: {len(rows)} bands, want {len(case.expect)}"
    return None


def check_stats_merge_json(case, out, args) -> Optional[str]:
    """`stats_merge_json`: a recipe's one value, the JSON text of the rows (band, count, sum, avg, variance, min, max) it
    merged from the partial states of tiles cut from one tile. `args[0]` is the WHOLE tile, so the merged statistics are
    those of the union of the pixels: the count, sum, mean, variance (M2 over the count), least and greatest.
    """
    import json

    if out is None:
        return f"{case.cid}: got NULL, want the merged statistics of each band"
    rows = sorted(json.loads(out), key=lambda r: r["band"])
    want = _stats_reference(args)
    if [r["band"] for r in rows] != list(range(1, len(want) + 1)):
        return f"{case.cid}: the bands are {[r['band'] for r in rows]}, want 1..{len(want)}"
    for r, w in zip(rows, want):
        n, s, m2, lo, hi, _ = w
        ref = {
            "count": n,
            "sum": s,
            "avg": s / n,
            "variance": m2 / n,
            "min": lo,
            "max": hi,
        }
        bad = {k: (r[k], v) for k, v in ref.items() if not _same_number(r[k], v)}
        if bad:
            return f"{case.cid}: band {r['band']} differs from the union (got, want): {bad}"
    return None


def _band_cells(args, e):
    """Per band the valid pixel values of each cell (a dict cell id -> list), the bands (masked) and the pixel centres,
    from the tile `args[0]`: a pixel is in the cell its centre is in (the same reading as `check_grid_stats`).
    """
    import numpy as np

    from databricks.labs.gbx.bench.oracles import _ref_dataset

    with _ref_dataset(args) as src:
        bands = [src.read(b, masked=True) for b in range(1, src.count + 1)]
        ys, xs = np.mgrid[0 : src.height, 0 : src.width]
        cx, cy = src.transform * (xs.ravel() + 0.5, ys.ravel() + 0.5)
        keys = _grid_keys(e["grid"], e["res"], cx, cy)
        geo = (src.height, src.width, src.transform)
    per_band = []
    for band in bands:
        cells: dict = {}
        flat, mask = band.data.ravel(), np.ma.getmaskarray(band).ravel()
        for k, v, m in zip(keys, flat, mask):
            if not m:
                cells.setdefault(k, []).append(float(v))
        per_band.append(cells)
    return bands, keys, per_band, geo


def check_grid_partial(case, out, args) -> Optional[str]:
    """`grid_partial`: `expect` is {"grid", "res" (an index)} and optionally "coverage" ("sparse": no cell but those
    with a valid pixel) and "assignment" ("covering": the counts of a band add up to its valid pixels). `out` is the
    array of structs (band, cellid, count, sum, m2, min, max).

    For the default centroid assignment numpy recomputes each field over the valid pixels of each cell: the count, the
    sum, M2 (the sum of the squared deviations from the cell's mean), the least and the greatest. A cell that holds no
    valid pixel may be there (complete coverage) with a count of 0.0 and NULL for the other fields. A band of nothing
    but NoData has only such cells.
    """
    import numpy as np

    e = case.expect
    if out is None:
        return f"{case.cid}: got NULL, want the cells' partial state"
    rows = list(out)
    bands, keys, per_band, geo = _band_cells(args, e)
    got: dict = {}
    for r in rows:
        k = (int(_field(r, "band")), _field(r, "cellid"))
        if k in got:
            return f"{case.cid}: band {k[0]} cell {k[1]} twice"
        got[k] = tuple(_field(r, f) for f in ("count", "sum", "m2", "min", "max"))
    if any(isinstance(c, (float, bool)) for _, c in got):
        return f"{case.cid}: a cell id is not an int or a string: {sorted(map(str, got))[:3]}"
    if e.get("assignment") == "covering":
        counts = {k: v[0] for k, v in got.items()}
        from databricks.labs.gbx.bench.oracles import _ref_dataset

        with _ref_dataset(args) as src:
            ys, xs = np.mgrid[0 : src.height, 0 : src.width]
            cx, cy = src.transform * (xs.ravel() + 0.5, ys.ravel() + 0.5)
            return _check_covering(
                case,
                {**e, "stat": "count"},
                bands,
                counts,
                keys,
                (src, cx, cy),
            )
    want: dict = {}
    for b, cells in enumerate(per_band, start=1):
        for k, v in cells.items():
            a = np.asarray(v)
            want[(b, k)] = (
                float(len(a)),
                float(a.sum()),
                float(((a - a.sum() / len(a)) ** 2).sum()),
                float(a.min()),
                float(a.max()),
            )
    missing = sorted(set(want) - set(got), key=str)
    if missing:
        return f"{case.cid}: {len(missing)} cells missing, e.g. {missing[:3]}"
    extra = sorted(set(got) - set(want), key=str)
    if extra and e.get("coverage") == "sparse":
        return f"{case.cid}: {len(extra)} cells without a valid pixel, e.g. {extra[:3]}"
    bad = [k for k in extra if got[k] != (0.0, None, None, None, None)]
    if bad:
        return f"{case.cid}: empty cells {bad[:3]} have the state {got[bad[0]]}, want (0.0, NULL, NULL, NULL, NULL)"
    for k, w in want.items():
        g = got[k]
        if any(x is None or abs(x - y) > 1e-9 * max(1.0, abs(y)) for x, y in zip(g, w)):
            return f"{case.cid}: {k} got {g}, want {w}"
    return None


def _cellfill_pairs(case, out):
    """The ``(cell id, value)`` pairs of a fill result and a problem text: the aggregate's BINARY payload (decoded here
    from its documented layout, so the check does not call the function under test) or the array form's structs.
    """
    import struct

    if not isinstance(
        out, (bytes, bytearray, memoryview)
    ):  # the array form: structs of (cellid, value)
        from databricks.labs.gbx.pygx import _bng

        def part(row, name, pos):
            try:
                return row[name]
            except (KeyError, TypeError, IndexError, ValueError):
                return row[pos]

        got = [
            (
                (lambda c: _bng.parse(c) if isinstance(c, str) else c)(
                    part(r, "cellid", 0)
                ),
                part(r, "value", 1),
            )
            for r in out
        ]
        return got, None
    data = bytes(out)
    (count,) = struct.unpack_from(">i", data, 0)
    pos, got = 4, []
    for _ in range(count):
        (cid,) = struct.unpack_from(">q", data, pos)
        (present,) = struct.unpack_from(">?", data, pos + 8)
        pos += 9
        if present:
            (v,) = struct.unpack_from(">d", data, pos)
            pos += 8
        else:
            v = None
        got.append((cid, v))
    if pos != len(data):
        return got, f"{case.cid}: {len(data) - pos} bytes left after {count} cells"
    return got, None


def check_cellfill(case, out, args) -> Optional[str]:
    """`cellfill`: a fill result holds `case.expect` ({cell id: value or None}, a BNG id as its string). The result is
    the BINARY payload of a cellfill aggregate (a big-endian Int32 count, then per cell an Int64 id, a present byte and,
    when present, a Float64) or the array of `(cellid, value)` structs of a `_cellfill_array` function; either is read
    here, not by the function under test. Cells come out in ascending unsigned id order.
    """
    import math

    if out is None:
        return f"{case.cid}: got NULL, want {len(case.expect)} cells"
    got, problem = _cellfill_pairs(case, out)
    if problem:
        return problem
    want = {}
    for k, v in case.expect.items():
        if isinstance(k, str):  # a BNG reference: its id is the grid's own number
            from databricks.labs.gbx.pygx import _bng

            k = _bng.parse(k)
        want[k] = v
    order = sorted(want, key=lambda x: x & 0xFFFF_FFFF_FFFF_FFFF)
    if [c for c, _ in got] != order:
        return f"{case.cid}: cells {[c for c, _ in got]}, want {order}"
    for c, v in got:
        w = want[c]
        if (v is None) != (w is None) or (
            v is not None and not math.isclose(v, w, rel_tol=1e-9, abs_tol=1e-9)
        ):
            return f"{case.cid}: cell {c} is {v}, want {w}"
    return None


def check_mvt_layer(case, out, args) -> Optional[str]:
    """`mvt_layer`: `out` is one MVT layer named `expect["layer"]` holding exactly `expect["features"]`, a list of
    (WKT in tile coordinates, properties), in any order. Decoded with mapbox_vector_tile, not the encoder under test.
    """
    import mapbox_vector_tile
    import shapely
    from shapely.geometry import shape

    if out is None:
        return f"{case.cid}: got NULL, want a layer"
    layers = mapbox_vector_tile.decode(
        bytes(out), default_options={"y_coord_down": True}
    )
    e = case.expect
    if list(layers) != [e["layer"]]:
        return f"{case.cid}: layers {list(layers)}, want {[e['layer']]}"
    got = sorted(
        (shapely.normalize(shape(f["geometry"])).wkt, sorted(f["properties"].items()))
        for f in layers[e["layer"]]["features"]
    )
    want = sorted(
        (shapely.normalize(shapely.from_wkt(g)).wkt, sorted(p.items()))
        for g, p in e["features"]
    )
    return None if got == want else f"{case.cid}: features {got}, want {want}"


def check_drivers(case, rows, args) -> Optional[str]:
    """`drivers`: the rows of ``gbx_drivers`` for one filter. `expect` is {"kind": the filter the call used
    (None: every driver), "present": {driver: {column: value}}, "absent": [driver]}. Beyond the named rows: each
    driver once; the filter holds for every row; ``tile_bytes`` implies a raster driver GeoBrix allows; ``available``
    implies ``allowed``; a driver that is not available says why in ``requires``; the raster names are rasterio's own
    registry's (an independent read of the same libgdal).
    """
    want = case.expect
    got = {}
    for r in rows:
        name = _field(r, "driver")
        if name in got:
            return f"{case.cid}: driver {name!r} is listed twice"
        got[name] = r
    kind = want.get("kind")
    for name, r in got.items():
        if kind is not None and not _field(r, kind):
            return f"{case.cid}: {name!r} is not a {kind} driver"
        if not _field(r, "description"):
            return f"{case.cid}: {name!r} has no description"
        if _field(r, "tile_bytes") and not (
            _field(r, "raster") and _field(r, "allowed")
        ):
            return f"{case.cid}: {name!r} holds tile bytes but is not an allowed raster driver"
        if _field(r, "available") and not _field(r, "allowed"):
            return f"{case.cid}: {name!r} is available but not allowed"
        if (
            not _field(r, "available")
            and _field(r, "allowed")
            and not _field(r, "requires")
        ):
            return f"{case.cid}: {name!r} is allowed but not available, and says nothing in `requires`"
        url = _field(r, "help_url")
        if url is not None and not url.startswith("https://gdal.org/"):
            return f"{case.cid}: {name!r} has the help URL {url!r}"
    for name, cols in want.get("present", {}).items():
        if name not in got:
            return f"{case.cid}: {name!r} is not listed"
        for col, value in cols.items():
            have = _field(got[name], col)
            have = list(have) if isinstance(have, (list, tuple)) else have
            if have != value:
                return f"{case.cid}: {name}.{col} is {have!r}, want {value!r}"
    for name in want.get("absent", ()):
        if name in got:
            return f"{case.cid}: {name!r} is listed"
    import rasterio

    with rasterio.Env() as env:
        theirs = set(env.drivers())  # every registered driver, vector ones too
    if kind is None and set(got) != theirs:
        return (
            f"{case.cid}: drivers differ from rasterio's registry: "
            f"only here {sorted(set(got) - theirs)[:5]}, only rasterio {sorted(theirs - set(got))[:5]}"
        )
    if kind is not None and not set(got) <= theirs:
        return (
            f"{case.cid}: not in rasterio's registry: {sorted(set(got) - theirs)[:5]}"
        )
    return None
