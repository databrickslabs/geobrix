"""The oracle tables are found on first use of the mapping, by any read, not only by a missing-key lookup."""

import subprocess
import sys

import pytest

PROBES = {
    "sorted": "sorted(oracles.TABLES)",
    "in": "'MVP_A' in oracles.TABLES",
    "len": "len(oracles.TABLES)",
    "get": "oracles.TABLES.get('MVP_A')",
    "keys": "list(oracles.TABLES.keys())",
    "items": "list(oracles.TABLES.items())",
    "values": "list(oracles.TABLES.values())",
}


@pytest.mark.parametrize("probe", sorted(PROBES))
def test_a_fresh_import_finds_the_tables_on_any_read(probe):
    """Each probe runs in a fresh interpreter, so nothing has looked a key up first."""
    code = (
        "from databricks.labs.gbx.bench import oracles; "
        f"r = {PROBES[probe]}; "
        "print(bool(r))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "True", probe


def test_concurrent_first_reads_import_the_family_modules_once(monkeypatch):
    import threading
    import time

    from databricks.labs.gbx.bench import oracles

    calls = []

    def slow_tables():
        calls.append(1)
        time.sleep(0.2)
        return {"T": ("row",)}

    monkeypatch.setattr(oracles, "_tables", slow_tables)
    tables, seen = oracles._Tables(), []
    threads = [
        threading.Thread(target=lambda: seen.append(dict(tables.items())))
        for _ in range(6)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(calls) == 1 and seen == [{"T": ("row",)}] * 6  # none saw a partial dict


def test_a_builder_a_family_module_registers_on_import_is_found_before_any_table_read(
    monkeypatch,
):
    """`_build` finds the tables first, which imports the family modules and so registers their builders."""
    from databricks.labs.gbx.bench import oracles

    def family_import():
        oracles.BUILDERS["late_fixture"] = lambda tmpdir: "late"
        return {}

    monkeypatch.setattr(oracles, "_tables", family_import)
    monkeypatch.setattr(oracles, "TABLES", oracles._Tables())
    monkeypatch.delitem(oracles.BUILDERS, "late_fixture", raising=False)
    try:
        assert oracles.resolve_arg(oracles.Build("late_fixture"), "/x") == "late"
    finally:
        oracles.BUILDERS.pop("late_fixture", None)


def test_the_aggregate_view_is_typed_from_the_manifest_params():
    import dataclasses

    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.bench import oracles

    fn = manifest.load().functions["gbx_rst_bng_rasterize_agg"]
    case = next(c for c in getattr(oracles, fn.oracle) if c.fn == fn.sql_name)
    assert oracles.argument_schema(case, []) == "c0 STRING, c1 DOUBLE"
    wide = dataclasses.replace(
        fn,
        params=(dataclasses.replace(fn.params[0], session=("BIGINT",)), *fn.params[1:]),
    )
    from types import SimpleNamespace

    man = SimpleNamespace(functions={fn.sql_name: wide})
    import unittest.mock

    with unittest.mock.patch.object(manifest, "load", lambda: man):
        assert oracles.argument_schema(case, []) == "c0 BIGINT, c1 DOUBLE"


def test_an_aggregate_verdict_checks_the_resolved_arguments(monkeypatch):
    import dataclasses

    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.bench import oracles

    fn = manifest.load().functions["gbx_rst_bng_rasterize_agg"]
    case = next(c for c in getattr(oracles, fn.oracle) if c.fn == fn.sql_name)
    case = dataclasses.replace(case, args=(oracles.Build("late_agg"), *case.args[1:]))
    monkeypatch.setitem(oracles.BUILDERS, "late_agg", lambda tmpdir: ["resolved"])
    seen = []
    monkeypatch.setattr(oracles, "check", lambda c, out, args: seen.append(args))
    oracles.verdict(case, ["out"], [], "/tmp")
    assert seen[0][0] == ["resolved"]


def _ewkb(wkt, srid):
    import shapely

    return shapely.to_wkb(
        shapely.set_srid(shapely.from_wkt(wkt), srid), include_srid=True
    )


def _sized(width, height, fill=0.0, nodata=None):
    """A tile whose raster is `width` x `height` pixels, all `fill`, declaring `nodata`."""
    import numpy as np

    from databricks.labs.gbx.bench import oracles

    return oracles._tile(
        oracles._gtiff(np.full((1, height, width), fill), "EPSG:3857", nodata=nodata)
    )


def _png(alpha_value, size=4):
    """A size x size RGBA PNG whose alpha band is all `alpha_value`."""
    import numpy as np

    from databricks.labs.gbx.bench import oracles

    px = np.full((4, size, size), alpha_value, dtype="uint8")
    return oracles._encode(px, "PNG")


def _with_overviews(levels):
    """The `_sized(32, 24)` raster with overviews at `levels` (none for an empty list)."""
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import ops

    raster = _sized(32, 24)["raster"]
    if not levels:
        return {**_sized(32, 24)}
    with MemoryFile(raster) as mf, mf.open() as ds:
        return {**_sized(32, 24), "raster": ops.build_overviews(ds, levels, "average")}


def _blocked(size):
    """The `_sized(32, 24)` raster as a COG with `size` x `size` blocks."""
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import analysis

    with MemoryFile(_sized(32, 24)["raster"]) as mf, mf.open() as ds:
        return {
            **_sized(32, 24),
            "raster": analysis.cog_convert(ds, "AUTO", size, "AVERAGE"),
        }


def _compressed(codec):
    """The `_sized(32, 24)` raster as a COG written with `codec`."""
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core import analysis

    with MemoryFile(_sized(32, 24)["raster"]) as mf, mf.open() as ds:
        return {
            **_sized(32, 24),
            "raster": analysis.cog_convert(ds, codec, 32, "AVERAGE"),
        }


# the arguments a tile_meta key that compares the result with them reads (the other probes read none)
_META_ARGS = {
    "pixels_of_arg0": (_sized(32, 24),),
    "grid_of_arg1": (_sized(32, 24), _sized(32, 24)),
}


@pytest.mark.parametrize(
    "kind,expect,good,bad",
    [
        (
            "geom_equals",
            "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
            "POLYGON((1 0, 1 1, 0 1, 0 0, 1 0))",  # the same ring, another start vertex
            "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))",
        ),
        ("cell_set", ["TQ3080", "TQ3180"], ["TQ3180", "TQ3080"], ["TQ3080", "TQ3080"]),
        ("approx", (1.0, 2.0), (1.0 + 1e-12, 2.0), (1.0, 2.1)),
        ("tile_size", (32, 24), _sized(32, 24), _sized(31, 24)),
        (
            "tile_meta",
            {"width": 32, "height": 24, "dtype": "float32", "no_crs": False},
            _sized(32, 24),
            _sized(31, 24),
        ),
        ("tile_meta", {"width": (31, 32)}, _sized(32, 24), _sized(33, 24)),
        ("tile_meta", {"pixels_of_arg0": True}, _sized(32, 24), _sized(32, 24, 1.0)),
        ("tile_meta", {"grid_of_arg1": True}, _sized(32, 24), _sized(31, 24)),
        ("tile_meta", {"nodata": None}, _sized(32, 24), _sized(32, 24, nodata=-9999.0)),
        (
            "tile_meta",
            {"materialized": True},
            _sized(32, 24),
            {
                **_sized(32, 24),
                "path": "/tmp/x.tif",
            },  # bytes and a path: not a materialized tile
        ),
        ("tile_meta", {"overviews": [2]}, _with_overviews([2]), _with_overviews([])),
        ("tile_meta", {"blocksize": (16, 16)}, _blocked(16), _blocked(32)),
        (
            "tile_meta",
            {"compression": "ZSTD"},
            _compressed("ZSTD"),
            _compressed("DEFLATE"),
        ),
        (
            "image_meta",
            {"driver": "PNG", "width": 4, "height": 4, "count": 4, "alpha": "opaque"},
            _png(255),
            _png(0),
        ),
        (
            "image_meta",
            {"width": 4, "alpha": "transparent"},
            _png(0),
            _png(255),
        ),
        ("image_meta", {"gray": (100, 104)}, _png(102), _png(50)),
        (
            "json_subset",
            {"size": [4, 3], "bands": [{"min": 2.0}]},
            '{"size": [4, 3], "extra": 1, "bands": [{"min": 2.0, "max": 12.0}]}',
            '{"size": [4, 3], "bands": [{"min": 3.0}]}',
        ),
        (
            "geom_srid",
            ("POINT Z(1 2 3)", 4326),
            _ewkb("POINT Z(1 2 3)", 4326),
            _ewkb("POINT Z(1 2 3)", 3857),
        ),
    ],
)
def test_a_new_check_kind_passes_its_value_and_fails_another(kind, expect, good, bad):
    from databricks.labs.gbx.bench import oracles

    args = next(
        (a for k, a in _META_ARGS.items() if isinstance(expect, dict) and k in expect),
        ("TQ3080",),
    )
    case = oracles.Case("probe", "gbx_bng_aswkt", args, kind, expect)
    assert oracles.check(case, good, case.args) is None
    assert oracles.check(case, bad, case.args) is not None
    assert oracles.check(case, None, case.args) is not None


def test_geom_srid_without_a_fixed_srid_follows_the_first_argument():
    from databricks.labs.gbx.bench import oracles

    case = oracles.Case(
        "probe", "gbx_st_node", ("SRID=4326;POINT(1 2)",), "geom_srid", "POINT(1 2)"
    )
    assert oracles.check(case, _ewkb("POINT(1 2)", 4326), case.args) is None
    assert oracles.check(case, _ewkb("POINT(1 2)", 3857), case.args) is not None
    assert oracles.check(case, _ewkb("POINT Z(1 2 3)", 4326), case.args) is not None


def test_a_reference_of_several_bands_holds_every_band_in_order():
    """`ref` with a 3-D reference (frombands, colour relief) compares each band, and the band count."""
    import numpy as np

    from databricks.labs.gbx.bench import oracles

    def two_bands(first, second):
        bands = np.stack([np.full((6, 8), first), np.full((6, 8), second)])
        return oracles._tile(oracles._gtiff(bands, "EPSG:3857"))

    a, b = _sized(8, 6, 1.0), _sized(8, 6, 2.0)
    case = oracles.Case("probe", "gbx_rst_frombands", ([a, b],), "ref", "frombands")
    assert oracles.check(case, two_bands(1.0, 2.0), case.args) is None
    assert (
        oracles.check(case, two_bands(2.0, 1.0), case.args) is not None
    )  # wrong band order
    assert (
        oracles.check(case, a, case.args) is not None
    )  # one band, the reference has two


def test_a_pmtiles_archive_has_exactly_its_tiles_metadata_and_type():
    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.pmtiles._agg_light import _assemble_archive

    png = b"\x89PNG\r\n\x1a\ntile"
    expect = {"tile_type": "png", "metadata": {"a": 1}, "tiles": {(0, 0, 0): png}}
    case = oracles.Case("probe", "gbx_pmtiles_agg", (), "pmtiles_archive", expect)
    good = _assemble_archive([png], [0], [0], [0], {"a": 1})
    assert oracles.check(case, good, ()) is None
    assert (
        oracles.check(case, _assemble_archive([png], [0], [0], [0], {"a": 2}), ())
        is not None
    )
    assert (
        oracles.check(
            case, _assemble_archive([png, png], [0, 1], [0, 0], [0, 0], {"a": 1}), ()
        )
        is not None
    )
    assert oracles.check(case, None, ()) is not None


def test_the_tiles_of_a_pyramid_are_exactly_the_expected_ones_as_png_images():
    from databricks.labs.gbx.bench import oracles

    img = _png(255, 8)
    case = oracles.Case(
        "probe",
        "gbx_rst_xyzpyramid",
        (),
        "xyz_tiles",
        {"tiles": [(8, 127, 84), (8, 127, 85)], "size": 8},
    )
    row = lambda z, x, y, b=img: {"z": z, "x": x, "y": y, "bytes": b}  # noqa: E731
    assert oracles._check_rows(case, [row(8, 127, 85), row(8, 127, 84)], ()) is None
    assert (
        oracles._check_rows(case, [row(8, 127, 84)], ()) is not None
    )  # a tile missing
    assert (
        oracles._check_rows(case, [row(8, 127, 84), row(8, 127, 84)], ()) is not None
    )  # one twice
    assert (
        oracles._check_rows(case, [row(8, 127, 84), row(8, 127, 85, _png(255, 4))], ())
        is not None
    )  # size


def test_the_grid_and_vector_generator_kinds_can_fail():
    import mapbox_vector_tile as mvt
    from shapely import wkb
    from shapely.geometry import Point, Polygon

    from databricks.labs.gbx.bench import oracles

    def judge(kind, expect, rows):
        return oracles._check_rows(
            oracles.Case("probe", "f", (), kind, expect), rows, ()
        )

    big = 5233961835417305087  # past 2**53: a float would round it
    cell = lambda c: {"cellid": c}  # noqa: E731
    assert judge("cell_rows", [big, 1], [cell(1), cell(big)]) is None
    assert judge("cell_rows", [big, 1], [cell(1)]) is not None  # a cell missing
    assert (
        judge("cell_rows", [big, 1], [cell(1), cell(1), cell(big)]) is not None
    )  # twice
    assert judge("cell_rows", [big], [cell(float(big))]) is not None  # through a float

    tri = lambda *v: {"triangle": wkb.dumps(Polygon(v))}  # noqa: E731
    t = [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]
    assert judge("triangles", [t], [tri(*t[::-1])]) is None  # vertex order aside
    assert (
        judge("triangles", [t], [tri((0.0, 0.0), (2.0, 0.0), (0.0, 1.0))]) is not None
    )
    assert judge("triangles", [t], []) is not None

    pt = lambda x, y, z: {"elevation_point": wkb.dumps(Point(x, y, z))}  # noqa: E731
    assert judge("elev_points", [(1.0, 2.0, 3.0)], [pt(1.0, 2.0, 3.0)]) is None
    assert judge("elev_points", [(1.0, 2.0, 3.0)], [pt(1.0, 2.0, 3.5)]) is not None
    assert judge("elev_points", [(1.0, 2.0, 3.0)], [pt(1.0, 2.0, 3.0)] * 2) is not None

    blob = mvt.encode(
        {"name": "l", "features": [{"geometry": "POINT(1 1)", "properties": {"a": 1}}]}
    )
    row = lambda z, b=blob: {"z": z, "x": 0, "y": 0, "mvt_bytes": b}  # noqa: E731
    want = {"tiles": [(0, 0, 0)], "layer": "l", "props": {"a": 1}}
    assert judge("mvt_tiles", want, [row(0)]) is None
    assert judge("mvt_tiles", {**want, "layer": "x"}, [row(0)]) is not None
    assert judge("mvt_tiles", {**want, "props": {"a": 2}}, [row(0)]) is not None
    assert judge("mvt_tiles", want, [row(1)]) is not None


def test_the_raster_generator_and_vectorize_kinds_can_fail():
    import numpy as np
    from shapely import wkb
    from shapely.geometry import LineString, box

    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.bench.oracle_tables.grid_scalar import cg_id

    def tile(values, origin=(0.0, 2000.0), px=1000.0):
        return oracles._gtiff(
            np.array(values, dtype="float64")[np.newaxis],
            "EPSG:27700",
            nodata=None,
            origin=origin,
            px=px,
        )

    def judge(kind, expect, rows, args=()):
        case = oracles.Case("probe", "f", args, kind, expect)
        return oracles._check_rows(case, rows, args)

    # tile_rows: the first band of each row, in order
    r = lambda v: {"raster": tile(v), "cellid": 0, "metadata": {}}  # noqa: E731
    ok = {"pixels": [[[1.0, 2.0]], [[3.0, 4.0]]]}
    assert judge("tile_rows", ok, [r([[1, 2]]), r([[3, 4]])]) is None
    assert judge("tile_rows", ok, [r([[3, 4]]), r([[1, 2]])]) is not None  # order
    assert judge("tile_rows", ok, [r([[1, 2]])]) is not None  # a row missing
    assert judge("tile_rows", ok, [r([[1, 2]]), r([[3, 5]])]) is not None  # a pixel

    # tessellation: a 2 x 2 raster of 1 km pixels at (0, 2000) is four custom-grid cells of resolution 0
    source = {"raster": tile([[1, 2], [3, 4]])}
    chip = lambda v, cell: {  # noqa: E731
        "cellid": cell,
        "raster": tile([[v]], origin=(0.0, 2000.0), px=1000.0),
    }
    cells = {(0, 1): 1.0, (1, 1): 2.0, (0, 0): 3.0, (1, 0): 4.0}
    rows = [chip(v, cg_id(c, rw, 0)) for (c, rw), v in cells.items()]
    want = {"grid": "custom", "res": 0}
    assert judge("tessellation", want, rows, (source,)) is None
    assert (
        judge("tessellation", want, rows[1:], (source,)) is not None
    )  # a cell missing
    bad = rows[:3] + [chip(9.0, cg_id(1, 0, 0))]
    assert judge("tessellation", want, bad, (source,)) is not None  # a sum
    assert (
        judge("tessellation", want, rows + [chip(0.0, cg_id(7, 7, 0))], (source,))
        is not None
    )  # an extra cell

    # polygons: value and area per row
    poly = lambda v, g: {"geom_wkb": wkb.dumps(g), "value": v}  # noqa: E731
    assert judge("polygons", [(1.0, 4.0)], [poly(1.0, box(0, 0, 2, 2))]) is None
    assert judge("polygons", [(1.0, 4.0)], [poly(1.0, box(0, 0, 2, 3))]) is not None
    assert judge("polygons", [(1.0, 4.0)], [poly(2.0, box(0, 0, 2, 2))]) is not None

    # isobands and contours judge the array a scalar returns
    iso = lambda b, a: {  # noqa: E731
        "geom_wkb": wkb.dumps(box(0, 0, a, 1)),
        "band": b,
        "lower": 0.0,
        "upper": 1.0,
    }
    case = oracles.Case("probe", "f", (), "isobands", [(0, 0.0, 1.0, 3.0)])
    assert oracles.check(case, [iso(0, 3.0)], ()) is None
    assert oracles.check(case, [iso(0, 4.0)], ()) is not None
    assert oracles.check(case, None, ()) is not None

    def line(v, x):
        return {"geom_wkb": wkb.dumps(LineString([(x, 0), (x, 5)])), "value": v}

    case = oracles.Case("probe", "f", (), "contours", {1.5: 20.0})
    assert oracles.check(case, [line(1.5, 20.0)], ()) is None
    assert (
        oracles.check(case, [line(1.5, 20.00001)], ()) is None
    )  # GDAL moves a line off a pixel value by 1e-5
    assert oracles.check(case, [line(1.5, 21.0)], ()) is not None
    assert oracles.check(case, [line(2.5, 20.0)], ()) is not None


def test_the_grid_stats_kind_can_fail():
    import numpy as np

    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.bench.oracle_tables.grid_scalar import cg_id

    # a 2 x 2 raster of 1 km pixels at (0, 2000) is four custom-grid cells of resolution 0, with one NoData pixel
    raster = oracles._gtiff(
        np.array([[[1.0, 2.0], [3.0, -9999.0]]]),
        "EPSG:27700",
        origin=(0.0, 2000.0),
        px=1000.0,
    )
    cells = {cg_id(0, 1, 0): 1.0, cg_id(1, 1, 0): 2.0, cg_id(0, 0, 0): 3.0}

    def judge(rows, **extra):
        expect = {"grid": "custom", "res": 0, "stat": "sum", **extra}
        case = oracles.Case("probe", "f", (), "grid_stats", expect)
        return oracles._check_rows(case, rows, ({"raster": raster},))

    def row(cell, measure, band=1):
        return {"band": band, "cellID": cell, "measure": measure}

    rows = [row(c, m) for c, m in cells.items()]
    empty = row(cg_id(1, 0, 0), None)
    assert (
        judge(rows + [empty]) is None
    )  # complete: the cell with only NoData may come out empty
    assert judge(rows + [empty], coverage="sparse") is not None  # sparse: it may not
    assert judge(rows[1:]) is not None  # a cell missing
    assert judge([row(rows[0]["cellID"], 9.0)] + rows[1:]) is not None  # a value
    assert (
        judge(rows + [row(cg_id(1, 0, 0), 5.0)]) is not None
    )  # an empty cell with a value
    assert (
        judge([row(float(c), m) for c, m in cells.items()]) is not None
    )  # an id through a float
    assert judge(rows, past_2_53=True) is not None  # no id there is past 2**53

    # covering (count): the shares add up to the 3 valid pixels, each centre cell has a share, and no cell is far off
    cover = {"stat": "count", "assignment": "covering"}
    c0, c1, c2 = cells
    far = cg_id(5, 5, 0)
    assert judge([row(c, 1.0) for c in cells], **cover) is None
    assert (
        judge([row(c0, 1.0), row(c1, 1.0), row(c2, 0.5)], **cover) is not None
    )  # adds up to 2.5
    assert (
        judge([row(c, 1.0, band=2) for c in cells], **cover) is not None
    )  # band 1 has none
    assert (
        judge([row(c0, 2.0), row(c1, 1.0)], **cover) is not None
    )  # a centre cell with no share
    assert (
        judge([row(c0, 1.0), row(c1, 1.0), row(c2, 0.5), row(far, 0.5)], **cover)
        is not None
    )  # a cell no valid pixel is near
    assert (
        judge([row(c, 1.0) for c in cells], stat="sum", assignment="covering")
        is not None
    )  # not count


def test_the_bng_oracle_names_the_100km_square_and_refuses_quadrants():
    from databricks.labs.gbx.bench import oracle_checks

    # TQ is the square holding (530000, 180000) (central London); at res 1 the id is the two letters alone
    assert oracle_checks._bng_square(530000.0, 180000.0, 100000) == "TQ"
    assert oracle_checks._bng_square(530000.0, 180000.0, 10000) == "TQ38"
    assert oracle_checks._grid_keys("bng", 1, [530000.0], [180000.0]) == ["TQ"]
    with pytest.raises(ValueError, match="unsupported in the oracle"):
        oracle_checks._grid_keys("bng", -3, [530000.0], [180000.0])


def test_the_cellfill_and_mvt_layer_kinds_can_fail():
    import shapely

    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.bench.oracle_tables.vector_aggregate import POINT, POLY
    from databricks.labs.gbx.pyvx import _mvt

    # a fill payload: two cells, the first valid (2.5), the second empty
    payload = (
        (2).to_bytes(4, "big")
        + (10).to_bytes(8, "big", signed=True)
        + b"\x01"
        + __import__("struct").pack(">d", 2.5)
        + (11).to_bytes(8, "big", signed=True)
        + b"\x00"
    )

    def fill(expect):
        return oracles.check(
            oracles.Case("probe", "f", (), "cellfill", expect), payload, ()
        )

    assert fill({10: 2.5, 11: None}) is None
    assert fill({10: 2.6, 11: None}) is not None  # a value
    assert fill({10: 2.5, 11: 1.0}) is not None  # a cell that must be empty
    assert fill({10: 2.5}) is not None  # a cell too many
    assert (
        fill({11: None, 10: 2.5, 12: 0.0}) is not None
    )  # a cell missing from the payload

    blob = _mvt.encode_layer(
        [{"geometry": POLY, "properties": {"n": 3}}], layer_name="lyr"
    )

    def layer(expect):
        return oracles.check(
            oracles.Case("probe", "f", (), "mvt_layer", expect), blob, ()
        )

    assert layer({"layer": "lyr", "features": [(POLY, {"n": 3})]}) is None
    assert (
        layer({"layer": "other", "features": [(POLY, {"n": 3})]}) is not None
    )  # the layer name
    assert (
        layer({"layer": "lyr", "features": [(POLY, {"n": 4})]}) is not None
    )  # an attribute
    assert (
        layer({"layer": "lyr", "features": [(POINT, {"n": 3})]}) is not None
    )  # the geometry
    assert layer({"layer": "lyr", "features": []}) is not None  # a feature too many
    assert shapely  # the module imports with the fixtures it names
