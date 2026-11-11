"""The oracle tables are found on first use of the mapping, by any read, not only by a missing-key lookup."""

import dataclasses
import struct
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
    case = next(c for c in oracles.TABLES[fn.oracle] if c.fn == fn.sql_name)
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
    case = next(c for c in oracles.TABLES[fn.oracle] if c.fn == fn.sql_name)
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
        (
            "wkb_bytes",
            struct.pack("<BIdd", 1, 1, 1.0, 2.0),
            bytearray(struct.pack("<BIdd", 1, 1, 1.0, 2.0)),
            struct.pack(
                ">BIdd", 0, 1, 1.0, 2.0
            ),  # the same point, big-endian: a byte-level difference
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


def test_a_list_literal_takes_the_element_type_of_its_parameter_and_ewkt_is_parsed_as_ewkt():
    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.bench import oracles

    man = manifest.load()

    def literal(fn, index, value):
        case = oracles.Case("probe", fn, (), "null", None, geom_srid=3857)
        return oracles._sql_literal(value, man.functions[fn].params[index], case)

    # ARRAY<BINARY>: bytes are hex literals, a NULL element and an empty array are BINARY
    binary = dataclasses.replace(
        man.functions["gbx_rst_gridfrompoints"].params[0], uc="ARRAY<BINARY>"
    )
    case = oracles.Case("probe", "gbx_rst_gridfrompoints", (), "null", None)
    assert oracles._sql_literal([b"\x01\xab", None], binary, case) == (
        "array(X'01ab', CAST(NULL AS BINARY))"
    )
    assert oracles._sql_literal([], binary, case) == "CAST(array() AS ARRAY<BINARY>)"
    # ARRAY<GEOMETRY(ANY)>: a point of WKB is built with st_geomfromwkb of ISO bytes; a NULL element is a NULL geometry
    assert literal("gbx_rst_gridfrompoints", 0, [None]) == (
        "array(CAST(NULL AS GEOMETRY(ANY)))"
    )
    assert literal("gbx_rst_gridfrompoints", 0, []) == (
        "CAST(array() AS ARRAY<GEOMETRY(ANY)>)"
    )
    # ARRAY<DOUBLE>
    assert literal("gbx_rst_isoband", 1, [0.0, None, 2.5]) == (
        "array(0.0, CAST(NULL AS DOUBLE), 2.5)"
    )
    assert literal("gbx_rst_isoband", 1, []) == "CAST(array() AS ARRAY<DOUBLE>)"
    # ARRAY<TILE>
    tiles = literal("gbx_rst_combineavg", 0, [None])
    assert tiles.startswith("array(CAST(NULL AS ") and "raster" in tiles
    # EWKT carries its SRID: it goes to st_geomfromewkt, plain WKT keeps the case SRID
    assert literal("gbx_rst_rasterize", 0, "SRID=4326;POINT(1 2)") == (
        "CAST(st_geomfromewkt('SRID=4326;POINT(1 2)') AS GEOMETRY(ANY))"
    )
    assert literal("gbx_rst_rasterize", 0, "POINT(1 2)") == (
        "CAST(st_geomfromwkt('POINT(1 2)', 3857) AS GEOMETRY(ANY))"
    )
    # an EWKT is recognised whatever its case or leading whitespace, and a quote in it is doubled
    for ewkt in (
        "srid=4326;POINT(1 2)",
        "  SRID=4326;POINT(1 2)",
        "\n\tSrid=4326;POINT(1 2)",
    ):
        assert literal("gbx_rst_rasterize", 0, ewkt) == (
            f"CAST(st_geomfromewkt('{ewkt}') AS GEOMETRY(ANY))"
        )
    assert literal("gbx_rst_rasterize", 0, "SRID=4326;POINT(1 2)'; --") == (
        "CAST(st_geomfromewkt('SRID=4326;POINT(1 2)''; --') AS GEOMETRY(ANY))"
    )


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
    t = [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]  # counter-clockwise from its lowest vertex
    assert judge("triangles", [t[::-1]], [tri(*t)]) is None  # the expected order aside
    # every level writes a ring one way: a clockwise ring, or one from another vertex, fails
    assert judge("triangles", [t], [tri(*t[::-1])]) is not None
    assert judge("triangles", [t], [tri(*t[1:], t[0])]) is not None
    assert (
        judge("triangles", [t], [tri((0.0, 0.0), (2.0, 0.0), (0.0, 1.0))]) is not None
    )
    assert judge("triangles", [t], []) is not None
    # triangles_exact takes each X and Y as it is: an error of 1e-12, within the 1e-9 of "triangles", fails
    near = tri((0.0, 0.0), (1.0 + 1e-12, 0.0), (0.0, 1.0))
    assert judge("triangles", [t], [near]) is None
    assert judge("triangles_exact", [t[::-1]], [tri(*t)]) is None
    assert judge("triangles_exact", [t], [near]) is not None
    assert judge("triangles_exact", [t], [tri(*t), tri(*t)]) is not None  # twice

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


def test_a_table_function_is_called_with_a_partition_clause_and_a_wrong_row_fails(
    spark, tmp_path
):
    """The one harness mode for a function that reads `TABLE(...) PARTITION BY`: the SQL it builds, a row that
    meets its expectation, and the same row with another expectation (which must be red).
    """
    import dataclasses

    from databricks.labs.gbx import manifest, register
    from databricks.labs.gbx.bench import oracles

    fn = manifest.load().functions["gbx_rst_bng_rasterize_tvf"]
    assert oracles.table_call_sql(fn) == (
        "SELECT key, raster AS out FROM gbx_rst_bng_rasterize_tvf("
        "TABLE(SELECT key, c0 AS cellid, c1 AS value FROM {src}) PARTITION BY (key){args})"
    )
    register(spark, only=[fn.sql_name])
    (case,) = [
        c
        for c in oracles.TABLES[fn.oracle]
        if c.fn == fn.sql_name and c.cid == "bt_dup_max"
    ]
    assert [m for _, m in oracles.run_groups(spark, [case], tmpdir=str(tmp_path))] == [
        None
    ]
    wrong = dataclasses.replace(case, expect=[[0.0] * 3, [0.0] * 3])
    ((_, msg),) = oracles.run_groups(spark, [wrong], tmpdir=str(tmp_path))
    assert msg and "pixels" in msg


def test_a_table_function_harness_leaves_out_omitted_members_and_takes_several_keys():
    import dataclasses

    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.bench import oracles

    fn = manifest.load().functions["gbx_rst_bng_rasterize_tvf"]
    left_out = oracles.table_call_sql(fn, omitted={1})
    assert "c1" not in left_out and "c0 AS cellid" in left_out
    two = dataclasses.replace(
        fn,
        table_input=dataclasses.replace(
            fn.table_input,
            columns=(("k1", "STRING"), ("k2", "STRING"), *fn.table_input.columns[1:]),
            partition_by=("k1", "k2"),
        ),
    )
    sql = oracles.table_call_sql(two)
    assert "key AS k1, key AS k2" in sql and "PARTITION BY (k1, k2)" in sql
    assert sql.startswith("SELECT k1 AS key, raster AS out FROM")


def test_a_table_function_shape_may_name_the_function_and_a_fixture_constant_is_a_harness_error(
    spark, tmp_path
):
    import dataclasses

    from databricks.labs.gbx import manifest, register
    from databricks.labs.gbx.bench import oracles

    fn = manifest.load().functions["gbx_rst_bng_rasterize_tvf"]
    register(spark, only=[fn.sql_name])
    (case,) = [c for c in oracles.TABLES[fn.oracle] if c.cid == "bt_dup_max"]
    shape = (
        "SELECT key, raster AS out FROM {fn}(TABLE(SELECT key, c0 AS cellid, c1 AS value "
        "FROM {src}) PARTITION BY (key){args})"
    )
    assert [m for _, m in oracles.run_groups(spark, [case], shape, str(tmp_path))] == [
        None
    ]
    fixture = dataclasses.replace(
        case, args=(*case.args[:2], oracles.Build("tile_32"), *case.args[3:])
    )
    with pytest.raises(TypeError, match="Python leg cannot pass"):
        oracles.run_groups(
            spark, [fixture], tmpdir=str(tmp_path), column_of=lambda name: None
        )


def test_a_str_only_argument_that_differs_between_the_rows_fails_the_python_leg(
    spark, tmp_path
):
    """The Python API takes a str_only argument as one Python str, so a case whose variant rows give it two values
    cannot run through it: the leg reports the row, it does not call the function."""
    from databricks.labs.gbx.bench import oracles

    src = "def f(in_ar, out_ar, *a):\n    out_ar[:] = in_ar[0]\n"
    case = oracles.Case(
        "probe_str_only",
        "gbx_rst_derivedband",
        (None, src, "f"),
        "null",
        variants=((None, src.replace("in_ar[0]", "in_ar[0] + 1"), "f"),),
    )

    def column_of(name):
        raise AssertionError("the Python API must not be called")

    ((_, msg),) = oracles.run_session(
        spark, [case], tmpdir=str(tmp_path), column_of=column_of
    )
    assert "python_func of gbx_rst_derivedband differs between the rows" in msg
    assert "Fix: give the variant rows one value for it" in msg


def test_the_slope_reference_takes_gdals_default_scale_for_the_crs():
    """``oracles._gdaldem_scale``: about 111 km a degree for a geographic DEM (x shrunk by the cosine of the mean
    latitude), the metres in a unit for a projected one, 1 for none; each divided by the heights' unit in metres.
    """
    import math

    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    from databricks.labs.gbx.bench import oracles

    def scale(crs, units=None):
        profile = dict(driver="GTiff", width=4, height=10, count=1, dtype="float32", crs=crs,
                       transform=from_origin(-73.99, 40.75, 0.0001, 0.0001))  # fmt: skip
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(np.zeros((1, 10, 4), "float32"))
                if units:
                    dst.units = (units,)
            with mf.open() as ds:
                return oracles._gdaldem_scale(ds)

    y = math.pi / 180.0 * 6378137.0
    mean_lat = math.radians(40.75 - 10 * 0.0001 / 2)
    gx, gy = scale("EPSG:4326")
    assert gy == pytest.approx(y, rel=1e-9) and gx == pytest.approx(
        y * math.cos(mean_lat), rel=1e-9
    )
    assert scale("EPSG:3857") == pytest.approx((1.0, 1.0))
    assert scale(None) == (1.0, 1.0)
    assert scale("EPSG:3857", "ft") == pytest.approx((1 / 0.3048, 1 / 0.3048))


def test_a_contour_reference_row_takes_the_exact_rule_only():
    """The scikit-image 'equivalent' rule is gone: a row that still names it is refused before it is judged."""
    from databricks.labs.gbx.bench import oracle_checks
    from databricks.labs.gbx.bench.oracles import Build, Case

    case = Case(
        "stale",
        "gbx_rst_contour",
        (Build("dem_3857"), [110.0]),
        "features_ref",
        "equivalent",
    )
    with pytest.raises(ValueError, match="takes the rule 'exact'"):
        oracle_checks.check_features_ref(case, [], case.args)


def test_an_ewkt_geometry_column_is_read_with_st_geomfromewkt_on_uc():
    """The session runner on UC reads an EWKT argument as the statement renderer does: st_geomfromwkt refuses the
    `SRID=<n>;` prefix (WKT_PARSE_ERROR on Serverless env 6), st_geomfromewkt reads it and keeps its SRID.
    """
    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.bench import oracles

    params = manifest.load().functions["gbx_rst_viewshed"].params
    case = oracles.Case("probe", "gbx_rst_viewshed", (), "null", None, geom_srid=3857)

    def proj(*observers):
        return oracles._projection(case, params, [(None, o) for o in observers], "uc")

    assert proj("SRID=4326;POINT(1 2)").split(", ")[1] == (
        f"CAST(st_geomfromewkt(c1) AS {oracles.UC_GEOMETRY})"
    )
    assert (
        proj(" srid=4326;POINT(1 2)", None)
        .split(", ")[1]
        .startswith("CAST(st_geomfromewkt(c1)")
    )
    assert (
        proj("POINT(1 2)")
        == f"c0, CAST(st_geomfromwkt(c1, 3857) AS {oracles.UC_GEOMETRY})"
    )
    with pytest.raises(ValueError, match="EWKT in some rows and WKT in others"):
        proj("SRID=4326;POINT(1 2)", "POINT(1 2)")
    # the session surface reads the column as it is
    assert (
        oracles._projection(case, params, [(None, "SRID=4326;POINT(1 2)")], "session")
        == "c0, c1"
    )


def test_a_uc_row_mirrored_from_a_named_session_row_carries_its_geometrys_srid():
    """A row named in ``null``/``refused`` builds its UC GEOMETRY as a promoted row does: the SRID of its EWKT, else 0
    (the session reads a plain WKT in the tile's CRS), never the 4326 default; one UC cannot be handed raises.
    """
    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.bench.oracle_tables import spec_analysis

    uc = {c.cid: c for c in spec_analysis.SPEC_ANALYSIS}
    # a UTM point: read as lon/lat its NULL would come from the wrong input
    assert uc["uc_viewshed_outside_dem_out_of_reach"].geom_srid == 0
    vs = "gbx_rst_viewshed"
    ewkt = oracles.Case(
        "vs_ewkt", vs, (b"II*\x00", "SRID=27700;POINT(1 2)", 2.0), "error_tile_has", "x"
    )
    got = {c.cid: c for c in oracles.mirror((ewkt,), null=("vs_ewkt",))}
    assert got["uc_vs_ewkt"].geom_srid == 27700 and got["uc_vs_ewkt"].kind == "null"
    wkb = dataclasses.replace(
        ewkt, cid="vs_wkb", args=(b"II*\x00", b"\x01\x01\x00\x00\x00" + bytes(16), 2.0)
    )
    with pytest.raises(AssertionError, match="vs_wkb"):
        oracles.mirror((wkb,), refused=("vs_wkb",))


# --- Batching: the cases of a call in one query (oracles._run_batched) --------------------------------------------------
def _batched(cases, red_in_batch=(), batch_raises=False, key=lambda c: c.kind):
    """`_run_batched` over fake cases: the batch answers each case its cid, a case in `red_in_batch` fails its judge
    there, and the alone run answers 'alone:<cid>'."""
    from databricks.labs.gbx.bench import oracles

    calls = {"batches": [], "alone": []}

    def prepare(case):
        return None if case.kind == "raises" else (key(case), case.cid)

    def run_batch(groups):
        calls["batches"].append([[p for _, p in g] for g in groups])
        if batch_raises:
            raise RuntimeError("the batch failed")
        return {k: [p] for g in groups for k, p in g}

    def judge(case, payload, outs):
        return f"{case.cid}: red" if case.cid in red_in_batch else None

    def run_alone(case):
        calls["alone"].append(case.cid)
        return [(case, f"alone:{case.cid}")]

    return oracles._run_batched(cases, prepare, run_batch, judge, run_alone), calls


def _fake(cid, kind="exact"):
    from databricks.labs.gbx.bench import oracles

    return oracles.Case(cid, "gbx_x", (), kind)


def test_a_batch_is_one_query_for_every_group_and_a_raising_case_runs_alone():
    cases = [_fake("a"), _fake("b", "null"), _fake("c", "raises"), _fake("d")]
    ran, calls = _batched(cases)
    assert calls["batches"] == [[["a", "d"], ["b"]]]  # one query, a group per key
    assert calls["alone"] == ["c"]
    assert [(c.cid, m) for c, m in ran] == [
        ("a", None),
        ("b", None),
        ("c", "alone:c"),
        ("d", None),
    ]


def test_a_case_red_in_its_batch_and_every_case_of_a_failed_batch_is_judged_alone():
    cases = [_fake("a"), _fake("b"), _fake("c")]
    ran, calls = _batched(cases, red_in_batch={"b"})
    assert calls["alone"] == ["b"] and [m for _, m in ran] == [None, "alone:b", None]
    ran, calls = _batched(cases, batch_raises=True)
    assert calls["alone"] == ["a", "b", "c"]
    assert [m for _, m in ran] == ["alone:a", "alone:b", "alone:c"]


def test_gbx_oracle_batch_0_turns_batching_off(monkeypatch):
    from databricks.labs.gbx.bench import oracles

    monkeypatch.delenv(oracles.BATCH_ENV, raising=False)
    assert oracles.batching()
    monkeypatch.setenv(oracles.BATCH_ENV, "0")
    assert not oracles.batching()


def test_a_batched_session_run_gives_each_case_the_verdict_it_has_alone(
    spark, tmp_path
):
    """One function's rows, with a wrong value and a missing refusal among them, batched and alone, through SQL and
    the Python API: the same verdict per case, and each red row named."""
    import dataclasses

    from databricks.labs.gbx import manifest, register
    from databricks.labs.gbx.bench import oracles

    name = "gbx_bng_cellarea"
    register(spark, only=[name], _levels={name: ("L3", "c0")})
    cases = [
        c
        for c in oracles.TABLES["GRID_SCALAR"]
        if c.fn == name and "session" in c.surfaces
    ][:6]
    wrong = dataclasses.replace(
        cases[0], cid="cellarea_wrong", kind="exact", expect=-1.0
    )
    refused = dataclasses.replace(
        cases[0], cid="cellarea_no_raise", kind="raises", expect="nope"
    )
    cases += [wrong, refused]
    py = manifest.resolve_ref(manifest.load().functions[name].python.ref)
    for column_of in (None, lambda f: py):
        got = {
            batch: [
                (c.cid, m is None)
                for c, m in oracles.run_session(
                    spark, cases, tmpdir=str(tmp_path), column_of=column_of, batch=batch
                )
            ]
            for batch in (True, False)
        }
        assert got[True] == got[False]
        assert dict(got[True])["cellarea_wrong"] is False
        assert dict(got[True])["cellarea_no_raise"] is False
        assert sum(ok for _, ok in got[True]) == 6, got[True]


def test_the_triangle_oracle_judges_ring_order_and_lets_a_degenerate_ring_through():
    """A ring that runs clockwise, or starts past its lowest vertex, is the wrong ring order. A zero-area ring has no
    orientation: the levels write it from its lowest vertex and in its input order, so only that part is judged.
    """
    from shapely import wkb
    from shapely.geometry import Polygon

    from databricks.labs.gbx.bench import oracles

    def judge(ring, expect=None):
        rows = [{"triangle": wkb.dumps(Polygon(ring))}]
        case = oracles.Case("probe", "f", (), "triangles", [expect or ring])
        return oracles._check_vector_rows(case, rows)

    ccw = [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]
    order = "does not run counter-clockwise from its lowest vertex"
    assert judge(ccw) is None
    assert order in judge([(0.0, 0.0), (0.0, 1.0), (1.0, 0.0)], ccw)  # clockwise
    assert order in judge([(1.0, 0.0), (0.0, 1.0), (0.0, 0.0)], ccw)  # rotated
    flat = [(0.0, 0.0), (2.0, 0.0), (1.0, 0.0)]  # collinear, from its lowest vertex
    assert judge(flat) is None
    assert order in judge(
        [(2.0, 0.0), (1.0, 0.0), (0.0, 0.0)], flat
    )  # not from its lowest vertex
    assert "want" in judge(
        flat, [(0.0, 0.0), (3.0, 0.0), (1.0, 0.0)]
    )  # other vertices: still red


class _SqlSpy:
    """The session, recording each SQL text it runs (to see the batch's one UNION ALL)."""

    def __init__(self, spark):
        self._spark, self.texts = spark, []

    def sql(self, text, *a, **k):
        self.texts.append(text)
        return self._spark.sql(text, *a, **k)

    def __getattr__(self, name):
        return getattr(self._spark, name)


def _batched_vs_alone(monkeypatch, runner, spark, cases, **kw):
    """``runner`` batched and alone on ``cases``: ({batch: [(cid, green)]}, the cids the batch ran alone, the batch
    queries' SQL texts)."""
    from databricks.labs.gbx.bench import oracles

    real, alone = getattr(oracles, runner), []

    def spy(s, cs, *a, **k):
        if not k.get("batch"):
            alone.extend(c.cid for c in cs)
        return real(s, cs, *a, **k)

    monkeypatch.setattr(oracles, runner, spy)
    sql = _SqlSpy(spark)
    got = {}
    for batch in (True, False):
        alone.clear()
        got[batch] = [
            (c.cid, m is None) for c, m in real(sql, cases, batch=batch, **kw)
        ]
        if batch:
            ran_alone = list(alone)
    return got, ran_alone, [t for t in sql.texts if "_gbx_oracle_b" in t]


def test_a_batched_generator_run_gives_each_case_its_alone_verdict_and_reruns_only_red_ones(
    spark, tmp_path, monkeypatch
):
    """Two generators in one UNION ALL (an answer column per function), through SQL and the DataFrame-first API: the
    verdicts match the alone run, and only the case red in the batch (plus the refusals, which never batch) runs alone.
    """
    import dataclasses

    from databricks.labs.gbx import manifest, register
    from databricks.labs.gbx.bench import oracles

    names = ("gbx_bng_kringexplode", "gbx_bng_kloopexplode")
    register(spark, only=list(names), _levels={n: ("L3", "-") for n in names})
    cases = [
        c
        for c in oracles.TABLES["GRID_GENERATOR"]
        if c.fn in names and "session" in c.surfaces
    ]
    rows = next(c for c in cases if c.kind == "cell_rows")
    wrong = dataclasses.replace(rows, cid="krings_wrong", expect=[1])
    cases.append(wrong)
    refusals = {c.cid for c in cases if c.kind in oracles._ALONE_KINDS}
    man = manifest.load()
    for table_of in (None, lambda f: manifest.resolve_ref(man.functions[f].python.ref)):
        got, alone, batch_sql = _batched_vs_alone(
            monkeypatch,
            "run_rows",
            spark,
            cases,
            tmpdir=str(tmp_path),
            table_of=table_of,
        )
        assert got[True] == got[False]
        assert dict(got[True])["krings_wrong"] is False
        assert sum(ok for _, ok in got[True]) == len(cases) - 1
        assert set(alone) == refusals | {"krings_wrong"}, alone
        if table_of is None:
            # one query for both functions: a branch each, each with its own answer column
            assert len(batch_sql) == 1 and batch_sql[0].count(" UNION ALL ") == 1
            assert "struct(t.*) AS o0, NULL AS o1" in batch_sql[0]
            assert "NULL AS o0, struct(t.*) AS o1" in batch_sql[0]


def test_a_batched_aggregate_run_groups_by_case_and_reruns_only_red_ones(
    spark, tmp_path, monkeypatch
):
    """Aggregate cases grouped by case in one query (GROUP BY _gbx_case), through SQL and the Column API: the
    verdicts match the alone run, and only the case red in the batch runs alone."""
    import dataclasses

    from databricks.labs.gbx import manifest, register
    from databricks.labs.gbx.bench import oracles

    names = ("gbx_st_coverageisvalid", "gbx_st_coverageinvalidedges")
    register(spark, only=list(names), _levels={n: ("L3", "c0") for n in names})
    cases = [
        c
        for c in oracles.TABLES["VECTOR_AGGREGATE"]
        if c.fn in names and "session" in c.surfaces
    ]
    exact = next(c for c in cases if c.fn == names[0] and c.kind == "exact")
    wrong = dataclasses.replace(exact, cid="coverage_wrong", expect=not exact.expect)
    cases.append(wrong)
    man = manifest.load()
    for column_of in (
        None,
        lambda f: manifest.resolve_ref(man.functions[f].python.ref),
    ):
        got, alone, batch_sql = _batched_vs_alone(
            monkeypatch,
            "run_groups",
            spark,
            cases,
            tmpdir=str(tmp_path),
            column_of=column_of,
        )
        assert got[True] == got[False]
        assert dict(got[True])["coverage_wrong"] is False
        assert sum(ok for _, ok in got[True]) == len(cases) - 1, got[True]
        assert alone == ["coverage_wrong"]
        if column_of is None:
            assert len(batch_sql) == 1 and " UNION ALL " in batch_sql[0]
            assert (
                batch_sql[0].count("GROUP BY _gbx_case")
                == batch_sql[0].count(" UNION ALL ") + 1
            )
