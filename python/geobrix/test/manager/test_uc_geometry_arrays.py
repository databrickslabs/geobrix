"""The point arrays of gbx_rst_dtmfromgeoms and gbx_rst_gridfrompoints on Unity Catalog are ARRAY<GEOMETRY(ANY)>: a UC Python
function receives them as ``Geometry`` values (WKB with the SRID beside it, the Z kept), where the session takes WKB, EWKB,
WKT or EWKT. These tests hold the type, the conversion of each element and the literal the warehouse rows build; the
warehouse itself runs the rows in the next workspace pass.
"""

import pytest
import shapely

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.bench.oracle_tables.raster_rasterize import RASTER_RASTERIZE
from databricks.labs.gbx.uc import ddl, tier_b

MAN = manifest.load()
# (function, the parameters whose UC type is an array of geometries)
FUNCTIONS = [
    ("gbx_rst_dtmfromgeoms", ("points_array", "breaklines_array")),
    ("gbx_rst_gridfrompoints", ("points_array",)),
]


class Geometry:
    """Stand-in for ``pyspark.sql.types.Geometry``: ``getBytes()`` is WKB without the SRID, ``getSrid()`` the SRID."""

    def __init__(self, geom, srid=0, flavor="iso"):
        self._wkb = shapely.to_wkb(geom, flavor=flavor)
        self._srid = srid

    def getBytes(self):  # noqa: N802 - the pyspark name
        return self._wkb

    def getSrid(self):  # noqa: N802
        return self._srid


@pytest.mark.parametrize("name,names", FUNCTIONS)
def test_the_uc_type_is_an_array_of_geometry_and_the_session_type_stays(name, names):
    fn = MAN.functions[name]
    for p in fn.params:
        if p.name in names:
            assert p.uc == "ARRAY<GEOMETRY>", p.name
            assert tuple(p.session) == ("ARRAY<BINARY>", "ARRAY<STRING>"), p.name
            assert ddl.sql_type(p.uc) == "ARRAY<GEOMETRY(ANY)>"
        elif p.name == "values_array":
            assert p.uc == "ARRAY<DOUBLE>"


def test_the_function_ddl_declares_the_arrays_as_array_of_geometry_any():
    stmts = [
        s
        for s in __import__("databricks.labs.gbx", fromlist=["uc"]).uc.install(
            "main", "geobrix", "/Volumes/main/geobrix/artifacts/env6", dry_run=True
        )
        if s.startswith("CREATE OR REPLACE FUNCTION")
    ]
    dtm = next(s for s in stmts if ".gbx_rst_dtmfromgeoms(" in s)
    gfp = next(s for s in stmts if ".gbx_rst_gridfrompoints(" in s)
    assert "points_array ARRAY<GEOMETRY(ANY)>" in dtm
    assert "breaklines_array ARRAY<GEOMETRY(ANY)>" in dtm
    assert "points_array ARRAY<GEOMETRY(ANY)>" in gfp
    assert "values_array ARRAY<DOUBLE>" in gfp


PLANE = [(0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (10.0, 10.0), (5.0, 5.0)]


def _dtm(points, breaklines=None):
    return tier_b.HANDLERS["gbx_rst_dtmfromgeoms"](
        points, breaklines, 0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 5, 5, 3857, -9999.0, None
    )


def _pixels(tile):
    import numpy as np
    from rasterio.io import MemoryFile

    with MemoryFile(tile["raster"]) as mf, mf.open() as ds:
        return np.asarray(ds.read(1))


def test_a_geometry_element_keeps_its_z_and_gives_the_session_terrain():
    """The terrain of Z-valued Geometry points is the plane z = x + 2 y: the Z reached the kernel, ISO type 1001 and all."""
    points = [Geometry(shapely.Point(x, y, x + 2 * y), 3857) for x, y in PLANE]
    px = _pixels(_dtm(points))
    # the pixel centres of a 5 x 5 grid over 0..10 are 1, 3, 5, 7, 9; row 0 is the north edge
    for r, y in enumerate((9.0, 7.0, 5.0, 3.0, 1.0)):
        for c, x in enumerate((1.0, 3.0, 5.0, 7.0, 9.0)):
            assert px[r, c] == pytest.approx(x + 2 * y), (r, c)


def test_a_point_without_z_in_the_array_is_refused_as_before():
    points = [Geometry(shapely.Point(x, y), 3857) for x, y in PLANE]
    with pytest.raises(ValueError, match="Z coordinate"):
        _dtm(points)


def test_a_null_element_is_dropped_with_its_value_and_a_bad_element_names_the_fix():
    pts = [
        Geometry(shapely.Point(x, y), 3857) for x, y in ((1, 1), (8, 2), (2, 9), (9, 8))
    ]
    vals = [10.0, 20.0, 30.0, 40.0]
    gfp = tier_b.HANDLERS["gbx_rst_gridfrompoints"]
    args = (0.0, 0.0, 10.0, 10.0, 4, 4, 3857, 2.0, 12, None)
    same = _pixels(gfp(pts, vals, *args))
    with_null = _pixels(gfp([None, *pts], [99.0, *vals], *args))
    assert (same == with_null).all()
    with pytest.raises(TypeError, match="Fix: build the GEOMETRY first"):
        gfp([b"\x01\x01\x00\x00\x00"], [1.0], *args)


def test_breaklines_are_geometries_too():
    points = [Geometry(shapely.Point(x, y, x + 2 * y), 3857) for x, y in PLANE]
    line = Geometry(shapely.LineString([(0, 0, 0), (10, 10, 30)]), 3857)
    with_line = _pixels(_dtm(points, [line]))
    without = _pixels(_dtm(points, []))
    assert with_line.shape == without.shape


# --- the oracle harness: the call the rows build ------------------------------------------------------------------------
def _param(fn, name):
    return next(p for p in MAN.functions[fn].params if p.name == name)


def test_the_handler_rows_hand_geometry_values_to_the_handler():
    case = next(c for c in RASTER_RASTERIZE if c.cid == "dtmfromgeoms_wkb")
    wkbs = oracles.resolve_arg(case.args[0], "/tmp")
    got = oracles._uc_value(wkbs, _param("gbx_rst_dtmfromgeoms", "points_array"), case)
    assert [type(g).__name__ for g in got] == ["UcGeometry"] * len(wkbs)
    assert shapely.from_wkb(got[4].getBytes()).has_z
    assert got[0].getSrid() == case.geom_srid
    assert (
        oracles._uc_value([], _param("gbx_rst_dtmfromgeoms", "breaklines_array"), case)
        == []
    )


def test_the_warehouse_literal_builds_each_point_from_iso_wkb():
    """The warehouse parses ISO WKB (type 1001) and rejects EWKB Z (type 2147483649), so the literal is st_geomfromwkb of
    ISO bytes cast to GEOMETRY(ANY); an empty array is a typed empty array."""
    case = next(c for c in RASTER_RASTERIZE if c.cid == "dtmfromgeoms_wkb")
    param = _param("gbx_rst_dtmfromgeoms", "points_array")
    wkbs = oracles.resolve_arg(case.args[0], "/tmp")
    lit = oracles._sql_literal(wkbs, param, case)
    assert lit.startswith("array(CAST(st_geomfromwkb(X'")
    assert lit.count("st_geomfromwkb(") == len(wkbs)
    assert lit.count(f", {case.geom_srid}) AS GEOMETRY(ANY))") == len(wkbs)
    first = lit.split("X'")[1].split("'")[0]
    iso = bytes.fromhex(first)
    assert int.from_bytes(iso[1:5], "little") == 1001  # a point with Z, ISO
    assert shapely.from_wkb(iso).has_z
    assert (
        oracles._sql_literal(
            [], _param("gbx_rst_dtmfromgeoms", "breaklines_array"), case
        )
        == "CAST(array() AS ARRAY<GEOMETRY(ANY)>)"
    )


def test_a_binary_literal_for_a_single_geometry_parameter_is_still_refused_by_the_engine():
    """Only the elements of a geometry array are built from WKB; the scalar row that pins the engine's refusal is unchanged."""
    case = next(c for c in RASTER_RASTERIZE if c.cid == "dtmfromgeoms_wkb")
    clip = _param("gbx_rst_clip", "geom")
    assert oracles._sql_literal(b"\x01\x01\x00\x00\x00", clip, case).startswith("X'")
