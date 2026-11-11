"""The rest of the generators on Unity Catalog: the four grid tessellations, the XYZ pyramid and the three TIN functions.
The oracle rows (bench/oracle_tables/raster_generator.py, web_tiles.py, vector_generator.py) hold the cells, the tiles,
the triangles and the elevation points, the refusals and the NULLs; this file holds the mechanisms: the table functions
over a private array routine, the same rows as the session kernel, the cap each family reads its input under, the array
bounds, and the geometry-array input of the TIN functions.
"""

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.bench.oracles import UcGeometry
from databricks.labs.gbx.pyrx.core import tessellate as tessellate_core
from databricks.labs.gbx.pyrx.core import xyz
from databricks.labs.gbx.pyrx.core.compression import write_gtiff
from databricks.labs.gbx.uc import ddl, forms
from databricks.labs.gbx.uc.handlers import _Refusal

MAN = manifest.load()
TESSELLATE = {
    "gbx_rst_h3_tessellate": ("gbx__rst_h3_tessellate_tiles", "h3"),
    "gbx_rst_quadbin_tessellate": ("gbx__rst_quadbin_tessellate_tiles", "quadbin"),
    "gbx_rst_bng_tessellate": ("gbx__rst_bng_tessellate_tiles", "bng"),
    "gbx_rst_custom_tessellate": ("gbx__rst_custom_tessellate_tiles", "custom"),
}
TIN = {
    "gbx_st_triangulate": "gbx__st_triangulate_rows",
    "gbx_st_interpolateelevationbbox": "gbx__st_interpolateelevationbbox_rows",
    "gbx_st_interpolateelevationgeom": "gbx__st_interpolateelevationgeom_rows",
}
FAMILY = {
    **{k: v[0] for k, v in TESSELLATE.items()},
    "gbx_rst_xyzpyramid": "gbx__rst_xyzpyramid_tiles",
    **TIN,
}


def _ramp(width, height, crs, origin, px, count=1, dtype="float32"):
    rng = np.random.default_rng(3)
    data = rng.random((count, height, width)).astype(dtype) * 100 + 1
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype=dtype,
        crs=crs,
        transform=from_origin(origin[0], origin[1], px, px),
        nodata=-9999.0,
    )
    return write_gtiff(profile, data)


def _tile(raster):
    return oracles._tile(raster)


# --- one table function over a private array routine per function ----------------------------------------------------
@pytest.mark.parametrize("name,private", sorted(FAMILY.items()))
def test_each_function_is_a_table_function_over_its_private_routine(name, private):
    """The five raster generators and the three TIN functions are SQL table functions over a private Python routine that
    returns the rows as one array (UC refuses a SQL function in a generator, so the private routine is called directly).
    """
    fn = MAN.functions[name]
    assert fn.levels["L4"] == {"tvf_over": private}
    pf = MAN.private[private]
    assert [n for n, _ in pf.params] == [p.name for p in fn.params]
    assert all(p.uc for p in fn.params), "every parameter has its UC type"
    text = ddl._table_function_ddl(fn, "`c`.`s`", name, private, "0.6.0")
    assert f"`c`.`s`.{private}(" in text
    rows = (
        "inline("
        if pf.returns.lower().startswith("array<struct") or pf.returns == "ARRAY<TILE>"
        else "explode("
    )
    assert rows in text


def test_the_tessellate_wrappers_keep_the_session_defaults():
    for name in TESSELLATE:
        fn = MAN.functions[name]
        defaults = {p.name: p.default for p in fn.params if p.default}
        assert defaults == {"assignment": "'centroid'", "coverage": "'complete'"}, name


def test_the_tin_functions_take_geometry_arrays_on_unity_catalog():
    """The warehouse hands a Python function an ARRAY<GEOMETRY(ANY)> as Geometry values that keep their Z; the session types
    stay WKB, EWKB, WKT and EWKT arrays."""
    for name in TIN:
        fn = MAN.functions[name]
        arrays = [
            p for p in fn.params if p.name in ("points_array", "breaklines_array")
        ]
        assert [p.uc for p in arrays] == ["ARRAY<GEOMETRY>"] * 2, name
        assert "ARRAY<GEOMETRY(ANY)>" in ddl._table_function_ddl(
            fn, "`c`.`s`", name, TIN[name], "0.6.0"
        )


# --- the tessellations: the session's cells -------------------------------------------------------------------------
def _grid_args(grid):
    if grid == "bng":
        return (_tile(_ramp(40, 30, "EPSG:27700", (530000.0, 183000.0), 100.0)), 3)
    return (
        _tile(_ramp(80, 60, "EPSG:4326", (-1.0, 52.0), 0.01)),
        4 if grid == "h3" else 10,
    )


@pytest.mark.parametrize(
    "assignment,coverage",
    [
        ("centroid", "complete"),
        ("centroid", "sparse"),
        ("covering", "complete"),
        ("covering", "sparse"),
    ],
)
@pytest.mark.parametrize(
    "name",
    ["gbx_rst_h3_tessellate", "gbx_rst_quadbin_tessellate", "gbx_rst_bng_tessellate"],
)
def test_a_tessellation_is_the_sessions_cells_with_the_sessions_ids(
    name, assignment, coverage
):
    """Same tiles as the session kernel: the cell ids (a BNG id as its digit id) and the raster bytes, in the kernel's order,
    each stamped with the grid in its metadata as the session stamps it."""
    private, grid = TESSELLATE[name]
    tile, res = _grid_args(grid)
    got = forms.HANDLERS[private](tile, res, assignment, coverage)
    with MemoryFile(bytes(tile["raster"])) as mf, mf.open() as ds:
        want = list(
            tessellate_core.iter_tessellate(
                ds, res, grid, mode=assignment, coverage=coverage
            )
        )
    assert [t["raster"] for t in got] == [r for _, r in want]
    if grid == "bng":
        from databricks.labs.gbx.pygx import _bng

        assert [t["cellid"] for t in got] == [_bng.parse(c) for c, _ in want]
    else:
        assert [t["cellid"] for t in got] == [c for c, _ in want]
    assert {t["metadata"]["gridSystem"] for t in got} == {grid}
    assert all(t["window"] is None and t["raster"] for t in got)


def test_a_custom_grid_tessellation_is_the_sessions_cells():
    from databricks.labs.gbx.bench.oracles import _build

    private, _ = TESSELLATE["gbx_rst_custom_tessellate"]
    tile, _res = _grid_args("bng")
    grid = _build("custom_grid_27700", "/tmp")
    got = forms.HANDLERS[private](tile, grid, 0, "centroid", "complete")
    from databricks.labs.gbx.pygx import _custom

    with MemoryFile(bytes(tile["raster"])) as mf, mf.open() as ds:
        want = list(
            tessellate_core.iter_tessellate(
                ds,
                0,
                "custom",
                mode="centroid",
                coverage="complete",
                conf=_custom.conf_from_row(grid),
            )
        )
    assert [t["cellid"] for t in got] == [c for c, _ in want] and got


@pytest.mark.parametrize("name", sorted(TESSELLATE))
def test_the_tessellations_hold_the_pixel_cap_not_the_windowed_cap(
    name, tmp_path, monkeypatch
):
    """A tessellation keeps a label raster of 4 bytes a pixel for the whole tile (and the BNG route warps it whole), so it
    is not a window reader: its input cap is the 64 MiB of a pixel function, not the 256 MiB of the retile family. Raising
    the windowed cap changes nothing for it."""
    monkeypatch.setattr(forms, "WINDOWED_DECODED_CAP", 10**12)
    over = oracles._build("rg_over_cap", str(tmp_path))  # 64.1 MiB decoded
    private, grid = TESSELLATE[name]
    args = (
        (over, oracles._build("custom_grid_27700", str(tmp_path)), 0)
        if grid == "custom"
        else (over, 3 if grid == "bng" else 4)
    )
    with pytest.raises(_Refusal, match="decoded, over the 64 MiB"):
        forms.HANDLERS[private](*args, "centroid", "complete")


def test_a_tessellation_array_over_the_cap_is_refused_as_the_tiles_are_cut(monkeypatch):
    """The running total of the tiles made is held to the cap: an array the tile let through is never returned over it."""
    tile, res = _grid_args("h3")
    full = forms.rst_h3_tessellate_tiles(tile, res, "covering", "complete")
    assert len(full) > 4
    one = len(full[0]["raster"])
    monkeypatch.setattr(forms, "DECODED_CAP", 3 * one)
    with pytest.raises(_Refusal, match="Fix:"):
        forms.rst_h3_tessellate_tiles(tile, res, "covering", "complete")


def test_a_tessellation_checks_its_words_before_reading_the_tile():
    tile, res = _grid_args("h3")
    with pytest.raises(ValueError, match="sideways"):
        forms.rst_h3_tessellate_tiles(tile, res, "sideways", "complete")
    with pytest.raises(ValueError, match="sideways"):
        forms.rst_h3_tessellate_tiles(tile, res, "centroid", "sideways")
    assert forms.rst_h3_tessellate_tiles(None, res, "sideways", "complete") is None


# --- the XYZ pyramid ------------------------------------------------------------------------------------------------
def test_the_pyramid_is_the_sessions_tiles():
    tile, _ = _grid_args("h3")
    got = forms.rst_xyzpyramid_tiles(tile, 8, 9, "PNG", 64, "bilinear", "auto")
    with MemoryFile(bytes(tile["raster"])) as mf, mf.open() as ds:
        want = list(xyz.iter_pyramid(ds, 8, 9, "PNG", 64, "bilinear", "auto"))
    assert [(r["z"], r["x"], r["y"], r["bytes"]) for r in got] == want and want


def test_the_pyramid_stops_rendering_at_the_cap(monkeypatch):
    """The tiles are counted as they are rendered, so a pyramid over the cap stops there and renders no more."""
    tile, _ = _grid_args("h3")
    rendered = []
    real = xyz.render_tile

    def spy(*a, **kw):
        rendered.append(a[1:4])
        return real(*a, **kw)

    monkeypatch.setattr(xyz, "render_tile", spy)
    monkeypatch.setattr(
        forms, "DECODED_CAP", 25_000
    )  # over the tile's 19 KB decoded, under the pyramid
    with pytest.raises(_Refusal, match="Fix:"):
        forms.rst_xyzpyramid_tiles(tile, 8, 12, "PNG", 256, "bilinear", "auto")
    with MemoryFile(bytes(tile["raster"])) as mf, mf.open() as ds:
        everything = xyz.tile_count(ds, 8, 12)
    assert 0 < len(rendered) < everything


def test_the_pyramid_holds_the_pixel_cap_not_the_windowed_cap(tmp_path, monkeypatch):
    """It renders through the kernel of gbx_rst_tilexyz, which UC reads under the 64 MiB pixel cap."""
    monkeypatch.setattr(forms, "WINDOWED_DECODED_CAP", 10**12)
    over = oracles._build("rg_over_cap", str(tmp_path))
    with pytest.raises(_Refusal, match="decoded, over the 64 MiB"):
        forms.rst_xyzpyramid_tiles(over, 8, 9, "PNG", 256, "bilinear", "auto")


# --- the TIN functions ----------------------------------------------------------------------------------------------
def _geoms(wkts, srid=27700):
    from shapely import to_wkb

    from databricks.labs.gbx._geom import parse_geom

    return [
        None if w is None else UcGeometry(to_wkb(parse_geom(w)), srid) for w in wkts
    ]


SQUARE = [
    "POINT Z (0 0 1)",
    "POINT Z (10 0 21)",
    "POINT Z (10 10 31)",
    "POINT Z (0 10 11)",
    "POINT Z (5 5 16)",
]


def test_the_triangles_are_the_kernels_with_the_z_dropped():
    from shapely import from_wkb

    from databricks.labs.gbx.pyvx import _tin

    rows = forms.st_triangulate_rows(
        _geoms(SQUARE), None, 0.0, 0.0, "NONENCROACHING", "constrained"
    )
    xy = np.array(
        [
            [float(v) for v in w.replace("POINT Z (", "").replace(")", "").split()]
            for w in SQUARE
        ]
    )
    want = _tin.triangulate(xy, [], 0.0, 0.0)
    assert len(rows) == len(want) == 4
    for blob, tri in zip(rows, want):
        poly = from_wkb(bytes(blob))
        assert not poly.has_z
        assert {tuple(c) for c in poly.exterior.coords} == {
            (p[0], p[1]) for p in tri
        } | {(tri[0][0], tri[0][1])}


def test_a_geometry_array_keeps_its_z_and_drops_a_null_element():
    """The Z of a Geometry element reaches the kernel; a NULL element is dropped; a multi-point adds each of its points."""
    from shapely import from_wkb

    pts = _geoms(SQUARE[:4] + [None]) + _geoms(["MULTIPOINT Z ((5 5 16))"])
    rows = forms.st_interpolateelevationgeom_rows(
        pts,
        None,
        0.0,
        0.0,
        "NONENCROACHING",
        _geoms(["POINT (0 10)"])[0],
        2,
        2,
        5.0,
        -5.0,
        "constrained",
    )
    z = sorted(round(from_wkb(bytes(b)).z, 6) for b in rows)
    want = sorted(round(2.0 * x + y + 1.0, 6) for x in (2.5, 7.5) for y in (2.5, 7.5))
    assert z == want


def test_the_elevation_points_are_iso_wkb_the_warehouse_parses():
    """A point with Z goes out as ISO WKB (type 1001), the form st_geomfromwkb reads; the EWKB Z flag is refused there."""
    import struct

    rows = forms.st_interpolateelevationbbox_rows(
        _geoms(SQUARE),
        None,
        0.0,
        0.0,
        "NONENCROACHING",
        0.0,
        0.0,
        10.0,
        10.0,
        2,
        2,
        0,
        "constrained",
    )
    assert rows
    for blob in rows:
        assert struct.unpack("<I", bytes(blob)[1:5])[0] == 1001


def test_a_geometry_array_over_the_cap_is_refused_before_the_surface(monkeypatch):
    monkeypatch.setattr(forms, "DECODED_CAP", 100)
    with pytest.raises(_Refusal, match="Fix:"):
        forms.st_triangulate_rows(
            _geoms(SQUARE), None, 0.0, 0.0, "NONENCROACHING", "constrained"
        )


def test_the_triangle_array_is_estimated_from_the_point_count(monkeypatch):
    """At most 2 n triangles come from n points: the array is refused from the count, before the triangulation runs."""
    import databricks.labs.gbx.pyvx._tin as tin

    monkeypatch.setattr(forms, "DECODED_CAP", 500)
    monkeypatch.setattr(
        tin,
        "triangulate",
        lambda *a, **k: pytest.fail("triangulated before the estimate"),
    )
    with pytest.raises(_Refusal, match="upper estimate"):
        forms.st_triangulate_rows(
            _geoms(SQUARE), None, 0.0, 0.0, "NONENCROACHING", "constrained"
        )


def test_the_conforming_triangulation_is_refused_as_its_split_points_pass_the_estimate(
    monkeypatch,
):
    """The conforming mode adds split points: the 2-per-point estimate is checked again as they are added, so a build
    that grows past the cap stops while it splits, before any triangle is made."""
    import databricks.labs.gbx.pyvx._conforming as conforming

    box = [
        "POINT Z (0 -5 0)",
        "POINT Z (10 -5 0)",
        "POINT Z (10 5 0)",
        "POINT Z (0 5 0)",
    ]
    crossing = [
        "LINESTRING Z (1 -0.5 0, 9 0.5 0)",
        "LINESTRING Z (2 0.5 0, 8.63 -0.5 0)",
    ]
    sites = len(box) + 4
    monkeypatch.setattr(
        forms, "DECODED_CAP", 2 * (sites + 2) * forms._TRIANGLE_ROW_BYTES
    )  # the sites pass, a few split points more do not
    for name in ("jts_delaunay", "unique_delaunay"):
        monkeypatch.setattr(
            conforming,
            name,
            lambda *a, **k: pytest.fail("triangulated past the estimate"),
        )
    with pytest.raises(_Refusal, match="upper estimate.*Fix:"):
        forms.st_triangulate_rows(
            _geoms(box), _geoms(crossing), 0.0, 0.0, "NONENCROACHING", "conforming"
        )


def test_a_bad_split_point_finder_and_crossing_break_lines_raise_with_the_function_name():
    with pytest.raises(
        ValueError, match="gbx_st_triangulate: split_point_finder must be"
    ):
        forms.st_triangulate_rows(
            _geoms(SQUARE), None, 0.0, 0.0, "SIDEWAYS", "constrained"
        )
    box = [
        "POINT Z (0 -5 0)",
        "POINT Z (10 -5 0)",
        "POINT Z (10 5 0)",
        "POINT Z (0 5 0)",
    ]
    crossing = [
        "LINESTRING Z (1 -0.5 0, 9 0.5 0)",
        "LINESTRING Z (2 0.5 0, 8.63 -0.5 0)",
    ]
    with pytest.raises(
        ValueError,
        match="gbx_st_triangulate: mode 'conforming' could not fit .* Fix: split crossing",
    ):
        forms.st_triangulate_rows(
            _geoms(box), _geoms(crossing), 0.0, 0.0, "NONENCROACHING", "conforming"
        )


_BOX = ["POINT Z (0 -5 0)", "POINT Z (10 -5 0)", "POINT Z (10 5 0)", "POINT Z (0 5 0)"]
_CROSSING = ["LINESTRING Z (1 -0.5 0, 9 0.5 0)", "LINESTRING Z (2 0.5 0, 8.63 -0.5 0)"]


@pytest.mark.parametrize(
    "fn,call",
    [
        (
            "gbx_st_interpolateelevationbbox",
            lambda pts, bl, spf, mode: forms.st_interpolateelevationbbox_rows(
                pts, bl, 0.0, 0.0, spf, 0.0, -5.0, 10.0, 5.0, 2, 2, 0, mode
            ),
        ),
        (
            "gbx_st_interpolateelevationgeom",
            lambda pts, bl, spf, mode: forms.st_interpolateelevationgeom_rows(
                pts,
                bl,
                0.0,
                0.0,
                spf,
                _geoms(["POINT (0 5)"])[0],
                2,
                2,
                5.0,
                -5.0,
                mode,
            ),
        ),
    ],
)
def test_the_elevation_forms_name_the_function_on_a_bad_finder_and_a_give_up(fn, call):
    with pytest.raises(ValueError, match=f"{fn}: split_point_finder must be"):
        call(_geoms(_BOX), None, "SIDEWAYS", "constrained")
    with pytest.raises(
        ValueError, match=f"{fn}: mode 'conforming' could not fit .* Fix:"
    ):
        call(_geoms(_BOX), _geoms(_CROSSING), "NONENCROACHING", "conforming")
    # with no points there is nothing to triangulate: no rows, as in the session, whatever the finder
    assert call([], None, "SIDEWAYS", "constrained") is None


@pytest.mark.parametrize(
    "fn,call",
    [
        (
            "gbx_st_interpolateelevationbbox",
            lambda pts, bl, spf, mode: forms.st_interpolateelevationbbox_rows(
                pts, bl, 0.0, 0.0, spf, 0.0, -5.0, 10.0, 5.0, 2, 2, 0, mode
            ),
        ),
        (
            "gbx_st_interpolateelevationgeom",
            lambda pts, bl, spf, mode: forms.st_interpolateelevationgeom_rows(
                pts,
                bl,
                0.0,
                0.0,
                spf,
                _geoms(["POINT (0 5)"])[0],
                2,
                2,
                5.0,
                -5.0,
                mode,
            ),
        ),
    ],
)
def test_the_elevation_surface_is_refused_from_the_point_count_before_it_is_built(
    fn, call, monkeypatch
):
    """The surface of n points has at most 2 n triangles: in either mode the same estimate as the triangulation's is
    checked against the cap before any triangle is made (the grid's own cap is apart).
    """
    import databricks.labs.gbx.pyvx._tin as tin

    monkeypatch.setattr(forms, "DECODED_CAP", 500)
    monkeypatch.setattr(
        tin,
        "triangulate",
        lambda *a, **k: pytest.fail("triangulated before the estimate"),
    )
    for mode in ("constrained", "conforming"):
        with pytest.raises(
            _Refusal, match=f"{fn}: .*upper estimate.*Fix: pass fewer points"
        ):
            call(_geoms(_BOX), None, "NONENCROACHING", mode)


@pytest.mark.parametrize(
    "fn,call",
    [
        (
            "gbx_st_interpolateelevationbbox",
            lambda pts, bl, spf, mode: forms.st_interpolateelevationbbox_rows(
                pts, bl, 0.0, 0.0, spf, 0.0, -5.0, 10.0, 5.0, 2, 2, 0, mode
            ),
        ),
        (
            "gbx_st_interpolateelevationgeom",
            lambda pts, bl, spf, mode: forms.st_interpolateelevationgeom_rows(
                pts,
                bl,
                0.0,
                0.0,
                spf,
                _geoms(["POINT (0 5)"])[0],
                2,
                2,
                5.0,
                -5.0,
                mode,
            ),
        ),
    ],
)
def test_the_conforming_elevation_surface_is_refused_as_its_split_points_pass_the_estimate(
    fn, call, monkeypatch
):
    import databricks.labs.gbx.pyvx._conforming as conforming

    sites = len(_BOX) + 4
    monkeypatch.setattr(
        forms, "DECODED_CAP", 2 * (sites + 2) * forms._TRIANGLE_ROW_BYTES
    )  # the sites pass, a few split points more do not
    for name in ("jts_delaunay", "unique_delaunay"):
        monkeypatch.setattr(
            conforming,
            name,
            lambda *a, **k: pytest.fail("triangulated past the estimate"),
        )
    with pytest.raises(_Refusal, match=f"{fn}: .*upper estimate.*Fix:"):
        call(_geoms(_BOX), _geoms(_CROSSING), "NONENCROACHING", "conforming")


def test_the_conforming_mode_runs_here():
    # no break lines: the conforming triangulation is the Delaunay one, the four triangles round the centre
    rows = forms.st_triangulate_rows(
        _geoms(SQUARE), None, 0.0, 0.0, "MIDPOINT", "conforming"
    )
    assert len(rows) == 4


def test_constrained_crossing_break_lines_raise_with_the_function_name_and_fix():
    lines = _geoms(["LINESTRING (1 3, 9 6)", "LINESTRING (1 6, 9 3)"])
    with pytest.raises(
        ValueError,
        match=r"^gbx_st_triangulate: mode 'constrained'.*Fix: split the break lines",
    ):
        forms.st_triangulate_rows(
            _geoms(SQUARE), lines, 0.0, 0.0, "NONENCROACHING", "constrained"
        )


def test_a_null_mode_gives_no_rows():
    assert (
        forms.st_triangulate_rows(
            _geoms(SQUARE), None, 0.0, 0.0, "NONENCROACHING", None
        )
        is None
    )
