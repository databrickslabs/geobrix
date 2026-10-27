"""A raster with an alpha band AND its own (per-dataset) mask band keeps both through the
light writers and warps, and a raster that is not alpha does not become one.

GDAL reports the own mask only when a raster has both (it outranks the mask derived from the
alpha band), so ``_nodata.has_own_mask`` used to exclude every alpha raster and the writers
dropped the mask: rows hidden by the mask came out valid. The rule is the one of
``_nodata.valid_mask``: a pixel is valid when a data band is valid, its alpha is above 0 and
the raster's own mask is valid.
"""

import h3
import numpy as np
import pytest
import rasterio
from rasterio.enums import ColorInterp as CI
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import _nodata as nd
from databricks.labs.gbx.pyrx.core import (
    accessors,
    agg,
    derivedband,
    features,
    focal,
    gridagg,
)
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core import stretch
from databricks.labs.gbx.pyrx.core import tessellate as T
from databricks.labs.gbx.pyrx.core import warp
from databricks.labs.gbx.pyrx.core.compression import write_gtiff
from databricks.labs.gbx.pyrx.core.gridagg import _stamp_crs_bytes
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
from databricks.labs.gbx.pyrx.functions import _dataset_to_gtiff_bytes

from . import _alpha_layouts as al
from .test_tessellate_centroid_window import _ALPHA_CASES, _FILL_N, _chips_of

H = 12
MASKED = (3, 4)
TRANSPARENT = (8,)
# an alpha band GDAL derives a mask from (4 bands), and one it does not (3 bands)
BOTH = ["rgba", "gray_x_alpha"]


def _fixture(name):
    return al.row_coded(
        name, height=H, masked_rows=MASKED, transparent_rows=TRANSPARENT
    )


def _want_rows(name):
    return al.valid_rows(name, H, MASKED, TRANSPARENT)


def _valid_rows(raster_bytes):
    """The source rows (coded in the pixels) that are valid in a same-grid output."""
    with MemoryFile(raster_bytes) as mf, mf.open() as ds:
        valid = nd.valid_mask(ds)
        coded = ds.read(1).astype("int64") - 100
    rows = sorted({int(r) for r in coded[valid]})
    # a row is valid or not as a whole in these fixtures
    for r in range(H):
        assert valid[coded == r].all() or not valid[coded == r].any(), r
    return rows


@pytest.mark.parametrize("name", BOTH)
def test_an_alpha_raster_with_its_own_mask_has_both(name):
    with MemoryFile(_fixture(name)) as mf, mf.open() as ds:
        assert nd.has_alpha(ds) and nd.has_own_mask(ds)
        assert nd.valid_mask(ds)[:, 0].tolist() == [
            r in _want_rows(name) for r in range(H)
        ]


@pytest.mark.parametrize("name", al.LAST_ALPHA)
def test_a_mask_derived_from_the_alpha_band_is_not_an_own_mask(name):
    with MemoryFile(al.row_coded(name, height=H, transparent_rows=TRANSPARENT)) as mf:
        with mf.open() as ds:
            assert nd.has_alpha(ds) and not nd.has_own_mask(ds)


# writer name -> fn(open dataset) -> GTiff bytes on the source's grid
SAME_GRID = {
    "identity_srid": lambda ds: warp.reproject_to_srid(ds, 4326),
    "identity_crs": lambda ds: warp.reproject_to_crs(ds, "EPSG:4326"),
    "stamp_crs": lambda ds: _stamp_crs_bytes(ds, "EPSG:3857"),
    "dataset_to_bytes": _dataset_to_gtiff_bytes,
}


@pytest.mark.parametrize("writer", list(SAME_GRID))
@pytest.mark.parametrize("name", BOTH)
def test_a_same_grid_writer_keeps_the_mask_beside_the_alpha_band(writer, name):
    with MemoryFile(_fixture(name)) as mf, mf.open() as ds:
        out = SAME_GRID[writer](ds)
    assert _valid_rows(out) == _want_rows(name), f"{writer} of {name}"


@pytest.mark.parametrize("pending_nodata", [False, True], ids=["plain", "initnodata"])
@pytest.mark.parametrize("name", BOTH)
def test_the_virtual_tile_read_keeps_the_mask_beside_the_alpha_band(
    tmp_path, name, pending_nodata
):
    """The lazy read of an alpha source with its own mask. With a pending ``rst_initnodata`` the masked
    pixels take the NoData instead of being masked (in every band, the alpha band included), and the
    rows hidden by the mask or by alpha stay invalid either way."""
    path = str(tmp_path / "s.tif")
    with open(path, "wb") as fh:
        fh.write(_fixture(name))
    meta = {ot.PENDING_NODATA: ot.PENDING_NODATA_AUTO} if pending_nodata else {}
    tile = VirtualTile(cellid=0, path=path, window=(0, 0, 12, H), metadata=meta)
    out = ot.materialize_to_bytes(tile).raster
    assert _valid_rows(out) == _want_rows(name)
    if not pending_nodata:
        assert _valid_rows(ot._windowed_materialize_bytes(tile)) == _want_rows(name)
    else:
        with MemoryFile(out) as mf, mf.open() as ds:
            assert ds.nodata == 0, "the pending NoData is stamped"


@pytest.mark.parametrize(
    "target,warper",
    [
        ("EPSG:3857", lambda ds: warp.reproject_to_srid(ds, 3857)),
        ("ESRI:54008", lambda ds: warp.reproject_to_crs(ds, "ESRI:54008")),
    ],
    ids=["srid-3857", "crs-54008"],
)
@pytest.mark.parametrize("name", BOTH)
def test_the_eager_warp_keeps_the_mask_beside_the_alpha_band(name, target, warper):
    """Output pixels that come from a masked or transparent source row are invalid, and the
    others are valid (the warp fill is alpha 0)."""
    with MemoryFile(_fixture(name)) as mf, mf.open() as ds:
        out = warper(ds)
    with MemoryFile(out) as mf, mf.open() as o:
        coded = o.read(1).astype("int64") - 100
        valid = nd.valid_mask(o)
        assert nd.has_alpha(o)
    covered = coded >= 0
    from_hidden = covered & np.isin(
        coded, [r for r in range(H) if r not in _want_rows(name)]
    )
    from_visible = covered & np.isin(coded, _want_rows(name))
    assert from_hidden.any() and from_visible.any(), "fixture: the warp keeps both"
    assert not valid[
        from_hidden
    ].any(), f"{int(valid[from_hidden].sum())} hidden pixels are valid"
    assert valid[from_visible].all()


def _per_band_nodata_masked(tmp_path, nodatavals):
    """A VRT (the only source rasterio reads with a NoData per band) over a GTiff with an
    internal mask band over row 5. Every band holds ``100 + row`` except rows 0-1, which hold
    that band's own NoData value, so a band is only right about its OWN NoData."""
    path = str(tmp_path / "m.tif")
    data = np.empty((3, 8, 8), dtype="uint8")
    data[:] = (100 + np.arange(8, dtype="uint8"))[None, :, None]
    for b, v in enumerate(nodatavals):
        data[b, :2] = 0 if v is None else int(v)
    mask = np.full((8, 8), 255, dtype="uint8")
    mask[5] = 0
    prof = dict(
        driver="GTiff",
        width=8,
        height=8,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(0, 8, 1, 1),
    )
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
        with rasterio.open(path, "w", **prof) as dst:
            dst.write(data)
            dst.write_mask(mask)
    bands = "".join(
        f'<VRTRasterBand dataType="Byte" band="{b}">'
        + (f"<NoDataValue>{v}</NoDataValue>" if v is not None else "")
        + f'<SimpleSource><SourceFilename relativeToVRT="0">{path}</SourceFilename>'
        f"<SourceBand>{b}</SourceBand></SimpleSource></VRTRasterBand>"
        for b, v in enumerate(nodatavals, 1)
    )
    mask_band = (
        '<MaskBand><VRTRasterBand dataType="Byte"><SimpleSource>'
        f'<SourceFilename relativeToVRT="0">{path}</SourceFilename>'
        "<SourceBand>mask,1</SourceBand></SimpleSource></VRTRasterBand></MaskBand>"
    )
    return (
        '<VRTDataset rasterXSize="8" rasterYSize="8"><SRS>EPSG:4326</SRS>'
        f"<GeoTransform>0,1,0,8,0,-1</GeoTransform>{bands}{mask_band}</VRTDataset>"
    )


def test_the_own_mask_warp_treats_each_band_by_its_own_nodata(tmp_path):
    """A source with an own mask whose bands declare different NoData values: band 1's value
    is what the output declares, and each band's OWN NoData pixels (rows 0-1) are still
    NoData after the warp, not data. Every band ends up invalid in the same pixels."""
    vrt = _per_band_nodata_masked(tmp_path, (10, 20, 30))
    with rasterio.open(vrt) as ds:
        assert ds.nodatavals == (10.0, 20.0, 30.0) and nd.has_own_mask(ds)
        out = warp.reproject_to_srid(ds, 3857)
    with MemoryFile(out) as mf, mf.open() as o:
        invalid = [(o.read_masks(b) == 0) for b in (1, 2, 3)]
        assert o.nodata == 10.0
    assert invalid[0].any()
    assert (invalid[1] == invalid[0]).all(), "band 2 kept its own NoData pixels as data"
    assert (invalid[2] == invalid[0]).all(), "band 3 kept its own NoData pixels as data"


def test_the_own_mask_warp_declares_the_first_nodata_when_band_one_declares_none(
    tmp_path,
):
    """Band 1 declares no NoData, bands 2 and 3 do: as for the fill plan (``_fill_plan``) the
    output declares the first declared value (20), and the masked pixels and the NoData
    pixels of bands 2 and 3 are NoData, while band 1 keeps its rows 0-1 (value 0) as data.
    """
    vrt = _per_band_nodata_masked(tmp_path, (None, 20, 30))
    with rasterio.open(vrt) as ds:
        assert ds.nodata is None and nd.has_own_mask(ds)
        out = warp.reproject_to_srid(ds, 3857)
    with MemoryFile(out) as mf, mf.open() as o:
        assert o.nodata == 20.0
        invalid = [(o.read_masks(b) == 0) for b in (1, 2, 3)]
    assert (invalid[1] == invalid[2]).all()
    assert invalid[0].any() and (invalid[0] & ~invalid[1]).sum() == 0
    assert (invalid[1] & ~invalid[0]).any(), "band 1 lost its rows 0-1"


# --- raster-to-grid reducers and accessors -----------------------------------------------
# A transparent pixel counts for no band, however many bands the raster has: GDAL derives a
# mask from the alpha band for 2- and 4-band rasters only, so a 3-band gray+x+alpha raster
# read as all valid.


def _valid_counts(name):
    """Per band, the pixels of ``al.pattern`` that are valid for it. A data band of an alpha
    layout counts the opaque pixels (rows 1 and 3, 4 pixels each); the alpha band counts every
    pixel (it is summarised by its own values); a layout with no alpha band counts every
    pixel."""
    n = len(al.LAYOUTS[name][0])
    return [8] * (n - 1) + [16] if al.LAYOUTS[name][1] else [16] * n


_CUSTOM_CONF = {
    "bound_x_min": 0,
    "bound_x_max": 200,
    "bound_y_min": 0,
    "bound_y_max": 200,
    "cell_splits": 2,
    "root_cell_size_x": 200,
    "root_cell_size_y": 200,
    "srid": -1,
}

# grid -> (resolution, crs, transform, grid_conf): a 4x4 raster inside a handful of cells
_GRIDS = {
    "h3": (4, "EPSG:4326", from_origin(10, 50, 0.5, 0.5), None),
    "quadbin": (4, "EPSG:4326", from_origin(10, 50, 0.5, 0.5), None),
    "bng": (3, "EPSG:27700", from_origin(530000, 180400, 200, 200), None),
    "custom": (1, None, from_origin(0, 200, 50, 50), _CUSTOM_CONF),
}


@pytest.mark.parametrize("assignment", ["centroid", "covering"])
@pytest.mark.parametrize("grid", list(_GRIDS))
@pytest.mark.parametrize("name", list(al.LAYOUTS))
def test_raster_to_grid_counts_only_the_pixels_valid_for_each_band(
    grid, name, assignment
):
    resolution, crs, transform, conf = _GRIDS[grid]
    raw = al.layout_bytes(name, crs=crs, transform=transform)
    with MemoryFile(raw) as mf, mf.open() as ds:
        result = gridagg.raster_to_grid(
            ds,
            resolution,
            grid,
            "count",
            coverage="sparse",
            assignment=assignment,
            grid_conf=conf,
        )
    totals = [sum(c["measure"] for c in band) for band in result]
    assert totals == pytest.approx(
        [float(n) for n in _valid_counts(name)]
    ), f"{grid} {assignment} {name}"


@pytest.mark.parametrize("name", list(al.LAYOUTS))
def test_accessor_statistics_cover_only_the_opaque_pixels(name):
    """Rows 1 and 3 of the pattern are opaque (band 1 holds 0 there and 7), so every band's
    statistics are over those pixels."""
    with MemoryFile(al.layout_bytes(name)) as mf, mf.open() as ds:
        alpha = al.LAYOUTS[name][1]
        assert accessors.pixelcount(ds) == _valid_counts(name)
        if alpha:
            # band 1: 4 zeros (row 1) and 4 sevens (row 3)
            assert accessors.minimum(ds)[0] == 0.0
            assert accessors.maximum(ds)[0] == 7.0
            assert accessors.avg(ds)[0] == 3.5
            assert accessors.median(ds)[0] == 3.5
            # the alpha band is summarised by its own values over every pixel: 0 and 255
            assert accessors.minimum(ds)[-1] == 0.0
            assert accessors.maximum(ds)[-1] == 255.0
        assert not accessors.isempty(ds)


@pytest.mark.parametrize("name", al.LAST_ALPHA)
def test_a_raster_that_is_entirely_transparent_is_empty(name):
    count = len(al.LAYOUTS[name][0])
    data = np.full((count, 4, 4), 9, dtype="uint8")
    data[-1] = 0
    with MemoryFile(al.layout_bytes(name, data=data)) as mf, mf.open() as ds:
        assert accessors.isempty(ds)
        assert accessors.pixelcount(ds)[:-1] == [0] * (count - 1)
        assert accessors.avg(ds)[:-1] == [None] * (count - 1)


# --- frombands / derivedband stacks ---------------------------------------------------------


def _band_tile(value_rows, *, mask_rows=(), alpha=False, nodata=None):
    """A 12x12 uint8 GTiff of ONE data band (or gray+alpha when ``alpha``): ``value_rows`` hold
    50, the others 0, and ``mask_rows`` are hidden by an own mask band."""
    data = np.zeros((2 if alpha else 1, 12, 12), dtype="uint8")
    data[0, list(value_rows), :] = 50
    if alpha:
        data[1] = 255
    prof = dict(
        driver="GTiff",
        width=12,
        height=12,
        count=data.shape[0],
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(10, 50, 0.5, 0.5),
        nodata=nodata,
        photometric="MINISBLACK",
    )
    mask = None
    if mask_rows:
        mask = np.full((12, 12), 255, dtype="uint8")
        mask[list(mask_rows), :] = 0
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True), MemoryFile() as mf:
        with mf.open(**prof) as dst:
            if alpha:
                from rasterio.enums import ColorInterp as CI

                dst.colorinterp = [CI.gray, CI.alpha]
            dst.write(data)
            if mask is not None:
                dst.write_mask(mask)
        return mf.read()


def test_four_stacked_single_band_tiles_are_not_given_an_alpha_band():
    """GDAL labels band 4 of a 4-band uint8 raster alpha when it creates one: a stack of four
    data tiles (R, G, B, NIR) has no alpha band, so a 0 in the 4th is valid."""
    tiles = [_band_tile(rows) for rows in ((1, 2), (3,), (4,), (5, 6))]
    out = agg.frombands_tiles(list(enumerate(tiles)))
    assert al.labelled_alpha(out) == []
    with MemoryFile(out) as mf, mf.open() as ds:
        assert nd.valid_mask(ds).all()


def test_a_stack_labels_alpha_only_when_the_last_tile_ends_in_one():
    gray, gray_alpha = _band_tile((1,)), _band_tile((2,), alpha=True)
    ends_in_alpha = agg.frombands_tiles([(0, gray), (1, gray_alpha)])
    alpha_inside = agg.frombands_tiles([(0, gray_alpha), (1, gray)])
    assert al.labelled_alpha(ends_in_alpha) == [2]
    assert al.labelled_alpha(alpha_inside) == []


def test_a_stack_keeps_the_mask_the_tiles_share():
    """Three tiles with the same own mask: the stack has it. If one tile has none, a band
    of the stack is valid everywhere and so is the stack."""
    rows = (3, 4)
    masked = [_band_tile((1,), mask_rows=rows) for _ in range(3)]
    out = agg.frombands_tiles(list(enumerate(masked)))
    with MemoryFile(out) as mf, mf.open() as ds:
        assert nd.has_own_mask(ds)
        assert nd.valid_mask(ds)[:, 0].tolist() == [r not in rows for r in range(12)]
    mixed = agg.frombands_tiles([(0, masked[0]), (1, _band_tile((1,))), (2, masked[2])])
    with MemoryFile(mixed) as mf, mf.open() as ds:
        assert nd.valid_mask(ds).all()


def _derive(raw):
    with MemoryFile(raw) as mf, mf.open() as ds:
        return derivedband.derivedband(ds, _SUM, "total")


_SUM = (
    "def total(in_ar, out_ar, xoff, yoff, xsize, ysize, rxsize, rysize, radius, gt, **kw):\n"
    "    out_ar[:] = sum(a.astype('float64') for a in in_ar)\n"
)


@pytest.mark.parametrize("name", BOTH + ["rgb_nir"])
def test_derivedband_output_is_invalid_where_the_input_is(name):
    """The single band derived from a raster has no alpha band of its own: the pixels the
    input marks transparent or masked are invalid in the output's own mask."""
    raw = al.row_coded(name, height=H, masked_rows=MASKED, transparent_rows=TRANSPARENT)
    out = _derive(raw)
    with MemoryFile(out) as mf, mf.open() as ds:
        assert ds.count == 1 and not nd.has_alpha(ds)
        assert nd.valid_mask(ds)[:, 0].tolist() == [
            r in _want_rows(name) for r in range(H)
        ]


def test_derivedband_agg_of_four_tiles_is_valid_where_the_fourth_is_zero():
    """The tiles are stacked as bands before the function runs: the 4th is data."""
    tiles = [_band_tile(rows) for rows in ((1, 2), (3,), (4,), (5, 6))]
    out = agg.derivedband_tiles(tiles, _SUM, "total")
    with MemoryFile(out) as mf, mf.open() as ds:
        assert nd.valid_mask(ds).all()


# --- the merge, in RAM and streamed ---------------------------------------------------------


@pytest.mark.parametrize("streaming", [False, True], ids=["in-ram", "streaming"])
@pytest.mark.parametrize("name", ["rgb_nir", "rgba", "gray_x_alpha"])
def test_a_merge_keeps_the_alpha_band_and_the_mask(monkeypatch, name, streaming):
    """Two side-by-side tiles with an own mask (and an alpha band): the mosaic is invalid
    where the tiles are, labelled alpha where they are, and a 4th data band stays data.
    """
    if streaming:
        monkeypatch.setattr(agg, "decoded_budget_bytes", lambda s: 1)
    tiles = [
        al.row_coded(
            name,
            height=H,
            masked_rows=MASKED,
            transparent_rows=TRANSPARENT,
            transform=from_origin(x, 50, 0.5, 0.5),
        )
        for x in (10, 16)
    ]
    out = agg.merge_tiles(tiles)
    alpha = al.LAYOUTS[name][1]
    with MemoryFile(out) as mf, mf.open() as ds:
        assert ds.width == 24
        assert al.labelled_alpha(out) == ([ds.count - 1] if alpha else [])
        valid = nd.valid_mask(ds)
    assert valid[:, 0].tolist() == [r in _want_rows(name) for r in range(H)]
    assert valid[:, 23].tolist() == [r in _want_rows(name) for r in range(H)]


@pytest.mark.parametrize("streaming", [False, True], ids=["in-ram", "streaming"])
def test_a_merge_keeps_valid_zero_pixels_when_a_tile_has_an_own_mask(
    monkeypatch, streaming
):
    """The union mask comes from the tiles' mask bands, not from the merged values: a valid pixel
    that holds 0 (black, sea level) stays valid, and the result does not depend on which path merged
    the group."""
    if streaming:
        monkeypatch.setattr(agg, "decoded_budget_bytes", lambda s: 1)

    def tile(x):
        data = np.full((1, 12, 12), 50, dtype="uint8")
        data[:, 5, :] = 0  # valid zeros
        mask = np.full((12, 12), 255, dtype="uint8")
        mask[3, :] = 0
        prof = dict(
            driver="GTiff",
            width=12,
            height=12,
            count=1,
            dtype="uint8",
            crs="EPSG:4326",
            transform=from_origin(x, 50, 0.5, 0.5),
        )
        with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True), MemoryFile() as mf:
            with mf.open(**prof) as dst:
                dst.write(data)
                dst.write_mask(mask)
            return mf.read()

    out = agg.merge_tiles([tile(10), tile(16)])
    with MemoryFile(out) as mf, mf.open() as ds:
        valid = nd.valid_mask(ds)
    assert valid[:, 0].tolist() == [r != 3 for r in range(12)]
    assert valid[:, 23].tolist() == [r != 3 for r in range(12)]


def _merge_tile(x, *, mask_rows=(), nodata=None, nodata_rows=()):
    """A 12x12 uint8 tile of 50s at ``x`` degrees: ``mask_rows`` are hidden by an own mask band,
    and ``nodata_rows`` hold the declared ``nodata`` value."""
    data = np.full((1, 12, 12), 50, dtype="uint8")
    if nodata_rows:
        data[:, list(nodata_rows), :] = nodata
    prof = dict(
        driver="GTiff",
        width=12,
        height=12,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(x, 50, 0.5, 0.5),
        nodata=nodata,
    )
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True), MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data)
            if mask_rows:
                mask = np.full((12, 12), 255, dtype="uint8")
                mask[list(mask_rows), :] = 0
                dst.write_mask(mask)
        return mf.read()


@pytest.mark.parametrize("streaming", [False, True], ids=["in-ram", "streaming"])
@pytest.mark.parametrize("masked_x", [10, 16], ids=["mask-left", "mask-right"])
def test_a_merge_keeps_nodata_pixels_invalid_beside_a_tile_with_an_own_mask(
    monkeypatch, streaming, masked_x
):
    """A tile whose invalid pixels are NoData, merged with a tile that has an own mask, stays
    invalid where its source is: the mosaic's mask is the union of what each tile says is valid
    (``read_masks``), not a mask that calls every pixel of a tile without its own mask valid. Both
    paths give the validity of the sources, whichever tile stands first in byte order.
    """
    if streaming:
        monkeypatch.setattr(agg, "decoded_budget_bytes", lambda s: 1)
    other_x = 26 - masked_x
    masked = _merge_tile(masked_x, mask_rows=(3,))
    plain = _merge_tile(other_x, nodata=9, nodata_rows=(7,))
    out = agg.merge_tiles([masked, plain])
    with MemoryFile(out) as mf, mf.open() as ds:
        valid = nd.valid_mask(ds)
    masked_col, plain_col = (0, 23) if masked_x == 10 else (23, 0)
    assert valid[:, masked_col].tolist() == [r != 3 for r in range(12)]
    assert valid[:, plain_col].tolist() == [r != 7 for r in range(12)]


def _two_crs_tiles(name):
    """A 4326 tile and, beside it, the same layout in 3857 (the 3857 one first: it is the
    reference CRS the other is reprojected to)."""

    def tile(x):
        return al.row_coded(
            name,
            height=H,
            masked_rows=MASKED,
            transparent_rows=TRANSPARENT,
            transform=from_origin(x, 50, 0.5, 0.5),
        )

    with MemoryFile(tile(13)) as mf, mf.open() as ds:
        in_3857 = warp.reproject_to_srid(ds, 3857)
    return [in_3857, tile(10)]


def _assert_rows_valid_as_their_source(raster_bytes, name):
    """Every output pixel names the source row it came from, and is valid exactly when that
    row is; the labels are the layout's."""
    alpha = al.LAYOUTS[name][1]
    with MemoryFile(raster_bytes) as mf, mf.open() as ds:
        assert ds.crs.to_epsg() == 3857
        assert al.labelled_alpha(raster_bytes) == ([ds.count - 1] if alpha else [])
        coded = ds.read(1).astype("int64") - 100
        valid = nd.valid_mask(ds)
    covered = coded >= 0
    assert covered.any()
    want = np.isin(coded, _want_rows(name))
    assert (
        valid[covered] == want[covered]
    ).all(), f"{int((valid[covered] != want[covered]).sum())} pixels differ in validity"


def _crs_tile(srid, x, width, extra_entries=False):
    """A 1-band uint8 tile of ``width`` columns in EPSG:``srid``: 4326 at 0.5 degrees or 3857 at
    50 km, side by side near 10E, 50N."""
    from rasterio.warp import transform

    if srid == 4326:
        tf = from_origin(x, 50, 0.5, 0.5)
    else:
        mx, my = transform("EPSG:4326", "EPSG:3857", [x], [50])
        tf = from_origin(mx[0], my[0], 50000.0, 50000.0)
    prof = dict(
        driver="GTiff",
        width=width,
        height=12,
        count=1,
        dtype="uint8",
        crs=f"EPSG:{srid}",
        transform=tf,
    )
    if extra_entries:
        prof["nodata"] = 0
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(np.full((1, 12, width), 7, dtype="uint8"))
            if extra_entries:
                dst.update_tags(note="x")
        return mf.read()


def _group_sorted_with_4326_first():
    """A 4326 tile and a 3857 tile whose byte order puts the 4326 one first: the order ``merge_tiles``
    folds them in, and the one in which the units of its size gate used to be the wrong ones. The
    first byte that differs is the number of entries in the tile's directory (15 for the 3857 tile
    and 16 for the 4326 one, whose CRS takes more keys), so the 3857 tile gets two more entries: a
    NoData and a tag."""
    a = _crs_tile(4326, 10, 12)
    b = _crs_tile(3857, 16, 12, extra_entries=True)
    assert sorted([a, b])[0] == a
    return [a, b]


def test_the_size_gate_of_a_two_crs_merge_is_in_the_reference_crs(monkeypatch):
    """The reference CRS is the one with the smallest EPSG code (3857 here), and the gate measures the
    mosaic there, whichever tile stands first in byte order: it used to mix degrees and metres, call a
    handful of pixels a mosaic of 10^13 and stream it."""
    group = _group_sorted_with_4326_first()
    streamed = []
    monkeypatch.setattr(
        agg, "_merge_tiles_streaming", lambda rasters: streamed.append(rasters)
    )
    out = agg.merge_tiles(group)
    assert not streamed, "a few pixels were sent to the streaming merge"
    with MemoryFile(out) as mf, mf.open() as ds:
        assert ds.crs.to_epsg() == 3857 and ds.count == 1


def test_the_streamed_merge_uses_the_pixel_size_of_a_tile_in_the_reference_crs(
    monkeypatch,
):
    """Streamed, the tile in another CRS is reprojected onto the pixel size of the reference tile, in
    the reference CRS's units; it used to take the size of the first tile whatever its CRS
    (0.5 degrees read as 0.5 metres)."""
    a, b = _group_sorted_with_4326_first()  # the 4326 tile first
    seen = []

    class Stop(Exception):
        pass

    def spy(src, dst_crs, target_res=None):
        seen.append(target_res)
        raise Stop

    monkeypatch.setattr(agg, "_reproject_dataset", spy)
    with pytest.raises(Stop):
        agg._merge_tiles_streaming([a, b])
    assert seen == [(50000.0, 50000.0)]


def _two_crs_group(first):
    """The 4326 + 3857 pair, a pixel or two apart (so that no pixel is shared and the compositing order
    cannot show), in the byte order that puts the ``first`` CRS's tile first."""
    a = _crs_tile(4326, 10, 12)
    b = _crs_tile(3857, 17, 12, extra_entries=(first == "4326"))
    assert sorted([a, b])[0] == (a if first == "4326" else b)
    return [a, b]


def _merged_grid(group):
    """``(pixel size, shape, origin, pixels, covered)`` of the merge of *group*: the tiles hold 7s,
    the rest of the mosaic 0."""
    with MemoryFile(agg.merge_tiles(group)) as mf, mf.open() as ds:
        assert ds.crs.to_epsg() == 3857
        t = ds.transform
        return (
            (abs(t.a), abs(t.e)),
            ds.shape,
            (t.c, t.f),
            ds.read(1),
            ds.read(1) == 7,
        )


@pytest.mark.parametrize("streaming", [False, True], ids=["in-ram", "streaming"])
@pytest.mark.parametrize("first", ["3857", "4326"])
def test_a_two_crs_merge_has_the_pixel_size_of_the_tile_in_the_reference_crs(
    monkeypatch, first, streaming
):
    """The pixel size is the reference tile's (3857, 50 km), whichever tile stands first in byte
    order: the tile in the other CRS is reprojected onto it, and the merge takes it, on both paths.
    The mosaic used to take the size of the first tile after reprojection (70 km when the 4326 tile
    stood first)."""
    if streaming:
        monkeypatch.setattr(agg, "decoded_budget_bytes", lambda s: 1)
    res, _, _, pixels, covered = _merged_grid(_two_crs_group(first))
    assert res == (50000.0, 50000.0)
    assert covered.any() and set(np.unique(pixels)) <= {0, 7}


@pytest.mark.parametrize("streaming", [False, True], ids=["in-ram", "streaming"])
def test_a_two_crs_merge_does_not_depend_on_the_byte_order(monkeypatch, streaming):
    """The same two tiles, in either byte order, merge to the same grid and the same pixels."""
    if streaming:
        monkeypatch.setattr(agg, "decoded_budget_bytes", lambda s: 1)
    a = _merged_grid(_two_crs_group("3857"))
    b = _merged_grid(_two_crs_group("4326"))
    assert a[0] == b[0] and a[1] == b[1] and a[2] == b[2]
    assert (a[4] == b[4]).all() and (a[3][a[4]] == b[3][b[4]]).all()


def test_the_in_ram_and_the_streamed_two_crs_merge_agree(monkeypatch):
    """The pixel size, the size of the mosaic, its origin and the number of pixels the tiles cover
    are those of the streamed merge. (Where a tile that is not on the reference grid lands within
    its pixel is each merge's own rounding, not asserted.)"""
    group = _two_crs_group("4326")
    in_ram = _merged_grid(group)
    monkeypatch.setattr(agg, "decoded_budget_bytes", lambda s: 1)
    streamed = _merged_grid(group)
    assert in_ram[0] == streamed[0] and in_ram[1] == streamed[1]
    assert in_ram[2] == pytest.approx(
        streamed[2], rel=1e-12
    )  # a COG writes the origin as text
    assert in_ram[4].sum() == streamed[4].sum()


@pytest.mark.parametrize("name", ["rgb_nir", "rgba", "gray_x_alpha"])
def test_the_reprojected_tile_of_a_merge_keeps_the_alpha_band_and_the_mask(name):
    """``_reproject_dataset``, which brings a tile of another CRS to the reference one."""
    _, in_4326 = _two_crs_tiles(name)
    with MemoryFile(in_4326) as mf, mf.open() as src:
        rep_mf, rep = agg._reproject_dataset(src, "EPSG:3857")
        try:
            # the warp turns the masked pixels into valid zeros: the mask band is what keeps them out
            assert nd.has_own_mask(rep), "the reprojected tile lost its own mask band"
            raster = _comp_bytes(rep)
        finally:
            rep.close()
            rep_mf.close()
    _assert_rows_valid_as_their_source(raster, name)


def _comp_bytes(ds):
    """The dataset's pixels, labels and own mask as GTiff bytes."""
    mask = ds.read_masks(1) if nd.has_own_mask(ds) else None
    return write_gtiff(ds.profile, ds.read(), alpha_like=ds.colorinterp, mask=mask)


@pytest.mark.parametrize("name", ["rgb_nir", "rgba", "gray_x_alpha"])
def test_the_streaming_merge_keeps_the_alpha_band_and_the_mask_of_a_reprojected_tile(
    name, monkeypatch
):
    tiles = _two_crs_tiles(name)
    scratch_masks = (
        []
    )  # whether each tile file written for the VRT has its own mask band
    real = agg._build_mosaic_vrt

    def spy(tile_paths, out_dir, *args, **kwargs):
        for path in tile_paths:
            with rasterio.open(path) as d:
                scratch_masks.append(nd.has_own_mask(d))
        return real(tile_paths, out_dir, *args, **kwargs)

    monkeypatch.setattr(agg, "_build_mosaic_vrt", spy)
    _assert_rows_valid_as_their_source(agg._merge_tiles_streaming(tiles), name)
    assert scratch_masks == [
        True,
        True,
    ], "a tile written for the streamed merge lost its mask"


# --- chips copy the colour interpretation of every band --------------------------------------


def _chip_labels_source(case, name, dtype):
    from rasterio.transform import from_bounds

    _, crs, bounds, _ = _ALPHA_CASES[case]
    n = _FILL_N
    count = len(al.LAYOUTS[name][0])
    p = np.arange(n * n, dtype="int64").reshape(n, n)
    data = np.stack([((p * (b + 1)) % 199 + 1).astype(dtype) for b in range(count)])
    if al.LAYOUTS[name][1]:
        data[-1] = 255
    return al.layout_bytes(
        name, dtype=dtype, data=data, crs=crs, transform=from_bounds(*bounds, n, n)
    )


def _expected_labels(name):
    """The layout's labels, an alpha label that is not on the last band being data."""
    labels = list(al.LAYOUTS[name][0])
    return [
        CI.undefined if c == CI.alpha and i != len(labels) - 1 else c
        for i, c in enumerate(labels)
    ]


@pytest.mark.parametrize(
    "assignment,coverage",
    [("centroid", "sparse"), ("centroid", "complete"), ("covering", "sparse")],
)
@pytest.mark.parametrize("case", ["h3", "bng_native"])
@pytest.mark.parametrize("dtype", ["uint8", "uint16"])
@pytest.mark.parametrize("name", list(al.LAYOUTS))
def test_tessellate_chips_copy_the_colour_interpretation_of_every_band(
    name, dtype, case, assignment, coverage
):
    """GDAL labels a new GTiff's bands from its band count and type (RGB for 3 bands, gray
    and undefined for 16-bit): the chips are labelled as the source is, band by band."""
    chips = _chips_of(
        _chip_labels_source(case, name, dtype), case, assignment, coverage
    )
    assert chips
    for _, cb in chips:
        with MemoryFile(cb) as mf, mf.open() as c:
            assert list(c.colorinterp) == _expected_labels(
                name
            ), f"{name} {dtype} {case} {assignment}"


@pytest.mark.parametrize("name", list(al.LAYOUTS))
def test_the_empty_chip_of_a_covered_cell_copies_the_colour_interpretation(name):
    """The 1x1 chip of a covered-but-empty cell (centroid + complete) is labelled as the
    source is, like the chips with pixels."""
    _, _, (west, south, east, north), res = _ALPHA_CASES["h3"]
    cell = h3.latlng_to_cell((south + north) / 2, (west + east) / 2, res)
    with MemoryFile(_chip_labels_source("h3", name, "uint8")) as mf, mf.open() as ds:
        raw = T._build_empty_chip(ds, cell, "h3")
    with MemoryFile(raw) as mf, mf.open() as c:
        assert list(c.colorinterp) == _expected_labels(name)


# --- the percentile stretch ------------------------------------------------------------------


def _stretch_source(name):
    """Layout ``name`` whose data bands hold ``10 + row`` over the visible rows and 250 over the
    rows that are masked (3-4) or, for an alpha layout, transparent (8)."""
    count = len(al.LAYOUTS[name][0])
    rows = np.arange(H)
    hidden = np.isin(
        rows, list(MASKED) + (list(TRANSPARENT) if al.LAYOUTS[name][1] else [])
    )
    col = np.where(hidden, 250, 10 + rows).astype("uint8")
    data = np.broadcast_to(col[None, :, None], (count, H, 12)).copy()
    if al.LAYOUTS[name][1]:
        data[-1] = np.where(np.isin(rows, list(TRANSPARENT)), 0, 255)[:, None]
    mask = np.full((H, 12), 255, dtype="uint8")
    mask[list(MASKED), :] = 0
    return al.layout_bytes(name, data=data, mask=mask), hidden


@pytest.mark.parametrize("name", ["gray_x_alpha", "rgba", "rgb_nir"])
def test_percentile_stretch_counts_only_the_pixels_valid_for_the_band(name):
    """The percentiles are over the opaque, unmasked pixels: the visible rows stretch over their own
    range, to 255 at the top. Counted, the 250s of the hidden rows would end the range there.
    """
    raw, hidden = _stretch_source(name)
    with MemoryFile(raw) as mf, mf.open() as ds:
        out = stretch.percentile_stretch(ds, 0.0, 100.0)
    with MemoryFile(out) as mf, mf.open() as o:
        band1 = o.read(1)
    visible = ~hidden
    assert band1[visible].max() == 255 and band1[visible].min() == 0
    assert (
        band1[hidden] == 0
    ).all(), "a pixel invalid for the band is written 0, as heavy does"


# --- focal filters, polygonize and isoband: transparent pixels are skipped like NoData ---------


def _striped_alpha_raster(name):
    """6x6 of layout ``name`` (1 degree pixels): data bands hold 5 over rows 0-1 and 4-5 and 99 over the
    transparent rows 2-3; the alpha band (an alpha layout's last band) is 255 over the opaque rows, 0 over
    the transparent ones. A layout without alpha has no transparent rows, but a mask over rows 2-3.
    """
    count = len(al.LAYOUTS[name][0])
    data = np.full((count, 6, 6), 5, dtype="uint8")
    data[:, 2:4, :] = 99
    mask = None
    if al.LAYOUTS[name][1]:
        data[-1] = 255
        data[-1, 2:4, :] = 0
    else:
        mask = np.full((6, 6), 255, dtype="uint8")
        mask[2:4, :] = 0
    return al.layout_bytes(
        name, data=data, mask=mask, transform=from_origin(0, 6, 1, 1)
    )


@pytest.mark.parametrize("name", ["gray_x_alpha", "rgba", "rgb_nir"])
def test_a_focal_max_filter_skips_transparent_and_masked_neighbours(name):
    """The 99s of the hidden rows are not neighbours: the max of a window is over the pixels valid for the
    band, so every pixel next to them (and, with valid neighbours left, they themselves) stays 5.
    """
    with MemoryFile(_striped_alpha_raster(name)) as mf, mf.open() as ds:
        out = focal.filt(ds, 3, "max")
    with MemoryFile(out) as mf, mf.open() as o:
        band1 = o.read(1)
    assert (band1 == 5).all(), band1.tolist()


@pytest.mark.parametrize("name", ["gray_x_alpha", "rgba", "rgb_nir"])
def test_a_convolution_skips_transparent_and_masked_neighbours(name):
    with MemoryFile(_striped_alpha_raster(name)) as mf, mf.open() as ds:
        out = focal.convolve(ds, np.ones((3, 3)))
    with MemoryFile(out) as mf, mf.open() as o:
        band1 = o.read(1)
    # row 1: its window holds rows 0-2: six valid 5s from rows 0 and 1 (edge columns shrink the sum to 4*... by
    # replicate-edge), none from the hidden row 2. The hidden 99s add nothing anywhere.
    assert band1.max() <= 9 * 5, band1.tolist()


@pytest.mark.parametrize("name", ["gray_x_alpha", "rgba", "rgb_nir"])
def test_polygonize_and_isoband_leave_out_transparent_and_masked_pixels(name):
    import shapely.wkb

    with MemoryFile(_striped_alpha_raster(name)) as mf, mf.open() as ds:
        shapes = features.polygonize(ds, 1, 4)
        bands = features.isoband(ds, [0, 10, 200], 1)
    assert sum(shapely.wkb.loads(g).area for g, _ in shapes) == 24.0
    assert {v for _, v in shapes} == {5.0}
    assert sum(shapely.wkb.loads(g).area for g, *_ in bands) == 24.0
    assert {b for _, b, *_ in bands} == {0}
