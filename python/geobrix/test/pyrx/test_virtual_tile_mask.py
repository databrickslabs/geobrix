"""A light virtual tile keeps its source's own (per-dataset) mask band when it is read.

``rst_fromfile`` returns virtual tiles by default, so a masked source (JPEG/YCbCr
imagery often carries an internal mask band) is read through ``open_tile``'s window
paths. Those paths wrote the window without the mask, so every masked pixel came back
valid in every light operation that starts from a lazily read tile.

Layout (the canonical-module matrix once, one wiring test per consumer):

  * the window read, the pending nodata / band-select instructions, the warp and
    clip variants and the materializers: ``open_tile`` results against the source
    read directly or the eager operation on the same bytes;
  * consumers (``rst_initnodata``, covering, BNG tessellate, raster-to-grid) run
    through Spark from ``rst_fromfile`` and from ``rst_fromcontent``: same output,
    and no masked source pixel in it.
"""

from pathlib import Path

import numpy as np
import pytest
import rasterio
import shapely
from rasterio.enums import MaskFlags
from rasterio.transform import from_origin
from rasterio.windows import Window
from shapely.geometry import box

from databricks.labs.gbx.pyrx.core import edit
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

from . import _tessellate_parity as tp

N = 40
MASKED_ROWS = (10, 11)
MARKER = 9999  # the pixels under the mask: not a ramp value, not 7, not 65535
ORIGIN = (1.0, 52.6)
PX = 0.005


def _values(count=1):
    """Ramp 1..1600; row 3 holds 65535 (the uint16 fallback NoData); the masked rows
    hold MARKER, so a masked pixel read as data shows up as 9999."""
    vals = (np.arange(N * N) + 1).reshape(N, N).astype("uint16")
    vals[3, :] = 65535
    vals[list(MASKED_ROWS), :] = MARKER
    return np.stack([vals + b for b in range(count)]) if count > 1 else vals[None]


def _profile(*, count=1, nodata=None, crs="EPSG:4326", origin=ORIGIN, px=PX):
    return dict(
        driver="GTiff",
        width=N,
        height=N,
        count=count,
        dtype="uint16",
        crs=crs,
        transform=from_origin(origin[0], origin[1], px, px),
        nodata=nodata,
    )


def write_masked(path, *, count=1, nodata=None, **kw):
    """uint16 GTiff with an internal per-dataset mask over ``MASKED_ROWS``."""
    mask = np.full((N, N), 255, dtype="uint8")
    mask[list(MASKED_ROWS), :] = 0
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
        with rasterio.open(
            path, "w", **_profile(count=count, nodata=nodata, **kw)
        ) as dst:
            dst.write(_values(count))
            dst.write_mask(mask)
    with rasterio.open(path) as chk:
        assert chk.mask_flag_enums[0] == [MaskFlags.per_dataset], "fixture needs a mask"
    return str(path)


def virtual(path, window=(0, 0, N, N), **kw):
    return VirtualTile(cellid=0, path=str(path), window=window, **kw)


def source_view(path, window=None):
    """(data, valid) of the source file read directly, optionally one window."""
    with rasterio.open(path) as src:
        win = None if window is None else Window(*window)
        return src.read(window=win), src.read_masks(window=win) != 0


def read_tile(tile):
    with ot.open_tile(tile) as ds:
        return ds.read(), ds.read_masks() != 0, ds.nodata


def decode(raster_bytes):
    with rasterio.MemoryFile(raster_bytes) as mf, mf.open() as ds:
        return ds.read(), ds.read_masks() != 0, ds.nodata


def assert_same_pixels(got, want, label):
    """Same validity everywhere, same values where valid."""
    (g_data, g_valid), (w_data, w_valid) = got, want
    assert g_valid.shape == w_valid.shape, f"{label}: shape {g_valid.shape}"
    assert (
        g_valid == w_valid
    ).all(), f"{label}: {int((g_valid != w_valid).sum())} pixels differ in validity"
    assert (g_data[g_valid] == w_data[w_valid]).all(), f"{label}: values differ"


# ---------------------------------------------------------------------------
# the window read
# ---------------------------------------------------------------------------

WINDOWS = [
    None,
    (0, 0, N, N),
    (0, 5, N, 20),  # spans the masked rows
    (5, 8, 20, 10),  # clips them at the top
    (0, 20, N, 10),  # misses them
]


@pytest.mark.parametrize("nodata", [None, 7.0])
@pytest.mark.parametrize("window", WINDOWS, ids=str)
def test_window_read_keeps_the_mask(tmp_path, nodata, window):
    p = write_masked(tmp_path / "m.tif", nodata=nodata)
    data, valid, _ = read_tile(virtual(p, window=window))
    want = source_view(p, None if window is None else window)
    assert_same_pixels((data, valid), want, f"window={window} nodata={nodata}")


def test_window_read_matches_a_tile_that_starts_from_bytes(tmp_path):
    p = write_masked(tmp_path / "m.tif")
    from_bytes = VirtualTile(cellid=0, raster=Path(p).read_bytes())
    data, valid, _ = read_tile(virtual(p))
    b_data, b_valid, _ = read_tile(from_bytes)
    assert_same_pixels((data, valid), (b_data, b_valid), "virtual vs bytes")
    assert not valid[:, list(MASKED_ROWS), :].any()


def test_a_source_without_a_mask_is_read_as_before(tmp_path):
    p = str(tmp_path / "plain.tif")
    with rasterio.open(p, "w", **_profile(nodata=7.0)) as dst:
        dst.write(_values())
    with ot.open_tile(virtual(p)) as ds:
        assert ds.mask_flag_enums[0] == [MaskFlags.nodata]
        assert (ds.read() == _values()).all()


def test_an_external_mask_file_is_kept(tmp_path):
    """A mask kept next to the file (``m.tif.msk``) is a per-dataset mask too."""
    p = str(tmp_path / "ext.tif")
    mask = np.full((N, N), 255, dtype="uint8")
    mask[list(MASKED_ROWS), :] = 0
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=False):
        with rasterio.open(p, "w", **_profile()) as dst:
            dst.write(_values())
            dst.write_mask(mask)
    assert (tmp_path / "ext.tif.msk").exists(), "fixture needs an external mask"
    _, valid, _ = read_tile(virtual(p))
    assert not valid[:, list(MASKED_ROWS), :].any()
    assert valid.sum() == N * N - len(MASKED_ROWS) * N


def test_an_alpha_band_is_left_to_the_alpha_band(tmp_path):
    """An alpha source is read as before: the alpha band stays a band (no mask band is
    added) and decides which pixels are valid."""
    from databricks.labs.gbx.pyrx.core import _nodata as nd

    p = str(tmp_path / "rgba.tif")
    data = np.full((4, N, N), 50, dtype="uint8")
    data[3] = 255
    data[3, 5, :] = 0  # transparent row, colour bands not 0
    prof = {**_profile(count=4), "dtype": "uint8"}
    with rasterio.open(p, "w", **prof) as dst:
        dst.write(data)
    with rasterio.open(p) as src:
        assert nd.has_alpha(src) and not nd.has_own_mask(src)
        want = nd.valid_mask(src)
    with ot.open_tile(virtual(p, window=(0, 2, N, 20))) as ds:
        assert ds.count == 4 and nd.has_alpha(ds) and not nd.has_own_mask(ds)
        assert (ds.read() == data[:, 2:22, :]).all()
        assert (nd.valid_mask(ds) == want[2:22, :]).all()
    assert not want[5].any() and want[6].all()


@pytest.mark.parametrize("nodata", [None, 7.0])
def test_pending_initnodata_matches_eager_initnodata(tmp_path, nodata):
    """The deferred rst_initnodata of a virtual tile equals the eager one on its bytes:
    the NoData is declared and the masked pixels take it."""
    p = write_masked(tmp_path / "m.tif", nodata=nodata)
    with rasterio.open(p) as src:
        eager = decode(edit.init_nodata(src))
    tile = virtual(p, metadata={ot.PENDING_NODATA: ot.PENDING_NODATA_AUTO})
    data, valid, got_nodata = read_tile(tile)
    assert got_nodata == eager[2] == (65535.0 if nodata is None else nodata)
    assert_same_pixels((data, valid), (eager[0], eager[1]), f"nodata={nodata}")
    assert not valid[:, list(MASKED_ROWS), :].any()
    assert (data[:, list(MASKED_ROWS), :] == got_nodata).all()


def test_pending_nodata_that_does_not_fit_still_keeps_the_mask(tmp_path):
    """-9999 does not fit uint16, so no NoData is stamped; the mask is the only guard."""
    p = write_masked(tmp_path / "m.tif")
    tile = virtual(p, metadata={ot.PENDING_NODATA: "-9999"})
    data, valid, got_nodata = read_tile(tile)
    assert got_nodata is None
    assert not valid[:, list(MASKED_ROWS), :].any()
    assert valid.sum() == N * N - len(MASKED_ROWS) * N


def test_pending_band_select_keeps_the_mask(tmp_path):
    p = write_masked(tmp_path / "m.tif", count=2)
    tile = virtual(p, metadata={ot.PENDING_BANDS: "2"})
    data, valid, _ = read_tile(tile)
    assert data.shape[0] == 1 and not valid[:, list(MASKED_ROWS), :].any()
    assert valid.sum() == N * N - len(MASKED_ROWS) * N


# ---------------------------------------------------------------------------
# the warp variants: a tile with a target CRS
# ---------------------------------------------------------------------------


def _index_file(path, nodata):
    """Same grid as the masked source, no mask: pixel (r, c) holds r + 100, so the warp
    of this file names the source row of every output pixel."""
    idx = np.repeat((np.arange(N) + 100)[:, None], N, axis=1).astype("uint16")
    with rasterio.open(path, "w", **_profile(nodata=nodata)) as dst:
        dst.write(idx, 1)
    return str(path)


@pytest.mark.parametrize("nodata", [None, 7.0])
@pytest.mark.parametrize("target", ["EPSG:3857", "ESRI:54008"])
def test_warp_variants_keep_the_mask(tmp_path, target, nodata):
    """The warp moves the mask with the pixels: the output pixels that come from masked
    source rows are invalid, and nothing else is (outside the footprint the warp fills
    with the NoData, or with plain zeros when the source declares none)."""
    masked = write_masked(tmp_path / "m.tif", nodata=nodata)
    index = _index_file(tmp_path / "idx.tif", nodata)
    data, valid, _ = read_tile(virtual(masked, crs=target))
    idx, _, _ = read_tile(virtual(index, crs=target))
    fill = 0 if nodata is None else nodata
    covered = idx[0] != fill
    from_masked = covered & np.isin(idx[0].astype("int64") - 100, MASKED_ROWS)
    assert from_masked.any(), "fixture: the warp must keep some masked source pixels"
    # a declared NoData also takes the (unmasked) ramp pixel equal to it
    want = (
        ~from_masked if nodata is None else covered & ~from_masked & (data[0] != fill)
    )
    assert (valid[0] == want).all(), (
        f"{target} nodata={nodata}: masked pixels valid: "
        f"{int((valid[0] & from_masked).sum())}, valid pixels lost: "
        f"{int((~valid[0] & want).sum())}"
    )


def test_warp_to_the_same_crs_is_the_plain_read(tmp_path):
    p = write_masked(tmp_path / "m.tif")
    _, valid, _ = read_tile(virtual(p, crs="EPSG:4326"))
    assert not valid[:, list(MASKED_ROWS), :].any()


# ---------------------------------------------------------------------------
# the clip variant: a tile with a cutline
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("nodata", [None, 7.0])
def test_clip_variant_equals_the_eager_clip(tmp_path, nodata):
    p = write_masked(tmp_path / "m.tif", nodata=nodata)
    x0, y0 = ORIGIN
    cut = box(x0 + 5 * PX, y0 - 25 * PX, x0 + 30 * PX, y0 - 5 * PX)
    wkb = shapely.to_wkb(cut)
    with rasterio.open(p) as src:
        eager = decode(edit.clip_to_geom(src, cut))
    got = read_tile(virtual(p, clip_polygon=wkb, clip_crs="EPSG:4326"))
    assert (
        got[0] == eager[0]
    ).all(), "clipped pixels differ from the eager clip of the bytes"
    assert (got[1] == eager[1]).all(), "clipped validity differs"
    fill = 0 if nodata is None else nodata
    assert (got[0] == MARKER).sum() == 0, "a masked pixel survived the clip"
    assert (got[0] == fill).any(), "the clip left no fill where the mask was"


# ---------------------------------------------------------------------------
# the materializers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("nodata", [None, 7.0])
def test_materializers_keep_the_mask(tmp_path, nodata):
    p = write_masked(tmp_path / "m.tif", nodata=nodata)
    vt = virtual(p)
    want = source_view(p)
    for label, raster in (
        ("materialize_to_bytes", ot.materialize_to_bytes(vt).raster),
        ("_tile_to_bytes", ot._tile_to_bytes(vt)),
        ("_windowed_materialize_bytes", ot._windowed_materialize_bytes(vt)),
    ):
        data, valid, _ = decode(raster)
        assert_same_pixels((data, valid), want, f"{label} nodata={nodata}")


def test_streamed_materialization_equals_the_in_memory_one(tmp_path):
    """The block-streaming path writes the same pixels, validity and NoData."""
    p = write_masked(tmp_path / "m.tif")
    for md in ({}, {ot.PENDING_NODATA: ot.PENDING_NODATA_AUTO}):
        vt = virtual(p, window=(0, 4, N, 20), metadata=md)
        a = decode(ot.materialize_to_bytes(vt).raster)
        b = decode(ot._windowed_materialize_bytes(vt))
        assert a[2] == b[2], f"{md}: NoData {a[2]} vs {b[2]}"
        assert_same_pixels((a[0], a[1]), (b[0], b[1]), f"streamed {md}")


# ---------------------------------------------------------------------------
# consumers: the same output from rst_fromfile (virtual) and rst_fromcontent
# ---------------------------------------------------------------------------


def _tile_df(spark, path, *, virtual_start):
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    if virtual_start:
        return spark.createDataFrame([(str(path),)], ["path"]).select(
            prx.rst_fromfile("path").alias("tile")
        )
    raw = Path(path).read_bytes()
    return spark.createDataFrame([(bytearray(raw),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )


def test_rst_initnodata_from_a_virtual_tile(spark, tmp_path):
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    p = write_masked(tmp_path / "m.tif")
    got = {}
    for start in (True, False):
        df = _tile_df(spark, p, virtual_start=start)
        row = df.select(
            prx.rst_pixelcount(prx.rst_initnodata("tile")).alias("n"),
            prx.rst_max(prx.rst_initnodata("tile")).alias("mx"),
        ).collect()[0]
        got[start] = (list(row["n"]), list(row["mx"]))
    # 1600 pixels - 80 masked - 40 real 65535s (the NoData it stamps)
    assert got[False][0] == [N * N - 80 - 40]
    assert got[True] == got[False], f"virtual {got[True]} vs bytes {got[False]}"
    assert got[True][1][0] < MARKER


def _chips(spark, path, *, virtual_start, sql_name, resolution, assignment, coverage):
    raw = Path(path).read_bytes()
    raster = tp.staged(raw, path) if virtual_start else raw
    return tp.light_chips(
        spark,
        raster,
        sql_name,
        resolution,
        assignment,
        coverage,
        lambda cid: cid,
    )


@pytest.mark.parametrize(
    "case,sql_name,resolution,assignment",
    [
        ("covering", "gbx_rst_h3_tessellate", 7, "covering"),
        ("bng", "gbx_rst_bng_tessellate", 3, "centroid"),
    ],
)
def test_tessellate_from_a_virtual_tile(
    spark, tmp_path, case, sql_name, resolution, assignment
):
    """Covering (H3, the source's own CRS) and BNG (warped to EPSG:27700): the chips of
    a virtual tile hold the same valid pixels as the chips of its bytes, and none of
    them is a masked source pixel."""
    p = write_masked(tmp_path / "m.tif")
    chips = {
        start: _chips(
            spark,
            p,
            virtual_start=start,
            sql_name=sql_name,
            resolution=resolution,
            assignment=assignment,
            coverage="sparse",
        )
        for start in (True, False)
    }
    assert chips[False], f"{case}: the bytes tile emitted no chips"
    assert not tp.error_rows(chips[True]), f"{case}: virtual start gave error rows"
    for start, rows in chips.items():
        values = []
        for _, b, _ in rows:
            with rasterio.MemoryFile(b) as mf, mf.open() as ds:
                values.append(ds.read(1)[ds.read_masks(1) != 0])
        v = np.concatenate(values)
        assert v.size, f"{case} start={'virtual' if start else 'bytes'}: no valid pixel"
        assert MARKER not in v, (
            f"{case} start={'virtual' if start else 'bytes'}: "
            f"{int((v == MARKER).sum())} masked source pixels are valid"
        )
    got = sorted((cid, tuple(np.sort(_valid_values(b)))) for cid, b, _ in chips[True])
    want = sorted((cid, tuple(np.sort(_valid_values(b)))) for cid, b, _ in chips[False])
    assert got == want, f"{case}: virtual start emits different chips than bytes"


def _valid_values(raster_bytes):
    with rasterio.MemoryFile(raster_bytes) as mf, mf.open() as ds:
        return ds.read(1)[ds.read_masks(1) != 0]


@pytest.mark.parametrize("agg", ["count", "max"])
def test_rastertogrid_from_a_virtual_tile(spark, tmp_path, agg):
    """BNG raster-to-grid of a 4326 source (warped to EPSG:27700): same cells and
    measures from a virtual tile as from its bytes, with no masked pixel aggregated."""
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    p = write_masked(tmp_path / "m.tif")
    out = {}
    for start in (True, False):
        _tile_df(spark, p, virtual_start=start).createOrReplaceTempView("_vm_grid")
        rows = spark.sql(
            f"SELECT t.cellID AS c, t.measure AS m FROM _vm_grid, "
            f"LATERAL gbx_rst_bng_rastertogrid{agg}(tile, 3, 'sparse', 'centroid') t"
        ).collect()
        out[start] = {r["c"]: r["m"] for r in rows}
    assert out[False], "the bytes tile aggregated nothing"
    assert out[True] == out[False], f"{agg}: virtual start differs from bytes start"
    if agg == "count":
        # at most the valid source pixels: the masked rows and the 65535 row are out
        assert sum(out[True].values()) <= N * N - 80 - 40
    else:
        assert max(out[True].values()) < MARKER, "a masked source pixel was aggregated"
