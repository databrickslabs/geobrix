"""Tests for grouped_tile_map — partition-scoped mapInPandas executor.

Local Spark (local[2]) always returns file_supported()=False, so all tests
exercise the fallback opener path over materialized tiles.  The FILE-stream
fast path is validated on-cluster.

The worker no longer calls file_supported() to decide the FILE path;
it keys off _file_ref column presence (has_fr_col) instead, avoiding the
getActiveSession()==None pitfall on Spark-Connect worker threads.
"""

import numpy as np
import pytest
from pyspark.sql.types import LongType, StructField, StructType

from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile
from databricks.labs.gbx.pyrx.grouped_exec import grouped_tile_map

_FLOAT32_ITEMSIZE = 4  # bytes per sample in the test GeoTIFFs (float32)


def _tile_df(spark, tile_bytes):
    """Create a 3-row DataFrame with materialized tiles (raster inline, path None)."""
    rows = [
        (VirtualTile.from_v1(cellid=i, raster=tile_bytes).to_row(),) for i in range(3)
    ]
    return spark.createDataFrame(
        rows, StructType([StructField("tile", V2_TILE_SCHEMA)])
    )


def test_grouped_map_matches_per_row_memsize(spark, gtiff_bytes):
    """grouped_tile_map result equals per-row memsize computation.

    gtiff_bytes = 4x3 float32 count=1; expected sz = 4*3*1*4 = 48 bytes.
    The fallback path (file_supported()=False locally) opens each materialized
    tile per-row via _open and applies core_fn on the open DatasetReader.
    """

    def core_fn(ds, cellid):
        itemsize = np.dtype(ds.dtypes[0]).itemsize
        return int(ds.count * ds.width * ds.height * itemsize)

    out = grouped_tile_map(
        _tile_df(spark, gtiff_bytes),
        core_fn,
        return_field=StructField("sz", LongType()),
    )
    vals = sorted(r["sz"] for r in out.collect())
    assert vals == [4 * 3 * 1 * 4] * 3, f"unexpected sizes: {vals}"


def test_grouped_map_output_schema_extends_input(spark, gtiff_bytes):
    """Output schema is input schema + return_field (no extra cast)."""
    df = _tile_df(spark, gtiff_bytes)
    out = grouped_tile_map(
        df,
        lambda ds, cellid: ds.count,
        return_field=StructField("band_count", LongType()),
    )
    assert out.schema.fieldNames() == ["tile", "band_count"]


def test_grouped_map_custom_tile_col(spark, gtiff_bytes):
    """tile_col kwarg selects the correct struct column by name."""
    rows = [
        (i, VirtualTile.from_v1(cellid=i, raster=gtiff_bytes).to_row())
        for i in range(2)
    ]
    df = spark.createDataFrame(
        rows,
        StructType(
            [
                StructField("id", LongType()),
                StructField("raster_tile", V2_TILE_SCHEMA),
            ]
        ),
    )
    out = grouped_tile_map(
        df,
        lambda ds, cellid: ds.count,
        return_field=StructField("nb", LongType()),
        tile_col="raster_tile",
    )
    nbs = [r["nb"] for r in out.collect()]
    assert all(nb == 1 for nb in nbs)


# ---------------------------------------------------------------------------
# _make_opener no longer calls worker-side file_supported()
# ---------------------------------------------------------------------------


def test_make_opener_returns_four_items_not_five():
    """_OpenerContext must expose fr_holder, open, close, weigh — no worker-side file_supported().

    Under the OLD implementation _make_opener called file_supported() on the
    worker and returned a 5-tuple (file_ok, fr_holder, opener, closer, weigher).
    The worker-side file_supported() call is gone; the opener/closer/
    weigher are now methods on _OpenerContext, and the FILE capability signal is
    the _file_ref column's presence (has_fr_col) set by the driver.
    """
    from databricks.labs.gbx.pyrx.grouped_exec import _OpenerContext

    ctx = _OpenerContext()
    assert (
        isinstance(ctx.fr_holder, list) and len(ctx.fr_holder) == 1
    ), "fr_holder must be a one-element list (the opener contract)"
    assert callable(ctx.open), "_OpenerContext.open must be callable"
    assert callable(ctx.close), "_OpenerContext.close must be callable"
    assert callable(ctx.weigh), "_OpenerContext.weigh must be callable"


# ---------------------------------------------------------------------------
# grouped executor robustness
# ---------------------------------------------------------------------------


def test_grouped_map_windowless_virtual_tile(spark, gtiff_bytes, tmp_path):
    """A windowless virtual tile (window=None) returns the full-source footprint.

    file_supported()=False locally so tiles take the fallback path.  The fallback
    must route windowless virtual tiles through open_header (not _open, which
    raises ValueError for window=None).  This matches per-row rst_memsize behavior.
    gtiff_bytes is 4x3 float32 count=1 → 4*3*1*4=48 bytes footprint.
    """
    tif = tmp_path / "whole.tif"
    tif.write_bytes(gtiff_bytes)

    vt = VirtualTile(cellid=7, path=str(tif), raster=None, window=None)
    df = spark.createDataFrame(
        [(vt.to_row(),)],
        StructType([StructField("tile", V2_TILE_SCHEMA)]),
    )

    def core_fn(ds, cellid):
        return int(ds.count * ds.width * ds.height * np.dtype(ds.dtypes[0]).itemsize)

    out = grouped_tile_map(df, core_fn, return_field=StructField("sz", LongType()))
    vals = [r["sz"] for r in out.collect()]
    assert vals == [
        4 * 3 * 1 * _FLOAT32_ITEMSIZE
    ], f"unexpected result for windowless virtual tile: {vals}"


# ---------------------------------------------------------------------------
# T9c: out_schema collision — return_field.name matches an input column
# ---------------------------------------------------------------------------


def test_grouped_map_out_col_collision_replaces_not_appends(spark, gtiff_bytes):
    """When return_field.name collides with an input column, out_schema REPLACES it.

    T9c root cause: rst_clip_grouped uses out_col='tile' by default, which
    collides with the input 'tile' column.  The pre-fix code did:
        out_schema = StructType(original_fields + [return_field])
    producing TWO 'tile' fields.  The _map function's .assign(**{out_name: ...})
    overwrites the pandas column (ONE 'tile' in the output pandas df), so Arrow
    sees one column against a schema declaring two — raising
    "not all nodes, buffers and variadicBufferCounts were consumed" on-cluster.

    Fix: filter out any field whose name matches return_field.name before appending.
    """
    df = _tile_df(spark, gtiff_bytes)  # schema has exactly one field: "tile"

    # Return a LongType column also named "tile" — the collision case.
    out = grouped_tile_map(
        df,
        lambda ds, cellid: ds.count,  # returns an int, typed as LongType
        return_field=StructField("tile", LongType()),
    )
    field_names = out.schema.fieldNames()
    assert field_names.count("tile") == 1, (
        f"Expected exactly one 'tile' field in output schema, got {field_names}. "
        f"T9c regression: duplicate field causes Arrow schema mismatch on-cluster."
    )
    # collect() must not raise (Arrow schema matches pandas output)
    rows = out.collect()
    assert len(rows) == 3
    # core_fn returns ds.count (1 band) cast to LongType
    assert all(r["tile"] == 1 for r in rows)


def test_grouped_map_unreadable_tile_degrades_gracefully(spark, gtiff_bytes, tmp_path):
    """An unreadable tile (bogus path) degrades to None; partition does NOT crash.

    The good tile in the same partition still computes its result correctly.
    gtiff_bytes is 4x3 float32 count=1 → 48 bytes footprint.
    """
    good = tmp_path / "good.tif"
    good.write_bytes(gtiff_bytes)

    rows = [
        (
            VirtualTile(
                cellid=0,
                path="/nonexistent/bogus.tif",
                raster=None,
                window=(0, 0, 4, 3),
            ).to_row(),
        ),
        (
            VirtualTile(
                cellid=1, path=str(good), raster=None, window=(0, 0, 4, 3)
            ).to_row(),
        ),
    ]
    df = spark.createDataFrame(rows, StructType([StructField("tile", V2_TILE_SCHEMA)]))

    def core_fn(ds, cellid):
        return int(ds.count * ds.width * ds.height * np.dtype(ds.dtypes[0]).itemsize)

    out = grouped_tile_map(df, core_fn, return_field=StructField("sz", LongType()))
    result = {r["tile"]["cellid"]: r["sz"] for r in out.collect()}

    assert result[0] is None, f"expected None for unreadable tile, got {result[0]}"
    assert (
        result[1] == 4 * 3 * 1 * _FLOAT32_ITEMSIZE
    ), f"readable tile wrong: {result[1]}"


# ---------------------------------------------------------------------------
# Header-vs-pixel view contract
# ---------------------------------------------------------------------------


def test_grouped_tile_map_pixel_view_allows_read(spark, gtiff_bytes):
    """view="pixels" hands core_fn a real DatasetReader whose .read(1) works.

    Uses the fallback path (materialized tiles, file_supported()=False locally).
    The fallback already yields a real DatasetReader, so view="pixels" passes it
    straight through — this test validates the kwarg is accepted and the path works.
    """

    def core(ds, cellid):
        return int(ds.read(1).sum())  # reads pixels

    out = grouped_tile_map(
        _tile_df(spark, gtiff_bytes),
        core,
        return_field=StructField("s", LongType()),
        view="pixels",
    )
    assert out.select("s").head()[0] is not None


# ---------------------------------------------------------------------------
# Tile size metadata: path_file_size / tile_size preference
# ---------------------------------------------------------------------------


def test_opener_context_has_size_holder():
    """_OpenerContext initializes a size_holder like fr_holder."""
    from databricks.labs.gbx.pyrx.grouped_exec import _OpenerContext

    ctx = _OpenerContext()
    assert (
        isinstance(ctx.size_holder, list) and len(ctx.size_holder) == 1
    ), "size_holder must be a one-element list (mirrors fr_holder)"
    assert ctx.size_holder[0] is None, "size_holder initial value must be None"


def test_weigh_prefers_metadata_size_over_fr_size():
    """When size_holder contains a metadata size, weigh() uses it (no fr.size call)."""
    from databricks.labs.gbx.pyrx.grouped_exec import _OpenerContext

    ctx = _OpenerContext()

    # Create a mock FileRef whose .size raises if called (proof it's not called)
    class CountingFileRef:
        def __init__(self):
            self.size_access_count = 0

        @property
        def size(self):
            self.size_access_count += 1
            raise RuntimeError("fr.size should not be called when metadata size is set")

    fr_mock = CountingFileRef()
    ctx.fr_holder[0] = fr_mock

    # Set metadata size (simulating what happens in _run_file_fast_path)
    ctx.size_holder[0] = 5_242_880  # 5 MiB

    # Mock src (only needed for id tracking)
    class MockDataset:
        pass

    src = MockDataset()

    # weigh() should use metadata size, not call fr.size
    weight = ctx.weigh(src, "dummy_uri")
    assert weight == 5_242_880, f"weigh() should use metadata size, got {weight}"
    assert fr_mock.size_access_count == 0, "fr.size should not have been called"


def test_weigh_falls_back_to_fr_size_when_metadata_absent():
    """When size_holder is None, weigh() falls back to fr.size."""
    from databricks.labs.gbx.pyrx.grouped_exec import _OpenerContext

    ctx = _OpenerContext()

    # Create a FileRef with a known size
    class FileRefWithSize:
        @property
        def size(self):
            return 10_485_760  # 10 MiB

    fr_mock = FileRefWithSize()
    ctx.fr_holder[0] = fr_mock
    ctx.size_holder[0] = None  # No metadata size

    # Mock src
    class MockDataset:
        pass

    src = MockDataset()

    # weigh() should fall back to fr.size
    weight = ctx.weigh(src, "dummy_uri")
    assert weight == 10_485_760, f"weigh() should fall back to fr.size, got {weight}"


def test_weigh_falls_back_to_nominal_when_all_absent():
    """When both metadata size and fr.size are absent, weigh() uses nominal."""
    from databricks.labs.gbx.pyrx.grouped_exec import (
        STREAM_NOMINAL_BYTES,
        _OpenerContext,
    )

    ctx = _OpenerContext()
    ctx.size_holder[0] = None  # No metadata size
    ctx.fr_holder[0] = None  # No FileRef

    # Mock src (not in _fuse_sources so it's not FUSE-weighted)
    class MockDataset:
        pass

    src = MockDataset()

    # weigh() should use nominal
    weight = ctx.weigh(src, "dummy_uri")
    assert weight == STREAM_NOMINAL_BYTES, f"weigh() should use nominal, got {weight}"


def test_grouped_tile_map_header_view_forbids_read(spark, gtiff_bytes):
    """view="header" (default) hands core_fn a _WindowHeaderView whose .read() raises.

    The view exposes header attrs (width, height, count, dtypes) but intentionally
    blocks pixel I/O.
    """

    def core(view, cellid):
        try:
            view.read(1)
            return -1  # should not reach here
        except Exception:
            return int(view.width)  # header attrs only

    out = grouped_tile_map(
        _tile_df(spark, gtiff_bytes),
        core,
        return_field=StructField("w", LongType()),
    )  # default view="header"
    assert out.select("w").head()[0] > 0


def test_grouped_tile_map_cellid_passed_to_core(spark, gtiff_bytes):
    """core_fn receives the cellid matching the tile's cellid field."""

    def core(ds, cellid):
        return int(cellid)

    out = grouped_tile_map(
        _tile_df(spark, gtiff_bytes),
        core,
        return_field=StructField("cid", LongType()),
    )
    result = {r["tile"]["cellid"]: r["cid"] for r in out.collect()}
    # Each row should return its own cellid (0, 1, 2 as created by _tile_df).
    for expected_cellid in [0, 1, 2]:
        assert result[expected_cellid] == expected_cellid, (
            f"cellid mismatch: tile cellid={expected_cellid}, "
            f"core_fn got {result[expected_cellid]}"
        )


# ---------------------------------------------------------------------------
# _make_opener weigher uses real FileRef.size (not STREAM_NOMINAL_BYTES)
# ---------------------------------------------------------------------------


def test_make_opener_weigher_uses_fileref_size():
    """_OpenerContext.weigh returns fr_holder[0].size (real bytes) for stream-opened entries.

    The old weigher always returned STREAM_NOMINAL_BYTES (16 MiB), so the LRU byte
    budget never fired for large files.  The fix: the weigher reads fr_holder[0].size
    for stream-opened entries (not in _fuse_sources) so large-file entries carry
    their real cost.
    """
    from databricks.labs.gbx.pyrx.grouped_exec import _OpenerContext

    ctx = _OpenerContext()

    class FakeSrc:
        pass

    src = FakeSrc()
    ctx.fr_holder[0] = type("FR", (), {"size": 500_000_000})()  # 500 MB FileRef
    assert (
        ctx.weigh(src, "uri") == 500_000_000
    ), "weigh must return fr_holder[0].size, not STREAM_NOMINAL_BYTES"


# ---------------------------------------------------------------------------
# Byte-identical regression lock on the materialized grouped read path
# ---------------------------------------------------------------------------


def test_grouped_tile_map_materialized_is_byte_identical(spark, gtiff_bytes):
    """Protected path: a materialized tile passed through grouped_tile_map with a
    pixel-read core_fn round-trips the pixels byte-identically.

    Uses the fallback opener path (file_supported()=False locally) over materialized
    tiles.  If this test FAILS, the composition changed byte output and must be
    fixed — do not weaken this test.
    """
    import rasterio
    from pyspark.sql.types import BinaryType, StructField

    def _read_pixels(ds, cellid):
        return bytearray(ds.read(1).tobytes())

    out = grouped_tile_map(
        _tile_df(spark, gtiff_bytes),
        _read_pixels,
        return_field=StructField("pixels", BinaryType()),
        view="pixels",
    ).collect()

    with rasterio.open(rasterio.io.MemoryFile(gtiff_bytes)) as ds:
        expected = ds.read(1).tobytes()

    assert all(
        bytes(r["pixels"]) == expected for r in out
    ), "grouped_tile_map materialized path is not byte-identical — composition changed"


# ---------------------------------------------------------------------------
# FILE fast path only reads the source for VIRTUAL tiles
# ---------------------------------------------------------------------------


class _SourceStubLRU:
    """LRU stand-in whose ``get`` opens the tile's SOURCE path.

    Mirrors what the real opener does with the FileRef on a cache miss: it
    streams the file at ``uri``. Any call here means the tile's source file, not
    its inline bytes, is about to be read.
    """

    def __init__(self):
        self.gets = []
        self._open = []

    def get(self, uri):
        import rasterio

        self.gets.append(uri)
        src = rasterio.open(uri)
        self._open.append(src)
        return src

    def close(self):
        for src in self._open:
            src.close()


def _dispatch_header(ds, cellid):
    return (ds.crs.to_epsg(), ds.width, ds.height)


@pytest.mark.parametrize(
    "metadata",
    [{}, {"pending_srid": "not-a-number"}],
    ids=["plain", "malformed-pending-key"],
)
def test_dispatch_materialized_tile_reads_its_bytes_not_its_path(tmp_path, metadata):
    """A materialized tile that keeps ``path`` as provenance is read from its bytes.

    Reader tiles, ``materialize=True`` outputs and whole-input retile outputs all
    carry the source ``path`` next to computed bytes. The bytes here differ from
    the path file (CRS and size), so a FILE fast path that followed the path would
    return the source's values. Pending instructions only concern a virtual tile, so a
    hand-built materialized tile with a malformed pending key is read, not nulled.
    """
    from databricks.labs.gbx.pyrx.grouped_exec import _dispatch_tile, _OpenerContext

    from .conftest import make_geotiff_bytes

    src = tmp_path / "source_4326.tif"
    src.write_bytes(make_geotiff_bytes(width=4, height=3, epsg=4326))
    computed = make_geotiff_bytes(width=6, height=5, epsg=3857)
    tile = VirtualTile(
        cellid=1, raster=computed, path=str(src), metadata=metadata
    ).to_row()

    for view in ("header", "pixels"):
        lru = _SourceStubLRU()
        try:
            got = _dispatch_tile(
                tile, object(), _OpenerContext(), lru, view, _dispatch_header
            )
        finally:
            lru.close()
        assert got == (3857, 6, 5), f"view={view}: read the source file, got {got}"
        assert lru.gets == [], f"view={view}: FILE open of the source: {lru.gets}"


def test_dispatch_virtual_tile_takes_file_fast_path(tmp_path):
    """Control: a virtual tile (no bytes) still reads its source via the LRU."""
    from databricks.labs.gbx.pyrx.grouped_exec import _dispatch_tile, _OpenerContext

    from .conftest import make_geotiff_bytes

    src = tmp_path / "source_4326.tif"
    src.write_bytes(make_geotiff_bytes(width=4, height=3, epsg=4326))
    tile = VirtualTile(cellid=1, path=str(src)).to_row()

    lru = _SourceStubLRU()
    try:
        got = _dispatch_tile(
            tile, object(), _OpenerContext(), lru, "pixels", _dispatch_header
        )
    finally:
        lru.close()
    assert got == (4326, 4, 3)
    assert lru.gets == [str(src)]


# ---------------------------------------------------------------------------
# A virtual tile's pending clip / CRS instructions are applied on the FILE path too
# ---------------------------------------------------------------------------


def _clip_wkb():
    """A cutline over the upper-left 2x2 pixels of ``make_geotiff_bytes(4, 3)`` (x 10..11,
    y 49..50 in EPSG:4326, half-degree pixels from the origin 10, 50)."""
    import shapely
    from shapely.geometry import box

    return shapely.to_wkb(box(10.0, 49.0, 11.0, 50.0))


def _dispatch_virtual(tmp_path, view, **tile_kw):
    from databricks.labs.gbx.pyrx.grouped_exec import _dispatch_tile, _OpenerContext

    from .conftest import make_geotiff_bytes

    src = tmp_path / "source_4326.tif"
    src.write_bytes(make_geotiff_bytes(width=4, height=3, epsg=4326))
    tile = VirtualTile(cellid=1, path=str(src), **tile_kw).to_row()
    lru = _SourceStubLRU()
    try:
        got = _dispatch_tile(
            tile, object(), _OpenerContext(), lru, view, _dispatch_header
        )
    finally:
        lru.close()
    return got, lru.gets, str(src)


@pytest.mark.parametrize("view", ["header", "pixels"])
@pytest.mark.parametrize("window", [(0, 0, 4, 3), None], ids=["windowed", "windowless"])
def test_dispatch_applies_a_pending_clip(tmp_path, view, window):
    """A virtual tile with a ``clip_polygon`` reads as the clipped raster, as
    ``open_tile`` reads it; the whole-window FILE fast path would skip the clip."""
    got, gets, _ = _dispatch_virtual(
        tmp_path,
        view,
        window=window,
        clip_polygon=_clip_wkb(),
        clip_crs="EPSG:4326",
    )
    assert got == (4326, 2, 2), f"view={view}: clip not applied, got {got}"
    assert gets == [], f"view={view}: the FILE fast path read an unclipped source"


@pytest.mark.parametrize("view", ["header", "pixels"])
@pytest.mark.parametrize("window", [(0, 0, 4, 3), None], ids=["windowed", "windowless"])
def test_dispatch_applies_a_pending_crs(tmp_path, view, window):
    """A virtual tile whose ``crs`` differs from its source's reads reprojected."""
    got, _, _ = _dispatch_virtual(tmp_path, view, window=window, crs="EPSG:3857")
    assert got[0] == 3857, f"view={view}: the target CRS was skipped, got {got}"


@pytest.mark.parametrize("view", ["header", "pixels"])
def test_dispatch_keeps_the_fast_path_when_the_crs_is_the_source_crs(tmp_path, view):
    """A target CRS the source already has asks for nothing: still the FILE fast path."""
    got, gets, src = _dispatch_virtual(
        tmp_path, view, window=(0, 0, 4, 3), crs="EPSG:4326"
    )
    assert got == (4326, 4, 3)
    assert gets == [src], f"view={view}: left the fast path for a no-op CRS: {gets}"


@pytest.mark.parametrize("view", ["header", "pixels"])
def test_windowless_tile_with_the_source_crs_keeps_the_header_path(
    tmp_path, monkeypatch, view
):
    """On the fallback, a windowless tile whose target ``crs`` is its source's reads the
    header, not the whole raster; one whose ``crs`` differs is read and reprojected."""
    from databricks.labs.gbx.pyrx.core import open_tile as ot

    full_reads = []
    real_open = ot._open

    def spy(tile, file_ref=None):
        full_reads.append(tile)
        return real_open(tile, file_ref=file_ref)

    monkeypatch.setattr(ot, "_open", spy)

    from databricks.labs.gbx.pyrx.grouped_exec import _dispatch_tile, _OpenerContext

    from .conftest import make_geotiff_bytes

    src = tmp_path / "source_4326.tif"
    src.write_bytes(make_geotiff_bytes(width=4, height=3, epsg=4326))
    for crs, want_epsg, reads in (("EPSG:4326", 4326, 0), ("EPSG:3857", 3857, 1)):
        del full_reads[:]
        tile = VirtualTile(cellid=1, path=str(src), crs=crs).to_row()
        got = _dispatch_tile(
            tile, None, _OpenerContext(), _SourceStubLRU(), view, _dispatch_header
        )
        assert got[0] == want_epsg, f"crs={crs}: {got}"
        assert len(full_reads) == reads, f"crs={crs}: {len(full_reads)} full reads"


# ---------------------------------------------------------------------------
# A windowless virtual tile's pending instructions are applied, in both views and on
# both runtimes (raster_gbx emits exactly these tiles by default)
# ---------------------------------------------------------------------------

_PENDING_KINDS = {
    "nodata": {"pending_nodata": "auto"},  # rst_initnodata
    "bands": {"pending_bands": "2"},  # rst_band
    "srid": {"pending_srid": "3857"},  # rst_setsrid
    "crs": {"pending_crs": "EPSG:3857"},  # rst_setcrs
}


def _windowless_pending_tile(tmp_path, kind):
    """(row, VirtualTile) for a two-band uint16 EPSG:4326 file with no NoData, no window
    and the pending instruction ``kind``."""
    import rasterio
    from rasterio.transform import from_origin

    src = tmp_path / "two_band.tif"
    prof = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=2,
        dtype="uint16",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    )
    with rasterio.open(src, "w", **prof) as dst:
        dst.write(np.arange(24, dtype="uint16").reshape(2, 3, 4) + 1)
    vt = VirtualTile(cellid=1, path=str(src), metadata=dict(_PENDING_KINDS[kind]))
    assert vt.window is None and vt.raster is None
    return vt.to_row(), vt


def _view_signature(ds, cellid):
    """What a header-only operation can see."""
    from databricks.labs.gbx.core.crs import authority_srid_of

    return (ds.count, ds.nodata, authority_srid_of(ds.crs), ds.width, ds.height)


def _pixel_signature(ds, cellid):
    return _view_signature(ds, cellid) + (ds.read().tobytes(),)


@pytest.mark.parametrize("kind", list(_PENDING_KINDS))
@pytest.mark.parametrize("view", ["header", "pixels"])
@pytest.mark.parametrize("runtime", ["plain", "file"])
def test_dispatch_applies_a_windowless_tiles_pending_instructions(
    tmp_path, runtime, view, kind
):
    """``rst_initnodata`` / ``rst_band`` / ``rst_setsrid`` / ``rst_setcrs`` on a
    whole-file virtual tile record an instruction; a grouped operation must see what
    ``open_tile`` shows, whichever way the tile is read. A pixel operation used to get
    NULL on a runtime without the FILE type (the header view refuses ``read``) and the
    source as it is, instruction ignored, on one with it."""
    from databricks.labs.gbx.pyrx.core import open_tile as ot
    from databricks.labs.gbx.pyrx.grouped_exec import _dispatch_tile, _OpenerContext

    row, vt = _windowless_pending_tile(tmp_path, kind)
    core = _pixel_signature if view == "pixels" else _view_signature
    with ot.open_tile(vt) as ds:
        want = core(ds, 1)
    lru = _SourceStubLRU()
    try:
        got = _dispatch_tile(
            row,
            object() if runtime == "file" else None,
            _OpenerContext(),
            lru,
            view,
            core,
        )
    finally:
        lru.close()
    assert got is not None, f"{runtime}/{view}/{kind}: NULL result"
    assert got == want, f"{runtime}/{view}/{kind}: {got[:5]} != eager {want[:5]}"


def test_rst_initnodata_then_a_pixel_grouped_function_on_a_whole_file_tile(
    spark, tmp_path
):
    """The path of a ``raster_gbx`` tile (no window) through ``rst_initnodata`` and a pixel
    ``_grouped`` function, on the local (no FILE type) runtime: a tile with the NoData
    applied, not NULL."""
    import rasterio
    from rasterio.transform import from_origin

    from databricks.labs.gbx.pyrx import functions as prx

    src = tmp_path / "whole.tif"
    prof = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="uint16",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    )
    with rasterio.open(src, "w", **prof) as dst:
        dst.write(np.arange(12, dtype="uint16").reshape(1, 3, 4) + 1)
    vt = VirtualTile(cellid=3, path=str(src))
    assert vt.window is None
    df = spark.createDataFrame(
        [(vt.to_row(),)], StructType([StructField("tile", V2_TILE_SCHEMA)])
    ).select(prx.rst_initnodata("tile").alias("tile"))
    out = prx.rst_resample_grouped(df, 2.0, "nearest").collect()
    assert len(out) == 1 and out[0]["tile"] is not None, "NULL tile"
    with rasterio.MemoryFile(bytes(out[0]["tile"]["raster"])) as mf, mf.open() as ds:
        assert ds.nodata == 65535.0
        assert (ds.width, ds.height) == (8, 6)


def test_a_windowless_tile_without_instructions_still_reads_the_source_directly(
    tmp_path,
):
    """Control: nothing pending, nothing to apply; the FILE path hands over the cached
    source itself."""
    from databricks.labs.gbx.pyrx.grouped_exec import _dispatch_tile, _OpenerContext

    row, vt = _windowless_pending_tile(tmp_path, "nodata")
    vt.metadata = {}
    seen = []

    def core(ds, cellid):
        seen.append(ds)
        return _pixel_signature(ds, cellid)

    lru = _SourceStubLRU()
    try:
        got = _dispatch_tile(
            vt.to_row(), object(), _OpenerContext(), lru, "pixels", core
        )
    finally:
        lru.close()
    assert got is not None and seen[0] is lru._open[0]


# ---------------------------------------------------------------------------
# grouped_tile_map runs every tile through _dispatch_tile
# ---------------------------------------------------------------------------


def test_grouped_tile_map_routes_every_tile_through_dispatch_tile(
    spark, gtiff_bytes, monkeypatch
):
    """The per-tile decision (FILE fast path vs fallback) lives in ``_dispatch_tile``
    alone: the executor must call it for each row, with the row's tile and FileRef, and
    use what it returns. A second copy of the logic inline in the executor would be
    tested by none of the ``_dispatch_tile`` tests above.

    ``mapInPandas`` is captured and the partition function run here in the driver, where
    the patched module attribute is visible (a Spark worker imports its own copy)."""
    import pandas as pd
    from pyspark.sql.types import StringType

    from databricks.labs.gbx.pyrx import grouped_exec

    calls = []

    def fake_dispatch(tile, fr, ctx, lru, view_mode, core_fn):
        calls.append((tile["cellid"], fr, view_mode, core_fn))
        return f"routed-{tile['cellid']}"

    monkeypatch.setattr(grouped_exec, "_dispatch_tile", fake_dispatch)

    df = _tile_df(spark, gtiff_bytes)
    captured = {}

    def capture(self, func, schema):
        captured["map"] = func
        return self

    monkeypatch.setattr(type(df), "mapInPandas", capture)

    def core(ds, cellid):
        raise AssertionError("core_fn must only run through _dispatch_tile")

    grouped_exec.grouped_tile_map(
        df, core, return_field=StructField("out", StringType()), view="pixels"
    )
    pdf = pd.DataFrame(
        {
            "tile": [
                VirtualTile.from_v1(cellid=i, raster=gtiff_bytes).to_row()
                for i in range(3)
            ]
        }
    )
    out = next(captured["map"](iter([pdf])))
    assert list(out["out"]) == ["routed-0", "routed-1", "routed-2"]
    assert [c[0] for c in calls] == [0, 1, 2]
    assert {c[2] for c in calls} == {"pixels"} and {c[3] for c in calls} == {core}
