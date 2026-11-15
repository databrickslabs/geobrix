"""The read path's decode and its oracle: virtual tiles decoded in one Python call per Arrow batch (`tile_batches`),
and the reader oracle that judges what a read brings back (coverage, CRS, dtype, every band).
"""

from test._volume_safe import open_raster

import pytest

from databricks.labs.gbx.bench import oracles

COVER = next(c for c in oracles.MVP_IO if c.cid == "io_cover")


@pytest.mark.parametrize("form", ["b", "d-py"])
def test_the_read_paths_cover_the_file_exactly_once(spark, tmp_path, form):
    assert oracles.run_reader(spark, COVER, form, str(tmp_path)) == []


def _batch_of(path, window=None, metadata=None):
    """A one-row Arrow batch holding the virtual tile of `path` (the mapInArrow input), its metadata an Arrow map."""
    import pyarrow as pa
    from pyspark.sql.pandas.types import to_arrow_type

    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile

    row = VirtualTile(
        cellid=1, path=path, window=window, metadata=metadata or {}
    ).to_row()
    schema = pa.schema([pa.field("tile", to_arrow_type(V2_TILE_SCHEMA))])
    return pa.RecordBatch.from_pylist([{"tile": row}], schema=schema)


def _tif(path, bands=1, size=64, dtype="float32", crs="EPSG:4326"):
    import numpy as np
    from rasterio.transform import from_origin

    with open_raster(
        path, "w", driver="GTiff", width=size, height=size, count=bands, dtype=dtype, crs=crs,
        transform=from_origin(0, size, 1, 1),
    ) as dst:  # fmt: skip
        dst.write(
            (np.arange(size * size * bands) % 97)
            .reshape(bands, size, size)
            .astype(dtype)
        )
    return str(path)


def _decoded_rows(batch, cap_bytes=None):
    from databricks.labs.gbx.pyrx import tile_batches

    (out,) = tile_batches.materialize_batches([batch], cap_bytes=cap_bytes)
    return [r["tile"] for r in out.to_pylist()]


def test_the_batch_decode_returns_an_error_tile_for_a_refused_location():
    """A path from data is judged by the remote-path gate before anything opens it; a refusal is a row of data (the
    light `rst_*` convention, as the JVM's safeEval), not a task failure."""
    (tile,) = _decoded_rows(_batch_of("/vsicurl/https://example.invalid/x.tif"))
    assert tile["raster"] is None and tile["cellid"] == 1
    assert "/vsicurl/" in dict(tile["metadata"])["last_error"]


@pytest.mark.parametrize("exc", [MemoryError, RecursionError])
def test_resource_exhaustion_fails_the_task_it_is_not_an_error_tile(
    tmp_path, monkeypatch, exc
):
    from databricks.labs.gbx.pyrx import tile_batches

    def boom(*a, **k):
        raise exc("while decoding")

    monkeypatch.setattr(tile_batches, "decode_py", boom)
    with pytest.raises(exc):
        _decoded_rows(_batch_of(_tif(tmp_path / "f.tif")))


def test_a_tile_with_pending_instructions_is_decoded_and_the_instruction_is_applied(
    tmp_path,
):
    """A real Arrow batch (map metadata as pairs): the pending key is applied and gone, and the row is the one the
    row-wise decode (`tile_batches.materialized`) gives."""
    from databricks.labs.gbx.pyrx import tile_batches
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    path = _tif(tmp_path / "f.tif")
    md = {"pending_nodata": "0", "keep": "me"}
    (got,) = _decoded_rows(_batch_of(path, metadata=md))
    want = tile_batches.materialized(
        VirtualTile(cellid=1, path=path, metadata=md).to_row(), None, who="test"
    )
    assert dict(got["metadata"]) == want["metadata"]
    assert (
        "pending_nodata" not in want["metadata"]
        and "tile_byte_size" in want["metadata"]
    )
    assert got["raster"] == want["raster"] and got["path"] is None


@pytest.mark.parametrize(
    "md,crs,fits",
    [
        ({}, None, False),
        ({"pending_bands": "1"}, None, True),
        ({"pending_bands": "1"}, "EPSG:3857", False),
    ],
    ids=["all-bands-over", "one-band-fits", "reprojection-doubles-it"],
)
def test_the_gate_sizes_the_tile_as_it_will_be_decoded(tmp_path, md, crs, fits):
    """3 bands x 64 x 64 float32 = 48 KiB; the cap is 20000 bytes: a band select and a warp change what is held."""
    from databricks.labs.gbx.pyrx import tile_batches
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    tile = VirtualTile(
        cellid=1, path=_tif(tmp_path / "f.tif", bands=3), metadata=md, crs=crs
    ).to_row()
    if fits:
        tile_batches.gate(tile, 20000)
    else:
        with pytest.raises(ValueError, match="retile it first"):
            tile_batches.gate(tile, 20000)


def test_the_gate_sizes_a_zarr_tile_from_its_own_header(tmp_path):
    """A Zarr-backed tile (path = the store, `zarr_array` in its metadata) is sized by its header view, not the store's."""
    pytest.importorskip("zarr")
    from test.zarr.test_tiles import cube, one_plan, virtual

    from databricks.labs.gbx.pyrx import tile_batches

    cube(tmp_path / "s.zarr")
    tile, *_ = virtual(one_plan(tmp_path / "s.zarr", variables=("v",)))
    row = tile.to_row()
    assert (
        tile_batches.decoded_bytes(row) == 6 * 6 * 1 * 4
    )  # window 6 x 6, one plane (one time step), float32
    with pytest.raises(ValueError, match="retile it first"):
        tile_batches.gate(row, 100)


def _window_tile(src_path, col, row, size, **override):
    """The decoded tile of a window of the file, as a read gives it (a dict with `raster` and `metadata`)."""
    from databricks.labs.gbx.pyrx.core import open_tile as ot
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    tile = ot.materialize_to_bytes(
        VirtualTile(cellid=1, path=src_path, window=(col, row, size, size))
    )
    return {"raster": tile.raster, "metadata": {}, **override}


@pytest.mark.parametrize(
    "case",
    ["exact", "overlap_and_gap", "wrong_crs", "wrong_dtype", "error_tile"],
)
def test_the_reader_oracle_judges_coverage_by_cell_crs_and_dtype(tmp_path, case):
    """Four 32 px windows cover the 64 px file once. An overlap with an equal-sized gap (same area) fails, as do a
    tile of another CRS or dtype and an error tile."""
    from databricks.labs.gbx._gdal_policy import open_data

    path = _tif(tmp_path / "f.tif")
    tiles = [_window_tile(path, c, r, 32) for r in (0, 32) for c in (0, 32)]
    if case == "overlap_and_gap":
        tiles[3] = tiles[2]  # (32, 32) is in no tile and (0, 32) is in two
    elif case == "wrong_crs":
        tiles[0]["raster"] = _rewritten(tiles[0]["raster"], crs="EPSG:3857")
    elif case == "wrong_dtype":
        tiles[0]["raster"] = _rewritten(tiles[0]["raster"], dtype="uint8")
    elif case == "error_tile":
        tiles[0] = {"raster": None, "metadata": {"last_error": "boom"}}
    with open_data(path) as src:
        got = oracles.reader_failures("io", "x", src, tiles)
    assert (got == []) == (case == "exact"), got


def _rewritten(raster, dtype=None, crs=None):
    """The bytes of `raster` with another CRS or data type and the same grid and values."""
    from rasterio.io import MemoryFile

    with MemoryFile(bytes(raster)) as mf, mf.open() as src:
        prof, data = src.profile, src.read()
    prof.update(dtype=dtype or prof["dtype"], crs=crs or prof["crs"])
    with MemoryFile() as out:
        with out.open(**prof) as dst:
            dst.write(data.astype(prof["dtype"]))
        return out.read()


def _reader_failures_of(path, tile_raster):
    from databricks.labs.gbx._gdal_policy import open_data

    with open_data(path) as src:
        return oracles.reader_failures(
            "io", "x", src, [{"raster": tile_raster, "metadata": {}}]
        )


def _whole_tile(path):
    return _window_tile(path, 0, 0, 64)["raster"]


@pytest.mark.parametrize("case", ["exact", "band_2_differs", "band_dropped"])
def test_the_reader_oracle_compares_every_band(tmp_path, case):
    """Band 1 alone passed a tile whose other bands were wrong or missing."""
    path = _tif(tmp_path / "f.tif", bands=3)
    raster = _whole_tile(path)
    if case != "exact":
        from rasterio.io import MemoryFile

        with MemoryFile(bytes(raster)) as mf, mf.open() as t:
            prof, data = t.profile, t.read()
        if case == "band_2_differs":
            data[1] = data[1] + 1
        else:
            data, prof["count"] = data[:1], 1
        with MemoryFile() as out:
            with out.open(**prof) as dst:
                dst.write(data)
            raster = out.read()
    got = _reader_failures_of(path, raster)
    assert (got == []) == (case == "exact"), got


def test_the_reader_oracle_treats_nan_as_equal_to_nan_in_pixels_and_nodata(tmp_path):
    """A float raster with NaN NoData (and NaN pixels) read back exactly is not a mismatch (nan != nan was one)."""
    import numpy as np
    from rasterio.transform import from_origin

    path = str(tmp_path / "nan.tif")
    data = (np.arange(64 * 64, dtype="float32") % 97).reshape(1, 64, 64)
    data[0, ::7, ::5] = np.nan
    with open_raster(
        path, "w", driver="GTiff", width=64, height=64, count=1, dtype="float32",
        crs="EPSG:4326", transform=from_origin(0, 64, 1, 1), nodata=float("nan"),
    ) as dst:  # fmt: skip
        dst.write(data)
    assert _reader_failures_of(path, _whole_tile(path)) == []


def test_a_rewritten_file_is_not_served_the_header_of_the_old_one(tmp_path):
    from databricks.labs.gbx.pyrx import tile_batches

    path = _tif(tmp_path / "f.tif", size=64, dtype="float32")
    assert tile_batches.source_header(path)[:4] == (64, 64, 1, "float32")
    _tif(tmp_path / "f.tif", size=32, dtype="uint8")
    assert tile_batches.source_header(path)[:4] == (32, 32, 1, "uint8")


def test_the_batch_decode_yields_bounded_slices_in_order(tmp_path, monkeypatch):
    """A batch of many tiles leaves as several output batches (a decoded column past 2 GiB cannot be built, and a
    Serverless UDF holds 1 GiB), with every row kept, in order."""
    import pyarrow as pa

    from databricks.labs.gbx.pyrx import tile_batches

    paths = [_tif(tmp_path / f"f{i}.tif") for i in range(5)]
    batches = [_batch_of(p) for p in paths]
    one = pa.RecordBatch.from_pylist(
        [r for b in batches for r in b.to_pylist()], schema=batches[0].schema
    )
    monkeypatch.setattr(tile_batches, "_OUT_CHUNK_BYTES", 1)  # a slice per decoded tile
    out = list(tile_batches.materialize_batches([one]))
    assert [b.num_rows for b in out] == [1] * 5
    assert all(b.to_pylist()[0]["tile"]["raster"] for b in out)
    monkeypatch.setattr(tile_batches, "_OUT_CHUNK_BYTES", 1 << 30)
    (whole,) = tile_batches.materialize_batches([one])
    assert whole.num_rows == 5
    assert [b.to_pylist()[0] for b in out] == whole.to_pylist()


def test_a_slice_is_cut_before_the_tile_that_would_pass_the_bound(monkeypatch):
    """The bound lies between two tile sizes (25 for tiles of 10): a slice holds two tiles, never three. Cutting
    after the tile that reaches the bound would give [3, 2], a slice over it."""
    import pyarrow as pa

    from databricks.labs.gbx.pyrx import tile_batches

    base = _batch_of("/x0.tif")
    rows = [r for i in range(5) for r in _batch_of(f"/x{i}.tif").to_pylist()]
    batch = pa.RecordBatch.from_pylist(rows, schema=base.schema)
    monkeypatch.setattr(tile_batches, "_OUT_CHUNK_BYTES", 25)
    monkeypatch.setattr(tile_batches.budget, "decoded_budget_bytes", lambda s: 1 << 40)

    def ten_bytes(tile, cap_bytes):
        return {**tile, "raster": b"x" * 10, "path": None}

    out = list(tile_batches.materialize_batches([batch], decode=ten_bytes))
    assert [b.num_rows for b in out] == [2, 2, 1]
    for b in out:
        assert sum(len(r["tile"]["raster"]) for r in b.to_pylist()) <= 25


def test_the_batch_decode_of_an_empty_batch_is_one_empty_batch_with_the_same_schema():
    import pyarrow as pa

    from databricks.labs.gbx.pyrx import tile_batches

    one = _batch_of("/nowhere.tif")
    empty = pa.RecordBatch.from_pylist([], schema=one.schema)
    (out,) = tile_batches.materialize_batches([empty])
    assert out.num_rows == 0 and out.schema == one.schema


def test_the_batch_decode_passes_skipped_rows_through_and_keeps_every_row_in_order(
    tmp_path, monkeypatch
):
    """A null tile, an already-materialized tile and a tile without a path are not decoded; the virtual ones between
    them are, and slicing at the byte bound loses and reorders nothing."""
    import pyarrow as pa

    from databricks.labs.gbx.pyrx import tile_batches
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    a, b = _tif(tmp_path / "a.tif"), _tif(tmp_path / "b.tif")
    base = _batch_of(a)

    def row(cellid, **kw):
        return {"tile": VirtualTile(cellid=cellid, **kw).to_row()}

    rows = [
        {"tile": None},
        row(cellid=1, path=a),  # decoded
        row(cellid=2, path=b, raster=b"already"),  # materialized: kept as it is
        {"tile": {**row(cellid=3, path=a)["tile"], "path": None}},  # no path: kept
        row(cellid=4, path=b),  # decoded
        {"tile": None},
    ]
    batch = pa.RecordBatch.from_pylist(rows, schema=base.schema)
    for chunk in (1 << 30, 1):  # one output batch, then a slice per decoded tile
        monkeypatch.setattr(tile_batches, "_OUT_CHUNK_BYTES", chunk)
        out = [
            r
            for ob in tile_batches.materialize_batches([batch])
            for r in ob.to_pylist()
        ]
        assert len(out) == 6
        assert [None if r["tile"] is None else r["tile"]["cellid"] for r in out] == [
            None,
            1,
            2,
            3,
            4,
            None,
        ]
        assert out[1]["tile"]["raster"] and out[4]["tile"]["raster"]  # decoded
        assert (
            out[2]["tile"]["raster"] == b"already" and out[3]["tile"]["raster"] is None
        )  # untouched
        assert out[0]["tile"] is None and out[5]["tile"] is None
