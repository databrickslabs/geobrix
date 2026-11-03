"""The read-path oracle (MVP-IO): the oracle file read through every path comes back exactly, once.

Forms: ``b`` the light reader (materialized tiles) and ``d-py`` its virtual tiles decoded in one Python call per Arrow
batch (pyrx). The manifest function ``gbx_mvp_rst_materialize`` is the same decode as a row UDF (L3 only).
"""

import os

import pytest

from databricks.labs.gbx import register
from databricks.labs.gbx.bench import oracles

COVER = next(c for c in oracles.MVP_IO if c.cid == "io_cover")
MATERIALIZE = next(c for c in oracles.MVP_IO if c.cid == "io_materialize")
FN = oracles.IO


@pytest.mark.parametrize("form", ["b", "d-py"])
def test_mvp_io_cover(spark, tmp_path, form):
    assert oracles.run_reader(spark, COVER, form, str(tmp_path)) == []


def test_mvp_io_materialize_row(spark, tmp_path):
    """The manifest function (the light UDF, its only level): a virtual tile of the file in, the file's pixels out."""
    register(spark, only=[FN], _levels={FN: ("L3", "-")})
    (result,) = oracles.run_session(spark, [MATERIALIZE], tmpdir=str(tmp_path))
    assert result[1] is None


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
    import rasterio
    from rasterio.transform import from_origin

    with rasterio.open(
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
    from databricks.labs.gbx.pyrx import _mvp_forms

    (out,) = _mvp_forms.materialize_batches([batch], cap_bytes=cap_bytes)
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
    from databricks.labs.gbx.pyrx import _mvp_forms

    def boom(*a, **k):
        raise exc("while decoding")

    monkeypatch.setattr(_mvp_forms, "_decode_py", boom)
    with pytest.raises(exc):
        _decoded_rows(_batch_of(_tif(tmp_path / "f.tif")))


def test_the_row_udf_returns_an_error_tile_for_a_refused_location():
    """L3 `gbx_mvp_rst_materialize`: the row UDF gives the same error tile as the batch form (the reason under
    `last_error`), keeping the tile's cell id."""
    from databricks.labs.gbx.pyrx import _mvp_forms
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    tile = VirtualTile(cellid=7, path="/vsicurl/https://example.invalid/x.tif").to_row()
    got = _mvp_forms.materialize_udf.func(tile)
    assert got["raster"] is None and got["cellid"] == 7
    assert "/vsicurl/" in dict(got["metadata"])["last_error"]


def test_a_tile_with_pending_instructions_decodes_as_the_row_udf_decodes_it(tmp_path):
    """A real Arrow batch (map metadata as pairs): the pending key is applied and gone, and the row is the L3 UDF's."""
    from databricks.labs.gbx.pyrx import _mvp_forms
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    path = _tif(tmp_path / "f.tif")
    md = {"pending_nodata": "0", "keep": "me"}
    (got,) = _decoded_rows(_batch_of(path, metadata=md))
    want = _mvp_forms.materialize_udf.func(
        VirtualTile(cellid=1, path=path, metadata=md).to_row()
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
    from databricks.labs.gbx.pyrx import _mvp_forms
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    tile = VirtualTile(
        cellid=1, path=_tif(tmp_path / "f.tif", bands=3), metadata=md, crs=crs
    ).to_row()
    if fits:
        _mvp_forms._gate(tile, 20000)
    else:
        with pytest.raises(ValueError, match="retile it first"):
            _mvp_forms._gate(tile, 20000)


def test_the_gate_sizes_a_zarr_tile_from_its_own_header(tmp_path):
    """A Zarr-backed tile (path = the store, `zarr_array` in its metadata) is sized by its header view, not the store's."""
    pytest.importorskip("zarr")
    from test.zarr.test_tiles import cube, one_plan, virtual

    from databricks.labs.gbx.pyrx import _mvp_forms

    cube(tmp_path / "s.zarr")
    tile, *_ = virtual(one_plan(tmp_path / "s.zarr", variables=("v",)))
    row = tile.to_row()
    assert (
        _mvp_forms._decoded_bytes(row) == 6 * 6 * 1 * 4
    )  # window 6 x 6, one plane (one time step), float32
    with pytest.raises(ValueError, match="retile it first"):
        _mvp_forms._gate(row, 100)


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
    import rasterio
    from rasterio.transform import from_origin

    path = str(tmp_path / "nan.tif")
    data = (np.arange(64 * 64, dtype="float32") % 97).reshape(1, 64, 64)
    data[0, ::7, ::5] = np.nan
    with rasterio.open(
        path, "w", driver="GTiff", width=64, height=64, count=1, dtype="float32",
        crs="EPSG:4326", transform=from_origin(0, 64, 1, 1), nodata=float("nan"),
    ) as dst:  # fmt: skip
        dst.write(data)
    assert _reader_failures_of(path, _whole_tile(path)) == []


def test_a_rewritten_file_is_not_served_the_header_of_the_old_one(tmp_path):
    from databricks.labs.gbx.pyrx import _mvp_forms

    path = _tif(tmp_path / "f.tif", size=64, dtype="float32")
    assert _mvp_forms._source_header(path)[:4] == (64, 64, 1, "float32")
    _tif(tmp_path / "f.tif", size=32, dtype="uint8")
    assert _mvp_forms._source_header(path)[:4] == (32, 32, 1, "uint8")


def test_plain_register_leaves_a_probe_out_and_a_named_one_installs_it():
    from databricks.labs.gbx import manager

    from .connect_stub import ConnectStub

    assert FN not in manager.register(ConnectStub())
    assert FN in manager.register(ConnectStub(), only=[FN], _levels={FN: ("L3", "-")})


def test_every_io_bench_form_runs(spark, tmp_path):
    """The levels leg's IO forms (reads b / udf / d-py / floor, writer W1) on four local tiles: each ends `ok`, and the
    writer leaves no scratch behind."""
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    from databricks.labs.gbx.bench import levels, levels_forms

    rows = tmp_path / "corpus" / "rows"
    rows.mkdir(parents=True)
    for i in range(4):
        with rasterio.open(
            rows / f"t{i}.tif",
            "w",
            driver="GTiff",
            width=16,
            height=16,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=from_origin(i * 16, 16, 1, 1),
        ) as dst:
            dst.write(np.full((1, 16, 16), i, dtype="float32"))
    got = levels.run_mvp(
        spark, "IO", "io-test", 0, 1, corpus=str(tmp_path / "corpus"), schema=None, scales=(1,), base_rows=4,
        where="test",
    )  # fmt: skip
    by_form = {r.form: r for r in got}
    assert set(by_form) == {f.variant for f in levels_forms.FORMS["IO"]}
    bad = {
        k: (r.status, r.failure_mode or r.note)
        for k, r in by_form.items()
        if r.status != "ok"
    }
    assert not bad, bad
    scratch = levels_forms._io_scratch(str(tmp_path / "corpus"))
    assert not os.path.isdir(scratch) or not os.listdir(scratch)


def test_the_batch_decode_yields_bounded_slices_in_order(tmp_path, monkeypatch):
    """A batch of many tiles leaves as several output batches (a decoded column past 2 GiB cannot be built, and a
    Serverless UDF holds 1 GiB), with every row kept, in order."""
    import pyarrow as pa

    from databricks.labs.gbx.pyrx import _mvp_forms

    paths = [_tif(tmp_path / f"f{i}.tif") for i in range(5)]
    batches = [_batch_of(p) for p in paths]
    one = pa.RecordBatch.from_pylist(
        [r for b in batches for r in b.to_pylist()], schema=batches[0].schema
    )
    monkeypatch.setattr(_mvp_forms, "_OUT_CHUNK_BYTES", 1)  # a slice per decoded tile
    out = list(_mvp_forms.materialize_batches([one]))
    assert [b.num_rows for b in out] == [1] * 5
    assert all(b.to_pylist()[0]["tile"]["raster"] for b in out)
    monkeypatch.setattr(_mvp_forms, "_OUT_CHUNK_BYTES", 1 << 30)
    (whole,) = _mvp_forms.materialize_batches([one])
    assert whole.num_rows == 5
    assert [b.to_pylist()[0] for b in out] == whole.to_pylist()


def test_the_batch_decode_of_an_empty_batch_is_one_empty_batch_with_the_same_schema():
    import pyarrow as pa

    from databricks.labs.gbx.pyrx import _mvp_forms

    one = _batch_of("/nowhere.tif")
    empty = pa.RecordBatch.from_pylist([], schema=one.schema)
    (out,) = _mvp_forms.materialize_batches([empty])
    assert out.num_rows == 0 and out.schema == one.schema


def test_the_batch_decode_passes_skipped_rows_through_and_keeps_every_row_in_order(
    tmp_path, monkeypatch
):
    """A null tile, an already-materialized tile and a tile without a path are not decoded; the virtual ones between
    them are, and slicing at the byte bound loses and reorders nothing."""
    import pyarrow as pa

    from databricks.labs.gbx.pyrx import _mvp_forms
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
        monkeypatch.setattr(_mvp_forms, "_OUT_CHUNK_BYTES", chunk)
        out = [
            r for ob in _mvp_forms.materialize_batches([batch]) for r in ob.to_pylist()
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
