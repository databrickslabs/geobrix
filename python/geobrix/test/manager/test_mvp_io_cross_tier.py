"""The read-path oracle (MVP-IO): the oracle file read through every path comes back exactly, once.

Forms: ``a`` the heavy ``gdal`` reader, ``b`` the light reader (materialized tiles), ``c`` the light reader's
virtual tiles decoded by the JVM (``gbx_mvp_rst_materialize``), ``d-py`` and ``d-rs`` the same tiles decoded in one
Python call per Arrow batch (pyrx, and the Rust build when its wheel has one). The JVM forms need the staged JAR and
run under `gbx-test-parity.sh`; ``d-rs`` skips unless the loaded `_gbxrs` has `decode_window`.
"""

import os

import pytest

from databricks.labs.gbx import _rs, register
from databricks.labs.gbx.bench import oracles

COVER = next(c for c in oracles.MVP_IO if c.cid == "io_cover")
MATERIALIZE = next(c for c in oracles.MVP_IO if c.cid == "io_materialize")
FN = oracles.IO
JVM = {"a", "c"}


def _has_rust_decode() -> bool:
    mod = _rs.load()
    return mod is not None and hasattr(mod, "decode_window")


@pytest.mark.parametrize(
    "form",
    [
        pytest.param(f, marks=pytest.mark.integration) if f in JVM else f
        for f in ("a", "b", "c", "d-py", "d-rs")
    ],
)
def test_mvp_io_cover(request, tmp_path, form):
    if form == "d-rs" and not _has_rust_decode():
        pytest.skip(
            _rs.unavailable_reason() or "this _gbxrs build has no decode_window"
        )
    spark = request.getfixturevalue("jvm_spark" if form in JVM else "spark")
    if form == "c":
        register(spark, only=[FN], _levels={FN: ("L1", "-")})
    assert oracles.run_reader(spark, COVER, form, str(tmp_path)) == []


@pytest.mark.parametrize(
    "level",
    [pytest.param("L1", marks=pytest.mark.integration), "L3"],
)
def test_mvp_io_materialize_row(request, tmp_path, level):
    """The manifest function at both of its levels: a virtual tile of the file in, the file's pixels out."""
    spark = request.getfixturevalue("jvm_spark" if level == "L1" else "spark")
    register(spark, only=[FN], _levels={FN: (level, "-")})
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


def _decoded_rows(batch, rust=False, cap_bytes=None):
    from databricks.labs.gbx.pyrx import _mvp_forms

    (out,) = _mvp_forms.materialize_batches([batch], rust=rust, cap_bytes=cap_bytes)
    return [r["tile"] for r in out.to_pylist()]


@pytest.mark.parametrize("rust", [False, True], ids=["d-py", "d-rs"])
def test_the_batch_decode_returns_an_error_tile_for_a_refused_location(rust):
    """A path from data is judged by the remote-path gate before anything opens it, on both paths; a refusal is a row of
    data (the light `rst_*` convention, as the JVM's safeEval), not a task failure."""
    if rust and not _has_rust_decode():
        pytest.skip("this _gbxrs build has no decode_window")
    (tile,) = _decoded_rows(_batch_of("/vsicurl/https://example.invalid/x.tif"), rust)
    assert tile["raster"] is None and tile["cellid"] == 1
    assert "/vsicurl/" in dict(tile["metadata"])["last_error"]


@pytest.mark.parametrize("build", ["none", "no-decode-window"])
def test_a_missing_rust_build_fails_the_batch_and_is_not_an_error_tile(
    tmp_path, monkeypatch, build
):
    """A build without `decode_window` is a configuration error: a d-rs run on it must raise, not return a column of
    error tiles that reads as a (cheap) result. A per-tile data error still becomes an error tile (the test above).
    """
    monkeypatch.setattr(_rs, "load", lambda: None if build == "none" else object())
    with pytest.raises(RuntimeError, match="Rust decode is unavailable"):
        _decoded_rows(_batch_of(_tif(tmp_path / "f.tif")), rust=True)


@pytest.mark.parametrize("exc", [MemoryError, RecursionError])
@pytest.mark.parametrize("rust", [False, True], ids=["d-py", "d-rs"])
def test_resource_exhaustion_fails_the_task_it_is_not_an_error_tile(
    tmp_path, monkeypatch, exc, rust
):
    from databricks.labs.gbx.pyrx import _mvp_forms

    def boom(*a, **k):
        raise exc("while decoding")

    monkeypatch.setattr(_mvp_forms, "_decode_rust", boom)
    monkeypatch.setattr(_mvp_forms, "_decode_py", boom)
    with pytest.raises(exc):
        _decoded_rows(_batch_of(_tif(tmp_path / "f.tif")), rust=rust)


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
    (got,) = _decoded_rows(
        _batch_of(path, metadata=md), rust=True
    )  # rust=True: pyrx must take it
    want = _mvp_forms.materialize_udf.func(
        VirtualTile(cellid=1, path=path, metadata=md).to_row()
    )
    assert dict(got["metadata"]) == want["metadata"]
    assert (
        "pending_nodata" not in want["metadata"]
        and "tile_byte_size" in want["metadata"]
    )
    assert got["raster"] == want["raster"] and got["path"] is None


def test_the_rust_decode_is_offered_a_plain_file_only():
    """An instruction, a clip, a Zarr array and an object-store or /vsi path go the pyrx way (rasterio rewrites an
    `s3://` URI to GDAL's /vsis3/; the Rust open does not). The map metadata is a dict by now, as a batch makes it.
    """
    from databricks.labs.gbx.pyrx import _mvp_forms

    plain = {
        "clip_polygon": None,
        "metadata": {"k": "v"},
        "path": "dbfs:/Volumes/c/s/v/a.tif",
    }
    assert _mvp_forms._rust_can_decode(plain)
    for change in (
        {"metadata": {"pending_srid": "3857"}},
        {"metadata": {"zarr_array": "a"}},
        {"clip_polygon": b"x"},
        *(
            {"path": p}
            for p in (
                "s3://b/k.tif",
                "abfss://c@a.dfs.core.windows.net/k.tif",
                "gs://b/k.tif",
                "/vsis3/b/k.tif",
                # a scheme with no "//" is a scheme to rasterio too (urllib.parse reads it)
                "zip:/x.zip",
                "gs:bucket/x.tif",
                "hdfs:/x.tif",
            )
        ),
    ):
        assert not _mvp_forms._rust_can_decode({**plain, **change}), change


def test_the_rust_decode_takes_a_plain_tile_without_pyrx_and_gives_the_pyrx_row(
    tmp_path, monkeypatch
):
    """d-rs must be Rust (pyrx made to raise), and its row equals the d-py row for the same tile."""
    from databricks.labs.gbx.pyrx import _mvp_forms

    if not _has_rust_decode():
        pytest.skip(
            _rs.unavailable_reason() or "this _gbxrs build has no decode_window"
        )
    batch = _batch_of(
        _tif(tmp_path / "f.tif"),
        window=(8, 8, 16, 16),
        metadata={"k": "v"},
    )
    (want,) = _decoded_rows(batch, rust=False)

    def no_pyrx(*a, **k):
        raise AssertionError("fell back to pyrx")

    monkeypatch.setattr(_mvp_forms, "_decode_py", no_pyrx)
    (got,) = _decoded_rows(batch, rust=True)
    for row in (
        got,
        want,
    ):  # the size stamped is the size of the bytes each decode wrote
        assert dict(row["metadata"]).pop("tile_byte_size") == str(len(row["raster"]))

    def meta(row):
        return {k: v for k, v in row["metadata"] if k != "tile_byte_size"}

    assert meta(got) == meta(want)
    assert {k: v for k, v in got.items() if k not in ("raster", "metadata")} == {
        k: v for k, v in want.items() if k not in ("raster", "metadata")
    }
    import numpy as np
    from rasterio.io import MemoryFile

    from databricks.labs.gbx._gdal_policy import open_bytes

    with (
        MemoryFile(bytes(got["raster"])) as a,
        open_bytes(a) as g,
        MemoryFile(bytes(want["raster"])) as b,
        open_bytes(b) as w,
    ):
        assert np.array_equal(g.read(), w.read())


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


def test_the_rust_decode_opens_with_the_allowed_drivers_only(tmp_path):
    """`drivers` is GDAL's driver list for the open: a GeoTIFF is refused when GTiff is not in it."""
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    if not _has_rust_decode():
        pytest.skip(
            _rs.unavailable_reason() or "this _gbxrs build has no decode_window"
        )
    path = str(tmp_path / "f.tif")
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=16,
        height=16,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 16, 1, 1),
    ) as dst:
        dst.write(np.arange(256, dtype="float32").reshape(1, 16, 16))
    mod = _rs.load()
    with pytest.raises(Exception, match="cannot open"):
        mod.decode_window(path, 0, 0, 8, 8, 3, ["PNG"])
    out = mod.decode_window(path, 8, 8, 8, 8, 3, ["GTiff", "PNG"])
    with rasterio.MemoryFile(bytes(out)) as mf, mf.open() as got:
        assert got.compression.value == "ZSTD" and got.shape == (8, 8)
        assert np.array_equal(
            got.read(1), np.arange(256, dtype="float32").reshape(16, 16)[8:, 8:]
        )
    with pytest.raises(ValueError, match="not inside"):
        mod.decode_window(path, 12, 12, 8, 8, 3, None)


@pytest.mark.integration
def test_every_io_bench_form_runs_with_the_jar(jvm_spark, tmp_path):
    """The levels leg's IO forms (reads a / b / c / udf / d-py / d-rs, writers W0 / W1 / W2) on four local tiles, the
    JVM forms included: each ends `ok`, or `na_by_design` for d-rs alone, and the writers leave no scratch behind.
    """
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
        jvm_spark, "IO", "io-test", 0, 1, corpus=str(tmp_path / "corpus"), schema=None, scales=(1,), base_rows=4,
        where="test",
    )  # fmt: skip
    by_form = {r.form: r for r in got}
    assert set(by_form) == {f.variant for f in levels_forms.FORMS["IO"]}
    bad = {
        k: (r.status, r.failure_mode or r.note)
        for k, r in by_form.items()
        if r.status != "ok"
    }
    allowed = (
        set() if _has_rust_decode() else {"d-rs"}
    )  # without decode_window the d-rs form is skipped by design
    assert set(bad) <= allowed and all(
        v[0] == "na_by_design" for v in bad.values()
    ), bad
    scratch = levels_forms._io_scratch(str(tmp_path / "corpus"))
    assert not os.path.isdir(scratch) or not os.listdir(scratch)
