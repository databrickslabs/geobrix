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


def _batch_of(path, window=None):
    """A one-row Arrow batch holding the virtual tile of `path` (the mapInArrow input)."""
    import pyarrow as pa
    from pyspark.sql.pandas.types import to_arrow_type

    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile

    row = VirtualTile(cellid=1, path=path, window=window).to_row()
    schema = pa.schema([pa.field("tile", to_arrow_type(V2_TILE_SCHEMA))])
    return pa.RecordBatch.from_pylist([{"tile": row}], schema=schema)


@pytest.mark.parametrize("rust", [False, True], ids=["d-py", "d-rs"])
def test_the_batch_decode_refuses_a_remote_location(rust):
    """A path from data is judged by the remote-path gate before anything opens it, on both paths."""
    from databricks.labs.gbx._gdal_policy import RemotePathError
    from databricks.labs.gbx.pyrx import _mvp_forms

    if rust and not _has_rust_decode():
        pytest.skip("this _gbxrs build has no decode_window")
    batch = _batch_of("/vsicurl/https://example.invalid/x.tif")
    with pytest.raises(RemotePathError, match="/vsicurl/"):
        list(_mvp_forms.materialize_batches([batch], rust=rust))


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
