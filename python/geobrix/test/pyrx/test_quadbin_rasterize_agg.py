"""Spark-level tests for the light quadbin rasterize_agg grouped-agg UDF."""

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx import functions as rx


def _quadbin_cell_ids(res=12):
    """A small quadbin res-12 cell set over central London (Long ids)."""
    from shapely import set_srid, to_wkb
    from shapely.geometry import box

    from databricks.labs.gbx.pygx import _quadbin as qb

    ewkb = to_wkb(set_srid(box(-0.13, 51.50, -0.06, 51.55), 4326), include_srid=True)
    return qb.polyfill(ewkb, res)


def test_rst_quadbin_rasterize_agg_presence_mask(spark):
    cells = _quadbin_cell_ids(12)
    assert len(cells) >= 2
    df = spark.createDataFrame([(int(c), "TX1") for c in cells], ["cellid", "tx"])
    out = (
        df.groupBy("tx")
        .agg(rx.rst_quadbin_rasterize_agg("cellid").alias("tile"))
        .collect()
    )
    tile = out[0]["tile"]
    assert tile is not None and tile["raster"] is not None
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        arr = ds.read(1)
        assert (arr == 1.0).sum() >= len(cells)
        assert ds.nodata == -9999.0


def test_rst_quadbin_rasterize_agg_null_cellid_value_alignment():
    """Null cellids must not shift value pairing for subsequent valid rows.

    Before the fix: ``cells`` filtered None from cellid but ``vals`` iterated
    the full value series, so ``zip(cells, vals)`` misaligned — the Nth valid
    cellid was paired with the Nth overall value, not its own.

    This test exercises the inner UDF function directly (not via Spark) to avoid
    the float64 precision loss that PySpark's Arrow bridge applies to nullable
    LongType pandas Series — quadbin cell IDs are 64-bit values that exceed
    float64's 53-bit mantissa.  pd.NA with Int64Dtype preserves full precision.

    Resolution 8 cells are chosen because the auto-computed gridspec is coarse
    enough that both cells get covered pixels, making the value-alignment check
    unambiguous.
    """
    import pandas as pd
    from shapely import set_srid, to_wkb
    from shapely.geometry import box

    from databricks.labs.gbx.pygx import _quadbin as qb
    from databricks.labs.gbx.pyrx.functions import _rst_quadbin_rasterize_agg_udf

    # Two adjacent res-8 cells over London — coarse enough that the auto gridspec
    # yields several pixels per cell.
    ewkb = to_wkb(set_srid(box(-0.50, 51.00, 0.50, 52.00), 4326), include_srid=True)
    cells = qb.polyfill(ewkb, 8)
    assert len(cells) >= 2, "need >=2 cells at res 8 over the London bbox"
    c0, c1 = int(cells[0]), int(cells[1])
    assert c0 != c1, "c0 and c1 must differ"

    # Wrap in pd.Series so .iloc indexing works (the UDF internally checks .iloc[0]).
    cellid_s = pd.Series(pd.array([c0, pd.NA, c1], dtype="Int64"))
    value_s = pd.Series([10.0, 99.0, 20.0], dtype="float64")
    # Pass Python None (not a NaN-filled Series) to trigger the `s is None` path
    # and use default values (srid=4326, mode=centroids, kring_pad=1).
    none_s = None

    fn = _rst_quadbin_rasterize_agg_udf.func
    result = fn(
        cellid_s,
        value_s,
        none_s,
        none_s,  # srid, pixel_size
        none_s,
        none_s,
        none_s,
        none_s,  # xmin, ymin, xmax, ymax
        none_s,
        none_s,  # width, height
        none_s,
        none_s,  # mode, kring_pad
    )
    assert result is not None, "UDF returned None — no valid cells after null filter"
    with _serde.open_tile(bytes(result)) as ds:
        arr = ds.read(1)
        covered = arr[arr != ds.nodata]
        assert covered.size >= 1, f"expected >=1 covered pixel, got {covered.size}"
        # Key assertion: 99.0 (the null-cellid row's value) must NOT appear.
        # Before the fix, zip(cells, vals) misaligned: c1 paired with 99.0 (the
        # null-row value) instead of 20.0, so 99.0 would appear in the output.
        assert 99.0 not in covered, (
            f"99.0 (null-cellid row value) appeared — cellid/value misalignment "
            f"bug still present: covered={sorted(set(covered.tolist()))}"
        )
        # At least one valid value (10.0 or 20.0) must be present.
        assert any(
            v in covered for v in (10.0, 20.0)
        ), f"neither 10.0 nor 20.0 found in output: {sorted(set(covered.tolist()))}"
