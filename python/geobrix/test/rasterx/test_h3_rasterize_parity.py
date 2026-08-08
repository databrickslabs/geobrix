"""JAR-gated heavy<->light parity test for rst_h3_rasterize_agg.

Both tiers rasterize the same res-9 H3 cell set onto the SAME explicit grid
(derived via the light-tier ``cellraster.compute_gridspec``). The pixel-centroid
burn is deterministic, so the covered-pixel masks must be identical.

Skip behavior: the entire module is skipped if no JAR is found in
``python/geobrix/lib/``, matching the convention used by every other test in
this directory.
"""

import logging
from pathlib import Path

import pytest

# Cross-tier parity test: needs BOTH the heavy JAR (gated below) AND the light-tier
# deps (h3 / rasterio / pandas, pulled in via pyrx). The heavyweight CI env has the
# JAR but not the light deps and still COLLECTS test/rasterx, so skip cleanly here —
# a module-top import of a light-only dep would otherwise error at collection.
pytest.importorskip("h3")
pytest.importorskip("rasterio")
pytest.importorskip("pandas")

import h3  # noqa: E402
import numpy as np  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as f  # noqa: E402

# ---------------------------------------------------------------------------
# JAR discovery — skip the whole module if absent (same as other rasterx tests)
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
_candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))

if not _candidates:
    pytest.skip(
        f"No geobrix JAR found in {LIBDIR} — skipping heavy<->light parity test",
        allow_module_level=True,
    )

JAR = _candidates[-1].resolve()


# ---------------------------------------------------------------------------
# Spark fixture — JAR loaded, both tiers registered
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    session = (
        SparkSession.builder.config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=ERROR,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib"
            ":/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    # Register heavy-tier SQL functions (gbx_rst_*).
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(session)
    return session


# ---------------------------------------------------------------------------
# Helper: build a small res-9 cell set
# ---------------------------------------------------------------------------


def _build_cells(res: int = 9):
    """Return a list of h3 integer cell ids for a small synthetic polygon."""
    # ~2km × 2km patch north of the equator, well inside WGS-84 bounds.
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    return [h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)]


# ---------------------------------------------------------------------------
# The parity test
# ---------------------------------------------------------------------------


def test_h3_rasterize_agg_mask_parity(spark):
    """Heavy and light must produce identical covered-pixel masks on the same grid.

    Both tiers use the SAME explicit canvas derived from
    ``cellraster.compute_gridspec`` with ``kring_pad=1``, so every rasterization
    parameter (xmin, ymin, xmax, ymax, width, height, srid) is identical. The
    pixel-centroid burn is deterministic, which means the covered-pixel sets must
    match exactly.
    """
    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx import functions as prx
    from databricks.labs.gbx.pyrx.core import cellraster

    cells = _build_cells(res=9)
    assert len(cells) >= 1, "test requires at least one cell"

    # Derive the shared grid from the light-tier helper (kring_pad=1).
    xmin, ymin, xmax, ymax, pixel_size, width, height, srid = (
        cellraster.compute_gridspec(cells, srid=4326, kring_pad=1)
    )

    group_key = "TX1"
    rows = [(int(c), group_key) for c in cells]
    df = spark.createDataFrame(rows, ["cellid", "tx"])

    # -----------------------------------------------------------------------
    # Light tier: prx.rst_h3_rasterize_agg with explicit grid
    # -----------------------------------------------------------------------
    light_out = (
        df.groupBy("tx")
        .agg(
            prx.rst_h3_rasterize_agg(
                "cellid",
                value=None,
                out_srid=f.lit(srid),
                pixel_size=f.lit(pixel_size),
                xmin=f.lit(xmin),
                ymin=f.lit(ymin),
                xmax=f.lit(xmax),
                ymax=f.lit(ymax),
                width=f.lit(width),
                height=f.lit(height),
                mode=f.lit("centroids"),
                kring_pad=f.lit(1),
            ).alias("tile")
        )
        .collect()
    )
    assert len(light_out) == 1, "light tier: expected one output row"
    light_tile = light_out[0]["tile"]
    assert light_tile is not None, "light tier: tile is None"
    assert light_tile["raster"] is not None, "light tier: raster is None"

    # -----------------------------------------------------------------------
    # Heavy tier: rx.rst_h3_rasterize_agg (Python wrapper, 12-arg form)
    # -----------------------------------------------------------------------
    from databricks.labs.gbx.rasterx import functions as rx

    heavy_out = (
        df.groupBy("tx")
        .agg(
            rx.rst_h3_rasterize_agg(
                f.col("cellid"),  # cellid LONG
                f.lit(None).cast("double"),  # value  -> presence mask (1.0)
                f.lit(srid),  # srid
                f.lit(pixel_size),  # pixel_size
                f.lit(xmin),  # xmin
                f.lit(ymin),  # ymin
                f.lit(xmax),  # xmax
                f.lit(ymax),  # ymax
                f.lit(width),  # width  (int)
                f.lit(height),  # height (int)
                f.lit("centroids"),  # mode
                f.lit(1),  # kring_pad
            ).alias("tile")
        )
        .collect()
    )
    assert len(heavy_out) == 1, "heavy tier: expected one output row"
    heavy_tile = heavy_out[0]["tile"]
    assert heavy_tile is not None, "heavy tier: tile is None"
    assert heavy_tile["raster"] is not None, "heavy tier: raster is None"

    # -----------------------------------------------------------------------
    # Compare covered-pixel masks
    # -----------------------------------------------------------------------
    nodata = -9999.0

    with _serde.open_tile(bytes(light_tile["raster"])) as lds:
        light_arr = lds.read(1)
        assert lds.width == width, f"light width mismatch: {lds.width} != {width}"
        assert lds.height == height, f"light height mismatch: {lds.height} != {height}"

    with _serde.open_tile(bytes(heavy_tile["raster"])) as hds:
        heavy_arr = hds.read(1)
        assert hds.width == width, f"heavy width mismatch: {hds.width} != {width}"
        assert hds.height == height, f"heavy height mismatch: {hds.height} != {height}"

    light_mask = light_arr != nodata
    heavy_mask = heavy_arr != nodata

    covered_light = int(light_mask.sum())
    covered_heavy = int(heavy_mask.sum())

    # Must have at least one covered pixel per tier.
    assert covered_light >= len(
        cells
    ), f"light tier: covered pixels ({covered_light}) < cell count ({len(cells)})"
    assert covered_heavy >= len(
        cells
    ), f"heavy tier: covered pixels ({covered_heavy}) < cell count ({len(cells)})"

    # The masks must be exactly identical.
    diverging = np.where(light_mask != heavy_mask)
    n_diverging = len(diverging[0])
    if n_diverging > 0:
        sample_rows = diverging[0][:5].tolist()
        sample_cols = diverging[1][:5].tolist()
        raise AssertionError(
            f"Heavy<->light mask parity FAILED: {n_diverging} pixel(s) differ "
            f"(grid {width}x{height}, {len(cells)} cells, srid={srid}). "
            f"First diverging pixels (row,col): {list(zip(sample_rows, sample_cols))}. "
            f"Light covered={covered_light}, heavy covered={covered_heavy}. "
            "This is a real cross-tier divergence — check H3 Long sign-extension "
            "or affine snap differences between tiers."
        )

    # Also assert the burned values match where covered (both tiers burn 1.0 for
    # presence mask).
    light_vals = light_arr[light_mask]
    heavy_vals = heavy_arr[heavy_mask]
    assert np.all(
        light_vals == 1.0
    ), f"light tier: unexpected burn values {np.unique(light_vals)}"
    assert np.all(
        heavy_vals == 1.0
    ), f"heavy tier: unexpected burn values {np.unique(heavy_vals)}"
