"""rst_viewshed_towers on the engine: a reprojection that fails fails the query, as a viewshed that fails does.

A failed step gives an error tile (no raster). Its histogram is NULL, and the counts would read 0 through their
``coalesce``. These run where the engine does (the dev container), over a small tiled DSM on local disk.
"""

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.engine import run

pytestmark = pytest.mark.skipif(
    bool(run.unavailable_reason()), reason=str(run.unavailable_reason())
)

X0, Y0, TILE, PX, N = -13642204.0, 4537132.0, 256.0, 256, 3


@pytest.fixture(scope="module")
def spark():
    import sys

    from pyspark.sql import SparkSession

    import databricks.labs.gbx as gbx

    session = (
        SparkSession.builder.master("local[1]")
        .appName("viewshed-towers-rows")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    gbx.register(session)
    yield session


@pytest.fixture(scope="module")
def dsm(spark, tmp_path_factory):
    d = tmp_path_factory.mktemp("dsm")
    rng = np.random.default_rng(0)
    rows = []
    for tx in range(N):
        for ty in range(N):
            z = (rng.random((PX, PX)) * 5 + 10).astype("float32")
            path = str(d / f"dsm_{tx}_{ty}.tif")
            with rasterio.open(
                path,
                "w",
                driver="GTiff",
                width=PX,
                height=PX,
                count=1,
                dtype="float32",
                crs="EPSG:3857",
                transform=from_origin(X0 + tx * TILE, Y0 + (ty + 1) * TILE, 1.0, 1.0),
                nodata=-9999.0,
                tiled=True,
                blockxsize=128,
                blockysize=128,
            ) as ds:
                ds.write(z, 1)
            rows.append((path, tx, ty))
    tiles = spark.createDataFrame(rows, "path string, tx int, ty int")
    towers = spark.createDataFrame(
        [(1, X0 + 1.5 * TILE + 0.5, Y0 + 1.5 * TILE + 0.5)],
        "tower_cellid long, x double, y double",
    )
    return towers, tiles


def _towers(dsm, to_crs):
    from databricks.labs.gbx.rasterx.helpers import rst_viewshed_towers

    towers, tiles = dsm
    return rst_viewshed_towers(
        towers,
        tiles,
        radius_m=200.0,
        observer_height=10.0,
        x0=X0,
        y0=Y0,
        tile_m=TILE,
        to_crs=to_crs,
    )


def test_the_reprojected_counts_are_the_reprojected_viewshed(dsm):
    row = _towers(dsm, 4326).first()
    assert row["visible_px"] > 0 and row["total_px"] > row["visible_px"]
    assert row["total_px_reproj"] > 0 and row["visible_px_reproj"] > 0


def test_a_reprojection_that_gives_an_error_tile_fails_the_query(dsm):
    """An SRID that names no CRS makes the reprojection's output an error tile: the query raises with its reason,
    never counts of 0."""
    with pytest.raises(Exception) as caught:
        _towers(dsm, 999999).collect()
    text = str(caught.value)
    assert (
        "rst_viewshed_towers: the reprojection of tower 1 to EPSG:999999 failed" in text
    )
    assert "999999" in text.split("failed:", 1)[1]


def test_the_default_mints_and_reads_each_vrt_in_one_call_and_leaves_none(
    dsm, monkeypatch
):
    """With no vrt_dir the window VRT is minted where it is read, in the same call: on Serverless the UDF that minted
    it and the one that read it did not share a local disk, and the viewshed had no DEM. Nothing is left behind.
    """
    import glob
    import re
    import tempfile

    def left():
        return {
            p
            for pat in ("gbx_towervrt_*", "gbx_mint_*")
            for p in glob.glob(f"{tempfile.gettempdir()}/{pat}")
        }

    before = left()
    df = _towers(dsm, None)
    plan = df._jdf.queryExecution().executedPlan().toString()
    # one Python evaluation does the mint and the reads: no VRT path crosses from one UDF to another
    assert len(re.findall(r"(?:BatchEvalPython|ArrowEvalPython)", plan)) == 1, plan
    row = df.first()
    assert row["visible_px"] > 0 and row["total_px"] > row["visible_px"]
    assert left() == before


def test_vrt_dir_keeps_the_vrts_and_gives_the_same_counts(dsm, tmp_path):
    from databricks.labs.gbx.rasterx.helpers import rst_viewshed_towers

    towers, tiles = dsm
    kept = rst_viewshed_towers(
        towers,
        tiles,
        radius_m=200.0,
        observer_height=10.0,
        x0=X0,
        y0=Y0,
        tile_m=TILE,
        vrt_dir=str(tmp_path / "vrt"),
    ).first()
    assert (tmp_path / "vrt" / "tower_1.vrt").exists()
    row = _towers(dsm, None).first()
    assert (kept["visible_px"], kept["total_px"]) == (
        row["visible_px"],
        row["total_px"],
    )


def test_an_unreadable_dsm_tile_fails_the_query_with_its_reason(spark, tmp_path):
    """A DSM tile that is not a raster fails the query with the reason the read gave, never 'no reason given'."""
    from databricks.labs.gbx.rasterx.helpers import rst_viewshed_towers

    bad = tmp_path / "dsm_0_0.tif"
    bad.write_bytes(b"not a raster at all")
    tiles = spark.createDataFrame([(str(bad), 0, 0)], "path string, tx int, ty int")
    towers = spark.createDataFrame(
        [(7, X0 + 10.5, Y0 + 10.5)], "tower_cellid long, x double, y double"
    )
    with pytest.raises(Exception) as caught:
        rst_viewshed_towers(
            towers,
            tiles,
            radius_m=50.0,
            observer_height=10.0,
            x0=X0,
            y0=Y0,
            tile_m=TILE,
        ).collect()
    text = str(caught.value)
    assert "rst_viewshed_towers: the viewshed of tower 7 failed" in text
    assert "no reason given" not in text and "dsm_0_0.tif" in text
