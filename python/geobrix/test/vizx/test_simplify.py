import shutil
from test._volume_safe import open_raster

import pytest

from databricks.labs.gbx.vizx._simplify import normalize_spec


@pytest.fixture(scope="module")
def spark():
    import logging

    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    s = SparkSession.builder.master("local[2]").appName("simplify-tests").getOrCreate()
    yield s


def test_defaults_applied():
    s = normalize_spec(None)
    assert (
        s["budget_mb"] == 64
        and s["min_z"] == 0
        and s["max_z"] == 10
        and s["effort"] == "fast"
    )


def test_override_and_validation():
    assert normalize_spec({"max_z": 12})["max_z"] == 12
    with pytest.raises(ValueError):
        normalize_spec({"min_z": 8, "max_z": 4})
    with pytest.raises(ValueError):
        normalize_spec({"effort": "turbo"})


@pytest.mark.skipif(
    shutil.which("tippecanoe") is None, reason="tippecanoe not installed"
)
def test_simplify_from_geojson_under_budget(tmp_path):
    import geopandas as gpd
    from shapely.geometry import Polygon

    from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

    gdf = gpd.GeoDataFrame(
        {"v": [1, 2]},
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
        ],
        crs="EPSG:4326",
    )
    out = tmp_path / "o.pmtiles"
    simplify_tiles_from_source(
        gdf, spec={"max_z": 6, "budget_mb": 8}, out_path=str(out)
    )
    assert out.exists() and out.read_bytes()[:7] == b"PMTiles"


@pytest.mark.skipif(
    shutil.which("tippecanoe") is None, reason="tippecanoe not installed"
)
def test_simplify_returns_bytes_without_out_path(tmp_path):
    """Without out_path, bytes are returned (not a file)."""
    import geopandas as gpd
    from shapely.geometry import Point

    from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[Point(0, 0)],
        crs="EPSG:4326",
    )
    result = simplify_tiles_from_source(gdf, spec={"max_z": 4})
    assert isinstance(result, bytes) and result[:7] == b"PMTiles"


@pytest.mark.skipif(
    shutil.which("tippecanoe") is None, reason="tippecanoe not installed"
)
def test_simplify_drop_densest_and_cluster(tmp_path):
    """spec options drop_densest + cluster_distance propagate without error."""
    import geopandas as gpd
    from shapely.geometry import Point

    from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

    gdf = gpd.GeoDataFrame(
        {"v": range(10)},
        geometry=[Point(i * 0.1, i * 0.1) for i in range(10)],
        crs="EPSG:4326",
    )
    out = tmp_path / "clustered.pmtiles"
    simplify_tiles_from_source(
        gdf,
        spec={"max_z": 4, "drop_densest": True, "cluster_distance": 5},
        out_path=str(out),
    )
    assert out.exists() and out.stat().st_size > 0


def test_distributed_engine_raises():
    """engine='distributed' raises NotImplementedError."""
    import geopandas as gpd
    from shapely.geometry import Point

    from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

    gdf = gpd.GeoDataFrame(
        {"v": [1]},
        geometry=[Point(0, 0)],
        crs="EPSG:4326",
    )
    with pytest.raises(NotImplementedError, match="distributed"):
        simplify_tiles_from_source(gdf, spec={"engine": "distributed"})


@pytest.mark.skipif(
    shutil.which("tippecanoe") is None, reason="tippecanoe not installed"
)
def test_bbox_clip_produces_distinct_archives(tmp_path):
    """Two non-overlapping bboxes tile to different archives, proving clip took effect."""
    import geopandas as gpd
    from shapely.geometry import Point

    from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

    # Two clusters of points in non-overlapping regions.
    gdf = gpd.GeoDataFrame(
        {"v": [1, 2, 3, 4]},
        geometry=[
            Point(-10, 0),  # west cluster
            Point(-9, 0),
            Point(10, 0),  # east cluster
            Point(11, 0),
        ],
        crs="EPSG:4326",
    )

    west_bbox = (-15.0, -5.0, -5.0, 5.0)  # covers only the west cluster
    east_bbox = (5.0, -5.0, 15.0, 5.0)  # covers only the east cluster

    west_bytes = simplify_tiles_from_source(gdf, spec={"max_z": 4}, bbox=west_bbox)
    east_bytes = simplify_tiles_from_source(gdf, spec={"max_z": 4}, bbox=east_bbox)

    # Both must be valid PMTiles.
    assert isinstance(west_bytes, bytes) and west_bytes[:7] == b"PMTiles"
    assert isinstance(east_bytes, bytes) and east_bytes[:7] == b"PMTiles"
    # The two archives must differ (different spatial content).
    assert (
        west_bytes != east_bytes
    ), "bbox clip had no effect — west and east archives are identical"


def test_simplify_raster_path(tmp_path):
    """Raster source (GeoTIFF path) → COG output exists and is a valid GeoTIFF."""
    import numpy as np

    from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

    pytest.importorskip("rasterio")
    from rasterio.transform import from_bounds

    # Write a tiny 64×64 GeoTIFF
    src_tif = tmp_path / "src.tif"
    data = np.random.randint(0, 255, (1, 64, 64), dtype=np.uint8)
    transform = from_bounds(-1.0, -1.0, 1.0, 1.0, 64, 64)
    with open_raster(
        str(src_tif),
        "w",
        driver="GTiff",
        height=64,
        width=64,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(data)

    out_cog = tmp_path / "out.tif"
    simplify_tiles_from_source(
        str(src_tif), spec={"raster_max_px": 32}, out_path=str(out_cog)
    )
    assert out_cog.exists() and out_cog.stat().st_size > 0
    # Verify it's a valid GeoTIFF (TIFF magic)
    magic = out_cog.read_bytes()[:4]
    assert magic in (b"II\x2a\x00", b"MM\x00\x2a"), f"not a TIFF: {magic!r}"


# --------------------------------------------------------------------------- #
# Spark DataFrame source: geom_col + WKB/WKT, and NO silent 10k-row cap.       #
# --------------------------------------------------------------------------- #
def test_spark_df_to_gdf_wkb_collects_all_rows(spark):
    """_spark_df_to_gdf collects ALL rows (no 10k cap) from a WKB geometry column.

    vizx.as_gdf caps at 10_000 for driver-side display; tiling must keep every
    feature, so the simplify path uses a no-cap collector instead.
    """
    from shapely import to_wkb
    from shapely.geometry import Point

    from databricks.labs.gbx.vizx._simplify import _spark_df_to_gdf

    n = 12_000  # > the old 10k cap
    rows = [(i, to_wkb(Point(i * 0.0001, 0.0))) for i in range(n)]
    df = spark.createDataFrame(rows, ["fid", "geometry"])
    gdf = _spark_df_to_gdf(df, "geometry")
    assert len(gdf) == n, "row cap must be gone — every feature must survive"
    assert gdf.crs.to_epsg() == 4326
    assert "fid" in gdf.columns
    assert (gdf.geometry.geom_type == "Point").all()


def test_spark_df_to_gdf_honors_named_wkt_col(spark):
    """A WKT string column under a non-'wkt' name is honored via geom_col."""
    from databricks.labs.gbx.vizx._simplify import _spark_df_to_gdf

    df = spark.createDataFrame(
        [(1, "POINT (1 2)"), (2, "POINT (3 4)")], ["fid", "geom"]
    )
    gdf = _spark_df_to_gdf(df, "geom")
    assert len(gdf) == 2
    assert list(gdf.geometry.x) == [1.0, 3.0]


def test_spark_df_to_gdf_missing_geom_col_raises(spark):
    from databricks.labs.gbx.vizx._simplify import _spark_df_to_gdf

    df = spark.createDataFrame([(1, "POINT (1 2)")], ["fid", "geom"])
    with pytest.raises(ValueError, match="geom_col"):
        _spark_df_to_gdf(df, "geometry")


@pytest.mark.skipif(
    shutil.which("tippecanoe") is None, reason="tippecanoe not installed"
)
def test_simplify_from_spark_wkb_source(spark):
    """End-to-end: a Spark DataFrame with a WKB 'geometry' column tiles to PMTiles."""
    from shapely import to_wkb
    from shapely.geometry import Point

    from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

    rows = [(i, to_wkb(Point(-122.4 + i * 0.001, 37.7))) for i in range(50)]
    df = spark.createDataFrame(rows, ["fid", "geometry"])
    out = simplify_tiles_from_source(df, spec={"max_z": 6}, geom_col="geometry")
    assert isinstance(out, bytes) and out[:7] == b"PMTiles"
