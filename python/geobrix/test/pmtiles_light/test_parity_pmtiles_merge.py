"""Light vs heavy gbx_pmtiles_agg vector-merge parity (POLYGON multi-feature).

Packs two POLYGON single-feature MVT blobs for the same (z,x,y) through both
tiers. Decodes the packed tile from each archive and asserts both features are
present with their geometry type and property values intact.

POLYGON is mandatory — points-only gives a false pass per the MVT tile-local
contract (see CLAUDE.md).

Reading note: the heavy (Scala) encoder writes ``internal_compression=NONE``
directories, which the library reader cannot read (it always gunzips). Both
archives are decoded with ``iter_tiles``, which reads them by the header.

Heavy requires the geobrix JAR staged under python/geobrix/lib/ and GDAL/OGR
native libraries. Auto-skips when absent. Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pmtiles_light/test_parity_pmtiles_merge.py \\
        --with-integration --log pmtiles-merge-parity.log
"""

import logging
from pathlib import Path

import mapbox_vector_tile as mvt
import pytest
from shapely.geometry import Polygon

from databricks.labs.gbx.pmtiles._agg_light import _assemble_archive

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents: [0]=pmtiles_light, [1]=test, [2]=python/geobrix
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

# Two distinct tile-local polygons (in [0, 4096] pixel space), different ids.
_EXTENT = 4096
_POLY_A = Polygon([(100, 100), (200, 100), (200, 200), (100, 200), (100, 100)])
_POLY_B = Polygon([(300, 300), (400, 300), (400, 400), (300, 400), (300, 300)])


def _make_mvt_blob(poly: Polygon, prop_id: int, layer: str = "bldg") -> bytes:
    return mvt.encode(
        {
            "name": layer,
            "features": [{"geometry": poly, "properties": {"id": prop_id}}],
        },
        default_options={"extents": _EXTENT, "y_coord_down": True},
    )


_BLOB_A = _make_mvt_blob(_POLY_A, prop_id=1)
_BLOB_B = _make_mvt_blob(_POLY_B, prop_id=2)

_Z, _X, _Y = 3, 2, 4


def _read_tile_from_archive(archive: bytes, z: int, x: int, y: int) -> bytes:
    """Read and return tile (z, x, y) from a PMTiles archive blob (gzip or
    uncompressed directories)."""
    from databricks.labs.gbx.pmtiles._directory import iter_tiles

    tile = dict(iter_tiles(archive)).get((z, x, y))
    assert tile is not None, f"tile ({z},{x},{y}) missing from archive"
    return tile


def _decode_features(tile: bytes) -> dict:
    """Return {id: geometry_type} for all features in the 'bldg' layer."""
    decoded = mvt.decode(tile)
    assert "bldg" in decoded, f"layer 'bldg' missing; layers: {list(decoded.keys())}"
    return {
        f["properties"]["id"]: f["geometry"]["type"]
        for f in decoded["bldg"]["features"]
    }


# ── Light tier (Spark-free) ─────────────────────────────────────────────────


def test_light_vector_merge_parity_polygon(tmp_path):
    """Light tier: two POLYGON blobs for same (z,x,y) → both in merged tile."""
    archive = _assemble_archive([_BLOB_A, _BLOB_B], [_Z, _Z], [_X, _X], [_Y, _Y], {})
    assert archive is not None
    tile = _read_tile_from_archive(archive, _Z, _X, _Y)
    feats = _decode_features(tile)
    assert set(feats.keys()) == {
        1,
        2,
    }, f"light: expected ids {{1,2}}; got {set(feats.keys())}"
    assert feats[1] == "Polygon", f"light: id=1 not Polygon: {feats[1]}"
    assert feats[2] == "Polygon", f"light: id=2 not Polygon: {feats[2]}"


# ── Heavy tier (JAR + GDAL) ──────────────────────────────────────────────────


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ "
            "— run in geobrix-dev Docker"
        )

    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live; run in isolation: "
                "gbx:test:python --path python/geobrix/test/pmtiles_light/"
                "test_parity_pmtiles_merge.py --with-integration"
            )
    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pmtiles-merge-parity")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib"
            ":/usr/java/packages/lib:/usr/lib64:/lib64:/lib"
            ":/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    yield session


def _heavy_archive(spark_with_jar):
    """Run heavy gbx_pmtiles_agg and return the archive bytes."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pmtiles import functions as pt

    pt.register(spark_with_jar)
    df = spark_with_jar.createDataFrame(
        [
            ("g", bytearray(_BLOB_A), _Z, _X, _Y),
            ("g", bytearray(_BLOB_B), _Z, _X, _Y),
        ],
        ["grp", "tile", "z", "x", "y"],
    )
    return bytes(
        df.groupBy("grp")
        .agg(f.expr("gbx_pmtiles_agg(tile, z, x, y)").alias("arc"))
        .collect()[0]["arc"]
    )


def test_heavy_vector_merge_parity_polygon(spark_with_jar):
    """Heavy tier: two POLYGON blobs for same (z,x,y) → both in merged tile."""
    archive = _heavy_archive(spark_with_jar)
    tile = _read_tile_from_archive(archive, _Z, _X, _Y)
    feats = _decode_features(tile)
    assert set(feats.keys()) == {
        1,
        2,
    }, f"heavy: expected ids {{1,2}}; got {set(feats.keys())}"
    assert feats[1] == "Polygon", f"heavy: id=1 not Polygon: {feats[1]}"
    assert feats[2] == "Polygon", f"heavy: id=2 not Polygon: {feats[2]}"


def test_light_vs_heavy_merged_tile_equivalent(spark_with_jar):
    """Light and heavy merged tiles must decode to equivalent feature sets.

    Geometry coordinate precision may differ by ±1 (integer quantization in
    OGR MVT round-trip vs mapbox_vector_tile native encoding), so we compare
    feature counts, geometry types, and attribute values — not raw bytes.
    """
    # Light merged tile.
    light_archive = _assemble_archive(
        [_BLOB_A, _BLOB_B], [_Z, _Z], [_X, _X], [_Y, _Y], {}
    )
    light_tile = _read_tile_from_archive(light_archive, _Z, _X, _Y)
    light_feats = _decode_features(light_tile)

    # Heavy merged tile.
    heavy_archive = _heavy_archive(spark_with_jar)
    heavy_tile = _read_tile_from_archive(heavy_archive, _Z, _X, _Y)
    heavy_feats = _decode_features(heavy_tile)

    assert light_feats.keys() == heavy_feats.keys(), (
        f"feature id mismatch: light={set(light_feats.keys())} "
        f"heavy={set(heavy_feats.keys())}"
    )
    for fid in light_feats:
        assert light_feats[fid] == heavy_feats[fid], (
            f"geometry type mismatch for id={fid}: "
            f"light={light_feats[fid]} heavy={heavy_feats[fid]}"
        )
