import pytest

pytest.importorskip("quadbin")
shapely = pytest.importorskip("shapely")
from shapely import from_wkb  # noqa: E402

from databricks.labs.gbx.pygx import functions as gx  # noqa: E402


def test_cellunion_and_agg(spark):
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(
        spark, ("gbx_bng_", "gbx_custom_", "gbx_quadbin_", "gbx_h3_"), level="L3"
    )
    import quadbin

    cells = list(quadbin.k_ring(quadbin.point_to_cell(0.0, 0.0, 8), 1))
    u = spark.sql(
        f"SELECT gbx_quadbin_cellunion(array({','.join(str(c) for c in cells)})) AS u"
    ).collect()[0]["u"]
    assert from_wkb(bytes(u)).geom_type in ("Polygon", "MultiPolygon")
    df = spark.createDataFrame([(c,) for c in cells], "cell long")
    a = df.agg(gx._light_quadbin_cellunion_agg("cell").alias("u")).collect()[0]["u"]
    assert from_wkb(bytes(a)).geom_type in ("Polygon", "MultiPolygon")
