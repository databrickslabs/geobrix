"""P2 probe (Task 23 Step 1): the statement text the Serverless notebook and the warehouse both run."""

from databricks.labs.gbx.bench import probes
from databricks.labs.gbx.manifest import TILE_SQL


def test_p2_statements_build_the_same_text_for_both_surfaces():
    deps = {"rasterio": "1.5.0", "pyproj": "3.7.2", "shapely": "2.1.2"}
    stmts = probes.p2_statements(
        "cat", "sch", "/Volumes/c/s/v/p2_tiles", "file:///Volumes/c/s/v/g.whl", deps
    )
    by = dict(stmts)
    assert [c for c, _ in stmts] == [
        "p2_create_echo",
        "p2_create_tmp",
        "p2_create_pyspark",
        *[f"p2_call_{lab}" for lab, _ in probes.P2_SIZES],
        "p2_call_tmp",
        "p2_call_pyspark",
        "p2_drop_gbx_mvp_p2_tile_echo",
        "p2_drop_gbx_mvp_p2_tmp_write",
        "p2_drop_gbx_mvp_p2_import_pyspark",
    ]
    echo = by["p2_create_echo"]
    assert f"`cat`.`sch`.gbx_mvp_p2_tile_echo(t {TILE_SQL}) RETURNS {TILE_SQL}" in echo
    assert "environment_version = '6'" in echo and "rasterio==1.5.0" in echo
    assert "geobrix @ file:///Volumes/c/s/v/g.whl" in echo
    call = by["p2_call_256MiB"]
    assert "/Volumes/c/s/v/p2_tiles/p2_256MiB.bin" in call
    assert f"AS {TILE_SQL})" in call and "binaryFile" in call
    assert all(";" not in s and "\n\n" not in s for s in by.values())
    assert [
        probes.p2_error_class(m)
        for m in ("Row size too large", "OutOfMemoryError", "timed out", "boom")
    ] == [
        "row_size",
        "memory",
        "timeout",
        "other",
    ]


def test_p2_call_answer_keeps_the_pipes_of_what_the_handler_received():
    got = probes.p2_parse_call(
        "10|pyspark.sql.types.Row|raster=bytes:10|metadata=dict|true", 10
    )
    assert got["received"] == "pyspark.sql.types.Row|raster=bytes:10|metadata=dict"
    assert got["roundtrip"] is True and got["out_bytes"] == 10
    assert probes.p2_parse_call("10|x|false", 10)["roundtrip"] is False
