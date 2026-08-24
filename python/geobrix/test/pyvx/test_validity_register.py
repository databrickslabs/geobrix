import json


def test_sql_makevalid_and_explain(spark):
    from databricks.labs.gbx.pyvx import functions as vx

    vx.register(spark, only=["gbx_st_makevalid", "gbx_st_explainvalidity"])
    row = spark.sql(
        "SELECT gbx_st_explainvalidity('POLYGON((0 0,1 1,1 0,0 1,0 0))') AS d, "
        "gbx_st_makevalid('POLYGON((0 0,1 1,1 0,0 1,0 0))') AS fixed"
    ).collect()[0]
    d = json.loads(row["d"])
    assert d["valid"] is False and d["code"] == 10
    assert row["fixed"] is not None  # BINARY, non-null


def test_sql_makevalid_structure_level(spark):
    from databricks.labs.gbx.pyvx import functions as vx

    vx.register(spark, only=["gbx_st_makevalid"])
    row = spark.sql(
        "SELECT gbx_st_makevalid('POLYGON((0 0,1 1,1 0,0 1,0 0))', 'structure') AS f"
    ).collect()[0]
    assert row["f"] is not None
