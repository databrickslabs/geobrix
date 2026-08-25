def test_sql_cleaning_functions(spark):
    from databricks.labs.gbx.pyvx import functions as vx

    vx.register(
        spark,
        only=[
            "gbx_st_simplifypreservetopology",
            "gbx_st_removerepeatedpoints",
            "gbx_st_reduceprecision",
            "gbx_st_node",
            "gbx_st_snap",
        ],
    )
    row = spark.sql(
        "SELECT gbx_st_reduceprecision('POINT(1.234 5.678)', 1.0) AS p, "
        "gbx_st_removerepeatedpoints('LINESTRING(0 0,0 0,1 1)') AS d, "
        "gbx_st_simplifypreservetopology('POLYGON((0 0,0 5,0.001 8,0 10,10 10,10 0,0 0))', 1.0) AS s, "
        "gbx_st_node('LINESTRING(0 0,10 10,0 10,10 0)') AS n, "
        "gbx_st_snap('LINESTRING(0 0.4,10 0.4)', 'LINESTRING(0 0,10 0)', 0.5) AS sn"
    ).collect()[0]
    assert all(row[k] is not None for k in ("p", "d", "s", "n", "sn"))
