from databricks.labs.gbx.pyvx import functions as vx


def test_register_only_is_granular():
    """Each coverage name maps to a distinct registrar so register(only=[...]) is granular.

    Uses the same structural check as test_cleaning_registrars_are_granular: verifies
    that the two coverage names are backed by DIFFERENT registrar objects in
    _registrar_groups(), which guarantees register(only=["gbx_st_coverageisvalid"])
    will not also register gbx_st_coverageinvalidedges.  Session-state-independent.
    """
    from databricks.labs.gbx.pyvx.functions import _registrar_groups

    coverage_names = {"gbx_st_coverageisvalid", "gbx_st_coverageinvalidedges"}
    groups = _registrar_groups()
    coverage_dict = None
    for _, group_dict in groups:
        if coverage_names.issubset(set(group_dict.keys())):
            coverage_dict = group_dict
            break
    assert coverage_dict is not None, "coverage group not found in _registrar_groups()"
    registrars = [coverage_dict[n] for n in sorted(coverage_names)]
    assert len(set(id(fn) for fn in registrars)) == len(coverage_names), (
        "register(only=['gbx_st_coverageisvalid']) would register both — each name must "
        "map to a distinct registrar (one-name→one-registrar contract violated)"
    )


def test_coverageisvalid_sql(spark):
    from shapely import box, to_wkb

    vx.register(spark, only=["gbx_st_coverageisvalid"])
    rows = [("c", to_wkb(box(0, 0, 1, 1))), ("c", to_wkb(box(1, 0, 2, 1)))]
    spark.createDataFrame(rows, "cov_id string, geom binary").createOrReplaceTempView(
        "cov"
    )
    r = spark.sql(
        "SELECT gbx_st_coverageisvalid(geom, 0.0) AS ok FROM cov GROUP BY cov_id"
    ).collect()
    assert r[0]["ok"] is True


def test_coverage_simplify_helper_n_to_n(spark):
    from shapely import box, from_wkb, to_wkb

    rows = [("c", "p0", to_wkb(box(0, 0, 1, 1))), ("c", "p1", to_wkb(box(1, 0, 2, 1)))]
    df = spark.createDataFrame(rows, "cov_id string, name string, geom binary")
    out = vx.coverage_simplify(df, "cov_id", "geom", 0.0).collect()
    assert len(out) == 2
    assert {row["name"] for row in out} == {"p0", "p1"}
    # PySpark collect() returns bytearray for BinaryType; bytes() makes it readable
    # by shapely 2.1.2's from_wkb (which doesn't accept bytearray in this version).
    assert all(from_wkb(bytes(row["geom_simplified"])).is_valid for row in out)
