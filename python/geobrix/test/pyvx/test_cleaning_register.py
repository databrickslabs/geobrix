def test_cleaning_registrars_are_granular():
    """Each cleaning name must map to a distinct registrar so register(only=[...]) is granular.

    Regression guard: the previous implementation mapped all 5 names to a single shared
    _reg_cleaning function, so register(only=["gbx_st_node"]) silently registered all 5.
    """
    from databricks.labs.gbx.pyvx.functions import _registrar_groups

    cleaning_names = {
        "gbx_st_simplifypreservetopology",
        "gbx_st_removerepeatedpoints",
        "gbx_st_reduceprecision",
        "gbx_st_node",
        "gbx_st_snap",
    }
    groups = _registrar_groups()
    cleaning_dict = None
    for _, group_dict in groups:
        if cleaning_names.issubset(set(group_dict.keys())):
            cleaning_dict = group_dict
            break

    assert cleaning_dict is not None, "cleaning group not found in _registrar_groups()"
    registrars = [cleaning_dict[n] for n in cleaning_names]
    assert len(set(id(fn) for fn in registrars)) == len(cleaning_names), (
        "register(only=['gbx_st_node']) would register all 5 — each name must map to a "
        "distinct registrar (one-name→one-registrar contract violated)"
    )


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
