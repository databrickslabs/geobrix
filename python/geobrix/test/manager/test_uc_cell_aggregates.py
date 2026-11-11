"""The cell-aggregate array forms on Unity Catalog and in the session: what the oracle rows (bench/oracle_tables/
cell_aggregate.py) do not say. The rows hold the values and the refusals; this file holds the SQL kernels' text (the
product calls cannot run on the host), the BNG cell polygon helper (pure SQL, judged against the kernel), the host
stand-ins that let the H3 fill run, and the cap of the Python fills.
"""

import pytest
import shapely

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import product_stand_ins
from databricks.labs.gbx.uc import ddl, forms, sqlbody

H3FA, BUA, BIA = (
    "gbx_h3_cellfill_array",
    "gbx_bng_cellunion_array",
    "gbx_bng_cellintersection_array",
)
MAN = manifest.load()


def _uc_ddl(name):
    return ddl.function_ddl(
        MAN.functions[name], "c", "s", "file:///w.whl", MAN.uc_dependencies, "0.6"
    )


# --- one kernel for both surfaces, no Python on UC ---------------------------------------------------------------------
@pytest.mark.parametrize("name", [H3FA, BUA, BIA])
def test_a_sql_array_form_is_one_body_for_the_session_and_uc_and_runs_no_python(name):
    fn = MAN.functions[name]
    assert fn.levels["L4"] == {"sql": "L0"} and "L0" in fn.levels
    stmts = _uc_ddl(name)
    assert stmts[-1].count("LANGUAGE SQL") == 1 and "PYTHON" not in "".join(stmts)
    # each helper is created before the function that calls it
    helper_names = [
        h["name"] for h in sqlbody.in_dependency_order(fn.levels["L0"]["sql_helpers"])
    ]
    created = [s.split("(")[0].split(".")[-1] for s in stmts[:-1]]
    assert created == helper_names


def test_the_h3_fill_asks_the_product_for_the_ring_and_names_a_bad_k_and_method():
    body, helpers = sqlbody.l0_source(MAN.functions[H3FA])
    text = body + " ".join(h["sql"] for h in helpers)
    assert "h3_kringdistances" in text
    assert (
        "try_element_at(m, " in text
    )  # a missing neighbour is NULL, not an ANSI error
    assert body.count("raise_error") == 2
    assert "k must be >= 0; got " in body and "method must be" in body
    # the private Python routine the procedures call is not what the SQL form is built from
    assert "gbx__h3_cellfill_cells" not in text


@pytest.mark.parametrize("name,op", [(BUA, "st_union"), (BIA, "st_intersection")])
def test_the_bng_chip_arrays_fold_with_the_product_geometry_functions(name, op):
    body, helpers = sqlbody.l0_source(MAN.functions[name])
    assert f"{op}(ua, ub)" in body and "st_geomfromewkb(" in body
    # a core chip names the answer before any geometry is read, chips of two cells are NULL, an empty fold is NULL
    assert body.index("exists(chips, cb -> cb.core)") < body.index("aggregate(")
    assert "array_distinct" in body and "gbx__bng_wkbornull" in body
    assert (
        name in product_stand_ins.WORKSPACE_ONLY
    )  # judged on the workspace, said once


@pytest.mark.parametrize("name", [BUA, BIA])
def test_the_bng_chip_arrays_write_ewkb_with_srid_27700_like_the_uc_chip_functions(
    name,
):
    """The scalars write EWKB with SRID 27700; so do the array kernels (st_asewkb, a core chip's polygon with the SRID,
    the empty test counting the SRID's four bytes). Plain st_asbinary would drop the SRID.
    """
    body, helpers = sqlbody.l0_source(MAN.functions[name])
    text = body + " ".join(h["sql"] for h in helpers)
    assert "st_asbinary" not in text
    assert body.count("st_asewkb(") == 1 and "27700" in text
    wkbornull = next(h for h in helpers if h["name"] == "gbx__bng_wkbornull")
    assert "length(w) <= 13" in wkbornull["sql"]  # an empty EWKB is 13 bytes
    # every chip is read as BNG: plain WKB chips (SRID 0, as a session writes them) fold to SRID 27700 too. Found on a
    # SQL warehouse: the two-chip rows answered SRID 0, the EWKB-chip rows 27700.
    reads = body.count("st_geomfromewkb(")
    assert reads == 2 and body.count("st_setsrid(st_geomfromewkb(") == reads
    assert body.count(".chip), 27700)") == reads


# --- the BNG cell polygon helper: pure SQL, judged against the kernel -----------------------------------------------------
def _cell_ids():
    import random

    from databricks.labs.gbx.pygx import _bng

    rnd = random.Random(7)
    pts = [
        (530500, 180500),
        (0, 0),
        (699999, 1299999),
        (400000, 800000),
        (12345, 678901),
    ]
    pts += [(rnd.randrange(0, 700000), rnd.randrange(0, 1300000)) for _ in range(40)]
    ids = set()
    for e, n in pts:
        for res in (-6, -5, -4, -3, -2, 1, 2, 3, 4, 5, 6):
            ids.add(_bng.point_as_cell(e, n, res))
    # the sweep must reach all four quadrant suffixes
    assert {i[-2:] for i in ids if i[-2:] in ("SW", "NW", "NE", "SE")} == {
        "SW",
        "NW",
        "NE",
        "SE",
    }
    return sorted(ids)


def test_the_bng_cell_polygon_helper_is_the_kernels_polygon_for_every_cell_shape(spark):
    from databricks.labs.gbx.pygx import _bng

    helpers = sqlbody.l0_source(MAN.functions[BUA])[1]
    expr = sqlbody.inlined("{q}gbx__bng_cellwkt(cellid)", helpers)
    spark.sql(
        f"CREATE OR REPLACE TEMPORARY FUNCTION bng_cellwkt_t(cellid STRING) RETURNS STRING RETURN {expr}"
    )
    try:
        ids = _cell_ids()
        assert len(ids) > 50
        got = dict(
            spark.createDataFrame([(i,) for i in ids], "id string")
            .selectExpr("id", "bng_cellwkt_t(id)")
            .collect()
        )
        wrong = {
            i: (got[i], _bng.cell_id_to_geometry(_bng.parse(i)).wkt)
            for i in ids
            if got[i] is None
            or not shapely.from_wkt(got[i]).equals_exact(
                _bng.cell_id_to_geometry(_bng.parse(i)), 0
            )
        }
        assert wrong == {}
        # a string that is no cell has no polygon
        bad = [
            r[0]
            for r in spark.createDataFrame(
                [
                    ("XX1234",),
                    ("TQ308",),
                    ("TQ3080XX",),
                    ("TQ30801",),
                    ("",),
                    ("T",),
                    (None,),
                ],
                "id string",
            )
            .selectExpr("bng_cellwkt_t(id)")
            .collect()
        ]
        assert bad == [None] * 7
    finally:
        spark.sql("DROP TEMPORARY FUNCTION IF EXISTS bng_cellwkt_t")


# --- the host stand-in for the product ring ---------------------------------------------------------------------------------
def test_the_h3_ring_stand_in_expands_the_call_in_the_text_and_refuses_a_ring_it_did_not_build(
    spark,
):
    text = product_stand_ins.rewrite("transform(a, x -> h3_kringdistances(x.c, 1))")
    assert "h3_kringdistances(" not in text and "CASE (x.c)" in text
    unknown = product_stand_ins.rewrite("h3_kringdistances(CAST(1 AS BIGINT), 1)")
    with pytest.raises(Exception, match="no ring built"):
        spark.sql(f"SELECT {unknown}").collect()
    # a k above the radius it was built to fails loudly, where it used to be a silent NULL; a NULL k is still NULL
    from databricks.labs.gbx.bench.oracle_tables.grid_scalar import H3_CELL

    above = product_stand_ins.rewrite(
        f"h3_kringdistances(CAST({H3_CELL} AS BIGINT), 4)"
    )
    with pytest.raises(Exception, match="no ring built for k = 4"):
        spark.sql(f"SELECT {above}").collect()
    null_k = product_stand_ins.rewrite(
        f"h3_kringdistances(CAST({H3_CELL} AS BIGINT), CAST(NULL AS INT))"
    )
    assert spark.sql(f"SELECT {null_k} AS r").first()["r"] is None
    inside = product_stand_ins.rewrite(
        f"h3_kringdistances(CAST({H3_CELL} AS BIGINT), 1)"
    )
    assert len(spark.sql(f"SELECT {inside} AS r").first()["r"]) == 7
    with pytest.raises(ValueError, match="takes 2 arguments"):
        product_stand_ins.rewrite("h3_kringdistances(x)")


# --- the cap of the Python fills ----------------------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "handler,cells,extra",
    [
        (forms.quadbin_cellfill_array, [(5233961839712272383, 1.0)], ()),
        (forms.bng_cellfill_array, [("TQ3080", 1.0)], ()),
    ],
    ids=["quadbin", "bng"],
)
def test_a_fill_array_over_the_cap_is_refused_with_a_fix_naming_the_aggregate(
    monkeypatch, handler, cells, extra
):
    cells = [{"cellid": c, "value": v} for c, v in cells]
    assert handler(cells, 1, "mean", 2.0)
    monkeypatch.setattr(forms, "DECODED_CAP", 10)
    with pytest.raises(ValueError, match=r"Fix: run gbx_(quadbin|bng)_cellfill in"):
        handler(cells, 1, "mean", 2.0)


def test_a_wrong_k_or_method_raises_whatever_the_cells_are():
    for handler in (forms.quadbin_cellfill_array, forms.bng_cellfill_array):
        with pytest.raises(ValueError, match="Fix: pass k of at least 0"):
            handler([], -1, "mean", 2.0)
        with pytest.raises(ValueError, match="'median'"):
            handler([], 1, "median", 2.0)


# --- the recipes ----------------------------------------------------------------------------------------------------------------
def _recipes():
    from databricks.labs.gbx.bench import oracles

    return [c for c in oracles.TABLES["CELL_AGGREGATE"] if c.sql is not None]


def test_the_h3_fill_recipes_run_on_the_host_and_give_the_functions_answer(
    spark, tmp_path
):
    from databricks.labs.gbx.bench import oracles

    h3_recipes = [c for c in _recipes() if c.cid.startswith("recipe_h3_fill")]
    assert {c.cid for c in h3_recipes} == {"recipe_h3_fill_mean", "recipe_h3_fill_idw"}
    ran = oracles.run_sql_kernels(spark, h3_recipes, tmpdir=str(tmp_path))
    assert [c.cid for c, _ in ran] == [c.cid for c in h3_recipes]  # each one ran
    assert [m for _, m in ran if m] == []


def test_the_union_recipes_are_statements_that_parse(spark):
    """They call the product geometry aggregate and a UC Python function: the workspace runs them; here the text parses."""
    union = [c for c in _recipes() if not c.cid.startswith("recipe_h3_fill")]
    assert len(union) == 3
    parser = spark._jsparkSession.sessionState().sqlParser()
    for c in union:
        parser.parsePlan(c.sql.replace("{q}", ""))
        assert "st_union_agg(" in c.sql


def test_the_python_twin_the_chip_union_is_timed_against_folds_like_the_session_kernel():
    box = shapely.box
    west = {
        "cellid": "TQ3080",
        "core": False,
        "chip": shapely.to_wkb(box(530000, 180000, 530600, 181000)),
    }
    east = {
        "cellid": "TQ3080",
        "core": False,
        "chip": shapely.to_wkb(box(530400, 180000, 531000, 181000)),
    }
    core = {"cellid": "TQ3080", "core": True, "chip": None}
    got = shapely.from_wkb(forms.bng_chips_union_probe([west, None, east]))
    assert got.equals(box(530000, 180000, 531000, 181000))
    whole = shapely.from_wkb(forms.bng_chips_union_probe([west, core]))
    assert whole.equals(box(530000, 180000, 531000, 181000))
    assert shapely.get_srid(whole) == 27700  # as the SQL fold writes it
    assert forms.bng_chips_union_probe(None) is None
    assert forms.bng_chips_union_probe([None]) is None
