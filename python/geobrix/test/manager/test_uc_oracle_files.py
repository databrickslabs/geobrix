"""The files the UC statement runner reads and writes, and the oracle's judgement of the rows it brings back."""

import pytest

from databricks.labs.gbx.bench import oracles

TE, RT = oracles.TE, oracles.RT


def test_scalar_uc_handlers_answer_the_rows_without_spark(tmp_path):
    """The UC handlers on Python values (GEOMETRY stand-in = EWKB bytes until a UC runtime shows what a handler
    receives)."""
    ran = oracles.run_handlers(oracles.MVP_S, tmpdir=str(tmp_path))
    assert [m for _, m in ran if m] == []
    assert len(ran) == sum(
        "uc" in c.surfaces and not oracles.engine_missing(c.fn) for c in oracles.MVP_S
    )


def test_the_uc_table_functions_answer_the_rows_without_spark(tmp_path):
    """The UC table function without Spark: the handler behind it, called with the values UC passes."""
    ran = oracles.run_handlers_rows(oracles.MVP_G, tmpdir=str(tmp_path))
    assert len(ran) == sum(
        c.fn in (TE, RT) and "uc" in c.surfaces for c in oracles.MVP_G
    )
    assert [m for _, m in ran if m] == []


@pytest.mark.parametrize(
    "raw,type_name,want",
    [
        ("TQ3080", "STRING", "TQ3080"),
        ("3", "INT", 3),
        ("NaN", "DOUBLE", float("nan")),
        ("AAH/", "BINARY", b"\x00\x01\xff"),
        (None, "BINARY", None),
        (
            '{"raster": "AAH/", "path": null, "metadata": {}}',
            "STRUCT",
            {"raster": b"\x00\x01\xff", "path": None, "metadata": {}},
        ),
    ],
)
def test_uc_warehouse_cells_decode_to_python_values(raw, type_name, want):
    assert repr(oracles.decode_uc_cell(raw, type_name)) == repr(want)  # nan-safe
    assert oracles.decode_uc_cell("AAH/", "BINARY", decode_binary=False) == "AAH/"


def test_uc_statement_file_and_result_file_round_trip(tmp_path):
    """The files gbx:uc:sql reads and writes: ids survive, bytes survive, and the oracle judges the rows."""
    from shapely import box, set_srid, to_wkb

    pairs = oracles.uc_sql(oracles.MVP_S, "cat", "sch", str(tmp_path))
    assert oracles.parse_uc_sql(oracles.uc_sql_text(pairs)) == pairs
    assert {cid for cid, _ in pairs} == {
        c.cid for c in oracles.MVP_S if "uc" in c.surfaces
    }
    results = {
        "bng_1km": {"value": "TQ3080", "wall_s": [0.2, 0.1]},
        "bng_null_e": {"value": None, "wall_s": [0.1]},
        "bng_bad_res": {
            "error": "[UDF_ERROR] BNG resolution not supported; found '3km'"
        },
        "uc_tcrs_wkt": {"error": "Fix: use the session function gbx_rst_transformcrs"},
        "mv_square_ogc": {
            "value": to_wkb(set_srid(box(0, 0, 1, 1), 4326), include_srid=True)
        },
    }
    path = str(tmp_path / "out.json")
    oracles.dump_uc_results(results, path)
    loaded = oracles.load_uc_results(path)
    assert loaded == results  # bytes come back as bytes
    ran = [c for c in oracles.MVP_S if c.cid in results]
    assert oracles.check_uc_results(ran, loaded, str(tmp_path)) == []
    loaded["bng_1km"]["value"] = "TQ3081"
    (failure,) = oracles.check_uc_results(ran, loaded, str(tmp_path))
    assert failure.startswith("bng_1km")
    # a statement that never ran fails too
    assert "bng_1km_str: no result" in oracles.check_uc_results(
        oracles.MVP_S, loaded, str(tmp_path)
    )


def test_uc_table_function_statements_and_rows_round_trip_for_the_generators(tmp_path):
    """The warehouse form of the UC table function: one statement per case answers its rows as one JSON cell, and
    the oracle judges the rows read back from it (a missing row fails)."""
    import base64
    import json

    from shapely import box, to_wkb

    pairs = dict(oracles.uc_sql(oracles.MVP_G, "cat", "sch", str(tmp_path)))
    assert " FROM `cat`.`sch`.gbx_bng_tessellateexplode(" in pairs["te_offset"]
    poly = box(530100, 180100, 532900, 181900)
    rows = [
        {
            "cellid": f"TQ{x}{y}",
            "core": False,
            "chip": base64.b64encode(to_wkb(poly & box(*c))).decode(),
        }
        for x, y, c in [
            (
                30 + i,
                80 + j,
                (
                    530000 + 1000 * i,
                    180000 + 1000 * j,
                    531000 + 1000 * i,
                    181000 + 1000 * j,
                ),
            )
            for j in (0, 1)
            for i in (0, 1, 2)
        ]
    ]
    results = {
        "te_offset": {"value": json.dumps(rows)},
        "te_null": {"value": "[]"},
        "te_wrong_srid_uc": {
            "error": "gbx_bng_tessellate: SRID 4326. Fix: use the session function"
        },
    }
    ran = [c for c in oracles.MVP_G if c.cid in results]
    assert oracles.check_uc_results(ran, results, str(tmp_path)) == []
    results["te_offset"]["value"] = json.dumps(rows[1:])
    (failure,) = oracles.check_uc_results(ran, results, str(tmp_path))
    assert failure.startswith("te_offset")


def test_a_tile_literal_types_its_null_fields_so_a_cte_column_matches_the_tile_parameter():
    """A NULL field of a tile literal is typed: a CTE column of struct<path: void> does not match the UC tile parameter
    (the recipe_stats_partial_merge row on a real warehouse: DATATYPE_MISMATCH, `path:void`).
    """
    from databricks.labs.gbx import manifest

    (param,) = manifest.load().functions["gbx_rst_stats_partial"].params[:1]
    case = next(c for c in oracles.TABLES["RASTER_STAT"] if c.cid == "stats_partial")
    sql = oracles._sql_literal(
        {"raster": b"\x00", "path": None, "metadata": {}}, param, case
    )
    assert "'path', CAST(NULL AS STRING)" in sql and "'path', NULL" not in sql
    sql = oracles._sql_literal(
        {"raster": None, "path": "/x.tif", "metadata": {}}, param, case
    )
    assert "'raster', CAST(NULL AS BINARY)" in sql and "'path', '/x.tif'" in sql
