"""Tests for the Unity Catalog functions page examples (run locally, no workspace needed)."""

import importlib.util
import inspect
import re
from pathlib import Path

import pytest

_path = Path(__file__).parent / "uc_functions_examples.py"
_spec = importlib.util.spec_from_file_location("uc_functions_examples", _path)
ex = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ex)

from databricks.labs.gbx import __version__, manifest  # noqa: E402
from databricks.labs.gbx.uc import sqlbody  # noqa: E402

VERSION = __version__.split("+", 1)[0]


def _uc_names():
    return {n for n, f in manifest.load().functions.items() if "L4" in f.levels}


def test_the_install_preview_is_one_create_per_function_plus_default_wrappers():
    statements = ex.preview_install()
    man = manifest.load()
    expected = 0
    helpers = set()  # a SQL kernel also installs the private SQL helpers it calls, once each (two kernels may share one)
    for n in _uc_names():
        fn = man.functions[n]
        # a SQL kernel carries its defaults itself; a Python or Scala handler sits behind a SQL wrapper that does
        wrapped = any(p.default for p in fn.params) and not {"tvf_over", "sql"} & set(fn.levels["L4"])
        expected += 2 if wrapped else 1
        kernel = sqlbody.l4_source(fn)
        helpers.update(h["name"] for h in (kernel[1] if kernel else ()))
    expected += len({f.levels["L4"]["tvf_over"] for f in man.functions.values() if f.levels.get("L4", {}).get("tvf_over") in man.private})
    expected += len(helpers)
    assert len(statements) == expected
    assert all(s.startswith("CREATE OR REPLACE FUNCTION `main`.`geobrix`.") for s in statements)
    assert not any("LANGUAGE SCALA" in s for s in statements)
    assert any("GEOMETRY(ANY)" in s for s in statements)


def test_every_function_carries_a_version_comment():
    statements = ex.preview_install()
    assert all(f"COMMENT 'geobrix {VERSION}: " in s for s in statements)


def test_the_environment_is_pinned_to_env_6():
    python_statements = [s for s in ex.preview_install() if "LANGUAGE PYTHON" in s]
    assert python_statements
    assert all("environment_version = '6'" in s for s in python_statements)
    assert all("+env6-py3-none-any.whl" in s for s in python_statements)


def test_a_jar_makes_the_jvm_only_functions_scala():
    statements = ex.preview_install_with_jar(VERSION)
    scala = [s for s in statements if "LANGUAGE SCALA" in s]
    man = manifest.load()
    # a row with a SQL kernel keeps its Scala form for a forced install, but the install makes it SQL
    expected = {
        n
        for n in _uc_names()
        if "scala" in man.functions[n].levels["L4"] and "sql" not in man.functions[n].levels["L4"]
    }
    assert len(scala) == len(expected) == 3
    for name in expected:
        assert any(f".{name}(" in s for s in scala)
    assert all(f"geobrix-{VERSION}-jar-with-dependencies.jar" in s for s in scala)


def test_only_installs_the_table_function_and_the_function_it_calls():
    statements = ex.install_only_the_bng_cells()
    text = "\n".join(statements)
    assert ".gbx_bng_kring(" in text
    assert ".gbx_bng_kringexplode(" in text
    assert "gbx_bng_pointascell" not in text


def test_the_default_install_shares_one_package_list_and_per_function_does_not():
    import re

    def lists(statements):
        return {m for s in statements for m in re.findall(r"dependencies = '(\[.*?\])'", s)}

    assert len(lists(ex.preview_install())) == 1
    assert len(lists(ex.install_with_each_function_its_own_packages())) > 1


def test_table_functions_are_sql_bodies_over_the_array_function():
    explode = [s for s in ex.preview_install() if ".gbx_bng_kringexplode(" in s]
    assert len(explode) == 1
    assert "RETURNS TABLE (cellid STRING)" in explode[0]
    assert "explode(" in explode[0]


PAGE = Path(__file__).parents[3] / "docs" / "api" / "uc-functions.mdx"


def _listed_on_the_page():
    """The function names in the tables under "What UC provides". Prose may name other functions, such as a Fix."""
    text = PAGE.read_text()
    section = text.split("## What UC provides", 1)[1].split("## Python and Scala handlers", 1)[0]
    rows = "\n".join(line for line in section.splitlines() if line.startswith("|"))
    return set(re.findall(r"`(gbx_[a-z0-9]+(?:_[a-z0-9]+)*)`", rows))


def test_the_slice_the_page_lists_is_the_manifest_slice():
    names = _uc_names()
    listed = _listed_on_the_page()
    assert names - listed == set(), "the page does not list these UC functions"
    assert listed - names == set(), "the page lists these functions, which have no UC level"
    assert len([n for n in names if n.startswith("gbx_rst_")]) > 90


def _split_args(call_args):
    depth, parts, cur = 0, [], ""
    for ch in call_args:
        if ch in "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    parts.append(cur)
    return [p for p in parts if p.strip()]


@pytest.mark.parametrize(
    "sql",
    [
        ex.UC_BNG_POINTASCELL_SQL,
        ex.UC_BNG_KRING_SQL,
        ex.UC_QUADBIN_POLYFILL_SQL,
        ex.UC_KRINGEXPLODE_SQL,
        ex.UC_WKB_TO_GEOMETRY_SQL,
        ex.UC_TRANSFORMCRS_SQL,
        ex.UC_RASTER_MERGE_SQL,
    ],
)
def test_sql_examples_call_a_uc_function_with_a_valid_argument_count(sql):
    man = manifest.load()
    m = re.search(r"main\.geobrix\.(gbx_\w+)\(", sql)
    assert m, sql
    name = m.group(1)
    assert name in _uc_names()
    start = m.end()
    depth, i = 1, start
    while depth:
        depth += {"(": 1, ")": -1}.get(sql[i], 0)
        i += 1
    args = _split_args(sql[start : i - 1])
    fn = man.functions[name]
    assert fn.min_args <= len(args) <= len(fn.params)


def test_the_geometry_examples_use_the_srid_of_their_grid():
    assert ", 27700)" in ex.UC_BNG_POINTASCELL_SQL
    assert ", 4326)" in ex.UC_QUADBIN_POLYFILL_SQL


def test_set_path_is_the_line_verify_prints():
    from databricks.labs.gbx import uc

    source = inspect.getsource(uc.verify)
    assert "SET PATH = {qc}.{qs}, system.session, system.builtin, system.ai, CURRENT_SCHEMA" in source
    assert ex.SET_PATH_SQL == "SET PATH = `main`.`geobrix`, system.session, system.builtin, system.ai, CURRENT_SCHEMA"


def test_register_target_uc_is_the_verify_call():
    import databricks.labs.gbx.manager as manager

    source = inspect.getsource(manager.register)
    assert "target == \"uc\"" in source and "uc.verify(" in source


def test_crs_forms():
    forms = ex.crs_forms()
    assert [(f.authority, f.srid) for f in forms] == [("EPSG", 4326), ("EPSG", 4326), ("ESRI", 102100)]


def test_any_other_crs_is_refused_with_a_fix():
    message = ex.crs_refused()
    assert "Fix:" in message
    assert "gbx_rst_transformcrs" in message


def test_corrupt_data_is_null():
    assert ex.bad_data_is_null() is None


def test_a_bad_parameter_raises():
    message = ex.bad_parameter_is_refused()
    assert "99" in message


@pytest.mark.parametrize("sql", [ex.UC_RASTER_HEADER_SQL, ex.UC_RASTER_TILE_SQL])
def test_raster_sql_examples_call_uc_functions_with_a_valid_argument_count(sql):
    man = manifest.load()
    calls = re.findall(r"main\.geobrix\.(gbx_\w+)\(([^()]*)\)", sql)
    assert len(calls) >= 2
    for name, args in calls:
        assert name in _uc_names()
        fn = man.functions[name]
        assert fn.min_args <= len(_split_args(args)) <= len(fn.params)


def test_the_raster_to_grid_example_is_the_oracle_recipe_called_in_a_subquery():
    """The page merges the partial state of each tile as the oracle's recipe does (judged against the session statistic of
    the whole tile), and calls the partial in a subquery: its public name is a SQL function, which UC refuses in a generator."""
    from databricks.labs.gbx.bench.oracle_tables import raster_to_grid as rtg

    flat = " ".join(ex.UC_RASTER_TO_GRID_SQL.replace("main.geobrix.", "").split())
    recipe = next(c for c in rtg.RASTER_TO_GRID if c.cid == "recipe_h3_merge_variance").sql
    assert (
        "try_divide(sum(m2) + sum(count * pow(try_divide(sum, count) - mu, 2)), sum(count)) AS variance" in flat
    )
    assert "sum(m2) + sum(count * pow(try_divide(sum, count) - mu, 2))" in recipe
    assert "FROM (SELECT" not in flat and "inline(cells)" in flat
    assert "gbx_rst_h3_rastertogrid_partial(tile, 7) AS cells FROM rasters" in flat
    man = manifest.load()
    (name, args), = re.findall(r"main\.geobrix\.(gbx_\w+)\(([^()]*)\)", ex.UC_RASTER_TO_GRID_SQL)
    assert name in _uc_names()
    assert man.functions[name].min_args <= len(_split_args(args)) <= len(man.functions[name].params)


@pytest.mark.parametrize(
    "sql", [ex.UC_RASTER_RETILE_SQL, ex.UC_RASTER_BANDS_SQL, ex.UC_RASTER_TESSELLATE_SQL, ex.UC_RASTER_PYRAMID_SQL]
)
def test_the_tile_splitting_examples_call_a_table_function_with_a_valid_argument_count(sql):
    man = manifest.load()
    ((name, args),) = re.findall(r"main\.geobrix\.(gbx_\w+)\(([^()]*)\)", sql)
    assert name in _uc_names() and man.functions[name].kind == "generator"
    assert man.functions[name].min_args <= len(_split_args(args)) <= len(man.functions[name].params)
    assert "LATERAL" in sql


def test_the_triangulate_example_passes_geometry_arrays_to_a_table_function():
    man = manifest.load()
    name = "gbx_st_triangulate"
    args = ex.UC_VECTOR_TRIANGULATE_SQL.split(f"main.geobrix.{name}(", 1)[1].rsplit(") AS t", 1)[0]
    assert man.functions[name].min_args <= len(_split_args(args)) <= len(man.functions[name].params)
    assert man.functions[name].kind == "generator" and "LATERAL" in ex.UC_VECTOR_TRIANGULATE_SQL
    assert "CAST(geom AS GEOMETRY(ANY))" in ex.UC_VECTOR_TRIANGULATE_SQL and "ARRAY<GEOMETRY(ANY)>" in args


def test_the_terrain_example_has_a_valid_argument_count_and_geometry_arrays():
    man = manifest.load()
    name = "gbx_rst_dtmfromgeoms"
    args = ex.UC_RASTER_DTM_SQL.split(f"main.geobrix.{name}(", 1)[1].rsplit(") AS dtm", 1)[0]
    assert man.functions[name].min_args <= len(_split_args(args)) <= len(man.functions[name].params)
    assert "CAST(geom AS GEOMETRY(ANY))" in args and "ARRAY<GEOMETRY(ANY)>" in args


def test_the_z_of_a_geometry_point_reaches_the_terrain():
    assert ex.dtm_from_geometry_points() == 15.0


def test_a_tile_over_256_mib_is_refused_by_retile_with_a_fix():
    message = ex.retile_over_the_limit()
    assert "the tile is 262 MiB decoded, over the 256 MiB" in message
    assert "Fix: run gbx_rst_retile in a notebook or job session" in message


def test_retile_cuts_a_tile_over_64_mib_that_a_pixel_function_refuses():
    count, message = ex.retile_a_tile_over_the_pixel_limit()
    assert count == 9
    assert "the tile is 67 MiB decoded, over the 64 MiB" in message


def test_the_four_tile_splitting_functions_are_table_functions_over_array_routines():
    man = manifest.load()
    for name in ("gbx_rst_retile", "gbx_rst_maketiles", "gbx_rst_tooverlappingtiles", "gbx_rst_separatebands"):
        stmts = [s for s in ex.preview_install() if f".{name}(" in s and s.startswith("CREATE OR REPLACE FUNCTION")]
        assert len(stmts) == 1, name
        private = man.functions[name].levels["L4"]["tvf_over"]
        assert f"RETURN SELECT inline(`main`.`geobrix`.{private}(" in stmts[0]
        assert any(s.startswith(f"CREATE OR REPLACE FUNCTION `main`.`geobrix`.{private}(") for s in ex.preview_install())


@pytest.mark.parametrize(
    "sql",
    [ex.UC_VECTOR_SNAP_SQL, ex.UC_VECTOR_COVERAGE_SQL, ex.UC_VECTOR_MVT_SQL, ex.UC_RASTER_FILES_SQL],
)
def test_the_vector_and_file_examples_call_uc_functions_with_a_valid_argument_count(sql):
    man = manifest.load()
    calls = re.findall(r"main\.geobrix\.(gbx_\w+)\(", sql)
    assert calls and all(name in _uc_names() for name in calls)
    for name in calls:
        start = sql.index(f"main.geobrix.{name}(") + len(f"main.geobrix.{name}(")
        depth, end = 1, start
        while depth:
            depth += {"(": 1, ")": -1}.get(sql[end], 0)
            end += 1
        count = len(_split_args(sql[start : end - 1]))
        assert man.functions[name].min_args <= count <= len(man.functions[name].params), name


def test_two_geometries_in_different_srids_are_refused_with_a_fix():
    message = ex.snap_across_srids_is_refused()
    assert "4326 and 3857" in message and "Fix: st_transform one of them" in message


def test_feature_attributes_that_are_a_struct_cast_to_text_are_named_as_such():
    message = ex.feature_attributes_must_be_json()
    assert "STRUCT cast to text" in message and "Fix: pass to_json(struct(" in message


def test_an_overview_level_below_2_raises_with_a_fix():
    message = ex.overview_levels_must_be_2_or_more()
    assert "each level must be >= 2" in message and "Fix: " in message


def test_the_eight_h3_statistics_are_table_functions_each_over_a_kernel_of_its_own():
    man = manifest.load()
    tvfs = [n for n in _uc_names() if n.startswith("gbx_rst_h3_rastertogrid") and n != "gbx_rst_h3_rastertogrid_partial"]
    stats = sorted(t[len("gbx_rst_h3_rastertogrid"):] for t in tvfs)
    assert stats == ["avg", "count", "max", "median", "min", "stddev", "sum", "variance"]
    for t in tvfs:
        assert man.functions[t].levels["L4"]["tvf_over"] == f"gbx__rst_h3_rastertogrid{t[len('gbx_rst_h3_rastertogrid'):]}_cells"
    stmts = [s for s in ex.preview_install() if ".gbx_rst_h3_rastertogridvariance(" in s]
    assert "gbx__rst_h3_rastertogridvariance_cells(tile, resolution, coverage, assignment)" in stmts[0]


def test_a_virtual_tile_is_refused_with_a_fix_naming_the_session_function():
    message = ex.virtual_tile_is_refused()
    assert "virtual tiles" in message
    assert "Fix: use the session function gbx_rst_avg" in message


def test_a_tile_over_64_mib_is_refused_by_a_pixel_function_and_a_header_function_answers():
    width, message = ex.tile_over_the_limit()
    assert width == 4200
    assert "the tile is 67 MiB decoded, over the 64 MiB" in message
    assert "Fix: run gbx_rst_avg in a notebook or job session, or retile first" in message


def test_the_procedures_install_after_the_functions_they_call():
    plain = ex.preview_install()
    statements = ex.preview_procedures()
    assert statements[: len(plain)] == plain
    procedures = [s for s in statements if s.startswith("CREATE OR REPLACE PROCEDURE")]
    assert procedures and all("SQL SECURITY INVOKER" in s for s in procedures)
    assert all(f"COMMENT 'geobrix {VERSION}: " in s for s in procedures)


def test_the_procedures_the_page_lists_are_the_procedures_installed():
    section = PAGE.read_text().split("## Procedures", 1)[1].split("## Names and the path", 1)[0]
    rows = "\n".join(line for line in section.splitlines() if line.startswith("|"))
    listed = set(re.findall(r"`(gbx_[a-z0-9_]+)`", rows))
    built = {re.search(r"PROCEDURE `main`\.`geobrix`\.(\w+)\(", s).group(1) for s in ex.preview_procedures() if "PROCEDURE" in s.split("(")[0]}
    assert built <= listed
    assert {n for n in listed if n.startswith("gbx_zarr_") or n in built} == listed


def test_the_call_example_names_a_procedure_and_its_parameters():
    from databricks.labs.gbx.uc import procedures

    assert "main.geobrix.gbx_polygons_to_bng(" in ex.CALL_POLYGONS_TO_BNG_SQL
    text = procedures.RECIPES["gbx_polygons_to_bng"]("main", "geobrix")
    for param in ("source_table", "target_table", "bng_resolution"):
        assert f"IN {param} STRING" in text and f"{param} =>" in ex.CALL_POLYGONS_TO_BNG_SQL


def _flat(sql):
    """The statement on one line, with no space inside a bracket: layout is not part of the recipe."""
    text = " ".join(sql.replace("{q}", "main.geobrix.").split())
    return re.sub(r"\s+\)", ")", re.sub(r"\(\s+", "(", text))


def test_the_h3_fill_recipe_is_the_oracle_recipe_with_the_mean_fill():
    from databricks.labs.gbx.bench.oracle_tables import cell_aggregate as ca

    oracle = _flat(ca._C1.format(rows="X", ring=1, value="mean_v"))
    doc = _flat(ex.UC_H3_FILL_SQL)
    for part in (
        "g AS (SELECT grp, cellid, max(value) AS value FROM",
        "holes AS (SELECT grp, cellid FROM g WHERE value IS NULL)",
        "LATERAL VIEW inline(h3_kringdistances(h.cellid, 1)) n WHERE n.distance >= 1",
        "avg(g.value) AS mean_v",
        "JOIN g ON g.grp = nb.grp AND g.cellid = nb.nb AND g.value IS NOT NULL GROUP BY nb.grp, nb.cellid",
        "SELECT g.grp, g.cellid, coalesce(g.value, f.mean_v) AS value FROM g",
        "LEFT JOIN f ON f.grp = g.grp AND f.cellid = g.cellid",
    ):
        assert part in doc and part in oracle, part


def test_the_union_recipes_call_the_cell_polygon_function_the_oracle_rows_call():
    from databricks.labs.gbx.bench.oracle_tables import cell_aggregate as ca

    assert "st_union_agg(st_geomfromewkb(main.geobrix.gbx_quadbin_aswkb(cell)))" in _flat(ex.UC_QUADBIN_UNION_SQL)
    assert "st_union_agg(st_geomfromewkb({q}gbx_quadbin_aswkb(c)))" in ca._C2.replace("{{q}}", "{q}")
    chips = _flat(ex.UC_BNG_CHIP_UNION_SQL)
    oracle = _flat(ca._C3.replace("{{q}}", "{q}"))
    for part in (
        "CASE WHEN bool_or(core) THEN st_asbinary(st_geomfromewkb(main.geobrix.gbx_bng_aswkb(cellid)))",
        "ELSE st_asbinary(st_union_agg(st_geomfromewkb(chip))) END AS union_wkb",
    ):
        assert part in chips
        assert part.replace("AS union_wkb", "AS out") in oracle


def test_the_binpoints_recipe_is_the_oracle_statement():
    from databricks.labs.gbx.bench.oracle_tables import raster_aggregate as ra

    oracle = _flat(ra.d1_sql("max", source="points", canvas="0.0, 0.0, 10.0, 10.0, 10, 10"))
    assert _flat(ex.UC_BINPOINTS_SQL).replace("AS tile FROM c", "AS out FROM c") == oracle


def test_the_by_key_calls_name_the_parameters_of_their_procedures():
    from databricks.labs.gbx.uc import procedures

    for name, sql, params in (
        ("gbx_mosaic_by_key", ex.CALL_MOSAIC_BY_KEY_SQL, ("source_table", "target_table")),
        ("gbx_combine_by_key", ex.CALL_COMBINE_BY_KEY_SQL, ("source_table", "target_table", "op")),
    ):
        text = procedures.RECIPES[name]("main", "geobrix")
        assert f"main.geobrix.{name}(" in sql
        for param in params:
            assert f"IN {param} STRING" in text and f"{param} =>" in sql


def test_the_ingest_headers_are_session_accessors_with_one_argument():
    man = manifest.load()
    calls = re.findall(r"\b(gbx_rst_\w+)\(tile\)", ex.HEADERS_AT_INGEST_SQL)
    assert len(calls) == 4 and "main.geobrix.gbx_" not in ex.HEADERS_AT_INGEST_SQL
    for name in calls:
        assert man.functions[name].min_args == 1 and name in _uc_names()


def test_the_read_files_route_reaches_a_uc_function_with_two_arguments():
    sql = ex.UC_READ_FILES_SQL
    assert "format => 'binaryFile'" in sql
    ((name, args),) = re.findall(r"main\.geobrix\.(gbx_\w+)\(([^()]*)\)", sql)
    fn = manifest.load().functions[name]
    assert name == "gbx_rst_fromcontent" and name in _uc_names()
    assert fn.min_args <= len(_split_args(args)) <= len(fn.params)
    # the limit the page states is the decode cap
    from databricks.labs.gbx.pyrx.core.budget import UC_DECODED_CAP, UC_WINDOWED_DECODED_CAP

    assert UC_DECODED_CAP == 64 << 20 and UC_WINDOWED_DECODED_CAP == 256 << 20


def test_the_routes_table_lists_each_session_function_with_a_stub_text():
    text = PAGE.read_text()
    region = text.split("BEGIN GENERATED uc-routes", 1)[1].split("END GENERATED uc-routes", 1)[0]
    listed = set(re.findall(r"^\| `(gbx_\w+)` \|", region, re.M))
    stubs = {n for n, f in manifest.load().functions.items() if f.stub}
    assert listed == stubs and len(stubs) == 30
    assert not stubs & _uc_names()
