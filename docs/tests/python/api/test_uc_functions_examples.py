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

VERSION = __version__.split("+", 1)[0]


def _uc_names():
    return {n for n, f in manifest.load().functions.items() if "L4" in f.levels}


def test_the_install_preview_is_one_create_per_function_plus_default_wrappers():
    statements = ex.preview_install()
    man = manifest.load()
    expected = 0
    for n in _uc_names():
        fn = man.functions[n]
        expected += 2 if any(p.default for p in fn.params) and "tvf_over" not in fn.levels["L4"] else 1
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
    expected = {n for n in _uc_names() if "scala" in man.functions[n].levels["L4"]}
    assert len(scala) == len(expected) == 5
    for name in expected:
        assert any(f".{name}(" in s for s in scala)
    assert all(f"geobrix-{VERSION}-jar-with-dependencies.jar" in s for s in scala)


def test_only_installs_the_table_function_and_the_function_it_calls():
    statements = ex.install_only_the_bng_cells()
    text = "\n".join(statements)
    assert ".gbx_bng_kring(" in text
    assert ".gbx_bng_kringexplode(" in text
    assert "gbx_bng_pointascell" not in text


def test_table_functions_are_sql_bodies_over_the_array_function():
    explode = [s for s in ex.preview_install() if ".gbx_bng_kringexplode(" in s]
    assert len(explode) == 1
    assert "RETURNS TABLE (cellid STRING)" in explode[0]
    assert "explode(" in explode[0]


def test_the_slice_the_page_lists_is_the_manifest_slice():
    names = _uc_names()
    listed = {
        "gbx_bng_pointascell", "gbx_bng_polyfill", "gbx_bng_aswkb", "gbx_bng_centroid", "gbx_bng_kring",
        "gbx_bng_geomkring", "gbx_bng_cellarea", "gbx_bng_eastnorthasbng", "gbx_bng_tessellate",
        "gbx_quadbin_pointascell", "gbx_quadbin_polyfill", "gbx_h3_geomkring",
        "gbx_st_explainvalidity", "gbx_st_legacyaswkb", "gbx_st_makevalid", "gbx_st_wrapx",
        "gbx_rst_transformcrs",
        "gbx_bng_kringexplode", "gbx_bng_geomkringexplode", "gbx_bng_tessellateexplode", "gbx_h3_geomkringexplode",
    }
    assert names == listed


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
