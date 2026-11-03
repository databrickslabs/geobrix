"""Every family oracle table at every implementation level the manifest declares and this environment
offers: L3 forms on the host (through SQL and the Python API), the JVM levels and the GDAL spec engine's
forms in the container with the staged JAR and the native payload. One harness for every manifest row.

Select tables with GBX_ORACLE_TABLES=<TABLE>[,<TABLE2>] (the container commands forward it), or names with
-k "gbx_bng_" (the ids are <sql_name>-<level><variant>-<api>)."""

import os

import pytest

from databricks.labs.gbx import _rs, manifest, register
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.ds.file_gbx import READ_CAP_KEY

# Tables that still have their own cross-tier file. A SPEC_* table of an engine family is never listed here.
_OWN_FILES = {"MVP_S", "MVP_G", "MVP_A", "MVP_H", "MVP_IO", "SPEC_SE"}
_ONLY = {t for t in os.environ.get("GBX_ORACLE_TABLES", "").split(",") if t}
_IMPL_KEYS = ("variants", "factory", "callable", "udtf")


def _l3_forms(l3) -> list:
    forms = list(l3.get("variants") or ())
    if not forms and any(k in l3 for k in _IMPL_KEYS):
        forms = ["-"]
    if l3.get("engine"):
        forms += ["engine-c0", "engine-c1"]
    return forms


def _rows():
    for fn in sorted(manifest.load().functions.values(), key=lambda f: f.sql_name):
        if (
            fn.status != "stable"
            or fn.oracle in _OWN_FILES
            or (_ONLY and fn.oracle not in _ONLY)
        ):
            continue
        for variant in _l3_forms(fn.levels.get("L3", {})):
            engine = variant.startswith("engine")
            # the Python API leg needs a Python entry: a Column function, or for a generator the DataFrame-first
            # function (`_api.table`), run over the same frame as the SQL leg
            apis = ("sql",) if fn.python is None else ("sql", "python")
            for api in apis:
                marks = [pytest.mark.integration] if engine else []
                yield pytest.param(
                    fn.sql_name,
                    "L3",
                    variant,
                    api,
                    marks=marks,
                    id=f"{fn.sql_name}-L3{variant}-{api}",
                )
        l1 = fn.levels.get("L1", {})
        for variant in (["-"] if "companion" in l1 else []) + (
            ["engine"] if l1.get("engine") else []
        ):
            yield pytest.param(
                fn.sql_name,
                "L1",
                variant,
                "sql",
                marks=pytest.mark.integration,
                id=f"{fn.sql_name}-L1{variant}-sql",
            )
        if "L2" in fn.levels:
            yield pytest.param(
                fn.sql_name,
                "L2",
                "-",
                "sql",
                marks=pytest.mark.integration,
                id=f"{fn.sql_name}-L2-sql",
            )


def _run(spark, fn, cases, tmpdir, column_of=None):
    if oracles.has_groups(
        fn
    ):  # an aggregate, or a table function over TABLE(...) PARTITION BY
        return oracles.run_groups(spark, cases, tmpdir=tmpdir, column_of=column_of)
    if fn.kind == "generator":
        return oracles.run_rows(spark, cases, tmpdir=tmpdir, table_of=column_of)
    return oracles.run_session(spark, cases, tmpdir=tmpdir, column_of=column_of)


ROWS = list(_rows())


@pytest.mark.parametrize("sql_name,level,variant,api", ROWS)
def test_family_oracle(request, tmp_path, sql_name, level, variant, api):
    if (level, variant) == ("L3", "d") and _rs.unavailable_reason():
        pytest.skip(_rs.unavailable_reason())
    if variant.startswith("engine"):
        from databricks.labs.gbx.engine import run

        if level == "L3" and run.unavailable_reason():
            pytest.skip(run.unavailable_reason())
    fn = manifest.load().functions[sql_name]
    jvm = level != "L3" or variant.startswith("engine")
    spark = request.getfixturevalue("jvm_spark" if jvm else "spark")
    register(spark, only=[sql_name], _levels={sql_name: (level, variant)})
    cases = [
        c
        for c in oracles.TABLES[fn.oracle]
        if c.fn == sql_name and ("session" in c.surfaces or "python" in c.surfaces)
    ]
    column_of = (
        (lambda f: manifest.resolve_ref(manifest.load().functions[f].python.ref))
        if api == "python"
        else None
    )
    capped = [c for c in cases if c.cid in oracles.READ_CAP_ROWS]
    ran = _run(
        spark, fn, [c for c in cases if c not in capped], str(tmp_path), column_of
    )
    # the decoded-size gate, with the session's read cap lowered for the row: the JVM reads the key per query, a
    # light function carries it from its registration
    for c in capped:
        spark.conf.set(READ_CAP_KEY, str(oracles.READ_CAP_ROWS[c.cid]))
        try:
            register(spark, only=[sql_name], _levels={sql_name: (level, variant)})
            ran += _run(spark, fn, [c], str(tmp_path), column_of)
        finally:
            spark.conf.unset(READ_CAP_KEY)
            register(spark, only=[sql_name], _levels={sql_name: (level, variant)})
    red = {c.cid: m for c, m in ran if m}
    pending = {
        cid
        for cid, lv in oracles.HEAVY_PENDING.items()
        if level in lv and cid in {c.cid for c in cases}
    }
    bad = [m for cid, m in red.items() if cid not in pending]
    assert not bad, "\n".join(
        m[-500:] for m in bad
    )  # every red row, not pytest's cut-down list
    assert pending <= set(red), f"{level} now passes {sorted(pending - set(red))}"


@pytest.mark.integration
def test_l1_arity_contract_for_every_companion(jvm_spark):
    installer = jvm_spark._jvm.com.databricks.labs.gbx.kernels.L1Installer
    for fn in manifest.load().functions.values():
        if "companion" in fn.levels.get("L1", {}):
            want = ",".join(str(n) for n in range(fn.min_args, fn.max_args + 1))
            got = installer.acceptedArities(
                fn.levels["L1"]["companion"], fn.min_args, fn.max_args
            )
            assert str(got) == want, fn.sql_name


# --- Unity Catalog: the handler behind every UC row of the slice tables, on Python values (no Spark) ---------------
# The warehouse statements of the same rows are run by bench.probes.run_uc_slice; a function that has a Scala handler
# is answered by the same rows in KernelFacadesSmokeTest (the JAR, in the container).
_UC_TABLES = ("GRID_SCALAR", "GRID_ARRAY", "GRID_GENERATOR", "VECTOR_SCALAR")


def _uc_fns(table):
    man = manifest.load()
    return sorted(
        {
            c.fn
            for c in oracles.TABLES[table]
            if "uc" in c.surfaces and "L4" in man.functions[c.fn].levels
        }
    )


UC_ROWS = [pytest.param(t, fn, id=f"{fn}-L4") for t in _UC_TABLES for fn in _uc_fns(t)]


@pytest.mark.parametrize("table,sql_name", UC_ROWS)
def test_family_oracle_uc_handlers(tmp_path, table, sql_name):
    cases = [c for c in oracles.TABLES[table] if c.fn == sql_name]
    fn = manifest.load().functions[sql_name]
    run = (
        oracles.run_handlers_rows
        if "tvf_over" in fn.levels["L4"]
        else oracles.run_handlers
    )
    ran = run(cases, tmpdir=str(tmp_path))
    assert ran and sum("uc" in c.surfaces for c in cases) == len(ran)
    assert [m for _, m in ran if m] == []


def test_every_uc_function_of_the_slice_has_its_uc_rows():
    """Each L4 function of the grid and vector tables has UC rows: a value, a NULL, and (for a geometry argument) a
    refusal or SRID row; the refusal of a BINARY for a GEOMETRY and the SRID of the BNG cell geometry are named rows.
    """
    man = manifest.load()
    for t in _UC_TABLES:
        for name in _uc_fns(t):
            rows = [c for c in oracles.TABLES[t] if c.fn == name and "uc" in c.surfaces]
            kinds = {c.kind for c in rows}
            assert "null" in kinds or any(c.kind in ("no_rows",) for c in rows), name
            if any(
                p.uc == "GEOMETRY" for p in man.functions[name].params
            ) and name.split("_")[1] in ("bng", "h3", "quadbin"):
                assert "uc_refused" in kinds, name
    ids = {c.cid for c in oracles.TABLES["GRID_SCALAR"] if "uc" in c.surfaces}
    assert {"uc_wkb_into_geometry_refused", "uc_bng_aswkb_srid"} <= ids


# The bytes of gbx_bng_aswkb / gbx_bng_centroid for TQ3080 (little-endian EWKB, SRID 27700, the ring from the south-west
# corner): the UC Python handler here, the UC Scala handler in KernelFacadesSmokeTest, one expected hex.
TQ3080_ASWKB_HEX = (
    "0103000020346c0000010000000500000000000000a02c20410000000000f9054100000000703420410000000000f9054100"
    "00000070342041000000004018064100000000a02c2041000000004018064100000000a02c20410000000000f90541"
)
TQ3080_CENTROID_HEX = "0101000020346c0000000000008830204100000000a0080641"


def test_the_uc_python_handlers_write_the_bytes_the_uc_scala_handlers_write():
    from databricks.labs.gbx.uc import handlers

    assert handlers.bng_aswkb("TQ3080").hex() == TQ3080_ASWKB_HEX
    assert handlers.bng_centroid("TQ3080").hex() == TQ3080_CENTROID_HEX


SLICE = {
    "gbx_bng_pointascell", "gbx_bng_polyfill", "gbx_bng_aswkb", "gbx_bng_centroid", "gbx_bng_kring",
    "gbx_bng_geomkring", "gbx_bng_cellarea", "gbx_quadbin_pointascell", "gbx_quadbin_polyfill",
    "gbx_h3_geomkring", "gbx_st_explainvalidity", "gbx_st_legacyaswkb", "gbx_st_wrapx",
}  # fmt: skip
TVFS = {"gbx_bng_kringexplode", "gbx_bng_geomkringexplode", "gbx_h3_geomkringexplode"}


def test_the_uc_slice_probe_runs_every_slice_function_and_its_table_functions():
    """bench.probes.run_uc_slice (run on the warehouse and Serverless env 6 in Task 28) takes these rows."""
    from databricks.labs.gbx.bench import probes

    cases = probes.uc_slice_cases()
    assert SLICE | TVFS <= {c.fn for c in cases}
    # statements it would send: every scalar row once, with 3-level names
    sql = oracles.uc_sql([c for c in cases if c.fn in SLICE], "cat", "sch", "/tmp")
    assert len(sql) == len([c for c in cases if c.fn in SLICE])
    assert all("`cat`.`sch`.gbx_" in stmt for _, stmt in sql)
    refused = dict(sql)["uc_wkb_into_geometry_refused"]
    assert (
        "X'" in refused and "st_geomfromwkt" not in refused
    )  # a BINARY goes in as BINARY
    legacy = dict(sql)["leg_polygon_hole"]
    assert "named_struct('typeId', 5, 'srid', 0" in legacy and "st_asewkb(" in legacy
