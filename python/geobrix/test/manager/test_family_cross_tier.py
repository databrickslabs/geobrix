"""Every family oracle table at every implementation level the manifest declares and this environment
offers: L3 forms on the host (through SQL and the Python API), the JVM levels and the GDAL spec engine's
forms in the container with the staged JAR and the native payload. One harness for every manifest row.

Select tables with GBX_ORACLE_TABLES=<TABLE>[,<TABLE2>] (the container commands forward it), or names with
-k "gbx_bng_" (the ids are <sql_name>-<level><variant>-<api>)."""

import os

import pytest

from databricks.labs.gbx import _rs, manifest, register
from databricks.labs.gbx.bench import oracles

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
            # the Python API leg needs a Python entry, and a generator is a DataFrame call (not a Column)
            apis = (
                ("sql",)
                if fn.kind == "generator" or fn.python is None
                else ("sql", "python")
            )
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
    if fn.kind == "generator":
        return oracles.run_rows(spark, cases, tmpdir=tmpdir)
    if fn.kind == "aggregate":
        return oracles.run_groups(spark, cases, tmpdir=tmpdir, column_of=column_of)
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
        if c.fn == sql_name and "session" in c.surfaces
    ]
    column_of = (
        (lambda f: manifest.resolve_ref(manifest.load().functions[f].python.ref))
        if api == "python"
        else None
    )
    red = {c.cid: m for c, m in _run(spark, fn, cases, str(tmp_path), column_of) if m}
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
