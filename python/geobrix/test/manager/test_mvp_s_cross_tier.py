"""The MVP-S oracle: every session row of every manifest function, at every implementation level,
through SQL. The light variants also run through the Python Column API; the JVM levels (Catalyst
and Java UDF) run when the staged JAR is in the session (`gbx-test-parity.sh` builds and stages it).
"""

import importlib

import pytest

from databricks.labs.gbx import manifest, register
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.manager import capabilities

_API_PACKAGE = {"bng": "pygx", "rst": "pyrx", "st": "pyvx"}


def _cases():
    for fn in manifest.load().functions.values():
        if fn.oracle != "MVP_S":
            continue
        for variant in fn.levels["L3"].get("variants") or {"-": None}:
            for api in ("sql", "column"):
                yield pytest.param(
                    fn.sql_name,
                    "L3",
                    variant,
                    api,
                    id=f"{fn.sql_name}-L3{variant}-{api}",
                )
        for level in ("L1", "L2"):  # the Column API reaches a JVM level through SQL
            if level in fn.levels:
                yield pytest.param(
                    fn.sql_name,
                    level,
                    "-",
                    "sql",
                    marks=pytest.mark.integration,
                    id=f"{fn.sql_name}-{level}-sql",
                )


def _column_function(sql_name):
    package = _API_PACKAGE[sql_name.split("_")[1]]
    module = importlib.import_module(f"databricks.labs.gbx.{package}.functions")
    return getattr(module, sql_name[len("gbx_") :])


@pytest.mark.parametrize("fn,level,variant,api", list(_cases()))
def test_mvp_s_oracle_session(request, tmp_path, fn, level, variant, api):
    spark = request.getfixturevalue("spark" if level == "L3" else "jvm_spark")
    register(spark, only=[fn], _levels={fn: (level, variant)})
    rows = [c for c in oracles.MVP_S if c.fn == fn]
    results = oracles.run_session(
        spark,
        rows,
        tmpdir=str(tmp_path),
        column_of=_column_function if api == "column" else None,
    )
    red = {case.cid: msg for case, msg in results if msg}
    # A row a JVM level is known to fail is a strict expected failure: it must fail, and
    # when it starts to pass it comes out of HEAVY_PENDING.
    pending = {
        cid
        for cid, lv in oracles.HEAVY_PENDING.items()
        if level in lv and cid in {c.cid for c in rows}
    }
    assert [msg for cid, msg in red.items() if cid not in pending] == []
    assert pending <= set(red), f"{level} now passes {sorted(pending - set(red))}"


@pytest.mark.integration
def test_l1_arity_contract(jvm_spark):
    installer = jvm_spark._jvm.com.databricks.labs.gbx.kernels.L1Installer
    for fn in manifest.load().functions.values():
        if "L1" in fn.levels:
            want = ",".join(str(n) for n in range(fn.min_args, fn.max_args + 1))
            got = str(
                installer.acceptedArities(
                    fn.levels["L1"]["companion"], fn.min_args, fn.max_args
                )
            )
            assert got == want, fn.sql_name


@pytest.mark.integration
def test_detect_sees_the_jar_and_gdal(jvm_spark):
    caps = capabilities.detect(jvm_spark, need_jvm=True)
    assert caps.jar is not None, caps
    assert caps.jar_reason == "" and caps.gdal_reason == "", caps
