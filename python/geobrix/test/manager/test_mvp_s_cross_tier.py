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
        if "L1" in fn.levels and fn.accepts_virtual_tile:
            # The Catalyst function itself, without the dispatch wrapper in front of it: the error-tile
            # rows must hold for a RasterX function that receives an error tile directly.
            yield pytest.param(
                fn.sql_name,
                "L1",
                "raw",
                "sql",
                marks=pytest.mark.integration,
                id=f"{fn.sql_name}-L1-raw-sql",
            )


def _column_function(sql_name):
    package = _API_PACKAGE[sql_name.split("_")[1]]
    module = importlib.import_module(f"databricks.labs.gbx.{package}.functions")
    return getattr(module, sql_name[len("gbx_") :])


@pytest.mark.parametrize("fn,level,variant,api", list(_cases()))
def test_mvp_s_oracle_session(request, tmp_path, fn, level, variant, api):
    spark = request.getfixturevalue("spark" if level == "L3" else "jvm_spark")
    raw = variant == "raw"
    register(spark, only=[fn], _levels={fn: (level, "-" if raw else variant)})
    rows = [
        c for c in oracles.MVP_S if c.fn == fn and (not raw or c.kind == "error_tile")
    ]
    results = oracles.run_session(
        spark,
        rows,
        name_of=(lambda f: f"gbx__l1_{f[len('gbx_'):]}") if raw else (lambda f: f),
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


def test_mvp_s_oracle_uc_handlers(tmp_path):
    """T0: the UC handlers on Python values (GEOMETRY stand-in = EWKB bytes until a UC runtime shows what a handler receives)."""
    ran = oracles.run_handlers(oracles.MVP_S, tmpdir=str(tmp_path))
    assert [m for _, m in ran if m] == []
    assert len(ran) == sum("uc" in c.surfaces for c in oracles.MVP_S)


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
        "tcrs_wkt_uc": {"error": "Fix: use the session function gbx_rst_transformcrs"},
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
