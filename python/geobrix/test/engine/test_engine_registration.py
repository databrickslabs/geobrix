"""How the manager registers the GDAL spec engine's UDFs, with a recording session in place of Spark.

The FILE-aware form (`file_ref: "engine"`) registers the row-wise UDF that takes the tile's FILE reference second, behind
a SQL wrapper that mints it; the batch form passes the same reference to its row-wise branch. A row without the key is
registered as it was. The registrations run on the host: the real queries are the SPEC_WARP rows.
"""

import dataclasses
import types

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import l3 as engine_l3
from databricks.labs.gbx.manager import capabilities, jvm
from databricks.labs.gbx.manager import l3 as mgr_l3
from databricks.labs.gbx.manager import sqlwrap

MAN = manifest.load()
RESAMPLE = MAN.functions["gbx_rst_resample"]
SLOPE = MAN.functions["gbx_rst_slope"]
FILE_SQL = "MINT_FILE(tile)"


class _Recorder:
    """A session that records what is registered, and the wrapper DDL, instead of running them."""

    def __init__(self):
        self.udfs, self.ddl, self.c0_calls = {}, [], []
        self.udf = types.SimpleNamespace(register=self._register)

    def _register(self, name, fn):
        self.udfs[name] = fn


@pytest.fixture
def session(monkeypatch):
    rec = _Recorder()
    monkeypatch.setattr(capabilities, "_sql_functions", lambda spark: True)
    monkeypatch.setattr(capabilities, "is_connect", lambda spark: False)
    monkeypatch.setattr(
        "databricks.labs.gbx._gdal_policy.carrying_session", lambda spark: rec
    )
    monkeypatch.setattr(
        "databricks.labs.gbx.ds.file_gbx.report_detected_cap", lambda spark=None: 123
    )
    monkeypatch.setattr(
        "databricks.labs.gbx.ds.file_gbx.file_ref_sql",
        lambda tile, spark=None: f"MINT_FILE({tile})",
    )
    monkeypatch.setattr(
        engine_l3,
        "c0",
        lambda name, grid=(), file_ref=False, **kw: rec.c0_calls.append(
            (name, kw.get("read_cap"), file_ref)
        )
        or f"c0[{name},{file_ref}]",
    )
    monkeypatch.setattr(engine_l3, "c1", lambda name, grid=(): f"c1[{name}]")
    monkeypatch.setattr(
        jvm, "_create_wrapper", lambda spark, fn, ddl: rec.ddl.append(ddl)
    )
    return rec


def test_the_file_aware_row_wise_form_registers_the_udf_that_takes_the_reference(
    session,
):
    form = mgr_l3.register_l3_engine(object(), RESAMPLE, "engine-c0")
    assert form == "engine-c0"
    assert session.c0_calls == [("gbx_rst_resample", 123, True)]
    assert session.udfs == {"gbx__l3f_rst_resample": "c0[gbx_rst_resample,True]"}
    assert (
        "RETURN gbx__l3f_rst_resample(tile, MINT_FILE(tile), factor, algorithm)"
        in session.ddl[0]
    )


def test_the_batch_form_hands_the_reference_to_its_row_wise_branch(session):
    form = mgr_l3.register_l3_engine(object(), RESAMPLE, "engine-c1")
    assert form == "engine-c1"
    assert session.c0_calls == [("gbx_rst_resample", 123, True)]
    assert set(session.udfs) == {"gbx__l3_rst_resample", "gbx__l3c1_rst_resample"}
    ddl = session.ddl[0]
    assert "gbx__l3_rst_resample(" in ddl and "MINT_FILE(tile)" in ddl
    assert (
        "IF(tile.raster IS NULL AND tile.path IS NOT NULL, MINT_FILE(tile), NULL)"
        in ddl
    )


def test_a_row_without_the_key_registers_as_it_always_did(session):
    assert mgr_l3.register_l3_engine(object(), SLOPE, "engine-c0") == "engine-c0"
    assert session.c0_calls == [("gbx_rst_slope", 123, False)]
    assert set(session.udfs) == {"gbx_rst_slope"} and session.ddl == []
    session.udfs.clear(), session.c0_calls.clear()
    mgr_l3.register_l3_engine(object(), SLOPE, "engine-c1")
    assert session.c0_calls == [("gbx_rst_slope", 123, False)]
    assert "MINT_FILE" not in session.ddl[0]


def test_without_sql_functions_the_file_aware_row_registers_plain(session, monkeypatch):
    monkeypatch.setattr(capabilities, "_sql_functions", lambda spark: False)
    assert mgr_l3._engine_file_ref(object(), RESAMPLE) is False
    assert mgr_l3.register_l3_engine(object(), RESAMPLE, "engine-c0") == "engine-c0"
    assert session.c0_calls == [("gbx_rst_resample", 123, False)]
    assert set(session.udfs) == {"gbx_rst_resample"} and session.ddl == []


def test_the_dispatch_passes_the_file_expression_only_when_given():
    plain = sqlwrap.tile_dispatch(RESAMPLE, "JVM({tile}{rest})", "light_udf")
    aware = sqlwrap.tile_dispatch(
        RESAMPLE, "JVM({tile}{rest})", "light_udf", "MINT_FILE(tile)"
    )
    assert (
        "light_udf((IF(tile.raster IS NULL AND tile.path IS NOT NULL, tile, NULL)), factor, algorithm)"
        in plain
    )
    assert (
        "light_udf((IF(tile.raster IS NULL AND tile.path IS NOT NULL, tile, NULL)), "
        "(IF(tile.raster IS NULL AND tile.path IS NOT NULL, MINT_FILE(tile), NULL)), factor, algorithm)"
    ) in aware


def test_an_engine_only_row_is_not_registered_through_the_plain_light_path(session):
    with pytest.raises(ValueError, match="GDAL spec engine alone"):
        mgr_l3.register_l3(object(), SLOPE, "-")


def test_a_file_form_built_by_a_factory_is_the_factorys_product(session, monkeypatch):
    """A row whose L3 has a ``factory`` and a ``file_ref`` registers what the factory builds from the file form."""
    row = dataclasses.replace(
        SLOPE,
        levels={
            **SLOPE.levels,
            "L3": {
                "register": "udf",
                "factory": "x:make",
                "factory_args": "read_cap",
                "file_ref": "x:file_form",
            },
        },
    )
    monkeypatch.setattr(
        mgr_l3, "resolve_ref", lambda ref: (lambda *a: ("built", ref, a))
    )
    monkeypatch.setattr(mgr_l3, "_factory_args", lambda kind, spark=None: (456,))
    monkeypatch.setattr(
        "databricks.labs.gbx.ds.file_gbx.file_ref_sql", lambda tile, spark=None: "MINT"
    )
    mgr_l3.register_l3(object(), row, "-")
    assert session.udfs == {"gbx__l3f_rst_slope": ("built", "x:file_form", (456,))}
    assert "gbx__l3f_rst_slope(" in session.ddl[0]
