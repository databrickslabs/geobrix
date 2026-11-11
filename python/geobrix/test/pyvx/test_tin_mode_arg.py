"""The ``mode`` argument of the TIN table functions on the Python level: left out is 'constrained', a NULL gives no
rows (as any NULL argument does), and the argument may be given by name."""

import pytest

pytest.importorskip("scipy")
from databricks.labs.gbx.pyvx import functions as F  # noqa: E402

SQUARE = "array('POINT Z (0 0 0)', 'POINT Z (10 0 0)', 'POINT Z (10 10 0)', 'POINT Z (0 10 0)', 'POINT Z (5 5 0)')"
ARGS = f"{SQUARE}, CAST(array() AS ARRAY<STRING>), 0.0D, 0.0D, 'NONENCROACHING'"


def test_mode_arg_tells_left_out_from_null():
    assert F._mode_arg(F._MODE_LEFT_OUT) == "constrained"
    assert F._mode_arg(None) is None
    assert F._mode_arg("CONFORMING") == "CONFORMING"
    with pytest.raises(ValueError, match="sideways"):
        F._mode_arg("sideways")


def test_an_empty_mode_is_refused_as_the_jvm_and_uc_levels_refuse_it():
    from databricks.labs.gbx.pyvx import _tin

    with pytest.raises(ValueError, match="mode must be"):
        F._mode_arg("")
    with pytest.raises(ValueError, match="mode must be"):
        _tin.validate_mode("")
    assert _tin.validate_mode(None) == "constrained"


def test_the_session_udtf_takes_mode_left_out_null_or_by_name(spark):
    spark.udtf.register("gbx_t_tri", F._TriangulateUDTF)
    count = lambda sql: spark.sql(sql).count()  # noqa: E731
    assert count(f"SELECT * FROM gbx_t_tri({ARGS})") == 4
    assert count(f"SELECT * FROM gbx_t_tri({ARGS}, CAST(NULL AS STRING))") == 0
    assert count(f"SELECT * FROM gbx_t_tri({ARGS}, mode => 'conforming')") == 4


@pytest.mark.parametrize(
    "udtf, grid",
    [
        ("_InterpElevBBoxUDTF", "0.0D, 0.0D, 10.0D, 10.0D, 2, 2, 0"),
        ("_InterpElevGeomUDTF", "'POINT (0 10)', 2, 2, 5.0D, -5.0D"),
    ],
)
def test_the_elevation_udtfs_take_mode_left_out_null_or_by_name(spark, udtf, grid):
    name = "gbx_t_" + udtf.lower().strip("_")
    spark.udtf.register(name, getattr(F, udtf))
    count = lambda sql: spark.sql(sql).count()  # noqa: E731
    assert count(f"SELECT * FROM {name}({ARGS}, {grid})") == 4
    assert count(f"SELECT * FROM {name}({ARGS}, {grid}, CAST(NULL AS STRING))") == 0
    assert count(f"SELECT * FROM {name}({ARGS}, {grid}, mode => 'conforming')") == 4
