"""A range input is written in a file per compute slot, so a form's scan has the compute's parallelism at every scale."""

from unittest import mock

import pytest

from databricks.labs.gbx.bench import levels_family as fam
from databricks.labs.gbx.bench import levels_forms as lf


def _spark():
    spark = mock.MagicMock()
    spark.catalog.tableExists.return_value = False
    return spark


def test_a_table_with_rows_splits_its_files_by_the_slots_and_without_rows_does_not():
    with mock.patch(
        "databricks.labs.gbx.bench.runner._bench_parallelism", return_value=32
    ):
        spark = _spark()
        df = mock.MagicMock()
        lf._table(spark, "s", "mvp_in_x", lambda: df, rows=4_000_000)
        df.write.format.assert_called_with("delta")
        df.write.format.return_value.option.assert_called_once_with(
            "maxRecordsPerFile", 125_000
        )
        df.write.format.return_value.option.return_value.saveAsTable.assert_called_once()

        df2 = mock.MagicMock()
        lf._table(
            _spark(), "s", "mvp_in_y", lambda: df2
        )  # a tile input: a file per row already
        df2.write.format.return_value.option.assert_not_called()
        df2.write.format.return_value.saveAsTable.assert_called_once()


def test_the_files_of_a_range_input_follow_the_rows_at_every_scale():
    with mock.patch(
        "databricks.labs.gbx.bench.runner._bench_parallelism", return_value=32
    ):
        sizes = {}
        for n in (1_000_000, 4_000_000, 16_000_000):
            df = mock.MagicMock()
            lf._table(_spark(), "s", f"mvp_in_z{n}", lambda: df, rows=n)
            per_file = df.write.format.return_value.option.call_args[0][1]
            sizes[n] = -(-n // per_file)  # files at that cap
        assert all(
            32 <= files <= 33 for files in sizes.values()
        ), sizes  # not 1 / 1 / 4


@pytest.mark.parametrize(
    "builder,args",
    [
        (lf._bng_points, ()),
        (lf._invalid_polys, ()),
        (lf._bng_polys, ()),
        (lf._bng_cells(False), ()),
        (fam._points_arrays, ()),
        (fam._mvt_rows, ()),
        (fam._tin_points, ()),
    ],
)
def test_every_range_input_passes_its_row_count_to_the_table(
    builder, args, spark_session_local
):
    seen = {}

    def table(spark, schema, name, build, **kw):
        seen.update(kw)
        return name

    with mock.patch.object(lf, "_table", table):
        builder(spark_session_local, 1234, "", "s")
    assert seen.get("rows") == 1234
