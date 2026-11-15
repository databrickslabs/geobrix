"""Planning spans: timed only inside ``record()``, and a read plans through them."""

import time

import pytest

pytest.importorskip("zarr")

from databricks.labs.gbx.zarr import _spans  # noqa: E402
from databricks.labs.gbx.zarr._read import gbx_zarr_read  # noqa: E402


def test_a_span_records_only_inside_a_record_and_adds_up_repeats():
    with _spans.span("outside"):
        pass
    with _spans.record() as got:
        with _spans.span("a"):
            time.sleep(0.01)
            with _spans.span("b"):
                time.sleep(0.01)
        with _spans.span("a"):
            time.sleep(0.01)
    assert set(got) == {"a", "b"}
    assert got["a"] >= 0.029 and got["b"] >= 0.009  # a holds b; two a's add up
    with _spans.span("after"):
        pass
    assert "after" not in got and "outside" not in got


def test_a_span_is_recorded_when_its_block_raises():
    with _spans.record() as got:
        with pytest.raises(ValueError):
            with _spans.span("boom"):
                raise ValueError
    assert "boom" in got


@pytest.mark.parametrize("output", ["blocks", "table"])
def test_a_plain_store_read_plans_through_its_spans(
    spark_delta, era5_v3_sharded, output
):
    with _spans.record() as got:
        gbx_zarr_read(spark_delta, str(era5_v3_sharded), output=output)
    assert {"open", "metadata", "axes", "slots", "units", "access", "frame"} <= set(got)
    assert got["open"] >= got["metadata"] > 0


def test_a_tile_read_plans_through_its_spans(spark_delta, era5_v3_sharded):
    with _spans.record() as got:
        gbx_zarr_read(
            spark_delta,
            str(era5_v3_sharded),
            variables=["t2m"],
            isel={"time": 0},
            output="tiles",
        )
    assert {"open", "tile_plans", "access", "frame"} <= set(got)
