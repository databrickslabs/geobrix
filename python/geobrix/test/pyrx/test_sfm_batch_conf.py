"""run_sfm asks for one image per Arrow batch while it runs, and puts the session's setting back afterwards."""

import pytest
from pyspark.sql import SparkSession

from databricks.labs.gbx.rasterx import sfm

KEY = "spark.sql.execution.arrow.maxRecordsPerBatch"


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[1]").appName("sfm-batch-conf").getOrCreate()
    )


@pytest.fixture
def restore_key(spark):
    old = spark.conf.get(KEY, None)
    yield
    if old is None:
        spark.conf.unset(KEY)
    else:
        spark.conf.set(KEY, old)


def _call(spark, body):
    """`run_sfm` with its body replaced: only the decorator around it is under test."""
    return sfm._one_image_per_batch(body)(spark)


def test_batch_size_is_one_inside_and_restored_after(spark, restore_key):
    spark.conf.set(KEY, "5000")
    seen = []
    _call(spark, lambda s: seen.append(s.conf.get(KEY)))
    assert seen == ["1"]
    assert spark.conf.get(KEY) == "5000"


def test_batch_size_is_restored_after_an_exception(spark, restore_key):
    spark.conf.set(KEY, "5000")

    def boom(s):
        raise RuntimeError("stage failed")

    with pytest.raises(RuntimeError, match="stage failed"):
        _call(spark, boom)
    assert spark.conf.get(KEY) == "5000"


def test_batch_size_is_back_to_the_session_default_when_it_was_unset(
    spark, restore_key
):
    spark.conf.unset(KEY)
    default = spark.conf.get(KEY, None)  # a registered conf reads as its default
    seen = []
    _call(spark, lambda s: seen.append(s.conf.get(KEY)))
    assert seen == ["1"]
    assert spark.conf.get(KEY, None) == default


def test_run_sfm_is_wrapped_and_keeps_its_signature():
    import inspect

    assert hasattr(sfm.run_sfm, "__wrapped__")
    assert list(inspect.signature(sfm.run_sfm).parameters)[0] == "spark"


def test_refused_set_goes_on_and_says_so(capsys):
    from ..manager.connect_stub import ConnectStub

    spark = ConnectStub()
    ran = []
    _call(spark, lambda s: ran.append(True))
    out = capsys.readouterr().out
    assert ran == [True]
    lines = [ln for ln in out.splitlines() if "batch" in ln]
    assert len(lines) == 1 and KEY in lines[0] and "default" in lines[0]
