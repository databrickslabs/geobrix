import pytest

from databricks.labs.gbx.pyrx.grouped_exec import align_partitions


def test_align_partitions_hashes_and_sorts_by_path(spark):
    df = spark.createDataFrame(
        [(1, "a.tif"), (2, "a.tif"), (3, "b.tif")], "cellid bigint, p string"
    )
    out = align_partitions(df, n=4, path_col="p")
    assert out.rdd.getNumPartitions() == 4
    # each path lands in exactly one partition (hash-by-path never splits a key)
    part_of = out.rdd.map(lambda r: (r["p"],)).glom().collect()
    seen = {}
    for i, rows in enumerate(part_of):
        for (p,) in rows:
            assert seen.setdefault(p, i) == i


def test_align_partitions_rejects_nonpositive_n(spark):
    df = spark.createDataFrame([(1, "a.tif")], "cellid bigint, p string")
    with pytest.raises(ValueError):
        align_partitions(df, n=0, path_col="p")
