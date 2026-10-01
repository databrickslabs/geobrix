"""lidar_gbx Writer Examples — single source of truth.

Code shown in docs/docs/writers/lidar.mdx is imported from here. Pure-Python
DataSource V2 writer (JAR-free, registered via gbx.ds.register). It is the
inverse of the lidar_gbx points reader:

- parts (default): one .laz per Spark partition, named *_<group>_<cluster>.laz
  when groupCol/clusterCol are set, else <partPrefix>-<uuid>.laz.
- singleFile="true": partition points captured as feather fragments on executors,
  then merged into ONE .laz by the driver.
- merge="true": post-hoc fold of existing .laz/.las files in the directory into
  one .laz without re-reading the DataFrame.

The writer requires columns x, y, z (float64) and accepts optional r, g, b
(0-255 integers). RGB is all-or-nothing: all three or none.
"""

REGISTER = """# Register the lightweight DataSources (once per session)
from databricks.labs.gbx.ds.register import register
register(spark)"""

WRITE_SHARDED = """# Parts mode (default): one .laz per Spark partition.
# With groupCol + clusterCol the shard name is deterministic: *_<group>_<cluster>.laz.
(df
    .repartitionByRange(4, "group", "cluster")
    .write.format("lidar_gbx")
    .option("groupCol", "group")
    .option("clusterCol", "cluster")
    .mode("overwrite")
    .save("/Volumes/main/geobrix_samples/lidar/output"))

# Read back via the lidar_gbx reader
cloud = (spark.read
    .format("lidar_gbx")
    .option("mode", "points")
    .load("/Volumes/main/geobrix_samples/lidar/output"))
cloud.select("x", "y", "z").show(5)"""

WRITE_SINGLEFILE = """# Consolidate into ONE .laz with singleFile="true".
# Executor partitions are captured as feather fragments; the driver merges them.
# Reach for this when the full point cloud fits in driver memory.
(df
    .repartition(4)
    .write.format("lidar_gbx")
    .option("singleFile", "true")
    .option("fileName", "merged")     # optional output stem
    .mode("overwrite")
    .save("/Volumes/main/geobrix_samples/lidar/single"))"""

WRITE_MERGE = """# Post-hoc: merge .laz/.las files ALREADY in a directory into one,
# without re-running the source DataFrame (the DataFrame rows are ignored).
(spark.range(1).write               # any DataFrame; source rows are ignored
    .format("lidar_gbx")
    .mode("append")
    .option("merge", "true")
    .option("fileName", "all")
    .option("keepParts", "true")     # keep the source parts alongside the merged file
    .save("/Volumes/main/geobrix_samples/lidar/output"))"""


def _register(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)


def _points_df(spark, n=20, with_rgb=True):
    """Synthesize a tiny points DataFrame: x, y, z + optional r, g, b + group, cluster."""
    import numpy as np
    from pyspark.sql import Row

    rng = np.random.default_rng(42)
    groups = ("A", "B")
    rows = []
    for i in range(n):
        grp = groups[i % len(groups)]
        clu = (i // 2) % 2  # 0,0,1,1,... -> all 4 (grp, clu) combos when 2 groups
        base = dict(
            x=float(rng.uniform(0, 100)),
            y=float(rng.uniform(0, 100)),
            z=float(rng.uniform(0, 50)),
            group=grp,
            cluster=int(clu),
        )
        if with_rgb:
            base.update(
                r=int(rng.integers(0, 256)),
                g=int(rng.integers(0, 256)),
                b=int(rng.integers(0, 256)),
            )
        rows.append(Row(**base))
    return spark.createDataFrame(rows)


def write_sharded(spark, out_dir):
    """Verify WRITE_SHARDED: parts mode writes sharded .laz files; the lidar_gbx
    reader round-trips the full point count."""
    import glob
    import os

    _register(spark)
    df = _points_df(spark, n=20, with_rgb=True)
    (
        df.repartitionByRange(4, "group", "cluster")
        .write.format("lidar_gbx")
        .option("groupCol", "group")
        .option("clusterCol", "cluster")
        .mode("overwrite")
        .save(out_dir)
    )

    parts = glob.glob(os.path.join(out_dir, "*.la*"))
    assert parts, "no .laz/.las files written"
    back = spark.read.format("lidar_gbx").option("mode", "points").load(out_dir)
    assert back.count() == 20


def write_singlefile(spark, out_dir):
    """Verify WRITE_SINGLEFILE: singleFile="true" produces exactly one .laz/.las
    with all points merged; the reader round-trips the count."""
    import glob
    import os

    _register(spark)
    df = _points_df(spark, n=20, with_rgb=True)
    (
        df.repartition(4)
        .write.format("lidar_gbx")
        .option("singleFile", "true")
        .option("fileName", "merged")
        .mode("overwrite")
        .save(out_dir)
    )

    files = glob.glob(os.path.join(out_dir, "merged.la*"))
    assert len(files) == 1, f"expected exactly one merged file, got {files}"
    back = spark.read.format("lidar_gbx").option("mode", "points").load(files[0])
    assert back.count() == 20


def write_merge(spark, out_dir):
    """Verify WRITE_MERGE: write sharded parts, then post-hoc merge="true" folds
    them into one .laz without recomputing; keepParts="true" retains the parts."""
    import glob
    import os

    _register(spark)
    df = _points_df(spark, n=20, with_rgb=True)
    (
        df.repartitionByRange(4, "group", "cluster")
        .write.format("lidar_gbx")
        .option("groupCol", "group")
        .option("clusterCol", "cluster")
        .mode("overwrite")
        .save(out_dir)
    )
    parts_before = sorted(glob.glob(os.path.join(out_dir, "*.la*")))
    assert parts_before, "no parts written"

    (
        spark.range(1)
        .write.format("lidar_gbx")
        .mode("append")
        .option("merge", "true")
        .option("fileName", "all")
        .option("keepParts", "true")
        .save(out_dir)
    )

    merged = glob.glob(os.path.join(out_dir, "all.la*"))
    assert len(merged) == 1, f"expected exactly one merged file, got {merged}"
    # keepParts=true -> every source part still on disk
    for p in parts_before:
        assert os.path.exists(p), f"part {p} was deleted despite keepParts"
    back = spark.read.format("lidar_gbx").option("mode", "points").load(merged[0])
    assert back.count() == 20
