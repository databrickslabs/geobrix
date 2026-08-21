import os


def test_stage_gpkg_bench_corpus_builds_dirs_per_copies(spark, tmp_path):
    from databricks.labs.gbx.bench.corpus_vector import stage_gpkg_bench_corpus

    dirs = stage_gpkg_bench_corpus(spark, str(tmp_path), rows=50, copies_ladder=(2, 3))
    assert set(dirs) == {2, 3}
    assert len([f for f in os.listdir(dirs[2]) if f.endswith(".gpkg")]) == 2
    assert len([f for f in os.listdir(dirs[3]) if f.endswith(".gpkg")]) == 3
