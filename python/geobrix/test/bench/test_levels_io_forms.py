"""The IO forms of the levels leg (read paths and writers): their shape, the scratch guard, and a local probe."""

import os

import numpy as np
import pytest

from databricks.labs.gbx.bench import levels_forms as lf
from databricks.labs.gbx.bench import preflight


def _corpus(tmp_path, n=4):
    import rasterio
    from rasterio.transform import from_origin

    rows = tmp_path / "corpus" / "rows"
    rows.mkdir(parents=True)
    for i in range(n):
        with rasterio.open(
            rows / f"t{i}.tif",
            "w",
            driver="GTiff",
            width=16,
            height=16,
            count=1,
            dtype="float32",
            crs="EPSG:4326",
            transform=from_origin(i * 16, 16, 1, 1),
        ) as dst:
            dst.write(np.full((1, 16, 16), i, dtype="float32"))
    return str(tmp_path / "corpus")


def test_the_io_set_holds_the_five_reads_the_udf_and_the_three_writers():
    forms = {f.variant: f for f in lf.FORMS["IO"]}
    assert set(forms) == {"a", "b", "c", "udf", "d-py", "d-rs", "W0", "W1", "W2"}
    assert {f.input for f in lf.FORMS["IO"]} == {"io_rows"}
    assert [lf.BASE_ROWS["io_rows"] * k for k in lf.SCALES["IO"]] == [1000, 4000, 10000]
    for f in lf.FORMS["IO"]:  # every form resolves to a function of this module
        assert callable(getattr(lf, f.run.rpartition(":")[2])), f.variant
        assert not f.setup or callable(
            getattr(lf, f.setup.rpartition(":")[2])
        ), f.variant


def test_a_writer_form_deletes_only_under_the_scratch_prefix(tmp_path):
    inside = tmp_path / "geobrix" / "0.6.0" / "scratch" / "io_1"
    inside.mkdir(parents=True)
    (inside / "f.tif").write_bytes(b"x")
    lf.remove_scratch(str(inside))
    assert not inside.exists()
    keep = tmp_path / "corpus"
    keep.mkdir()
    for bad in (
        str(keep),
        str(tmp_path / "geobrix" / "0.6.0" / "scratch" / ".." / "corpus"),
        "/",
    ):
        with pytest.raises(ValueError, match="refusing to delete"):
            lf.remove_scratch(bad)
    assert keep.exists()


def test_the_scratch_directory_is_beside_the_corpus_on_a_volume_and_in_temp_elsewhere():
    assert (
        lf._io_scratch("/Volumes/c/s/v/geobrix/0.6.0/corpus/")
        == "/Volumes/c/s/v/geobrix/0.6.0/scratch"
    )
    assert lf._io_scratch("/tmp/x/corpus").endswith(lf.SCRATCH_PREFIX.rstrip("/"))


def test_the_light_io_forms_run_on_a_local_corpus(spark_session_local, tmp_path):
    """b, d-py, the row-wise UDF and the light writer W1 on four tiles; the JVM forms are skipped without the JAR."""
    corpus = _corpus(tmp_path)
    failures, skipped = preflight.probe_failures(
        spark_session_local, "IO", corpus, rows=4, forms=("b", "d-py", "udf", "W1")
    )
    assert failures == [], failures
    assert not os.path.exists(lf._io_scratch(corpus)) or not os.listdir(
        lf._io_scratch(corpus)
    )
