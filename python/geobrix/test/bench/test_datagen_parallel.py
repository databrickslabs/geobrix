"""Tests for parallel write mode (--jobs N) in bench/datagen.py.

Verifies that:
  - jobs=4 and jobs=1 with the same seed produce an identical file set
  - Each r{j}.tif is byte-identical between modes (determinism)
  - corpus.json is identical between modes (manifest determinism)
  - _default_jobs() returns a sensible value
"""

from pathlib import Path

from databricks.labs.gbx.bench import datagen as dg


def _small_corpus(out_dir: Path, jobs: int) -> None:
    """Generate a minimal corpus (8 row-pool tiles) into out_dir."""
    dg.generate_corpus(
        out_dir=out_dir,
        seed=999,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326, 3857],
        nodata_fracs=[0.0],
        row_rows=8,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
        jobs=jobs,
    )


def test_parallel_produces_same_fileset(tmp_path):
    """jobs=4 and jobs=1 with the same seed write the same set of row files."""
    dir1 = tmp_path / "serial"
    dir2 = tmp_path / "parallel"
    dir1.mkdir()
    dir2.mkdir()

    _small_corpus(dir1, jobs=1)
    _small_corpus(dir2, jobs=4)

    def row_names(d):
        return sorted(p.name for p in (d / "rows").iterdir())

    assert row_names(dir1) == row_names(dir2)


def test_parallel_files_are_byte_identical(tmp_path):
    """Every r{j}.tif produced by jobs=4 must be byte-identical to jobs=1."""
    dir1 = tmp_path / "serial"
    dir2 = tmp_path / "parallel"
    dir1.mkdir()
    dir2.mkdir()

    _small_corpus(dir1, jobs=1)
    _small_corpus(dir2, jobs=4)

    names = sorted(p.name for p in (dir1 / "rows").iterdir())
    assert names, "no row files produced"
    for name in names:
        b1 = (dir1 / "rows" / name).read_bytes()
        b2 = (dir2 / "rows" / name).read_bytes()
        assert b1 == b2, f"{name}: bytes differ between serial and parallel runs"


def test_parallel_manifest_is_identical(tmp_path):
    """corpus.json seed/row entries must be identical between serial and parallel."""
    dir1 = tmp_path / "serial"
    dir2 = tmp_path / "parallel"
    dir1.mkdir()
    dir2.mkdir()

    _small_corpus(dir1, jobs=1)
    _small_corpus(dir2, jobs=4)

    j1 = (dir1 / "corpus.json").read_text()
    j2 = (dir2 / "corpus.json").read_text()
    assert j1 == j2, "corpus.json differs between serial and parallel"


def test_default_jobs_is_sensible():
    """_default_jobs() must return a value in [1, 32]."""
    j = dg._default_jobs()
    assert 1 <= j <= 32, f"unexpected default jobs: {j}"


def test_jobs_one_is_serial_path(tmp_path):
    """jobs=1 forces the serial code path and still produces a valid corpus."""
    from databricks.labs.gbx.bench import manifest as m

    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=42,
        tile_px=[32],
        bands=[1],
        dtypes=["uint8"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=4,
        row_tile_px=32,
        row_bands=1,
        row_dtype="uint8",
        jobs=1,
    )
    assert isinstance(corpus, m.Corpus)
    assert len(corpus.row_pool.tiles) == 4
    for te in corpus.row_pool.tiles:
        assert (tmp_path / te.path).exists()
