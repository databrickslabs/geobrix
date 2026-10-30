"""Unit tests for the bench `tile_array` input synthesizer.

The C3 multi-tile functions (rst_frombands / rst_combineavg / rst_merge) each
consume an ARRAY of tiles, but the corpus row provides ONE tile. The synthesizer
derives the multi-tile input from that single source and writes it to disk ONCE,
so BOTH benchmark engines (pyrx + heavy) read byte-identical input files.

These tests assert the synthesizer's contract via rasterio on the outputs:
  * frombands  -> N single-band GTiffs (N = source band count)
  * combineavg -> 2 aligned copies (identical geotransform/shape/CRS)
  * merge      -> 2 copies with DISTINCT geotransform origins (offset extents)
and that it is idempotent (same inputs -> same files, no duplication).
"""

from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.bench import synth


def _write_src(path, bands=3, px=16, dtype="float32", srid=4326):
    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=px,
        height=px,
        count=bands,
        dtype=dtype,
        crs=f"EPSG:{srid}",
        transform=transform,
    )
    with rasterio.open(path, "w", **profile) as dst:
        for b in range(1, bands + 1):
            dst.write(np.full((px, px), float(b), dtype=dtype), b)
    return path


def test_synthesize_frombands_splits_into_single_band_tiles(tmp_path):
    src = _write_src(tmp_path / "src.tif", bands=3)
    out = tmp_path / "out"
    paths = synth.synthesize(str(src), "frombands", str(out))
    assert len(paths) == 3, "frombands -> one tile per source band"
    for i, p in enumerate(paths, start=1):
        with rasterio.open(p) as ds:
            assert ds.count == 1, "each frombands tile is single-band"
            # band i carries the i-th source band's constant value
            assert float(ds.read(1).flat[0]) == float(i)


def test_synthesize_combineavg_two_aligned_copies(tmp_path):
    src = _write_src(tmp_path / "src.tif", bands=2)
    out = tmp_path / "out"
    paths = synth.synthesize(str(src), "combineavg", str(out))
    assert len(paths) == 2, "combineavg -> 2 copies"
    with rasterio.open(paths[0]) as a, rasterio.open(paths[1]) as b:
        # aligned: identical transform, shape, CRS
        assert a.transform == b.transform
        assert (a.width, a.height, a.count) == (b.width, b.height, b.count)
        assert a.crs == b.crs


def test_synthesize_merge_two_distinct_origins(tmp_path):
    src = _write_src(tmp_path / "src.tif", bands=1, px=16)
    out = tmp_path / "out"
    paths = synth.synthesize(str(src), "merge", str(out))
    assert len(paths) == 2, "merge -> 2 copies"
    with rasterio.open(paths[0]) as a, rasterio.open(paths[1]) as b:
        # DISTINCT origins so their extents tile into a union
        assert (a.transform.c, a.transform.f) != (b.transform.c, b.transform.f)
        # same pixel size / CRS so they mosaic onto one grid
        assert a.res == b.res
        assert a.crs == b.crs


def test_synthesize_is_idempotent(tmp_path):
    src = _write_src(tmp_path / "src.tif", bands=2)
    out = tmp_path / "out"
    first = synth.synthesize(str(src), "frombands", str(out))
    first_bytes = [open(p, "rb").read() for p in first]
    second = synth.synthesize(str(src), "frombands", str(out))
    assert first == second, "same inputs -> same paths"
    second_bytes = [open(p, "rb").read() for p in second]
    assert first_bytes == second_bytes, "same inputs -> byte-identical files"


def test_synth_dir_is_deterministic():
    a = synth.synth_dir("/corpus", "tiles/t0.tif", "merge")
    b = synth.synth_dir("/corpus", "tiles/t0.tif", "merge")
    assert a == b, "both engines must compute the same synth dir"
    assert synth.synth_dir("/corpus", "tiles/t0.tif", "frombands") != a


def _fake_corpus(corpus_root, size_rels, row_rels):
    """Build a minimal Corpus over fake tiles written under ``corpus_root``."""
    from databricks.labs.gbx.bench import manifest as m

    def _te(rel, cellid, bands):
        return m.TileEntry(rel, cellid, 4326, "float32", bands, 16, 0.0)

    size_sweep = []
    for i, rel in enumerate(size_rels):
        p = corpus_root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        _write_src(p, bands=2)
        size_sweep.append(_te(rel, i, 2))
    row_tiles = []
    for j, rel in enumerate(row_rels):
        p = corpus_root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        _write_src(p, bands=2)
        row_tiles.append(_te(rel, 100 + j, 2))
    return m.Corpus(
        seed=1,
        size_sweep=size_sweep,
        row_pool=m.RowPool(16, 2, "float32", row_tiles),
    )


def test_materialize_all_writes_every_recipe_for_every_size_tile(tmp_path):
    corpus = _fake_corpus(
        tmp_path, ["size/t0.tif", "size/t1.tif"], ["rows/r0.tif", "rows/r1.tif"]
    )
    written = synth.materialize_all(str(tmp_path), corpus)

    # Source tiles synthesized: both size-sweep tiles + the FIRST row_pool tile
    # (the spark-path `_array_root`); NOT the rest of the row pool.
    src_rels = ["size/t0.tif", "size/t1.tif", "rows/r0.tif"]
    for rel in src_rels:
        for recipe in ("frombands", "combineavg", "merge"):
            d = Path(synth.synth_dir(str(tmp_path), rel, recipe))
            assert d.is_dir(), f"missing synth dir for {rel}/{recipe}"
            files = list(d.glob("*.tif"))
            assert files, f"no synth tiles for {rel}/{recipe}"

    # row_pool tiles past the first must NOT be synthesized
    for recipe in ("frombands", "combineavg", "merge"):
        d = Path(synth.synth_dir(str(tmp_path), "rows/r1.tif", recipe))
        assert (
            not d.exists()
        ), "only the first row_pool tile is the spark-path array root"

    assert written, "materialize_all returns the written paths"
    assert all(Path(p).exists() for p in written)


def test_materialize_all_is_idempotent(tmp_path):
    corpus = _fake_corpus(tmp_path, ["size/t0.tif"], ["rows/r0.tif"])
    first = synth.materialize_all(str(tmp_path), corpus)
    first_bytes = {p: open(p, "rb").read() for p in first}
    second = synth.materialize_all(str(tmp_path), corpus)
    assert sorted(first) == sorted(second), "same corpus -> same synth paths"
    for p in second:
        assert open(p, "rb").read() == first_bytes[p], "re-run is byte-identical"


def test_materialize_all_reads_corpus_json_when_corpus_omitted(tmp_path):
    corpus = _fake_corpus(tmp_path, ["size/t0.tif"], ["rows/r0.tif"])
    corpus.write(tmp_path / "corpus.json")
    written = synth.materialize_all(str(tmp_path))  # corpus=None -> read json
    assert written, "materialize_all loads corpus.json when corpus is omitted"


# ---------------------------------------------------------------------------
# Volume safety: no GDAL write onto the destination, torn cache files rewritten
# ---------------------------------------------------------------------------

_TORN = bytes.fromhex("49492a0000000000") + b"\x00" * 64  # first-IFD offset 0


def test_synth_never_writes_gdal_onto_the_destination(tmp_path, monkeypatch):
    """GDAL seeks back on close; on a Volume that tears the file. It must write locally, then copy."""
    src = _write_src(tmp_path / "src.tif", bands=2)
    out = tmp_path / "out"
    written = []
    real = synth.open_data

    def spy(path, mode="r", **kw):
        if mode == "w":
            written.append(Path(str(path)))
        return real(path, mode, **kw)

    monkeypatch.setattr(synth, "open_data", spy)
    for recipe in ("frombands", "combineavg", "merge"):
        synth.synthesize(str(src), recipe, str(out))
    assert written, "the spy saw no writes"
    assert all(out not in p.parents for p in written), written
    # nothing partial left beside the finished files
    assert sorted(p.suffix for p in out.iterdir()) == [".tif"] * len(
        list(out.iterdir())
    )


def test_synth_rewrites_a_torn_cached_file(tmp_path):
    src = _write_src(tmp_path / "src.tif", bands=2)
    out = tmp_path / "out"
    paths = synth.synthesize(str(src), "merge", str(out))
    good = Path(paths[0]).read_bytes()
    Path(paths[0]).write_bytes(_TORN)
    again = synth.synthesize(str(src), "merge", str(out))
    assert again == paths
    assert Path(paths[0]).read_bytes() == good, "torn file not rewritten"
    with rasterio.open(paths[0]) as ds:
        assert ds.read(1).shape == (16, 16)


def test_synth_keeps_a_valid_cached_file(tmp_path):
    src = _write_src(tmp_path / "src.tif", bands=2)
    out = tmp_path / "out"
    paths = synth.synthesize(str(src), "combineavg", str(out))
    before = [Path(p).stat().st_mtime_ns for p in paths]
    synth.synthesize(str(src), "combineavg", str(out))
    assert [Path(p).stat().st_mtime_ns for p in paths] == before


def test_publish_file_falls_back_when_replace_is_refused(tmp_path, monkeypatch):
    local = tmp_path / "a.bin"
    local.write_bytes(b"abc")
    dest = tmp_path / "d" / "a.bin"
    dest.parent.mkdir()

    def refuse(*a, **k):
        raise OSError("no rename here")

    monkeypatch.setattr(synth.os, "replace", refuse)
    synth.publish_file(local, dest)
    assert dest.read_bytes() == b"abc"
    assert [p.name for p in dest.parent.iterdir()] == [
        "a.bin"
    ], "temp sibling left behind"
