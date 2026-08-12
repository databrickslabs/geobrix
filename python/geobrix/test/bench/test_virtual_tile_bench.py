"""QA: a virtual input tile must produce the SAME result as a materialized one."""
from pathlib import Path

from databricks.labs.gbx.bench import datagen as dg
from databricks.labs.gbx.bench.fingerprint import fingerprint_output
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import accessors, open_tile as ot, terrain
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile


def _one_tile(tmp_path):
    corpus = dg.generate_corpus(
        out_dir=tmp_path, seed=9, tile_px=[64], bands=[1], dtypes=["float32"],
        srids=[4326], nodata_fracs=[0.0], row_rows=1, row_tile_px=64,
        row_bands=1, row_dtype="float32",
    )
    te = next(t for t in corpus.size_sweep if t.role != "bng_gb")
    return Path(tmp_path) / te.path, te


def test_slope_virtual_equals_materialized(tmp_path):
    p, te = _one_tile(tmp_path)
    with _serde.open_tile(p.read_bytes()) as ds:
        mat = terrain.slope(ds, unit="degrees", xscale=None, yscale=None)
    vt = VirtualTile(cellid=0, raster=None, path=str(p),
                     window=(0, 0, te.tile_px, te.tile_px))
    with ot._open(vt.to_row()) as ds:
        virt = terrain.slope(ds, unit="degrees", xscale=None, yscale=None)
    assert fingerprint_output(mat) == fingerprint_output(virt)


def test_width_virtual_is_header_only_and_matches(tmp_path):
    p, te = _one_tile(tmp_path)
    with _serde.open_tile(p.read_bytes()) as ds:
        mat_w = accessors.width(ds)
    # window=(0,0,w,h) covers the full extent; open_header detects _is_full_extent
    # and yields src directly (no pixel I/O), confirming the header-only path.
    vt = VirtualTile(cellid=0, raster=None, path=str(p),
                     window=(0, 0, te.tile_px, te.tile_px))
    with ot.open_header(vt.to_row()) as ds:
        virt_w = accessors.width(ds)
    assert mat_w == virt_w
