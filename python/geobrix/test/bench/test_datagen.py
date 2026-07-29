import numpy as np

from databricks.labs.gbx.bench import datagen as dg
from databricks.labs.gbx.bench import manifest as m
from databricks.labs.gbx.pyrx import _serde


def _open(b):
    return _serde.open_tile(b)


def test_make_tile_shape_crs_bands_dtype():
    b = dg.make_tile_bytes(
        tile_px=64, bands=4, dtype="float32", srid=32618, nodata_frac=0.0, seed=7
    )
    with _open(b) as ds:
        assert ds.width == 64 and ds.height == 64
        assert ds.count == 4
        assert ds.dtypes[0] == "float32"
        assert ds.crs.to_epsg() == 32618


def test_make_tile_is_deterministic_for_seed():
    a = dg.make_tile_bytes(
        tile_px=32, bands=1, dtype="int16", srid=4326, nodata_frac=0.0, seed=42
    )
    b = dg.make_tile_bytes(
        tile_px=32, bands=1, dtype="int16", srid=4326, nodata_frac=0.0, seed=42
    )
    assert a == b


def test_nodata_fraction_is_approximately_respected():
    b = dg.make_tile_bytes(
        tile_px=100,
        bands=1,
        dtype="float32",
        srid=4326,
        nodata_frac=0.25,
        seed=1,
        nodata_mode="sparse",
    )
    with _open(b) as ds:
        arr = ds.read(1)
        nod = ds.nodata
        frac = float(np.mean(arr == nod))
    assert 0.20 <= frac <= 0.30


def test_band_correlation_yields_valid_ndvi_range():
    # red=band1, nir=band2 with band-correlated values -> NDVI within [-1,1]
    b = dg.make_tile_bytes(
        tile_px=32, bands=2, dtype="float32", srid=4326, nodata_frac=0.0, seed=3
    )
    with _open(b) as ds:
        red = ds.read(1).astype("float64")
        nir = ds.read(2).astype("float64")
    denom = nir + red
    ndvi = np.where(denom != 0, (nir - red) / denom, 0.0)
    assert ndvi.min() >= -1.0 and ndvi.max() <= 1.0


def test_generate_corpus_writes_tiles_and_manifest(tmp_path):
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=11,
        tile_px=[32, 64],
        bands=[1, 2],
        dtypes=["float32"],
        srids=[4326, 3857],
        nodata_fracs=[0.0, 0.1],
        row_rows=20,
        row_tile_px=64,
        row_bands=2,
        row_dtype="float32",
    )
    assert isinstance(corpus, m.Corpus)
    assert len(corpus.size_sweep) > 0
    assert len(corpus.row_pool.tiles) == 20
    # every referenced tile file exists under out_dir
    for te in corpus.size_sweep + corpus.row_pool.tiles:
        assert (tmp_path / te.path).exists()
    assert (tmp_path / "corpus.json").exists()


def test_validity_gate_passes_for_generated_corpus(tmp_path):
    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=5,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.02],
        row_rows=3,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    problems = dg.validity_gate(tmp_path, corpus, nodata_warn_threshold=0.9)
    assert problems == []


def test_corpus_includes_gb_tile_yielding_nonempty_bng_cells(tmp_path):
    # The corpus must carry exactly one Great-Britain-overlapping tile (role
    # "bng_gb", EPSG:27700 over London) so the BNG raster->grid / tessellate fns
    # bench REAL cells instead of a vacuous empty grid. Verify it exists, is
    # 27700, matches the first sweep tile's band/dtype/px, and that warping +
    # binning it to BNG yields > 0 cells (the whole point of the tile).
    from databricks.labs.gbx.pyrx.core import gridagg

    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=1234,
        tile_px=[128],
        bands=[4],
        dtypes=["float32"],
        srids=[4326, 3857],
        nodata_fracs=[0.02],
        row_rows=1,
        row_tile_px=128,
        row_bands=4,
        row_dtype="float32",
    )
    gb = [t for t in corpus.size_sweep if t.role == "bng_gb"]
    assert len(gb) == 1, "expected exactly one GB (bng_gb) tile"
    te = gb[0]
    assert te.srid == 27700
    # matches the first sweep tile's conventions
    first = next(t for t in corpus.size_sweep if t.role == "sweep")
    assert (te.bands, te.dtype, te.tile_px) == (first.bands, first.dtype, first.tile_px)
    import rasterio

    with rasterio.open(tmp_path / te.path) as ds:
        assert ds.crs.to_epsg() == 27700
        cells = gridagg.raster_to_grid(ds, 3, "bng", "avg")
    total = sum(len(band) for band in cells)
    assert total > 0, "GB tile must bin non-empty BNG cells"


def test_int16_band_correlation_yields_valid_ndvi_range():
    # int16 tiles must also keep NDVI within [-1, 1] (non-negative reflectance).
    b = dg.make_tile_bytes(
        tile_px=32, bands=2, dtype="int16", srid=4326, nodata_frac=0.0, seed=3
    )
    with _serde.open_tile(b) as ds:
        red = ds.read(1).astype("float64")
        nir = ds.read(2).astype("float64")
    denom = nir + red
    ndvi = np.where(denom != 0, (nir - red) / denom, 0.0)
    assert ndvi.min() >= -1.0 and ndvi.max() <= 1.0
