"""A light writer labels a band alpha only where its source's alpha band is.

GDAL labels band 4 of every 4-band ``uint8`` GTiff alpha when it creates one, and drops the
alpha label of a 16-bit RGBA or a gray+alpha raster, so a writer that copies ``ds.profile``
mislabels RGB+NIR (band 4 reads as a transparency mask, and a NoData declared on the raster
shadows it) and loses the alpha band of other rasters. Each writer below goes through
``compression.write_gtiff(alpha_like=)`` / ``match_alpha`` (the central matrix is in
``test_compression_sites.py``); the alpha band is the last band, when it is labelled alpha
(``_nodata.alpha_index``).

One wiring test per writer: a source whose band 4 is data (RGB+NIR) keeps it data, a source
whose last band is alpha keeps it alpha, and an alpha label that is not the last band is not
carried.
"""

import numpy as np
import pytest
import rasterio
import rasterio.shutil  # not loaded by `import rasterio`; _getsubdataset_of calls rasterio.shutil.copy
from rasterio.enums import ColorInterp as CI
from rasterio.io import MemoryFile
from rasterio.windows import Window

from databricks.labs.gbx.pyrx.core import _nodata as nd
from databricks.labs.gbx.pyrx.core import agg, analysis, edit, features, focal
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core import ops, resample, stretch, tiling
from databricks.labs.gbx.pyrx.core.gridagg import _stamp_crs_bytes
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile
from databricks.labs.gbx.pyrx.functions import _dataset_to_gtiff_bytes

from . import _alpha_layouts as al

# writer name -> fn(ds) -> GTiff bytes with the source's band count
WRITERS = {
    "update_type": lambda ds: edit.update_type(ds, "Byte"),
    "init_nodata": edit.init_nodata,
    "threshold": lambda ds: edit.threshold(ds, ">", 1.0),
    "set_srid": lambda ds: edit.set_srid(ds, 4326),
    "set_crs": lambda ds: edit.set_crs(ds, "EPSG:4326"),
    "focal_filt": lambda ds: focal.filt(ds, 3, "max"),
    "focal_convolve": lambda ds: focal.convolve(ds, np.ones((3, 3)) / 9.0),
    "fill_nodata": features.fill_nodata,
    "percentile_stretch": lambda ds: stretch.percentile_stretch(ds, 2.0, 98.0),
    "resample": lambda ds: resample.resample_by_factor(ds, 2.0),
    "as_format": lambda ds: ops.as_format(ds, "GTiff"),
    "build_overviews": lambda ds: ops.build_overviews(ds, [2]),
    "cog_convert": lambda ds: analysis.cog_convert(ds, "DEFLATE", 256, "NEAREST"),
    "stamp_crs": lambda ds: _stamp_crs_bytes(ds, "EPSG:3857"),
    "dataset_to_bytes": _dataset_to_gtiff_bytes,
    "getsubdataset": lambda ds: _getsubdataset_of(ds),
}


def _getsubdataset_of(ds):
    """``accessors.getsubdataset`` of a container whose one subdataset is a copy of *ds*."""
    import os
    import tempfile
    from types import SimpleNamespace

    from databricks.labs.gbx.pyrx.core import accessors
    from databricks.labs.gbx.pyrx.core.local_temp import local_temp_root

    with tempfile.TemporaryDirectory(dir=local_temp_root()) as tmp:
        path = os.path.join(tmp, "sub.tif")
        rasterio.shutil.copy(ds, path, driver="GTiff")
        return accessors.getsubdataset(SimpleNamespace(subdatasets=[path]), "sub.tif")


def _alpha_bands(raster_bytes):
    return [
        i
        for i, c in enumerate(rasterio.open(MemoryFile(raster_bytes)).colorinterp)
        if c == CI.alpha
    ]


# group/tile writers: fn(raster bytes) -> GTiff bytes with the source's band count
GROUP_WRITERS = {
    "merge": lambda raw: agg.merge_tiles([raw, raw]),
    "combineavg": lambda raw: agg.combineavg_tiles([raw, raw]),
    "combine_min": lambda raw: agg.combine_min_tiles([raw, raw]),
    "align_to": lambda raw: agg.align_to_tiles(raw, raw),
    "retile": lambda raw: _retile(raw),
}


def _retile(raw):
    with MemoryFile(raw) as mf, mf.open() as ds:
        return tiling.retile(ds, 2, 2)[0]


@pytest.mark.parametrize("writer", list(GROUP_WRITERS))
@pytest.mark.parametrize("name", list(al.LAYOUTS))
def test_a_group_writer_labels_alpha_where_its_source_does(writer, name):
    out = GROUP_WRITERS[writer](al.layout_bytes(name))
    want = [len(al.LAYOUTS[name][0]) - 1] if al.LAYOUTS[name][1] else []
    assert _alpha_bands(out) == want, f"{writer} of {name}"


@pytest.mark.parametrize("writer", list(WRITERS))
@pytest.mark.parametrize("name", list(al.LAYOUTS))
def test_a_writer_labels_alpha_where_its_source_does(writer, name):
    raw = al.layout_bytes(name, nodata=None)
    with MemoryFile(raw) as mf, mf.open() as ds:
        out = WRITERS[writer](ds)
        want = [ds.count - 1] if al.LAYOUTS[name][1] else []
    assert _alpha_bands(out) == want, f"{writer} of {name}"


def _write_file(tmp_path, name):
    path = str(tmp_path / f"{name}.tif")
    with open(path, "wb") as fh:
        fh.write(al.layout_bytes(name))
    return path


@pytest.mark.parametrize("name", list(al.LAYOUTS))
def test_eager_fromfile_labels_alpha_where_its_source_does(tmp_path, name):
    """``rst_fromfile`` with ``materialize`` re-encodes the file into the tile: RGB+NIR keeps
    band 4 as data and an RGBA file keeps its alpha band."""
    from databricks.labs.gbx.pyrx import functions as F

    path = _write_file(tmp_path, name)
    want = [len(al.LAYOUTS[name][0]) - 1] if al.LAYOUTS[name][1] else []
    tile = F._fromfile_impl(path, "GTiff", True)
    raster = tile["raster"] if isinstance(tile, dict) else tile.raster
    assert raster is not None
    assert _alpha_bands(bytes(raster)) == want


@pytest.mark.parametrize("name", list(al.LAYOUTS))
def test_virtual_tile_reads_label_alpha_where_their_source_does(tmp_path, name):
    """The window of a virtual tile (every read path of ``open_tile`` and the materializers)
    labels alpha exactly where the source file does."""
    path = _write_file(tmp_path, name)
    want = [len(al.LAYOUTS[name][0]) - 1] if al.LAYOUTS[name][1] else []
    tile = VirtualTile(cellid=0, path=path, window=(0, 0, 3, 3))
    with rasterio.open(path) as src:
        assert _alpha_bands(ot._window_dataset_bytes(src, Window(0, 0, 3, 3))) == want
    with ot.open_tile(tile) as ds:
        assert [i for i, c in enumerate(ds.colorinterp) if c == CI.alpha] == want
    assert _alpha_bands(ot.materialize_to_bytes(tile).raster) == want
    assert _alpha_bands(ot._windowed_materialize_bytes(tile)) == want
    assert _alpha_bands(ot._tile_to_bytes(tile)) == want


@pytest.mark.parametrize("target", ["EPSG:3857", "ESRI:54008"])
@pytest.mark.parametrize("name", list(al.LAYOUTS))
def test_a_virtual_tile_with_a_target_crs_labels_alpha_where_its_source_does(
    tmp_path, name, target
):
    """The lazy reprojected read (``_warp_bytes``) carries the alpha label of the window it
    warps, through an EPSG and a non-EPSG target."""
    path = _write_file(tmp_path, name)
    want = [len(al.LAYOUTS[name][0]) - 1] if al.LAYOUTS[name][1] else []
    tile = VirtualTile(cellid=0, path=path, window=(0, 0, 4, 4), crs=target)
    with ot.open_tile(tile) as ds:
        assert [i for i, c in enumerate(ds.colorinterp) if c == CI.alpha] == want
    assert _alpha_bands(ot.materialize_to_bytes(tile).raster) == want


@pytest.mark.parametrize("name", list(al.LAYOUTS))
def test_the_empty_tile_of_a_disjoint_clip_declares_no_nodata_over_an_alpha_band(name):
    """The 1x1 tile standing for a clip that misses the raster is 0 in every band: for a
    source with an alpha band that is alpha 0 with no NoData declared (a NoData would shadow
    the alpha band); otherwise it is declared NoData 0. It is labelled alpha where its source
    is."""
    alpha_last = al.LAYOUTS[name][1]
    with MemoryFile(al.layout_bytes(name)) as mf, mf.open() as ref:
        empty = ot._empty_dataset_bytes(ref)
        with MemoryFile(empty) as em, em.open() as out:
            assert (out.width, out.height, out.count) == (1, 1, ref.count)
            assert (out.read() == 0).all()
            assert out.nodata == (None if alpha_last else 0)
            assert [i for i, c in enumerate(out.colorinterp) if c == CI.alpha] == (
                [ref.count - 1] if alpha_last else []
            )
            assert not nd.valid_mask(out).any(), "the empty tile holds a valid pixel"
