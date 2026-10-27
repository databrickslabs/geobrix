"""The eager light warp keeps a source's own (per-dataset) mask band.

A masked source (JPEG/YCbCr imagery often carries an internal mask band) reprojected by the
eager ``rst_transform`` / ``rst_transformcrs`` (``warp.reproject_to_*``) came back with every
masked pixel valid, and so did the identity short-circuit (a raster already in the target CRS,
re-encoded without its mask). The lazy path (``open_tile`` with a target CRS) already carries
it; the eager warp must equal it: masked pixels stay invalid through the warp, as through the
identity, and a source that declares a NoData has the masked pixels take it.
"""

import numpy as np
import pytest
import rasterio

from databricks.labs.gbx.pyrx import functions as F
from databricks.labs.gbx.pyrx.core import warp

from . import test_virtual_tile_mask as vm

N, MASKED_ROWS = vm.N, vm.MASKED_ROWS


def _eager(path, fn, target):
    with rasterio.open(path) as src:
        raw = fn(src, target)
    return vm.decode(raw)


@pytest.mark.parametrize("nodata", [None, 7.0])
@pytest.mark.parametrize(
    "target,warper",
    [
        ("EPSG:3857", lambda ds, t: warp.reproject_to_srid(ds, int(t.split(":")[1]))),
        ("EPSG:3857", warp.reproject_to_crs),
        ("ESRI:54008", warp.reproject_to_crs),
    ],
    ids=["srid-3857", "crs-3857", "crs-54008"],
)
def test_the_eager_warp_keeps_the_mask(tmp_path, target, warper, nodata):
    """Output pixels that come from masked source rows are invalid, and nothing else is
    (outside the footprint the warp fills with the NoData, or plain zeros with none)."""
    masked = vm.write_masked(tmp_path / "m.tif", nodata=nodata)
    index = vm._index_file(tmp_path / "idx.tif", nodata)
    data, valid, _ = _eager(masked, warper, target)
    idx, _, _ = _eager(index, warper, target)
    fill = 0 if nodata is None else nodata
    covered = idx[0] != fill
    from_masked = covered & np.isin(idx[0].astype("int64") - 100, MASKED_ROWS)
    assert from_masked.any(), "fixture: the warp must keep some masked source pixels"
    want = (
        ~from_masked if nodata is None else covered & ~from_masked & (data[0] != fill)
    )
    assert (valid[0] == want).all(), (
        f"{target} nodata={nodata}: masked pixels valid: "
        f"{int((valid[0] & from_masked).sum())}, valid pixels lost: "
        f"{int((~valid[0] & want).sum())}"
    )


@pytest.mark.parametrize("nodata", [None, 7.0])
@pytest.mark.parametrize(
    "warper,target",
    [
        (lambda ds, t: warp.reproject_to_srid(ds, 4326), 4326),
        (lambda ds, t: warp.reproject_to_crs(ds, "EPSG:4326"), 4326),
    ],
    ids=["srid-identity", "crs-identity"],
)
def test_the_identity_short_circuit_keeps_the_mask(tmp_path, warper, target, nodata):
    masked = vm.write_masked(tmp_path / "m.tif", nodata=nodata)
    got = _eager(masked, warper, target)
    want = vm.source_view(masked)
    vm.assert_same_pixels(got[:2], want, "identity")


@pytest.mark.parametrize("srid", [3857, 4326])
def test_rst_transform_keeps_the_mask(tmp_path, srid):
    """The tile function behind ``rst_transform``, from a virtual tile (the default)."""
    masked = vm.write_masked(tmp_path / "m.tif")
    raw = F._transform_bytes(vm.virtual(masked), srid)
    data, valid, _ = vm.decode(raw)
    assert (~valid).any(), "the mask is lost"
    if srid == 4326:
        vm.assert_same_pixels((data, valid), vm.source_view(masked), "rst_transform")
