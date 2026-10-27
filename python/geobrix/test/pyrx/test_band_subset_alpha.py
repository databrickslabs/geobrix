"""A band subset keeps the Alpha label only where the source's alpha band is selected last.

The alpha band is the LAST band, when labelled alpha (``_nodata.alpha_index``). A band subset
carries that label when ALL of these hold: the selected band is the source's alpha band, it is
selected last, and at least one data band is selected with it. Otherwise every selected band is
data, valid wherever its own NoData or mask says so: a lone alpha band (``rst_band(tile, 4)`` of
an RGBA raster) is wanted for its values, and an alpha label on any other band is data.

The lazy read of a virtual tile with a pending band selection, the eager ``rst_band`` and the
heavyweight ``gbx_rst_band`` (``test_parity_bng_quadbin_raster_grid.py``) agree on this.
"""

import numpy as np
import pytest
import rasterio
from rasterio.enums import ColorInterp as CI
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core import _nodata as nd
from databricks.labs.gbx.pyrx.core import edit
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

from . import _alpha_layouts as al

# (layout, bands, 0-based position of the alpha band of the subset or None)
CASES = [
    ("rgba", (4,), None),  # a lone alpha band is data
    (
        "rgba_x",
        (2, 4),
        None,
    ),  # band 4 is data here: the alpha label is not the last band
    ("rgba_x", (4,), None),
    ("rgba", (1, 2, 3, 4), 3),  # the alpha band, selected last with data: kept
    ("rgba", (4, 1), None),  # selected first, so not last
    ("rgba", (1, 4), 1),  # selected last with a data band: kept
    ("rgba", (4, 4), None),  # no data band
    ("rgba", (1,), None),
    ("gray_x_alpha", (1, 3), 1),
    ("gray_x_alpha", (3,), None),
    ("rgb_nir", (1, 4), None),
]


def _source(name):
    """Source bytes and arrays: every band 7, except zeros in rows 0 and 2 of the last band
    and, for ``rgba_x``, of band 4 (its alpha-labelled band) too."""
    count = len(al.LAYOUTS[name][0])
    data = np.full((count, 4, 4), 7, dtype="uint8")
    data[-1, [0, 2]] = 0
    if name == "rgba_x":
        data[3, [0, 2]] = 0
    return al.layout_bytes(name, data=data), data


def _expected_valid(name, bands, alpha_at, data):
    """Valid pixels by the rule, from the source arrays: alpha above 0 where the subset has an
    alpha band, every pixel otherwise (the data bands are all 7 or have a data band beside).
    """
    if alpha_at is None:
        return np.ones((4, 4), dtype=bool)
    return data[bands[alpha_at] - 1] > 0


def _labels_and_valid(raw):
    with MemoryFile(raw) as mf, mf.open() as ds:
        alpha = [i for i, c in enumerate(ds.colorinterp) if c == CI.alpha]
        return alpha, nd.valid_mask(ds), ds.read()


@pytest.mark.parametrize("name,bands,alpha_at", CASES)
def test_subset_colorinterp_is_the_one_rule(name, bands, alpha_at):
    labels = al.LAYOUTS[name][0]
    got = nd.subset_colorinterp(labels, bands)
    assert len(got) == len(bands)
    assert [i for i, c in enumerate(got) if c == CI.alpha] == (
        [] if alpha_at is None else [alpha_at]
    )
    assert nd.alpha_index(got) == alpha_at


@pytest.mark.parametrize("name,bands,alpha_at", CASES)
def test_lazy_band_subset_agrees_with_the_rule(tmp_path, name, bands, alpha_at):
    """Every lazy read of a tile with a pending band selection: window read, materialize,
    block-streaming materialize."""
    raw, data = _source(name)
    path = str(tmp_path / "src.tif")
    with open(path, "wb") as fh:
        fh.write(raw)
    tile = VirtualTile(
        cellid=0,
        path=path,
        window=(0, 0, 4, 4),
        metadata={ot.PENDING_BANDS: ",".join(str(b) for b in bands)},
    )
    want = _expected_valid(name, bands, alpha_at, data)
    outs = {
        "materialize_to_bytes": ot.materialize_to_bytes(tile).raster,
        "_windowed_materialize_bytes": ot._windowed_materialize_bytes(tile),
        "_tile_to_bytes": ot._tile_to_bytes(tile),
    }
    with rasterio.open(path) as src:
        outs["_window_dataset_bytes"] = ot._window_dataset_bytes(
            src, rasterio.windows.Window(0, 0, 4, 4), pending=(list(bands), None, None)
        )
    for label, out in outs.items():
        alpha, valid, got = _labels_and_valid(out)
        assert alpha == ([] if alpha_at is None else [alpha_at]), f"{label}: {alpha}"
        assert (valid == want).all(), f"{label}: validity differs from the rule"
        assert (got == data[[b - 1 for b in bands]]).all(), f"{label}: values"
    with ot.open_tile(tile) as ds:
        assert [i for i, c in enumerate(ds.colorinterp) if c == CI.alpha] == (
            [] if alpha_at is None else [alpha_at]
        )


@pytest.mark.parametrize("name", ["rgba", "rgba_x"])
def test_eager_rst_band_of_the_alpha_labelled_band_is_data(name):
    """``edit.band`` writes the band as data, valid wherever its values and NoData say: the
    same as the lazy read of the same band (the cases above)."""
    raw, data = _source(name)
    with MemoryFile(raw) as mf, mf.open() as ds:
        alpha, valid, got = _labels_and_valid(edit.band(ds, 4))
    assert alpha == []
    assert valid.all()
    assert (got[0] == data[3]).all()


def test_a_read_with_no_band_selection_keeps_a_raster_that_is_only_an_alpha_band(
    tmp_path,
):
    """The subset rule is for a band selection: reading the whole of a one-band alpha raster
    is not a lone-alpha extraction, so the label stays."""
    prof = dict(
        driver="GTiff",
        width=4,
        height=4,
        count=1,
        dtype="uint8",
        photometric="MINISBLACK",
    )
    path = str(tmp_path / "alpha_only.tif")
    with rasterio.open(path, "w", **prof) as dst:
        dst.colorinterp = (CI.alpha,)
        dst.write(np.arange(16, dtype="uint8").reshape(1, 4, 4))
    tile = VirtualTile(cellid=0, path=path, window=(0, 0, 4, 4))
    for raw in (
        ot.materialize_to_bytes(tile).raster,
        ot._windowed_materialize_bytes(tile),
    ):
        alpha, valid, _ = _labels_and_valid(raw)
        assert alpha == [0]
        assert valid.sum() == 15  # the one 0 pixel is transparent
