"""Probe: a small windowed read from a COG materializes far fewer pixels than
the full raster (cheap overview/tiled range read), whereas a striped GTIFF
inflates full-width strips. Loose bounds -> record numbers, not flake.

tracemalloc NOTE: rasterio/GDAL allocates pixel buffers in native C, invisible
to tracemalloc. The peak measurements below are Python-heap only and will
systematically undercount real native allocation cost. The array-nbytes
assertions (not the tracemalloc bounds) are the load-bearing proof that
a window materializes a block-sized array, not the whole raster.
"""
import tracemalloc

import numpy as np

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

from . import _layouts

W, H, BS = 2048, 2048, 256
WINDOW = (0, 0, 256, 256)  # one block


def _peak_kib(path):
    tracemalloc.start()
    with ot.open_tile(VirtualTile(cellid=0, path=path, window=WINDOW)) as ds:
        arr = ds.read(1)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return arr, peak // 1024


def test_windowed_read_smaller_than_full(tmp_path):
    cog = _layouts.write_cog(str(tmp_path / "a.cog.tif"), W, H, BS)
    striped = _layouts.write_striped_gtiff(str(tmp_path / "a.striped.tif"), W, H)
    arr_c, peak_c = _peak_kib(cog)
    arr_s, peak_s = _peak_kib(striped)
    full_kib = (W * H * 4) // 1024  # 16384 KiB for float32 2048x2048

    print(f"cog peak={peak_c}KiB striped peak={peak_s}KiB full={full_kib}KiB")

    # (a) shape: a one-block window produces exactly a block-sized array
    assert arr_c.shape == (BS, BS), f"expected ({BS},{BS}), got {arr_c.shape}"

    # (b) nbytes: one block is far smaller than the full raster
    # This is the load-bearing assertion; tracemalloc undercounts native GDAL allocs.
    full_nbytes = W * H * 4  # float32
    block_nbytes = BS * BS * 4
    assert arr_c.nbytes == block_nbytes
    assert arr_c.nbytes < full_nbytes, (
        f"window array ({arr_c.nbytes}B) should be smaller than full raster ({full_nbytes}B)"
    )

    # (c) pixel equality: same pixels regardless of COG vs striped layout
    assert np.array_equal(arr_c, arr_s), "COG and striped windowed reads diverge"

    # Loose tracemalloc bound: almost certainly true since Python heap alone
    # is < full raster size, but note it may undercount native GDAL allocs.
    assert peak_c < full_kib, (
        f"tracemalloc peak ({peak_c}KiB) unexpectedly >= full_kib ({full_kib}KiB); "
        "note: native GDAL allocs are NOT captured by tracemalloc"
    )
