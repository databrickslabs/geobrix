import numpy as np

from databricks.labs.gbx.pyrx.imagery import zbuffer_ortho


def test_zbuffer_ortho_top_surface_rgb_and_dsm():
    # two points in cell (0,0): higher-z wins RGB; a third in a far cell.
    x = np.array([0.5, 0.5, 10.5])
    y = np.array([0.5, 0.5, 0.5])
    z = np.array([1.0, 5.0, 2.0])
    r = np.array([10, 200, 30], "u1")
    g = np.array([10, 200, 30], "u1")
    b = np.array([10, 200, 30], "u1")
    rgb, dsm, transform = zbuffer_ortho(
        x, y, z, r, g, b, xmin=0, ymin=0, xmax=11, ymax=1, px=1.0
    )
    assert rgb.shape[0] == 3, "RGB is band-first (3,H,W)"
    assert tuple(int(v) for v in rgb[:, 0, 0]) == (
        200,
        200,
        200,
    )  # higher-z point's color wins cell (0,0)
    assert float(dsm[0, 0]) == 5.0  # DSM = max z in the cell
    assert rgb.dtype == np.uint8 and dsm.dtype == np.float32


def test_zbuffer_ortho_empty_cells_are_nodata():
    x = np.array([0.5])
    y = np.array([0.5])
    z = np.array([3.0])
    r = g = b = np.array([100], "u1")
    rgb, dsm, transform = zbuffer_ortho(
        x, y, z, r, g, b, xmin=0, ymin=0, xmax=3, ymax=1, px=1.0
    )
    # a cell with no points -> RGB 0 (nodata) and DSM NaN
    assert tuple(int(v) for v in rgb[:, 0, 2]) == (0, 0, 0)
    assert np.isnan(dsm[0, 2])
