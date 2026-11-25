"""Before / after timing of the H3 covering pass on an HLS-like 30 m UTM tile at res 12 (spec R1, §5.3)."""

import time

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import gridagg

pytestmark = pytest.mark.perf


def _tile(n):
    y, x = np.mgrid[0:n, 0:n]
    data = (1000 + (x * 13 + y * 7) % 400).astype("float32")
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=n,
            height=n,
            count=1,
            dtype="float32",
            crs="EPSG:32615",
            transform=from_origin(446000.0, 4650000.0, 30.0, 30.0),
            nodata=-9999.0,
        ) as dst:
            dst.write(data, 1)
        return mf.read()


def _time(b, vectorised, monkeypatch):
    monkeypatch.setattr(gridagg, "_H3_COVERING_VECTORISED", vectorised)
    with MemoryFile(b) as mf, mf.open() as ds:
        t0 = time.perf_counter()
        out = gridagg.raster_to_grid_partial(
            ds, 12, "h3", coverage="complete", assignment="covering"
        )
        return time.perf_counter() - t0, out


def test_h3_covering_res12_before_after(monkeypatch):
    b = _tile(200)  # 6 km x 6 km, 40,000 pixels, about 117,000 res-12 cells
    # Both paths run in this process, so the bound is a ratio, not a wall time. The vectorised run is the short one,
    # so a noisy neighbour on a CI runner hits it hardest: its first call also pays the imports and caches, and the
    # ratio takes its best of three after that. The slow per-pixel run only gets slower under load, which helps.
    _time(b, True, monkeypatch)  # warm-up
    runs = [_time(b, True, monkeypatch) for _ in range(3)]
    t_new, new = min(runs, key=lambda r: r[0])
    t_old, old = _time(b, False, monkeypatch)
    old_map = {c["cellID"]: (c["count"], c["sum"]) for c in old[0]}
    assert {c["cellID"] for c in new[0]} == set(old_map)
    for c in new[0]:
        assert c["count"] == pytest.approx(old_map[c["cellID"]][0], rel=1e-9, abs=1e-12)
    print(
        f"\nH3 covering res 12, 200 x 200 30 m tile: per-pixel {t_old:.2f} s, vectorised {t_new:.2f} s, "
        f"speedup {t_old / t_new:.1f}x"
    )
    assert t_old / t_new >= 3.0
