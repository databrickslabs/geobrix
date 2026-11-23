"""gbx_rst_h3_fitresolution core: the H3 area table, the pixel area of the centre pixel, the two rules (spec §5.1)."""

import contextlib
import math

import h3
import numpy as np
import pytest
from pyproj import Geod, Transformer
from rasterio.io import MemoryFile
from rasterio.transform import Affine, from_origin

from databricks.labs.gbx.pyrx.core import h3fit

HRAP = "+proj=stere +lat_0=90 +lat_ts=60 +lon_0=-105 +k=1 +x_0=0 +y_0=0 +a=6371200 +b=6371200 +units=m +no_defs"
SPEC_TABLE = {
    5: 252903858.182,
    6: 36129062.164,
    7: 5161293.360,
    8: 737327.598,
    9: 105332.513,
    10: 15047.502,
    11: 2149.643,
    12: 307.092,
    13: 43.870,
}


@contextlib.contextmanager
def _ds(width, height, crs, transform):
    data = np.ones((height, width), "float32")
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=width,
            height=height,
            count=1,
            dtype="float32",
            crs=crs,
            transform=transform,
            nodata=-9999.0,
        ) as dst:
            dst.write(data, 1)
        with mf.open() as ds:
            yield ds


def _centred(crs, lon, lat, px, n=9):
    """An n x n north-up grid of px-sized pixels whose centre pixel holds (lon, lat)."""
    if crs == "EPSG:4326":
        x, y = lon, lat
    else:
        x, y = Transformer.from_crs("EPSG:4326", crs, always_xy=True).transform(
            lon, lat
        )
    return from_origin(x - px * (n / 2), y + px * (n / 2), px, px)


def _geodesic_centre_area(ds):
    t, c, r = ds.transform, ds.width // 2, ds.height // 2
    pts = [t * (c, r), t * (c + 1, r), t * (c + 1, r + 1), t * (c, r + 1)]
    if ds.crs is not None and ds.crs.to_string() != "EPSG:4326":
        tr = Transformer.from_crs(ds.crs.to_wkt(), "EPSG:4326", always_xy=True)
        pts = [tr.transform(x, y) for x, y in pts]
    area, _ = Geod(ellps="WGS84").polygon_area_perimeter(
        [p[0] for p in pts], [p[1] for p in pts]
    )
    return abs(area)


def test_area_table_matches_h3_and_the_spec():
    assert len(h3fit.H3_AVG_CELL_AREA_M2) == 16
    for res, area in enumerate(h3fit.H3_AVG_CELL_AREA_M2):
        assert area == pytest.approx(h3.average_hexagon_area(res, "m^2"), rel=1e-12)
    for res, area in SPEC_TABLE.items():
        assert h3fit.H3_AVG_CELL_AREA_M2[res] == pytest.approx(area, abs=0.0005)


# (name, crs, pixel size in CRS units, spec pixel area m2, not_larger, log_nearest) -- spec §5.1 and §11
CASES = [
    ("stage4_hrap", HRAP, 4762.5, 18.15e6, 7, 6),
    ("one_24th_degree", "EPSG:4326", 1 / 24, 15.98e6, 7, 6),
    ("grace_0125", "EPSG:4326", 0.125, 143.8e6, 6, 5),
    ("hls_30m_utm", "EPSG:32615", 30.0, 900.0, 12, 11),
    ("cdl_10m_utm", "EPSG:32615", 10.0, 100.0, 13, 13),
]


@pytest.mark.parametrize(
    "name,crs,px,spec_area,want,want_log", CASES, ids=[c[0] for c in CASES]
)
def test_spec_table_at_iowa(name, crs, px, spec_area, want, want_log):
    with _ds(9, 9, crs, _centred(crs, -93.6, 42.0, px)) as ds:
        area = h3fit.pixel_area_m2(ds)
        assert area == pytest.approx(_geodesic_centre_area(ds), rel=1e-9)
        assert area == pytest.approx(spec_area, rel=0.01)
        assert h3fit.fit_resolution_of(ds) == want
        assert h3fit.fit_resolution_of(ds, "not_larger") == want
        assert h3fit.fit_resolution_of(ds, "log_nearest") == want_log


def test_rule_edges():
    big = h3fit.H3_AVG_CELL_AREA_M2[0] * 10
    assert h3fit.fit_resolution(big) == 0
    assert h3fit.fit_resolution(0.01) == 15  # finer than res 15: the finest resolution
    assert (
        h3fit.fit_resolution(h3fit.H3_AVG_CELL_AREA_M2[7]) == 7
    )  # "no larger than" includes equality
    assert h3fit.fit_resolution(0.01, "log_nearest") == 15


@pytest.mark.parametrize("bad", ["nearest", "NOT_LARGER", "", None, 1])
def test_bad_rule_raises_naming_both_words(bad):
    with pytest.raises(ValueError, match="not_larger.*log_nearest"):
        h3fit.check_rule(bad)
    with pytest.raises(ValueError, match="rule"):
        h3fit.fit_resolution(100.0, bad)


@pytest.mark.parametrize("area", [0.0, -1.0, math.nan, math.inf])
def test_bad_area_raises(area):
    with pytest.raises(ValueError, match="pixel area"):
        h3fit.fit_resolution(area)


def test_degenerate_tiles():
    # Review Focus 3: no CRS is read as EPSG:4326; one pixel; a rotated geotransform; a tile near 80 N.
    with _ds(9, 9, None, _centred("EPSG:4326", -93.6, 42.0, 1 / 24)) as ds:
        assert h3fit.pixel_area_m2(ds) == pytest.approx(15.98e6, rel=0.01)
        assert h3fit.fit_resolution_of(ds) == 7
    with _ds(1, 1, "EPSG:32615", from_origin(446000.0, 4650000.0, 30.0, 30.0)) as ds:
        assert h3fit.fit_resolution_of(ds) == 12
    rot = Affine.rotation(30.0) * Affine.scale(30.0, -30.0)
    with _ds(9, 9, "EPSG:32615", Affine.translation(446000.0, 4650000.0) * rot) as ds:
        assert h3fit.pixel_area_m2(ds) == pytest.approx(
            _geodesic_centre_area(ds), rel=1e-9
        )
        assert h3fit.fit_resolution_of(ds) == 12
    with _ds(9, 9, "EPSG:4326", _centred("EPSG:4326", 10.0, 80.0, 0.125)) as ds:
        area = h3fit.pixel_area_m2(ds)
        assert area == pytest.approx(_geodesic_centre_area(ds), rel=1e-9)
        assert h3fit.fit_resolution_of(ds) == h3fit.fit_resolution(area)
