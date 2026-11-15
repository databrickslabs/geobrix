"""Comparisons of a GeoBrix tile with the plain-GDAL reading of the same file."""

import math
from typing import Optional

import numpy as np


def _horizontal(crs):
    return crs.sub_crs_list[0] if crs.is_compound else crs


def same_crs(a: Optional[str], b: Optional[str]) -> bool:
    """Whether two CRS texts (WKT, 'EPSG:n' or None) name the same horizontal system; None only equals None."""
    from pyproj import CRS

    if not a or not b:
        return not a and not b
    return _horizontal(CRS.from_user_input(a)).equals(
        _horizontal(CRS.from_user_input(b)), ignore_axis_order=True
    )


def same_nodata(a: Optional[float], b: Optional[float]) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if math.isnan(a) or math.isnan(b):
        return math.isnan(a) and math.isnan(b)
    return math.isclose(a, b, rel_tol=1e-6)


def assert_tile_is(
    tile,
    ref,
    what: str,
    tol: Optional[float] = None,
    crs: bool = True,
    nodata: bool = True,
) -> None:
    """Open ``tile`` the way a GeoBrix function does and hold it to ``ref`` (a ``_raster.Reference``): band count,
    size, data type, every pixel, NoData, georeferencing and CRS. ``tol`` is a mean absolute pixel error allowed
    (a lossy format); None is exact."""
    from databricks.labs.gbx.pyrx.core import open_tile as ot

    with ot._open(tile) as ds:
        assert (
            ds.count == ref.count
        ), f"{what}: {ds.count} bands, GDAL reads {ref.count}"
        assert (ds.width, ds.height) == ref.size, f"{what}: size"
        got = ds.read()
        for i, want in enumerate(ref.arrays):
            assert (
                got[i].dtype.name == ref.dtypes[i]
            ), f"{what}: band {i + 1} is {got[i].dtype.name}, GDAL reads {ref.dtypes[i]}"
            if tol is None:
                assert np.array_equal(
                    got[i], want, equal_nan=got[i].dtype.kind == "f"
                ), f"{what}: band {i + 1} pixels differ"
            else:
                err = float(np.abs(got[i].astype("f8") - want.astype("f8")).mean())
                assert err <= tol, f"{what}: band {i + 1} mean error {err} over {tol}"
        if nodata:
            for i, want in enumerate(ref.nodata):
                assert same_nodata(
                    ds.nodatavals[i], want
                ), f"{what}: band {i + 1} NoData {ds.nodatavals[i]}, GDAL reads {want}"
        if crs:
            assert same_crs(
                ds.crs.to_wkt() if ds.crs else None, ref.crs
            ), f"{what}: CRS {ds.crs and ds.crs.to_string()}"
        if tol is None:
            assert np.allclose(
                ds.transform.to_gdal(), ref.gt, rtol=1e-9, atol=1e-9
            ), f"{what}: geotransform {ds.transform.to_gdal()} vs {ref.gt}"


def tile_dict(row) -> dict:
    return row.asDict(recursive=True) if hasattr(row, "asDict") else row
