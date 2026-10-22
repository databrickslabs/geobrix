"""Helpers shared by the Zarr tests, the doc tests and the bench harness.

Not public API. ``make_era5_like`` writes a small synthetic stand-in for an ERA5
reanalysis cube (hourly 2 m temperature and 10 m wind on a regular 2.5 degree grid)
in any of the three layouts GeoBrix has to handle: Zarr v3 with sharding, Zarr v3
without it, and Zarr v2. It needs ``xarray`` and ``zarr`` (the ``zarr_dbr19`` extra
and the light base), imported on first use.
"""

from __future__ import annotations

from pathlib import Path

# Grid of the synthetic cube. time x latitude x longitude.
TIME_STEPS = 48
LATITUDES = 73  # 90 .. -90, descending, 2.5 degree steps
LONGITUDES = 144  # 0 .. 357.5, 2.5 degree steps
SHAPE = (TIME_STEPS, LATITUDES, LONGITUDES)
INNER_CHUNKS = (12, 37, 72)
SHARDS = (
    24,
    74,
    144,
)  # whole multiples of INNER_CHUNKS; wider than the data in latitude
NOISE_SEED = 7

TIME_UNITS = "hours since 1900-01-01"


def era5_like_dataset():
    """The in-memory ERA5-like ``xarray.Dataset`` (float32 ``t2m`` and ``u10``).

    The fields are smooth in space and time plus seeded noise (seed 7), so two calls
    return identical arrays. ``time`` is a datetime64 coordinate; ``make_era5_like``
    encodes it to CF int64 hours.
    """
    import numpy as np
    import pandas as pd
    import xarray as xr

    time = pd.date_range("2023-01-01", periods=TIME_STEPS, freq="h")
    lat = np.linspace(90.0, -90.0, LATITUDES)
    lon = np.arange(LONGITUDES) * 2.5
    rng = np.random.default_rng(NOISE_SEED)

    la = np.deg2rad(lat)[None, :, None]
    lo = np.deg2rad(lon)[None, None, :]
    hour = np.arange(TIME_STEPS)[:, None, None]
    t2m = 288.0 + 15.0 * np.cos(la) + 4.0 * np.sin(2 * np.pi * hour / 24 + lo)
    u10 = 5.0 * np.sin(2 * la) * np.cos(lo + 2 * np.pi * hour / 48)
    t2m = (t2m + rng.normal(0.0, 0.5, SHAPE)).astype("float32")
    u10 = (u10 + rng.normal(0.0, 0.8, SHAPE)).astype("float32")

    dims = ("time", "latitude", "longitude")
    return xr.Dataset(
        {
            "t2m": (dims, t2m, {"units": "K", "long_name": "2 metre temperature"}),
            "u10": (
                dims,
                u10,
                {"units": "m s-1", "long_name": "10 metre U wind component"},
            ),
        },
        coords={
            "time": ("time", time, {"long_name": "time"}),
            "latitude": (
                "latitude",
                lat,
                {"units": "degrees_north", "long_name": "latitude"},
            ),
            "longitude": (
                "longitude",
                lon,
                {"units": "degrees_east", "long_name": "longitude"},
            ),
        },
    )


def make_era5_like(path, *, zarr_format: int = 3, sharded: bool = True) -> Path:
    """Write the ERA5-like cube to ``path`` as a Zarr store and return the path.

    Layouts (``t2m`` and ``u10``; chunks always ``(12, 37, 72)``):

    - ``zarr_format=3, sharded=True``: shards ``(24, 74, 144)``, zstd.
    - ``zarr_format=3, sharded=False``: one object per chunk, zstd.
    - ``zarr_format=2``: blosc-lz4, chunk keys such as ``t2m/0.0.0``
      (``dimension_separator "."``).

    ``time`` is CF-encoded int64 ``hours since 1900-01-01`` (standard calendar);
    ``latitude`` and ``longitude`` carry ``degrees_north`` / ``degrees_east`` units.
    Metadata is not consolidated.
    """
    if zarr_format not in (2, 3):
        raise ValueError(f"zarr_format must be 2 or 3, got {zarr_format!r}")
    if sharded and zarr_format != 3:
        raise ValueError("Zarr v2 has no sharding; use zarr_format=3 with sharded=True")

    import numcodecs
    from zarr.codecs import ZstdCodec

    if zarr_format == 3:
        var_enc = {"chunks": INNER_CHUNKS, "compressors": (ZstdCodec(level=3),)}
        if sharded:
            var_enc["shards"] = SHARDS
    else:
        var_enc = {
            "chunks": INNER_CHUNKS,
            "compressors": (numcodecs.Blosc(cname="lz4"),),
            "chunk_key_encoding": {"name": "v2", "separator": "."},
        }
    encoding = {name: dict(var_enc) for name in ("t2m", "u10")}
    encoding["time"] = {"units": TIME_UNITS, "calendar": "standard", "dtype": "int64"}
    # CF: coordinate variables carry no _FillValue.
    encoding["latitude"] = {"_FillValue": None}
    encoding["longitude"] = {"_FillValue": None}

    path = Path(path)
    era5_like_dataset().to_zarr(
        str(path),
        mode="w",
        zarr_format=zarr_format,
        consolidated=False,
        encoding=encoding,
    )
    return path
