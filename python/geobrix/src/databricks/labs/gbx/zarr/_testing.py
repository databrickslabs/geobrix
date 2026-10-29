"""Helpers shared by the Zarr tests, the doc tests, the bench harness and the sample cube.

Not public API. It ships in the wheel because ``sample/zarr_era5.py`` (the bundle's ERA5-like
Zarr cube) builds on ``era5_like_dataset`` and ``make_era5_like``; keep their keywords.
``make_era5_like`` writes a small synthetic stand-in for an ERA5
reanalysis cube (hourly 2 m temperature and 10 m wind on a regular 2.5 degree grid)
in any of the three layouts GeoBrix has to handle: Zarr v3 with sharding, Zarr v3
without it, and Zarr v2. ``plain_store_rows`` reads such a store back as manifest rows
and ``rewrite_one_inner_chunk`` overrides one inner chunk of it. They need ``xarray``
and ``zarr`` (the ``zarr_dbr19`` extra and the light base), imported on first use.
"""

from __future__ import annotations

import json
from pathlib import Path

# Grid of the synthetic cube. time x latitude x longitude.
TIME_STEPS = 48
LATITUDES = 73  # 90 .. -90, descending, 2.5 degree steps
LONGITUDES = 144  # 0 .. 357.5, 2.5 degree steps
SHAPE = (TIME_STEPS, LATITUDES, LONGITUDES)
INNER_CHUNKS = (12, 37, 72)
# Whole multiples of INNER_CHUNKS; the latitude shard (74) is wider than the data (73).
SHARDS = (24, 74, 144)
NOISE_SEED = 7

TIME_UNITS = "hours since 1900-01-01"

# The fields of the cube in the order their noise is drawn, with the standard deviation of
# that noise and the attributes each variable carries.
_FIELDS = ("t2m", "u10", "v10", "msl")
_NOISE_STD = {"t2m": 0.5, "u10": 0.8, "v10": 0.8, "msl": 50.0}
_ATTRS = {
    "t2m": {"units": "K", "long_name": "2 metre temperature"},
    "u10": {"units": "m s-1", "long_name": "10 metre U wind component"},
    "v10": {"units": "m s-1", "long_name": "10 metre V wind component"},
    "msl": {"units": "Pa", "long_name": "Mean sea level pressure"},
}


def era5_like_dataset(
    latitude=None, *, hours=TIME_STEPS, step=2.5, variables=("t2m", "u10"), noise=1.0
):
    """The in-memory ERA5-like ``xarray.Dataset`` (float32, default ``t2m`` and ``u10``).

    The fields are smooth in space and time plus seeded noise (seed 7), so two calls
    return identical arrays. ``time`` is a datetime64 coordinate; ``make_era5_like``
    encodes it to CF int64 hours. ``latitude`` replaces the regular latitudes with the
    given ones, for an irregular axis such as a Gaussian grid; it needs one value per
    latitude row.

    The defaults are the test cube: 48 hours on a 2.5 degree grid (73 x 144), ``t2m`` and
    ``u10``. ``hours`` and ``step`` (degrees) resize it, ``variables`` picks from
    ``t2m``, ``u10``, ``v10`` and ``msl`` (kept in that order), and ``noise`` scales the
    seeded noise (0 for none). The noise of each variable is drawn in that fixed order
    whatever ``variables`` holds, so a variable has the same noise in every call.
    """
    import numpy as np
    import pandas as pd
    import xarray as xr

    unknown = set(variables) - set(_FIELDS)
    if unknown:
        raise ValueError(f"unknown variables {sorted(unknown)}; pick from {_FIELDS}")
    n_lat, n_lon = round(180 / step) + 1, round(360 / step)
    shape = (hours, n_lat, n_lon)
    time = pd.date_range("2023-01-01", periods=hours, freq="h")
    lat = np.linspace(90.0, -90.0, n_lat)
    if latitude is not None:
        lat = np.asarray(latitude, dtype="float64")
        if lat.shape != (n_lat,):
            raise ValueError(f"latitude needs {n_lat} values, got {lat.shape}")
    lon = np.arange(n_lon) * step
    rng = np.random.default_rng(NOISE_SEED)

    la = np.deg2rad(lat)[None, :, None]
    lo = np.deg2rad(lon)[None, None, :]
    hour = np.arange(hours)[:, None, None]
    base = {
        "t2m": 288.0 + 15.0 * np.cos(la) + 4.0 * np.sin(2 * np.pi * hour / 24 + lo),
        "u10": 5.0 * np.sin(2 * la) * np.cos(lo + 2 * np.pi * hour / 48),
        "v10": 4.0 * np.cos(2 * la) * np.sin(lo - 2 * np.pi * hour / 48),
        "msl": 101325.0
        + 1500.0 * np.cos(3 * la) * np.cos(2 * lo + 2 * np.pi * hour / 48),
    }
    dims = ("time", "latitude", "longitude")
    data = {}
    for name in _FIELDS:
        spread = _NOISE_STD[name] * noise
        noisy = base[name] + rng.normal(0.0, spread, shape)
        if name in variables:
            data[name] = (dims, noisy.astype("float32"), dict(_ATTRS[name]))
    return xr.Dataset(
        data,
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


def make_era5_like(
    path,
    *,
    zarr_format: int = 3,
    sharded: bool | None = None,
    latitude=None,
    dataset=None,
    chunks=None,
    shards=None,
    compressor=None,
) -> Path:
    """Write the ERA5-like cube to ``path`` as a Zarr store and return the path.

    Layouts (every data variable; chunks ``(12, 37, 72)`` unless ``chunks`` says otherwise):

    - ``zarr_format=3, sharded=True``: shards ``(24, 74, 144)``, zstd.
    - ``zarr_format=3, sharded=False``: one object per chunk, zstd.
    - ``zarr_format=2``: blosc-lz4, chunk keys such as ``t2m/0.0.0``
      (``dimension_separator "."``).

    ``sharded=None`` (the default) means sharded for Zarr v3 and unsharded for v2,
    so ``make_era5_like(path, zarr_format=2)`` just works. v2 has no sharding:
    ``sharded=True`` with ``zarr_format=2`` raises ``ValueError``.

    ``time`` is CF-encoded int64 ``hours since 1900-01-01`` (standard calendar);
    ``latitude`` and ``longitude`` carry ``degrees_north`` / ``degrees_east`` units.
    Metadata is not consolidated. ``latitude`` is passed to ``era5_like_dataset``.

    ``dataset`` writes that ``xarray.Dataset`` (dims ``time``, ``latitude``, ``longitude``)
    in place of the default cube, and then ``latitude`` must stay unset. ``chunks`` and
    ``shards`` replace the defaults above (shards need whole multiples of the chunks), and
    ``compressor`` replaces the codec: a ``zarr.codecs`` codec for v3, a ``numcodecs``
    codec for v2.
    """
    if zarr_format not in (2, 3):
        raise ValueError(f"zarr_format must be 2 or 3, got {zarr_format!r}")
    if sharded is None:
        sharded = zarr_format == 3
    if sharded and zarr_format != 3:
        raise ValueError("Zarr v2 has no sharding; use zarr_format=3 with sharded=True")
    if dataset is not None and latitude is not None:
        raise ValueError("latitude belongs to the default cube; pass it in the dataset")

    import numcodecs
    from zarr.codecs import ZstdCodec

    chunks = INNER_CHUNKS if chunks is None else tuple(chunks)
    if zarr_format == 3:
        codec = ZstdCodec(level=3) if compressor is None else compressor
        var_enc = {"chunks": chunks, "compressors": (codec,)}
        if sharded:
            var_enc["shards"] = SHARDS if shards is None else tuple(shards)
    else:
        codec = numcodecs.Blosc(cname="lz4") if compressor is None else compressor
        var_enc = {
            "chunks": chunks,
            "compressors": (codec,),
            "chunk_key_encoding": {"name": "v2", "separator": "."},
        }
    if dataset is None:
        dataset = era5_like_dataset(latitude)
    encoding = {name: dict(var_enc) for name in dataset.data_vars}
    encoding["time"] = {"units": TIME_UNITS, "calendar": "standard", "dtype": "int64"}
    # CF: coordinate variables carry no _FillValue.
    encoding["latitude"] = {"_FillValue": None}
    encoding["longitude"] = {"_FillValue": None}

    path = Path(path)
    dataset.to_zarr(
        str(path),
        mode="w",
        zarr_format=zarr_format,
        consolidated=False,
        encoding=encoding,
    )
    return path


# --- a plain store as manifest rows -----------------------------------------------

_METADATA_FILES = ("zarr.json", ".zarray", ".zattrs", ".zgroup", ".zmetadata")


class PlainRows(list):
    """Manifest rows made by ``plain_store_rows``; ``root`` is the store they describe."""

    root: Path


def plain_store_rows(path, *, origin="gbx", identity=False):
    """Read a local Zarr store as ``(metas, rows, arrays)``.

    ``metas`` maps each metadata key to its bytes (``zarr.json``, ``t2m/zarr.json``,
    ``u10/.zarray`` ...). ``rows`` holds one ``shard`` row per object of a sharded
    array and one ``chunk`` row per object of any other array, with ``file`` relative
    to the store, ``offset`` 0, ``length`` the object's size, ``origin`` as given
    (``"external"`` makes it a registered store's own files) and a zero ``write_id``.
    ``shard_key`` is the key on a shard row and NULL on a chunk row, as register writes it.
    ``identity=True`` records each file's ``"<mtime_ms>:<size>"`` identity, as register
    does, so that a reader checks the file against it. ``arrays`` maps each array name to
    its ``ArrayMeta``.
    """
    from databricks.labs.gbx.zarr._meta import parse_v2, parse_v3

    root = Path(path)
    metas = {
        f.relative_to(root).as_posix(): f.read_bytes()
        for f in sorted(root.rglob("*"))
        if f.is_file() and f.name in _METADATA_FILES
    }
    arrays = {}
    for key, raw in metas.items():
        prefix, _, name = key.rpartition("/")
        if not prefix:
            continue
        if name == "zarr.json" and json.loads(raw).get("node_type") == "array":
            arrays[prefix] = parse_v3(prefix, json.loads(raw))
        elif name == ".zarray":
            zattrs = json.loads(metas.get(f"{prefix}/.zattrs", b"{}"))
            arrays[prefix] = parse_v2(prefix, json.loads(raw), zattrs)

    rows = PlainRows()
    rows.root = root
    for name, meta in sorted(arrays.items()):
        kind = "shard" if meta.shard is not None else "chunk"
        for f in sorted((root / name).rglob("*")):
            if not f.is_file() or f.name in _METADATA_FILES:
                continue
            key = f.relative_to(root / name).as_posix()
            rows.append(
                dict(
                    kind=kind,
                    array=name,
                    key=key,
                    shard_key=key if kind == "shard" else None,
                    file=f.relative_to(root).as_posix(),
                    offset=0,
                    length=f.stat().st_size,
                    checksum=None,
                    origin=origin,
                    identity=(
                        f"{f.stat().st_mtime_ns // 1_000_000}:{f.stat().st_size}"
                        if identity
                        else None
                    ),
                    write_id="0" * 32,
                    tag=None,
                    meta=None,
                )
            )
    return metas, rows, arrays


def rewrite_one_inner_chunk(rows, meta, dir, chunk, value):
    """Override one inner chunk of a sharded array with a chunk filled with ``value``.

    The chunk is encoded as a standalone chunk of ``inner_meta_doc(meta)`` in a
    ``MemoryStore`` and written to a new file in ``dir``; its row is ``origin="gbx"``
    with ``file`` the file name, relative to ``dir``, so a reader whose ``root`` is
    ``dir`` finds it (the store's own files stay under a ``base_root``). Returns
    ``(rows + [that chunk row], expected)``, where ``expected`` is the array as the
    store now reads: the original with that chunk's block set to ``value``. ``rows``
    must come from ``plain_store_rows``, which knows the store.
    """
    import numpy as np
    import zarr
    from zarr.core.buffer import default_buffer_prototype
    from zarr.storage import MemoryStore

    from databricks.labs.gbx.zarr._keys import shard_of
    from databricks.labs.gbx.zarr._meta import inner_meta_doc

    chunk = tuple(int(c) for c in chunk)
    held = {
        "zarr.json": default_buffer_prototype().buffer.from_bytes(
            json.dumps(inner_meta_doc(meta)).encode()
        )
    }
    # a chunk equal to the fill value is skipped unless empty chunks are written
    with zarr.config.set({"array.write_empty_chunks": True}):
        arr = zarr.open_array(store=MemoryStore(store_dict=held), mode="r+")
        arr[...] = np.full(meta.chunks, value, dtype=meta.dtype)
    data = held[meta.key_encoding.chunk_key((0,) * len(meta.chunks))].to_bytes()

    name = "inner_" + "_".join(str(c) for c in chunk) + ".bin"
    (Path(dir) / name).write_bytes(data)

    new = PlainRows(
        [
            *rows,
            dict(
                kind="chunk",
                array=meta.name,
                key=meta.key_encoding.chunk_key(chunk),
                shard_key=meta.key_encoding.chunk_key(shard_of(chunk, meta.shard.cps)),
                file=name,
                offset=0,
                length=len(data),
                checksum=None,
                origin="gbx",
                identity=None,
                write_id="1" * 32,
                tag=None,
                meta=None,
            ),
        ]
    )
    new.root = rows.root

    expected = zarr.open_array(str(rows.root / meta.name), mode="r")[...]
    block = tuple(slice(c * n, (c + 1) * n) for c, n in zip(chunk, meta.chunks))
    expected[block] = value
    return new, expected
