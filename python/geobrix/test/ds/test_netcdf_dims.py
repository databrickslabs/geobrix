"""TDD tests for netcdf_gbx dimIndex and fanout options.

Fixture: (time=3, level=2, lat=3, lon=4) variable 'temp'.
Slice (t, l) has all values == t * 10 + l, so exact slice identity
is detectable without ambiguity.

Also covers: default backward-compat, overlap ValueError, var-without-fanout-dim,
unknown/typo'd dim validation, duplicate dimIndex key warning, coord values in metadata.
"""

from __future__ import annotations

import logging
import re
from typing import List, Tuple

import numpy as np
import pytest
from netCDF4 import Dataset
from rasterio.io import MemoryFile

from databricks.labs.gbx.ds import _netcdf
from databricks.labs.gbx.ds.netcdf import NetcdfRasterReader
from databricks.labs.gbx.ds.raster import _FilePartition

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _write_4d_grid(path: str, ntime: int = 3, nlevel: int = 2) -> None:
    """Write a (time, level, lat, lon) NetCDF with known per-slice values.

    Value at (t, l, :, :) == float(t * 10 + l).
    Coordinates: time=[0.0,1.0,2.0], level=[1000.0,500.0].
    """
    with Dataset(path, "w") as ds:
        ds.createDimension("time", ntime)
        ds.createDimension("level", nlevel)
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)

        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]

        t_var = ds.createVariable("time", "f8", ("time",))
        t_var[:] = [float(i) for i in range(ntime)]

        l_var = ds.createVariable("level", "f8", ("level",))
        l_var[:] = [1000.0, 500.0][:nlevel]

        v = ds.createVariable(
            "temp", "f4", ("time", "level", "lat", "lon"), fill_value=-9999.0
        )
        for t in range(ntime):
            for lev in range(nlevel):
                v[t, lev, :, :] = float(t * 10 + lev)


def _write_mixed_grid(path: str) -> None:
    """Write a file with two variables in the same grid extent.

    - 'temp' (time=3, lat=3, lon=4): value at time t == float(t).
    - 'sst'  (lat=3, lon=4): no leading dims.
    Coordinates: time=[0.0, 1.0, 2.0].
    Used to test per-variable fanout: 'temp' expands, 'sst' stays single-row.
    """
    with Dataset(path, "w") as ds:
        ds.createDimension("time", 3)
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)

        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]

        t_var = ds.createVariable("time", "f8", ("time",))
        t_var[:] = [0.0, 1.0, 2.0]

        temp = ds.createVariable(
            "temp", "f4", ("time", "lat", "lon"), fill_value=-9999.0
        )
        for t in range(3):
            temp[t, :, :] = float(t)

        sst = ds.createVariable("sst", "f4", ("lat", "lon"), fill_value=-9999.0)
        sst[:] = np.arange(12, dtype="float32").reshape(3, 4)


def _read_reader(path: str, options: dict) -> List[Tuple]:
    """Run NetcdfRasterReader.read() directly and collect all yielded tuples."""
    opts = {"path": path, **options}
    reader = NetcdfRasterReader(opts)
    partition = _FilePartition(path, -1)
    return list(reader.read(partition))


def _tile_mean(row: Tuple) -> float:
    """Decode the (source, tile) row and return the mean pixel value."""
    _, tile_tuple = row
    _, raster_bytes, _ = tile_tuple
    with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as ds:
        return float(ds.read(1).mean())


# ---------------------------------------------------------------------------
# The option table: (file, reader options) -> the rows, bands, sources, metadata and warnings
# ---------------------------------------------------------------------------


def _read(path: str, options: dict, recwarn, caplog):
    """The rows a read yields, as (source, per-band means, metadata), and every warning it raised."""
    caplog.set_level(logging.WARNING)
    out = []
    for source, (_, raster_bytes, meta) in _read_reader(path, options):
        with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as rds:
            out.append(
                (
                    source,
                    tuple(float(rds.read(b).mean()) for b in range(1, rds.count + 1)),
                    meta,
                )
            )
    warnings_raised = [str(w.message) for w in recwarn.list] + [
        r.getMessage() for r in caplog.records
    ]
    return (
        out,
        warnings_raised,
        sum(issubclass(w.category, UserWarning) for w in recwarn.list),
    )


def _grid(tmp_path, kind):
    p = str(tmp_path / f"{kind}.nc")
    (_write_4d_grid if kind == "g" else _write_mixed_grid)(p)
    return p


def _suffix(source: str) -> str:
    return source.split(":")[-1]


# name -> (file, options, expected). Expected keys (all optional):
#   bands   the per-band means of each row, sorted      suffixes  the source suffix of each row, sorted
#   meta    {key: value} every row carries              no_meta   keys no row carries
#   warns   substrings a warning must contain           silent    True: no UserWarning at all
DIMS = {
    # default: one row per variable, index 0, byte-compatible source, no slice metadata
    "default": ("g", {}, {"bands": [(0.0,)], "suffixes": ["temp"], "no_meta": ["sliceDims", "sliceCoord_time"], "warns": ["time", "leading dimension"]}),
    "dimindex_one_slice": (
        "g", {"dimIndex": "time=2,level=1"},
        {"bands": [(21.0,)], "suffixes": ["temp[level=1,time=2]"], "meta": {"sliceDims": "level=1,time=2", "sliceCoord_time": 2.0, "sliceCoord_level": 500.0}},
    ),
    "dimindex_zero_is_intentional": ("g", {"dimIndex": "time=0,level=0"}, {"bands": [(0.0,)], "no_warn": "leading dimension"}),
    "dimindex_last_duplicate_wins": ("g", {"dimIndex": "time=1,time=2"}, {"bands": [(20.0,)], "warns": ["duplicate"]}),
    "dimindex_typo": ("g", {"dimIndex": "tyme=2"}, {"bands": [(0.0,)], "suffixes": ["temp"], "warns": ["tyme", "time", "level"]}),
    "fanout_both_dims": (
        "g", {"fanout": "time,level"},
        {"bands": sorted((float(t * 10 + lev),) for t in range(3) for lev in range(2)),
         "suffixes": sorted(f"temp[level={lev},time={t}]" for t in range(3) for lev in range(2)),
         "meta_keys": ["sliceDims", "sliceCoord_time", "sliceCoord_level"]},
    ),
    "fanout_time_pins_level_with_a_warning": (
        "g", {"fanout": "time"}, {"bands": [(0.0,), (10.0,), (20.0,)], "warns": ["level", "leading dimension"]},
    ),
    "fanout_typo": ("g", {"fanout": "tyme"}, {"bands": [(0.0,)], "warns": ["tyme"]}),
    "fanout_leaves_a_variable_without_the_dim_alone": (
        "m", {"fanout": "time"}, {"n": 4, "suffixes": ["sst", "temp[time=0]", "temp[time=1]", "temp[time=2]"]},
    ),
    "fanout_of_a_flat_variable_is_moot": ("m", {"fanout": "time", "variable": "sst"}, {"n": 1, "silent": True}),
    "banddim_stacks_a_dim_as_bands": (
        "g", {"bandDim": "time"},
        {"bands": [(0.0, 10.0, 20.0)], "meta": {"bandDim": "time", "bandCoords": "0.0,1.0,2.0"},
         "meta_contains": {"bandDescriptions": ["time=0.0", "time=2.0"]}, "source_contains": "bandDim=time"},
    ),
    "banddim_with_dimindex": ("g", {"bandDim": "time", "dimIndex": "level=1"}, {"bands": [(1.0, 11.0, 21.0)]}),
    "banddim_with_fanout": (
        "g", {"bandDim": "time", "fanout": "level"}, {"bands": [(0.0, 10.0, 20.0), (1.0, 11.0, 21.0)]},
    ),
    "banddim_typo": ("g", {"bandDim": "tyme"}, {"bands": [(0.0,)], "warns": ["tyme", "time", "level"]}),
    "banddim_of_a_flat_variable_is_moot": ("m", {"bandDim": "time", "variable": "sst"}, {"n": 1, "bands_count": 1, "silent": True}),
    "banddim_typo_on_a_flat_variable_is_moot": ("m", {"bandDim": "tyme", "variable": "sst"}, {"n": 1, "silent": True}),
}  # fmt: skip


@pytest.mark.parametrize("kind,options,want", DIMS.values(), ids=DIMS.keys())
def test_dim_options_shape_the_rows(tmp_path, recwarn, caplog, kind, options, want):
    p = _grid(tmp_path, kind)
    rows, warned, n_user = _read(p, options, recwarn, caplog)
    if "n" in want:
        assert len(rows) == want["n"]
    if "bands" in want:
        assert sorted(b for _, b, _ in rows) == pytest.approx(want["bands"], abs=1e-4)
    if "bands_count" in want:
        assert [len(b) for _, b, _ in rows] == [want["bands_count"]] * len(rows)
    if "suffixes" in want:
        assert sorted(_suffix(s) for s, _, _ in rows) == want["suffixes"]
        assert all(s.startswith(f'NETCDF:"{p}":') for s, _, _ in rows)
    if "source_contains" in want:
        assert all(want["source_contains"] in s for s, _, _ in rows)
    for _, _, meta in rows:
        for k, v in want.get("meta", {}).items():
            assert (
                (float(meta[k]) == pytest.approx(v))
                if isinstance(v, float)
                else meta[k] == v
            ), k
        for k, parts in want.get("meta_contains", {}).items():
            assert all(part in meta[k] for part in parts), k
        assert set(want.get("meta_keys", [])) <= set(meta)
        assert not set(want.get("no_meta", [])) & set(meta)
    for part in want.get("warns", []):
        assert any(part in m for m in warned), (part, warned)
    if want.get("silent"):
        assert n_user == 0, warned
    if "no_warn" in want:
        assert not any(want["no_warn"] in m for m in warned), warned


def test_fanout_coordinates_pair_each_slice_with_its_coordinate_values(
    tmp_path, recwarn, caplog
):
    rows, _, _ = _read(_grid(tmp_path, "g"), {"fanout": "time,level"}, recwarn, caplog)
    pairs = {
        (float(m["sliceCoord_time"]), float(m["sliceCoord_level"])) for _, _, m in rows
    }
    assert pairs == {
        (float(t), [1000.0, 500.0][lev]) for t in range(3) for lev in range(2)
    }


# a dimIndex outside the dim's size is refused with the sentence that says which
@pytest.mark.parametrize(
    "options,sentence",
    [
        (
            {"dimIndex": "time=99"},
            "netcdf_gbx: dimIndex time=99 is out of range (size=3) for variable 'temp'.",
        ),
        (
            {"dimIndex": "level=-1"},
            "netcdf_gbx: dimIndex level=-1 is out of range (size=2) for variable 'temp'.",
        ),
        (
            {"dimIndex": "level=2"},
            "netcdf_gbx: dimIndex level=2 is out of range (size=2) for variable 'temp'.",
        ),
    ],
    ids=["past_the_end", "negative", "one_past_the_last"],
)
def test_a_dim_index_outside_the_dim_is_refused(tmp_path, options, sentence):
    with pytest.raises(ValueError, match=re.escape(sentence)):
        _read_reader(_grid(tmp_path, "g"), options)


# options naming one dim twice are refused (dimIndex+fanout, bandDim+dimIndex, bandDim+fanout)
@pytest.mark.parametrize(
    "options,match",
    [
        (
            {"dimIndex": "time=1", "fanout": "time"},
            "dimIndex.*fanout|fanout.*dimIndex|both",
        ),
        ({"bandDim": "time", "dimIndex": "time=1"}, "bandDim|stacked|pinned"),
        ({"bandDim": "time", "fanout": "time"}, "bandDim|stacked|expanded"),
    ],
    ids=["dimindex_fanout", "banddim_dimindex", "banddim_fanout"],
)
def test_a_dim_named_by_two_options_is_refused(tmp_path, options, match):
    with pytest.raises(ValueError, match=match):
        _read_reader(_grid(tmp_path, "g"), options)


def test_dimindex_and_banddim_are_honored_through_spark_read(spark, tmp_path):
    """Spark hands a reader its options case-insensitively (as lowercase keys); the
    camel-case names dimIndex and bandDim were then never found and silently ignored."""
    from databricks.labs.gbx import manager as gbx_manager

    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    gbx_manager.register_sources(spark, only=["netcdf"])
    rows = (
        spark.read.format("netcdf_gbx")
        .option("bandDim", "time")
        .option("dimIndex", "level=1")
        .load(p)
        .collect()
    )
    assert len(rows) == 1
    with MemoryFile(bytes(rows[0]["tile"]["raster"])) as mf, mf.open() as rds:
        assert rds.count == 3  # bandDim: time stacked as bands
        # dimIndex: level 1, so band k = t*10+1 (level 0 would read 0, 10, 20)
        got = [float(rds.read(b).mean()) for b in (1, 2, 3)]
    np.testing.assert_allclose(got, [1.0, 11.0, 21.0], atol=1e-4)


def test_banddim_soft_warn_large_stack(tmp_path, caplog, monkeypatch):
    """Soft-warn fires when estimated decoded bytes exceed _BANDDIM_WARN_BYTES."""
    import databricks.labs.gbx.ds.netcdf as netcdf_module

    monkeypatch.setattr(netcdf_module, "_BANDDIM_WARN_BYTES", 1)
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with caplog.at_level(logging.WARNING):
        with pytest.warns(UserWarning, match="estimated|fanout|MiB"):
            _read_reader(p, {"bandDim": "time"})


# ---------------------------------------------------------------------------
# Slice reads decode only the slice (a bad chunk fails only its slice)
# ---------------------------------------------------------------------------

_N = 20  # lat / lon size of the per-slice-chunk fixtures


def _slice_values(t: int) -> np.ndarray:
    return np.arange(_N * _N, dtype="float32").reshape(_N, _N) + 1000.0 * t


def _write_chunked_slices(path: str, ntime: int = 3) -> None:
    """(time, lat, lon) 'temp' stored as one zlib chunk per time slice; slice t
    holds _slice_values(t). Latitude descends, so array_2d returns it as stored."""
    with Dataset(path, "w", format="NETCDF4") as ds:
        ds.createDimension("time", ntime)
        ds.createDimension("lat", _N)
        ds.createDimension("lon", _N)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = np.linspace(50.0, 49.0, _N)
        lon[:] = np.linspace(10.0, 11.0, _N)
        ds.createVariable("time", "f8", ("time",))[:] = np.arange(ntime, dtype="f8")
        v = ds.createVariable(
            "temp",
            "f4",
            ("time", "lat", "lon"),
            zlib=True,
            shuffle=False,
            chunksizes=(1, _N, _N),
        )
        for t in range(ntime):
            v[t, :, :] = _slice_values(t)


def _garble_chunk(path: str, t: int) -> None:
    """XOR the middle of the zlib stream holding slice t (found by inflating every
    candidate stream and matching the slice's stored bytes)."""
    import zlib

    want = _slice_values(t).astype("<f4").tobytes()
    data = bytearray(open(path, "rb").read())
    window = 2 * len(want) + 64
    for i in range(len(data) - 1):
        if data[i] != 0x78:
            continue
        d = zlib.decompressobj()
        try:
            out = d.decompress(bytes(data[i : i + window]))
        except zlib.error:
            continue
        if d.eof and out == want:
            used = min(window, len(data) - i) - len(d.unused_data)
            for j in range(i + used // 2, i + used // 2 + 8):
                data[j] ^= 0x5A
            open(path, "wb").write(bytes(data))
            return
    raise AssertionError(f"no zlib chunk for slice {t} in {path}")


def test_array_2d_corrupt_chunk_fails_only_its_slice(tmp_path):
    p = str(tmp_path / "bad.nc")
    _write_chunked_slices(p)
    _garble_chunk(p, 1)
    with _netcdf.open_dataset(p, None) as ds:
        for t in (0, 2):
            arr = _netcdf.array_2d(ds, "temp", sel={"time": t})
            np.testing.assert_array_equal(arr, _slice_values(t))
        with pytest.raises(RuntimeError, match="HDF"):
            _netcdf.array_2d(ds, "temp", sel={"time": 1})


def test_reader_reads_the_good_slices_of_a_variable_with_a_bad_chunk(tmp_path):
    p = str(tmp_path / "bad.nc")
    _write_chunked_slices(p)
    _garble_chunk(p, 1)
    (row,) = _read_reader(p, {"dimIndex": "time=2"})
    assert _tile_mean(row) == pytest.approx(float(_slice_values(2).mean()))
    # fanout: slice 0 is emitted, then the bad slice fails the read.
    reader = NetcdfRasterReader({"path": p, "fanout": "time"})
    rows = []
    with pytest.raises(RuntimeError, match="HDF"):
        for r in reader.read(_FilePartition(p, -1)):
            rows.append(r)
    assert len(rows) == 1 and _tile_mean(rows[0]) == pytest.approx(
        float(_slice_values(0).mean())
    )


def test_fanout_reads_one_slice_per_row(tmp_path, monkeypatch):
    """A fanout read decodes one slice per row, never the whole variable: each of
    the 3 rows reads one 20 x 20 slice of 'temp' from the file (the first row used
    to read, decode and hold all 3 x 20 x 20 values, then slice them)."""
    from xarray.backends import netCDF4_

    read = []
    real = netCDF4_.NetCDF4ArrayWrapper._getitem

    def spy(self, key):
        out = real(self, key)
        if self.variable_name == "temp":
            read.append(int(np.asarray(out).size))
        return out

    monkeypatch.setattr(netCDF4_.NetCDF4ArrayWrapper, "_getitem", spy)
    p = str(tmp_path / "good.nc")
    _write_chunked_slices(p)
    rows = _read_reader(p, {"fanout": "time"})
    assert [_tile_mean(r) for r in rows] == pytest.approx(
        [float(_slice_values(t).mean()) for t in range(3)]
    )
    assert read == [_N * _N] * 3


def test_array_2d_undecodable_variable_reads_raw_per_slice(tmp_path):
    """The raw fallback (an integer variable whose CF mask decode fails, like NOAA
    coral bleaching_alert_area: uint8, _FillValue 251, integer scale_factor) still
    returns each slice's raw values with the fill kept."""
    p = str(tmp_path / "baa.nc")
    raw = np.array([[[0, 1], [251, 4]], [[2, 251], [3, 0]]], dtype="u1")
    with Dataset(p, "w", format="NETCDF4") as ds:
        ds.createDimension("time", 2)
        ds.createDimension("lat", 2)
        ds.createDimension("lon", 2)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [1.0, 0.0]
        lon[:] = [0.0, 1.0]
        v = ds.createVariable("baa", "u1", ("time", "lat", "lon"), fill_value=251)
        v.set_auto_maskandscale(False)
        v.scale_factor = np.int64(1)
        v.valid_min = np.uint8(0)
        v.valid_max = np.uint8(4)
        v[:] = raw
    with _netcdf.open_dataset(p, None) as ds:
        with pytest.raises(ValueError):
            ds["baa"].values  # noqa: B018 - the decode this fallback exists for
        for t in (0, 1):
            arr = _netcdf.array_2d(ds, "baa", sel={"time": t})
            assert arr.dtype == np.uint8
            np.testing.assert_array_equal(arr, raw[t])


def _write_undecodable_variable(grp, raw) -> None:
    """``baa`` in ``grp`` (the file or one of its groups) whose CF mask decode fails
    (uint8, _FillValue 251, integer scale_factor), on a 2 x 2 north-up grid."""
    grp.createDimension("time", 2)
    grp.createDimension("lat", 2)
    grp.createDimension("lon", 2)
    lat = grp.createVariable("lat", "f8", ("lat",))
    lon = grp.createVariable("lon", "f8", ("lon",))
    lat.standard_name = "latitude"
    lon.standard_name = "longitude"
    lat[:] = [1.0, 0.0]
    lon[:] = [0.0, 1.0]
    v = grp.createVariable("baa", "u1", ("time", "lat", "lon"), fill_value=251)
    v.set_auto_maskandscale(False)
    v.scale_factor = np.int64(1)
    v.valid_min = np.uint8(0)
    v.valid_max = np.uint8(4)
    v[:] = raw


def test_array_2d_raw_fallback_reads_the_requested_group(tmp_path):
    """The raw fallback reopens the file at the same group: a variable of a group
    is read from there, not from a variable of the same name at the file's root."""
    p = str(tmp_path / "grouped.nc")
    in_group = np.array([[[0, 1], [251, 4]], [[2, 251], [3, 0]]], dtype="u1")
    at_root = np.full((2, 2, 2), 9, dtype="u1")
    with Dataset(p, "w", format="NETCDF4") as ds:
        _write_undecodable_variable(ds, at_root)
        _write_undecodable_variable(ds.createGroup("g"), in_group)
    with _netcdf.open_dataset(p, "g") as ds:
        with pytest.raises(ValueError):
            ds["baa"].values  # noqa: B018 - the decode this fallback exists for
        for t in (0, 1):
            arr = _netcdf.array_2d(ds, "baa", sel={"time": t})
            np.testing.assert_array_equal(arr, in_group[t])


def test_array_2d_raw_fallback_of_a_group_variable_absent_from_the_root(tmp_path):
    """A group-only variable still reads raw (the root has no such variable, so
    reopening there fails with a KeyError)."""
    p = str(tmp_path / "grouped_only.nc")
    in_group = np.array([[[0, 1], [251, 4]], [[2, 251], [3, 0]]], dtype="u1")
    with Dataset(p, "w", format="NETCDF4") as ds:
        _write_undecodable_variable(ds.createGroup("g"), in_group)
    with _netcdf.open_dataset(p, "g") as ds:
        arr = _netcdf.array_2d(ds, "baa", sel={"time": 1})
        np.testing.assert_array_equal(arr, in_group[1])


def test_reader_group_option_reaches_the_raw_fallback(tmp_path):
    """netcdf_gbx with ``group`` reads an undecodable group variable raw (fill
    kept), not the same-named variable at the file's root."""
    p = str(tmp_path / "grouped_reader.nc")
    in_group = np.array([[[0, 1], [251, 4]], [[2, 251], [3, 0]]], dtype="u1")
    at_root = np.full((2, 2, 2), 9, dtype="u1")
    with Dataset(p, "w", format="NETCDF4") as ds:
        _write_undecodable_variable(ds, at_root)
        _write_undecodable_variable(ds.createGroup("g"), in_group)
    rows = _read_reader(p, {"group": "g", "variables": "baa", "fanout": "time"})
    assert [_tile_mean(r) for r in rows] == pytest.approx(
        [float(in_group[t].mean()) for t in (0, 1)]
    )


def _write_uint8_steps(path: str, steps: int = 4) -> None:
    """A (time, lat, lon) ``uint8`` variable 'u8': step k holds 7 + k, except the last step,
    which is 0 in the left half of the pixels."""
    with Dataset(path, "w") as ds:
        ds.createDimension("time", steps)
        ds.createDimension("lat", 3)
        ds.createDimension("lon", 4)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name = "latitude"
        lon.standard_name = "longitude"
        lat[:] = [50.0, 49.5, 49.0]
        lon[:] = [10.0, 10.5, 11.0, 11.5]
        t_var = ds.createVariable("time", "f8", ("time",))
        t_var[:] = [float(i) for i in range(steps)]
        v = ds.createVariable("u8", "u1", ("time", "lat", "lon"), fill_value=False)
        for k in range(steps):
            v[k, :, :] = 7 + k
        v[steps - 1, :, :2] = 0


@pytest.mark.parametrize("steps", [3, 4, 5])
def test_banddim_stack_of_uint8_steps_has_no_alpha_band(tmp_path, steps):
    """The steps of a variable are plain bands: GDAL labels band 4 of a 4-band ``uint8`` GTiff
    alpha when it creates one, which made the zeros of the 4th step transparent."""
    from ..pyrx import _alpha_layouts as al

    p = str(tmp_path / "u8.nc")
    _write_uint8_steps(p, steps)
    rows = _read_reader(p, {"bandDim": "time"})
    assert len(rows) == 1
    _, (_, raster_bytes, _) = rows[0]
    raster_bytes = bytes(raster_bytes)
    with MemoryFile(raster_bytes) as mf, mf.open() as rds:
        assert rds.count == steps
        valid = rds.read_masks(1) != 0
        zeros = (rds.read(steps) == 0).sum()
    assert zeros, "fixture: the last step must hold zeros"
    assert al.labelled_alpha(raster_bytes) == []
    assert valid.all(), "no pixel of a step stack is transparent"


# ---------------------------------------------------------------------------
# fill_kept: the cells xarray decoded from the fill value go back to it
# ---------------------------------------------------------------------------


def test_fill_kept_sets_decoded_gaps_back_to_the_fill_value_and_leaves_the_rest():
    a = np.array([[1.0, np.nan], [np.nan, 4.0]], dtype="float32")
    out = _netcdf.fill_kept(a, -9999.0)
    assert out.dtype == np.float32
    assert out.tolist() == [[1.0, -9999.0], [-9999.0, 4.0]]
    # unchanged: no fill value, an integer array, a NaN fill value, no gaps (the same array comes back)
    assert _netcdf.fill_kept(a, None) is a
    ints = np.array([[1, 2]], dtype="int16")
    assert _netcdf.fill_kept(ints, -9999.0) is ints
    assert _netcdf.fill_kept(a, float("nan")) is a
    full = np.ones((2, 2), "float64")
    assert _netcdf.fill_kept(full, -1.0) is full


def test_fill_kept_covers_every_band_of_a_stack_and_the_banddim_read_keeps_the_fill(
    tmp_path,
):
    """On a 3-D (band, y, x) array each band's gaps are set back; the bandDim reader, which applies it slice by slice,
    writes a multi-band tile whose gap pixels equal the NoData in every band."""
    stack = np.full((3, 2, 2), 5.0, "float32")
    stack[0, 0, 0] = stack[1, 1, 1] = stack[2, 0, 1] = np.nan
    out = _netcdf.fill_kept(stack, -1.0)
    assert (out == -1.0).sum() == 3 and not np.isnan(out).any()
    assert out[0, 0, 0] == out[1, 1, 1] == out[2, 0, 1] == -1.0

    p = str(tmp_path / "f.nc")
    with Dataset(p, "w") as ds:
        ds.createDimension("time", 3)
        ds.createDimension("lat", 2)
        ds.createDimension("lon", 2)
        lat = ds.createVariable("lat", "f8", ("lat",))
        lon = ds.createVariable("lon", "f8", ("lon",))
        lat.standard_name, lon.standard_name = "latitude", "longitude"
        lat[:] = [50.0, 49.0]
        lon[:] = [10.0, 11.0]
        t = ds.createVariable("time", "f8", ("time",))
        t[:] = [0.0, 1.0, 2.0]
        v = ds.createVariable("v", "f4", ("time", "lat", "lon"), fill_value=-9999.0)
        v[:] = 5.0
        v[0, 0, 0] = v[1, 1, 1] = v[2, 0, 1] = -9999.0
    rows = _read_reader(p, {"bandDim": "time"})
    assert len(rows) == 1
    _, (_, raster_bytes, _) = rows[0]
    with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as rds:
        assert rds.count == 3 and rds.nodata == -9999.0
        data = rds.read()
    assert not np.isnan(data).any()
    assert data[0, 0, 0] == data[1, 1, 1] == data[2, 0, 1] == -9999.0
    assert (data == 5.0).sum() == 9
