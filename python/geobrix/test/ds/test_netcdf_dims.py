"""TDD tests for netcdf_gbx dimIndex and fanout options.

Fixture: (time=3, level=2, lat=3, lon=4) variable 'temp'.
Slice (t, l) has all values == t * 10 + l, so exact slice identity
is detectable without ambiguity.

Also covers: default backward-compat, overlap ValueError, var-without-fanout-dim,
unknown/typo'd dim validation, duplicate dimIndex key warning, coord values in metadata.
"""

from __future__ import annotations

import logging
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
# Helper tests: leading_dims
# ---------------------------------------------------------------------------


def test_leading_dims_returns_non_spatial(tmp_path):
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        ld = _netcdf.leading_dims(ds, "temp")
    assert ld == [("time", 3), ("level", 2)]


def test_leading_dims_empty_for_2d_var(tmp_path):
    p = str(tmp_path / "m.nc")
    _write_mixed_grid(p)  # 'sst' is a pure-2-D var in the mixed file
    with _netcdf.open_dataset(p, None) as ds:
        ld = _netcdf.leading_dims(ds, "sst")
    assert ld == []


# ---------------------------------------------------------------------------
# Helper tests: array_2d with sel
# ---------------------------------------------------------------------------


def test_array_2d_sel_reads_named_slice(tmp_path):
    """array_2d(sel={"time": 2, "level": 1}) reads slice (t=2, l=1) → all 21."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        arr = _netcdf.array_2d(ds, "temp", sel={"time": 2, "level": 1})
    np.testing.assert_allclose(arr, np.full((3, 4), 21.0, dtype="float32"), rtol=1e-6)


def test_array_2d_sel_index_zero_no_warn(tmp_path, caplog):
    """Explicitly selecting index 0 via sel must NOT warn (it's intentional)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.ds._netcdf"):
            _netcdf.array_2d(ds, "temp", sel={"time": 0, "level": 0})
    warn_msgs = [
        r.getMessage() for r in caplog.records if "leading dimension" in r.getMessage()
    ]
    assert warn_msgs == [], f"unexpected warn for explicit sel=0: {warn_msgs}"


def test_array_2d_sel_out_of_range_raises(tmp_path):
    """Out-of-range index → ValueError."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with _netcdf.open_dataset(p, None) as ds:
        with pytest.raises(ValueError, match="out of range"):
            _netcdf.array_2d(ds, "temp", sel={"time": 99})


# ---------------------------------------------------------------------------
# (a) dimIndex option
# ---------------------------------------------------------------------------


def test_dimindex_reads_correct_slice(tmp_path):
    """dimIndex='time=2,level=1' reads the (t=2, l=1) slice → mean=21.0."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"dimIndex": "time=2,level=1"})
    assert len(rows) == 1
    assert pytest.approx(_tile_mean(rows[0]), abs=1e-4) == 21.0


def test_dimindex_source_contains_suffix(tmp_path):
    """dimIndex source must be NETCDF:'path':var[level=1,time=2] (sorted suffix)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"dimIndex": "time=2,level=1"})
    source, _ = rows[0]
    assert source.endswith(":temp[level=1,time=2]"), f"unexpected source: {source}"


def test_dimindex_slice_in_metadata(tmp_path):
    """dimIndex: tile metadata must contain sliceDims key."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"dimIndex": "time=2,level=1"})
    _, (_, _, meta) = rows[0]
    assert "sliceDims" in meta
    assert "level=1" in meta["sliceDims"]
    assert "time=2" in meta["sliceDims"]


# ---------------------------------------------------------------------------
# (b) fanout="time,level" — Cartesian product
# ---------------------------------------------------------------------------


def test_fanout_both_dims_emits_cartesian(tmp_path):
    """fanout='time,level' → 3×2 = 6 rows, each the correct slice."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time,level"})
    assert len(rows) == 6


def test_fanout_both_dims_correct_values(tmp_path):
    """Each fanout row carries the correct mean value."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time,level"})
    means = sorted(_tile_mean(r) for r in rows)
    expected = sorted(float(t * 10 + lev) for t in range(3) for lev in range(2))
    np.testing.assert_allclose(means, expected, atol=1e-4)


def test_fanout_both_dims_unique_sources(tmp_path):
    """All 6 fanout rows must have unique source strings."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time,level"})
    sources = [r[0] for r in rows]
    assert len(set(sources)) == 6, f"non-unique sources: {sources}"


def test_fanout_both_dims_sources_have_suffix(tmp_path):
    """Every fanout source must contain the bracketed slice suffix."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time,level"})
    for source, _ in rows:
        assert "[" in source and "]" in source, f"missing suffix in source: {source}"


def test_fanout_slice_info_in_metadata(tmp_path):
    """Each fanout row has sliceDims in its tile metadata."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time,level"})
    for _, (_, _, meta) in rows:
        assert "sliceDims" in meta


# ---------------------------------------------------------------------------
# (c) fanout="time" only — level pinned to 0 with warn
# ---------------------------------------------------------------------------


def test_fanout_time_only_emits_3_rows(tmp_path):
    """fanout='time' with no level dimIndex → 3 rows (level pinned at 0)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time"})
    assert len(rows) == 3


def test_fanout_time_only_values_correct(tmp_path):
    """With fanout='time', each row is the t=0/1/2 slice at level=0 → 0,10,20."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time"})
    means = sorted(_tile_mean(r) for r in rows)
    np.testing.assert_allclose(means, [0.0, 10.0, 20.0], atol=1e-4)


def test_fanout_time_only_warns_on_level(tmp_path, caplog):
    """fanout='time' must warn that 'level' is pinned to 0 (size>1, not selected)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.ds._netcdf"):
        _read_reader(p, {"fanout": "time"})
    warn_msgs = [
        r.getMessage() for r in caplog.records if "leading dimension" in r.getMessage()
    ]
    assert any(
        "level" in m for m in warn_msgs
    ), f"expected a warning for unpinned 'level' dim; got: {warn_msgs}"


# ---------------------------------------------------------------------------
# (d) Default behavior (no options) — backward compat
# ---------------------------------------------------------------------------


def test_default_one_row_per_variable(tmp_path):
    """No options → one row per variable (unchanged)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {})
    assert len(rows) == 1


def test_default_source_format_unchanged(tmp_path):
    """No options → source is NETCDF:'path':var with no suffix (byte-compat)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {})
    source, _ = rows[0]
    assert source == f'NETCDF:"{p}":temp'


def test_default_warns_on_multi_time(tmp_path, caplog):
    """No options on a time>1 variable → still warns (unchanged)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.ds._netcdf"):
        _read_reader(p, {})
    msgs = [r.getMessage() for r in caplog.records]
    assert any("time" in m and "leading dimension" in m for m in msgs)


def test_default_reads_index_zero_slice(tmp_path):
    """No options → reads slice (t=0, l=0) → mean=0."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {})
    assert pytest.approx(_tile_mean(rows[0]), abs=1e-4) == 0.0


def test_default_no_slice_dims_in_metadata(tmp_path):
    """No options → sliceDims must NOT appear in tile metadata (strict compat)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {})
    _, (_, _, meta) = rows[0]
    assert "sliceDims" not in meta


# ---------------------------------------------------------------------------
# (e) Overlap guard
# ---------------------------------------------------------------------------


def test_overlap_dimindex_and_fanout_raises(tmp_path):
    """A dim in both dimIndex and fanout must raise ValueError."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with pytest.raises(ValueError, match="dimIndex.*fanout|fanout.*dimIndex|both"):
        _read_reader(p, {"dimIndex": "time=1", "fanout": "time"})


# ---------------------------------------------------------------------------
# (f) Variable lacking the fanout dim → single row, no expansion
#     (uses a mixed file so the dim IS known to at least one var)
# ---------------------------------------------------------------------------


def test_fanout_var_lacking_dim_emits_one_row(tmp_path):
    """In a mixed file, the var without the fanout dim emits exactly one row."""
    p = str(tmp_path / "m.nc")
    _write_mixed_grid(p)
    rows = _read_reader(p, {"fanout": "time"})
    # temp → 3 rows (time=0,1,2); sst → 1 row (no time dim)
    sst_rows = [r for r in rows if ":sst" in r[0]]
    assert len(sst_rows) == 1


def test_fanout_var_lacking_dim_has_no_suffix(tmp_path):
    """In a mixed file, the var without the fanout dim has a bare source."""
    p = str(tmp_path / "m.nc")
    _write_mixed_grid(p)
    rows = _read_reader(p, {"fanout": "time"})
    sst_sources = [r[0] for r in rows if ":sst" in r[0]]
    assert len(sst_sources) == 1
    assert "[" not in sst_sources[0], f"unexpected suffix: {sst_sources[0]}"


def test_fanout_total_rows_mixed_file(tmp_path):
    """In a mixed file, fanout='time' → 3 (temp) + 1 (sst) = 4 total rows."""
    p = str(tmp_path / "m.nc")
    _write_mixed_grid(p)
    rows = _read_reader(p, {"fanout": "time"})
    assert len(rows) == 4


# ---------------------------------------------------------------------------
# (g) Unknown/typo'd dim → UserWarning + fall-through (not ValueError)
#     Pure-2-D variable → silent no-op
# ---------------------------------------------------------------------------


def test_dimindex_unknown_dim_warns(tmp_path):
    """A typo'd dim in dimIndex (unknown to a var with other leading dims) →
    UserWarning + index-0 fall-through (1 row, value=0)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with pytest.warns(UserWarning, match="tyme"):
        rows = _read_reader(p, {"dimIndex": "tyme=2"})
    # "tyme" unrecognised → sel={} → index 0 for all dims → value=0.0
    assert len(rows) == 1
    assert pytest.approx(_tile_mean(rows[0]), abs=1e-4) == 0.0


def test_dimindex_unknown_dim_source_bare(tmp_path):
    """When a dimIndex dim is unknown the source has no suffix (nothing applied)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with pytest.warns(UserWarning):
        rows = _read_reader(p, {"dimIndex": "tyme=2"})
    source, _ = rows[0]
    assert source == f'NETCDF:"{p}":temp', f"unexpected source: {source}"


def test_fanout_unknown_dim_warns(tmp_path):
    """A typo'd dim in fanout (unknown to a var with other leading dims) →
    UserWarning + single default row (no expansion)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with pytest.warns(UserWarning, match="tyme"):
        rows = _read_reader(p, {"fanout": "tyme"})
    # no expansion → 1 row at default index 0 for all dims → value=0.0
    assert len(rows) == 1
    assert pytest.approx(_tile_mean(rows[0]), abs=1e-4) == 0.0


def test_dimindex_unknown_warning_names_actual_dims(tmp_path):
    """The UserWarning for a typo'd dim names the variable's actual leading dims."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with pytest.warns(UserWarning) as warning_list:
        _read_reader(p, {"dimIndex": "tyme=2"})
    msgs = [str(w.message) for w in warning_list]
    assert any(
        "time" in m and "level" in m for m in msgs
    ), f"warning should name actual leading dims; got: {msgs}"


def test_pure_2d_var_unknown_dim_silent(tmp_path):
    """A pure-2-D variable with a requested dim emits NO UserWarning (moot)."""
    import warnings as _warnings

    p = str(tmp_path / "m.nc")
    _write_mixed_grid(p)
    # Select only 'sst' (no leading dims) — "time" is legitimately moot.
    with _warnings.catch_warnings(record=True) as caught:
        _warnings.simplefilter("always")
        rows = _read_reader(p, {"fanout": "time", "variable": "sst"})
    user_warns = [w for w in caught if issubclass(w.category, UserWarning)]
    assert user_warns == [], f"unexpected UserWarning for pure-2-D var: {user_warns}"
    assert len(rows) == 1


# ---------------------------------------------------------------------------
# (h) Duplicate key in dimIndex → UserWarning, last value wins
# ---------------------------------------------------------------------------


def test_dimindex_duplicate_key_warns(tmp_path):
    """dimIndex with a repeated dim emits UserWarning; last value is used."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with pytest.warns(UserWarning, match="duplicate"):
        rows = _read_reader(p, {"dimIndex": "time=1,time=2"})
    # last value wins: time=2, level falls to 0 → value = 2*10+0 = 20.0
    assert pytest.approx(_tile_mean(rows[0]), abs=1e-4) == 20.0


# ---------------------------------------------------------------------------
# (i) Coordinate values in tile metadata
# ---------------------------------------------------------------------------


def test_dimindex_coord_values_in_metadata(tmp_path):
    """dimIndex: sliceCoord_time and sliceCoord_level carry the actual coord values."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"dimIndex": "time=2,level=1"})
    _, (_, _, meta) = rows[0]
    # Fixture: time=[0.0,1.0,2.0] → time[2]=2.0; level=[1000.0,500.0] → level[1]=500.0
    assert "sliceCoord_time" in meta, "sliceCoord_time missing from metadata"
    assert "sliceCoord_level" in meta, "sliceCoord_level missing from metadata"
    assert float(meta["sliceCoord_time"]) == pytest.approx(2.0)
    assert float(meta["sliceCoord_level"]) == pytest.approx(500.0)


def test_fanout_coord_values_in_metadata(tmp_path):
    """fanout: each row's sliceCoord_* carries the correct coordinate value."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"fanout": "time,level"})
    # Every row must carry both coord keys.
    for _, (_, _, meta) in rows:
        assert "sliceCoord_time" in meta
        assert "sliceCoord_level" in meta
    # Build the set of (time_coord, level_coord) pairs across all rows.
    coord_pairs = frozenset(
        (float(meta["sliceCoord_time"]), float(meta["sliceCoord_level"]))
        for _, (_, _, meta) in rows
    )
    # Fixture: time=[0,1,2], level=[1000.0(idx0),500.0(idx1)]
    expected = frozenset(
        (float(t), 1000.0 if lev == 0 else 500.0) for t in range(3) for lev in range(2)
    )
    assert coord_pairs == expected, f"coord pairs mismatch: got {coord_pairs}"


# ---------------------------------------------------------------------------
# (j) bandDim option — multi-band stacking
# ---------------------------------------------------------------------------


def _open_bands(raster_bytes: bytes):
    """Return an open rasterio DatasetReader for the tile bytes (context manager)."""
    return MemoryFile(raster_bytes).open()


def test_banddim_time_one_tile_three_bands(tmp_path, caplog):
    """bandDim='time' → 1 tile, 3 bands."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.ds._netcdf"):
        rows = _read_reader(p, {"bandDim": "time"})
    assert len(rows) == 1
    _, (_, raster_bytes, _) = rows[0]
    with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as rds:
        assert rds.count == 3


def test_banddim_time_band_values_correct(tmp_path, caplog):
    """bandDim='time': band k has mean = time*10 (level defaults to 0)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.gbx.ds._netcdf"):
        rows = _read_reader(p, {"bandDim": "time"})
    _, (_, raster_bytes, _) = rows[0]
    with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as rds:
        # band 1 = time=0,level=0 → 0; band 2 = time=1 → 10; band 3 = time=2 → 20
        np.testing.assert_allclose(rds.read(1).mean(), 0.0, atol=1e-4)
        np.testing.assert_allclose(rds.read(2).mean(), 10.0, atol=1e-4)
        np.testing.assert_allclose(rds.read(3).mean(), 20.0, atol=1e-4)


def test_banddim_with_dimindex_pins_level(tmp_path):
    """bandDim='time' + dimIndex='level=1' → 3-band tile, each band at level=1."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"bandDim": "time", "dimIndex": "level=1"})
    assert len(rows) == 1
    _, (_, raster_bytes, _) = rows[0]
    with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as rds:
        assert rds.count == 3
        # level=1: band k = t*10+1 → 1, 11, 21
        np.testing.assert_allclose(rds.read(1).mean(), 1.0, atol=1e-4)
        np.testing.assert_allclose(rds.read(2).mean(), 11.0, atol=1e-4)
        np.testing.assert_allclose(rds.read(3).mean(), 21.0, atol=1e-4)


def test_dimindex_and_banddim_are_honored_through_spark_read(spark, tmp_path):
    """Spark hands a reader its options case-insensitively (as lowercase keys); the
    camel-case names dimIndex and bandDim were then never found and silently ignored."""
    from databricks.labs.gbx.ds.register import register

    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    register(spark, only=["netcdf"])
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


def test_banddim_with_fanout_two_rows(tmp_path):
    """bandDim='time' + fanout='level' → 2 rows, each a 3-band tile."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"bandDim": "time", "fanout": "level"})
    assert len(rows) == 2
    for _, (_, raster_bytes, _) in rows:
        with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as rds:
            assert rds.count == 3


def test_banddim_with_fanout_correct_band_values(tmp_path):
    """bandDim='time' + fanout='level': correct band values per row."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {"bandDim": "time", "fanout": "level"})
    row_band_means = []
    for _, (_, raster_bytes, _) in rows:
        with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as rds:
            row_band_means.append(
                tuple(float(rds.read(b + 1).mean()) for b in range(3))
            )
    row_band_means.sort()
    # level=0 row: bands = 0*10+0, 1*10+0, 2*10+0 = 0, 10, 20
    assert row_band_means[0] == pytest.approx((0.0, 10.0, 20.0), abs=1e-4)
    # level=1 row: bands = 0*10+1, 1*10+1, 2*10+1 = 1, 11, 21
    assert row_band_means[1] == pytest.approx((1.0, 11.0, 21.0), abs=1e-4)


def test_banddim_dimindex_overlap_raises(tmp_path):
    """bandDim and dimIndex naming the same dim → ValueError."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with pytest.raises(ValueError, match="bandDim|stacked|pinned"):
        _read_reader(p, {"bandDim": "time", "dimIndex": "time=1"})


def test_banddim_fanout_overlap_raises(tmp_path):
    """bandDim and fanout naming the same dim → ValueError."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with pytest.raises(ValueError, match="bandDim|stacked|expanded"):
        _read_reader(p, {"bandDim": "time", "fanout": "time"})


def test_banddim_metadata_present(tmp_path, caplog):
    """bandDim tile metadata carries bandDim, bandCoords, bandDescriptions."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with caplog.at_level(logging.WARNING):
        rows = _read_reader(p, {"bandDim": "time"})
    _, (_, _, meta) = rows[0]
    assert meta.get("bandDim") == "time"
    # bandCoords: time=[0.0, 1.0, 2.0]
    assert meta.get("bandCoords") == "0.0,1.0,2.0"
    # bandDescriptions: "time=0.0,time=1.0,time=2.0"
    assert "bandDescriptions" in meta
    assert "time=0.0" in meta["bandDescriptions"]
    assert "time=2.0" in meta["bandDescriptions"]


def test_banddim_source_contains_marker(tmp_path, caplog):
    """Source for a bandDim row contains 'bandDim=time'."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with caplog.at_level(logging.WARNING):
        rows = _read_reader(p, {"bandDim": "time"})
    source, _ = rows[0]
    assert "bandDim=time" in source, f"unexpected source: {source}"


def test_banddim_pure_2d_var_silent(tmp_path):
    """Pure-2-D variable + bandDim='time' → NO UserWarning, 1 single-band tile."""
    import warnings as _warnings

    p = str(tmp_path / "m.nc")
    _write_mixed_grid(p)
    with _warnings.catch_warnings(record=True) as caught:
        _warnings.simplefilter("always")
        rows = _read_reader(p, {"bandDim": "time", "variable": "sst"})
    user_warns = [w for w in caught if issubclass(w.category, UserWarning)]
    assert user_warns == [], f"unexpected UserWarning for pure-2-D var: {user_warns}"
    assert len(rows) == 1
    _, (_, raster_bytes, _) = rows[0]
    with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as rds:
        assert rds.count == 1


def test_banddim_soft_warn_large_stack(tmp_path, caplog, monkeypatch):
    """Soft-warn fires when estimated decoded bytes exceed _BANDDIM_WARN_BYTES."""
    import databricks.labs.gbx.ds.netcdf as netcdf_module

    monkeypatch.setattr(netcdf_module, "_BANDDIM_WARN_BYTES", 1)
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with caplog.at_level(logging.WARNING):
        with pytest.warns(UserWarning, match="estimated|fanout|MiB"):
            _read_reader(p, {"bandDim": "time"})


def test_banddim_unset_default_single_band(tmp_path):
    """No bandDim → count=1, source unchanged (backward compat)."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    rows = _read_reader(p, {})
    _, (_, raster_bytes, _) = rows[0]
    with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as rds:
        assert rds.count == 1


def test_banddim_typo_warns_and_single_band_fallthrough(tmp_path):
    """A typo'd bandDim name (unknown to a var with leading dims) → UserWarning
    naming the unknown dim + single-band tile (count=1) fall-through."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    # 'tyme' is not a leading dim of 'temp' (which has 'time' and 'level')
    with pytest.warns(UserWarning, match="tyme"):
        rows = _read_reader(p, {"bandDim": "tyme"})
    assert len(rows) == 1
    _, (_, raster_bytes, _) = rows[0]
    with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as rds:
        assert rds.count == 1  # fell through to single-band (bandDim not applied)


def test_banddim_typo_warning_names_actual_leading_dims(tmp_path):
    """The refined-B UserWarning for a typo'd bandDim names the var's actual dims."""
    p = str(tmp_path / "g.nc")
    _write_4d_grid(p)
    with pytest.warns(UserWarning) as warning_list:
        _read_reader(p, {"bandDim": "tyme"})
    msgs = [str(w.message) for w in warning_list]
    assert any(
        "time" in m and "level" in m for m in msgs
    ), f"warning should name actual leading dims; got: {msgs}"


def test_banddim_typo_pure_2d_still_silent(tmp_path):
    """A typo'd bandDim on a pure-2-D variable emits NO UserWarning (moot)."""
    import warnings as _warnings

    p = str(tmp_path / "m.nc")
    _write_mixed_grid(p)
    with _warnings.catch_warnings(record=True) as caught:
        _warnings.simplefilter("always")
        rows = _read_reader(p, {"bandDim": "tyme", "variable": "sst"})
    user_warns = [w for w in caught if issubclass(w.category, UserWarning)]
    assert (
        user_warns == []
    ), f"unexpected UserWarning for pure-2-D + typo'd bandDim: {user_warns}"
    assert len(rows) == 1


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
