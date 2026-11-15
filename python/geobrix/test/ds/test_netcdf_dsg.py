"""netcdf_gbx vector mode reads the CF-1.8 DSG feature types: point, profile and trajectory.

Each fixture under ``src/test/resources/binary/netcdf-dsg*`` (made by ``scripts/testdata/make_netcdf_dsg*.py``) is read
into the shared vector schema, one point feature per observation. ``EXPECTED`` is what the GDAL netCDF driver gave for
the same files through the former ``netcdf_ogr`` reader (captured before that reader was retired): the rows, the
geometry (a point; a 3D point where the profile carries an altitude) and the attribute columns with their values.
The GDAL reader gives raw CF numbers for ``time`` and no CRS; netcdf_gbx decodes ``time`` to a timestamp and gives the
CF lat/lon its CRS, so those two columns are compared after the decoding (``days since 2020-01-01``) and as EPSG:4326.
"""

from datetime import datetime, timezone
from pathlib import Path

import pytest
import shapely

from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource

RES = Path(__file__).resolve().parents[4] / "src/test/resources/binary"
FIXTURES = {
    "point": RES / "netcdf-dsg",
    "profile": RES / "netcdf-dsg-profile",
    "trajectory": RES / "netcdf-dsg-trajectory",
    "ragged": RES
    / "netcdf-dsg-ragged",  # the profiles as a contiguous ragged array: only netcdf_gbx reads it
    "trajectory_indexed": RES  # a ragged trajectory, lat/lon per observation: only netcdf_gbx reads it
    / "netcdf-dsg-trajectory-indexed",
}
_T0 = datetime(2020, 1, 1, tzinfo=timezone.utc)

# per type: the attribute columns, then the features (WKT, attribute values) as the GDAL netCDF driver gave them
EXPECTED = {
    "point": (
        {"ch4"},
        [
            ("POINT (10 50)", {"ch4": 0.0}),
            ("POINT (10.1 50.1)", {"ch4": 1.0}),
            ("POINT (10.2 50.2)", {"ch4": 2.0}),
            ("POINT (10.3 50.3)", {"ch4": 3.0}),
            ("POINT (10.4 50.4)", {"ch4": 4.0}),
        ],
    ),
    "profile": (
        {"profile", "temp", "time"},
        [
            ("POINT Z (10 50 0)", {"profile": 101, "temp": 0.5, "time": 0.0}),
            ("POINT Z (10 50 10)", {"profile": 101, "temp": 1.5, "time": 0.0}),
            ("POINT Z (10 50 20)", {"profile": 101, "temp": 2.5, "time": 0.0}),
            ("POINT Z (11 51 0)", {"profile": 102, "temp": 3.5, "time": 1.0}),
            ("POINT Z (11 51 10)", {"profile": 102, "temp": 4.5, "time": 1.0}),
            ("POINT Z (11 51 20)", {"profile": 102, "temp": 5.5, "time": 1.0}),
            ("POINT Z (12 52 0)", {"profile": 103, "temp": 6.5, "time": 2.0}),
            ("POINT Z (12 52 10)", {"profile": 103, "temp": 7.5, "time": 2.0}),
            ("POINT Z (12 52 20)", {"profile": 103, "temp": 8.5, "time": 2.0}),
        ],
    ),
    "trajectory": (
        {"trajectory", "temp", "time"},
        [
            ("POINT (10 50)", {"trajectory": 7, "temp": 0.25, "time": 0.0}),
            ("POINT (10.1 50.1)", {"trajectory": 7, "temp": 1.25, "time": 0.1}),
            ("POINT (10.2 50.2)", {"trajectory": 7, "temp": 2.25, "time": 0.2}),
            ("POINT (10.3 50.3)", {"trajectory": 7, "temp": 3.25, "time": 0.3}),
            ("POINT (20 40)", {"trajectory": 8, "temp": 4.25, "time": 1.0}),
            ("POINT (20.1 40.1)", {"trajectory": 8, "temp": 5.25, "time": 1.1}),
        ],
    ),
}


def _read_gbx(spark, name):
    spark.dataSource.register(NetcdfGbxDataSource)
    return (
        spark.read.format("netcdf_gbx")
        .option("mode", "vector")
        .load(str(FIXTURES[name]))
    )


def _normalise(rows, geom_col="geom_0"):
    """(WKT, attributes) per feature, ``time`` as the CF days since 2020-01-01, sorted by geometry."""
    out = []
    for r in rows:
        d = r.asDict()
        wkt = shapely.from_wkb(bytes(d.pop(geom_col))).wkt
        for k in [k for k in d if k.startswith(geom_col)]:
            d.pop(k)
        t = d.get("time")
        if isinstance(t, datetime):
            d["time"] = (
                (
                    t.astimezone(timezone.utc)
                    if t.tzinfo
                    else t.replace(tzinfo=timezone.utc)
                )
                - _T0
            ).total_seconds() / 86400
        out.append((wkt, d))
    return sorted(out, key=lambda f: f[0])


def _same(got, want):
    assert [g[0] for g in got] == [w[0] for w in want]
    for (_, g), (_, w) in zip(got, want):
        assert g.keys() == w.keys()
        for k, v in w.items():
            assert g[k] == pytest.approx(v, abs=1e-6), (k, g[k], v)


@pytest.mark.parametrize("name", list(EXPECTED))
def test_netcdf_gbx_vector_reads_the_dsg_features_netcdf_ogr_read(spark, name):
    """Rows, geometry, attribute columns and values of the GDAL netCDF driver's output; the CRS is EPSG:4326."""
    columns, features = EXPECTED[name]
    df = _read_gbx(spark, name)
    assert set(df.columns) == columns | {"geom_0", "geom_0_srid", "geom_0_srid_proj"}
    rows = df.collect()
    assert {(r["geom_0_srid"], r["geom_0_srid_proj"]) for r in rows} == {
        ("4326", "EPSG:4326")
    }
    _same(_normalise(rows), sorted(features, key=lambda f: f[0]))


def test_a_contiguous_ragged_profile_file_reads_as_the_indexed_one_does(spark):
    """A ``row_size`` variable with ``sample_dimension`` says how many observations follow each profile: the same 9
    points, with the profile's id and time on each (the GDAL driver refuses this layout).
    """
    columns, features = EXPECTED["profile"]
    df = _read_gbx(spark, "ragged")
    assert set(df.columns) == columns | {"geom_0", "geom_0_srid", "geom_0_srid_proj"}
    _same(_normalise(df.collect()), sorted(features, key=lambda f: f[0]))


def test_a_ragged_trajectory_with_lat_lon_per_observation_keeps_its_instance_columns_and_z(
    spark,
):
    """The CF trajectory layout: lat/lon on the observation dimension, the id and `platform` on the trajectory
    dimension, an index per observation. The id and `platform` are expanded per observation, `time` is a column and
    the altitude is the point's Z (this file used to fall back to plain points: no id, no platform, no Z).
    """
    df = _read_gbx(spark, "trajectory_indexed")
    assert set(df.columns) == {
        "trajectory",
        "platform",
        "temp",
        "time",
        "geom_0",
        "geom_0_srid",
        "geom_0_srid_proj",
    }
    want = [
        (
            "POINT Z (10 50 0)",
            {"trajectory": 7, "platform": 70, "temp": 0.75, "time": 0.0},
        ),
        (
            "POINT Z (10.1 50.1 5)",
            {"trajectory": 7, "platform": 70, "temp": 1.75, "time": 0.1},
        ),
        (
            "POINT Z (10.2 50.2 10)",
            {"trajectory": 7, "platform": 70, "temp": 2.75, "time": 0.2},
        ),
        (
            "POINT Z (20 40 0)",
            {"trajectory": 8, "platform": 80, "temp": 3.75, "time": 1.0},
        ),
        (
            "POINT Z (20.1 40.1 5)",
            {"trajectory": 8, "platform": 80, "temp": 4.75, "time": 1.1},
        ),
    ]
    _same(_normalise(df.collect()), sorted(want, key=lambda f: f[0]))


def _ragged_dataset(index=(0, 0, 1, 1), z_attrs=None, counts=None):
    """An in-memory CF ragged profile file: 2 profiles, 4 observations, lat/lon on the profiles."""
    import numpy as np
    import xarray as xr

    ds = xr.Dataset(
        {
            "profile": ("profile", np.array([101, 102]), {"cf_role": "profile_id"}),
            "latitude": (
                "profile",
                np.array([50.0, 51.0]),
                {"standard_name": "latitude", "units": "degrees_north"},
            ),
            "longitude": (
                "profile",
                np.array([10.0, 11.0]),
                {"standard_name": "longitude", "units": "degrees_east"},
            ),
            "depth": (
                "obs",
                np.array([0.0, 10.0, 0.0, 20.0]),
                z_attrs or {"standard_name": "depth", "positive": "down"},
            ),
            "temp": ("obs", np.arange(4, dtype="float32")),
        },
        attrs={"featureType": "profile"},
    )
    if counts is None:
        ds["profile_index"] = (
            "obs",
            np.array(index),
            {"instance_dimension": "profile"},
        )
    else:
        ds["row_size"] = ("profile", np.array(counts), {"sample_dimension": "obs"})
    return ds


def test_a_depth_is_the_z_with_the_sign_the_gdal_netcdf_driver_gives():
    """`positive="down"` / `standard_name="depth"` make the variable the Z, and its values are kept as stored (positive
    down): the GDAL netCDF driver (netcdf_ogr) gives the same Z for such a file and does not flip the sign.
    """
    from databricks.labs.gbx.ds import _netcdf

    ds = _ragged_dataset()
    layout = _netcdf.dsg_layout(ds)
    assert layout.z == "depth"
    _, _, z, _, _ = _netcdf.dsg_arrays(ds, layout, ["temp"])
    assert list(z) == [0.0, 10.0, 0.0, 20.0]


@pytest.mark.parametrize(
    "dataset,message",
    [
        (
            _ragged_dataset(index=(0, -1, 1, 1)),
            r"'profile_index' gives observation 1 .*no instance.*-1",
        ),
        (
            _ragged_dataset(index=(0, 0, 1, 2)),
            r"observation 3 .*not an index in \[0, 2\)",
        ),
        (_ragged_dataset(counts=(2, 1)), r"add up to 3, but 'obs' holds 4"),
        (_ragged_dataset(counts=(3, 2)), r"add up to 5, but 'obs' holds 4"),
    ],
    ids=["negative-index", "index-past-the-instances", "counts-short", "counts-long"],
)
def test_ragged_index_and_counts_that_do_not_fit_the_file_are_refused_with_a_fix(
    dataset, message
):
    """An observation with no instance is refused, not dropped or wrapped onto the last instance; counts that do
    not sum to the observation dimension are refused, not paired with the wrong observations.
    """
    from databricks.labs.gbx.ds import _netcdf

    layout = _netcdf.dsg_layout(dataset)
    with pytest.raises(ValueError, match=message) as err:
        _netcdf.dsg_arrays(dataset, layout, ["temp"])
    assert "Fix:" in str(err.value)
