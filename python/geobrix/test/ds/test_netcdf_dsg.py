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
