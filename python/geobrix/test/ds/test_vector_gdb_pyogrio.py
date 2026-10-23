"""FileGDB writing goes through pyogrio, with no native GDAL bindings (osgeo).

pyogrio's bundled GDAL writes OpenFileGDB (GDAL >= 3.6), so ``file_gdb_gbx`` is a
pure lightweight writer: it runs wherever the light tier runs, Serverless and ARM
included. These tests run the writer's driver-side commit IN PROCESS with ``osgeo``
made un-importable (a Spark commit runs in a separate Python worker, where a
monkeypatch of this process would not reach), then read the ``.gdb`` back through
pyogrio and through the ``file_gdb_gbx`` reader.
"""

import datetime
import decimal
import os
import subprocess
import sys

import pyarrow as pa
import pyogrio
import pytest
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from shapely import LineString, Point, Polygon
from shapely import from_wkb as _from_wkb
from shapely import to_wkb

from databricks.labs.gbx.core.crs import authority_srid_of, resolve_crs, to_pyproj_crs
from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.ds.vector import VectorGbxWriter

_META = [
    StructField("geom_0", BinaryType()),
    StructField("geom_0_srid", StringType()),
    StructField("geom_0_srid_proj", StringType()),
]


@pytest.fixture(autouse=True)
def no_osgeo(monkeypatch):
    """Make every ``import osgeo[.x]`` raise ImportError for the test's duration."""
    for name in [n for n in sys.modules if n == "osgeo" or n.startswith("osgeo.")]:
        monkeypatch.delitem(sys.modules, name)
    for name in (
        "osgeo",
        "osgeo.ogr",
        "osgeo.osr",
        "osgeo.gdal",
        "osgeo._ogr",
        "osgeo._osr",
        "osgeo._gdal",
    ):
        monkeypatch.setitem(sys.modules, name, None)


def _schema(*attrs):
    return StructType([*attrs, *_META])


def _write_gdb(path, schema, partitions, options=None):
    """Run the writer's executor ``write`` per partition, then the driver ``commit``,
    exactly as a Spark write does but in this process. Returns the resolved path."""
    w = VectorGbxWriter(str(path), schema, "OpenFileGDB", dict(options or {}), True)
    messages = [w.write(iter(rows)) for rows in partitions]
    w.commit(messages)
    return w.path


def _pts(n, offset=0, srid="4326", proj=""):
    return [
        (f"p{offset + i}", bytearray(to_wkb(Point(offset + i, i))), srid, proj)
        for i in range(n)
    ]


_NAME = StructField("name", StringType())


def test_osgeo_is_blocked():
    with pytest.raises(ImportError):
        from osgeo import ogr  # noqa: F401


def test_writer_module_does_not_import_osgeo():
    """The writer is light: no osgeo import anywhere in ds/vector.py."""
    import re

    import databricks.labs.gbx.ds.vector as vec

    with open(vec.__file__, encoding="utf-8") as fh:
        src = fh.read()
    found = re.findall(r"^\s*(?:import|from)\s+osgeo\b.*$", src, re.MULTILINE)
    assert not found, f"ds/vector.py must not import osgeo: {found}"


@pytest.mark.parametrize(
    "geom,allowed",
    [
        (Point(1.0, 2.0), {"Point"}),
        (LineString([(0, 0), (1, 1), (2, 0)]), {"LineString", "MultiLineString"}),
        (
            Polygon([(0, 0), (4, 0), (4, 3), (0, 3), (0, 0)]),
            {"Polygon", "MultiPolygon"},
        ),
    ],
    ids=["point", "line", "polygon"],
)
def test_geometry_types(tmp_path, geom, allowed):
    rows = [(f"g{i}", bytearray(to_wkb(geom)), "4326", "") for i in range(3)]
    out = _write_gdb(tmp_path / "geom.gdb", _schema(_NAME), [rows])

    info = pyogrio.read_info(out)
    assert info["features"] == 3
    assert info["geometry_type"] in allowed
    got = pyogrio.read_dataframe(out)
    assert set(got["name"]) == {"g0", "g1", "g2"}
    for g in got.geometry:
        assert g.geom_type in allowed
        assert g.equals(geom) or g.bounds == geom.bounds


def test_default_layer_name_and_layername_option(tmp_path):
    default = _write_gdb(tmp_path / "d.gdb", _schema(_NAME), [_pts(2)])
    assert [n for n, _ in pyogrio.list_layers(default)] == ["layer"]

    named = _write_gdb(
        tmp_path / "n.gdb", _schema(_NAME), [_pts(2)], {"layerName": "roads"}
    )
    assert [n for n, _ in pyogrio.list_layers(named)] == ["roads"]


def test_multi_layer_gdb_reads_each_layer(spark, tmp_path):
    """The writer's .gdb is an ordinary multi-layer geodatabase: a second layer
    appended after the write is served beside the first, each by name."""
    register(spark)
    out = _write_gdb(
        tmp_path / "multi.gdb", _schema(_NAME), [_pts(2)], {"layerName": "roads"}
    )
    second = pa.table(
        {
            "name": ["x", "y", "z"],
            "SHAPE": pa.array([to_wkb(Point(i, i)) for i in range(3)], pa.binary()),
        }
    )
    pyogrio.write_arrow(
        second,
        out,
        driver="OpenFileGDB",
        layer="parcels",
        geometry_name="SHAPE",
        geometry_type="Point",
        crs="EPSG:4326",
        append=True,
    )
    assert sorted(n for n, _ in pyogrio.list_layers(out)) == ["parcels", "roads"]

    roads = spark.read.format("file_gdb_gbx").option("layerName", "roads").load(out)
    parcels = spark.read.format("file_gdb_gbx").option("layerName", "parcels").load(out)
    assert {r["name"] for r in roads.collect()} == {"p0", "p1"}
    assert {r["name"] for r in parcels.collect()} == {"x", "y", "z"}


def test_many_fragments_all_land(tmp_path):
    """One fragment per partition; the commit merges every one into the layer."""
    parts = [_pts(4, offset=i * 4) for i in range(5)]
    out = _write_gdb(tmp_path / "frags.gdb", _schema(_NAME), parts)
    got = pyogrio.read_dataframe(out)
    assert len(got) == 20
    assert set(got["name"]) == {f"p{i}" for i in range(20)}


def test_commit_streams_in_one_write_arrow(tmp_path, monkeypatch):
    """All fragments feed ONE write_arrow as a record-batch stream (bounded driver
    memory, single pass) -- never concatenated into one in-memory table."""
    monkeypatch.setattr(
        pa,
        "concat_tables",
        lambda *a, **k: pytest.fail("commit must not concat fragments"),
    )
    seen = []
    real = pyogrio.write_arrow

    def spy(obj, path, **kw):
        seen.append((type(obj), kw.get("append", False)))
        return real(obj, path, **kw)

    monkeypatch.setattr(pyogrio, "write_arrow", spy)
    parts = [_pts(3, offset=i * 3) for i in range(4)]
    out = _write_gdb(tmp_path / "one.gdb", _schema(_NAME), parts)

    assert seen == [(pa.RecordBatchReader, False)], seen
    assert pyogrio.read_info(out)["features"] == 12


def test_append_path_when_stream_unsupported(tmp_path, monkeypatch):
    """If the GDAL build cannot consume a record-batch stream, the commit falls
    back to a first write plus ``append=True`` per remaining fragment."""
    seen = []
    real = pyogrio.write_arrow

    def no_stream(obj, path, **kw):
        if isinstance(obj, pa.RecordBatchReader):
            raise TypeError("stream not supported")
        seen.append(kw.get("append", False))
        return real(obj, path, **kw)

    monkeypatch.setattr(pyogrio, "write_arrow", no_stream)
    parts = [_pts(3, offset=i * 3) for i in range(3)]
    out = _write_gdb(tmp_path / "app.gdb", _schema(_NAME), parts)

    assert seen == [False, True, True], seen
    got = pyogrio.read_dataframe(out)
    assert len(got) == 9
    assert set(got["name"]) == {f"p{i}" for i in range(9)}


def test_attribute_types(tmp_path):
    big = 2**53 + 1  # not representable as a float64: proves a true Int64 field
    schema = _schema(
        StructField("s", StringType()),
        StructField("i", IntegerType()),
        StructField("l", LongType()),
        StructField("sh", ShortType()),
        StructField("d", DoubleType()),
        StructField("f", FloatType()),
        StructField("b", BooleanType()),
        StructField("dt", DateType()),
        StructField("ts", TimestampType()),
        StructField("dec", DecimalType(10, 2)),
        StructField("blob", BinaryType()),
    )
    g = bytearray(to_wkb(Point(1.0, 2.0)))
    rows = [
        (
            "alpha",
            7,
            big,
            3,
            1.25,
            2.5,
            True,
            datetime.date(2020, 1, 2),
            datetime.datetime(2020, 1, 2, 3, 4, 5),
            decimal.Decimal("12345.67"),
            b"\x01\x02",
            g,
            "4326",
            "",
        ),
        (
            None,
            8,
            big + 2,
            4,
            -1.5,
            0.5,
            False,
            None,
            None,
            None,
            None,
            g,
            "4326",
            "",
        ),
    ]
    out = _write_gdb(tmp_path / "types.gdb", schema, [rows])

    dtypes = dict(
        zip(pyogrio.read_info(out)["fields"], pyogrio.read_info(out)["dtypes"])
    )
    assert dtypes["s"] == "object"
    assert dtypes["i"] == "int32"
    assert dtypes["l"] == "int64"
    assert dtypes["sh"] == "int16"
    assert dtypes["d"] == "float64"
    assert dtypes["f"] == "float32"
    assert dtypes["dt"] == "datetime64[D]"
    assert dtypes["ts"].startswith("datetime64")

    got = pyogrio.read_dataframe(out).set_index("l")
    assert sorted(got.index) == [big, big + 2]  # exact: Int64, not Float64
    first = got.loc[big]
    assert first["s"] == "alpha"
    assert first["i"] == 7
    assert first["sh"] == 3
    assert first["d"] == 1.25
    assert first["f"] == 2.5
    assert bool(first["b"]) is True
    assert str(first["dt"])[:10] == "2020-01-02"
    assert str(first["ts"])[:19] == "2020-01-02 03:04:05"
    assert float(first["dec"]) == 12345.67
    assert bytes(first["blob"]) == b"\x01\x02"
    second = got.loc[big + 2]
    assert second["s"] is None and second["blob"] is None
    assert bool(second["b"]) is False


@pytest.mark.parametrize(
    "srid,proj,expected",
    [
        ("4326", "", 4326),
        ("27700", "", 27700),
        ("54008", "", 54008),  # an ESRI code keeps its authority
    ],
    ids=["epsg-4326", "epsg-27700", "esri-54008"],
)
def test_crs_is_written(tmp_path, srid, proj, expected):
    out = _write_gdb(
        tmp_path / "crs.gdb", _schema(_NAME), [_pts(2, srid=srid, proj=proj)]
    )
    crs = pyogrio.read_info(out)["crs"]
    assert crs is not None
    assert authority_srid_of(resolve_crs(crs)) == expected


def test_crs_from_proj4_fallback(tmp_path):
    proj4 = "+proj=longlat +datum=WGS84 +no_defs"
    out = _write_gdb(
        tmp_path / "p4.gdb", _schema(_NAME), [_pts(2, srid="0", proj=proj4)]
    )
    crs = to_pyproj_crs(resolve_crs(pyogrio.read_info(out)["crs"]))
    assert crs.is_geographic
    assert crs.ellipsoid.name == "WGS 84"


def test_no_crs_when_unknown(tmp_path):
    out = _write_gdb(tmp_path / "nocrs.gdb", _schema(_NAME), [_pts(2, srid="0")])
    assert pyogrio.read_info(out)["crs"] is None
    assert pyogrio.read_info(out)["features"] == 2


def test_roundtrip_through_reader_and_pyogrio(spark, tmp_path):
    """Written without osgeo, the .gdb reads back through the file_gdb_gbx reader
    (the shared schema attrs..., <geometry>, <geometry>_srid, <geometry>_srid_proj;
    the geometry field of a FileGDB is SHAPE) and through pyogrio, with the same
    rows, geometries and CRS."""
    register(spark)
    schema = _schema(_NAME, StructField("pop", IntegerType()))
    rows = [
        (
            f"feat_{i}",
            i * 10,
            bytearray(
                to_wkb(Polygon([(i, 0), (i + 1, 0), (i + 1, 1), (i, 1), (i, 0)]))
            ),
            "27700",
            "",
        )
        for i in range(6)
    ]
    out = _write_gdb(tmp_path / "rt.gdb", schema, [rows[:3], rows[3:]])

    back = spark.read.format("file_gdb_gbx").load(out)
    cols = [f.name for f in back.schema.fields]
    assert cols[-3:] == ["SHAPE", "SHAPE_srid", "SHAPE_srid_proj"]
    got = back.collect()
    assert {r["name"]: r["pop"] for r in got} == {f"feat_{i}": i * 10 for i in range(6)}
    assert {r["SHAPE_srid"] for r in got} == {"27700"}
    assert {_from_wkb(bytes(r["SHAPE"])).geom_type for r in got} <= {
        "Polygon",
        "MultiPolygon",
    }

    pdf = pyogrio.read_dataframe(out)
    assert len(pdf) == 6
    assert sorted(pdf["pop"]) == [i * 10 for i in range(6)]
    assert authority_srid_of(resolve_crs(pdf.crs)) == 27700


def test_zip_output_layout(tmp_path):
    """zip=true still packs the .gdb at the archive root and publishes only the zip."""
    out = _write_gdb(tmp_path / "bundle", _schema(_NAME), [_pts(3)], {"zip": "true"})
    assert out.endswith(".gdb.zip")
    assert os.path.isfile(out)
    assert not os.path.exists(out[: -len(".zip")])  # no loose .gdb beside the archive
    assert pyogrio.read_info("/vsizip/" + out)["features"] == 3


# A column named like the geometry field (SHAPE, in any letter case) makes
# pyogrio.write_arrow abort the whole process (GDAL 3.11 and 3.12), so each case
# runs in a CHILD process: a regression then fails one test instead of killing pytest.
_CHILD_WRITE = """
import sys, warnings
from pyspark.sql.types import BinaryType, LongType, StringType, StructField, StructType
from shapely import Point, to_wkb
from databricks.labs.gbx.ds.vector import VectorGbxWriter

out, name, kind = sys.argv[1], sys.argv[2], sys.argv[3]
schema = StructType(
    [
        StructField(name, LongType() if kind == "long" else StringType()),
        StructField("keep", StringType()),
        StructField("geom_0", BinaryType()),
        StructField("geom_0_srid", StringType()),
        StructField("geom_0_srid_proj", StringType()),
    ]
)
rows = [(7 if kind == "long" else "a", "b", bytearray(to_wkb(Point(1.0, 2.0))), "4326", "")]
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    try:
        w = VectorGbxWriter(out, schema, "OpenFileGDB", {}, True)
        w.commit([w.write(iter(rows))])
    except ValueError as e:
        print("VALUEERROR " + str(e).replace(chr(10), " "))
        sys.exit(0)
print("WARNINGS " + " | ".join(str(c.message).replace(chr(10), " ") for c in caught))
"""


def _child_write(tmp_path, column, kind="string"):
    """Write a one-row FileGDB with an attribute named ``column`` (a string, or a
    long when ``kind`` is "long") in a child process; return (CompletedProcess, output path).
    """
    out = str(tmp_path / "child.gdb")
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path))
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD_WRITE, out, column, kind],
        capture_output=True,
        text=True,
        env=env,
        timeout=300,
    )
    return proc, out


@pytest.mark.parametrize("column", ["shape", "Shape", "SHAPE", "sHaPe"])
def test_attribute_named_like_the_geometry_field_is_refused(tmp_path, column):
    """A ``shape`` attribute in any letter case raises a clean ValueError that names
    the column and the fix; the write does not reach pyogrio (which aborts the process).
    """
    proc, out = _child_write(tmp_path, column)
    assert proc.returncode == 0, (
        f"child died with return code {proc.returncode} (negative = killed by a signal)\n"
        f"{proc.stderr[-1500:]}"
    )
    lines = [x for x in proc.stdout.splitlines() if x.startswith("VALUEERROR ")]
    assert lines, f"no ValueError for column {column!r}: {proc.stdout!r}"
    msg = lines[0]
    assert repr(column) in msg
    assert "SHAPE" in msg and "shape_attr" in msg and "Rename" in msg
    assert not os.path.exists(out)  # refused before any output


@pytest.mark.parametrize("column", ["OBJECTID", "objectid", "ObjectID"])
def test_attribute_named_objectid_warns_it_is_the_feature_id(tmp_path, column):
    """A FileGDB layer keeps its feature id in OBJECTID: GDAL takes an integer column of
    that name as the feature ids instead of writing it as an attribute. The writer says
    so, naming the column."""
    proc, out = _child_write(tmp_path, column, kind="long")
    assert (
        proc.returncode == 0
    ), f"child died with return code {proc.returncode}\n{proc.stderr[-1500:]}"
    lines = [x for x in proc.stdout.splitlines() if x.startswith("WARNINGS ")]
    assert lines, f"the write did not finish: {proc.stdout!r}"
    assert repr(column) in lines[0] and "feature id" in lines[0]
    info = pyogrio.read_info(out)
    assert info["features"] == 1 and "keep" in info["fields"]
    assert column.lower() not in [f.lower() for f in info["fields"]]  # not an attribute
    assert list(pyogrio.read_dataframe(out, fid_as_index=True).index) == [
        7
    ]  # the feature id
