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
import warnings

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
    TimestampNTZType,
    TimestampType,
)
from shapely import (
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)
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
        assert g.equals(geom)  # the same shape, not just the same bounds


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


# ---------------------------------------------------------------------------
# targetArcGisVersion: which ArcGIS clients the layer is written for
# ---------------------------------------------------------------------------


def _all_gdbtable_bytes(gdb):
    out = b""
    for name in sorted(os.listdir(gdb)):
        if name.endswith(".gdbtable"):
            with open(os.path.join(gdb, name), "rb") as fh:
                out += fh.read()
    return out


def _stamped_for_pro_3_2(gdb):
    """Whether the layer carries ``RequiredGeodatabaseClientVersion 13.2`` (the GDAL
    stamp that makes Esri clients older than ArcGIS Pro 3.2 refuse the layer)."""
    return b"RequiredGeodatabaseClientVersion>13.2<" in _all_gdbtable_bytes(gdb)


def _g():
    return bytearray(to_wkb(Point(1.0, 2.0)))


def _gdb_with(tmp_path, field, value, options=None, name="t.gdb"):
    schema = _schema(StructField("v", field))
    rows = [(value, _g(), "4326", "")]
    return _write_gdb(tmp_path / name, schema, [rows], options)


def test_target_version_auto_with_only_strings_writes_no_client_stamp(tmp_path):
    out = _write_gdb(tmp_path / "s.gdb", _schema(_NAME), [_pts(2)])
    assert not _stamped_for_pro_3_2(out)


@pytest.mark.parametrize(
    "field,value",
    [
        (LongType(), 2**53 + 1),
        (DateType(), datetime.date(2020, 1, 2)),
        (TimestampType(), datetime.datetime(2020, 1, 2, 3, 4, 5)),
        (TimestampNTZType(), datetime.datetime(2020, 1, 2, 3, 4, 5)),
    ],
    ids=["long", "date", "timestamp", "timestamp_ntz"],
)
def test_target_version_auto_stamps_for_long_date_and_timestamp(tmp_path, field, value):
    out = _gdb_with(tmp_path, field, value)
    assert _stamped_for_pro_3_2(out)


def test_target_version_auto_keeps_a_long_exact(tmp_path):
    out = _gdb_with(tmp_path, LongType(), 2**53 + 1)
    info = pyogrio.read_info(out)
    assert dict(zip(info["fields"], info["dtypes"]))["v"] == "int64"
    assert pyogrio.read_dataframe(out)["v"].tolist() == [2**53 + 1]


def test_target_version_all_writes_a_long_as_float64_without_the_stamp(tmp_path):
    out = _gdb_with(tmp_path, LongType(), 2**53 + 1, {"targetArcGisVersion": "ALL"})
    info = pyogrio.read_info(out)
    assert dict(zip(info["fields"], info["dtypes"]))["v"] == "float64"
    assert not _stamped_for_pro_3_2(out)


def test_target_version_all_makes_a_timestamp_a_date_time_without_offset(tmp_path):
    """Without the Pro 3.2 target a timestamp is an Esri date-time field, which has no
    offset; the Pro 3.2 target writes a timestamp-with-offset field."""
    ts = datetime.datetime(2020, 1, 2, 3, 4, 5)
    plain = _gdb_with(
        tmp_path, TimestampType(), ts, {"targetArcGisVersion": "ALL"}, "a.gdb"
    )
    pro = _gdb_with(tmp_path, TimestampType(), ts, name="b.gdb")
    assert b"esriFieldTypeTimestampOffset" not in _all_gdbtable_bytes(plain)
    assert b"esriFieldTypeTimestampOffset" in _all_gdbtable_bytes(pro)


def test_target_version_pro_3_2_stamps_even_without_a_trigger_column(tmp_path):
    out = _write_gdb(
        tmp_path / "p.gdb",
        _schema(_NAME),
        [_pts(2)],
        {"targetArcGisVersion": "ARCGIS_PRO_3_2_OR_LATER"},
    )
    assert _stamped_for_pro_3_2(out)


@pytest.mark.parametrize("value", ["all", "All", "ALL"])
def test_target_version_all_is_case_insensitive(tmp_path, value):
    out = _gdb_with(
        tmp_path, LongType(), 5, {"targetArcGisVersion": value}, f"{value}.gdb"
    )
    assert not _stamped_for_pro_3_2(out)


def test_target_version_auto_is_case_insensitive(tmp_path):
    out = _gdb_with(tmp_path, LongType(), 5, {"targetArcGisVersion": "AUTO"})
    assert _stamped_for_pro_3_2(out)


def test_target_version_auto_ignores_an_objectid_column(tmp_path):
    """An integer OBJECTID becomes the feature id: no long field is written for it, so it
    does not by itself target Pro 3.2. A real long column still does."""
    only_oid = _schema(_NAME, StructField("objectid", LongType()))
    rows = [(f"p{i}", 7 + i, _g(), "4326", "") for i in range(2)]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = _write_gdb(tmp_path / "oid.gdb", only_oid, [rows])
    assert "objectid" not in [f.lower() for f in pyogrio.read_info(out)["fields"]]
    assert list(pyogrio.read_dataframe(out, fid_as_index=True).index) == [7, 8]
    assert not _stamped_for_pro_3_2(out)

    with_long = _schema(
        _NAME, StructField("objectid", LongType()), StructField("n", LongType())
    )
    rows = [(f"p{i}", 7 + i, 2**40 + i, _g(), "4326", "") for i in range(2)]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        out = _write_gdb(tmp_path / "oid2.gdb", with_long, [rows])
    assert _stamped_for_pro_3_2(out)


def test_target_version_invalid_value_raises_before_any_output(tmp_path):
    target = tmp_path / "bad.gdb"
    with pytest.raises(ValueError) as ei:
        VectorGbxWriter(
            str(target),
            _schema(_NAME),
            "OpenFileGDB",
            {"targetArcGisVersion": "ARCGIS_PRO_9"},
            True,
        )
    msg = str(ei.value)
    assert "targetArcGisVersion" in msg and "ARCGIS_PRO_9" in msg
    for allowed in ("auto", "ALL", "ARCGIS_PRO_3_2_OR_LATER"):
        assert allowed in msg
    assert not target.exists()


def test_target_version_is_ignored_by_other_drivers(tmp_path):
    """Only FileGDB has the option: another driver neither reads nor validates it."""
    w = VectorGbxWriter(
        str(tmp_path / "o.gpkg"),
        _schema(_NAME),
        "GPKG",
        {"targetArcGisVersion": "x"},
        True,
    )
    assert w.driver == "GPKG"


# ---------------------------------------------------------------------------
# geometry: Z, multi-part, mixed types, NULL
# ---------------------------------------------------------------------------


def _zs(geom):
    """Every Z of a geometry (all parts, rings included)."""
    out = []
    for part in getattr(geom, "geoms", [geom]):
        rings = (
            [part.exterior, *part.interiors] if hasattr(part, "exterior") else [part]
        )
        for ring in rings:
            out += [c[2] for c in ring.coords]
    return out


def _geoms(*shapes, srid="4326"):
    return [(f"g{i}", bytearray(to_wkb(g)), srid, "") for i, g in enumerate(shapes)]


@pytest.mark.parametrize(
    "shapes",
    [
        [Point(1, 2, 3), Point(4, 5, 6)],
        [LineString([(0, 0, 1), (1, 1, 2)]), LineString([(2, 2, 5), (3, 3, 6)])],
        [Polygon([(0, 0, 1), (1, 0, 2), (1, 1, 3), (0, 0, 1)])],
    ],
    ids=["point-z", "line-z", "polygon-z"],
)
def test_z_coordinates_survive_the_write(tmp_path, shapes):
    out = _write_gdb(tmp_path / "z.gdb", _schema(_NAME), [_geoms(*shapes)])
    got = pyogrio.read_dataframe(out)
    assert len(got) == len(shapes)
    assert all(g.has_z for g in got.geometry)
    assert sorted(z for g in got.geometry for z in _zs(g)) == sorted(
        z for s in shapes for z in _zs(s)
    )


@pytest.mark.parametrize(
    "shapes,allowed",
    [
        (
            [MultiPoint([(1, 2), (3, 4)]), MultiPoint([(5, 6)])],
            {"MultiPoint"},
        ),
        (
            [
                MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]]),
                MultiLineString([[(5, 5), (6, 6)]]),
            ],
            {"MultiLineString"},
        ),
        (
            [
                MultiPolygon(
                    [
                        Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
                        Polygon([(2, 2), (3, 2), (3, 3), (2, 2)]),
                    ]
                ),
                MultiPolygon([Polygon([(5, 5), (6, 5), (6, 6), (5, 5)])]),
            ],
            {"MultiPolygon"},
        ),
        (
            # a Polygon and a MultiPolygon in one layer: a FileGDB polygon layer holds both
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
                MultiPolygon([Polygon([(2, 2), (3, 2), (3, 3), (2, 2)])]),
            ],
            {"MultiPolygon"},
        ),
    ],
    ids=["multipoint", "multiline", "multipolygon", "polygon-and-multipolygon"],
)
def test_multi_part_geometries_keep_every_part(tmp_path, shapes, allowed):
    out = _write_gdb(tmp_path / "m.gdb", _schema(_NAME), [_geoms(*shapes)])
    assert pyogrio.read_info(out)["geometry_type"] in allowed
    got = pyogrio.read_dataframe(out)
    assert len(got) == len(shapes)
    assert sum(len(getattr(g, "geoms", [g])) for g in got.geometry) == sum(
        len(getattr(s, "geoms", [s])) for s in shapes
    )


def test_mixed_geometry_types_fail_the_write_with_a_gdal_error(tmp_path):
    """A FileGDB layer holds one geometry type: a point and a line are refused by GDAL."""
    with pytest.raises(pyogrio.errors.DataLayerError):
        _write_gdb(
            tmp_path / "mix.gdb",
            _schema(_NAME),
            [_geoms(Point(1, 2), LineString([(0, 0), (1, 1)]))],
        )


def test_null_geometry_among_geometries_is_written_as_a_null_feature(tmp_path):
    rows = _geoms(Point(1, 2), Point(3, 4))
    rows.insert(1, ("gnull", None, "4326", ""))
    out = _write_gdb(tmp_path / "n.gdb", _schema(_NAME), [rows])
    got = pyogrio.read_dataframe(out)
    assert len(got) == 3
    by_name = dict(zip(got["name"], got.geometry))
    assert by_name["gnull"] is None
    assert by_name["g0"] is not None and by_name["g1"] is not None


def test_only_null_geometries_raise_a_clear_error_naming_the_option(tmp_path):
    rows = [("a", None, "4326", ""), ("b", None, "4326", "")]
    with pytest.raises(ValueError) as ei:
        _write_gdb(tmp_path / "allnull.gdb", _schema(_NAME), [rows])
    msg = str(ei.value)
    assert "geometryType" in msg and "NULL" in msg


def test_an_unknown_geometry_type_option_is_not_blamed_on_null_geometries(tmp_path):
    """geometryType='Unknown' is not a type a FileGDB layer can have; with real geometries
    the error must say so, not that every geometry is NULL."""
    with pytest.raises(ValueError) as ei:
        _write_gdb(
            tmp_path / "unk.gdb", _schema(_NAME), [_pts(2)], {"geometryType": "Unknown"}
        )
    msg = str(ei.value)
    assert "Unknown" in msg and "every geometry is NULL" not in msg


def test_only_null_geometries_write_when_the_type_is_given(tmp_path):
    rows = [("a", None, "4326", ""), ("b", None, "4326", "")]
    out = _write_gdb(
        tmp_path / "typed.gdb", _schema(_NAME), [rows], {"geometryType": "Point"}
    )
    got = pyogrio.read_dataframe(out)
    assert len(got) == 2 and got.geometry.isna().all()
    assert pyogrio.read_info(out)["geometry_type"] == "Point"


@pytest.mark.parametrize("null_first", [True, False], ids=["null-first", "null-last"])
def test_all_null_partition_does_not_decide_the_geometry_type_or_crs(
    tmp_path, null_first
):
    """The geometry type and CRS come from the first partition that HAS a geometry, in
    either order: an all-NULL partition neither raises nor leaves the layer typeless."""
    nulls = [(f"n{i}", None, "4326", "") for i in range(2)]
    good = [(f"g{i}", bytearray(to_wkb(Point(i, i))), "27700", "") for i in range(2)]
    parts = [nulls, good] if null_first else [good, nulls]
    out = _write_gdb(tmp_path / "order.gdb", _schema(_NAME), parts)
    info = pyogrio.read_info(out)
    assert info["geometry_type"] == "Point" and info["features"] == 4
    assert authority_srid_of(resolve_crs(info["crs"])) == 27700
    got = pyogrio.read_dataframe(out)
    assert got.geometry.isna().sum() == 2


def test_every_partition_null_still_raises_the_geometry_type_error(tmp_path):
    parts = [[("a", None, "4326", "")], [("b", None, "4326", "")]]
    with pytest.raises(ValueError, match="geometryType"):
        _write_gdb(tmp_path / "allnull2.gdb", _schema(_NAME), parts)


# ---------------------------------------------------------------------------
# column names: reserved words and the feature id
# ---------------------------------------------------------------------------


def test_sql_reserved_column_name_is_laundered_with_a_trailing_underscore(tmp_path):
    """GDAL renames a field called after an SQL reserved word (``by``) to ``by_`` and the
    values still land in it."""
    schema = _schema(StructField("by", StringType()), StructField("keep", StringType()))
    rows = [("x", "k", _g(), "4326", ""), ("y", "k", _g(), "4326", "")]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = _write_gdb(tmp_path / "by.gdb", schema, [rows])
    fields = list(pyogrio.read_info(out)["fields"])
    assert "by_" in fields and "by" not in fields and "keep" in fields
    assert sorted(pyogrio.read_dataframe(out)["by_"]) == ["x", "y"]
    assert any("by_" in str(c.message) for c in caught)


@pytest.mark.parametrize(
    "kind,values",
    [
        (DoubleType(), [1.5, 2.5]),
        (StringType(), ["a", "b"]),
        (IntegerType(), [0, 1]),
        (IntegerType(), [-1, 1]),
        (LongType(), [2**31 + 5, 1]),
        (IntegerType(), [7, 7]),
    ],
    ids=["float", "string", "zero", "negative", "above-2**31", "duplicate"],
)
def test_unusable_objectid_values_fail_the_write_with_a_gdal_error(
    tmp_path, kind, values
):
    """GDAL takes an integer OBJECTID column as the feature ids: values that cannot be
    ids (or a non-integer column) fail the write with a GDAL error."""
    schema = _schema(StructField("objectid", kind))
    rows = [(v, _g(), "4326", "") for v in values]
    with pytest.raises((pyogrio.errors.DataLayerError, pyogrio.errors.FieldError)):
        _write_gdb(tmp_path / "oid.gdb", schema, [rows])
