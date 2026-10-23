"""Tests for the OpenFileGDB fragment-streaming commit path.

The vector writer's OpenFileGDB commit goes through ``_write_streaming``: partition
fragments are streamed record batch by record batch into one pyogrio ``write_arrow``
(bounded driver memory -- never the whole dataset in RAM at once).

Tests here verify:
  - The streaming invariant: no fragment is ever loaded as a whole table by the
    write loop (spy on ``feather.read_table``), exercised as a UNIT TEST that calls
    ``_write_streaming`` directly so the monkeypatch runs in the same process.
  - Round-trip correctness: all rows, geometry values, and CRS survive a
    multi-fragment write and read-back cycle (exercised end-to-end via Spark).
"""

import pyarrow as pa
import pyarrow.feather as feather
import pyogrio
from pyspark.sql import functions as F
from shapely import Polygon as _Polygon
from shapely import from_wkb as _from_wkb
from shapely import to_wkb

from databricks.labs.gbx.ds.register import register

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _box(x, y):
    """Return a WKB Polygon box with lower-left corner (x, y), size 1x1."""
    return bytearray(
        to_wkb(_Polygon([(x, y), (x + 1, y), (x + 1, y + 1), (x, y + 1), (x, y)]))
    )


def _make_fragment(tmp_path, idx, n_rows, geom_col="geom_0"):
    """Write one Arrow-IPC fragment file (the format produced by VectorGbxWriter.write()).

    Schema mirrors what the writer produces: binary geometry, string srid, string proj,
    and a string attribute column.
    """
    srid_col = f"{geom_col}_srid"
    proj_col = f"{geom_col}_srid_proj"
    schema = pa.schema(
        [
            pa.field("name", pa.string()),
            pa.field(geom_col, pa.binary()),
            pa.field(srid_col, pa.string()),
            pa.field(proj_col, pa.string()),
        ]
    )
    offset = idx * n_rows
    tbl = pa.table(
        {
            "name": [f"f{idx}r{r}" for r in range(n_rows)],
            geom_col: [bytes(_box(float(offset + r), 0.0)) for r in range(n_rows)],
            srid_col: ["4326"] * n_rows,
            proj_col: [""] * n_rows,
        },
        schema=schema,
    )
    path = str(tmp_path / f"frag{idx}.arrow")
    feather.write_feather(tbl, path)
    return path


def _minimal_writer(out_path, geom_col="geom_0"):
    """Return a VectorGbxWriter instance with only the attributes required by
    ``_write_streaming``, bypassing the full ``__init__`` that needs Spark."""
    from databricks.labs.gbx.ds.vector import VectorGbxWriter

    w = object.__new__(VectorGbxWriter)
    w.geom_col = geom_col
    w.srid_col = f"{geom_col}_srid"
    w.proj_col = f"{geom_col}_srid_proj"
    w.layer_name = None
    w.driver = "OpenFileGDB"
    w.path = out_path
    return w


# ---------------------------------------------------------------------------
# Unit test: the write loop never loads a whole fragment as a table
# ---------------------------------------------------------------------------


def test_write_streaming_never_loads_a_whole_fragment(tmp_path, monkeypatch):
    """Unit test for ``_write_streaming`` on OpenFileGDB: calls the method directly
    (same process, so the monkeypatch intercepts ``feather.read_table``).

    The fragments are memory-mapped and handed to ONE ``write_arrow`` as a stream
    of record batches; a path that loaded each fragment as a table (let alone all
    of them) would call ``feather.read_table`` for them.
    """
    n_frags = 3
    frags_dir = tmp_path / "frags"
    frags_dir.mkdir()
    frags = [_make_fragment(frags_dir, i, n_rows=4) for i in range(n_frags)]
    out = str(tmp_path / "out.gdb")

    read_calls: list = []
    real_read = feather.read_table

    def spy_read(path, *args, **kwargs):
        read_calls.append(path)
        return real_read(path, *args, **kwargs)

    monkeypatch.setattr(feather, "read_table", spy_read)

    w = _minimal_writer(out)
    w._write_streaming(frags, out, "Polygon", "EPSG:4326")

    assert not read_calls, f"fragments were loaded as whole tables: {read_calls}"
    assert (tmp_path / "out.gdb").exists(), "output .gdb directory not created"
    assert pyogrio.read_info(out)["features"] == 4 * n_frags


# ---------------------------------------------------------------------------
# Round-trip test via Spark: rows, geometries, and CRS survive multi-fragment write
# ---------------------------------------------------------------------------


def _gdb_df(spark, n_rows=12, n_partitions=3):
    """Multi-partition DataFrame with polygon geometry and EPSG:4326 CRS."""
    rows = [
        (f"feat_{i}", i * 10, _box(float(i), 0.0), "4326", "") for i in range(n_rows)
    ]
    return spark.createDataFrame(
        rows,
        schema=(
            "name string, pop int, geom_0 binary, "
            "geom_0_srid string, geom_0_srid_proj string"
        ),
    ).repartition(n_partitions, F.col("name"))


def test_gdb_multi_fragment_roundtrip_rows_geoms_crs(spark, tmp_path):
    """Write a multi-partition FileGDB then read it back; assert all rows,
    attribute values, geometry values, and the EPSG:4326 CRS survive intact."""
    register(spark)

    n = 12
    out = str(tmp_path / "rt.gdb")
    _gdb_df(spark, n_rows=n, n_partitions=3).write.format("file_gdb_gbx").mode(
        "overwrite"
    ).save(out)

    back = spark.read.format("file_gdb_gbx").load(out)
    rows_back = back.collect()

    # Row count
    assert len(rows_back) == n, f"expected {n} rows, got {len(rows_back)}"

    # Attribute integrity
    names_back = {r["name"] for r in rows_back}
    assert names_back == {
        f"feat_{i}" for i in range(n)
    }, f"name set mismatch: {names_back}"
    pop_map = {r["name"]: r["pop"] for r in rows_back}
    for i in range(n):
        assert (
            pop_map[f"feat_{i}"] == i * 10
        ), f"pop mismatch for feat_{i}: {pop_map[f'feat_{i}']}"

    # Geometry type and non-null (OpenFileGDB may promote Polygon→MultiPolygon)
    gcol = next(
        f.name
        for f in back.schema.fields
        if f.name not in ("name", "pop")
        and not f.name.endswith(("_srid", "_srid_proj"))
    )
    geom_types = {
        _from_wkb(bytes(r[gcol])).geom_type for r in rows_back if r[gcol] is not None
    }
    assert geom_types <= {
        "Polygon",
        "MultiPolygon",
    }, f"unexpected geometry types after round-trip: {geom_types}"
    assert len(geom_types) > 0, "all geometries were null after round-trip"

    # CRS: the _srid column must round-trip as "4326"
    srid_col = gcol + "_srid"
    srid_vals = {r[srid_col] for r in rows_back}
    assert srid_vals == {
        "4326"
    }, f"CRS (_srid) mismatch after round-trip; expected {{'4326'}}, got {srid_vals}"
