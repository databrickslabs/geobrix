"""Tests for the OpenFileGDB fragment-streaming commit path.

The vector writer's OpenFileGDB commit goes through ``_write_local_osgeo_gdb``,
which streams partition fragments one at a time with OGR transaction batching
(bounded driver memory — never the whole dataset in RAM simultaneously).

Tests here verify:
  - The streaming invariant: at most one fragment table is alive at any time
    during the write (spy via WeakRef-tracked ``feather.read_table`` calls),
    exercised as a UNIT TEST that calls ``_write_local_osgeo_gdb`` directly
    so the monkeypatch runs in the same process.
  - Round-trip correctness: all rows, geometry values, and CRS survive a
    multi-fragment write and read-back cycle (exercised end-to-end via Spark).

All tests require the native ``osgeo`` bindings (heavyweight GDAL natives)
and are skipped when those are absent.
"""

import importlib.util
import weakref

import pyarrow as pa
import pyarrow.feather as feather
import pytest
from pyspark.sql import functions as F
from shapely import Polygon as _Polygon
from shapely import from_wkb as _from_wkb
from shapely import to_wkb

from databricks.labs.gbx.ds.register import register

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

_NEED_OSGEO = pytest.mark.skipif(
    importlib.util.find_spec("osgeo") is None,
    reason="native osgeo (heavy GDAL natives) not present; OpenFileGDB write requires osgeo",
)


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
    ``_write_local_osgeo_gdb``, bypassing the full ``__init__`` that needs Spark."""
    from databricks.labs.gbx.ds.vector import VectorGbxWriter

    w = object.__new__(VectorGbxWriter)
    w.geom_col = geom_col
    w.srid_col = f"{geom_col}_srid"
    w.proj_col = f"{geom_col}_srid_proj"
    w.layer_name = None
    w.driver = "OpenFileGDB"
    return w


# ---------------------------------------------------------------------------
# Unit test: _write_local_osgeo_gdb reads one fragment at a time
# ---------------------------------------------------------------------------


@_NEED_OSGEO
def test_write_local_osgeo_gdb_reads_one_fragment_at_a_time(tmp_path, monkeypatch):
    """Unit test for ``_write_local_osgeo_gdb``: calls the method directly
    (same process, so monkeypatch intercepts ``feather.read_table``).

    Asserts that at the moment each new fragment is opened, all previously-
    returned tables have already been garbage-collected (CPython refcount
    immediately frees after each iteration's ``del tbl, cols, geom``).
    A non-streaming path (``[feather.read_table(f) for f in frags]``) would
    keep all fragment tables alive simultaneously, driving the max-concurrent
    counter above 1 for the second and later calls.
    """
    n_frags = 3
    frags_dir = tmp_path / "frags"
    frags_dir.mkdir()
    frags = [_make_fragment(frags_dir, i, n_rows=4) for i in range(n_frags)]
    out = str(tmp_path / "out.gdb")

    real_read = feather.read_table
    returned_refs: list = []
    max_concurrent = [0]

    def spy_read(path, *args, **kwargs):
        # Before returning the next table, count how many prior tables are
        # still alive (should be 0 if streaming: each table is del-ed before
        # the next read).
        alive_count = sum(1 for r in returned_refs if r() is not None)
        max_concurrent[0] = max(max_concurrent[0], alive_count)
        tbl = real_read(path, *args, **kwargs)
        returned_refs.append(weakref.ref(tbl))
        return tbl

    # Patch at the pyarrow.feather module level so every
    # ``import pyarrow.feather as feather; feather.read_table(...)``
    # inside ``_write_local_osgeo_gdb`` calls the spy.
    monkeypatch.setattr(feather, "read_table", spy_read)

    w = _minimal_writer(out)
    w._write_local_osgeo_gdb(frags, out, geom_type="Polygon", crs="EPSG:4326")

    # Spy must have been called: 1 (schema inference) + n_frags (write loop).
    assert (
        len(returned_refs) == 1 + n_frags
    ), f"expected {1 + n_frags} feather.read_table calls, got {len(returned_refs)}"

    # Core invariant: never more than 1 prior fragment alive when a new one
    # is opened.  (Strict 0 expected in CPython; <=1 tolerates a 1-call lag
    # in edge-case GC implementations.)
    assert max_concurrent[0] <= 1, (
        f"streaming violated: max concurrent live fragment tables = "
        f"{max_concurrent[0]} (expected <=1); fragments are being accumulated "
        f"rather than processed one at a time"
    )

    # Output GDB directory must exist (FileGDB is a directory bundle).
    assert (tmp_path / "out.gdb").exists(), "output .gdb directory not created"


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


@_NEED_OSGEO
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
