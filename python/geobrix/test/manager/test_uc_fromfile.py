"""The Unity Catalog form of gbx_rst_fromfile: a SQL body that fetches the head of a FILE, asks a private Python planner
for the COG block ranges a window needs, fetches them, and asks a private Python reader for the tile.

FILE exists only on Databricks, so these host tests stand SQL's ranged FILE casts in with slices of the bytes
(``_simulate``), and a local Spark runs the body with a FILE stand-in. The workspace rows are in the oracle table.
"""

import base64
import re
from test._cog_fixture import cog_bytes as _cog_bytes

import numpy as np
import pytest
import shapely
import shapely.geometry as sg
import shapely.ops
from pyproj import Transformer
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx import manifest, uc
from databricks.labs.gbx.pyrx.core import cog_ranges as cr
from databricks.labs.gbx.uc import sqlbody, tier_b

FN = "gbx_rst_fromfile"
PRIVATE = ("gbx__fromfile_plan", "gbx__fromfile_read")
POLY = sg.box(405000, 3990000, 412000, 3996000)


class _Geom:  # what a UC Python function receives for GEOMETRY(ANY) (see uc/handlers._geom_bytes)
    def __init__(self, g, srid):
        self._g, self._s = g, srid

    def getBytes(self):  # noqa: N802 - the pyspark name
        return self._g.wkb

    def getSrid(self):  # noqa: N802
        return self._s


def _simulate(blob, geom=None, srid=32614, resolution=None, clip_crs=None):
    head = blob[: cr.HEAD_BYTES]
    g = None if geom is None else _Geom(geom, srid)
    plan = tier_b.fromfile_plan(head, len(blob), g, clip_crs, resolution)
    if plan is None or plan["empty"]:
        return None
    # what SQL's ranged FILE casts return
    chunks = [blob[o : o + n] for o, n in zip(plan["offs"], plan["lens"])]
    return tier_b.fromfile_read(
        head, len(blob), plan["offs"], chunks, g, clip_crs, plan["level"], "GTiff"
    )


def _pixels(tile):
    with MemoryFile(bytes(tile["raster"])) as mf, mf.open() as ds:
        return ds.read(), ds.transform, ds.crs


def _python_level(tmp_path, blob, **kw):
    from databricks.labs.gbx.pyrx.functions import _fromfile_impl

    p = tmp_path / "s.tif"
    p.write_bytes(blob)
    return _fromfile_impl(str(p), "GTiff", True, **kw)


# ---- the two private routines, against the Python level ----------------------------------------------------------


def test_uc_path_equals_python_level(tmp_path):
    blob = _cog_bytes()
    assert (
        len(blob) > cr.HEAD_BYTES
    )  # the window's blocks lie past the head: they are fetched
    py = _python_level(tmp_path, blob, geom=POLY.wkb, clip_crs="EPSG:32614")
    a, b = _pixels(py), _pixels(_simulate(blob, POLY))
    assert np.array_equal(a[0], b[0]) and a[1] == b[1] and a[2] == b[2]


def test_uc_resolution_equals_python_level(tmp_path):
    blob = _cog_bytes()
    py = _python_level(tmp_path, blob, resolution=40.0)
    ucr = _simulate(blob, None, resolution=40.0)
    a, b = _pixels(py), _pixels(ucr)
    assert np.array_equal(a[0], b[0]) and a[1] == b[1]
    assert a[0].shape[1:] == (1500 // 4, 2000 // 4)  # the 4x overview


def test_uc_clip_crs_names_the_crs_of_a_geometry_with_no_srid(tmp_path):
    blob = _cog_bytes()
    to_lonlat = Transformer.from_crs(32614, 4326, always_xy=True)
    lonlat = shapely.ops.transform(to_lonlat.transform, POLY)
    py = _python_level(tmp_path, blob, geom=lonlat.wkb, clip_crs="EPSG:4326")
    ucr = _simulate(blob, lonlat, srid=0, clip_crs="EPSG:4326")
    assert np.array_equal(_pixels(py)[0], _pixels(ucr)[0])


def test_uc_outside_is_null():
    assert _simulate(_cog_bytes(), sg.box(0, 0, 1, 1)) is None


def test_uc_outside_fetches_nothing():
    blob = _cog_bytes()
    plan = tier_b.fromfile_plan(
        blob[: cr.HEAD_BYTES], len(blob), _Geom(sg.box(0, 0, 1, 1), 32614), None, None
    )
    assert plan["empty"] and plan["offs"] == [] and plan["lens"] == []


def test_uc_a_file_inside_the_head_fetches_no_more_ranges():
    blob = _cog_bytes(width=600, height=600, count=1)
    assert len(blob) < cr.HEAD_BYTES
    plan = tier_b.fromfile_plan(blob, len(blob), None, None, None)
    assert plan["offs"] == [] and not plan["empty"]
    with MemoryFile(blob) as mf, mf.open() as ds:
        full = ds.read()
    assert np.array_equal(_pixels(_simulate(blob))[0], full)


def test_uc_null_in_is_null_out():
    assert tier_b.fromfile_plan(None, 10, None, None, None) is None
    assert tier_b.fromfile_read(None, 10, [], [], None, None, -1, "GTiff") is None


def test_uc_cap_refused_before_any_range_is_fetched():
    blob = _cog_bytes(
        width=3000, height=3000, count=1, dtype="float64"
    )  # 72 MB decoded, over 64 MiB
    with pytest.raises(Exception, match="Fix:"):
        tier_b.fromfile_plan(blob[: cr.HEAD_BYTES], len(blob), None, None, None)
    with pytest.raises(Exception, match="Fix:"):
        _simulate(blob, None)


def test_uc_striped_refused():
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=600,
            height=600,
            count=1,
            dtype="uint8",
            crs="EPSG:4326",
            transform=from_origin(0, 10, 0.01, 0.01),
        ) as ds:  # not tiled: strips
            ds.write(np.ones((1, 600, 600), "uint8"))
        blob = mf.read()
    with pytest.raises(ValueError, match=f"{FN}: .*Fix:"):
        tier_b.fromfile_plan(blob, len(blob), None, None, None)


def test_uc_header_past_the_head_refused():
    blob = _cog_bytes()
    with pytest.raises(ValueError, match=f"{FN}: .*Fix:"):
        tier_b.fromfile_plan(blob[:200], len(blob), _Geom(POLY, 32614), None, None)


def test_uc_bytes_that_are_no_raster_are_null():
    assert tier_b.fromfile_plan(b"not a tiff" * 10, 100, None, None, None) is None


def test_uc_a_driver_that_is_no_raster_driver_raises():
    blob = _cog_bytes(width=600, height=600, count=1)
    with pytest.raises(ValueError, match="Fix:"):
        tier_b.fromfile_read(blob, len(blob), [], [], None, None, -1, "GeoJSON")


def test_uc_a_range_the_plan_did_not_fetch_is_refused_not_zeros():
    blob = _cog_bytes()
    head = blob[: cr.HEAD_BYTES]
    plan = tier_b.fromfile_plan(head, len(blob), _Geom(POLY, 32614), None, None)
    assert plan["offs"]
    with pytest.raises(ValueError, match="Fix:"):
        tier_b.fromfile_read(
            head, len(blob), [], [], _Geom(POLY, 32614), None, plan["level"], "GTiff"
        )


def test_the_private_routines_are_manifest_rows_with_their_handlers():
    from databricks.labs.gbx.uc import forms

    man = manifest.load()
    assert tuple(man.functions[FN].levels["L4"]["private"]) == PRIVATE
    for name in PRIVATE:
        assert manifest.resolve_ref(man.private[name].handler) is forms.HANDLERS[name]


# ---- the SQL body ------------------------------------------------------------------------------------------------


def _install_text():
    return uc.install("cat", "sch", "/Volumes/cat/sch/vol/uc", dry_run=True, only=[FN])


def test_ddl_body_shape():
    stmts = _install_text()
    text = "\n".join(stmts)
    assert (
        "LANGUAGE SQL" in text
        and "AS FILE) AS BINARY" in text
        and "gbx__fromfile_read(" in text
    )
    public = stmts[-1]
    assert public.startswith(f"CREATE OR REPLACE FUNCTION `cat`.`sch`.{FN}(path FILE, ")
    assert "LANGUAGE SQL\nDETERMINISTIC" in public and "PYTHON" not in public
    # the head is fetched once, and it is the size the planner reads
    assert text.count("'offset', CAST(0 AS BIGINT)") == 1
    assert f"CAST({cr.HEAD_BYTES} AS BIGINT)" in text
    assert ";" not in text.replace(
        ";\n", ""
    )  # no statement separator inside a function's text


def _lambda_bodies(text):
    """The text after ``->`` of each lambda in ``text``, up to the end of the call that holds it."""
    out = []
    for m in re.finditer(r"\b(transform|filter|aggregate|exists|forall)\(", text):
        args, _ = sqlbody._call_end(text, m.end() - 1)
        out += [a.split("->", 1)[1] for a in args if "->" in a]
    return out


def test_no_python_routine_runs_inside_a_lambda():
    # Spark refuses a Python UDF anywhere in a lambda body (UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF), so
    # the private routines are called only where a SQL function's argument is computed
    bodies = [b for s in _install_text() for b in _lambda_bodies(s)]
    assert bodies  # the range fetch is a lambda
    for b in bodies:
        assert not any(p in b for p in PRIVATE), b


def test_install_order_privates_then_helpers_then_the_function():
    stmts = _install_text()
    made = [re.search(r"FUNCTION `cat`.`sch`.(\w+)\(", s).group(1) for s in stmts]
    assert made[-1] == FN
    for p in PRIVATE:
        assert made.count(p) == 1
    first_helper = min(
        made.index(h["name"])
        for h in sqlbody.l4_source(manifest.load().functions[FN])[1]
    )
    assert max(made.index(p) for p in PRIVATE) < first_helper


# ---- the body on a local Spark, with stand-ins for FILE, GEOMETRY and the Python routines -------------------------

_FILE_T = "STRUCT<uri:STRING, size:BIGINT, content_type:STRING, checksum:STRING>"
# CAST(CAST(named_struct('uri', U, 'offset', O, 'size', N, ...) AS FILE) AS BINARY) -> bytes O..O+N of the stand-in
_RANGE = re.compile(
    r"CAST\(CAST\(named_struct\('uri', (.+?), 'offset', (.+?), 'size', (.+?), "
    r"'content_type', .+?, 'checksum', [^)]+\) AS FILE\) AS BINARY\)"
)


def _local(text):
    """``text`` for a local Spark: a FILE is a struct whose uri holds the file's bytes in base64, a range of it is a
    substring, a GEOMETRY is EWKB."""
    text = _RANGE.sub(
        r"substring(unbase64(\1), CAST(\2 AS INT) + 1, CAST(\3 AS INT))", text
    )
    return (
        text.replace("GEOMETRY(ANY)", "BINARY")
        .replace(" FILE", f" {_FILE_T}")
        .replace("(FILE", f"({_FILE_T}")
    )


@pytest.fixture
def local_fromfile(spark):
    from databricks.labs.gbx.manifest import TILE_SQL
    from databricks.labs.gbx.uc.ddl import sql_type

    man = manifest.load()
    fn = man.functions[FN]
    body, helpers = sqlbody.l4_source(fn)
    plan_t = sql_type(man.private["gbx__fromfile_plan"].returns)

    # defined here, so the worker gets them by value (the test module is not importable there)
    def geometry(ewkb):
        import shapely

        from databricks.labs.gbx.bench.oracles import UcGeometry

        if ewkb is None:
            return None
        return UcGeometry(bytes(ewkb), shapely.get_srid(shapely.from_wkb(bytes(ewkb))))

    def plan(h, n, g, c, r):
        from databricks.labs.gbx.uc import tier_b

        return tier_b.fromfile_plan(h, n, geometry(g), c, r)

    def read(h, n, o, ch, g, c, lv, d):
        from databricks.labs.gbx.uc import tier_b

        return tier_b.fromfile_read(h, n, o, ch, geometry(g), c, lv, d)

    spark.udf.register("gbx__fromfile_plan", plan, plan_t)
    spark.udf.register("gbx__fromfile_read", read, TILE_SQL)
    made = list(PRIVATE)
    name = "t5_local_fromfile"
    try:
        for h in helpers:
            sig = ", ".join(f"{n} {sql_type(t)}" for n, t in h["params"])
            spark.sql(
                _local(
                    f"CREATE OR REPLACE TEMPORARY FUNCTION {h['name']}({sig}) RETURNS {sql_type(h['returns'])} "
                    f"RETURN {sqlbody.render(h['sql'], '')}"
                )
            )
            made.append(h["name"])
        from databricks.labs.gbx.uc.ddl import _params_sig

        spark.sql(
            _local(
                f"CREATE OR REPLACE TEMPORARY FUNCTION {name}({_params_sig(fn)}) "
                f"RETURNS {sql_type(fn.returns_uc)} RETURN {sqlbody.render(body, '')}"
            )
        )
        made.append(name)
        yield name
    finally:
        for n in made:
            spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")


def test_the_body_runs_on_a_local_spark_with_stand_ins(spark, local_fromfile):
    blob = _cog_bytes(width=1100, height=900, count=1)
    poly = shapely.set_srid(sg.box(402000, 3993000, 409000, 3998000), 32614)
    rows = [
        (
            base64.b64encode(blob).decode(),
            len(blob),
            shapely.to_wkb(poly, include_srid=True),
        ),
        (base64.b64encode(blob).decode(), len(blob), None),
        (None, None, None),
    ]
    spark.createDataFrame(rows, "b STRING, n BIGINT, g BINARY").createOrReplaceTempView(
        "t5_files"
    )
    got = spark.sql(
        f"SELECT {local_fromfile}(named_struct('uri', b, 'size', n, 'content_type', 'image/tiff', 'checksum', 'x'), "
        "'GTiff', g, NULL, NULL) AS t, "
        f"{local_fromfile}(named_struct('uri', b, 'size', n, 'content_type', 'image/tiff', 'checksum', 'x'), "
        "'GTiff', g, NULL, 40.0) AS coarse FROM t5_files"
    ).collect()
    clip, whole, none = got
    want = _simulate(blob, poly, srid=32614)
    assert np.array_equal(_pixels(clip["t"].asDict())[0], _pixels(want)[0])
    with MemoryFile(blob) as mf, mf.open() as ds:
        full = ds.read()
    assert np.array_equal(_pixels(whole["t"].asDict())[0], full)
    assert _pixels(whole["coarse"].asDict())[0].shape[1:] == (900 // 4, 1100 // 4)
    assert none["t"] is None
