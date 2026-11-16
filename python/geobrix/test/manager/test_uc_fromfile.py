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


def _png():
    with MemoryFile() as mf:
        with mf.open(driver="PNG", width=8, height=8, count=1, dtype="uint8") as ds:
            ds.write(np.ones((1, 8, 8), "uint8"))
        return mf.read()


@pytest.mark.parametrize(
    "blob", [_png(), bytes(range(256)) * 4], ids=["png", "random-bytes"]
)
def test_uc_a_file_that_is_no_tiff_is_refused_with_the_session_route(blob):
    with pytest.raises(ValueError, match="tiled GeoTIFF/COG files only. Fix: "):
        tier_b.fromfile_plan(blob, len(blob), None, None, None)


def test_uc_a_corrupt_tiff_is_null():
    blob = b"II*\x00" + b"\x08\x00\x00\x00" + b"\xff" * 200
    assert tier_b.fromfile_plan(blob, len(blob), None, None, None) is None


# ---- a header is data: the ranges it names are bounded ----------------------------------------------------------


def _planned(monkeypatch, ranges):
    """The planner over a real COG head whose block table names ``ranges`` (a crafted header)."""
    blob = _cog_bytes(width=600, height=600, count=1)
    monkeypatch.setattr(
        cr,
        "plan_ranges",
        lambda *a, **k: cr.Plan(None, (0, 0, 600, 600), tuple(ranges)),
    )
    return blob


@pytest.mark.parametrize(
    "ranges",
    [[(10**9, 100)], [(-5, 100)], [(100, 0)], [(100, -3)]],
    ids=["past-the-end", "negative-offset", "zero-size", "negative-size"],
)
def test_uc_a_block_range_outside_the_file_is_refused(monkeypatch, ranges):
    blob = _planned(monkeypatch, ranges)
    with pytest.raises(ValueError, match="block table names bytes .* Fix: "):
        tier_b.fromfile_plan(blob, len(blob), None, None, None)


def test_uc_block_ranges_over_the_fetch_cap_are_refused(monkeypatch):
    blob = _planned(monkeypatch, [(100, 600), (800, 600)])
    monkeypatch.setattr(tier_b, "FETCH_CAP", 1000)
    with pytest.raises(ValueError, match="over the .* Fix: "):
        tier_b.fromfile_plan(blob, len(blob), None, None, None)


def test_uc_the_reader_bounds_what_sql_passed_in(monkeypatch):
    blob = _cog_bytes(width=600, height=600, count=1)
    monkeypatch.setattr(tier_b, "FETCH_CAP", 1000)
    read = tier_b.fromfile_read
    with pytest.raises(ValueError, match="over the .* Fix: "):
        read(
            blob, len(blob), [0, 700], [b"x" * 600, b"x" * 600], None, None, -1, "GTiff"
        )
    with pytest.raises(ValueError, match="block table names bytes .* Fix: "):
        read(blob, len(blob), [len(blob) - 2], [b"xyz"], None, None, -1, "GTiff")
    assert tier_b.FETCH_CAP < len(
        blob
    )  # the cap is on the fetched ranges, not the head


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
    assert text.count("'size', least(path.size") == 1
    # a FILE that is a slice of its object (an offset) is read from its own first byte
    assert text.count("'offset', coalesce(path.offset, CAST(0 AS BIGINT))") == 2
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

_FILE_T = "STRUCT<uri:STRING, offset:BIGINT, size:BIGINT, content_type:STRING, checksum:STRING>"
# CAST(CAST(named_struct('uri', U, 'offset', O, 'size', N, ...) AS FILE) AS BINARY): bytes O..O+N of the object
_CAST = r"CAST\(CAST\(named_struct\('uri', (.+?), 'offset', (.+?), 'size', ({size}), 'content_type', .+?, 'checksum', [^)]+\) AS FILE\) AS BINARY\)"
_HEAD = re.compile(_CAST.format(size=r"least\(path\.size, CAST\(\d+ AS BIGINT\)\)"))
_RANGE = re.compile(_CAST.format(size=r".+?"))


def _local(text):
    """``text`` for a local Spark: a FILE is a struct whose uri holds its object's bytes in base64, a range of it is a
    substring (the head through ``t5_head``, which counts its calls), a GEOMETRY is EWKB.
    """
    text = _HEAD.sub(r"t5_head(\1, \2, \3)", text)
    text = _RANGE.sub(
        r"substring(unbase64(\1), CAST(\2 AS INT) + 1, CAST(\3 AS INT))", text
    )
    return (
        text.replace("GEOMETRY(ANY)", "BINARY")
        .replace(" FILE", f" {_FILE_T}")
        .replace("(FILE", f"({_FILE_T}")
    )


@pytest.fixture
def local_fromfile(spark, tmp_path):
    """``(name, calls)``: the body as temporary functions, and a function that counts the head fetches and plan calls."""
    from databricks.labs.gbx.manifest import TILE_SQL
    from databricks.labs.gbx.uc.ddl import _params_sig, sql_type

    man = manifest.load()
    fn = man.functions[FN]
    body, helpers = sqlbody.l4_source(fn)
    plan_t = sql_type(man.private["gbx__fromfile_plan"].returns)
    log = str(tmp_path / "calls.txt")

    # defined here, so the worker gets them by value (the test module is not importable there)
    def note(what):
        with open(log, "a") as fh:
            fh.write(what + "\n")

    def geometry(ewkb):
        import shapely

        from databricks.labs.gbx.bench.oracles import UcGeometry

        if ewkb is None:
            return None
        return UcGeometry(bytes(ewkb), shapely.get_srid(shapely.from_wkb(bytes(ewkb))))

    def head(uri, off, n):
        import base64

        if uri is None:
            return None
        note("head")
        return base64.b64decode(uri)[off : off + n]

    def plan(h, n, g, c, r):
        from databricks.labs.gbx.uc import tier_b

        if h is not None:
            note("plan")
        return tier_b.fromfile_plan(h, n, geometry(g), c, r)

    def read(h, n, o, ch, g, c, lv, d):
        from databricks.labs.gbx.uc import tier_b

        return tier_b.fromfile_read(h, n, o, ch, geometry(g), c, lv, d)

    def calls():
        try:
            lines = open(log).read().split()
        except FileNotFoundError:
            return {"head": 0, "plan": 0}
        return {k: lines.count(k) for k in ("head", "plan")}

    spark.udf.register("t5_head", head, "BINARY")
    spark.udf.register("gbx__fromfile_plan", plan, plan_t)
    spark.udf.register("gbx__fromfile_read", read, TILE_SQL)
    made = ["t5_head", *PRIVATE]
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
        spark.sql(
            _local(
                f"CREATE OR REPLACE TEMPORARY FUNCTION {name}({_params_sig(fn)}) "
                f"RETURNS {sql_type(fn.returns_uc)} RETURN {sqlbody.render(body, '')}"
            )
        )
        made.append(name)
        yield name, calls
    finally:
        for n in made:
            spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {n}")


def _ewkb(g, srid=32614):
    return shapely.to_wkb(shapely.set_srid(g, srid), include_srid=True)


def _run(spark, name, rows, resolution="NULL"):
    """``name(FILE, 'GTiff', g, NULL, resolution)`` over rows ``(object bytes or None, offset, size, g)``."""
    data = [
        (None if b is None else base64.b64encode(b).decode(), off, n, g)
        for b, off, n, g in rows
    ]
    spark.createDataFrame(
        data, "b STRING, o BIGINT, n BIGINT, g BINARY"
    ).createOrReplaceTempView("t5_files")
    return [
        r["t"]
        for r in spark.sql(
            f"SELECT {name}(named_struct('uri', b, 'offset', o, 'size', n, 'content_type', 'image/tiff', "
            f"'checksum', 'x'), 'GTiff', g, NULL, {resolution}) AS t FROM t5_files"
        ).collect()
    ]


def test_the_body_runs_on_a_local_spark_with_stand_ins(spark, local_fromfile):
    name, _ = local_fromfile
    blob = _cog_bytes(width=1100, height=900, count=1)
    poly = sg.box(402000, 3993000, 409000, 3998000)
    clip, whole, none = _run(
        spark,
        name,
        [(blob, None, len(blob), _ewkb(poly)), (blob, None, len(blob), None)]
        + [(None, None, None, None)],
    )
    want = _simulate(blob, poly, srid=32614)
    assert np.array_equal(_pixels(clip.asDict())[0], _pixels(want)[0])
    with MemoryFile(blob) as mf, mf.open() as ds:
        full = ds.read()
    assert np.array_equal(_pixels(whole.asDict())[0], full)
    assert none is None
    (coarse,) = _run(spark, name, [(blob, None, len(blob), None)], resolution="40.0")
    assert _pixels(coarse.asDict())[0].shape[1:] == (900 // 4, 1100 // 4)


def test_the_body_fetches_ranges_past_the_head_on_a_local_spark(
    spark, local_fromfile, tmp_path
):
    name, calls = local_fromfile
    blob = _cog_bytes(
        count=1
    )  # 7 MB: over the head, small enough for the stand-in's base64 FILE
    assert len(blob) > cr.HEAD_BYTES
    plan = tier_b.fromfile_plan(
        blob[: cr.HEAD_BYTES], len(blob), _Geom(POLY, 32614), None, None
    )
    assert plan[
        "offs"
    ]  # the window's blocks lie past the head: the lambda fetches them
    clip, outside = _run(
        spark,
        name,
        [
            (blob, None, len(blob), _ewkb(POLY)),
            (blob, None, len(blob), _ewkb(sg.box(0, 0, 1, 1))),
        ],
    )
    a = _pixels(clip.asDict())
    for other in (
        _simulate(blob, POLY),
        _python_level(tmp_path, blob, geom=POLY.wkb, clip_crs="EPSG:32614"),
    ):
        b = _pixels(other)
        assert np.array_equal(a[0], b[0]) and a[1] == b[1] and a[2] == b[2]
    assert outside is None  # a geom that misses the raster: NULL, nothing raised
    # each row fetches its head once and plans once (Spark binds a SQL function's inputs once)
    assert calls() == {"head": 2, "plan": 2}


def test_a_file_that_is_a_slice_of_its_object_reads_as_that_file(spark, local_fromfile):
    # larger than the head, so both the head and the planned ranges are read relative to the slice
    name, _ = local_fromfile
    blob = _cog_bytes(count=1)
    before, after = b"\x00" * 12345, b"\xff" * 999
    obj = before + blob + after
    sliced, own = _run(
        spark,
        name,
        [
            (obj, len(before), len(blob), _ewkb(POLY)),
            (blob, None, len(blob), _ewkb(POLY)),
        ],
    )
    a, b = _pixels(sliced.asDict()), _pixels(own.asDict())
    assert np.array_equal(a[0], b[0]) and a[1] == b[1]
