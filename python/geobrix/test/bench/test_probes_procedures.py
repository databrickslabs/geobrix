"""bench.probes.run_procedures: the cases, the CALL text and the progress lines, on a session that records its SQL."""

import re

import pytest

from databricks.labs.gbx.bench import probes
from databricks.labs.gbx.uc import procedures


class _Result:
    def collect(self):
        return []

    def first(self):
        return None


class _Spark:
    def __init__(self):
        self.stmts = []

    def sql(self, text):
        self.stmts.append(text)
        return _Result()

    def createDataFrame(self, *_a, **_k):  # noqa: N802 - the Spark API name
        raise RuntimeError("no Spark in this test")


def _params(ddl_text):
    """(name, has_default) of each IN parameter of a procedure's DDL."""
    head = ddl_text.split("\n)\nLANGUAGE", 1)[0]
    return [
        (m.group(1), " DEFAULT " in m.group(0))
        for m in re.finditer(r"^  IN (\w+) [^\n]*$", head, re.M)
    ]


def test_each_case_calls_its_procedure_with_the_names_it_declares():
    cases = probes.proc_cases("c", "s")
    assert cases
    for case in cases:
        declared = _params(procedures.RECIPES[case["name"]]("c", "s"))
        assert set(case["args"]) <= {n for n, _ in declared}, case["name"]
        assert {n for n, d in declared if not d} <= set(case["args"]), case["name"]
        assert case["compare"] in ("rows", "pixels", "mvt")


def test_the_probe_covers_every_recipe_that_takes_a_table(monkeypatch):
    import inspect

    monkeypatch.setattr(probes, "_proc_tile_cases", lambda *_a: [])
    covered = {c["name"] for c in probes.proc_cases("c", "s")}
    assert covered == {
        "gbx_polygons_to_bng",
        "gbx_clean_geometries",
        "gbx_migrate_mosaic_geometry",
        "gbx_points_to_h3_surface",
        "gbx_features_to_mvt",
    }
    # building the cases takes no session, so it can send nothing
    assert list(inspect.signature(probes.proc_cases).parameters) == [
        "catalog",
        "schema",
    ]


def test_run_procedures_calls_each_procedure_and_prints_its_progress(
    monkeypatch, capsys, tmp_path
):
    import databricks.labs.gbx as gbx

    monkeypatch.setattr(gbx, "register", lambda *_a, **_k: {})
    monkeypatch.setattr(probes, "_proc_tile_cases", lambda *_a: [])
    spark = _Spark()
    out = probes.run_procedures(spark, "c", "s", str(tmp_path))
    text = capsys.readouterr().out
    calls = [s for s in spark.stmts if s.startswith("CALL ")]
    assert any(
        s.startswith("CALL `c`.`s`.gbx_polygons_to_bng(source_table => '")
        for s in calls
    )
    assert set(out["recipes"]) >= {
        "gbx_polygons_to_bng",
        "gbx_clean_geometries",
        "zarr",
    }
    for name in ("gbx_polygons_to_bng", "gbx_features_to_mvt"):
        assert f"[PROC] {name} call START" in text
        assert re.search(rf"\[PROC\] {name} call DONE [0-9.]+s rows=0", text)
        assert re.search(rf"\[PROC\] {name} session DONE [0-9.]+s", text)
    # both sides are empty here: that proves nothing, so it is not a match
    rec = out["recipes"]["gbx_polygons_to_bng"]
    assert rec["status"] == "empty" and rec["match"] is False
    assert out["dropped"]  # the tables are dropped at the end, whatever happened
    assert any(
        s.startswith("DROP TABLE IF EXISTS `c`.`s`.gbxproc_") for s in spark.stmts
    )


def test_a_failed_call_is_recorded_and_the_run_goes_on(monkeypatch, capsys, tmp_path):
    import databricks.labs.gbx as gbx

    class Failing(_Spark):
        def sql(self, text):
            if text.startswith("CALL ") and "gbx_clean_geometries" in text:
                raise RuntimeError("boom")
            return super().sql(text)

    monkeypatch.setattr(gbx, "register", lambda *_a, **_k: {})
    monkeypatch.setattr(probes, "_proc_tile_cases", lambda *_a: [])
    out = probes.run_procedures(Failing(), "c", "s", str(tmp_path))
    assert "boom" in out["recipes"]["gbx_clean_geometries"]["error"]
    assert out["recipes"]["gbx_migrate_mosaic_geometry"]["status"] == "empty"
    assert "[PROC] gbx_clean_geometries call DONE" in capsys.readouterr().out


@pytest.mark.parametrize(
    "value,want",
    [
        (b"ab", "sha:fb8e20fc2e4c3f24"),
        (1.23456789012, 1.23456789),
        ([1.0, b""], [1.0, "sha:e3b0c44298fc1c14"]),
    ],
)
def test_cells_are_compared_by_digest_and_rounded_value(value, want):
    assert probes._proc_norm(value) == want


def test_warehouse_statements_call_each_procedure_then_diff_it_against_plain_sql():
    stmts = dict(probes.proc_warehouse_statements("c", "s", "/Volumes/c/s/v/scratch"))
    ids = list(stmts)
    for name in procedures.RECIPES:
        if name.startswith("gbx_zarr_"):
            continue
        # a procedure with several cases (combine by key, one per op) has the id `<name>:<op>:call`
        calls = [i for i in ids if i.startswith(name) and i.endswith(":call")]
        assert calls, name
        for call in calls:
            assert stmts[call].startswith(f"CALL `c`.`s`.{name}("), call
            assert ids.index(call) < ids.index(call.replace(":call", ":diff")), call
    # every table a case makes is dropped at the end, and the zarr manifest is one of them
    created = {
        m.group(1)
        for sql in stmts.values()
        for m in [
            re.match(r"CREATE (?:OR REPLACE )?TABLE `c`\.`s`\.(gbxproc_\w+)", sql)
        ]
        if m
    }
    dropped = {c.split(":", 1)[1] for c in ids if c.startswith("drop:")}
    assert created <= dropped and "gbxproc_zm_wh" in dropped
    # a statement is one line of SQL per cid: no semicolon, no blank line (gbx-uc-sql splits on the cid marks)
    assert all(";" not in s and "\n\n" not in s for s in stmts.values())
    assert (
        "FILE" not in stmts["zarr:create"]
    )  # the fuse manifest: a plain STRING file column


def test_install_procedures_replaces_only_the_procedures_before_the_calls(
    monkeypatch, capsys, tmp_path
):
    import databricks.labs.gbx as gbx

    monkeypatch.setattr(gbx, "register", lambda *_a, **_k: {})
    monkeypatch.setattr(probes, "_proc_tile_cases", lambda *_a: [])
    spark = _Spark()
    out = probes.run_procedures(spark, "c", "s", str(tmp_path), install_procedures=True)
    creates = [
        s
        for s in spark.stmts
        if s.startswith(("CREATE OR REPLACE PROCEDURE", "CREATE OR REPLACE FUNCTION"))
    ]
    assert creates and all(
        s.startswith("CREATE OR REPLACE PROCEDURE `c`.`s`.gbx_") for s in creates
    )  # no function is touched
    assert out["installed"] == len(creates)
    first_call = next(i for i, s in enumerate(spark.stmts) if s.startswith("CALL "))
    assert spark.stmts.index(creates[-1]) < first_call
    assert "[PROC] install procedures DONE" in capsys.readouterr().out


class _Row:
    def __init__(self, **kw):
        self._d = kw

    def asDict(self, recursive=False):  # noqa: N802 - the Spark Row API name
        return dict(self._d)


def test_the_compare_reads_ewkb_and_wkb_as_one_geometry_and_merges_mvt_per_tile():
    import mapbox_vector_tile as mvt
    import shapely

    g = shapely.from_wkt("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))")
    plain = shapely.to_wkb(g)
    ewkb = shapely.to_wkb(shapely.set_srid(g, 27700), include_srid=True)
    assert plain != ewkb
    case = {"compare": "rows", "geometries": ["chip"]}
    assert probes._proc_rows([_Row(chip=plain)], case) == probes._proc_rows(
        [_Row(chip=ewkb)], case
    )

    def tile(*points):
        return mvt.encode(
            [
                {
                    "name": "layer",
                    "features": [
                        {"geometry": f"POINT ({x} {y})", "properties": {"n": n}}
                        for n, (x, y) in enumerate(points)
                    ],
                }
            ]
        )

    case = {"compare": "mvt"}
    merged = [_Row(z=0, x=0, y=0, mvt=tile((1, 1), (2, 2)))]
    split = [
        _Row(z=0, x=0, y=0, mvt=tile((1, 1))),
        _Row(z=0, x=0, y=0, mvt=tile((2, 2))),
    ]
    got = probes._proc_rows(merged, case)
    assert len(got) == 1 and len(got[0]["mvt"]) == 2
    # one tile with both features equals two one-feature tiles of the same tile id (the property n restarts at 0)
    assert [len(r["mvt"]) for r in probes._proc_rows(split, case)] == [2]


class _Rows(_Spark):
    """A session whose output and reference queries of the polygons-to-BNG case return the rows it is given."""

    def __init__(self, out_rows, ref_rows):
        super().__init__()
        self._out, self._ref = out_rows, ref_rows

    def sql(self, text):
        self.stmts.append(text)
        rows = []
        if "FROM `c`.`s`.gbxproc_bng_tgt" in text:
            rows = self._out
        elif "LATERAL gbx_bng_tessellateexplode" in text:
            rows = self._ref
        return type(
            "R", (), {"collect": lambda self_: rows, "first": lambda self_: None}
        )()


def _bng_status(monkeypatch, tmp_path, out_rows, ref_rows):
    import databricks.labs.gbx as gbx

    monkeypatch.setattr(gbx, "register", lambda *_a, **_k: {})
    monkeypatch.setattr(probes, "_proc_tile_cases", lambda *_a: [])
    out = probes.run_procedures(_Rows(out_rows, ref_rows), "c", "s", str(tmp_path))
    return out["recipes"]["gbx_polygons_to_bng"]


def test_a_result_that_differs_from_the_session_flow_is_a_mismatch(
    monkeypatch, tmp_path
):
    got = [_Row(id=1, cellid=5, core=True, chip=None)]
    want = [_Row(id=1, cellid=6, core=True, chip=None)]
    rec = _bng_status(monkeypatch, tmp_path, got, want)
    assert rec["status"] == "mismatch" and rec["match"] is False
    assert (
        rec["diff"]["procedure"][0]["cellid"] == 5
        and rec["diff"]["session"][0]["cellid"] == 6
    )


def test_equal_rows_match_and_two_empty_results_do_not(monkeypatch, tmp_path):
    rows = [_Row(id=1, cellid=5, core=True, chip=None)]
    rec = _bng_status(monkeypatch, tmp_path, rows, list(rows))
    assert rec["status"] == "match" and rec["match"] is True and "diff" not in rec
    rec = _bng_status(monkeypatch, tmp_path, [], [])
    assert rec["status"] == "empty" and rec["match"] is False
    # the procedure wrote nothing where the session flow has rows: a mismatch, not an empty
    assert _bng_status(monkeypatch, tmp_path, [], rows)["status"] == "mismatch"


def _zarr_tag(monkeypatch, tmp_path, tagged, twin):
    from databricks.labs.gbx.zarr import _maint
    from databricks.labs.gbx.zarr import _manifest as M

    class Spark(_Spark):
        def sql(self, text):
            self.stmts.append(text)
            rows = []
            if text.startswith("SELECT kind, array, key, write_id FROM"):
                rows = tagged if "zm_proc" in text else twin
            return type(
                "R", (), {"collect": lambda self_: rows, "first": lambda self_: None}
            )()

    monkeypatch.setattr(M, "create_manifest", lambda *_a, **_k: None)
    monkeypatch.setattr(_maint, "gbx_zarr_tag", lambda *_a, **_k: None)

    def step(_label, _surface, fn):
        return fn(), None, 0.0

    return probes._proc_zarr(Spark(), "c", "s", str(tmp_path), step, [])


def test_the_zarr_tag_compare_needs_tagged_rows_on_both_sides(monkeypatch, tmp_path):
    row = ("group", None, ".", "w2")
    assert _zarr_tag(monkeypatch, tmp_path, [row], [row])["tag_status"] == "match"
    assert (
        _zarr_tag(monkeypatch, tmp_path, [row], [("array", "v", "v/.zarray", "w2")])[
            "tag_status"
        ]
        == "mismatch"
    )
    rec = _zarr_tag(monkeypatch, tmp_path, [], [])
    assert rec["tag_status"] == "empty" and rec["tag_match"] is False


def test_the_tile_cases_register_each_table_before_creating_it():
    # createDataFrame raises in this session: the first create fails, and the tables are already listed
    tables = []
    with pytest.raises(RuntimeError, match="no Spark"):
        probes._proc_tile_cases(_Spark(), "c", "s", tables)
    assert "`c`.`s`.gbxproc_mos_src" in tables and "`c`.`s`.gbxproc_zs_tgt" in tables


# --- the tree procedures, the combine and the H3 surface in the probe ----------------------------------------------


def _defs():
    q = "`c`.`s`"

    def t(name):
        return f"{q}.{probes.PROC_PREFIX}{name}"

    def lit(name):
        return "'" + t(name).replace("`", "") + "'"

    return probes._proc_tile_case_defs(t, lit)


def test_the_inputs_have_a_mosaic_key_past_one_function_call_and_one_past_two_levels():
    mosaic, _zone = probes._proc_tile_inputs()
    per_key = {}
    for key, _tile in mosaic:
        per_key[key] = per_key.get(key, 0) + 1
    assert per_key["a"] == 2 and per_key["b"] == 1  # as before
    # more than the 8 a call takes: two levels for 12, three for 70 (ceil(n / 8): 12 -> 2 -> 1; 70 -> 9 -> 2 -> 1)
    assert per_key["wide"] == 12 and per_key["deep"] == 70
    combine = probes._proc_combine_inputs()
    per_key = {}
    for key, _tile in combine:
        per_key[key] = per_key.get(key, 0) + 1
    assert per_key == {"one": 1, "few": 3, "many": 12}


def test_there_is_a_combine_case_for_each_op_and_the_session_reference_is_the_same_op():
    cases = _defs()
    combine = [c for c in cases if c["name"] == "gbx_combine_by_key"]
    assert [c["label"] for c in combine] == [
        f"gbx_combine_by_key:{op}"
        for op in ("avg", "sum", "count", "min", "max", "stddev")
    ]
    for c in combine:
        op = c["label"].split(":")[1]
        assert c["args"]["op"] == f"'{op}'"
        assert f"gbx_rst_combine{op}(collect_list(tile))" in c["ref"]
        assert c["compare"] == "pixels"
    # the Float32 results (avg, stddev) are compared to fewer digits than the exact ones
    digits = {c["label"].split(":")[1]: c["decimals"] for c in combine}
    assert digits["avg"] == digits["stddev"] == 3 and digits["sum"] == 9
    # the cases name the procedure parameters the procedure declares
    for c in combine:
        declared = {n for n, _d in _params(procedures.RECIPES[c["name"]]("c", "s"))}
        assert set(c["args"]) <= declared


def test_the_tile_inputs_are_built_through_the_guarded_client_dataframe(monkeypatch):
    from databricks.labs.gbx.bench import clientcap

    seen = []

    def fake(spark, rows, schema, what=None):
        seen.append(what)
        raise RuntimeError("stop after the first")

    monkeypatch.setattr(clientcap, "client_dataframe", fake)
    with pytest.raises(RuntimeError, match="stop after the first"):
        probes._proc_tile_cases(_Spark(), "c", "s", [])
    assert seen == ["mosaic tile input"]


def test_a_combine_case_has_its_own_record_and_its_own_target(
    monkeypatch, capsys, tmp_path
):
    import databricks.labs.gbx as gbx

    monkeypatch.setattr(gbx, "register", lambda *_a, **_k: {})
    monkeypatch.setattr(probes, "_proc_tile_cases", lambda *a: _defs())
    spark = _Spark()
    out = probes.run_procedures(spark, "c", "s", str(tmp_path))
    labels = [k for k in out["recipes"] if k.startswith("gbx_combine_by_key:")]
    assert len(labels) == 6
    calls = [s for s in spark.stmts if s.startswith("CALL `c`.`s`.gbx_combine_by_key(")]
    assert len(calls) == 6
    assert (
        len({re.search(r"target_table => '([^']*)'", s).group(1) for s in calls}) == 6
    )
    text = capsys.readouterr().out
    assert "[PROC] gbx_combine_by_key:stddev call START" in text


def test_the_pixel_compare_rounds_and_treats_a_pixel_with_no_data_as_none():
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    arr = np.array([[1.0000049, -9999.0], [2.5, 3.0]], dtype="float32")
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff", width=2, height=2, count=1, dtype="float32", nodata=-9999.0,
            crs="EPSG:3857", transform=from_origin(0, 20, 10, 10),
        ) as dst:  # fmt: skip
            dst.write(arr[np.newaxis])
        blob = mf.read()
    assert probes._proc_pixels(blob, 3) == [[1.0, None], [2.5, 3.0]]
    assert probes._proc_pixels(None) is None


def test_the_h3_case_has_valued_cells_and_absent_cells_between_them():
    import h3

    case = next(
        c
        for c in probes.proc_cases("c", "s")
        if c["name"] == "gbx_points_to_h3_surface"
    )
    values = re.search(r"VALUES (.*)$", case["setup"][1]).group(1)
    points = re.findall(r"\(([-\d.e]+), ([-\d.e]+), (NULL|[\d.]+)\)", values)
    assert len(points) == 4  # the empty centre and three cells of the ring around it
    cells = {h3.latlng_to_cell(float(lat), float(lon), 8): v for lon, lat, v in points}
    centre = next(c for c, v in cells.items() if v == "NULL")
    ring = set(h3.grid_ring(centre, 1))
    valued = [c for c, v in cells.items() if v != "NULL"]
    absent = ring - set(valued)
    assert len(valued) == 3 and len(absent) == 3 and set(valued) <= ring
    # each absent cell touches two valued ones: the padded fill gives it a value
    for a in absent:
        assert sum(h3.are_neighbor_cells(a, v) for v in valued) == 2


def test_the_h3_reference_pads_the_session_fill_input_with_the_h3_library():
    import h3

    class Spark(_Spark):
        def __init__(self):
            super().__init__()
            self.views = {}

        def createDataFrame(self, rows, schema=None):  # noqa: N802 - the Spark API name
            return type(
                "DF",
                (),
                {
                    "createOrReplaceTempView": lambda _s, name: self.views.__setitem__(
                        name, list(rows)
                    )
                },
            )()

        def sql(self, text):
            self.stmts.append(text)
            centre = h3.str_to_int(h3.latlng_to_cell(37.77, -122.42, 8))
            if "GROUP BY 1" in text:
                rows = [{"cellid": centre, "value": 1.0}]
            else:
                rows = []
            return type(
                "R",
                (),
                {"collect": lambda _s: rows, "first": lambda _s: [b"\x00\x00\x00\x00"]},
            )()

    import databricks.labs.gbx.bench.clientcap as clientcap

    spark = Spark()
    monkey = pytest.MonkeyPatch()
    monkey.setattr(
        clientcap,
        "client_dataframe",
        lambda sp, rows, schema, what=None: sp.createDataFrame(rows, schema),
    )
    try:
        probes._proc_h3_reference(spark, "src")
    finally:
        monkey.undo()
    padded = spark.views["gbxproc_h3_padded"]
    assert (
        len(padded) == 7
    )  # the cell and its six neighbours, the neighbours with no value
    assert sum(1 for _c, v in padded if v is not None) == 1


def test_the_clean_geometries_rejects_are_compared_and_a_missing_one_is_a_mismatch(
    monkeypatch, tmp_path
):
    import databricks.labs.gbx as gbx

    class Spark(_Spark):
        def __init__(self, rejects):
            super().__init__()
            self.rejects = rejects

        def sql(self, text):
            self.stmts.append(text)
            rows = []
            if "FROM `c`.`s`.gbxproc_cg_tgt" in text or "gbx_st_makevalid" in text:
                rows = [_Row(id=2, wkt="POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))")]
            elif "FROM `c`.`s`.gbxproc_cg_rej" in text:
                rows = self.rejects
            elif "'the geometry is NULL' AS reason" in text:
                rows = [_Row(id=3, reason="the geometry is NULL")]
            return type(
                "R", (), {"collect": lambda _s: rows, "first": lambda _s: None}
            )()

    monkeypatch.setattr(gbx, "register", lambda *_a, **_k: {})
    monkeypatch.setattr(probes, "_proc_tile_cases", lambda *_a: [])
    reject = [_Row(id=3, reason="the geometry is NULL")]
    ok = probes.run_procedures(Spark(reject), "c", "s", str(tmp_path))["recipes"][
        "gbx_clean_geometries"
    ]
    assert (
        ok["status"] == "match"
        and ok["also_status"] == "match"
        and ok["match_also"] is True
    )
    # the target is right but a rejected row is missing: the call is a mismatch of the rejects, not of the target
    bad = probes.run_procedures(Spark([]), "c", "s", str(tmp_path))["recipes"][
        "gbx_clean_geometries"
    ]
    assert (
        bad["status"] == "match"
        and bad["also_status"] == "mismatch"
        and bad["match_also"] is False
    )


def test_a_zarr_tag_that_errors_is_an_error_not_an_empty_compare(monkeypatch, tmp_path):
    from databricks.labs.gbx.zarr import _maint
    from databricks.labs.gbx.zarr import _manifest as M

    class Spark(_Spark):
        def sql(self, text):
            self.stmts.append(text)
            if text.startswith("CALL ") and "gbx_zarr_tag_version" in text:
                raise RuntimeError("boom")
            return type("R", (), {"collect": lambda _s: [], "first": lambda _s: None})()

    monkeypatch.setattr(M, "create_manifest", lambda *_a, **_k: None)
    monkeypatch.setattr(_maint, "gbx_zarr_tag", lambda *_a, **_k: None)

    def step(_label, _surface, fn):
        try:
            return fn(), None, 0.0
        except Exception as exc:  # noqa: BLE001
            return None, f"{type(exc).__name__}: {exc}", 0.0

    rec = probes._proc_zarr(Spark(), "c", "s", str(tmp_path), step, [])
    assert rec["tag_status"] == "error" and rec["tag_match"] is False
    assert "boom" in rec["error"]
    assert (
        rec["second_tag_refused"] is False
    )  # it did not fail for the reason a tag that exists does


def test_the_mosaic_reference_merges_at_most_one_call_of_tiles_at_a_time(spark):
    """The UC merge takes TREE_FAN_IN tiles per call: the warehouse reference of keys of 12 and 70 tiles is a tree of
    such calls, not one call over the whole key."""
    fan = procedures.TREE_FAN_IN
    sql = dict(probes.proc_warehouse_statements("c", "s", "/Volumes/c/s/v/scratch"))[
        "gbx_mosaic_by_key:diff"
    ]
    # the largest key has 70 tiles: 70 -> 9 -> (at most 8, merged once more), so two bucket levels and a last merge
    tree = probes._merge_tree_sql("`c`.`s`", "`c`.`s`.gbxproc_mos_src", 70)
    assert tree in sql
    assert tree.count(f") - 1) DIV {fan} AS bucket") == 2
    assert tree.count("gbx_rst_merge(collect_list(tile))") == 3
    # no call merges a whole key: every merge sits over a bucket level or the last level
    assert "FROM `c`.`s`.gbxproc_mos_src GROUP BY tile_key)" not in sql
    # a key of at most one call's worth is merged once, with no bucket level
    flat = probes._merge_tree_sql("`c`.`s`", "t", fan)
    assert "bucket" not in flat and flat.count("gbx_rst_merge") == 1
    # the statement is valid SQL
    spark._jsparkSession.sessionState().sqlParser().parsePlan(
        probes._merge_tree_sql("`c`.`s`", "t", 70)
    )


def test_the_zonal_stats_case_compares_the_two_summary_shapes_by_what_both_hold():
    """The procedure writes the UC level's summary (GeoBrix's earlier shape), the session reference GDAL's own JSON: the
    case compares their size and band statistics, so the same statistics match and different ones do not.
    """
    import json

    case = next(c for c in _defs() if c["name"] == "gbx_zonal_stats")
    assert case["compare"] == "summary" and case["column"] == "summary"
    uc = {
        "driverShortName": "GTiff", "size": [4, 3], "coordinateSystem": {"epsg": 3857, "crs": "EPSG:3857"},
        "bands": [{"band": 1, "type": "Float32", "noDataValue": -9999.0, "min": 2.0, "max": 12.0,
                   "mean": 7.0, "stdDev": 3.1622776601683795}],
    }  # fmt: skip
    gdal = {
        "driverShortName": "GTiff", "size": [4, 3], "coordinateSystem": {"wkt": "PROJCRS[...]"},
        "cornerCoordinates": {}, "bands": [{"band": 1, "type": "Float32", "noDataValue": -9999.0,
                                           "minimum": 2.0, "maximum": 12.0, "mean": 7.0, "stdDev": 3.162}],
    }  # fmt: skip

    def rows(info):
        return [_Row(zone_id=1, tile_id=1, summary=json.dumps(info))]

    assert probes._proc_rows(rows(uc), case) == probes._proc_rows(rows(gdal), case)
    gdal["bands"][0]["maximum"] = 11.0
    assert probes._proc_rows(rows(uc), case) != probes._proc_rows(rows(gdal), case)
    assert (
        probes._proc_rows([_Row(zone_id=1, tile_id=1, summary=None)], case)[0][
            "summary"
        ]
        is None
    )
