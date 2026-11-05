"""The vector ports on Unity Catalog (the shapely scalars, the coverage and vector-tile array forms): what the oracle rows
(bench/oracle_tables/vector_scalar.py, vector_aggregate.py) do not say. The rows hold the values, the NULLs and the
refusals; this file holds the mechanisms: the SRID the result carries, the Fix of every wrong argument, the SRID check of
the two-geometry functions, the cap of the arrays (checked before a member is parsed), the DDL text, and the sandbox
(no pyspark, no pandas) the handlers run in.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import shapely

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench.oracles import UcGeometry
from databricks.labs.gbx.uc import ddl, forms, handlers

MAN = manifest.load()
SEVEN = {
    "gbx_st_node": ("LINESTRING(0 0, 10 10, 0 10, 10 0)",),
    "gbx_st_reduceprecision": ("POINT(1.234 5.678)", 1.0),
    "gbx_st_removerepeatedpoints": ("LINESTRING(0 0, 0 0, 1 1)", 0.0),
    "gbx_st_shiftlongitude": ("LINESTRING(-10 0, 10 0)",),
    "gbx_st_simplifypreservetopology": (
        "POLYGON((0 0, 0 5, 0.001 8, 0 10, 10 10, 10 0, 0 0))",
        1.0,
    ),
    "gbx_st_snap": ("LINESTRING(0 0.4, 10 0.4)", "LINESTRING(0 0, 10 0)", 0.5),
    "gbx_st_split": ("LINESTRING(0 0, 10 0)", "LINESTRING(5 -1, 5 1)"),
}


def _geom(wkt, srid=4326):
    return UcGeometry(shapely.to_wkb(shapely.from_wkt(wkt)), srid)


def _call(name, args, srid=4326):
    """The handler on UC arguments: the strings are geometries in ``srid``, the numbers stay numbers."""
    fn = getattr(handlers, name[len("gbx_st_") :] and "st_" + name[len("gbx_st_") :])
    made = [_geom(a, srid) if isinstance(a, str) else a for a in args]
    with _geometry_stand_in():
        return fn(*made)


class _geometry_stand_in:
    """The handlers build their GEOMETRY results with the stand-in while the test calls them (no pyspark)."""

    def __enter__(self):
        self.saved = handlers._geometry_type
        handlers._geometry_type = lambda: UcGeometry

    def __exit__(self, *exc):
        handlers._geometry_type = self.saved


@pytest.mark.parametrize("name,args", SEVEN.items())
def test_a_result_carries_the_srid_of_its_first_argument(name, args):
    """The mechanism the SRID rows rest on: EPSG:3857 in is EPSG:3857 out, for each of the seven."""
    out = _call(name, args, srid=3857)
    assert isinstance(out, UcGeometry) and out.srid == 3857


@pytest.mark.parametrize(
    "name,args,bad",
    [
        ("gbx_st_reduceprecision", ("POINT(1 1)", -1.0), "-1.0"),
        ("gbx_st_reduceprecision", ("POINT(1 1)", float("inf")), "inf"),
        ("gbx_st_removerepeatedpoints", ("LINESTRING(0 0, 1 1)", -0.5), "-0.5"),
        (
            "gbx_st_simplifypreservetopology",
            ("LINESTRING(0 0, 1 1)", float("nan")),
            "nan",
        ),
        ("gbx_st_snap", ("LINESTRING(0 0, 1 1)", "POINT(0 0)", -1.0), "-1.0"),
        ("gbx_st_split", ("POINT(1 1)", "LINESTRING(0 0, 2 2)"), "Splitting Point"),
    ],
)
def test_a_wrong_argument_raises_with_a_fix_and_names_the_value(name, args, bad):
    with pytest.raises(ValueError) as err:
        _call(name, args)
    assert bad in str(err.value) and "Fix:" in str(err.value)


@pytest.mark.parametrize("name", ["gbx_st_snap", "gbx_st_split"])
def test_two_geometries_in_different_srids_are_refused_with_a_fix(name):
    args = SEVEN[name]
    fn = getattr(handlers, "st_" + name[len("gbx_st_") :])
    a, b, *rest = args
    with _geometry_stand_in(), pytest.raises(handlers._Refusal) as err:
        fn(_geom(a, 4326), _geom(b, 3857), *rest)
    assert "4326 and 3857" in str(err.value) and "Fix: st_transform" in str(err.value)


def test_a_null_tolerance_means_exact_repeats_only_but_a_null_grid_size_is_null():
    with _geometry_stand_in():
        out = handlers.st_removerepeatedpoints(_geom("LINESTRING(0 0, 0 0, 1 1)"), None)
        assert shapely.from_wkb(out.wkb).wkt == "LINESTRING (0 0, 1 1)"
        assert handlers.st_reduceprecision(_geom("POINT(1 1)"), None) is None
        assert handlers.st_node(None) is None


def test_the_geometry_arguments_and_results_are_geometry_in_the_ddl():
    for name in SEVEN:
        # a function with a default is a SQL wrapper over a private Python routine: the pair holds both
        stmts = "\n".join(
            ddl.function_ddl(
                MAN.functions[name],
                "c",
                "s",
                "file:///w.whl",
                MAN.uc_dependencies,
                "0.6",
            )
        )
        assert (
            "RETURNS GEOMETRY(ANY)" in stmts
            and " GEOMETRY(ANY)" in stmts.split("\n")[0]
        ), name
        assert "shapely" in stmts, name


# --- the array twins ------------------------------------------------------------------------------------------------------
ARRAYS = {
    "gbx_st_coverageisvalid_array": ("array<binary>", "BOOLEAN"),
    "gbx_st_coverageinvalidedges_array": ("array<binary>", "BINARY"),
    "gbx_st_asmvt_array": ("array<struct<geom:binary,attrs:string>>", "BINARY"),
}


@pytest.mark.parametrize("name,types", ARRAYS.items())
def test_an_array_form_is_python_on_both_surfaces_and_its_aggregate_keeps_its_name(
    name, types
):
    fn = MAN.functions[name]
    assert {"L3", "L4"} <= set(fn.levels) and "handler" in fn.levels["L4"]
    assert fn.params[0].uc == types[0] and fn.returns_uc == types[1]
    assert (
        name[: -len("_array")] in MAN.functions
    )  # the aggregate is still there, unchanged
    stmts = "\n".join(
        ddl.function_ddl(fn, "c", "s", "file:///w.whl", MAN.uc_dependencies, "0.6")
    )
    assert "LANGUAGE PYTHON" in stmts and "shapely" in stmts


def _box(x0, x1):
    return shapely.box(x0, 0, x1, 1).wkb


def test_an_array_over_the_cap_is_refused_before_a_member_is_parsed(monkeypatch):
    """The size is the bytes of the members, summed first: nothing is decoded when it is over the cap."""
    monkeypatch.setattr(forms, "DECODED_CAP", 100)
    parsed = []
    real = shapely.from_wkb
    monkeypatch.setattr(
        shapely, "from_wkb", lambda *a, **k: parsed.append(a) or real(*a, **k)
    )
    from databricks.labs.gbx.pyvx import _coverage, _mvt

    monkeypatch.setattr(
        _mvt, "encode_layer", lambda *a, **k: pytest.fail("encoded over the cap")
    )
    monkeypatch.setattr(
        _coverage,
        "coverage_is_valid_agg",
        lambda *a, **k: pytest.fail("parsed over the cap"),
    )
    polygons = [_box(i, i + 1) for i in range(8)]
    for fn, session in (
        (forms.st_coverageisvalid_array, "gbx_st_coverageisvalid"),
        (forms.st_coverageinvalidedges_array, "gbx_st_coverageinvalidedges"),
    ):
        with pytest.raises(ValueError, match=rf"over the .*Fix: .*{session}"):
            fn(polygons, 0.0)
    features = [{"geom": g, "attrs": '{"a": 1}'} for g in polygons]
    with pytest.raises(ValueError, match=r"over the .*Fix: .*gbx_st_asmvt"):
        forms.st_asmvt_array(features, "lyr")
    assert parsed == []


def test_the_attributes_count_toward_the_cap_of_a_feature_array(monkeypatch):
    big_attrs = json.dumps({"k": "x" * 5000})
    monkeypatch.setattr(forms, "DECODED_CAP", 4000)
    with pytest.raises(ValueError, match="over the"):
        forms.st_asmvt_array(
            [{"geom": shapely.Point(1, 1).wkb, "attrs": big_attrs}], "lyr"
        )


def test_features_become_one_layer_with_native_property_types():
    import mapbox_vector_tile as mvt

    rows = [
        {"geom": shapely.Point(5, 7).wkb, "attrs": json.dumps({"n": 3, "ok": True})},
        {"geom": shapely.Point(6, 8).wkb, "attrs": None},
        None,
        {"geom": None, "attrs": "{}"},
    ]
    layer = mvt.decode(
        forms.st_asmvt_array(rows, "lyr"), default_options={"y_coord_down": True}
    )["lyr"]
    assert [f["properties"] for f in layer["features"]] == [{"n": 3, "ok": True}, {}]


# --- the sandbox ----------------------------------------------------------------------------------------------------------
def test_the_vector_handlers_run_in_a_sandbox_without_pyspark():
    """A UC Python sandbox holds the declared dependencies only: every UC row of the vector tables runs without them."""
    code = (
        "import sys, tempfile\n"
        "for blocked in ('pyspark', 'pandas', 'pyarrow', 'scipy'):\n"
        "    sys.modules[blocked] = None  # an import of it raises ImportError\n"
        "from databricks.labs.gbx.bench import oracles\n"
        "ran = oracles.run_handlers(oracles.TABLES['VECTOR_SCALAR'], tmpdir=tempfile.mkdtemp())\n"
        "ran += oracles.run_handlers(oracles.TABLES['VECTOR_AGGREGATE'], tmpdir=tempfile.mkdtemp())\n"
        "bad = [m for _, m in ran if m]\n"
        "assert ran and not bad, bad\n"
        "print(len(ran))\n"
    )
    src = Path(manifest.__file__).resolve().parents[4]
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            filter(None, [str(src), os.environ.get("PYTHONPATH")])
        ),
    }
    out = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True
    )
    assert out.returncode == 0, out.stderr[-2000:]
    from databricks.labs.gbx.bench import oracles

    assert int(out.stdout) == sum(
        "uc" in c.surfaces and c.sql is None and "L4" in MAN.functions[c.fn].levels
        for t in ("VECTOR_SCALAR", "VECTOR_AGGREGATE")
        for c in oracles.TABLES[t]
    )
