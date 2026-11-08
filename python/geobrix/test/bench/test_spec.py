"""The bench FnSpec registry: every registered function is benched, and each spec declares the modes, fingerprint and
input shape its family needs. The per-family expectations are one table (``_FAMILIES``), checked by one test.
"""

import inspect
import json
import re
from pathlib import Path

import pytest

from databricks.labs.gbx.bench import spec as s
from databricks.labs.gbx.bench import synth

BOTH = ("pure-core", "spark-path")
PURE = ("pure-core",)
SPARK = ("spark-path",)

# family -> (names, expected declarations). ``modes`` is exact; ``fingerprint`` / ``input_kind`` / ``fingerprint_kind``
# are checked when given. Timing-only members (fingerprint=False) cannot be compared cross-engine by design: map/struct
# outputs, world->raster coordinates, render-engine bytes (tilexyz, xyzpyramid), the clip/sample/viewshed literals that
# are in-extent for one CRS only, and rst_threshold (the two tiers implement different documented contracts).
_FAMILIES = {
    "scalar_accessors": (
        {
            "rst_format",
            "rst_getnodata",
            "rst_isempty",
            "rst_pixelheight",
            "rst_pixelwidth",
            "rst_rotation",
            "rst_scalex",
            "rst_scaley",
            "rst_skewx",
            "rst_skewy",
            "rst_srid",
            "rst_upperleftx",
            "rst_upperlefty",
        },
        dict(modes=BOTH, category="accessor", args={}),
    ),
    "scalar_accessors_pure": (
        {"rst_memsize", "rst_type"},
        dict(modes=PURE, fingerprint=False),
    ),
    "coord_both": (
        {
            "rst_rastertoworldcoord",
            "rst_rastertoworldcoordx",
            "rst_rastertoworldcoordy",
        },
        dict(modes=BOTH, category="accessor"),
    ),
    "coord_pure": (
        {
            "rst_worldtorastercoord",
            "rst_worldtorastercoordx",
            "rst_worldtorastercoordy",
            "rst_tilexyz",
        },
        dict(modes=PURE, fingerprint=False),
    ),
    "map_struct": (
        {
            "rst_metadata",
            "rst_bandmetadata",
            "rst_georeference",
            "rst_boundingbox",
            "rst_summary",
            "rst_histogram",
        },
        dict(modes=PURE, fingerprint=False, category="accessor"),
    ),
    "tile_out_scalar": (
        {
            "rst_band",
            "rst_initnodata",
            "rst_setsrid",
            "rst_updatetype",
            "rst_fillnodata",
            "rst_filter",
            "rst_convolve",
            "rst_asformat",
            "rst_cog_convert",
            "rst_resample",
            "rst_resample_to_size",
        },
        dict(modes=BOTH, fingerprint=True),
    ),
    "tile_out_timing_only": ({"rst_threshold"}, dict(modes=BOTH, fingerprint=False)),
    "tile_out_pure": ({"rst_resample_to_res"}, dict(modes=PURE, fingerprint=False)),
    "complex_arg_compared": (
        {
            "rst_evi",
            "rst_savi",
            "rst_index",
            "rst_mapalgebra",
            "rst_derivedband",
            "rst_proximity",
        },
        dict(modes=BOTH, fingerprint=True),
    ),
    "complex_arg_timing_only": (
        {"rst_clip", "rst_color_relief", "rst_viewshed", "rst_sample"},
        dict(modes=PURE, fingerprint=False),
    ),
    "readers_bytes": (
        {"rst_tryopen", "rst_fromcontent"},
        dict(modes=BOTH, fingerprint=True, input_kind="bytes"),
    ),
    "readers_path": (
        {"rst_fromfile"},
        dict(modes=PURE, fingerprint=True, input_kind="path"),
    ),
    "buildoverviews": (
        {"rst_buildoverviews"},
        dict(modes=BOTH, fingerprint=True, input_kind="tile"),
    ),
    "subdatasets": (
        {"rst_subdatasets", "rst_getsubdataset"},
        dict(modes=PURE, fingerprint=False, input_kind="tile"),
    ),
    "multi_tile_input": (
        {"rst_frombands", "rst_combineavg", "rst_merge"},
        dict(modes=BOTH, fingerprint=True, input_kind="tile_array"),
    ),
    "tiling": (
        {"rst_maketiles", "rst_retile", "rst_tooverlappingtiles", "rst_separatebands"},
        dict(modes=BOTH, fingerprint=True, input_kind="tile"),
    ),
    "tiling_timing_only": (
        {"rst_xyzpyramid"},
        dict(modes=PURE, fingerprint=False, input_kind="tile"),
    ),
    "dggs": (
        {
            f"rst_{g}_rastertogrid{a}"
            for g in ("h3", "quadbin")
            for a in (
                "avg",
                "count",
                "max",
                "median",
                "min",
                "sum",
                "variance",
                "stddev",
            )
        }
        | {"rst_h3_tessellate"},
        dict(
            modes=BOTH,
            category="dggs",
            fingerprint=True,
            input_kind="tile",
            fingerprint_kind="dggs_grid",
        ),
    ),
    "vector_out": (
        {"rst_contour", "rst_polygonize"},
        dict(
            modes=BOTH,
            category="vector",
            fingerprint=True,
            input_kind="tile",
            fingerprint_kind="vector",
        ),
    ),
    "geometry_in": (
        {"rst_rasterize", "rst_gridfrompoints", "rst_dtmfromgeoms"},
        dict(
            modes=PURE, fingerprint=True, input_kind="geometry", fingerprint_kind="auto"
        ),
    ),
    "tile_aggregators": (
        {
            "rst_combineavg_agg",
            "rst_merge_agg",
            "rst_frombands_agg",
            "rst_derivedband_agg",
        },
        dict(
            modes=SPARK,
            fingerprint=True,
            input_kind="tile_aggregate",
            fingerprint_kind="auto",
        ),
    ),
    "geometry_aggregators": (
        {"rst_rasterize_agg", "rst_gridfrompoints_agg", "rst_dtmfromgeoms_agg"},
        dict(
            modes=SPARK,
            fingerprint=True,
            input_kind="geometry_aggregate",
            fingerprint_kind="auto",
        ),
    ),
}
_FAMILY_ROWS = [
    (n, f, exp) for f, (names, exp) in _FAMILIES.items() for n in sorted(names)
]
_BENCH_ROOT = next(
    p for p in Path(__file__).resolve().parents if (p / "pom.xml").exists()
)


def test_fnspec_defaults():
    fs = s.FnSpec("x", "gbx_x", "accessor", ("pure-core",))
    assert (fs.fingerprint, fs.input_kind, fs.fingerprint_kind) == (
        True,
        "tile",
        "auto",
    )


@pytest.mark.parametrize(
    "name,family,exp", _FAMILY_ROWS, ids=[f"{r[1]}:{r[0]}" for r in _FAMILY_ROWS]
)
def test_family_declarations(name, family, exp):
    """Each benched function declares what its family needs: modes, fingerprint, input shape, args."""
    fs = s.REGISTRY[name]
    assert fs.sql_name == f"gbx_{name}"
    assert fs.core is False, "only the representative set is core"
    assert callable(fs.core_fn) and callable(fs.col_fn)
    assert tuple(fs.modes) == exp["modes"]
    for field in ("fingerprint", "input_kind", "fingerprint_kind", "category", "args"):
        if field in exp:
            assert getattr(fs, field) == exp[field], field
    if exp.get("input_kind") not in (None, "bytes", "path"):
        assert fs.sources


def test_families_are_disjoint_and_registered():
    names = [r[0] for r in _FAMILY_ROWS]
    assert len(names) == len(set(names))
    assert set(names) <= set(s.REGISTRY)


def test_core_set_is_the_representative_nineteen():
    core = {f.name for f in s.select(set="core")}
    assert len(core) == 19
    assert core <= {f.name for f in s.select(set="full")}
    assert {
        "rst_width",
        "rst_avg",
        "rst_slope",
        "rst_ndvi",
        "rst_transform",
        "rst_to_webmercator",
    } <= core
    assert not ({n for _, (names, _) in _FAMILIES.items() for n in names} & core)
    assert {f.name for f in s.select()} == core, "the default set is core"


def test_full_set_is_the_registered_functions():
    """Bench coverage == registration: every registered rst_ function is benched once under its own name."""
    full = s.select(set="full")
    assert len({f.name for f in full}) == len(full)
    own = [f for f in full if f.variant_of is None]
    assert len(own) == len(s.registered_rst())
    assert s.benched_functions(full) == s.registered_rst() != set()
    for f in full:
        assert f.core_fn is not None and f.col_fn is not None
        assert set(f.modes) <= {"pure-core", "spark-path"}
        assert f.sql_name.startswith("gbx_") and f.modes


def test_fnspec_fields_and_functions_json(tmp_path):
    fs = s.REGISTRY["rst_slope"]
    assert (fs.sql_name, fs.category) == ("gbx_rst_slope", "terrain")
    assert set(BOTH) <= set(fs.modes)
    p = tmp_path / "functions.json"
    s.dump_functions_json(p)
    by_name = {d["name"]: d for d in json.loads(p.read_text())}
    assert by_name["rst_slope"]["args"] == {"unit": "degrees"}
    assert "core_fn" not in by_name["rst_slope"], "callables are not serialized"


def test_select_filters():
    assert all(f.category == "accessor" for f in s.select(categories=["accessor"]))
    assert [f.name for f in s.select(functions=["rst_slope"])] == ["rst_slope"]
    # regression: the core filter used to run before an explicit name list and yielded zero specs
    names = ["rst_threshold", "rst_derivedband"]
    assert not any(s.REGISTRY[n].core for n in names)
    assert {f.name for f in s.select(functions=names, set="core")} == set(names)


def test_both_modes_select_each_fn_in_its_declared_mode():
    """A change-aware run over a pure-core fn and an aggregator: "both" is the union of declared modes, so the
    aggregator is not a skipped zero-row pure-core fn."""
    mixed = s.select(functions=["rst_slope", "rst_combineavg_agg"])
    assert {f.name for f in mixed if "pure-core" in f.modes} == {"rst_slope"}
    assert {f.name for f in mixed if "spark-path" in f.modes} == {
        "rst_slope",
        "rst_combineavg_agg",
    }


def test_family_arguments_match_the_heavy_defaults():
    a = {n: s.REGISTRY[n].args for n in s.REGISTRY}
    assert a["rst_band"] is not None and s.REGISTRY["rst_band"].min_bands == 2
    for n in ("rst_evi", "rst_savi", "rst_index"):
        assert s.REGISTRY[n].min_bands == 2, n
    for n, keys in {
        "rst_maketiles": {"size_in_mb"},
        "rst_retile": {"tile_width", "tile_height"},
        "rst_tooverlappingtiles": {"tile_width", "tile_height", "overlap"},
        "rst_separatebands": set(),
        "rst_xyzpyramid": {"min_z", "max_z"},
    }.items():
        assert set(a[n]) == keys, n
    for n in _FAMILIES["dggs"][0]:
        res = a[n]["resolution"]
        assert res == (15 if n.startswith("rst_quadbin_") else 7), n
    assert a["rst_contour"]["levels"] == [0.2, 0.4, 0.6, 0.8]
    assert (a["rst_polygonize"]["band"], a["rst_polygonize"]["connectedness"]) == (1, 4)
    assert (
        a["rst_dtmfromgeoms_agg"]["merge_tolerance"],
        a["rst_dtmfromgeoms_agg"]["snap_tolerance"],
    ) == (0.0, 0.0)
    assert a["rst_derivedband_agg"]["func_name"] == "mean_bands"
    # a small max_pts made the light cKDTree pick only the nearest k while gdal_grid invdist uses every point
    for n in ("rst_gridfrompoints", "rst_gridfrompoints_agg"):
        assert a[n]["max_pts"] >= 64 and a[n]["power"] == 2.0, n


def test_geometry_core_fn_takes_the_geometry_set():
    for n in _FAMILIES["geometry_in"][0]:
        assert len(inspect.signature(s.REGISTRY[n].core_fn).parameters) == 3, n


def test_synth_recipes_are_ones_the_synthesizer_implements():
    for n in _FAMILIES["multi_tile_input"][0]:
        assert s.synth_recipe(n) == n.removeprefix("rst_"), n
        assert s.synth_recipe(n) in synth._RECIPES
    agg = {
        "rst_combineavg_agg": "combineavg",
        "rst_merge_agg": "merge",
        "rst_frombands_agg": "frombands",
        "rst_derivedband_agg": "frombands",
    }
    for n, recipe in agg.items():
        assert s.agg_synth_recipe(n) == recipe, n
        assert recipe in synth._RECIPES


def test_variant_cases_bench_a_registered_function():
    variants = {n: f for n, f in s.REGISTRY.items() if f.variant_of}
    assert "rst_h3_tessellate_centroid" in variants
    for name, fs in variants.items():
        assert (
            fs.variant_of in s.REGISTRY and s.REGISTRY[fs.variant_of].variant_of is None
        ), name
        assert fs.sql_name == f"gbx_{fs.variant_of}" and name.startswith(
            fs.variant_of + "_"
        ), name


def test_h3_tessellate_centroid_case_drains_the_default_assignment():
    # The case must time centroid + complete (the function default), while
    # rst_h3_tessellate's own pure-core case times covering.
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_bounds

    from databricks.labs.gbx.pyrx.core import tessellate

    prof = dict(
        driver="GTiff",
        width=64,
        height=64,
        count=1,
        dtype="uint16",
        nodata=0,
        crs="EPSG:4326",
        transform=from_bounds(-74.02, 40.70, -73.99, 40.73, 64, 64),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(np.arange(1, 64 * 64 + 1, dtype="uint16").reshape(1, 64, 64))
        with mf.open() as ds:
            fs = s.REGISTRY["rst_h3_tessellate_centroid"]
            got = fs.core_fn(ds, fs.args)
            want = list(
                tessellate.iter_tessellate(
                    ds, 7, "h3", mode="centroid", coverage="complete"
                )
            )
            covering = s.REGISTRY["rst_h3_tessellate"].core_fn(ds, {"resolution": 7})
    assert fs.args == {
        "resolution": 7,
        "assignment": "centroid",
        "coverage": "complete",
    }
    assert got == [want]
    assert got != covering


def test_every_fnspec_declares_existing_sources():
    for f in s.select(set="full"):
        assert f.sources, f"{f.name} has no sources"
        for p in f.sources:
            assert (_BENCH_ROOT / p).exists(), f"{f.name}: missing source {p}"


def test_accessor_virtual_disposition():
    for fn in (
        "rst_avg",
        "rst_min",
        "rst_max",
        "rst_median",
        "rst_pixelcount",
        "rst_summary",
        "rst_histogram",
    ):
        assert s.accessor_disposition(fn) == "materialized", fn
    for fn in (
        "rst_width",
        "rst_height",
        "rst_numbands",
        "rst_srid",
        "rst_pixelwidth",
        "rst_georeference",
    ):
        assert s.accessor_disposition(fn) == "deferred", fn
    import dataclasses

    fs = dataclasses.replace(
        s.REGISTRY["rst_width"], virtual_disposition="materialized"
    )
    assert s.accessor_disposition("rst_width", fs) == "materialized"


def test_light_only_specs_are_the_names_bench_dispatch_dropped():
    """``heavy`` is True exactly for the names ``BenchDispatch`` has a case for.

    A spec marked ``heavy=False`` has no JVM form: BenchDispatch must not know its name. A spec marked
    ``heavy=True`` with a spark-path (or pure-core) mode must be known there, or the heavyweight run
    sends the JVM a name it cannot dispatch.
    """
    text = (
        _BENCH_ROOT / "src/test/scala/com/databricks/labs/gbx/bench/BenchDispatch.scala"
    ).read_text()
    known = set(re.findall(r'"([a-z0-9_]+)"\s*(?:->|=>)', text))
    light_only = {n for n, f in s.REGISTRY.items() if not f.heavy}
    assert {"rst_median", "rst_retile", "rst_h3_rastertogridavg"} <= light_only
    assert {"rst_fromfile", "rst_land_cover", "st_makevalid"} <= light_only
    assert not (light_only & known), light_only & known
    heavy_unknown = {
        n
        for n, f in s.REGISTRY.items()
        if f.heavy and ({"spark-path", "pure-core"} & set(f.modes)) and n not in known
    }
    assert not heavy_unknown, sorted(heavy_unknown)
