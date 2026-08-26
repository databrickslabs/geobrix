import json

from databricks.labs.gbx.bench import spec as s


def test_registry_has_representative_functions():
    names = set(s.REGISTRY)
    assert {"rst_width", "rst_avg", "rst_slope", "rst_ndvi", "rst_transform"} <= names


def test_fnspec_fields():
    fs = s.REGISTRY["rst_slope"]
    assert fs.sql_name == "gbx_rst_slope"
    assert fs.category == "terrain"
    assert "pure-core" in fs.modes and "spark-path" in fs.modes
    assert callable(fs.core_fn) and callable(fs.col_fn)


def test_select_filters_by_category_and_name():
    only_acc = s.select(categories=["accessor"])
    assert all(f.category == "accessor" for f in only_acc)
    one = s.select(functions=["rst_slope"])
    assert [f.name for f in one] == ["rst_slope"]


def test_select_by_name_ignores_core_tier():
    # An explicit functions list must ignore the tier: requesting core=False
    # functions by name under the default set="core" must still return them
    # (regression: the core filter previously ran first and yielded zero specs).
    names = ["rst_threshold", "rst_derivedband"]
    for n in names:
        assert s.REGISTRY[n].core is False
    picked = {f.name for f in s.select(functions=names, set="core")}
    assert picked == set(names)


def test_dump_functions_json(tmp_path):
    p = tmp_path / "functions.json"
    s.dump_functions_json(p)
    data = json.loads(p.read_text())
    by_name = {d["name"]: d for d in data}
    assert by_name["rst_slope"]["sql_name"] == "gbx_rst_slope"
    assert by_name["rst_slope"]["args"] == {"unit": "degrees"}
    assert "core_fn" not in by_name["rst_slope"]  # callables are not serialized


def test_registry_covers_core_accessor_and_terrain_families():
    names = set(s.REGISTRY)
    expected_subset = {
        "rst_width",
        "rst_height",
        "rst_numbands",
        "rst_avg",
        "rst_min",
        "rst_max",
        "rst_median",
        "rst_pixelcount",
        "rst_slope",
        "rst_aspect",
        "rst_hillshade",
        "rst_tri",
        "rst_tpi",
        "rst_roughness",
        "rst_ndvi",
        "rst_ndwi",
        "rst_nbr",
        "rst_transform",
        "rst_to_webmercator",
    }
    missing = expected_subset - names
    assert not missing, f"registry missing: {sorted(missing)}"


def test_every_spec_has_valid_bindings_and_modes():
    for name, fs in s.REGISTRY.items():
        assert fs.sql_name.startswith("gbx_")
        assert fs.modes  # non-empty
        assert callable(fs.core_fn) and callable(fs.col_fn)


def test_select_core_is_subset_of_full():
    core = {f.name for f in s.select(set="core")}
    full = {f.name for f in s.select(set="full")}
    assert core
    assert core <= full
    assert (
        len(core) == 19
    )  # core == the 19 representative RST fns (ST fns are spark-path-only, not core)
    assert "rst_slope" in core and "rst_ndvi" in core


def test_select_defaults_to_core():
    assert {f.name for f in s.select()} == {f.name for f in s.select(set="core")}


def test_full_has_only_registered_names():
    reg = s.registered_rst()
    assert {f.name for f in s.select(set="full")} <= reg
    # The registered rst_ set is the canonical registered_functions.txt list. Guard
    # the real invariant (every bench FnSpec is a registered function; the counts
    # agree) without a brittle literal that trips whenever a function is legitimately
    # added. registered_rst() is the source of truth; the bench REGISTRY must be a
    # subset of it AND cover every one of them (bench parity == full registration).
    assert reg  # non-empty
    assert {f.name for f in s.select(set="full")} == reg


def test_every_full_spec_is_wellformed():
    for f in s.select(set="full"):
        assert f.core_fn is not None and f.col_fn is not None
        assert set(f.modes) <= {"pure-core", "spark-path"}


def test_explicit_functions_filter_ignores_set():
    # naming a function selects it even if not core
    got = {f.name for f in s.select(functions=["rst_width"], set="full")}
    assert got == {"rst_width"}


# --- Task 2: scalar-accessor coverage (15 no-arg accessors) -----------------
_SCALAR_ACCESSORS = {
    "rst_format",
    "rst_getnodata",
    "rst_isempty",
    "rst_memsize",
    "rst_pixelheight",
    "rst_pixelwidth",
    "rst_rotation",
    "rst_scalex",
    "rst_scaley",
    "rst_skewx",
    "rst_skewy",
    "rst_srid",
    "rst_type",
    "rst_upperleftx",
    "rst_upperlefty",
}

# rst_memsize and rst_type cannot produce a cross-engine-identical fingerprint
# (memsize differs file-size vs in-memory; type is a string array with no heavy
# array-of-strings fingerprint constructor), so they run pure-core-only.
_PURE_CORE_ONLY = {"rst_memsize", "rst_type"}


def test_scalar_accessors_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _SCALAR_ACCESSORS - full
    assert not missing, f"registry missing scalar accessors: {sorted(missing)}"


def test_scalar_accessors_wellformed():
    for name in _SCALAR_ACCESSORS:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.category == "accessor"
        assert fs.args == {}
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        assert set(fs.modes) <= {"pure-core", "spark-path"}


def test_scalar_accessors_modes():
    for name in _SCALAR_ACCESSORS:
        fs = s.REGISTRY[name]
        if name in _PURE_CORE_ONLY:
            assert fs.modes == ("pure-core",), name
        else:
            assert "pure-core" in fs.modes and "spark-path" in fs.modes, name


def test_scalar_accessors_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_SCALAR_ACCESSORS & core)


# --- Task 3: coordinate / index accessors (7) -------------------------------
_COORD_ACCESSORS = {
    "rst_rastertoworldcoord",
    "rst_rastertoworldcoordx",
    "rst_rastertoworldcoordy",
    "rst_worldtorastercoord",
    "rst_worldtorastercoordx",
    "rst_worldtorastercoordy",
    "rst_tilexyz",
}

# raster->world is pure affine (forward geotransform), so rasterio.xy and
# GDAL.toWorldCoord agree for any pixel index in any CRS -> both modes, compared.
_COORD_BOTH = {
    "rst_rastertoworldcoord",
    "rst_rastertoworldcoordx",
    "rst_rastertoworldcoordy",
}
# world->raster cannot be made cross-engine-consistent over the multi-CRS corpus:
# a single fixed world literal is in-extent for only one CRS, and for the others
# the inverse-affine index is huge/negative (the EPSG:4326 0.0001-deg grid even
# overflows int32, where rasterio.index floor-casts and GDAL .toInt truncate
# differently). rst_tilexyz renders a warped+encoded image whose bytes depend on
# the warp/encode stack (GDAL vs rasterio/PIL) and the source CRS. All four run
# pure-core-only; their fingerprints are suppressed in the scorecard, exactly as
# rst_memsize / rst_type are.
_COORD_PURE_CORE_ONLY = {
    "rst_worldtorastercoord",
    "rst_worldtorastercoordx",
    "rst_worldtorastercoordy",
    "rst_tilexyz",
}


def test_coord_accessors_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _COORD_ACCESSORS - full
    assert not missing, f"registry missing coord accessors: {sorted(missing)}"


def test_coord_accessors_wellformed():
    for name in _COORD_ACCESSORS:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.category == "accessor"
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        assert set(fs.modes) <= {"pure-core", "spark-path"}


def test_coord_accessors_modes():
    for name in _COORD_ACCESSORS:
        fs = s.REGISTRY[name]
        if name in _COORD_PURE_CORE_ONLY:
            assert fs.modes == ("pure-core",), name
        else:
            assert "pure-core" in fs.modes and "spark-path" in fs.modes, name


def test_coord_accessors_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_COORD_ACCESSORS & core)


# --- Task 4: timing-only fingerprint flag + map/struct coverage (6) ---------
# Map/struct outputs (metadata maps, georeference dicts, bbox WKB, gdalinfo JSON,
# per-band histograms) cannot be made byte- or value-identical cross-engine, so
# they are timed but never compared: fingerprint=False suppresses the fingerprint
# on BOTH sides, and an empty fingerprint compares as `na` (not divergent).
_MAP_STRUCT = {
    "rst_metadata",
    "rst_bandmetadata",
    "rst_georeference",
    "rst_boundingbox",
    "rst_summary",
    "rst_histogram",
}

# The 6 prior ad-hoc downgrades (Task 2/3) now ride the same flag instead of
# emitting a real-but-suppressed fingerprint.
_RETROFITTED = {
    "rst_memsize",
    "rst_type",
    "rst_worldtorastercoord",
    "rst_worldtorastercoordx",
    "rst_worldtorastercoordy",
    "rst_tilexyz",
}


def test_fnspec_fingerprint_defaults_true():
    fs = s.FnSpec("x", "gbx_x", "accessor", ("pure-core",))
    assert fs.fingerprint is True


def test_map_struct_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _MAP_STRUCT - full
    assert not missing, f"registry missing map/struct fns: {sorted(missing)}"


def test_map_struct_wellformed_timing_only():
    for name in _MAP_STRUCT:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.category == "accessor"
        assert fs.core is False
        assert fs.modes == ("pure-core",), name
        assert fs.fingerprint is False, name
        assert callable(fs.core_fn) and callable(fs.col_fn)


def test_map_struct_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_MAP_STRUCT & core)


def test_retrofitted_downgrades_are_timing_only():
    for name in _RETROFITTED:
        fs = s.REGISTRY[name]
        assert fs.modes == ("pure-core",), name
        assert fs.fingerprint is False, name


# --- Task 5: tile-out transforms with scalar / fixed args (13) ---------------
# Each produces a raster tile, so its output is compared via the raster
# fingerprint (same path as terrain). All are core=False and fingerprint=True
# (both modes, compared) EXCEPT rst_resample_to_res: a single fixed ground
# resolution cannot be sane across the multi-CRS corpus (a 0.0001-deg grid and a
# 10-m grid have no common absolute resolution), exactly like the world->raster
# coord functions, so it runs pure-core-only with its fingerprint suppressed.
_TILE_OUT_SCALAR = {
    "rst_band",
    "rst_threshold",
    "rst_initnodata",
    "rst_setsrid",
    "rst_updatetype",
    "rst_fillnodata",
    "rst_filter",
    "rst_convolve",
    "rst_asformat",
    "rst_cog_convert",
    "rst_resample",
    "rst_resample_to_res",
    "rst_resample_to_size",
}

_TILE_OUT_PURE_CORE_ONLY = {"rst_resample_to_res"}

# rst_threshold runs BOTH modes (timed) but is never fingerprint-compared: the
# two tiers implement different, documented output contracts (heavy binarises to a
# single-band 0/1 mask; light keeps passing pixels and preserves all bands). A
# by-design contract difference, not a bug — so it is timing-only.
_TILE_OUT_TIMING_ONLY = {"rst_threshold"}


def test_tile_out_scalar_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _TILE_OUT_SCALAR - full
    assert not missing, f"registry missing tile-out fns: {sorted(missing)}"


def test_tile_out_scalar_wellformed():
    for name in _TILE_OUT_SCALAR:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        assert set(fs.modes) <= {"pure-core", "spark-path"}


def test_tile_out_scalar_modes_and_fingerprint():
    for name in _TILE_OUT_SCALAR:
        fs = s.REGISTRY[name]
        if name in _TILE_OUT_PURE_CORE_ONLY:
            assert fs.modes == ("pure-core",), name
            assert fs.fingerprint is False, name
        elif name in _TILE_OUT_TIMING_ONLY:
            assert "pure-core" in fs.modes and "spark-path" in fs.modes, name
            assert fs.fingerprint is False, name
        else:
            assert "pure-core" in fs.modes and "spark-path" in fs.modes, name
            assert fs.fingerprint is True, name


def test_tile_out_scalar_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_TILE_OUT_SCALAR & core)


def test_rst_band_requires_two_bands():
    assert s.REGISTRY["rst_band"].min_bands == 2


# --- Task 6: tile-out transforms with geometry/expression/band-map/func (10) -
# All produce a raster tile, all core=False. SIX run both modes and are compared
# (fingerprint=True): the args are CRS-/band-count-independent and the two engines
# run the same algorithm. FOUR run pure-core-only (fingerprint=False) because no
# single literal geometry/observer/point is in-extent across the multi-CRS corpus
# (clip, sample, viewshed) and/or the engines use different interpolation/scan
# algorithms (color_relief np.interp vs GDAL DEMProcessing; viewshed xrspatial vs
# GDAL sweep).
_COMPLEX_ARG = {
    "rst_evi",
    "rst_savi",
    "rst_index",
    "rst_mapalgebra",
    "rst_derivedband",
    "rst_clip",
    "rst_color_relief",
    "rst_proximity",
    "rst_viewshed",
    "rst_sample",
}

_COMPLEX_FULL = {
    "rst_evi",
    "rst_savi",
    "rst_index",
    "rst_mapalgebra",
    "rst_derivedband",
    "rst_proximity",
}

_COMPLEX_TIMING_ONLY = {
    "rst_clip",
    "rst_color_relief",
    "rst_viewshed",
    "rst_sample",
}


def test_complex_arg_partition_is_total():
    # the full + timing-only sets exactly partition the 10 Task-6 functions
    assert _COMPLEX_FULL | _COMPLEX_TIMING_ONLY == _COMPLEX_ARG
    assert not (_COMPLEX_FULL & _COMPLEX_TIMING_ONLY)
    assert len(_COMPLEX_ARG) == 10
    assert len(_COMPLEX_FULL) == 6
    assert len(_COMPLEX_TIMING_ONLY) == 4


def test_complex_arg_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _COMPLEX_ARG - full
    assert not missing, f"registry missing complex-arg fns: {sorted(missing)}"


def test_complex_arg_wellformed():
    for name in _COMPLEX_ARG:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        assert set(fs.modes) <= {"pure-core", "spark-path"}


def test_complex_arg_full_comparison_modes_and_fingerprint():
    for name in _COMPLEX_FULL:
        fs = s.REGISTRY[name]
        assert "pure-core" in fs.modes and "spark-path" in fs.modes, name
        assert fs.fingerprint is True, name


def test_complex_arg_timing_only_modes_and_fingerprint():
    for name in _COMPLEX_TIMING_ONLY:
        fs = s.REGISTRY[name]
        assert fs.modes == ("pure-core",), name
        assert fs.fingerprint is False, name


def test_complex_arg_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_COMPLEX_ARG & core)


def test_complex_arg_band_index_specs_require_two_bands():
    # evi/savi/index all read a 2nd band on the 2-band corpus
    for name in ("rst_evi", "rst_savi", "rst_index"):
        assert s.REGISTRY[name].min_bands == 2, name


def test_full_set_covers_every_registered_function():
    # The bench "full" set must cover EVERY registered rst_ function (no heavy-only
    # fn left unbenched, no bench fn that isn't registered). Derive the expectation
    # from registered_rst() so the test self-adjusts when a function is legitimately
    # added -- it fails only when bench coverage and registration actually diverge,
    # not on a magic count. (The bench REGISTRY is the light-side mirror of the
    # canonical registered_functions.txt list.)
    assert {f.name for f in s.select(set="full")} == s.registered_rst()


# --- bucket C, group C1/C2: readers + buildoverviews + subdataset fns (6) ----
# C1 (compared): rst_tryopen (bytes->scalar), rst_fromcontent (bytes->raster),
# rst_fromfile (path->raster), rst_buildoverviews (tile->raster). C2 (timing-
# only): rst_subdatasets, rst_getsubdataset — a plain GTiff corpus tile has no
# subdatasets (empty map / no match), so they are timed but never compared.
#
# These introduce the `input_kind` adapter on FnSpec: the bytes/path readers do
# NOT receive an opened dataset — the runner hands core_fn the raw raster bytes
# ("bytes") or the corpus file path ("path") instead of an open ds ("tile",
# the default that preserves every pre-existing function).
_C1 = {"rst_tryopen", "rst_fromcontent", "rst_fromfile", "rst_buildoverviews"}
_C2 = {"rst_subdatasets", "rst_getsubdataset"}

_C1_INPUT_KIND = {
    "rst_tryopen": "bytes",
    "rst_fromcontent": "bytes",
    "rst_fromfile": "path",
    "rst_buildoverviews": "tile",
}


def test_fnspec_input_kind_defaults_tile():
    fs = s.FnSpec("x", "gbx_x", "accessor", ("pure-core",))
    assert fs.input_kind == "tile"


def test_bucket_c_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = (_C1 | _C2) - full
    assert not missing, f"registry missing bucket-C fns: {sorted(missing)}"


def test_c1_wellformed_input_kind_and_fingerprint():
    for name in _C1:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        assert fs.input_kind == _C1_INPUT_KIND[name], name
        assert fs.fingerprint is True, name
        assert "pure-core" in fs.modes, name


def test_c1_compared_modes():
    # tryopen, fromcontent, buildoverviews run both modes; fromfile is pure-core
    # only because the spark-path tile DataFrame carries no file-path column.
    for name in ("rst_tryopen", "rst_fromcontent", "rst_buildoverviews"):
        fs = s.REGISTRY[name]
        assert "pure-core" in fs.modes and "spark-path" in fs.modes, name
    assert s.REGISTRY["rst_fromfile"].modes == ("pure-core",)


def test_c2_timing_only():
    for name in _C2:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.core is False
        assert fs.fingerprint is False, name
        assert fs.modes == ("pure-core",), name
        assert fs.input_kind == "tile", name


def test_bucket_c_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not ((_C1 | _C2) & core)


# --- bucket C, group C3: multi-tile-input functions (3) ----------------------
# rst_frombands / rst_combineavg / rst_merge each consume an ARRAY of tiles. The
# corpus row gives ONE tile, so the runner SYNTHESIZES the multi-tile input from
# it (bench.synth) and writes it to disk ONCE, so both engines read byte-identical
# files. These introduce input_kind == "tile_array": the runner synthesizes, opens
# each into a ds list, and feeds core_fn(ds_list, args); the col_fn builds a Spark
# ARRAY<tile> column from the same synthesized tiles. All produce a raster tile, so
# the output is compared via the raster fingerprint, and all run both modes.
_C3 = {"rst_frombands", "rst_combineavg", "rst_merge"}

_C3_SYNTH = {
    "rst_frombands": "frombands",
    "rst_combineavg": "combineavg",
    "rst_merge": "merge",
}


def test_c3_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _C3 - full
    assert not missing, f"registry missing C3 fns: {sorted(missing)}"


def test_c3_wellformed_tile_array_and_fingerprint():
    for name in _C3:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        assert fs.input_kind == "tile_array", name
        assert fs.fingerprint is True, name
        assert fs.sources, name


def test_c3_both_modes():
    for name in _C3:
        fs = s.REGISTRY[name]
        assert "pure-core" in fs.modes and "spark-path" in fs.modes, name


def test_c3_synth_recipe_maps_to_known_recipe():
    from databricks.labs.gbx.bench import synth

    for name, recipe in _C3_SYNTH.items():
        assert s.synth_recipe(name) == recipe, name
        # the recipe is one the synthesizer actually implements
        assert recipe in synth._RECIPES


def test_c3_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_C3 & core)


# --- bucket C, group C4: tiling functions (5) --------------------------------
# rst_maketiles / rst_retile / rst_tooverlappingtiles / rst_separatebands /
# rst_xyzpyramid each take ONE tile and emit a COLLECTION of tiles. They run on
# the default input_kind == "tile" (a single open dataset), but their core_fn
# returns a LIST of tile bytes, so the runner fingerprints the list with the new
# `raster_collection` fingerprint (tile count + pooled, order-independent agg).
# All run both modes (the spark-path col_fn yields an ARRAY column).
_C4 = {
    "rst_maketiles",
    "rst_retile",
    "rst_tooverlappingtiles",
    "rst_separatebands",
    "rst_xyzpyramid",
}

# rst_xyzpyramid is a tile-in / collection-out function like the rest of C4, but
# its emitted tiles are slippy-map XYZ renders: each is one rst_tilexyz render
# (the pyramid just loops RST_TileXYZ.execute over the intersecting (z,x,y) set).
# Like rst_tilexyz, the rendered bytes are render-engine-specific (heavy GDAL
# gdal_translate -of PNG vs light rio-tiler/PIL RGBA), so they cannot be made
# pooled-pixel-identical cross-engine. The intersecting tile COUNT already agrees,
# so the cell is TIMED but never pixel-compared: timing-only (pure-core,
# fingerprint=False), exactly like rst_tilexyz.
#
# Canonical-render note (follow-up, not fixed here): "XYZ pyramid" is the
# web-mercator slippy-map DISPLAY convention -> a rescaled RGBA PNG (0-255) is the
# canonical output, which is what the lightweight rio-tiler tier emits. The
# heavyweight RST_TileXYZ pipes raw source values through gdal_translate -of PNG
# (no rescale to RGBA), which is the NON-canonical render. A heavy-tier fix to
# emit a true RGBA web-map tile is deferred (Scala change; tracked separately).
_C4_TIMING_ONLY = {"rst_xyzpyramid"}
_C4_COMPARED = _C4 - _C4_TIMING_ONLY

_C4_ARGS = {
    "rst_maketiles": {"size_in_mb"},
    "rst_retile": {"tile_width", "tile_height"},
    "rst_tooverlappingtiles": {"tile_width", "tile_height", "overlap"},
    "rst_separatebands": set(),
    "rst_xyzpyramid": {"min_z", "max_z"},
}


def test_c4_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _C4 - full
    assert not missing, f"registry missing C4 fns: {sorted(missing)}"


def test_c4_wellformed_tile_in_collection_out():
    for name in _C4:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        # C4 takes a single open dataset (default input_kind), not a tile_array.
        assert fs.input_kind == "tile", name
        assert fs.sources, name


def test_c4_compared_fns_fingerprint_true():
    for name in _C4_COMPARED:
        fs = s.REGISTRY[name]
        assert fs.fingerprint is True, name


def test_c4_xyzpyramid_is_timing_only():
    # Render-engine-specific bytes (GDAL vs rio-tiler/PIL) -> count agrees,
    # pixels cannot match; timed but not compared, like rst_tilexyz.
    for name in _C4_TIMING_ONLY:
        fs = s.REGISTRY[name]
        assert fs.modes == ("pure-core",), name
        assert fs.fingerprint is False, name


def test_c4_compared_both_modes():
    for name in _C4_COMPARED:
        fs = s.REGISTRY[name]
        assert "pure-core" in fs.modes and "spark-path" in fs.modes, name


def test_c4_args_present():
    for name, keys in _C4_ARGS.items():
        fs = s.REGISTRY[name]
        assert set(fs.args.keys()) == keys, name


def test_c4_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_C4 & core)


# --- bucket B, group B-grid: DGGS functions (11) -----------------------------
# rst_h3_tessellate + the 10 rst_{h3,quadbin}_rastertogrid{avg,count,max,median,
# min} functions each map a raster into discrete-global-grid cells. Their core_fn
# returns a per-band list of cell records (gridagg.raster_to_grid) -- which is a
# list-of-lists, NOT bytes and NOT scalars -- so the runner's auto fingerprint
# detection would mis-classify it. They declare fingerprint_kind == "dggs_grid"
# so the runner routes the output through fingerprint_dggs_grid (cell count +
# sorted signed-int64 cell-id hash + order-independent agg over measures).
# H3/quadbin cell ids are PARITY-comparable across the heavy/light tiers.
#
# Resolutions are chosen for the small-extent corpus tiles (EPSG:4326 NYC,
# 256/512 px at 0.0001-deg): H3 res 7 (~1.2 km edge) lands a handful of cells
# (~5/band on a 256px tile); quadbin res 15 (~0.011-deg cell) lands ~12/band.
# (Non-4326 corpus tiles feed their projected origins to h3/quadbin as raw
# lon/lat -- exactly as the heavy tier does, no reprojection -- so the two
# engines stay consistent; out-of-range tiles surface identically on both.)
_DGGS = {
    "rst_h3_tessellate",
    "rst_h3_rastertogridavg",
    "rst_h3_rastertogridcount",
    "rst_h3_rastertogridmax",
    "rst_h3_rastertogridmedian",
    "rst_h3_rastertogridmin",
    "rst_h3_rastertogridsum",
    "rst_h3_rastertogridvariance",
    "rst_h3_rastertogridstddev",
    "rst_quadbin_rastertogridavg",
    "rst_quadbin_rastertogridcount",
    "rst_quadbin_rastertogridmax",
    "rst_quadbin_rastertogridmedian",
    "rst_quadbin_rastertogridmin",
    "rst_quadbin_rastertogridsum",
    "rst_quadbin_rastertogridvariance",
    "rst_quadbin_rastertogridstddev",
}

_DGGS_H3_RES = 7
_DGGS_QUADBIN_RES = 15


def test_dggs_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _DGGS - full
    assert not missing, f"registry missing DGGS fns: {sorted(missing)}"


def test_dggs_count_is_seventeen():
    assert len(_DGGS) == 17


def test_dggs_wellformed_dggs_grid_fingerprint():
    for name in _DGGS:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.category == "dggs"
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        assert fs.input_kind == "tile", name
        assert fs.fingerprint is True, name
        assert fs.fingerprint_kind == "dggs_grid", name
        assert fs.sources, name


def test_dggs_both_modes():
    for name in _DGGS:
        fs = s.REGISTRY[name]
        assert "pure-core" in fs.modes and "spark-path" in fs.modes, name


def test_dggs_resolution_args_valid():
    # H3 resolutions in [0, 15]; quadbin in [0, 20]. tessellate + the h3 grid fns
    # carry the H3 res; the quadbin grid fns carry the quadbin res.
    for name in _DGGS:
        fs = s.REGISTRY[name]
        assert "resolution" in fs.args, name
        res = fs.args["resolution"]
        if name.startswith("rst_quadbin_"):
            assert res == _DGGS_QUADBIN_RES and 0 <= res <= 20, name
        else:
            assert res == _DGGS_H3_RES and 0 <= res <= 15, name


def test_dggs_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_DGGS & core)


# --- bucket B, group B-vec: vector-out functions (2) -------------------------
# rst_contour (contour LINES) + rst_polygonize (POLYGONS) emit a set of vector
# features (geometry + a per-feature value), NOT bytes / scalars / a grid -- so
# they declare fingerprint_kind == "vector" to route the output through
# fingerprint_vector (feature COUNT + total measure [line length for lines,
# polygon area for polygons, chosen by geometry type] + order-independent agg
# over the per-feature attribute). The two engines may emit features in any
# order and still agree; count is compared exactly.
#
# Heavy arg defaults matched exactly: rst_contour uses FIXED_LEVELS, so the
# bench rides explicit fixed levels [0.2, 0.4, 0.6, 0.8] (the float32 corpus
# band is ~[0,1], so these span its range and trace a handful of contours);
# rst_polygonize uses band 1 + connectedness 4 (the heavy builder's defaults).
_BVEC = {"rst_contour", "rst_polygonize"}
_BVEC_CONTOUR_LEVELS = [0.2, 0.4, 0.6, 0.8]
_BVEC_POLYGONIZE_CONNECTEDNESS = 4


def test_bvec_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _BVEC - full
    assert not missing, f"registry missing B-vec fns: {sorted(missing)}"


def test_bvec_count_is_two():
    assert len(_BVEC) == 2


def test_bvec_wellformed_vector_fingerprint():
    for name in _BVEC:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}"
        assert fs.category == "vector"
        assert fs.core is False
        assert callable(fs.core_fn) and callable(fs.col_fn)
        assert fs.input_kind == "tile", name
        assert fs.fingerprint is True, name
        assert fs.fingerprint_kind == "vector", name
        assert fs.sources, name


def test_bvec_both_modes():
    for name in _BVEC:
        fs = s.REGISTRY[name]
        assert "pure-core" in fs.modes and "spark-path" in fs.modes, name


def test_bvec_args_match_heavy_defaults():
    contour = s.REGISTRY["rst_contour"]
    assert contour.args["levels"] == _BVEC_CONTOUR_LEVELS
    poly = s.REGISTRY["rst_polygonize"]
    assert poly.args["band"] == 1
    assert poly.args["connectedness"] == _BVEC_POLYGONIZE_CONNECTEDNESS


def test_bvec_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_BVEC & core)


def test_fnspec_fingerprint_kind_defaults_auto():
    fs = s.FnSpec("x", "gbx_x", "accessor", ("pure-core",))
    assert fs.fingerprint_kind == "auto"


# --- bucket D: geometry-in functions (3) -------------------------------------
# rst_rasterize / rst_gridfrompoints / rst_dtmfromgeoms each take GEOMETRY input
# (a polygon to burn, an array of POINTs to interpolate, an array of 3D POINTs to
# triangulate) and PRODUCE a raster tile. No single literal geometry is in-extent
# across the multi-CRS corpus, so they ride input_kind == "geometry": the runner
# hands core_fn(ds, args, geom) the tile's GeometrySet (boxes / points / zpoints
# as WKB + burn values, in the tile CRS, deterministic + identical across both
# engines via geometry.json). The output is a raster, compared via the raster
# fingerprint. The extent/size/srid come from the tile ds the geometry was derived
# from (ds.bounds, ds.width/height, ds CRS -> srid), so the burn grid aligns with
# the source tile on every CRS. Pure-core-only: the spark-path tile DataFrame
# carries no geometry column.
_BUCKET_D = {"rst_rasterize", "rst_gridfrompoints", "rst_dtmfromgeoms"}


def test_bucket_d_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _BUCKET_D - full
    assert not missing, f"registry missing bucket-D fns: {sorted(missing)}"


def test_bucket_d_count_is_three():
    assert len(_BUCKET_D) == 3


def test_bucket_d_wellformed_geometry_input_kind():
    for name in _BUCKET_D:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}", name
        assert fs.core is False, name
        assert callable(fs.core_fn) and callable(fs.col_fn), name
        assert fs.input_kind == "geometry", name
        assert fs.fingerprint is True, name
        # raster output -> the auto fingerprint detector classifies the GTiff bytes
        assert fs.fingerprint_kind == "auto", name
        assert fs.sources, name


def test_bucket_d_pure_core_only():
    # spark-path tile DataFrame has no geometry column -> pure-core only.
    for name in _BUCKET_D:
        assert s.REGISTRY[name].modes == ("pure-core",), name


def test_bucket_d_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_BUCKET_D & core)


def test_bucket_d_core_fn_takes_three_args():
    # input_kind == "geometry" core_fn signature is (ds, args, geom).
    import inspect

    for name in _BUCKET_D:
        sig = inspect.signature(s.REGISTRY[name].core_fn)
        assert len(sig.parameters) == 3, name


# --- bucket A: the 7 *_agg aggregators (Spark groupBy aggregate harness) ------
# rst_combineavg_agg / rst_merge_agg / rst_frombands_agg / rst_derivedband_agg
# (tile aggregators) + rst_rasterize_agg / rst_gridfrompoints_agg /
# rst_dtmfromgeoms_agg (geometry aggregators) each reduce a GROUP of rows to ONE
# output tile via a real Spark df.groupBy(key).agg(col_fn(...)). They run ONLY in
# the spark-path mode (there is no single-row pure-core analogue of a UDAF). The
# four tile aggregators ride input_kind == "tile_aggregate" (the harness builds a
# tile DataFrame + a key, and for consistency aggregates a FIXED deterministic
# group of the synthesized tiles into ONE tile -> raster fingerprint). The three
# geometry aggregators ride input_kind == "geometry_aggregate" (the harness builds
# rows of (geom_wkb, value[, ...]) from the per-tile GeometrySet + a key, and
# aggregates the fixed group into ONE tile -> raster fingerprint). The output tile
# is fingerprinted as a raster (auto). All are core=False.
_BUCKET_A = {
    "rst_combineavg_agg",
    "rst_merge_agg",
    "rst_frombands_agg",
    "rst_derivedband_agg",
    "rst_rasterize_agg",
    "rst_gridfrompoints_agg",
    "rst_dtmfromgeoms_agg",
}
_BUCKET_A_TILE = {
    "rst_combineavg_agg",
    "rst_merge_agg",
    "rst_frombands_agg",
    "rst_derivedband_agg",
}
_BUCKET_A_GEOMETRY = {
    "rst_rasterize_agg",
    "rst_gridfrompoints_agg",
    "rst_dtmfromgeoms_agg",
}
# the synth recipe each tile aggregator consumes for its fixed consistency group:
# combineavg over aligned copies, merge over offset copies, frombands/derivedband
# over the per-band split (each band tile is one group row / one band input).
_BUCKET_A_SYNTH = {
    "rst_combineavg_agg": "combineavg",
    "rst_merge_agg": "merge",
    "rst_frombands_agg": "frombands",
    "rst_derivedband_agg": "frombands",
}


def test_bucket_a_partition_is_total():
    assert _BUCKET_A_TILE | _BUCKET_A_GEOMETRY == _BUCKET_A
    assert not (_BUCKET_A_TILE & _BUCKET_A_GEOMETRY)
    assert len(_BUCKET_A) == 7
    assert len(_BUCKET_A_TILE) == 4
    assert len(_BUCKET_A_GEOMETRY) == 3


def test_bucket_a_registered_in_full():
    full = {f.name for f in s.select(set="full")}
    missing = _BUCKET_A - full
    assert not missing, f"registry missing bucket-A aggregators: {sorted(missing)}"


def test_bucket_a_wellformed_aggregate_input_kind():
    for name in _BUCKET_A:
        fs = s.REGISTRY[name]
        assert fs.sql_name == f"gbx_{name}", name
        assert fs.core is False, name
        assert callable(fs.core_fn) and callable(fs.col_fn), name
        assert fs.fingerprint is True, name
        # raster output -> the auto fingerprint detector classifies the GTiff bytes
        assert fs.fingerprint_kind == "auto", name
        assert fs.sources, name


def test_bucket_a_tile_aggregators_input_kind():
    for name in _BUCKET_A_TILE:
        assert s.REGISTRY[name].input_kind == "tile_aggregate", name


def test_bucket_a_geometry_aggregators_input_kind():
    for name in _BUCKET_A_GEOMETRY:
        assert s.REGISTRY[name].input_kind == "geometry_aggregate", name


def test_bucket_a_spark_path_only():
    # there is no single-row pure-core analogue of a UDAF: aggregate-only.
    for name in _BUCKET_A:
        assert s.REGISTRY[name].modes == ("spark-path",), name


def test_bucket_a_not_in_core_set():
    core = {f.name for f in s.select(set="core")}
    assert not (_BUCKET_A & core)


def test_bucket_a_tile_synth_recipe_maps_to_known_recipe():
    from databricks.labs.gbx.bench import synth

    for name, recipe in _BUCKET_A_SYNTH.items():
        assert s.agg_synth_recipe(name) == recipe, name
        assert recipe in synth._RECIPES


def test_bucket_a_gridfrompoints_args():
    fs = s.REGISTRY["rst_gridfrompoints_agg"]
    assert fs.args["power"] == 2.0
    # max_pts is a large sentinel (>= any corpus point count) so BOTH tiers IDW
    # over ALL points. gdal_grid `invdist` (no search radius) ignores max_points;
    # feeding a small max_pts made the lightweight cKDTree pick only the nearest k,
    # diverging from heavy's all-points grid by the neighbor-selection artifact.
    assert fs.args["max_pts"] >= 64


def test_gridfrompoints_uses_all_points_for_idw_parity():
    """Both rst_gridfrompoints and its aggregator must feed an all-points max_pts.

    Heavy gdal_grid `invdist` with no radius interpolates from every point and
    ignores max_points; the lightweight idw_grid does a nearest-max_pts selection.
    A small max_pts therefore compares DIFFERENT effective point sets. Asserting a
    large sentinel locks in the arg-fix that keeps the two IDW grids comparable.
    """
    for name in ("rst_gridfrompoints", "rst_gridfrompoints_agg"):
        fs = s.REGISTRY[name]
        assert fs.args["max_pts"] >= 64, name
        assert fs.args["power"] == 2.0, name


def test_aggregators_are_spark_path_only():
    """Every *_agg aggregator declares spark-path ONLY (no pure-core UDAF analogue).

    gbx:bench:changed must run affected fns in their DECLARED modes. When it
    hardcoded --modes pure-core, the runner's per-fn filter (`if "pure-core" not in
    fs.modes: continue`) skipped all 7 aggregators on the lightweight side -> 0 rows
    and no consistency captured. This locks in that aggregators are spark-path-only,
    so a validation run MUST include spark-path (the fix runs --modes both).
    """
    for name in _BUCKET_A:
        assert s.REGISTRY[name].modes == ("spark-path",), name


def test_both_modes_select_each_fn_in_its_declared_mode():
    """The runner's per-fn mode filter, exercised over a mixed affected set.

    A change-aware run over a pure-core fn + a spark-path aggregator must, under
    "both", run the pure-core fn pure-core-only and the aggregator spark-path-only
    -- i.e. "both" is the UNION of the affected fns' modes, not every fn in every
    mode. This is the behavior the gbx:bench:changed --modes both fix relies on.
    """
    mixed = s.select(functions=["rst_slope", "rst_combineavg_agg"])
    pure = [f for f in mixed if "pure-core" in f.modes]
    spark = [f for f in mixed if "spark-path" in f.modes]
    assert {f.name for f in pure} == {"rst_slope"}
    assert {f.name for f in spark} == {"rst_slope", "rst_combineavg_agg"}
    # the aggregator is NOT in the pure-core leg (would be a skipped 0-row fn)
    assert "rst_combineavg_agg" not in {f.name for f in pure}


def test_bucket_a_dtmfromgeoms_tolerances_zero():
    fs = s.REGISTRY["rst_dtmfromgeoms_agg"]
    assert fs.args["merge_tolerance"] == 0.0
    assert fs.args["snap_tolerance"] == 0.0


def test_bucket_a_derivedband_func_name_present():
    fs = s.REGISTRY["rst_derivedband_agg"]
    assert fs.args["func_name"] == "mean_bands"


def test_full_set_size_equals_registered_count():
    # The "full" bench set size == the registered rst_ count (every registered fn
    # is benched, and nothing extra). Derived from registered_rst() so a legitimate
    # function addition self-adjusts; only a real coverage/registration divergence
    # fails this. Every entry is unique (REGISTRY is keyed by name).
    full = s.select(set="full")
    assert len({f.name for f in full}) == len(full)  # no dupes
    assert len(full) == len(s.registered_rst())


def test_every_fnspec_declares_existing_sources():
    from pathlib import Path

    root = Path(__file__).resolve()
    for _ in range(12):
        if (root / "pom.xml").exists():
            break
        root = root.parent
    for f in s.select(set="full"):
        assert f.sources, f"{f.name} has no sources"
        for p in f.sources:
            assert (root / p).exists(), f"{f.name}: missing source {p}"


# --- Task 3: accessor virtual-tile disposition classification ----------------


def test_pixel_reading_accessors_are_materialized():
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


def test_header_accessors_are_deferred():
    for fn in (
        "rst_width",
        "rst_height",
        "rst_numbands",
        "rst_srid",
        "rst_pixelwidth",
        "rst_georeference",
        "rst_boundingbox",
    ):
        assert s.accessor_disposition(fn) == "deferred", fn


def test_virtual_disposition_override_wins():
    fs = s.REGISTRY["rst_width"]
    fs2 = __import__("dataclasses").replace(fs, virtual_disposition="materialized")
    assert s.accessor_disposition("rst_width", fs2) == "materialized"
