"""Oracle table for the generic GDAL command of the GDAL spec engine family (`gbx_rst_gdal`): inputs -> expected, run at
every level by test_family_cross_tier.py.

The pixels of a step come from GDAL's own `gdal.DEMProcessing` (oracles._ref_gdaldem_slope), never from the function under
test; a pipeline is judged by what its last step must write (its CRS, size and type). The refusal rows list one case for
each way a command could leave the tile it is given: a step outside the list, an engine argument, a path or an `@file`
value, a CRS or a polygon that names a file, a nested pipeline. The exhaustive matrix (every allowed argument against every
file-like value) is `test/engine/test_command.py`.
"""

from databricks.labs.gbx.bench.oracles import UPSTREAM_ERROR, Build, Case

G = "gbx_rst_gdal"
DEM, VIRT, ERR = Build("dem_3857"), Build("dem_file"), Build("tile_error")
CORRUPT, MISSING = Build("tile_corrupt"), Build("vz_missing")
ONLY = ("session",)

# A refused command raises, naming the cause; each row is a way to leave the tile.
_REFUSED = (
    ("short_flag", "slope -p", "-p"),
    ("loose_value", "slope --unit percent", "unit"),
    ("write_step", "write --output=/tmp/gbx_oracle_out.tif", "write"),
    ("read_step", "read --input=/etc/passwd", "read"),
    ("vector_step", "vector buffer", "vector"),
    ("dialect", "calc --calc=A --dialect=builtin", "calc"),
    ("config_arg", "slope --config=GDAL_CACHEMAX=1", "config"),
    ("output_override", "slope --output=/tmp/gbx_oracle_out.tif", "output"),
    ("input_override", "slope --input=/etc/passwd", "input"),
    ("creation_option", "slope --creation-option=COMPRESS=NONE", "creation-option"),
    ("like_file", "reproject --like=/Volumes/c/s/v/x.tif", "like"),
    ("path_value", "reclassify --mapping=@/etc/passwd", "mapping"),
    ("crs_path", "reproject --output-crs=/etc/hostname", "output-crs"),
    (
        "bbox_crs_file",
        "reproject --output-crs=3857 --bbox=0,0,1,1 --bbox-crs=foo.prj",
        "bbox-crs",
    ),
    ("geometry_path", "clip --geometry=/tmp/cut.geojson", "geometry"),
    ("nested_pipeline", "slope ! [ tpi ]", "["),
    (
        "separator_in_value",
        'slope "--unit=percent ! write --output=/tmp/x.tif"',
        "unit",
    ),
    ("unterminated_quote", 'reclassify --mapping="[0,1]=1', "quote"),
)

SPEC_GDAL = (
    # a step gives the pixels of GDAL's own command (a 3 x 3 window: the border is NoData)
    Case(
        "gdal_slope_percent",
        G,
        (DEM, "slope --unit=percent"),
        "ref",
        "gdaldem_slope_percent",
        ONLY,
    ),
    Case(
        "gdal_slope_virtual",
        G,
        (VIRT, "slope --unit=percent"),
        "ref",
        "gdaldem_slope_percent",
        ONLY,
    ),
    # a pipeline runs its steps in order: the last one decides the type, the first the CRS
    Case(
        "gdal_pipeline",
        G,
        (DEM, "reproject --output-crs=EPSG:4326 ! slope"),
        "tile_meta",
        {"crs": "EPSG:4326", "dtype": "float32", "count": 1},
        ONLY,
    ),
    Case(
        "gdal_resize",
        G,
        (DEM, "resize --size=32,32"),
        "tile_meta",
        {"width": 32, "height": 32},
        ONLY,
    ),
    Case(
        "gdal_window",
        G,
        (DEM, "clip --window=0,0,16,8"),
        "tile_meta",
        {"width": 16, "height": 8},
        ONLY,
    ),
    Case(
        "gdal_chain_type",
        G,
        (DEM, "resize --size=32,32 ! set-type --output-data-type=Int16"),
        "tile_meta",
        {"width": 32, "height": 32, "dtype": "int16"},
        ONLY,
    ),
    Case(
        "gdal_quoted_mapping",
        G,
        (DEM, 'reclassify --mapping="[0,1000]=1; DEFAULT=0" --output-data-type=Byte'),
        "tile_meta",
        {"dtype": "uint8"},
        ONLY,
    ),
    # the refusals
    *(
        Case(f"gdal_{cid}", G, (DEM, cmd), "raises", names, ONLY)
        for cid, cmd, names in _REFUSED
    ),
    # GDAL refusing the arguments is the caller's: raised, naming the cause
    Case(
        "gdal_missing_kernel",
        G,
        (DEM, "neighbors --method=mean --size=3"),
        "raises",
        "kernel",
        ONLY,
    ),
    # a failure in the run is the tile's: an error tile with GDAL's reason
    Case(
        "gdal_run_failure",
        G,
        (DEM, 'reclassify --mapping="[0,20]=1; [20,100]=2"'),
        "error_tile_has",
        "overlaps",
        ONLY,
    ),
    # a refusal GDAL still makes after the grammar is the tile's: a tile with no CRS gives an error tile that says how to set one
    Case(
        "gdal_reproject_no_crs",
        G,
        (Build("tile_nocrs"), "reproject --output-crs=3857"),
        "error_fix",
        None,
        ONLY,
    ),
    # the input rows
    Case("gdal_error_in", G, (ERR, "slope"), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("gdal_corrupt", G, (CORRUPT, "slope"), "error_tile", None, ONLY),
    Case("gdal_null_tile", G, (None, "slope"), "null", None, ONLY),
    Case("gdal_null_command", G, (DEM, None), "null", None, ONLY),
    # a virtual tile whose file is not there is retried, then raises naming the path
    Case(
        "gdal_missing_path", G, (MISSING, "slope"), "raises", "gbx_missing_tile", ONLY
    ),
)
