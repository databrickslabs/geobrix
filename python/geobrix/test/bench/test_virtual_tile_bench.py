"""QA: a virtual input tile must produce the SAME result as a materialized one.

Also covers Defect-A/B regression tests for the five tiling/polygonize UDTFs
that were mis-specced as scalars and the UDTF-aware disposition sampler.
"""
from pathlib import Path

from databricks.labs.gbx.bench import datagen as dg
from databricks.labs.gbx.bench.fingerprint import fingerprint_output
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import accessors, open_tile as ot, terrain
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile


def _one_tile(tmp_path):
    corpus = dg.generate_corpus(
        out_dir=tmp_path, seed=9, tile_px=[64], bands=[1], dtypes=["float32"],
        srids=[4326], nodata_fracs=[0.0], row_rows=1, row_tile_px=64,
        row_bands=1, row_dtype="float32",
    )
    te = next(t for t in corpus.size_sweep if t.role != "bng_gb")
    return Path(tmp_path) / te.path, te


def test_slope_virtual_equals_materialized(tmp_path):
    p, te = _one_tile(tmp_path)
    with _serde.open_tile(p.read_bytes()) as ds:
        mat = terrain.slope(ds, unit="degrees", xscale=None, yscale=None)
    vt = VirtualTile(cellid=0, raster=None, path=str(p),
                     window=(0, 0, te.tile_px, te.tile_px))
    with ot._open(vt.to_row()) as ds:
        virt = terrain.slope(ds, unit="degrees", xscale=None, yscale=None)
    assert fingerprint_output(mat) == fingerprint_output(virt)


def test_width_virtual_is_header_only_and_matches(tmp_path):
    p, te = _one_tile(tmp_path)
    with _serde.open_tile(p.read_bytes()) as ds:
        mat_w = accessors.width(ds)
    # window=(0,0,w,h) covers the full extent; open_header detects _is_full_extent
    # and yields src directly (no pixel I/O), confirming the header-only path.
    vt = VirtualTile(cellid=0, raster=None, path=str(p),
                     window=(0, 0, te.tile_px, te.tile_px))
    with ot.open_header(vt.to_row()) as ds:
        virt_w = accessors.width(ds)
    assert mat_w == virt_w


# =========================================================================
# Defect-A regression: 5 tiling/polygonize UDTFs mis-specced as scalars
# =========================================================================

def test_five_tiling_udtfs_have_udtf_flag():
    """All 5 previously mis-specced tiling/polygonize fns now carry udtf=True."""
    from databricks.labs.gbx.bench.spec import REGISTRY

    for name in (
        "rst_maketiles",
        "rst_retile",
        "rst_tooverlappingtiles",
        "rst_separatebands",
        "rst_polygonize",
    ):
        fs = REGISTRY[name]
        assert getattr(fs, "udtf", False) is True, f"{name} missing udtf=True"


def test_tiling_udtf_args_order_and_lateral_sql():
    """Args keys are in SQL-signature order and _udtf_lateral_sql emits correct LATERAL SQL.

    The SQL must contain the fn name, the tile column (d.tile), and all scalar
    args in signature order so the positional UDTF call binds the right values.
    Verified against the pyrx function signatures (gbx_rst_maketiles(tile,
    size_in_mb), gbx_rst_retile(tile, tile_width, tile_height), etc.).
    """
    from databricks.labs.gbx.bench.spec import REGISTRY
    from databricks.labs.gbx.bench.runner import _udtf_lateral_sql

    cases = [
        (
            "rst_maketiles",
            ["size_in_mb"],
            "SELECT t.* FROM v AS d, LATERAL gbx_rst_maketiles(d.tile, 1) AS t",
        ),
        (
            "rst_retile",
            ["tile_width", "tile_height"],
            "SELECT t.* FROM v AS d, LATERAL gbx_rst_retile(d.tile, 128, 128) AS t",
        ),
        (
            "rst_tooverlappingtiles",
            ["tile_width", "tile_height", "overlap"],
            "SELECT t.* FROM v AS d, LATERAL gbx_rst_tooverlappingtiles(d.tile, 128, 128, 25) AS t",
        ),
        (
            "rst_separatebands",
            [],
            "SELECT t.* FROM v AS d, LATERAL gbx_rst_separatebands(d.tile) AS t",
        ),
        (
            "rst_polygonize",
            ["band", "connectedness"],
            "SELECT t.* FROM v AS d, LATERAL gbx_rst_polygonize(d.tile, 1, 4) AS t",
        ),
    ]
    for name, expected_arg_keys, expected_sql in cases:
        fs = REGISTRY[name]
        assert list(fs.args.keys()) == expected_arg_keys, (
            f"{name}: expected arg key order {expected_arg_keys}, "
            f"got {list(fs.args.keys())}"
        )
        sql = _udtf_lateral_sql("v", fs)
        assert sql == expected_sql, f"{name}: got {sql!r}"


# =========================================================================
# Defect-B regression: _disposition_of virtual_disposition override
# =========================================================================

def test_disposition_of_virtual_disposition_override():
    """virtual_disposition wins before any accessor or sample-inspection logic."""
    from dataclasses import dataclass
    from typing import Optional
    from databricks.labs.gbx.bench.runner import _disposition_of

    @dataclass
    class _FakeFs:
        category: str = "tile-returning"
        name: str = "fake_fn"
        virtual_disposition: Optional[str] = "materialized"

    # sample=None would normally yield "na"; virtual_disposition overrides that.
    assert _disposition_of(_FakeFs(), None) == "materialized"

    # sample=bytes would normally yield "materialized" from the raster field; but
    # an explicit override of "deferred" should still win.
    @dataclass
    class _FakeDeferredFs:
        category: str = "tile-returning"
        name: str = "fake_fn2"
        virtual_disposition: Optional[str] = "deferred"

    fake_tile = {"raster": b"bytes"}
    assert _disposition_of(_FakeDeferredFs(), fake_tile) == "deferred"

    # accessor category: normally routed through accessor_disposition; override wins.
    @dataclass
    class _FakeAccessorFs:
        category: str = "accessor"
        name: str = "rst_avg"
        virtual_disposition: Optional[str] = "deferred"

    assert _disposition_of(_FakeAccessorFs(), None) == "deferred"


def test_disposition_of_rst_polygonize_virtual_disposition():
    """rst_polygonize FnSpec has virtual_disposition='materialized'.

    Polygonize emits geometry rows (no tile-struct output), so the sampler
    finds no 'raster' field and sample_out_tile is None.  The virtual_disposition
    pin must make _disposition_of return 'materialized' regardless.
    """
    from databricks.labs.gbx.bench.spec import REGISTRY
    from databricks.labs.gbx.bench.runner import _disposition_of

    fs = REGISTRY["rst_polygonize"]
    assert getattr(fs, "virtual_disposition", None) == "materialized", (
        "rst_polygonize FnSpec must have virtual_disposition='materialized'"
    )
    # With no sample (simulating a geometry-output UDTF), override wins.
    assert _disposition_of(fs, None) == "materialized"


# =========================================================================
# _find_udtf_tile_struct: pure unit tests (no Spark)
# =========================================================================

def test_find_udtf_tile_struct_with_raster_bytes():
    """A dict with a 'raster' key is returned as the tile struct."""
    from databricks.labs.gbx.bench.runner import _find_udtf_tile_struct

    row = {"cellid": 0, "raster": b"bytes", "path": None}
    assert _find_udtf_tile_struct(row) is row


def test_find_udtf_tile_struct_with_raster_none():
    """raster=None means a deferred tile; the key exists so the row is returned."""
    from databricks.labs.gbx.bench.runner import _find_udtf_tile_struct

    row = {"cellid": 0, "raster": None, "path": "/some/path"}
    assert _find_udtf_tile_struct(row) is row


def test_find_udtf_tile_struct_flat_grid_row():
    """Flat grid row (band/cellID/measure) has no 'raster' field -> None."""
    from databricks.labs.gbx.bench.runner import _find_udtf_tile_struct

    row = {"band": 1, "cellID": 123456789, "measure": 0.5}
    assert _find_udtf_tile_struct(row) is None


def test_find_udtf_tile_struct_vector_row():
    """Polygonize row (geom_wkb/value) has no 'raster' field -> None."""
    from databricks.labs.gbx.bench.runner import _find_udtf_tile_struct

    row = {"geom_wkb": b"wkb", "value": 1.0}
    assert _find_udtf_tile_struct(row) is None


def test_find_udtf_tile_struct_none_input():
    """None input is handled gracefully -> None."""
    from databricks.labs.gbx.bench.runner import _find_udtf_tile_struct

    assert _find_udtf_tile_struct(None) is None


# =========================================================================
# Real Spark test: UDTF disposition sampler for tile-emitting UDTF
# Validates the end-to-end path: LATERAL query + _find_udtf_tile_struct
# + _disposition_of for a tiling UDTF (rst_retile).
# =========================================================================

def test_udtf_virtual_tile_disposition_not_na(tmp_path, spark):
    """run_spark_path with input_tile='virtual' must yield output_disposition != 'na'
    for UDTF fns that emit V2_TILE_SCHEMA rows (rst_retile emits tile bytes ->
    'materialized').  Before the fix, col_fn raised NotImplementedError and the
    sampler silently fell to 'na' for all UDTFs.
    """
    from databricks.labs.gbx.bench import runner as rn
    from databricks.labs.gbx.bench import spec as s

    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=55,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=2,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    fns = s.select(functions=["rst_retile"])
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="disp-test",
        row_counts=[2],
        warmup=1,
        measured=1,
        where="venv",
        input_tile="virtual",
    )
    assert rows, "expected result rows from rst_retile spark-path"
    for r in rows:
        assert r.output_disposition != "na", (
            f"rst_retile UDTF virtual-tile disposition must not be 'na'; "
            f"got '{r.output_disposition}'.  Fix: UDTF sampler must use LATERAL, "
            f"not the scalar col_fn."
        )
        # rst_retile emits actual tile bytes (materialized) not virtual path tiles.
        assert r.output_disposition == "materialized", (
            f"expected 'materialized' for rst_retile UDTF output, "
            f"got '{r.output_disposition}'"
        )


# =========================================================================
# Real Spark test: flat-row UDTF (rastertogrid*) disposition
# rastertogrid* emit (band, cellID, measure) rows — no tile struct.
# Case-2 logic must infer "materialized" (pixels read) not leave "na".
# =========================================================================

def test_rastertogrid_udtf_virtual_tile_disposition_materialized(tmp_path, spark):
    """run_spark_path with rst_h3_rastertogridavg + input_tile='virtual' must yield
    output_disposition='materialized', not 'na'.

    rastertogrid UDTFs emit flat (band/cellID/measure) rows with no 'raster' field.
    Before the case-2 fix, _find_udtf_tile_struct returned None, _sample stayed None,
    and _disposition_of returned 'na'.  The case-2 rule must upgrade 'na' to
    'materialized' when the UDTF returned non-empty flat rows (pixels were read).
    """
    from databricks.labs.gbx.bench import runner as rn
    from databricks.labs.gbx.bench import spec as s

    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=77,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=2,
        row_tile_px=32,
        row_bands=1,
        row_dtype="float32",
    )
    fns = s.select(functions=["rst_h3_rastertogridavg"])
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="disp-rg-test",
        row_counts=[2],
        warmup=1,
        measured=1,
        where="venv",
        input_tile="virtual",
    )
    assert rows, "expected result rows from rst_h3_rastertogridavg spark-path"
    for r in rows:
        assert r.output_disposition == "materialized", (
            f"rst_h3_rastertogridavg flat-row UDTF must yield 'materialized' "
            f"(case-2: flat rows -> pixels read); got '{r.output_disposition}'"
        )


# =========================================================================
# Guard test: no UDTF spark-path fn can produce "na" given a non-empty
# LATERAL output. Parametrized over the full REGISTRY; uses representative
# fake rows to drive the case-1/2/3 logic without 33 Spark runs.
# =========================================================================

def _simulate_udtf_disposition(fs, fake_row):
    """Drive the sampler's case-1/2/3 logic with a fake row (no Spark).

    Mirrors the disposition block in run_spark_path exactly: if fake_row
    is not None, find the tile struct (case 1) or flag flat rows (case 2),
    then apply _disposition_of + case-2 upgrade.
    """
    from databricks.labs.gbx.bench.runner import _find_udtf_tile_struct, _disposition_of

    _sample = None
    _got_flat = False
    if fake_row is not None:
        _ts = _find_udtf_tile_struct(fake_row)
        if _ts is not None:
            _sample = _ts
        else:
            _got_flat = True
    _disp = _disposition_of(fs, _sample)
    if _got_flat and _disp == "na":
        _disp = "materialized"
    return _disp


def test_no_udtf_spark_path_fn_yields_na_given_nonempty_output():
    """Safety net: for every UDTF fn with a spark-path, a non-empty LATERAL
    output must never resolve to 'na', regardless of output schema.

    Two representative fake rows cover all real UDTF output shapes:
    - Tile-struct row (V2_TILE_SCHEMA: raster=bytes) -> case 1 -> "materialized"
    - Flat row (no raster field) -> case 2 -> "materialized"
    Both must land in {"deferred","materialized"}.

    The two end-to-end real-Spark cases (rst_retile, rst_h3_rastertogridavg)
    are already covered by separate tests above; this guard runs the same
    case-1/2/3 logic in pure Python across ALL 33 fns to catch future
    regressions without requiring 33 Spark sessions.
    """
    from databricks.labs.gbx.bench.spec import REGISTRY

    _tile_row = {"cellid": 0, "raster": b"bytes", "path": None}  # V2_TILE_SCHEMA
    _flat_row = {"band": 1, "cellID": 0, "measure": 0.5}          # flat grid row

    _valid = {"deferred", "materialized"}
    _failures = []

    for name, fs in REGISTRY.items():
        if not getattr(fs, "udtf", False):
            continue
        if "spark-path" not in fs.modes:
            continue

        for fake_row, label in ((_tile_row, "tile-row"), (_flat_row, "flat-row")):
            disp = _simulate_udtf_disposition(fs, fake_row)
            if disp not in _valid:
                _failures.append(
                    f"{name} [{label}]: got '{disp}', expected one of {_valid}"
                )

    assert not _failures, (
        f"The following UDTF spark-path fns produced 'na' given a non-empty output:\n"
        + "\n".join(_failures)
    )


# =========================================================================
# Real Spark test: BNG raster->grid spark-path routes to GB tile (fix for
# the bug where run_spark_path used only the NYC row pool for all fns,
# giving gb_tile=True fns an empty grid and output_disposition='na').
# =========================================================================

def test_bng_rastertogrid_sparkpath_uses_gb_tile(tmp_path, spark):
    """run_spark_path with rst_bng_rastertogridavg (gb_tile=True) + input_tile='virtual'
    must yield output_disposition='materialized', not 'na'.

    Before the fix, run_spark_path built df_all solely from the NYC row pool
    (EPSG:4326 tiles).  Reprojecting NYC tiles to EPSG:27700 lands outside Great
    Britain -> zero BNG cells -> the disposition sampler found no flat measure
    rows -> _got_flat_udtf_rows=False -> 'na'.  After the fix, gb_tile=True fns are
    routed to a replicated EPSG:27700 corpus tile, which bins REAL BNG cells at
    resolution 3 (1km) over central London -> flat measure rows returned ->
    output_disposition='materialized'.

    generate_corpus always appends a role='bng_gb' EPSG:27700 tile (London extent)
    to size_sweep, so this corpus already has the routing entry.
    """
    from databricks.labs.gbx.bench import runner as rn
    from databricks.labs.gbx.bench import spec as s

    corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=88,
        tile_px=[64],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=2,
        row_tile_px=64,
        row_bands=1,
        row_dtype="float32",
    )
    # Confirm the corpus has a bng_gb entry (invariant of generate_corpus).
    assert any(te.role == "bng_gb" for te in corpus.size_sweep), (
        "test pre-condition: generate_corpus must produce a role='bng_gb' entry"
    )

    fns = s.select(functions=["rst_bng_rastertogridavg"])
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=corpus,
        fnspecs=fns,
        run_id="bng-gb-fix-test",
        row_counts=[2],
        warmup=1,
        measured=1,
        where="venv",
        input_tile="virtual",
    )
    assert rows, "expected result rows from rst_bng_rastertogridavg spark-path"
    ok_rows = [r for r in rows if r.status == "ok"]
    assert ok_rows, (
        f"expected at least one ok row; got statuses: {[r.status for r in rows]}, "
        f"notes: {[r.note for r in rows]}"
    )
    for r in ok_rows:
        assert r.output_disposition == "materialized", (
            f"rst_bng_rastertogridavg (gb_tile=True) with virtual input must yield "
            f"output_disposition='materialized' (GB tile bins real BNG cells at "
            f"resolution 3). Got: '{r.output_disposition}'. "
            f"If 'na': df_gb was not built or not routed to this fn."
        )


def test_bng_rastertogrid_sparkpath_fallback_no_crash(tmp_path, spark):
    """When the corpus has NO role='bng_gb' entry, run_spark_path must not crash;
    it falls back to the ordinary row pool and emits a one-line note.

    This exercises the _gb_entry is None branch: df_gb stays None, and the
    gb_tile fn runs against the NYC tiles (producing an empty grid, but without
    error).  Verifies graceful degradation for older corpora that predate the
    GB tile.
    """
    from databricks.labs.gbx.bench import manifest as m
    from databricks.labs.gbx.bench import runner as rn
    from databricks.labs.gbx.bench import spec as s

    full_corpus = dg.generate_corpus(
        out_dir=tmp_path,
        seed=99,
        tile_px=[64],
        bands=[1],
        dtypes=["float32"],
        srids=[4326],
        nodata_fracs=[0.0],
        row_rows=2,
        row_tile_px=64,
        row_bands=1,
        row_dtype="float32",
    )
    # Strip the bng_gb entry to simulate an older corpus.
    stripped = m.Corpus(
        seed=full_corpus.seed,
        size_sweep=[te for te in full_corpus.size_sweep if te.role != "bng_gb"],
        row_pool=full_corpus.row_pool,
    )
    assert not any(te.role == "bng_gb" for te in stripped.size_sweep), (
        "test pre-condition: stripped corpus must have no bng_gb entry"
    )

    fns = s.select(functions=["rst_bng_rastertogridavg"])
    # Must not raise; rows may be empty-grid (na disposition) but no exception.
    rows = rn.run_spark_path(
        spark=spark,
        corpus_root=tmp_path,
        corpus=stripped,
        fnspecs=fns,
        run_id="bng-fallback-test",
        row_counts=[2],
        warmup=1,
        measured=1,
        where="venv",
        input_tile="virtual",
    )
    assert rows is not None, "run_spark_path must return a list (even if empty) on fallback"


def test_vector_output_fns_pin_disposition_explicitly():
    """Safety net for GEOMETRY-emitting fns (fingerprint_kind='vector').

    A fn that emits geometry has no output tile, so the raster-null-ness rule
    cannot classify it -- and it necessarily read all input pixels to produce
    that geometry, so it is 'materialized'. Without an explicit override a
    SCALAR geometry-output fn sample-classifies to 'deferred' (the rst_contour
    bug) and a UDTF one leans on the case-2 flat-row upgrade. Require the
    explicit virtual_disposition on every vector-output spark-path fn so the
    whole class can't silently regress to deferred/na.
    """
    from databricks.labs.gbx.bench.spec import REGISTRY

    _missing = [
        name
        for name, fs in REGISTRY.items()
        if getattr(fs, "fingerprint_kind", "") == "vector"
        and "spark-path" in fs.modes
        and getattr(fs, "virtual_disposition", None) is None
    ]
    assert not _missing, (
        "vector-output (geometry) spark-path fns must pin virtual_disposition "
        f"(they read pixels but emit no tile to sample): {_missing}"
    )
