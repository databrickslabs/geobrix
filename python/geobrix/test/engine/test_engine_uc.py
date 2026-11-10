"""The Unity Catalog handlers of the GDAL spec engine's rows (engine.uc) answer what the session's engine UDF answers.

The oracle tables judge each UC row against its expectation (test_family_cross_tier.py); these rows hold the claim on top
of that: for every session row a UC function shares, the UC handler's value is the session's row-wise UDF's value
(``engine.l3.c0``, its Python body), the same tile bytes, features or text, and the same error. They also hold the rules
the handler adds (the cap, the refusals) and that it runs without pyspark. They skip where ``osgeo`` is not rasterio's
GDAL.
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.engine import _tile, l3, run
from databricks.labs.gbx.engine import uc as engine_uc

MAN = manifest.load()
ENGINE_UC = sorted(n for n, f in MAN.functions.items() if oracles.engine_uc(n))
TABLES = ("SPEC_SE", "SPEC_DEM", "SPEC_CALC", "SPEC_WARP", "SPEC_ANALYSIS", "MVP_S")


@pytest.fixture(autouse=True)
def _engine():
    why = run.unavailable_reason()
    if why:
        pytest.skip(why)


def test_every_engine_row_with_a_uc_form_is_served_by_the_engine_handler():
    """No engine row keeps a UC handler of its own: each UC form runs engine.uc, and engine.uc serves no other row."""
    with_uc = sorted(
        n for n, f in MAN.functions.items() if f.gdal is not None and "L4" in f.levels
    )
    assert with_uc == ENGINE_UC
    scalars = [n for n in ENGINE_UC if MAN.functions[n].kind == "scalar"]
    assert sorted(engine_uc.HANDLERS) == scalars
    for n in scalars:
        ref = MAN.functions[n].levels["L4"]["handler"]
        assert manifest.resolve_ref(ref) is engine_uc.HANDLERS[n], n
    # the one generator is a table function over its private array function
    pg = MAN.functions["gbx_rst_polygonize"].levels["L4"]["tvf_over"]
    assert (
        manifest.resolve_ref(MAN.private[pg].handler)
        is engine_uc.rst_polygonize_features
    )
    # gbx_rst_gdal is a session function only
    assert "L4" not in MAN.functions["gbx_rst_gdal"].levels


def test_the_tile_fields_are_the_v2_tile_struct():
    from databricks.labs.gbx.pyrx._serde import V2_TILE_SCHEMA

    assert _tile.TILE_FIELDS == tuple(f.name for f in V2_TILE_SCHEMA.fields)


def test_the_virtual_fixtures_are_the_ones_that_build_a_virtual_tile(tmp_path):
    """``mirror`` keeps a row session-only by the name of its fixture: the names must be the fixtures that build one."""
    used = set()
    for t in TABLES:
        for c in oracles.TABLES[t]:
            first = c.args[0] if c.args else None
            for a in first if isinstance(first, (list, tuple)) else [first]:
                if isinstance(a, oracles.Build):
                    used.add(a.name)
    for name in sorted(used):
        value = oracles.resolve_arg(oracles.Build(name), str(tmp_path))
        tiles = value if isinstance(value, list) else [value]
        virtual = any(
            isinstance(t, dict)
            and t.get("raster") is None
            and t.get("path") is not None
            for t in tiles
        )
        assert virtual == (name in oracles.VIRTUAL_BUILDS), name


def _session(fn, args):
    """The session's value: the row-wise engine UDF's Python body (a generator's rows as dicts)."""
    if fn.kind == "generator":
        rows = list(l3.udtf(fn.sql_name).func().eval(*args))
        names = ["geom_wkb"] + [n for n, _, _ in fn.gdal.out_fields]
        return [dict(zip(names, r)) for r in rows] or None
    return l3.c0(fn.sql_name).func(*args)


def _uc(fn, args, case):
    handler = (
        engine_uc.rst_polygonize_features
        if fn.kind == "generator"
        else engine_uc.HANDLERS[fn.sql_name]
    )
    uc_args = [args[0]] + [
        oracles._uc_value(a, p, case) for a, p in zip(args[1:], fn.params[1:])
    ]
    with oracles._uc_geometry_type():
        return handler(*uc_args)


def _outcome(f, *a):
    try:
        return "value", f(*a)
    except Exception as exc:  # noqa: BLE001 - the two sides must fail alike
        return "raises", (type(exc).__name__, str(exc))


SHARED = [
    pytest.param(t, c.cid, id=f"{t}-{c.cid}")
    for t in TABLES
    for c in oracles.TABLES[t]
    if oracles.engine_uc(c.fn) and {"session", "uc"} <= set(c.surfaces)
]


@pytest.mark.parametrize("table,cid", SHARED)
def test_a_uc_function_answers_what_its_session_function_answers(table, cid):
    case = next(c for c in oracles.TABLES[table] if c.cid == cid)
    fn = MAN.functions[case.fn]
    tmp = tempfile.mkdtemp(prefix="gbx-engine-uc-")
    for row in (case.args, *case.variants):
        args = [oracles.resolve_arg(a, tmp) for a in row]
        session = _outcome(_session, fn, args)
        uc = _outcome(_uc, fn, args, case)
        assert (
            uc == session
        ), f"{cid}: UC {str(uc)[:300]} != session {str(session)[:300]}"


def _dem(tmp_path):
    return oracles.resolve_arg(oracles.Build("dem_3857"), str(tmp_path))


def test_a_call_over_the_uc_cap_is_refused_with_the_engines_fix(tmp_path, monkeypatch):
    """The cap holds each input and the call (inputs and estimated output): over it the UC function raises the reason
    and the Fix the session's error tile carries, and names the session function."""
    dem = _dem(tmp_path)
    monkeypatch.setattr(engine_uc, "DECODED_CAP", 1024)
    with pytest.raises(ValueError) as err:
        engine_uc.rst_slope(dem, "degrees", None, None)
    text = str(err.value)
    assert "over this compute's read cap (1 KiB)" in text and "Fix:" in text
    assert "run gbx_rst_slope in a notebook or job session" in text
    # a call whose input fits but whose output does not: the budget, judged before GDAL runs
    big = run.run(
        MAN.functions["gbx_rst_resample_to_size"],
        [bytes(dem["raster"])],
        {"width_px": 4000, "height_px": 4000, "algorithm": "nearest"},
        cap=1 << 20,
    )
    assert big.over_cap and "decoded (input and output)" in big.error


def test_a_data_error_is_null_and_an_error_tile_goes_through(tmp_path):
    corrupt = oracles.resolve_arg(oracles.Build("tile_corrupt"), str(tmp_path))
    err = oracles.resolve_arg(oracles.Build("tile_error"), str(tmp_path))
    assert engine_uc.rst_slope(corrupt, "degrees", None, None) is None
    assert engine_uc.rst_summary(corrupt) is None
    out = engine_uc.rst_slope(err, "degrees", None, None)
    assert out["raster"] is None and out["metadata"]["last_error"]
    assert engine_uc.rst_summary(err) is None


def test_a_virtual_tile_and_a_crs_uc_does_not_take_are_refused_with_a_fix(tmp_path):
    virtual = oracles.resolve_arg(oracles.Build("tile_virtual"), str(tmp_path))
    with pytest.raises(
        ValueError, match="session-only. Fix: use the session function gbx_rst_slope"
    ):
        engine_uc.rst_slope(virtual, "degrees", None, None)
    with pytest.raises(
        ValueError, match="Fix: use the session function gbx_rst_transformcrs"
    ):
        engine_uc.rst_transformcrs(_dem(tmp_path), "+proj=merc")


class _Stream:
    """A streamed features output that counts the rows taken from it and records its close."""

    def __init__(self, n, wkb):
        self.n, self.wkb, self.taken, self.closed = n, wkb, 0, False

    def __iter__(self):
        for _ in range(self.n):
            self.taken += 1
            yield (self.wkb, 1.0)

    def close(self):
        self.closed = True


def test_features_are_judged_against_the_cap_as_they_are_read(monkeypatch):
    """The running size of the rows read is checked after each row: the call is refused the moment it passes the cap,
    with no further row taken from the stream, and the stream's file is removed."""
    from databricks.labs.gbx.uc import tier_b

    monkeypatch.setattr(engine_uc, "DECODED_CAP", 10_000)
    fn = MAN.functions["gbx_rst_polygonize"]
    wkb = b"\x01" * 93  # a one-pixel polygon's WKB is 93 bytes
    per_row = tier_b.row_bytes({"geom_wkb": wkb})
    stream = _Stream(1_000_000, wkb)
    with pytest.raises(ValueError, match="of geometry, over the.*Fix: "):
        engine_uc._feature_rows(fn, stream)
    assert stream.taken == 10_000 // per_row + 1 and stream.closed
    under = _Stream(10, wkb)
    assert len(engine_uc._feature_rows(fn, under)) == 10 and under.closed


def test_a_polygonize_over_the_cap_is_refused_before_its_stream_is_read_out(
    tmp_path, monkeypatch
):
    """A 64 x 64 checkerboard has 4096 one-pixel regions, about 500 KiB of rows. Under a 64 KiB cap the input and the
    estimate fit, GDAL writes the regions to its local file, and the handler stops reading it once the rows pass the
    cap: far fewer rows than the output holds are ever read."""
    import numpy as np

    board = (np.indices((64, 64)).sum(axis=0) % 2 + 1).astype("float64")
    tile = oracles._tile(
        oracles._gtiff(board[np.newaxis], "EPSG:3857", nodata=None, dtype="uint8")
    )
    seen = []
    real_run = run.run

    def spy(*a, **k):
        res = real_run(*a, **k)
        seen.append((res, res.features._scratch))  # the stream's local folder
        return res

    monkeypatch.setattr(run, "run", spy)
    monkeypatch.setattr(engine_uc, "DECODED_CAP", 64 * 1024)
    with pytest.raises(ValueError, match="of geometry, over the 64 KiB.*Fix: "):
        engine_uc.rst_polygonize_features(tile, 1, 4)
    ((res, scratch),) = seen
    assert res.features.expected == 4096 and res.features.read < 4096 // 4
    assert scratch and not os.path.exists(scratch)  # the stream's file is removed


def test_the_engine_handlers_run_in_a_sandbox_without_pyspark():
    """A UC Python sandbox holds the declared deps only: the engine handler imports no pyspark, pandas or pyarrow."""
    code = (
        "import sys, tempfile\n"
        "for blocked in ('pyspark', 'pandas', 'pyarrow', 'scipy', 'skimage', 'numexpr'):\n"
        "    sys.modules[blocked] = None  # an import of it raises ImportError\n"
        "from databricks.labs.gbx.bench import oracles\n"
        "from databricks.labs.gbx.engine import uc\n"
        "oracles.TABLES['SPEC_ANALYSIS']  # the family modules register their fixtures\n"
        "tmp = tempfile.mkdtemp()\n"
        "dem = oracles.resolve_arg(oracles.Build('dem_3857'), tmp)\n"
        "assert uc.rst_slope(dem, 'degrees', None, None)['raster']\n"
        "assert uc.rst_summary(dem).startswith('{')\n"
        "assert uc.rst_contour(dem, [110.0], 0.0, 0.0, 'elev')\n"
        "assert uc.rst_polygonize_features(oracles.resolve_arg(oracles.Build('pg_halves'), tmp), 1, 4)\n"
        "assert not [m for m in sys.modules if m.split('.')[0] == 'pyspark' and sys.modules[m] is not None]\n"
        "print('ok')\n"
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
    assert out.returncode == 0 and out.stdout.strip() == "ok", out.stderr[-2000:]


# The tile as a warehouse statement sends an error tile's struct: an EMPTY map under `metadata`, no raster and no path.
_EMPTY = dict.fromkeys(_tile.TILE_FIELDS, None) | {"cellid": 0, "metadata": {}}


@pytest.mark.parametrize(
    "md",
    [
        {"last_error": "upstream"},
        [("last_error", "upstream")],
        [{"key": "last_error", "value": "upstream"}],
    ],
    ids=["dict", "pairs", "key_value_structs"],
)
def test_an_error_tile_passes_through_before_an_argument_is_read_in_every_form_a_map_arrives_in(
    md, tmp_path
):
    err = {**_EMPTY, "cellid": 7, "metadata": md}
    for out in (
        engine_uc.rst_mapalgebra([err, err], "A +* 2"),
        engine_uc.rst_slope(err, "radians", None, None),
        engine_uc.rst_transform(err, 3857),
    ):
        assert (
            out["raster"] is None
            and out["cellid"] == 7
            and out["metadata"] == {"last_error": "upstream"}
        )
    assert engine_uc.rst_contour(err, [1.0], 0.0, 0.0, "elev") is None
    # a tile with nothing in it is empty, not an error tile: NULL
    assert engine_uc.rst_mapalgebra([_EMPTY, _EMPTY], "A +* 2") is None
    assert engine_uc.rst_ndvi(_EMPTY, 1, 2) is None
    pair = oracles.resolve_arg(oracles.Build("se_pair"), str(tmp_path))
    with pytest.raises(ValueError, match="GDAL cannot evaluate the expression"):
        engine_uc.rst_mapalgebra(pair, "A +* 2")


def test_a_parameter_left_out_takes_its_default_and_an_extra_one_raises(tmp_path):
    dem = _dem(tmp_path)
    assert engine_uc.rst_slope(dem) == engine_uc.rst_slope(dem, "degrees", None, None)
    with pytest.raises(TypeError, match="takes 1 arguments, got 2"):
        engine_uc.rst_tpi(dem, 1)


def test_the_features_array_serves_only_a_generator_with_a_features_output(tmp_path):
    with pytest.raises(
        ValueError, match="serves a generator row with a features output"
    ):
        engine_uc.features("gbx_rst_slope")(_dem(tmp_path))


class _FailingStream(_Stream):
    """A streamed output whose file fails: before its first row (``expected`` unset) or after ``after`` rows."""

    def __init__(self, after):
        super().__init__(10, b"\x01" * 93)
        self.after, self.expected, self.read = after, None, 0

    def __iter__(self):
        if self.after is None:
            raise run.DataError("the output has no readable feature count")
        self.expected = 10
        for row in super().__iter__():
            if self.read == self.after:
                raise run.DataError("GDAL's output ended before its last feature")
            self.read += 1
            yield row


def test_a_stream_that_fails_before_its_first_row_is_null_and_one_that_fails_later_raises():
    """As the session's table function: an unreadable output is a data error (NULL on UC); a read that fails once rows
    are out raises, because a result is never silently partial. The file is removed either way.
    """
    fn = MAN.functions["gbx_rst_polygonize"]
    early = _FailingStream(None)
    assert engine_uc._feature_rows(fn, early) is None and early.closed
    late = _FailingStream(3)
    with pytest.raises(RuntimeError, match="failed after 3 of 10 features"):
        engine_uc._feature_rows(fn, late)
    assert late.closed


def test_a_required_argument_left_out_raises_and_a_result_over_the_cap_is_refused(
    tmp_path, monkeypatch
):
    """A UC function never answers NULL for a call that is short of a required argument. A result is also judged after
    GDAL ran, from its header, in case the estimate before the run was short."""
    dem = _dem(tmp_path)
    with pytest.raises(TypeError, match="takes at least 2 arguments, got 1"):
        engine_uc.rst_resample(dem)
    monkeypatch.setattr(
        run, "_judge_budget", lambda *a, **k: None
    )  # an estimate that lets the call run
    monkeypatch.setattr(engine_uc, "DECODED_CAP", 2 * 1024 * 1024)
    with pytest.raises(
        ValueError, match="the result would be .* decoded, over the 2 MiB.*Fix: "
    ):
        engine_uc.rst_resample_to_size(dem, 1000, 1000, "nearest")
