"""The file writers on Unity Catalog (gbx_rst_buildoverviews, gbx_rst_cog_convert): what the oracle rows
(bench/oracle_tables/raster_file_op.py) do not say. The rows hold the tiles and the refusals; this file holds the
mechanisms: the Fix of every wrong argument, an error tile that passes whatever the arguments are, the three size bounds
(the input is checked before a pixel is read, the written bytes before the tile is built), the local file the kernel writes
and removes, and the sandbox the handlers run in.
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import pytest
from rasterio.transform import from_origin

from databricks.labs.gbx import manifest
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.pyrx.core import analysis, ops
from databricks.labs.gbx.pyrx.core.compression import write_gtiff
from databricks.labs.gbx.uc import ddl, tier_b
from databricks.labs.gbx.uc.handlers import _Refusal

MAN = manifest.load()
BO, CG = "gbx_rst_buildoverviews", "gbx_rst_cog_convert"


def _raster(width=64, height=48):
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 10, 0.1, 0.1),
    )
    data = np.arange(width * height, dtype="float32").reshape(1, height, width)
    return write_gtiff(profile, data)


def _tile(**fields):
    return {**oracles._tile(_raster()), **fields}


def _error_tile():
    return {
        **oracles._tile(None),
        "metadata": {"last_error": "upstream failed"},
        "path": None,
    }


@pytest.mark.parametrize(
    "call,args,bad",
    [
        (tier_b.rst_buildoverviews, ([2], "bogus"), "bogus"),
        (tier_b.rst_buildoverviews, ([1],), "levels"),
        (tier_b.rst_buildoverviews, ([],), "levels"),
        (tier_b.rst_cog_convert, ("AUTO", 100), "100"),
        (tier_b.rst_cog_convert, ("SQUASH",), "SQUASH"),
        (tier_b.rst_cog_convert, ("AUTO", 512, ""), "overview_resampling"),
    ],
)
def test_a_wrong_argument_raises_with_a_fix_and_names_the_value(call, args, bad):
    with pytest.raises(ValueError) as err:
        call(_tile(), *args)
    assert bad in str(err.value) and "Fix: " in str(err.value)


def test_a_null_level_raises_with_a_fix_not_a_bare_type_error():
    with pytest.raises(ValueError) as err:
        tier_b.rst_buildoverviews(_tile(), [2, None])
    assert "Fix: " in str(err.value) and not isinstance(err.value, TypeError)


@pytest.mark.parametrize(
    "call,args",
    [
        (tier_b.rst_buildoverviews, ([1], "bogus")),
        (tier_b.rst_cog_convert, ("SQUASH", 100)),
    ],
)
def test_an_error_tile_comes_out_as_itself_whatever_the_arguments(call, args):
    out = call(_error_tile(), *args)
    assert out["raster"] is None and out["metadata"]["last_error"] == "upstream failed"


def test_a_null_option_is_its_default_as_in_a_session():
    plain = tier_b.rst_cog_convert(_tile())
    assert (
        tier_b.rst_cog_convert(_tile(), None, None, None)["raster"] == plain["raster"]
    )
    avg = tier_b.rst_buildoverviews(_tile(), [2])
    assert tier_b.rst_buildoverviews(_tile(), [2], None)["raster"] == avg["raster"]


def _spy(monkeypatch, module, name):
    """Replace a core kernel with a failing stand-in, and say whether it was asked."""
    asked = []
    monkeypatch.setattr(
        module, name, lambda *a, **k: asked.append(a) or pytest.fail(f"{name} ran")
    )
    return asked


@pytest.mark.parametrize(
    "call,args,module,kernel",
    [
        (tier_b.rst_buildoverviews, ([2],), ops, "build_overviews"),
        (tier_b.rst_cog_convert, (), analysis, "cog_convert"),
    ],
)
def test_a_tile_decoded_over_the_cap_is_refused_before_the_kernel_reads_a_pixel(
    monkeypatch, call, args, module, kernel
):
    """The input bound: a 64 x 48 float32 tile is 12 KiB decoded."""
    monkeypatch.setattr(tier_b, "DECODED_CAP", 12_000)
    asked = _spy(monkeypatch, module, kernel)
    with pytest.raises(
        _Refusal, match=r"over the .*Fix: run gbx_rst_\w+ in a notebook or job session"
    ):
        call(_tile(), *args)
    assert asked == []


@pytest.mark.parametrize(
    "call,args,module,kernel",
    [
        (tier_b.rst_buildoverviews, ([2],), ops, "build_overviews"),
        (tier_b.rst_cog_convert, (), analysis, "cog_convert"),
    ],
)
def test_written_bytes_over_the_cap_and_a_third_are_refused_not_returned(
    monkeypatch, call, args, module, kernel
):
    """The output bound: the cap plus the third a pyramid adds; a kernel that wrote more is refused with a Fix."""
    monkeypatch.setattr(tier_b, "DECODED_CAP", 13_000)  # the tile fits
    limit = 13_000 + 13_000 // tier_b.OVERVIEW_ALLOWANCE
    monkeypatch.setattr(module, kernel, lambda *a, **k: b"x" * (limit + 1))
    with pytest.raises(_Refusal, match=r"the result would be .*Fix: run gbx_rst_\w+"):
        call(_tile(), *args)
    monkeypatch.setattr(module, kernel, lambda *a, **k: _raster())
    assert call(_tile(), *args)["raster"] is not None  # at the limit it is not refused


@pytest.mark.parametrize(
    "call,args", [(tier_b.rst_buildoverviews, ([2, 4],)), (tier_b.rst_cog_convert, ())]
)
def test_the_local_file_the_kernel_writes_is_removed(call, args):
    before = set(os.listdir(tempfile.gettempdir()))
    assert call(_tile(), *args)["raster"] is not None
    assert set(os.listdir(tempfile.gettempdir())) - before == set()


def test_the_overviews_are_in_the_bytes_and_the_tile_says_gtiff():
    import rasterio
    from rasterio.io import MemoryFile

    out = tier_b.rst_buildoverviews(_tile(), [2, 4])
    with MemoryFile(out["raster"]) as mf, rasterio.open(mf) as ds:
        assert ds.overviews(1) == [2, 4]
    assert out["metadata"]["driver"] == "GTiff"


@pytest.mark.parametrize("name", [BO, CG])
def test_the_ddl_is_a_python_function_over_a_tile(name):
    stmts = "\n".join(
        ddl.function_ddl(
            MAN.functions[name], "c", "s", "file:///w.whl", MAN.uc_dependencies, "0.6"
        )
    )
    # rasterio is vendored in the env wheel (manifest uc_provided): declared, never pinned
    assert "LANGUAGE PYTHON" in stmts and "rasterio==" not in stmts
    assert "rasterio" in MAN.functions[name].levels["L4"]["deps"]
    assert (
        MAN.functions[name].params[0].uc == "TILE"
        and MAN.functions[name].returns_uc == "TILE"
    )


def test_the_file_writers_run_in_a_sandbox_without_pyspark():
    code = (
        "import sys, tempfile\n"
        "for blocked in ('pyspark', 'pandas', 'pyarrow', 'scipy'):\n"
        "    sys.modules[blocked] = None  # an import of it raises ImportError\n"
        "from databricks.labs.gbx.bench import oracles\n"
        "ran = oracles.run_handlers(oracles.TABLES['RASTER_FILE_OP'], tmpdir=tempfile.mkdtemp())\n"
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
    assert int(out.stdout) == sum(
        "uc" in c.surfaces and c.sql is None for c in oracles.TABLES["RASTER_FILE_OP"]
    )
