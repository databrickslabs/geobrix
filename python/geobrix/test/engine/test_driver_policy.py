"""The process-level driver policy (AP-43), in fresh processes: every GeoBrix GDAL path refuses the crafted-data cases.

The policy is process-wide once a process has GeoBrix's GDAL, so each row runs in its own interpreter, with the
fixtures the oracle rows use (benign: a pipeline, a raw band over a sibling file, a network connection string). The
rows need the payload's GDAL (the container with GBX_NATIVE_ROOT); elsewhere they skip with the reason.

- Classification: the payload's registered raster drivers, enumerated with the switch off (so the refused ones are
  still registered to be seen), every one of them allowed or refused. A GDAL bump that adds a driver fails here.
- Fresh-process rows: open_header, a light pixel read, reader planning, the bytes path, and the activation at import.
- The switch: GBX_GDAL_DRIVER_POLICY=off leaves GDAL as GDAL has it, so the same row opens.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from databricks.labs.gbx import _gdal_policy, manifest

REPO = Path(__file__).resolve().parents[4]


def _payload_or_skip():
    if _gdal_policy.state().get("mode") != "full":
        pytest.skip(
            "PRECONDITION MISSING: the payload's osgeo on rasterio's GDAL (GBX_NATIVE_ROOT); the process rows are not run"
        )


def _run(code: str, env_extra=None) -> str:
    env = {**os.environ, **(env_extra or {})}
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
        cwd=str(REPO),
    )
    assert proc.returncode == 0, (proc.stdout + proc.stderr)[-3000:]
    return proc.stdout


def test_every_registered_raster_driver_is_classified():
    """The payload's GDAL registers rasters the manifest does not classify: a bump forces a classification (AP-43).
    Enumerated with the switch off, so the refused drivers are still registered to be seen.
    """
    _payload_or_skip()
    code = (
        "import json\n"
        "from databricks.labs.gbx.engine import run\n"
        "gdal = run._gdal()\n"
        "names = []\n"
        "for i in range(gdal.GetDriverCount()):\n"
        "    d = gdal.GetDriver(i)\n"
        "    if d.GetMetadataItem(gdal.DCAP_RASTER) == 'YES':\n"
        "        names.append(d.ShortName)\n"
        "print('DRIVERS ' + json.dumps(sorted(names)))\n"
    )
    out = _run(code, {"GBX_GDAL_DRIVER_POLICY": "off"})
    found = json.loads(
        next(ln for ln in out.splitlines() if ln.startswith("DRIVERS ")).split(" ", 1)[
            1
        ]
    )
    man = manifest.load().gdal
    classified = set(man.allowed_drivers) | set(man.refused_drivers)
    unclassified = sorted(set(found) - classified)
    assert (
        unclassified == []
    ), f"unclassified raster driver(s) for this GDAL: {unclassified}"
    assert (
        len(found) >= 142
    ), f"expected the pinned GDAL's 142 raster drivers, saw {len(found)}"


_PRELUDE = (
    "import sys, tempfile\n"
    "from databricks.labs.gbx.bench import oracles\n"
    "from databricks.labs.gbx.pyrx.core import open_tile as ot\n"
    "d = tempfile.mkdtemp()\n"
)


def _outcome(body: str) -> str:
    """`body` runs under a try; its last printed line is OPENED, or the refusal is printed as REFUSED with its text."""
    return (
        _PRELUDE
        + "try:\n"
        + "\n".join("    " + line for line in body.strip("\n").split("\n"))
        + "\n    print('OPENED')\n"
        + "except Exception as exc:\n"
        + "    print('REFUSED ' + type(exc).__name__ + ' ' + str(exc))\n"
    )


def _refused(out: str) -> str:
    line = next(ln for ln in out.splitlines() if ln.startswith(("OPENED", "REFUSED")))
    assert line.startswith("REFUSED"), line
    return line


FRESH_GDALG_HEADER = _outcome("""
tile = oracles._build("dem_gdalg", d)
vt = ot._to_virtual_tile(tile)
with ot.open_header(vt) as h:
    pass
""")

FRESH_RAW_PIXELS = _outcome("""
tile = oracles._build("dem_inline_raw", d)
vt = ot._to_virtual_tile(tile)
with ot.open_tile(vt) as ds:
    ds.read(1)
""")

FRESH_PLANNING = _outcome("""
from databricks.labs.gbx.ds import raster as r
tile = oracles._build("dem_vrt_raw", d)
r._plan_partitions_for_file(tile["path"], 0, emit_virtual=False)
""")

FRESH_BYTES = _outcome("""
from rasterio.io import MemoryFile
import os
oracles._build("dem_gdalg", d)
with open(os.path.join(d, "se_dem.gdalg.json"), "rb") as fh:
    raw = fh.read()
with MemoryFile(raw).open() as ds:
    ds.read(1)
""")


FRESH_EXPLICIT_DRIVER = _outcome("""
from databricks.labs.gbx._gdal_policy import open_data
tile = oracles._build("dem_gdalg", d)
open_data(tile["path"], driver="GDALG")
""")

FRESH_NOT_A_RASTER = _outcome("""
import os
from databricks.labs.gbx._gdal_policy import open_data
with open(os.path.join(d, "not_a_raster.bin"), "wb") as fh:
    fh.write(b"not a raster any driver reads")
open_data(os.path.join(d, "not_a_raster.bin"))
""")


@pytest.mark.integration
def test_open_data_refuses_a_driver_GeoBrix_does_not_open_by_name():
    """open_data's explicit driver is checked against the allowed set (the one GDAL-side restriction a caller can ask
    for): GDALG named by the caller is refused with the policy's reason, not handed to rasterio.
    """
    _payload_or_skip()
    line = _refused(_run(FRESH_EXPLICIT_DRIVER))
    assert "driver GDALG is one GeoBrix does not open" in line, line


@pytest.mark.integration
def test_open_data_refuses_a_file_no_allowed_driver_reads():
    """Identify-then-open: a file that exists and no allowed driver identifies is refused with the policy's reason
    (rasterio would otherwise report a generic read error for it)."""
    _payload_or_skip()
    line = _refused(_run(FRESH_NOT_A_RASTER))
    assert "is not a raster any driver GeoBrix opens reads" in line, line


@pytest.mark.integration
def test_open_header_refuses_a_gdalg_fixture_in_a_fresh_process():
    _payload_or_skip()
    line = _refused(_run(FRESH_GDALG_HEADER))
    assert "which GeoBrix does not open" in line, line


@pytest.mark.integration
def test_a_light_pixel_read_refuses_the_inline_raw_band_in_a_fresh_process():
    _payload_or_skip()
    line = _refused(_run(FRESH_RAW_PIXELS))
    assert "a VRT raw band" in line and "which GeoBrix does not open" in line, line


@pytest.mark.integration
def test_reader_planning_refuses_a_crafted_raw_band_vrt_in_a_fresh_process():
    _payload_or_skip()
    line = _refused(_run(FRESH_PLANNING))
    assert "a VRT raw band" in line and "which GeoBrix does not open" in line, line


@pytest.mark.integration
def test_tile_bytes_that_are_a_pipeline_cannot_be_opened_in_a_fresh_process():
    """A tile's bytes are data too: the registry refuses a GDALG the bytes name (no driver, so nothing runs)."""
    _payload_or_skip()
    out = _run(FRESH_BYTES)
    assert "REFUSED" in out and "not recognized" in out, out[-600:]


@pytest.mark.integration
def test_the_package_import_activates_the_policy_before_any_open():
    """The activation runs at import (the first GeoBrix GDAL use is no later): a process that imports GeoBrix and then
    asks GDAL for GDALG finds no driver."""
    _payload_or_skip()
    code = (
        "import databricks.labs.gbx\n"
        "from osgeo import gdal\n"
        "print('GDALG', 'absent' if gdal.GetDriverByName('GDALG') is None else 'registered')\n"
    )
    assert "GDALG absent" in _run(code)


@pytest.mark.integration
def test_the_one_switch_off_leaves_gdal_as_gdal_has_it():
    """GBX_GDAL_DRIVER_POLICY=off: the same open that the policy refuses is GDAL's own, and it opens."""
    _payload_or_skip()
    out = _run(FRESH_GDALG_HEADER, {"GBX_GDAL_DRIVER_POLICY": "off"})
    assert "OPENED" in out and "REFUSED" not in out, out[-600:]
