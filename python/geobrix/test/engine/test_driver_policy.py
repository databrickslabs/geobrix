"""The process-level driver policy, in fresh processes: every GeoBrix GDAL path refuses the crafted-data cases.

The policy is process-wide once a process has GeoBrix's GDAL, so each row runs in its own interpreter, with the
fixtures the oracle rows use (benign: a pipeline, a raw band over a sibling file, a network connection string). The
rows need the payload's GDAL (the container with GBX_NATIVE_ROOT); elsewhere they skip with the reason.

- Classification: the payload's registered raster drivers, enumerated with the switch off (so the refused ones are
  still registered to be seen), every one of them allowed or refused. A GDAL bump that adds a driver fails here.
- Fresh-process rows: open_header, a light pixel read, reader planning, the bytes path, and the activation at import.
- The switch: GBX_GDAL_DRIVER_POLICY=off leaves GDAL as GDAL has it, so the same row opens.
- Partial mode (a rasterio that is not the payload's): the skip at registration, the warning when rasterio registered
  first, and the allowed-driver list on every open. These need no payload.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from databricks.labs.gbx import _gdal_policy, _native, manifest

REPO = Path(__file__).resolve().parents[4]

# Every row here is a policy row: a skip where the payload is present fails the meta-row at the end.
pytestmark = pytest.mark.gdal_policy


def _payload_or_skip():
    if _gdal_policy.state().get("mode") != "full":
        pytest.skip(
            "PRECONDITION MISSING: the payload's osgeo on rasterio's GDAL (GBX_NATIVE_ROOT); the process rows are not run"
        )


# The policy's own process settings (set by GeoBrix's activation in THIS process, inherited by a child): a child must
# start from the environment the caller set, or a switch-off child would still skip the drivers its parent skipped.
_POLICY_PROCESS_ENV = (
    "GDAL_SKIP",
    "GDAL_VRT_ENABLE_RAWRASTERBAND",
    "GDAL_VRT_ENABLE_PYTHON",
)


def _child_env(extra=None) -> dict:
    env = {k: v for k, v in os.environ.items() if k not in _POLICY_PROCESS_ENV}
    env.update(extra or {})
    return env


def _run(code: str, env_extra=None) -> str:
    env = _child_env(env_extra)
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
    """The payload's GDAL registers rasters the manifest does not classify: a bump forces a classification.
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
def test_a_file_no_driver_reads_keeps_the_bindings_io_error():
    """Identify-then-open: a file that exists, that no allowed driver identifies and that is no refused driver's (a
    corrupt raster, a format GDAL does not read) keeps rasterio's own I/O error, an OSError the read retries and the
    readers' quarantine catch; the policy's refusal is for a refused driver's file (the GDALG and KML rows).
    """
    _payload_or_skip()
    line = _refused(_run(FRESH_NOT_A_RASTER))
    assert "RasterioIOError" in line and "which GeoBrix does not open" not in line, line


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
def test_a_payload_rasterio_used_before_the_import_loses_the_refused_drivers_at_the_import():
    """The env wheel's rasterio can be imported and used before GeoBrix (its start-up hook serves it): GeoBrix's
    import then deregisters the refused drivers at once, before any GeoBrix open, so a bytes open in between cannot
    reach one. The hook is simulated by putting the payload's bindings first on sys.path.
    """
    _payload_or_skip()
    code = (
        "import os, platform, sys\n"
        "root = os.environ['GBX_NATIVE_ROOT']\n"
        "sys.path[:0] = [os.path.join(root, '_vendor', platform.machine()), os.path.join(root, '_vendor', 'common')]\n"
        "import rasterio\n"
        "with rasterio.Env() as env:\n"
        "    print('BEFORE', 'GDALG' in env.drivers(), rasterio.__file__.startswith(root))\n"
        "import databricks.labs.gbx\n"
        "with rasterio.Env() as env:\n"
        "    print('AFTER', 'GDALG' in env.drivers())\n"
    )
    out = _run(code)
    assert "BEFORE True True" in out, out[-800:]
    assert "AFTER False" in out, out[-800:]


_PAYLOAD_FIRST = (
    "import os, platform, sys\n"
    "root = os.environ['GBX_NATIVE_ROOT']\n"
    "os.environ.setdefault('PROJ_DATA', os.path.join(root, '_native', 'share', 'proj'))\n"
    "os.environ.setdefault('GDAL_DATA', os.path.join(root, '_native', 'share', 'gdal'))\n"
    "sys.path[:0] = [os.path.join(root, '_vendor', platform.machine()), os.path.join(root, '_vendor', 'common')]\n"
)


@pytest.mark.integration
def test_a_payload_pyogrio_imported_before_geobrix_loses_the_refused_drivers_at_the_import():
    """pyogrio registers every GDAL driver at its import, and the env wheel's pyogrio shares the payload's libgdal: a
    GeoBrix import after it sees the mapped libgdal and deregisters the refused drivers at once (the module names it
    checked before missed pyogrio). The start-up hook is simulated by putting the payload's bindings first on
    sys.path."""
    _payload_or_skip()
    code = _PAYLOAD_FIRST + (
        "import pyogrio\n"
        "d = pyogrio.list_drivers()\n"
        "print('BEFORE', 'WFS' in d, 'GDALG' in d, pyogrio.__file__.startswith(root))\n"
        "import databricks.labs.gbx\n"
        "d = pyogrio.list_drivers()\n"
        "print('AFTER', 'WFS' in d, 'GDALG' in d)\n"
    )
    out = _run(code)
    assert "BEFORE True True True" in out, out[-800:]
    assert "AFTER False False" in out, out[-800:]


@pytest.mark.integration
def test_geobrix_vector_reads_activate_the_policy_before_their_first_pyogrio_open(
    tmp_path,
):
    """GeoBrix's own pyogrio reads take pyogrio from the policy (``_gdal_policy.pyogrio()``), so the policy is active
    before the first one: a process that only imported GeoBrix has no state yet, and has one after the read.
    """
    _payload_or_skip()
    src = tmp_path / "empty.geojson"
    src.write_text('{"type": "FeatureCollection", "features": []}')
    code = (
        "import databricks.labs.gbx\n"
        "from databricks.labs.gbx import _gdal_policy as p\n"
        "print('BEFORE', p._STATE is None)\n"
        "from databricks.labs.gbx.ds import vector as v\n"
        f"v._detect({str(src)!r}, None)\n"
        "print('AFTER', p._STATE is not None and p._STATE['mode'])\n"
    )
    out = _run(code)
    assert "BEFORE True" in out, out[-800:]
    assert "AFTER full" in out, out[-800:]


@pytest.mark.parametrize(
    "name", ["GTI:{gpkg}", "WMS:http://127.0.0.1:9/wms?", "EEDAI:projects/x/assets/y"]
)
def test_a_refused_drivers_connection_string_is_the_policy_refusal(tmp_path, name):
    """A refused driver's connection string is no file to GDAL (it reports it missing): it is named as the policy's
    refusal, not as a missing file."""
    _rasterio_or_skip()
    gpkg = tmp_path / "index.gpkg"
    gpkg.write_bytes(b"SQLite format 3\x00")
    with pytest.raises(_gdal_policy.DriverPolicyError) as raised:
        _gdal_policy.open_data(name.format(gpkg=gpkg))
    msg = str(raised.value)
    assert (
        "a refused driver's connection" in msg and _gdal_policy.REFUSED_PHRASE in msg
    ), msg


def test_corrupt_tile_bytes_keep_the_bindings_io_error():
    """Tile bytes no driver reads (corrupt, truncated, not a raster) keep rasterio's own I/O error, an OSError; only
    a refused driver's bytes are the policy's refusal (the pipeline rows)."""
    _rasterio_or_skip()
    with pytest.raises(OSError) as raised:
        _gdal_policy.open_bytes(b"\x00\x01 not a raster any driver reads " * 8)
    assert not isinstance(raised.value, _gdal_policy.DriverPolicyError), raised.value


def test_with_the_switch_off_tile_bytes_open_through_their_memoryfile(monkeypatch):
    """Off: GDAL as GDAL has it, through MemoryFile.open (rasterio 1.3's rasterio.open would take a MemoryFile down
    its file-like path); no driver list is passed."""
    _rasterio_or_skip()
    import rasterio
    import rasterio.io as rio_io

    seen = []

    class FakeMemoryFile:
        def open(self, **kw):
            seen.append(("MemoryFile.open", kw.get("driver")))
            return _Opened()

    monkeypatch.setattr(rio_io, "MemoryFile", FakeMemoryFile)
    monkeypatch.setattr(
        rasterio, "open", lambda *a, **k: seen.append(("rasterio.open",))
    )
    monkeypatch.setattr(_gdal_policy, "activate", lambda: {"mode": "off"})
    _gdal_policy.open_bytes(FakeMemoryFile()).close()
    assert seen == [("MemoryFile.open", None)]


@pytest.mark.parametrize("name", ["missing.mrf", "tiles_wms", "a.gti"])
def test_a_missing_file_stays_a_missing_file_error(tmp_path, name):
    """A missing file is the binding's own I/O error (an OSError, which the read retries catch), even when its name
    ends like a refused driver's prefix; it is not reported as a policy refusal."""
    _rasterio_or_skip()
    with pytest.raises(OSError) as raised:
        _gdal_policy.open_data(str(tmp_path / name))
    assert not isinstance(raised.value, _gdal_policy.DriverPolicyError)
    assert "No such file or directory" in str(raised.value)


def test_tile_bytes_open_with_the_allowed_drivers_only(tmp_path):
    """Where GDALG did register (rasterio first, a partial process), bytes that are a stored pipeline open with GDALG
    through a plain MemoryFile open, and are refused through open_bytes, which every GeoBrix tile-byte read uses.
    """
    _rasterio_or_skip()
    dem = tmp_path / "dem.tif"
    code = (
        _REGISTERED_FIRST + "import json\n"
        "import numpy as np\n"
        "from rasterio.io import MemoryFile\n"
        "from rasterio.transform import from_origin\n"
        f"dem = {str(dem)!r}\n"
        "with rasterio.open(dem, 'w', driver='GTiff', width=4, height=4, count=1, dtype='float32',\n"
        "                   crs='EPSG:32631', transform=from_origin(0, 4, 1, 1)) as w:\n"
        "    w.write(np.arange(16, dtype='float32').reshape(1, 4, 4))\n"
        "raw = json.dumps({'type': 'gdal_streamed_alg', 'command_line': 'gdal raster pipeline read ' + dem + ' ! slope',\n"
        "                  'relative_paths_relative_to_this_file': False}).encode()\n"
        "with MemoryFile(raw, ext='.gdalg.json') as mf, mf.open() as plain:\n"
        "    print('PLAIN', plain.driver)\n"
        "from databricks.labs.gbx._gdal_policy import open_bytes\n"
        "try:\n"
        "    with MemoryFile(raw, ext='.gdalg.json') as mf, open_bytes(mf) as ds:\n"
        "        print('OPENED', ds.driver)\n"
        "except Exception as exc:\n"
        "    print('REFUSED ' + str(exc))\n"
        "try:\n"
        "    open_bytes(raw).close()\n"
        "    print('OPENED raw')\n"
        "except Exception as exc:\n"
        "    print('REFUSED raw ' + str(exc))\n"
    )
    out = _partial_run(code)
    assert "PLAIN GDALG" in out, out[-800:]
    assert "REFUSED " in out and "OPENED GDALG" not in out, out[-800:]
    assert "is not a raster any driver GeoBrix opens reads" in out, out[-800:]
    assert "REFUSED raw" in out, out[-800:]


def _partial_run(code: str) -> str:
    """A fresh process with no payload (GBX_NATIVE_DISABLE): rasterio is the PyPI or the system one, the partial mode.
    These rows need no payload, so they run wherever rasterio is installed."""
    env = _child_env()
    env.pop("GBX_NATIVE_ROOT", None)
    env["GBX_NATIVE_DISABLE"] = "1"
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
        cwd=str(REPO),
    )
    assert proc.returncode == 0, (proc.stdout + proc.stderr)[-3000:]
    return proc.stdout + proc.stderr


def _rasterio_or_skip():
    pytest.importorskip("rasterio")


def test_the_refused_drivers_are_not_registered_under_the_policy_and_are_under_the_switch_off():
    """The refused drivers (the super-overlay's NetworkLink fetch, ESRIC's CRS URL, MRF's cache writes among them) are
    absent from rasterio's registry in a process that imported GeoBrix; with the switch off they are GDAL's own again.
    """
    _rasterio_or_skip()
    probe = (
        "import json\n"
        "import databricks.labs.gbx\n"
        "import rasterio\n"
        "from databricks.labs.gbx import _gdal_policy as p\n"
        "with rasterio.Env() as env:\n"
        "    names = set(env.drivers())\n"
        "print('PRESENT ' + json.dumps(sorted(names & set(p.refused_names()))))\n"
    )

    def present(out):
        line = next(ln for ln in out.splitlines() if ln.startswith("PRESENT "))
        return set(json.loads(line.split(" ", 1)[1]))

    assert (
        present(_run(probe)) == set()
    ), "a refused driver is registered under the policy"
    assert {"KMLSUPEROVERLAY", "ESRIC", "MRF"} <= present(
        _run(probe, {"GBX_GDAL_DRIVER_POLICY": "off"})
    )


_OVERLAY_PROBE = """
import http.server, threading
hits = []
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        hits.append(self.path)
        self.send_response(404)
        self.end_headers()
    do_HEAD = do_GET
    def log_message(self, *a):
        pass
srv = http.server.HTTPServer(("127.0.0.1", 0), H)
threading.Thread(target=srv.serve_forever, daemon=True).start()
box = ("<Region><LatLonAltBox><north>1</north><south>0</south><east>1</east><west>0</west></LatLonAltBox>"
       "<Lod><minLodPixels>128</minLodPixels></Lod></Region>")
href = "http://127.0.0.1:%d/sub.kml" % srv.server_address[1]
with open(KML, "w") as fh:
    fh.write('<kml xmlns="http://www.opengis.net/kml/2.2"><Document>' + box + "<NetworkLink>" + box
             + "<Link><href>" + href + "</href></Link></NetworkLink></Document></kml>")
from databricks.labs.gbx._gdal_policy import open_data
try:
    open_data(KML).close()
    print("OPENED")
except Exception as exc:
    print("REFUSED " + str(exc))
print("HITS", len(hits))
"""


@pytest.mark.integration
def test_a_super_overlay_fetches_its_network_link_only_with_the_switch_off(tmp_path):
    """A super-overlay whose NetworkLink names an http URL (a local server here): under the policy the open is refused
    with the policy's reason and the URL is never fetched; with the switch off, GDAL's own driver fetches it.
    """
    _payload_or_skip()
    code = f"KML = {str(tmp_path / 'overlay.kml')!r}\n" + _OVERLAY_PROBE
    on = _run(code)
    assert "is not a raster any driver GeoBrix opens reads" in on, on[-800:]
    assert "HITS 0" in on, on[-800:]
    off = _run(code, {"GBX_GDAL_DRIVER_POLICY": "off"})
    assert "HITS 0" not in off and "HITS " in off, off[-800:]


@pytest.mark.parametrize(
    "before",
    ["", "import rasterio\n"],
    ids=["geobrix-first", "rasterio-imported-first"],
)
def test_partial_mode_skips_the_refused_drivers_at_registration(before):
    """A partial process (no payload) puts the skip list in the environment at GeoBrix's import, before rasterio
    registers GDAL's drivers (its first open or Env, not its import), so the refused drivers are never registered there:
    with GeoBrix imported first, and with rasterio imported but not yet used."""
    _rasterio_or_skip()
    code = (
        before + "import os\n"
        "import databricks.labs.gbx\n"
        "from databricks.labs.gbx import _gdal_policy as p\n"
        "import rasterio\n"
        "print('MODE', p.state()['mode'], p.state()['registered_refused'])\n"
        "with rasterio.Env() as env:\n"
        "    print('KML', 'KMLSUPEROVERLAY' in env.drivers())\n"
        "print('SKIP', os.environ.get('GDAL_SKIP'))\n"
        "print('RAWBAND', os.environ.get('GDAL_VRT_ENABLE_RAWRASTERBAND'))\n"
    )
    out = _partial_run(code)
    assert "MODE partial []" in out, out[-800:]
    assert "KML False" in out, out[-800:]
    assert "KMLSUPEROVERLAY" in out and "GDALG" in out, out[-800:]
    assert "RAWBAND NO" in out, out[-800:]


_REGISTERED_FIRST = "import rasterio\nwith rasterio.Env():\n    pass\n"


# Where the process maps can be read (Linux), the policy removes the refused drivers from every libgdal the process
# maps; elsewhere it cannot, and warns.
_MAPS = os.path.exists("/proc/self/maps")


def test_partial_mode_removes_or_warns_once_when_rasterio_registered_first():
    """rasterio registered GDAL's drivers before GeoBrix was imported, so the skip cannot apply: on Linux the policy
    removes the refused ones from rasterio's libgdal (ctypes, the libgdal the process maps) and nothing is left to warn
    of; elsewhere one loud warning names the drivers left and the fix, points at the caller's line, and the state
    lists them."""
    _rasterio_or_skip()
    code = (
        _REGISTERED_FIRST + "import warnings\n"
        "with warnings.catch_warnings(record=True) as caught:\n"
        "    warnings.simplefilter('always')\n"
        "    import databricks.labs.gbx\n"
        "    from databricks.labs.gbx import _gdal_policy as p\n"
        "    state = p.state()\n"
        "    p.state()\n"
        "gap = [w for w in caught if 'Fix: import databricks.labs.gbx before rasterio' in str(w.message)]\n"
        "with rasterio.Env() as env:\n"
        "    left = 'GDALG' in env.drivers()\n"
        "print('MODE', state['mode'], 'GDALG' in state['registered_refused'], 'GDALG' in state['deregistered'])\n"
        "print('WARNINGS', len(gap), gap and gap[0].filename, 'LEFT', left)\n"
    )
    out = _partial_run(code)
    if _MAPS:
        assert "MODE partial False True" in out, out[-800:]
        assert "WARNINGS 0 [] LEFT False" in out, out[-800:]
    else:
        assert "MODE partial True False" in out, out[-800:]
        assert "WARNINGS 1 <string> LEFT True" in out, out[-800:]


def test_partial_mode_removes_or_names_the_drivers_a_pyogrio_imported_first_registered():
    """A PyPI pyogrio imported before GeoBrix registered every driver of its own GDAL at its import: on Linux the
    policy removes the refused ones from pyogrio's libgdal too (no WFS for its vector reads); elsewhere the warning
    names pyogrio and its refused drivers, and points at the caller's line (the `import`).
    """
    _rasterio_or_skip()
    pytest.importorskip("pyogrio")
    code = (
        "import warnings\n"
        "import pyogrio\n"
        "with warnings.catch_warnings(record=True) as caught:\n"
        "    warnings.simplefilter('always')\n"
        "    import databricks.labs.gbx\n"
        "    from databricks.labs.gbx import _gdal_policy as p\n"
        "    state = p.state()\n"
        "gap = [w for w in caught if 'cannot remove these drivers' in str(w.message)]\n"
        "ogr = [k for k, v in state['deregistered_by'].items() if 'pyogrio' in k and 'WFS' in v]\n"
        "print('BY', sorted(state['registered_refused_by']), 'WFS' in state['registered_refused_by'].get('pyogrio', []))\n"
        "print('REMOVED', len(ogr), 'WFS' in pyogrio.list_drivers())\n"
        "print('WARN', len(gap), bool(gap) and 'pyogrio: ' in str(gap[0].message), gap and gap[0].filename)\n"
    )
    out = _partial_run(code)
    if _MAPS:
        assert "BY [] False" in out and "REMOVED 1 False" in out, out[-800:]
        assert "WARN 0 False []" in out, out[-800:]
    else:
        assert "BY ['pyogrio'] True" in out and "REMOVED 0 True" in out, out[-800:]
        assert "WARN 1 True <string>" in out, out[-800:]


def test_a_partial_process_where_gdalg_registered_still_refuses_a_stored_pipeline(
    tmp_path,
):
    """Where the refused driver did register (rasterio first), open_data still opens with the allowed drivers only: a
    stored pipeline, which GDALG would open there, is refused with the policy's reason.
    """
    _rasterio_or_skip()
    dem = tmp_path / "dem.tif"
    gdalg = tmp_path / "dem.gdalg.json"
    code = (
        _REGISTERED_FIRST + "import json\n"
        "import numpy as np\n"
        "from rasterio.transform import from_origin\n"
        f"dem, gdalg = {str(dem)!r}, {str(gdalg)!r}\n"
        "with rasterio.open(dem, 'w', driver='GTiff', width=4, height=4, count=1, dtype='float32',\n"
        "                   crs='EPSG:32631', transform=from_origin(0, 4, 1, 1)) as w:\n"
        "    w.write(np.arange(16, dtype='float32').reshape(1, 4, 4))\n"
        "json.dump({'type': 'gdal_streamed_alg', 'command_line': 'gdal raster pipeline read ' + dem + ' ! slope',\n"
        "           'relative_paths_relative_to_this_file': False}, open(gdalg, 'w'))\n"
        "with rasterio.open(gdalg) as plain:\n"
        "    print('PLAIN', plain.driver)\n"
        "from databricks.labs.gbx._gdal_policy import open_data\n"
        "try:\n"
        "    open_data(gdalg).close()\n"
        "    print('OPENED')\n"
        "except Exception as exc:\n"
        "    print('REFUSED ' + str(exc))\n"
    )
    out = _partial_run(code)
    assert "PLAIN GDALG" in out, out[-800:]
    assert (
        "REFUSED" in out
        and "is not a raster any driver GeoBrix opens reads" in out
        and "which GeoBrix does not open" in out
    ), out[-800:]


class _Opened:
    """What a captured open returns: a dataset GeoBrix's check reads the driver of."""

    driver = "GTiff"

    def __init__(self):
        import contextlib

        self._env = contextlib.ExitStack()

    def close(self):
        self._env.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def test_every_read_through_open_data_names_the_allowed_drivers(monkeypatch):
    """open_data never opens a read with GDAL's whole registry: whatever the mode (full, partial, none), the name (a
    local file, a /vsicurl/ or /vsimem/ name, a MemoryFile, a file-like) and whether the identify found a driver, GDAL
    is given the identified driver or the allowed list. The opens are captured, not made: no file or network is read.
    """
    _rasterio_or_skip()
    import io

    import rasterio
    import rasterio.io as rio_io

    allowed = list(_gdal_policy.allowed_names())
    seen = []

    def fake_open(fp, mode="r", driver=None, **kw):
        seen.append(("rasterio.open", mode, driver))
        return _Opened()

    def fake_reader(path, driver=None, **kw):
        seen.append(("DatasetReader", "r", driver))
        return _Opened()

    class FakeMemoryFile:
        def __init__(self, data=None):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def open(self, driver=None, **kw):
            seen.append(("MemoryFile.open", "r", driver))
            return _Opened()

    monkeypatch.setattr(rasterio, "open", fake_open)
    monkeypatch.setattr(rio_io, "DatasetReader", fake_reader)
    monkeypatch.setattr(rio_io, "MemoryFile", FakeMemoryFile)

    cases = [
        # (mode, fp, open mode, identified, exists locally, the driver GDAL must be given)
        ("full", "/data/a.tif", "r", "GTiff", True, "GTiff"),
        ("full", "/vsicurl/http://127.0.0.1:9/a.tif", "r", None, False, allowed),
        ("full", "/vsimem/nowhere.tif", "r", None, False, allowed),
        ("full", "/vsimem/nowhere.tif", "r+", None, False, allowed),
        ("partial", "/data/a.tif", "r", None, True, allowed),
        ("none", "/data/a.tif", "r", None, True, allowed),
        ("full", FakeMemoryFile(), "r", None, False, allowed),
        ("partial", io.BytesIO(b"bytes"), "r", None, False, allowed),
    ]
    for mode, fp, open_mode, identified, exists, want in cases:
        seen.clear()
        monkeypatch.setattr(_gdal_policy, "activate", lambda m=mode: {"mode": m})
        monkeypatch.setattr(_gdal_policy, "_identify", lambda path, n=identified: n)
        monkeypatch.setattr(_gdal_policy, "_exists", lambda path, e=exists: e)
        _gdal_policy.open_data(fp, open_mode).close()
        assert seen, (mode, fp)
        first = seen[0][2]
        assert first == want, (mode, fp, open_mode, seen)
        for _call, _mode, driver in seen:
            assert driver is not None, (mode, fp, seen)
            names = [driver] if isinstance(driver, str) else driver
            assert set(names) <= set(allowed), (mode, fp, seen)
    # a driver the policy refuses is refused by name in every mode, before any open
    for mode in ("full", "partial", "none"):
        seen.clear()
        monkeypatch.setattr(_gdal_policy, "activate", lambda m=mode: {"mode": m})
        with pytest.raises(_gdal_policy.DriverPolicyError, match="driver GDALG is one"):
            _gdal_policy.open_data("/data/a.gdalg.json", driver="GDALG")
        assert seen == []


@pytest.mark.policy_meta
def test_no_security_row_was_skipped_where_the_payload_is_present(request):
    """The meta-row: where the payload is present, no driver-policy row may skip. A row that skips because
    this process's rasterio is another GDAL (a test module importing rasterio before GeoBrix) proves nothing, so the
    run fails instead. It runs after the policy rows (test/conftest.py orders it), and checks their outcomes.
    """
    if _native.native_dir() is None:
        pytest.skip(
            "PRECONDITION MISSING: no env-wheel payload (GBX_NATIVE_ROOT); the policy rows skip by design here"
        )
    rows = [
        it
        for it in request.session.items
        if "gdal_policy" in it.keywords and "policy_meta" not in it.keywords
    ]
    skipped = sorted(
        it.nodeid for it in rows if getattr(it, "gbx_policy_outcome", None) == "skipped"
    )
    not_run = sorted(it.nodeid for it in rows if not hasattr(it, "gbx_policy_outcome"))
    assert (
        skipped == [] and not_run == []
    ), f"policy rows skipped where the payload is present: {skipped}; not run before this row: {not_run}"


@pytest.mark.integration
def test_the_one_switch_off_leaves_gdal_as_gdal_has_it():
    """GBX_GDAL_DRIVER_POLICY=off: the same open that the policy refuses is GDAL's own, and it opens."""
    _payload_or_skip()
    out = _run(FRESH_GDALG_HEADER, {"GBX_GDAL_DRIVER_POLICY": "off"})
    assert "OPENED" in out and "REFUSED" not in out, out[-600:]


# The remote-path gate for paths that come from data: each path, and the location the gate names for it (None: the
# path is one GeoBrix opens). The switch (GBX_ALLOW_REMOTE_PATHS) lets every refused row through.
_URL = "/vsicurl/http://127.0.0.1:9/a.tif"
DATA_PATHS = [
    ("/Volumes/c/s/v/a.tif", None),
    ("/Workspace/Users/u/a.tif", None),
    ("/dbfs/tmp/a.tif", None),
    ("dbfs:/Volumes/c/s/v/a.tif", None),
    ("file:/tmp/a.tif", None),
    ("file:///tmp/a.tif", None),
    ("relative/a.tif", None),
    ("/data/vsi_cache/a.tif", None),
    ("s3://b/a.tif", None),
    ("s3a://b/a.tif", None),
    ("abfss://c@acct.dfs.core.windows.net/a.tif", None),
    ("gs://b/a.tif", None),
    ("wasbs://c@acct.blob.core.windows.net/a.tif", None),
    ("/vsis3/b/a.tif", None),
    ("/vsiaz/c/a.tif", None),
    ("/vsiadls/c/a.tif", None),
    ("/vsigs/b/a.tif", None),
    ("/vsizip//Volumes/c/s/v/a.zip/a.tif", None),
    ("/vsizip/{/tmp/a.zip}/a.tif", None),
    ("/vsitar//vsis3/b/a.tar/a.tif", None),
    ("zip+s3://b/a.zip!/a.tif", None),
    ("/vsimem/gbx/a.tif", None),
    (_URL, "/vsicurl/"),
    ("/vsicurl_streaming/https://h/a.tif", "/vsicurl_streaming/"),
    ("/vsicurl?url=http://h/a.tif", "/vsicurl/"),
    ("http://h/a.tif", "http://"),
    ("https://h/a.tif", "https://"),
    ("HTTPS://h/a.tif", "https://"),
    ("ftp://h/a.tif", "ftp://"),
    ("/vsizip//vsicurl/http://h/a.zip/a.tif", "/vsicurl/"),
    ("/vsitar/{/vsicurl_streaming/http://h/a.tar}/a.tif", "/vsicurl_streaming/"),
    ("zip+https://h/a.zip!/a.tif", "zip+https://"),
    ('NETCDF:"/vsicurl/http://h/a.nc":v', "/vsicurl/"),
    ("/vsiwebhdfs/http://h/a.tif", "/vsiwebhdfs/"),
]


@pytest.mark.parametrize("path,location", DATA_PATHS, ids=[p for p, _ in DATA_PATHS])
def test_the_remote_path_gate_refuses_a_remote_location_unless_the_switch_allows_it(
    monkeypatch, path, location
):
    monkeypatch.delenv(_gdal_policy.REMOTE_ENV, raising=False)
    if location is None:
        assert _gdal_policy.data_path(path, "the tile") == path
    else:
        with pytest.raises(_gdal_policy.RemotePathError) as raised:
            _gdal_policy.data_path(path, "the tile")
        msg = str(raised.value)
        assert (
            f"({location})" in msg and _gdal_policy.REMOTE_ENV in msg and "Fix:" in msg
        ), msg
    monkeypatch.setenv(_gdal_policy.REMOTE_ENV, "true")
    assert _gdal_policy.data_path(path, "the tile") == path


def _vrt_naming(tmp_path, source: str) -> str:
    vrt = tmp_path / "remote.vrt"
    vrt.write_text(
        '<VRTDataset rasterXSize="1" rasterYSize="1"><VRTRasterBand dataType="Byte" band="1"><SimpleSource>'
        f'<SourceFilename relativeToVRT="0">{source}</SourceFilename><SourceBand>1</SourceBand>'
        "</SimpleSource></VRTRasterBand></VRTDataset>"
    )
    return str(vrt)


def _through_open_tile(tmp_path):
    from databricks.labs.gbx.pyrx.core import open_tile as ot

    with ot._open({"cellid": 0, "raster": None, "path": _URL, "window": None}):
        pass


def _through_a_manifest_row(tmp_path):
    from databricks.labs.gbx.ds import raster as r

    r._partitions_from_tile_rows(
        [{"path": _URL, "window": [0, 0, 1, 1]}],
        emit_virtual=True,
        budget_bytes=1 << 20,
    )


def _through_a_vrt_member(tmp_path):
    from databricks.labs.gbx.ds import raster as r

    r._parse_vrt_members(_vrt_naming(tmp_path, _URL))


def _through_a_vector_table_path(tmp_path):
    import pandas as pd

    from databricks.labs.gbx.pyvx import file_read

    decode = file_read._make_vector_decode_fn(True, 0)
    next(
        decode(iter([pd.DataFrame({"source": ["/vsicurl/http://127.0.0.1:9/a.gpkg"]})]))
    )


def _through_vizx(tmp_path):
    pytest.importorskip("matplotlib")
    from databricks.labs.gbx.vizx import _raster

    _raster.plot_raster(
        {"cellid": 0, "raster": None, "path": _URL, "window": None}, debug_mode=0
    )


GATED_SITES = {
    "virtual-tile": _through_open_tile,
    "manifest-row": _through_a_manifest_row,
    "vrt-member": _through_a_vrt_member,
    "vector-table-path": _through_a_vector_table_path,
    "vizx-virtual-tile": _through_vizx,
}


@pytest.mark.parametrize("site", list(GATED_SITES))
def test_each_path_from_data_goes_through_the_remote_path_gate(
    monkeypatch, tmp_path, site
):
    """Each place a path from data is opened refuses a remote one before any open (no connection is made)."""
    _rasterio_or_skip()
    monkeypatch.delenv(_gdal_policy.REMOTE_ENV, raising=False)
    with pytest.raises(_gdal_policy.RemotePathError, match=r"\(/vsicurl/\)"):
        GATED_SITES[site](tmp_path)


def test_the_engine_gives_an_error_tile_for_a_remote_virtual_tile(monkeypatch):
    """The L3 engine's read of a virtual tile goes through the same gate: the error tile names the location."""
    from databricks.labs.gbx.engine import l3, run

    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    monkeypatch.delenv(_gdal_policy.REMOTE_ENV, raising=False)
    fn = manifest.load().functions["gbx_rst_slope"]
    with pytest.raises(run.DataError, match=r"\(/vsicurl/\)"):
        l3._inputs(fn, {"cellid": 0, "raster": None, "path": _URL, "window": None})
