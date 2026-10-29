"""Structural guard: every direct raster open in the Python sources goes through ``open_data``, or is an allowlisted
site with its reason.

A direct open is a call to ``rasterio.open``, osgeo's ``gdal.Open`` / ``gdal.OpenEx`` (and the ``GDALOpen*`` names of
the ctypes engine), rio-tiler's ``Reader`` or a rasterio ``shutil.copy``, on a path that is not a string literal, under
whatever name the file binds the module or the function to (``import rasterio as _rio``, ``from rasterio.shutil import
copy``, ``rasterio.shutil.copy``). Such a call opens a file the data may name, with GDAL's whole registry and no driver
list of GeoBrix's, so it is the policy's (``_gdal_policy.open_data``) or it is a site whose file GeoBrix itself wrote,
with the reason given below.

The allowlist is keyed by file, the exact source text of the line, and the number of occurrences of that line in the
file: a new site, and a verbatim copy of an allowlisted line, both fail until the allowlist names them.
"""

import ast
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
PY_ROOT = REPO / "python/geobrix/src/databricks/labs/gbx"

# Names a module is often bound to without an import in the same file (a module passed in, the ctypes libgdal).
OPEN_RECEIVERS = {
    "rasterio",
    "_rasterio",
    "gdal",
    "_gdal",
    "ogr",
    "_ogr",
    "osgeo",
    "_gdal_lib",
    "gdal_lib",
}
# module -> the attributes of it that open a file by name
OPENERS = {
    "rasterio": {"open"},
    "osgeo.gdal": {"Open", "OpenEx", "OpenShared"},
    "osgeo.ogr": {"Open", "OpenShared"},
    "rasterio.shutil": {"copy"},
    "rio_tiler.io": {"Reader"},
}
FIXED_ATTRS = {"open", "Open", "OpenEx", "OpenShared", "GDALOpen", "GDALOpenEx"}
COPY_RECEIVERS = ("rio_shutil", "_rio_shutil", "rio_copy")
PATH_KEYWORDS = ("fp", "input", "src_path", "path", "src")


def _path_arg(call: ast.Call):
    if call.args:
        return call.args[0]
    for kw in call.keywords:
        if kw.arg in PATH_KEYWORDS:
            return kw.value
    return None


def _bindings(tree) -> tuple:
    """(modules, functions): the file's local names for the modules of OPENERS, and for their opening functions."""
    modules, functions = {}, {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name in OPENERS or a.name.startswith(("rasterio", "osgeo")):
                    if a.asname:
                        modules[a.asname] = a.name
                    else:
                        modules[a.name.split(".")[0]] = a.name.split(".")[0]
        elif isinstance(node, ast.ImportFrom) and node.module:
            for a in node.names:
                qual = f"{node.module}.{a.name}"
                if qual in OPENERS:
                    modules[a.asname or a.name] = qual
                elif a.name in OPENERS.get(node.module, ()):
                    functions[a.asname or a.name] = (node.module, a.name)
    return modules, functions


def _dotted(node):
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return ".".join(reversed(parts))
    return None


def _hands_a_dataset(call: ast.Call) -> bool:
    """rio-tiler's Reader opens its input by name unless it is handed an open dataset (dataset=...)."""
    return any(
        kw.arg == "dataset"
        and not (isinstance(kw.value, ast.Constant) and kw.value.value is None)
        for kw in call.keywords
    )


def _is_direct_open(call: ast.Call, modules: dict, functions: dict) -> bool:
    func = call.func
    if isinstance(func, ast.Name):
        target = functions.get(func.id)
        if target is None:
            return False
        return not (target[1] == "Reader" and _hands_a_dataset(call))
    if not isinstance(func, ast.Attribute):
        return False
    recv = _dotted(func.value)
    if recv is None:
        return False
    head, _, rest = recv.partition(".")
    module = modules.get(head, head if head in ("rasterio", "osgeo") else None)
    if module is not None:
        qual = f"{module}.{rest}" if rest else module
        if func.attr in OPENERS.get(qual, ()):
            return not (func.attr == "Reader" and _hands_a_dataset(call))
    if "." not in recv and recv in OPEN_RECEIVERS and func.attr in FIXED_ATTRS:
        return True
    return func.attr == "copy" and recv in COPY_RECEIVERS


def open_sites_in(text: str):
    """(line number, source line) of every direct open whose path is not a string literal."""
    tree = ast.parse(text)
    lines = text.splitlines()
    modules, functions = _bindings(tree)
    hits = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not _is_direct_open(
            node, modules, functions
        ):
            continue
        path = _path_arg(node)
        if path is None or (
            isinstance(path, ast.Constant) and isinstance(path.value, str)
        ):
            continue
        hits.append((node.lineno, lines[node.lineno - 1].strip()))
    return hits


# The choke point itself: the one place GeoBrix calls rasterio.open for a read (open_data).
EXCLUDED_FILES = {"_gdal_policy.py"}


def _scan():
    found = Counter()
    for py in sorted(PY_ROOT.rglob("*.py")):
        if "__pycache__" in py.parts:
            continue
        rel = py.relative_to(PY_ROOT).as_posix()
        if rel in EXCLUDED_FILES:
            continue
        for _lineno, line in open_sites_in(py.read_text()):
            found[(rel, line)] += 1
    return found


# Each allowlisted site: (file, exact line text) -> (occurrences, reason). A GeoBrix-written file is one this code
# created in its own scratch directory, or a dataset it wrote; a read of a data-named path is never allowlisted.
OPEN_SITES = {
    ("_native/_selfcheck.py", 'ds = ogr.Open(os.path.join(d, "r.gpkg"))'): (
        1,
        "the self-check's own GeoPackage, written in its scratch directory",
    ),
    (
        "_native/_selfcheck.py",
        'out = gdal.Open(os.path.join(d, "c.tif"))  # keep the dataset while its band is read',
    ): (1, "the self-check's own GeoTIFF, written in its scratch directory"),
    ("_osgeo.py", "ds = gdal.Open(mem.name)"): (
        1,
        "the cross-libgdal check's /vsimem fixture, which GeoBrix wrote a line above",
    ),
    ("_osgeo.py", "with rasterio.open(theirs) as src:"): (
        1,
        "the cross-libgdal check's /vsimem fixture, which osgeo wrote a line above",
    ),
    ("bench/runner.py", "with _rasterio.open(abs_p) as ds:"): (
        2,
        "the bench harness reads the rasters its own _synth step wrote (GeoBrix's outputs, never user data)",
    ),
    ("engine/run.py", "ds = gdal.OpenEx("): (
        1,
        "identify with the allowed drivers (IdentifyDriverEx), then open with the one identified: the engine's identify-then-open",
    ),
    (
        "engine/run.py",
        'ds = gdal.OpenEx(name, gdal.OF_RASTER | gdal.OF_READONLY, allowed_drivers=["GTiff"])',
    ): (
        1,
        "a /vsimem name the engine wrote from bytes that passed the TIFF magic check; GTiff only",
    ),
    ("pyrx/core/analysis.py", '_rio_shutil.copy(ds, dem_path, driver="GTiff")'): (
        1,
        "copies the DEM from the dataset the caller opened through open_data; no name is opened here",
    ),
    (
        "pyrx/core/analysis.py",
        "ds_h = gdal.GDALOpen(dem_path.encode(), 0)  # GA_ReadOnly = 0",
    ): (
        1,
        "the ctypes viewshed engine opens the DEM copy written a few lines above (GeoBrix's scratch file)",
    ),
    (
        "pyrx/core/analysis.py",
        'rio_shutil.copy(_src, dst_path, driver="COG", **creation)',
    ): (
        1,
        "the COG conversion copies from the dataset open_data opened on the line above",
    ),
    (
        "pyrx/core/analysis.py",
        'with _rasterio.open(dem_path, "w", **dem_profile) as wdst:',
    ): (
        1,
        "the windowed fallback writes GeoBrix's scratch DEM (a write, not a read of data)",
    ),
    ("pyrx/core/analysis.py", "with _rasterio.open(out_path) as result_ds:"): (
        1,
        "reads the viewshed output GDAL wrote into GeoBrix's scratch file",
    ),
    (
        "pyrx/core/xyz.py",
        "rasterio_shutil.copy(src_ds, dst_m.name, driver=driver)",
    ): (
        1,
        "copies an open dataset (the tile GeoBrix rendered into a MemoryFile) to another MemoryFile; no name is opened",
    ),
}


def test_every_direct_open_is_open_data_or_allowlisted():
    found = _scan()
    unlisted = sorted(k for k in found if k not in OPEN_SITES)
    assert (
        unlisted == []
    ), f"direct raster open(s) outside open_data and the allowlist: {unlisted}"
    counts = {k: n for k, n in found.items()}
    wrong = {
        k: (counts.get(k, 0), OPEN_SITES[k][0])
        for k in OPEN_SITES
        if counts.get(k, 0) != OPEN_SITES[k][0]
    }
    assert wrong == {}, f"allowlisted open count differs (found, listed): {wrong}"


def test_the_guard_flags_a_planted_open_of_a_data_named_path():
    text = "import rasterio\n\ndef f(p):\n    with rasterio.open(p) as ds:\n        return ds.read(1)\n"
    assert open_sites_in(text) == [(4, "with rasterio.open(p) as ds:")]


def test_the_guard_flags_rio_tiler_reader_and_shutil_copy_on_a_name():
    text = (
        "from rio_tiler.io import Reader\n"
        "import rasterio.shutil as rio_shutil\n"
        "def f(p, q):\n"
        "    with Reader(p) as cog:\n"
        "        rio_shutil.copy(p, q, driver='COG')\n"
    )
    assert [n for n, _ in open_sites_in(text)] == [4, 5]


def test_a_literal_path_and_a_non_gdal_open_are_not_sites():
    text = (
        "import rasterio\nimport xarray as xr\n"
        "with rasterio.open('/fixed/path.tif') as ds:\n    pass\n"
        "xr.open_dataset(p)\nopen(p)\n"
    )
    assert open_sites_in(text) == []


def test_the_guard_follows_the_names_a_file_binds_the_openers_to():
    text = (
        "import rasterio as _rio_cap\n"
        "import rasterio.shutil\n"
        "from rasterio.shutil import copy as rcopy\n"
        "from osgeo import gdal as G\n"
        "import rio_tiler.io\n"
        "def f(p, q, ds):\n"
        "    _rio_cap.open(p)\n"
        "    rasterio.shutil.copy(p, q)\n"
        "    rcopy(p, q)\n"
        "    G.OpenEx(p)\n"
        "    rio_tiler.io.Reader(p)\n"
        "    rio_tiler.io.Reader(p, dataset=ds)\n"
    )
    assert [n for n, _ in open_sites_in(text)] == [7, 8, 9, 10, 11]


def test_a_verbatim_copy_of_an_allowlisted_line_is_a_new_site():
    """The allowlist is keyed by occurrence count, so a second copy of an allowed line is caught."""
    body = "import rasterio\n\ndef f(p):\n    with rasterio.open(p) as ds:\n        return ds.read(1)\n"
    two = (
        body
        + "\ndef g(p):\n    with rasterio.open(p) as ds:\n        return ds.read(1)\n"
    )
    one_site = open_sites_in(body)
    two_sites = open_sites_in(two)
    assert len(one_site) == 1 and len(two_sites) == 2
    line = one_site[0][1]
    # the allowlist as it stands names one copy: the second copy makes the count differ
    assert Counter(text for _n, text in two_sites)[line] != 1
