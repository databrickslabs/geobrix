"""Structural guard: every direct raster open in the Python sources goes through ``open_data`` (a name) or
``open_bytes`` (bytes), or is an allowlisted site with its reason.

A direct open is a call that opens a raster with GDAL's whole driver registry, under whatever name the file binds the
module or the function to (``import rasterio as _rio``, ``from rasterio.shutil import copy``, ``rasterio.shutil.copy``):

- by name, on a path that is not a string literal: ``rasterio.open``, rasterio's ``DatasetReader``, osgeo's
  ``gdal.Open`` / ``gdal.OpenEx`` / ``gdal.OpenShared`` (the path given positionally, or as ``utf8_path=``), the
  ``GDALOpen*`` names of the ctypes engine, rio-tiler's ``Reader`` (``rio_tiler.io`` or ``rio_tiler.io.rasterio``)
  unless it is handed an open dataset, a rasterio ``shutil.copy``, and the GDAL utilities (``Translate``, ``Warp``,
  ``Info``, ``BuildVRT``, ``DEMProcessing``, ``VectorTranslate``, ``MultiDimTranslate``) given a non-literal source;
- of bytes: the read form of a ``MemoryFile`` or a ``FilePath`` (``.open()`` with no positional argument and only
  the read keywords ``driver``, ``sharing``, ``thread_safe``, ``opener``), chained (``MemoryFile(b).open()``) or on a
  name or attribute the file binds to one (``with MemoryFile(b) as mf``, ``mf = MemoryFile(b)``, an annotated
  assignment, ``mf = stack.enter_context(MemoryFile(b))``, ``self._mf = MemoryFile(b)``).

A source given as a literal (a string, or a list of strings) is no site. Such a call is the policy's (``_gdal_policy``,
which is not scanned) or a site whose file or bytes GeoBrix itself wrote, with the reason given below. Not detected,
on purpose: ``getattr(rasterio, "open")``, a ctypes ``GDALOpenShared``, and a ``MemoryFile`` reached through a name
the file does not bind to one (a parameter); none is used today.

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
    "rasterio.io": {"DatasetReader"},
    "osgeo.gdal": {"Open", "OpenEx", "OpenShared"},
    "osgeo.ogr": {"Open", "OpenShared"},
    "rasterio.shutil": {"copy"},
    "rio_tiler.io": {"Reader"},
    "rio_tiler.io.rasterio": {"Reader"},
}
FIXED_ATTRS = {"open", "Open", "OpenEx", "OpenShared", "GDALOpen", "GDALOpenEx"}
COPY_RECEIVERS = ("rio_shutil", "_rio_shutil", "rio_copy")
PATH_KEYWORDS = ("fp", "input", "src_path", "path", "src", "utf8_path")
# osgeo.gdal's utilities: the position of the source argument, and its keyword names
UTILITIES = {
    "Info": 0,
    "Translate": 1,
    "Warp": 1,
    "BuildVRT": 1,
    "DEMProcessing": 1,
    "VectorTranslate": 1,
    "MultiDimTranslate": 1,
}
SOURCE_KEYWORDS = ("ds", "srcDS", "srcDSOrSrcDSTab")
# the keywords of a MemoryFile's or FilePath's read form (a write form passes the profile: width, count, dtype ...)
READ_KEYWORDS = {"driver", "sharing", "thread_safe", "opener"}
# a MemoryFile read form has no path: it is a site whatever it holds
BYTES = object()


def _arg(call: ast.Call, index: int, keywords):
    if len(call.args) > index:
        return call.args[index]
    for kw in call.keywords:
        if kw.arg in keywords:
            return kw.value
    return None


def _bindings(tree) -> tuple:
    """(modules, functions, memfiles): the file's local names for the modules of OPENERS, for their opening functions,
    and for MemoryFile instances."""
    modules, functions, memfiles = {}, {}, set()
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
                elif node.module == "osgeo.gdal" and a.name in UTILITIES:
                    functions[a.asname or a.name] = (node.module, a.name)
        elif isinstance(node, ast.withitem) and _is_memoryfile(node.context_expr):
            if node.optional_vars is not None and _dotted(node.optional_vars):
                memfiles.add(_dotted(node.optional_vars))
        elif isinstance(node, ast.Assign) and _is_memoryfile(node.value):
            memfiles.update(_dotted(t) for t in node.targets if _dotted(t))
        elif isinstance(node, ast.AnnAssign) and _is_memoryfile(node.value):
            if _dotted(node.target):
                memfiles.add(_dotted(node.target))
    return modules, functions, memfiles


def _is_memoryfile(node) -> bool:
    """A MemoryFile (or FilePath) made here, directly or through ``stack.enter_context(...)``."""
    if not isinstance(node, ast.Call):
        return False
    f = node.func
    if isinstance(f, ast.Attribute) and f.attr == "enter_context" and node.args:
        return _is_memoryfile(node.args[0])
    names = ("MemoryFile", "FilePath")
    return (isinstance(f, ast.Name) and f.id in names) or (
        isinstance(f, ast.Attribute) and f.attr in names
    )


def _is_read_form(call: ast.Call) -> bool:
    return not call.args and all(kw.arg in READ_KEYWORDS for kw in call.keywords)


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


def _opened(call: ast.Call, modules: dict, functions: dict, memfiles: set):
    """What a direct open opens (the path node, or BYTES), or None when the call is not a direct open."""
    func = call.func
    if isinstance(func, ast.Name):
        target = functions.get(func.id)
        if target is None or (target[1] == "Reader" and _hands_a_dataset(call)):
            return None
        if target[0] == "osgeo.gdal" and target[1] in UTILITIES:
            return _arg(call, UTILITIES[target[1]], SOURCE_KEYWORDS)
        return _arg(call, 0, PATH_KEYWORDS)
    if not isinstance(func, ast.Attribute):
        return None
    if (
        func.attr == "open"
        and (_is_memoryfile(func.value) or _dotted(func.value) in memfiles)
        and _is_read_form(call)
    ):
        return BYTES
    recv = _dotted(func.value)
    if recv is None:
        return None
    head, _, rest = recv.partition(".")
    module = modules.get(head, head if head in ("rasterio", "osgeo") else None)
    qual = (f"{module}.{rest}" if rest else module) if module is not None else None
    if qual is not None and func.attr in OPENERS.get(qual, ()):
        if func.attr == "Reader" and _hands_a_dataset(call):
            return None
        return _arg(call, 0, PATH_KEYWORDS)
    gdal_module = qual == "osgeo.gdal" or (
        "." not in recv and recv in ("gdal", "_gdal")
    )
    if gdal_module and func.attr in UTILITIES:
        return _arg(call, UTILITIES[func.attr], SOURCE_KEYWORDS)
    if "." not in recv and recv in OPEN_RECEIVERS and func.attr in FIXED_ATTRS:
        return _arg(call, 0, PATH_KEYWORDS)
    if func.attr == "copy" and recv in COPY_RECEIVERS:
        return _arg(call, 0, PATH_KEYWORDS)
    return None


def _literal(node) -> bool:
    """A source given as a literal: a string, or a list or tuple of strings."""
    if isinstance(node, ast.Constant):
        return isinstance(node.value, str)
    if isinstance(node, (ast.List, ast.Tuple)):
        return bool(node.elts) and all(_literal(e) for e in node.elts)
    return False


def open_sites_in(text: str):
    """(line number, source line) of every direct open: of bytes, or of a path that is not a string literal."""
    tree = ast.parse(text)
    lines = text.splitlines()
    modules, functions, memfiles = _bindings(tree)
    hits = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        what = _opened(node, modules, functions, memfiles)
        if what is None or _literal(what):
            continue
        hits.append((node.lineno, lines[node.lineno - 1].strip()))
    return sorted(hits)


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
        "copies the DEM from the dataset the caller opened (through open_data or open_bytes); no name is opened here",
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
    (
        "_native/_selfcheck.py",
        "with MemoryFile(geotiff_bytes()) as mf, mf.open() as ds:",
    ): (
        1,
        "the self-check reads the GeoTIFF bytes it encoded a line above (geotiff_bytes)",
    ),
    ("bench/compression_sweep.py", "with mf2.open() as ds2:"): (
        1,
        "the compression sweep reads back the bytes it encoded a few lines above",
    ),
    ("bench/datagen.py", "with mf.open() as src:"): (
        1,
        "the data generator reads the tile bytes it synthesized a few lines above",
    ),
    ("bench/oracles.py", "a.open() as got,"): (
        2,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    ("bench/oracles.py", "b.open() as base,"): (
        1,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    ("bench/oracles.py", "b.open() as want,"): (
        1,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    (
        "bench/oracles.py",
        "with MemoryFile(_raster_of(arg_rows[0][0])) as mf, mf.open() as src:",
    ): (
        1,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    (
        "bench/oracles.py",
        "with MemoryFile(_raster_of(args[0])) as mf, mf.open() as ds:",
    ): (
        1,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    (
        "bench/oracles.py",
        "with MemoryFile(_raster_of(args[0])) as mf, mf.open() as src:",
    ): (
        1,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    (
        "bench/oracles.py",
        "with MemoryFile(_raster_of(args[0])) as src_mf, src_mf.open() as src:",
    ): (
        1,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    (
        "bench/oracles.py",
        "with MemoryFile(_raster_of(tile)) as mf, mf.open() as src:",
    ): (
        1,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    (
        "bench/oracles.py",
        'with MemoryFile(_tile_bytes("EPSG:4326", 32, 32)) as mf, mf.open() as ds:',
    ): (
        1,
        "a bench oracle reads a tile it synthesized on the line (_tile_bytes)",
    ),
    ("bench/oracles.py", "with MemoryFile(a) as mf, mf.open() as ds:"): (
        2,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    ("bench/oracles.py", "with MemoryFile(b) as m, m.open() as ds:"): (
        1,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    (
        "bench/oracles.py",
        'with MemoryFile(bytes(_field(t, "raster"))) as o, o.open() as got:',
    ): (
        1,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    (
        "bench/oracles.py",
        'with MemoryFile(bytes(_field(t, "raster"))) as o, o.open() as piece:',
    ): (
        1,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    (
        "bench/oracles.py",
        'with MemoryFile(bytes(_field(tile, "raster"))) as mf, mf.open() as got:',
    ): (
        1,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    ("bench/oracles.py", "with MemoryFile(bytes(raster)) as a, a.open() as got:"): (
        1,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    ("bench/oracles.py", "with MemoryFile(bytes(raster)) as mf, mf.open() as ds:"): (
        1,
        "a bench oracle reads a tile the bench built as its fixture, or the result tile the function under test returned",
    ),
    ("sample/_bundle.py", "gdal.Translate("): (
        1,
        "the sample-bundle generator converts a raster it downloaded or wrote (the viewable Byte copy)",
    ),
    (
        "sample/_bundle.py",
        'gdal.Translate(str(tif_path), str(hgt_path), format="GTiff").Close()',
    ): (
        1,
        "the sample-bundle generator converts the SRTM .hgt it downloaded to GeoTIFF",
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


def test_the_guard_flags_bytes_utilities_and_the_reader_forms():
    text = (
        "import rasterio.io as rio_io\n"
        "from rasterio.io import DatasetReader, MemoryFile\n"
        "from rio_tiler.io.rasterio import Reader\n"
        "from osgeo import gdal\n"
        "def f(p, q, raw):\n"
        "    DatasetReader(p)\n"
        "    rio_io.DatasetReader(p)\n"
        "    gdal.OpenEx(utf8_path=p)\n"
        "    Reader(p)\n"
        "    gdal.Translate(q, p)\n"
        "    gdal.Info(p)\n"
        "    gdal.Warp(q, [p])\n"
        "    with MemoryFile(raw) as mf, mf.open() as ds:\n"
        "        pass\n"
        "    m2 = MemoryFile(raw)\n"
        "    m2.open()\n"
        "    gdal.Translate('/out.tif', '/fixed.tif')\n"
        "    with MemoryFile() as w, w.open(driver='GTiff', width=1, height=1, count=1, dtype='uint8') as d:\n"
        "        pass\n"
    )
    assert [n for n, _ in open_sites_in(text)] == [6, 7, 8, 9, 10, 11, 12, 13, 16]


def test_the_guard_flags_the_other_bytes_forms_and_a_utility_imported_by_name():
    text = (
        "import contextlib\n"
        "from rasterio.io import FilePath, MemoryFile\n"
        "from osgeo import gdal\n"
        "from osgeo.gdal import Translate\n"
        "def f(p, q, raw, fh, drv):\n"
        "    MemoryFile(raw).open()\n"
        "    with contextlib.ExitStack() as stack:\n"
        "        fresh = stack.enter_context(MemoryFile(raw))\n"
        "        fresh.open()\n"
        "    m: MemoryFile = MemoryFile(raw)\n"
        "    m.open(sharing=True)\n"
        "    m.open(driver=drv)\n"
        "    FilePath(fh).open()\n"
        "    Translate(q, p)\n"
        "    gdal.Warp('/o.tif', ['/i.tif'])\n"
        "    m.open(driver='GTiff', width=1, height=1, count=1, dtype='uint8')\n"
        "class C:\n"
        "    def g(self, raw):\n"
        "        self._mf = MemoryFile(raw)\n"
        "        return self._mf.open()\n"
    )
    assert [n for n, _ in open_sites_in(text)] == [6, 9, 11, 12, 13, 14, 20]


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


# ---------------------------------------------------------------------------
# The remote-path gate runs inside the openers: ``open_data`` judges every read of a named path, and the reads of
# ``_gdal_policy.pyogrio()`` every vector path, unless told ``remote_ok=True``. Together with the guard above (every
# raster open is ``open_data`` or ``open_bytes``), a light open of a path from data cannot pass around the gate: the
# opt-out appears only at the sites below, each a path that is not data; and pyogrio is imported only by the policy
# module and the listed sites, none of which reads a path from data through it.
# ---------------------------------------------------------------------------

REMOTE_OK_SITES = {
    (
        "stac/_download.py",
        "windowed_download",
    ): "an http(s) href core._http checked, or a path the gate allowed",
    ("vizx/_raster.py", "plot_file"): "the caller's own path, to a driver-side call",
    ("vizx/_raster.py", "plot_mosaic"): "the caller's own VRT, to a driver-side call",
    ("vizx/_cog.py", "plot_cog"): "the caller's own path, to a driver-side call",
    (
        "vizx/_maplibre.py",
        "_raster_to_image",
    ): "the caller's own layer path, to a driver-side call",
    (
        "vizx/_simplify.py",
        "_simplify_raster",
    ): "the caller's own path, to a driver-side call",
    ("models/runner.py", "_open_input"): "the caller's own path, to a driver-side call",
}
PYOGRIO_IMPORTS = {
    "_native/_selfcheck.py": "the payload's self-check of its own pyogrio, on files it writes in its scratch directory",
    "sample/_bundle.py": "the development-only sample-data generator (the security page names it)",
    "ds/vector.py": "pyogrio.errors (the exception types) and pyogrio.raw.write (a writer)",
}
# What a pyogrio import may be used for where it is not the policy module's: no read of a path.
_PYOGRIO_READS = {
    "read_arrow",
    "open_arrow",
    "read_info",
    "read_bounds",
    "read_dataframe",
    "list_layers",
    "read",
}


def remote_ok_sites_in(text: str) -> list:
    """The functions (by name) that call an opener with ``remote_ok`` other than a literal False."""
    tree = ast.parse(text)
    found = []

    def visit(node, where):
        for child in ast.iter_child_nodes(node):
            name = (
                child.name
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                else where
            )
            if isinstance(child, ast.Call):
                for kw in child.keywords:
                    if kw.arg == "remote_ok" and not (
                        isinstance(kw.value, ast.Constant) and kw.value.value is False
                    ):
                        found.append(where)
            visit(child, name)

    visit(tree, "<module>")
    return sorted(set(found))


_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)


def _own_nodes(scope):
    """The nodes of ``scope`` outside the functions and classes nested in it."""
    todo = list(ast.iter_child_nodes(scope))
    while todo:
        node = todo.pop()
        yield node
        if not isinstance(node, _SCOPES):
            todo.extend(ast.iter_child_nodes(node))


def _pyogrio_names(nodes) -> set:
    names = set()
    for node in nodes:
        if isinstance(node, ast.Import):
            names.update(
                a.asname or "pyogrio"
                for a in node.names
                if a.name.split(".")[0] == "pyogrio"
            )
        elif (
            isinstance(node, ast.ImportFrom)
            and (node.module or "").split(".")[0] == "pyogrio"
        ):
            names.update(a.asname or a.name for a in node.names)
    return names


def pyogrio_reads_in(text: str) -> tuple:
    """(whether the file imports pyogrio itself, the lines that read a path through a name such an import binds in
    that scope: a name assigned there, such as ``pyogrio = _gdal_policy.pyogrio()``, is the gated module instead).
    """
    tree = ast.parse(text)
    reads = []

    def check(scope, inherited):
        own = list(_own_nodes(scope))
        assigned = {
            t.id
            for n in own
            if isinstance(n, (ast.Assign, ast.AnnAssign))
            for t in (n.targets if isinstance(n, ast.Assign) else [n.target])
            if isinstance(t, ast.Name)
        }
        raw = (inherited - assigned) | _pyogrio_names(own)
        for node in own:
            if isinstance(node, _SCOPES):
                check(node, raw)
            elif isinstance(node, ast.Call):
                f = node.func
                if isinstance(f, ast.Name) and f.id in raw and f.id in _PYOGRIO_READS:
                    reads.append(node.lineno)
                elif isinstance(f, ast.Attribute) and f.attr in _PYOGRIO_READS:
                    root = f.value
                    while isinstance(root, ast.Attribute):
                        root = root.value
                    if isinstance(root, ast.Name) and root.id in raw:
                        reads.append(node.lineno)

    check(tree, set())
    return bool(_pyogrio_names(ast.walk(tree))), sorted(reads)


def test_the_openers_gate_every_path_but_the_listed_ones():
    remote_ok, imports, reads = {}, {}, {}
    for py in sorted(PY_ROOT.rglob("*.py")):
        if "__pycache__" in py.parts:
            continue
        rel = py.relative_to(PY_ROOT).as_posix()
        if rel in EXCLUDED_FILES:
            continue
        text = py.read_text()
        for fn in remote_ok_sites_in(text):
            remote_ok[(rel, fn)] = True
        imported, lines = pyogrio_reads_in(text)
        if imported:
            imports[rel] = True
        if lines and rel not in ("_native/_selfcheck.py", "sample/_bundle.py"):
            reads[rel] = lines
    assert sorted(remote_ok) == sorted(REMOTE_OK_SITES)
    assert sorted(imports) == sorted(PYOGRIO_IMPORTS)
    assert reads == {}, f"a path read through a pyogrio import of its own: {reads}"


def test_the_gate_guards_flag_a_planted_opt_out_and_a_raw_vector_read():
    text = (
        "import pyogrio.raw\n"
        "from databricks.labs.gbx import _gdal_policy\n"
        "from databricks.labs.gbx._gdal_policy import open_data\n"
        "def f(p):\n"
        "    with open_data(p, remote_ok=True) as ds:\n"
        "        return pyogrio.read_info(p)\n"
        "def g(p):\n"
        "    pyogrio = _gdal_policy.pyogrio()\n"
        "    pyogrio.read_info(p)\n"
        "    return open_data(p, remote_ok=False)\n"
    )
    assert remote_ok_sites_in(text) == ["f"]
    assert pyogrio_reads_in(text) == (True, [6])
