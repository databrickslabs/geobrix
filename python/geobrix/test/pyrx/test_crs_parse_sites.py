"""Structural guard: every CRS parse site in the Scala and Python sources is the canonical resolver's, or an
allowlisted site with its reason. A new site outside the allowlist fails, in either language.

- Scala (src/main/scala): a grep for the OSR parse calls (ImportFrom*, SetFromUserInput), `new SpatialReference(text)`
  and the GDAL warp and translate options that take a CRS (`-t_srs`, `-s_srs`, `-a_srs`), comments dropped.
- Python (python/geobrix/src): an AST walk for the parse calls (`CRS.from_*`, `Transformer.from_crs`, `to_crs`, and the
  osr methods), so a string in a call is found, not a line of text.

The allowlist is keyed by file, the exact source text of the line, and the number of times that line occurs in the
file: a new call in an allowlisted file is a new site, and so is a verbatim copy of an allowlisted line. A site is
allowlisted only with its reason. The canonical resolvers (SpatialRefOps, core/crs.py) are the one place a CRS text is
parsed on the user's path. This guard does not follow a CRS value into a keyword argument of a library call
(rasterio's `crs=`); those values are resolved by their callers, which the sweep of every parse site reviewed.
"""

import ast
import re
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
SCALA_ROOT = (
    REPO / "src/main/scala/com/databricks/labs/gbx"
)  # the allowlist's paths are relative to this
PY_ROOT = (
    REPO / "python/geobrix/src/databricks/labs/gbx"
)  # ... and these are relative to this

# ---- Scala -------------------------------------------------------------------------------------------------------
SCALA_PARSE = re.compile(
    r"\b(?:ImportFrom\w+|SetFromUserInput)\(|\bnew SpatialReference\(\s*[^)\s]|-[tsa]_srs\b"
)
SCALA_COMMENT = re.compile(r"^\s*(?:/\*|\*|//)")

# The reasons below name the class of site. A site is found by file and exact source text.
INT_EPSG = "an integer SRID: OGR's int import names no file and opens no URL, so the value cannot reach PROJ's grid reads"
CONST_CRS = "a constant CRS (EPSG code written in the source), never a caller's value"
VIEWER_CONST = "a constant integer target (EPSG:4326 or 3857, the viewer's CRS): an integer names no file"
DATASET_WKT = (
    "GDAL's own export of a dataset's SRS (GetProjection): GDAL parsed the same text when the dataset opened, so no "
    "file is read here that the open did not read"
)
DATASET_INT = (
    "an EPSG code a dataset or grid already carries, an integer: no file is named"
)
GRID_INT = (
    "an EPSG code of a grid this code defines (a GridX constant, not a caller's text)"
)
WARP_CONST = "a warp option with a constant CRS (EPSG code in the source)"
WARP_DATASET = (
    "a warp option whose CRS is the dataset's own SRS (an AUTH:CODE GDAL reports, or a WKT file this code writes from "
    "the dataset's export)"
)
WARP_RESOLVED = "a warp option whose CRS is a resolved CRS (the canonical resolver's AUTH:CODE, or its WKT file)"

# The Scala sites are allowlisted by file and exact source line, each expected once unless SCALA_COUNTS says otherwise:
# a new line, or a second copy of an allowed one, is a new site. The one file-level entry is the canonical resolver
# itself. Each site is classified by its class (the constants above).
SCALA_FILE_ALLOW = {
    "operations/SpatialRefOps.scala": "the canonical resolver: every CRS text is parsed here, classified first",
}
SCALA_SITES = {
    (
        "rasterx/expressions/RST_AlignTo.scala",
        'val tSrs = srsToken.map(t => s"-t_srs $t ").getOrElse("")',
    ): DATASET_WKT,
    ("rasterx/expressions/RST_AlignTo.scala", "sr.ImportFromWkt(wkt)"): DATASET_WKT,
    ("rasterx/expressions/RST_BinPoints.scala", "sr.ImportFromEPSG(srid)"): INT_EPSG,
    (
        "rasterx/expressions/RST_Chm.scala",
        'val tSrs = srsToken.map(t => s"-t_srs $t ").getOrElse("")',
    ): DATASET_WKT,
    ("rasterx/expressions/RST_Chm.scala", "sr.ImportFromWkt(wkt)"): DATASET_WKT,
    (
        "rasterx/expressions/RST_Clip.scala",
        "if (dsEpsgCode != 0) geomSR.ImportFromEPSG(dsEpsgCode)",
    ): DATASET_INT,
    (
        "rasterx/expressions/RST_Clip.scala",
        "else geomSR.ImportFromWkt(dsSR.ExportToWkt())",
    ): DATASET_WKT,
    (
        "rasterx/expressions/RST_Transform.scala",
        "val rc = dstSR.ImportFromEPSG(srid)",
    ): INT_EPSG,
    (
        "rasterx/expressions/accessors/RST_Crs.scala",
        "val sr = new SpatialReference(proj)",
    ): DATASET_WKT,
    (
        "rasterx/expressions/accessors/RST_SRID.scala",
        "val proj = new SpatialReference(ds.GetProjection())",
    ): DATASET_WKT,
    (
        "rasterx/expressions/agg/RST_Custom_RasterizeAgg.scala",
        "val srcSR = new SpatialReference(); srcSR.ImportFromEPSG(gridCrs)",
    ): INT_EPSG,
    (
        "rasterx/expressions/agg/RST_Custom_RasterizeAgg.scala",
        "val dstSR = new SpatialReference(); dstSR.ImportFromEPSG(srid)",
    ): INT_EPSG,
    (
        "rasterx/expressions/agg/RST_H3_RasterizeAgg.scala",
        "val srcSR = new SpatialReference(); srcSR.ImportFromEPSG(H3.crsID)",
    ): INT_EPSG,
    (
        "rasterx/expressions/agg/RST_H3_RasterizeAgg.scala",
        "val dstSR = new SpatialReference(); dstSR.ImportFromEPSG(srid)",
    ): INT_EPSG,
    (
        "rasterx/expressions/agg/RST_Quadbin_RasterizeAgg.scala",
        "val srcSR = new SpatialReference(); srcSR.ImportFromEPSG(CRS_ID)",
    ): INT_EPSG,
    (
        "rasterx/expressions/agg/RST_Quadbin_RasterizeAgg.scala",
        "val dstSR = new SpatialReference(); dstSR.ImportFromEPSG(srid)",
    ): INT_EPSG,
    (
        "rasterx/expressions/agg/RasterizeBurn.scala",
        "val s = new SpatialReference(); s.ImportFromEPSG(srid); s",
    ): INT_EPSG,
    (
        "rasterx/expressions/agg/RasterizeBurn.scala",
        "val d = new SpatialReference(); d.ImportFromEPSG(grid.crsSrid); d",
    ): INT_EPSG,
    (
        "rasterx/expressions/grid/GridReprojection.scala",
        "val dstSR = new SpatialReference(); dstSR.ImportFromEPSG(gridSrid)",
    ): GRID_INT,
    (
        "rasterx/expressions/grid/GridReprojection.scala",
        "val srcSR = new SpatialReference(); srcSR.ImportFromWkt(srcWkt)",
    ): DATASET_WKT,
    (
        "rasterx/expressions/grid/GridReprojection.scala",
        'val (result, _) = WarpSource.warpFillAsNoData(ds, resultPath, s"gdalwarp -t_srs EPSG:$gridSrid -r near")',
    ): GRID_INT,
    (
        "rasterx/expressions/grid/RST_H3_CellBBox.scala",
        "val gridSR = new SpatialReference(); gridSR.ImportFromEPSG(H3.crsID)",
    ): CONST_CRS,
    (
        "rasterx/expressions/grid/RST_H3_CellBBox.scala",
        "val srcSR = new SpatialReference(); srcSR.ImportFromEPSG(H3.crsID)",
    ): CONST_CRS,
    (
        "rasterx/expressions/grid/RasterToGridGeneric.scala",
        "sr.ImportFromEPSG(crsSrid)",
    ): GRID_INT,
    (
        "rasterx/expressions/vector/RST_Isoband.scala",
        "val outSr = if (srcSrs != null) srcSrs else { sr.ImportFromEPSG(4326); sr }",
    ): CONST_CRS,
    (
        "rasterx/expressions/vector/RST_Polygonize.scala",
        "val outSr = if (srcSrs != null) srcSrs else { sr.ImportFromEPSG(4326); sr }",
    ): CONST_CRS,
    (
        "rasterx/expressions/web/RST_TileXYZ.scala",
        'command = s"gdalwarp -t_srs EPSG:3857 -te $xmin $ymin $xmax $ymax -ts $size $size -r $resampling -dstalpha"',
    ): WARP_CONST,
    (
        "rasterx/expressions/web/RST_ToWebMercator.scala",
        'command = s"gdalwarp -t_srs EPSG:3857 -r $resampling"',
    ): WARP_CONST,
    ("rasterx/gdal/GDAL.scala", "wsg84.ImportFromEPSG(4326)"): CONST_CRS,
    ("rasterx/gdal/GDAL.scala", "epsg3857.ImportFromEPSG(3857)"): CONST_CRS,
    (
        "rasterx/operations/CustomGridBounds.scala",
        "gridSR.ImportFromEPSG(grid.crsSrid)",
    ): GRID_INT,
    (
        "rasterx/operations/RasterAlignment.scala",
        "val sa = new SpatialReference(); sa.ImportFromWkt(wa)",
    ): DATASET_WKT,
    (
        "rasterx/operations/RasterAlignment.scala",
        "val sb = new SpatialReference(); sb.ImportFromWkt(wb)",
    ): DATASET_WKT,
    (
        "rasterx/operations/RasterAlignment.scala",
        "val sr = new SpatialReference(); sr.ImportFromWkt(wkt)",
    ): DATASET_WKT,
    (
        "rasterx/operations/RasterProject.scala",
        'command = s"gdalwarp -t_srs $authName:$authCode"',
    ): WARP_RESOLVED,
    (
        "rasterx/operations/RasterProject.scala",
        'command = s"gdalwarp -t_srs ${wktFile.toString}"',
    ): WARP_RESOLVED,
    (
        "rasterx/operations/RasterTessellate.scala",
        "sr.ImportFromEPSG(27700)",
    ): CONST_CRS,
    (
        "rasterx/operations/RasterTessellate.scala",
        "s.ImportFromWkt(srcWkt)",
    ): DATASET_WKT,
    (
        "rasterx/operations/RasterTessellate.scala",
        'val (result, _) = WarpSource.warpFillAsNoData(ds, resultPath, "gdalwarp -t_srs EPSG:27700 -r near")',
    ): WARP_CONST,
    (
        "rasterx/operations/RasterTessellate.scala",
        "sr.ImportFromEPSG(grid.crsSrid)",
    ): GRID_INT,
    ("rasterx/operator/GDALWarp.scala", 'srsVec.add("-s_srs")'): WARP_CONST,
    ("vectorx/mvt/MvtWriter.scala", "srs.ImportFromEPSG(3857)"): CONST_CRS,
}


def _scala_hits_in(rel: str, text: str):
    for lineno, line in enumerate(text.split("\n"), 1):
        if SCALA_COMMENT.match(line) or not SCALA_PARSE.search(line):
            continue
        yield rel, lineno, line.strip()


def _scala_hits():
    for path in sorted(SCALA_ROOT.rglob("*.scala")):
        rel = path.relative_to(SCALA_ROOT).as_posix()
        yield from _scala_hits_in(rel, path.read_text())


# (file, line) -> how many times the line occurs, where that is not once
SCALA_COUNTS: dict = {}


def _scala_allowed(rel: str, line: str) -> bool:
    return rel in SCALA_FILE_ALLOW or (rel, line) in SCALA_SITES


def _drift(hits, file_allow, sites, counts):
    """(new sites, wrong counts): the hits outside file_allow that sites does not list, and each listed site whose
    number of occurrences differs from its expected count (1 unless counts says otherwise).
    """
    found = Counter((rel, line) for rel, _n, line in hits if rel not in file_allow)
    new = sorted(k for k in found if k not in sites)
    wrong = {
        k: (found.get(k, 0), counts.get(k, 1))
        for k in sites
        if found.get(k, 0) != counts.get(k, 1)
    }
    return new, wrong


# ---- Python ------------------------------------------------------------------------------------------------------
PY_CRS_CLASS_PARSE = {
    "from_user_input",
    "from_proj4",
    "from_string",
    "from_authority",
    "from_epsg",
    "from_wkt",
}
PY_CONSTRUCTORS = {"CRS", "SpatialReference"}
PY_OSR_PARSE = {
    "SetFromUserInput",
    "ImportFromProj4",
    "ImportFromWkt",
    "ImportFromEPSG",
    "ImportFromESRI",
}
PY_ALLOW_BY_LINE = {
    ("engine/argv.py", "rc = sr.SetFromUserInput("): (
        "the engine's binder: crs_text_kind classifies the value first (never a file, a URL or a grid by path), then "
        "OSR parses it with file and network access off"
    ),
    ("engine/argv.py", "rc = sr.ImportFromEPSG(int(v))"): INT_EPSG,
    ("bench/oracles.py", "dst_crs = CRS.from_user_input(target_crs)"): (
        "the oracle's independent reference: rasterio parses the case's fixed CRS by design (it must not call gbx "
        "code); the values are the oracle's own constants"
    ),
    (
        "pyrx/ortho.py",
        "tf = pyproj.Transformer.from_crs(4326, epsg, always_xy=True)",
    ): INT_EPSG,
    ("sample/_bundle.py", "srs.ImportFromEPSG(4326)"): CONST_CRS,
    (
        "_native/_selfcheck.py",
        't = pyproj.Transformer.from_crs("EPSG:27700", "EPSG:4258", always_xy=True)',
    ): CONST_CRS,
    (
        "_native/_selfcheck.py",
        "fwd = pyproj.Transformer.from_crs(src, dst, always_xy=True)",
    ): ("the native self-check: its CRS pairs are its own constants"),
    (
        "vizx/_static_map.py",
        'tx, ty = Transformer.from_crs(src_crs, "EPSG:3857", always_xy=True).transform(',
    ): (
        "src_crs is the layer's CRS object (the reader resolved it); no string from a caller reaches this call"
    ),
    ("bench/oracles.py", "srs.ImportFromEPSG(3857)"): CONST_CRS,
    ("vizx/_dynamic.py", "first_source = _gdf_for(layer).to_crs(4326)"): VIEWER_CONST,
    ("vizx/_maplibre.py", "gdf = _gdf_for(layer).to_crs(4326)"): VIEWER_CONST,
    ("vizx/_maplibre.py", "source = _gdf_for(layer).to_crs(4326)"): VIEWER_CONST,
    (
        "vizx/_static_map.py",
        "plot_gdf = gdf.to_crs(3857) if gdf.crs is not None else gdf",
    ): VIEWER_CONST,
}
# (file, line) -> how many times the line occurs, where that is not once
PY_COUNTS = {
    (
        "vizx/_static_map.py",
        "plot_gdf = gdf.to_crs(3857) if gdf.crs is not None else gdf",
    ): 2,
}


def _receiver_text(node) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_receiver_text(node.value)}.{node.attr}"
    return ""


def _python_sites_in(text: str):
    """(line, source text) of every CRS parse call in `text`: a constructor by its bare name or its module."""
    lines = text.split("\n")  # the AST's line numbers count "\n" only
    for node in ast.walk(ast.parse(text)):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            # a constructor by its bare name: CRS(value), SpatialReference(value)
            site = node.func.id in PY_CONSTRUCTORS and bool(node.args)
        elif isinstance(node.func, ast.Attribute):
            attr = node.func.attr
            recv = _receiver_text(node.func.value)
            site = (
                (attr in PY_CRS_CLASS_PARSE and recv.split(".")[-1].endswith("CRS"))
                or (attr == "from_crs" and recv.split(".")[-1] == "Transformer")
                or attr == "to_crs"
                or attr in PY_OSR_PARSE
                # a constructor reached through a module: pyproj.CRS(value), osr.SpatialReference(value)
                or (attr in PY_CONSTRUCTORS and bool(node.args))
            )
        else:
            continue
        if site:
            yield node.lineno, lines[node.lineno - 1].strip()


def _python_hits():
    for path in sorted(PY_ROOT.rglob("*.py")):
        rel = path.relative_to(PY_ROOT).as_posix()
        for lineno, line in _python_sites_in(path.read_text()):
            yield rel, lineno, line


PY_ALLOW_FILES = {
    "core/crs.py": "the canonical resolver: every CRS text is classified and parsed here"
}


def _python_allowed(rel: str, line: str) -> bool:
    return rel in PY_ALLOW_FILES or (rel, line) in PY_ALLOW_BY_LINE


def test_the_scala_parse_sites_are_the_allowlisted_ones():
    hits = list(_scala_hits())
    assert hits, "the scan found no CRS parse site: the guard's pattern is wrong"
    new, wrong = _drift(hits, SCALA_FILE_ALLOW, SCALA_SITES, SCALA_COUNTS)
    assert new == [], (
        "new CRS parse site(s), outside the allowlist (route through SpatialRefOps.resolveCrs or add a reasoned entry):\n"
        + "\n".join(f"  {rel}: {line}" for rel, line in new)
    )
    assert (
        wrong == {}
    ), f"allowlisted CRS parse line count differs (found, listed): {wrong}"


def test_the_python_parse_sites_are_the_allowlisted_ones():
    hits = list(_python_hits())
    assert hits, "the scan found no CRS parse site: the guard's pattern is wrong"
    new, wrong = _drift(hits, PY_ALLOW_FILES, PY_ALLOW_BY_LINE, PY_COUNTS)
    assert new == [], (
        "new CRS parse site(s), outside the allowlist (route through resolve_crs or add a reasoned entry):\n"
        + "\n".join(f"  {rel}: {line}" for rel, line in new)
    )
    assert (
        wrong == {}
    ), f"allowlisted CRS parse line count differs (found, listed): {wrong}"


def test_a_verbatim_copy_of_an_allowlisted_parse_line_is_a_new_site():
    """The allowlists are keyed by occurrence count, in both languages: a second copy of an allowed line fails."""
    rel = "vectorx/mvt/MvtWriter.scala"
    line = "srs.ImportFromEPSG(3857)"
    text = (SCALA_ROOT / rel).read_text()
    hits = [h for h in _scala_hits() if h[0] != rel] + list(
        _scala_hits_in(rel, text + "\n" + line + "\n")
    )
    _new, wrong = _drift(hits, SCALA_FILE_ALLOW, SCALA_SITES, SCALA_COUNTS)
    assert wrong == {(rel, line): (2, 1)}
    prel, pline = (
        "pyrx/ortho.py",
        "tf = pyproj.Transformer.from_crs(4326, epsg, always_xy=True)",
    )
    ptext = (PY_ROOT / prel).read_text() + "\n\ndef _copy(epsg):\n    " + pline + "\n"
    phits = [h for h in _python_hits() if h[0] != prel] + [
        (prel, n, ln) for n, ln in _python_sites_in(ptext)
    ]
    _new, pwrong = _drift(phits, PY_ALLOW_FILES, PY_ALLOW_BY_LINE, PY_COUNTS)
    assert pwrong == {(prel, pline): (2, 1)}


def test_the_guard_finds_a_planted_unrouted_parse():
    """The guard is not vacuous: a raw ImportFromProj4 of a column value (the GeoJSONL writer's old shape) is a hit."""
    text = "val srs = new SpatialReference()\nsrs.ImportFromProj4(proj4)\n"
    found = [ln for ln in text.splitlines() if SCALA_PARSE.search(ln)]
    assert found == ["srs.ImportFromProj4(proj4)"]
    tree = ast.parse("CRS.from_user_input(value)\n")
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
    assert any(
        isinstance(n.func, ast.Attribute) and n.func.attr == "from_user_input"
        for n in calls
    )


def test_the_geojsonl_writer_parses_its_crs_through_the_resolver():
    """The GeoJSONL writer's PROJ4 and SRID columns go through SpatialRefOps.resolveCrs, never ImportFromProj4."""
    text = (SCALA_ROOT / "vectorx/ds/geojsonl/GeoJSONL_RowWriter.scala").read_text()
    assert "ImportFromProj4" not in text
    # the PROJ4 and SRID columns both go through resolveLoudly, which calls the canonical resolver (loud, with a Fix)
    assert "SpatialRefOps.resolveCrs(value)" in text
    assert 'resolveLoudly(proj4, "PROJ4")' in text


def test_a_planted_user_text_parse_in_an_allowlisted_file_is_flagged():
    """The allowlist is by line: a PROJ4 parse of a user column planted inside RST_Transform.scala (a file the
    allowlist names) is a new site; the file's own lines are allowed as they stand.
    """
    rel = "rasterx/expressions/RST_Transform.scala"
    real = (SCALA_ROOT / rel).read_text()
    assert [
        l for _, _, l in _scala_hits_in(rel, real) if not _scala_allowed(rel, l)
    ] == []
    anchor = "        val rc = dstSR.ImportFromEPSG(srid)\n"
    assert real.count(anchor) == 1
    planted = real.replace(anchor, anchor + "        dstSR.ImportFromProj4(userCol)\n")
    new = [
        (n, l) for _, n, l in _scala_hits_in(rel, planted) if not _scala_allowed(rel, l)
    ]
    assert [l for _, l in new] == ["dstSR.ImportFromProj4(userCol)"], new


def test_a_planted_constructor_parse_is_flagged():
    """A constructor that parses a value is a parse site, by its bare name or its module: pyproj.CRS(value) and
    osr.SpatialReference(value). A reasoned constructor with no value (CRS() ) is not.
    """
    planted = (
        "import pyproj\nfrom osgeo import osr\n"
        "def f(user):\n"
        "    a = pyproj.CRS(user)\n"
        "    b = osr.SpatialReference(user)\n"
        "    c = osr.SpatialReference()\n"
    )
    found = [line for _, line in _python_sites_in(planted)]
    assert found == ["a = pyproj.CRS(user)", "b = osr.SpatialReference(user)"], found
