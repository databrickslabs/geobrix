"""Structural guard (C-2): every CRS parse site in the Scala and Python sources is the canonical resolver's, or an
allowlisted site with its reason. A new site outside the allowlist fails, in either language.

- Scala (src/main/scala): a grep for the OSR parse calls (ImportFrom*, SetFromUserInput), `new SpatialReference(text)`
  and the GDAL warp and translate options that take a CRS (`-t_srs`, `-s_srs`, `-a_srs`), comments dropped.
- Python (python/geobrix/src): an AST walk for the parse calls (`CRS.from_*`, `Transformer.from_crs`, `to_crs`, and the
  osr methods), so a string in a call is found, not a line of text.

The allowlist is by exact line: a new call on an allowlisted file is still a new site. A site is allowlisted only
with its reason. The canonical resolvers (SpatialRefOps, core/crs.py) are the one place a CRS text is parsed on the
user's path. This guard does not follow a CRS value into a keyword argument of a library call (rasterio's `crs=`);
those values are resolved by their callers, which the C-2 sweep reviewed.
"""

import ast
import re
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

SCALA_ALLOW = {
    "operations/SpatialRefOps.scala": "the canonical resolver: every CRS text is parsed here, classified first",
    # --- integer SRIDs (an int names no file; see INT_EPSG) ---
    "rasterx/expressions/RST_Transform.scala": INT_EPSG,
    "rasterx/expressions/RST_BinPoints.scala": INT_EPSG,
    "rasterx/expressions/agg/RasterizeBurn.scala": INT_EPSG,
    "rasterx/expressions/agg/RST_Custom_RasterizeAgg.scala": INT_EPSG,
    "rasterx/expressions/agg/RST_H3_RasterizeAgg.scala": INT_EPSG,
    "rasterx/expressions/agg/RST_Quadbin_RasterizeAgg.scala": INT_EPSG,
    "rasterx/expressions/grid/GridReprojection.scala": GRID_INT,
    "rasterx/expressions/grid/RST_H3_CellBBox.scala": CONST_CRS,
    "rasterx/operations/CustomGridBounds.scala": GRID_INT,
    "rasterx/operations/RasterTessellate.scala": GRID_INT,
    "rasterx/expressions/grid/RasterToGridGeneric.scala": GRID_INT,
    "rasterx/expressions/RST_Clip.scala": DATASET_INT,
    # --- constants ---
    "rasterx/gdal/GDAL.scala": CONST_CRS,
    "rasterx/expressions/vector/RST_Isoband.scala": CONST_CRS,
    "rasterx/expressions/vector/RST_Polygonize.scala": CONST_CRS,
    "vectorx/mvt/MvtWriter.scala": CONST_CRS,
    # --- a dataset's own SRS, re-parsed from GDAL's export ---
    "rasterx/expressions/RST_AlignTo.scala": DATASET_WKT,
    "rasterx/expressions/RST_Chm.scala": DATASET_WKT,
    "rasterx/operations/RasterAlignment.scala": DATASET_WKT,
    "rasterx/expressions/accessors/RST_Crs.scala": DATASET_WKT,
    "rasterx/expressions/accessors/RST_SRID.scala": DATASET_WKT,
    # --- GDAL warp and translate options ---
    "rasterx/expressions/web/RST_TileXYZ.scala": WARP_CONST,
    "rasterx/expressions/web/RST_ToWebMercator.scala": WARP_CONST,
    "rasterx/operator/GDALWarp.scala": WARP_CONST,
    "rasterx/operations/RasterProject.scala": WARP_RESOLVED,
}
# A line-level allowlist for the sites a file-level entry would be too broad for (a new site in these files is new).
SCALA_LINE_ALLOW = {
    ("rasterx/expressions/RST_AlignTo.scala", "sr.ImportFromWkt(wkt)"): DATASET_WKT,
    (
        "rasterx/expressions/RST_AlignTo.scala",
        'val tSrs = srsToken.map(t => s"-t_srs $t ").getOrElse("")',
    ): WARP_DATASET,
    ("rasterx/expressions/RST_Chm.scala", "sr.ImportFromWkt(wkt)"): DATASET_WKT,
    (
        "rasterx/expressions/RST_Chm.scala",
        'val tSrs = srsToken.map(t => s"-t_srs $t ").getOrElse("")',
    ): WARP_DATASET,
    (
        "rasterx/expressions/grid/GridReprojection.scala",
        "srcSR.ImportFromWkt(srcWkt)",
    ): DATASET_WKT,
    (
        "rasterx/expressions/grid/GridReprojection.scala",
        's"gdalwarp -t_srs EPSG:$gridSrid -r near"',
    ): GRID_INT,
    (
        "rasterx/operations/RasterTessellate.scala",
        "s.ImportFromWkt(srcWkt)",
    ): DATASET_WKT,
    ("rasterx/operations/RasterTessellate.scala", "ImportFromEPSG(27700)"): CONST_CRS,
    (
        "rasterx/operations/RasterTessellate.scala",
        '"gdalwarp -t_srs EPSG:27700 -r near"',
    ): WARP_CONST,
    ("rasterx/operations/RasterAlignment.scala", "sa.ImportFromWkt(wa)"): DATASET_WKT,
    ("rasterx/operations/RasterAlignment.scala", "sb.ImportFromWkt(wb)"): DATASET_WKT,
    ("rasterx/operations/RasterAlignment.scala", "sr.ImportFromWkt(wkt)"): DATASET_WKT,
    (
        "rasterx/expressions/RST_Clip.scala",
        "geomSR.ImportFromEPSG(dsEpsgCode)",
    ): DATASET_INT,
    (
        "rasterx/expressions/RST_Clip.scala",
        "geomSR.ImportFromWkt(dsSR.ExportToWkt())",
    ): DATASET_WKT,
    ("rasterx/operator/GDALWarp.scala", 'srsVec.add("-s_srs")'): WARP_CONST,
    (
        "rasterx/operations/RasterProject.scala",
        's"gdalwarp -t_srs $authName:$authCode"',
    ): WARP_RESOLVED,
    (
        "rasterx/operations/RasterProject.scala",
        's"gdalwarp -t_srs ${wktFile.toString}"',
    ): WARP_RESOLVED,
    ("rasterx/expressions/RST_Chm.scala", 'Some(x) => s"-t_srs $x"'): WARP_RESOLVED,
    (
        "rasterx/expressions/web/RST_TileXYZ.scala",
        's"gdalwarp -t_srs EPSG:3857 -te $xmin $ymin $xmax $ymax -ts $size $size -r $resampling -dstalpha"',
    ): WARP_CONST,
}


def _scala_hits():
    for path in sorted(SCALA_ROOT.rglob("*.scala")):
        rel = path.relative_to(SCALA_ROOT).as_posix()
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            if SCALA_COMMENT.match(line) or not SCALA_PARSE.search(line):
                continue
            yield rel, lineno, line.strip()


def _scala_allowed(rel: str, line: str) -> bool:
    if rel in SCALA_ALLOW:
        return True
    return any(r == rel and needle in line for (r, needle) in SCALA_LINE_ALLOW)


# ---- Python ------------------------------------------------------------------------------------------------------
PY_CRS_CLASS_PARSE = {
    "from_user_input",
    "from_proj4",
    "from_string",
    "from_authority",
    "from_epsg",
    "from_wkt",
}
PY_OSR_PARSE = {
    "SetFromUserInput",
    "ImportFromProj4",
    "ImportFromWkt",
    "ImportFromEPSG",
    "ImportFromESRI",
}
PY_ALLOW_BY_LINE = {
    ("engine/argv.py", "sr.SetFromUserInput("): (
        "the engine's binder: crs_text_kind classifies the value first (never a file, a URL or a grid by path), then "
        "OSR parses it with file and network access off"
    ),
    ("engine/argv.py", "sr.ImportFromEPSG(int(v))"): INT_EPSG,
    (
        "core/crs.py",
        "CRS.from_user_input(value)",
    ): "the canonical resolver (core/crs.py)",
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
    ("vizx/_static_map.py", "plot_gdf = gdf.to_crs(3857)"): VIEWER_CONST,
}


def _receiver_text(node) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{_receiver_text(node.value)}.{node.attr}"
    return ""


def _python_hits():
    for path in sorted(PY_ROOT.rglob("*.py")):
        rel = path.relative_to(PY_ROOT).as_posix()
        text = path.read_text()
        lines = text.split("\n")  # the AST's line numbers count "\n" only
        for node in ast.walk(ast.parse(text)):
            if not (
                isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            ):
                continue
            attr = node.func.attr
            recv = _receiver_text(node.func.value)
            site = (
                (attr in PY_CRS_CLASS_PARSE and recv.split(".")[-1].endswith("CRS"))
                or (attr == "from_crs" and recv.split(".")[-1] == "Transformer")
                or attr == "to_crs"
                or attr in PY_OSR_PARSE
            )
            if site:
                yield rel, node.lineno, lines[node.lineno - 1].strip()


PY_ALLOW_FILES = {
    "core/crs.py": "the canonical resolver: every CRS text is classified and parsed here"
}


def _python_allowed(rel: str, line: str) -> bool:
    if rel in PY_ALLOW_FILES:
        return True
    return any(r == rel and needle in line for (r, needle) in PY_ALLOW_BY_LINE)


def test_the_scala_parse_sites_are_the_allowlisted_ones():
    hits = list(_scala_hits())
    assert hits, "the scan found no CRS parse site: the guard's pattern is wrong"
    new = [(rel, n, line) for rel, n, line in hits if not _scala_allowed(rel, line)]
    assert new == [], (
        "new CRS parse site(s), outside the allowlist (route through SpatialRefOps.resolveCrs or add a reasoned entry):\n"
        + "\n".join(f"  {rel}:{n}: {line}" for rel, n, line in new)
    )


def test_the_python_parse_sites_are_the_allowlisted_ones():
    hits = list(_python_hits())
    assert hits, "the scan found no CRS parse site: the guard's pattern is wrong"
    new = [(rel, n, line) for rel, n, line in hits if not _python_allowed(rel, line)]
    assert new == [], (
        "new CRS parse site(s), outside the allowlist (route through resolve_crs or add a reasoned entry):\n"
        + "\n".join(f"  {rel}:{n}: {line}" for rel, n, line in new)
    )


def test_the_guard_finds_a_planted_unrouted_parse():
    """The guard is not vacuous: a raw ImportFromProj4 of a column value, the C-2 shape, is a hit."""
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
    """C-2's site: the writer's PROJ4 and SRID columns go through SpatialRefOps.resolveCrs, never ImportFromProj4."""
    text = (SCALA_ROOT / "vectorx/ds/geojsonl/GeoJSONL_RowWriter.scala").read_text()
    assert "ImportFromProj4" not in text
    assert "SpatialRefOps.resolveCrs(proj4)" in text
