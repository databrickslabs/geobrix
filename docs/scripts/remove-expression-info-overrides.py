#!/usr/bin/env python3
"""Remove orphan DESCRIBE FUNCTION overrides (description, usageArgs, examples, extendedUsageArgs, extendedDescription) from expression objects. Function info now comes from function-info.json only."""

import re
import sys
from pathlib import Path

# Script lives in docs/scripts/; repo root is two levels up.
SRC = Path(__file__).resolve().parent.parent.parent / "src" / "main" / "scala"

OVERRIDE_PATTERN = re.compile(
    r"^\s+override def (description|usageArgs|examples|extendedUsageArgs|extendedDescription)\b"
)
COMMENT_PATTERN = re.compile(
    r"^\s+/\* FOR `DESCRIBE FUNCTION EXTENDED|^\s+//TODO: ADD EXPRESSION INFO"
)


def end_of_override(lines: list, i: int) -> int:
    """Return the last line index (inclusive) of the override starting at line i."""
    if i >= len(lines):
        return i
    line = lines[i]
    stripped = line.strip()
    # override def X: String = "..."  (single line)
    if '= "' in line and (line.rstrip().endswith('"') or line.rstrip().endswith('"""')):
        return i
    # override def X: String = s"${...}" (single line)
    if '= s"' in line and line.rstrip().endswith('"'):
        return i
    # override def X: String =  (continuation on next line)
    if ": String =" in line and "{" not in line:
        j = i + 1
        while j < len(lines):
            next_strip = lines[j].strip()
            if not next_strip:
                j += 1
                continue
            if next_strip.startswith('"') or next_strip.startswith('s"""') or next_strip.startswith('"""'):
                # Find closing quote for multi-line string
                if '"""' in lines[j]:
                    close_count = lines[j].count('"""')
                    if close_count >= 2:
                        return j
                    k = j + 1
                    while k < len(lines):
                        if '"""' in lines[k]:
                            return k
                        k += 1
                    return k - 1 if k else j
                if next_strip.endswith('"'):
                    return j
                j += 1
                while j < len(lines):
                    if '"' in lines[j]:
                        return j
                    j += 1
                return j - 1
            if next_strip.startswith("override def"):
                return i
            j += 1
        return j - 1 if j > i + 1 else i
    # override def examples: String = {
    if "= {" in line:
        depth = 1
        j = i + 1
        while j < len(lines):
            l = lines[j]
            depth += l.count("{") - l.count("}")
            if depth <= 0:
                return j
            j += 1
        return j - 1 if j else i
    return i


def remove_block(content: str) -> str:
    lines = content.split("\n")
    start_i = None
    for i, line in enumerate(lines):
        if COMMENT_PATTERN.search(line):
            start_i = i
            break
    if start_i is None:
        # Find first override of the five
        for i, line in enumerate(lines):
            if OVERRIDE_PATTERN.search(line):
                start_i = i
                break
    if start_i is None:
        return content

    # If we started on a comment, skip to first override (next non-blank that is an override)
    if start_i is not None and COMMENT_PATTERN.search(lines[start_i]):
        i = start_i + 1
        while i < len(lines):
            if OVERRIDE_PATTERN.search(lines[i]):
                start_i = i
                break
            if lines[i].strip() == "}" and i > start_i:
                return content
            i += 1
        if not OVERRIDE_PATTERN.search(lines[start_i]):
            # Comment only, remove just the comment line
            new_lines = lines[:start_i] + lines[start_i + 1:]
            return "\n".join(new_lines)

    # Find end: last of the five overrides (with full body)
    end_i = start_i
    i = start_i
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if stripped == "}" and i > start_i:
            break
        if OVERRIDE_PATTERN.search(line):
            end_i = end_of_override(lines, i)
            i = end_i
        i += 1

    # Remove optional blank line before block
    if start_i > 0 and lines[start_i - 1].strip() == "":
        start_i -= 1
    new_lines = lines[:start_i] + lines[end_i + 1:]
    return "\n".join(new_lines)


def main():
    files = [
        "com/databricks/labs/gbx/rasterx/expressions/agg/RST_CombineAvgAgg.scala",
        "com/databricks/labs/gbx/rasterx/expressions/agg/RST_DerivedBandAgg.scala",
        "com/databricks/labs/gbx/rasterx/expressions/agg/RST_MergeAgg.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_SRID.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_GeoReference.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_NDVI.scala",
        "com/databricks/labs/gbx/rasterx/expressions/grid/RST_H3_RasterToGridMin.scala",
        "com/databricks/labs/gbx/rasterx/expressions/grid/RST_H3_RasterToGridMedian.scala",
        "com/databricks/labs/gbx/rasterx/expressions/grid/RST_H3_RasterToGridMax.scala",
        "com/databricks/labs/gbx/rasterx/expressions/grid/RST_H3_RasterToGridCount.scala",
        "com/databricks/labs/gbx/rasterx/expressions/grid/RST_H3_RasterToGridAvg.scala",
        "com/databricks/labs/gbx/rasterx/expressions/generators/RST_ToOverlappingTiles.scala",
        "com/databricks/labs/gbx/rasterx/expressions/generators/RST_SeparateBands.scala",
        "com/databricks/labs/gbx/rasterx/expressions/generators/RST_ReTile.scala",
        "com/databricks/labs/gbx/rasterx/expressions/generators/RST_MakeTiles.scala",
        "com/databricks/labs/gbx/rasterx/expressions/generators/RST_H3_Tessellate.scala",
        "com/databricks/labs/gbx/rasterx/expressions/constructor/RST_FromFile.scala",
        "com/databricks/labs/gbx/rasterx/expressions/constructor/RST_FromContent.scala",
        "com/databricks/labs/gbx/rasterx/expressions/constructor/RST_FromBands.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Width.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_UpperLeftY.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_UpperLeftX.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Type.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Summary.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Subdatasets.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_SkewY.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_SkewX.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_ScaleY.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_ScaleX.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Rotation.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_PixelWidth.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_PixelHeight.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_PixelCount.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_NumBands.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Min.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_MetaData.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_MemSize.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Median.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Max.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Height.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_GetSubdataset.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_GetNoData.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Format.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_BoundingBox.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_BandMetaData.scala",
        "com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Avg.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_WorldToRasterCoordY.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_WorldToRasterCoordX.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_WorldToRasterCoord.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_UpdateType.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_Transform.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_RasterToWorldCoordY.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_RasterToWorldCoordX.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_RasterToWorldCoord.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_Merge.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_MapAlgebra.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_InitNoData.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_Filter.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_DerivedBand.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_DTMFromGeoms.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_Convolve.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_CombineAvg.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_Clip.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_AsFormat.scala",
        "com/databricks/labs/gbx/rasterx/expressions/RST_TryOpen.scala",
        "com/databricks/labs/gbx/gridx/bng/generators/BNG_GeometryKRingExplode.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_GeometryKRing.scala",
        "com/databricks/labs/gbx/gridx/bng/agg/BNG_CellIntersectionAgg.scala",
        "com/databricks/labs/gbx/gridx/bng/generators/BNG_GeometryKLoopExplode.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_GeometryKLoop.scala",
        "com/databricks/labs/gbx/gridx/bng/agg/BNG_CellUnionAgg.scala",
        "com/databricks/labs/gbx/gridx/bng/generators/BNG_KLoopExplode.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_EastNorthAsBNG.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_KRing.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_AsWKT.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_CellArea.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_CellIntersection.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_Tessellate.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_Polyfill.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_Centroid.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_AsWKB.scala",
        "com/databricks/labs/gbx/gridx/bng/generators/BNG_TessellateExplode.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_EuclideanDistance.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_KLoop.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_Distance.scala",
        "com/databricks/labs/gbx/gridx/bng/BNG_CellUnion.scala",
        "com/databricks/labs/gbx/gridx/bng/generators/BNG_KRingExplode.scala",
        "com/databricks/labs/gbx/vectorx/jts/legacy/expressions/ST_LegacyAsWKB.scala",
    ]
    # Also discover any other .scala under expressions that might have overrides
    for rel in list(files):
        path = SRC / rel
        if not path.exists():
            print(f"Skip (missing): {rel}", file=sys.stderr)
            continue
        text = path.read_text()
        new_text = remove_block(text)
        if new_text != text:
            path.write_text(new_text)
            print(f"Updated: {rel}")
        else:
            pass  # No block found

    # Second pass: any file under rasterx/expressions, gridx/bng, vectorx that still has overrides
    for base in ["com/databricks/labs/gbx/rasterx/expressions", "com/databricks/labs/gbx/gridx/bng", "com/databricks/labs/gbx/vectorx"]:
        base_path = SRC / base
        if not base_path.exists():
            continue
        for path in base_path.rglob("*.scala"):
            rel = path.relative_to(SRC).as_posix()
            if rel in files:
                continue
            text = path.read_text()
            if not OVERRIDE_PATTERN.search(text):
                continue
            new_text = remove_block(text)
            if new_text != text:
                path.write_text(new_text)
                print(f"Updated: {rel}")

if __name__ == "__main__":
    main()
