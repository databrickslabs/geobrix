#!/usr/bin/env python3
"""
Parser for Scala expression builders to extract parameter names and arities.

Reads Scala source files and extracts:
  - Case class field names (strip 'Expr' suffix, convert to snake_case)
  - Builder arity patterns (e.g. 'case 5 =>' vs 'case 6 =>')
  - Optional parameters (detected when builder injects Literal(...) defaults)

Output: dict keyed by function name with values like:
  {
    "usage_args": "tile, band_idx, [resampling]",
    "field_count": 3,
    "optional_from": 3
  }

Validation: Raises if a derived usage_args doesn't match real builder arity or contains unparseable syntax.
"""

import re
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Diagnostics collected during a scan and printed by main(). Kept module-level so the
# generator can surface them rather than swallowing an ambiguous parse.
MULTI_COMPANION_NOTES: List[str] = []


def camel_to_snake(name: str) -> str:
    """Convert camelCase to snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def extract_case_class_fields(scala_content: str, class_name: str) -> Optional[List[str]]:
    """
    Extract field names from a case class definition, filtering out internal aggregation state.

    Example:
      case class ST_Triangulate(
          pointsArray: Expression,
          breaklinesArray: Expression,
          mergeTolerance: Expression,
          ...
          modeExpr: Expression
      ) extends ...

    Returns: ["pointsArray", "breaklinesArray", "mergeTolerance", ..., "modeExpr"]
    Filters out: fields with type Int/Long = (default value) which are aggregation internals.
    """
    # Match the case class declaration and fields (until we hit ) extends or {)
    pattern = rf'case\s+(?:final\s+)?class\s+{re.escape(class_name)}\s*\(\s*(.*?)\s*\)\s*(?:extends|:|$)'
    match = re.search(pattern, scala_content, re.DOTALL | re.MULTILINE)
    if not match:
        return None

    fields_str = match.group(1)
    # Split by comma, but be careful about nested parens
    fields = []
    current_field = ""
    paren_depth = 0
    for char in fields_str:
        if char == '(':
            paren_depth += 1
            current_field += char
        elif char == ')':
            paren_depth -= 1
            current_field += char
        elif char == ',' and paren_depth == 0:
            if current_field.strip():
                # Extract field declaration: "name: Type" or "name: Type = default"
                field_decl = current_field.strip()
                # Skip if it's an Int/Long with a default value (aggregation buffer state)
                if not re.search(r':\s*(?:Int|Long)\s*=', field_decl):
                    # Extract just the name (before the colon)
                    field_name = field_decl.split(':')[0].strip()
                    if field_name:
                        fields.append(field_name)
            current_field = ""
        else:
            current_field += char

    if current_field.strip():
        field_decl = current_field.strip()
        if not re.search(r':\s*(?:Int|Long)\s*=', field_decl):
            field_name = field_decl.split(':')[0].strip()
            if field_name:
                fields.append(field_name)

    return fields if fields else None


def strip_expr_suffix(field_name: str) -> str:
    """Strip 'Expr' suffix and convert to snake_case."""
    if field_name.endswith('Expr'):
        field_name = field_name[:-4]  # Remove 'Expr'
    return camel_to_snake(field_name)


def should_exclude_field(field_name: str) -> bool:
    """Check if a field should be excluded from user-facing parameter list.

    Excludes:
    - exprConf / exprConfExpr: internal Spark config state
    """
    return field_name.lower() in ('exprconf', 'exprconfexpr')


def extract_builder_arities(scala_content: str, function_name: str) -> Optional[Tuple[int, Optional[int]]]:
    """
    Extract builder arity branches from a WithExpressionInfo companion object.

    Example:
      override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
          case 5 => ST_Triangulate(c(0), c(1), c(2), c(3), c(4), Literal("constrained"))
          case 6 => ST_Triangulate(c(0), c(1), c(2), c(3), c(4), c(5))
          case n => throw ...
      }

    Returns: (min_arity, max_arity) or (exact_arity, None) if only one branch.
             (5, 6) means 5 or 6 args, with 6th being optional.

    Brace style must NOT matter. Both of these are in use and mean the same thing::

        = (c: Seq[Expression]) => c.length match {      // ST_Triangulate
        = (c: Seq[Expression]) => {                     // ST_AsMvtPyramid
              c.length match {

    An earlier version anchored the regex directly on ``c.length`` after the arrow, silently
    returned None for the second
    form, and so dropped the `[extent]` bracket from gbx_st_asmvt_pyramid — publishing an
    optional argument as required, the exact defect this parser exists to prevent. Locate
    `builder()` and scan its body instead of matching one specific layout.
    """
    # Locate builder() and take everything to the end of the enclosing companion. Scanning a
    # generous window is safe: we only look for `case <int> =>` and `c(<int>)` tokens, and a
    # companion holds at most one builder.
    start = re.search(r'def\s+builder\s*\(\s*\)\s*:\s*FunctionBuilder\s*=', scala_content)
    if not start:
        return None
    builder_body = scala_content[start.end():]

    # Pattern 1: explicit arity branches, `case 5 =>`.
    arities = sorted({int(a) for a in re.findall(r'case\s+(\d+)\s*=>', builder_body)})

    # Pattern 2: `case Seq(a, b) =>` — arity is the number of top-level binders per branch.
    if not arities:
        seq_arities = set()
        for grp in re.findall(r'case\s+Seq\(([^)]*)\)\s*=>', builder_body):
            grp = grp.strip()
            seq_arities.add(len([p for p in grp.split(',') if p.strip()]) if grp else 0)
        arities = sorted(seq_arities)

    # Pattern 3: single fixed form, `=> new X(c(0), c(1))` / `=> X(c.head)`. Arity is the
    # highest c(i) index + 1, or 1 for the c.head/c(0)-only shape.
    if not arities:
        idxs = [int(i) for i in re.findall(r'c\((\d+)\)', builder_body)]
        if idxs:
            arities = [max(idxs) + 1]
        elif re.search(r'c\.head', builder_body):
            arities = [1]

    if not arities:
        return None
    if len(arities) == 1:
        return (arities[0], None)
    # Contiguous run (N, N+1, ... M) => args N+1..M are optional. Non-contiguous runs still
    # bracket everything past the shortest branch: over-marking as optional is honest here,
    # since the shortest form IS callable.
    return (arities[0], arities[-1])


def build_usage_args(
    fields: List[str],
    min_arity: int,
    max_arity: Optional[int]
) -> Tuple[str, Optional[int]]:
    """
    Build a usage_args string from field names and arity.

    Filters out internal fields (exprConf) and applies snake_case conversion.

    Returns: (usage_args, optional_from_index) where optional_from_index is 0-based position
             of the first optional arg, or None if no optional args.
    """
    # Filter out internal fields
    user_fields = [f for f in fields if not should_exclude_field(f)]

    if max_arity is None or max_arity == min_arity:
        # No optional args
        param_names = [strip_expr_suffix(f) for f in user_fields[:min_arity]]
        return (", ".join(param_names), None)

    # There are optional args starting at position min_arity
    mandatory = [strip_expr_suffix(f) for f in user_fields[:min_arity]]
    optional = [strip_expr_suffix(f) for f in user_fields[min_arity:max_arity]]

    # Format optional params with brackets
    all_parts = mandatory + [f"[{o}]" for o in optional]
    return (", ".join(all_parts), min_arity)


def parse_expression_file(filepath: str) -> Optional[Dict]:
    """
    Parse a single Scala expression file and extract metadata.

    Returns:
      {
        "function_name": "gbx_st_triangulate",
        "class_name": "ST_Triangulate",
        "usage_args": "points_geom, breaklines_geom, merge_tolerance, snap_tolerance, split_point_finder, [mode]",
        "optional_from": 6,
        "field_count": 6
      }
      or None if parsing fails.
    """
    try:
        with open(filepath, 'r') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

    # Extract class name from "case class ClassName"
    class_match = re.search(r'case\s+class\s+(\w+)', content)
    if not class_match:
        return None
    class_name = class_match.group(1)

    # Extract function name from companion object's override def name
    name_match = re.search(r'override\s+def\s+name\s*:\s*String\s*=\s*["\']([^"\']+)["\']', content)
    if not name_match:
        return None
    function_name = name_match.group(1)

    # A file may hold SEVERAL companions sharing ONE registered SQL name, each fronting a
    # different-arity case class (ST_TransformCrs / ST_TransformCrs3 both register
    # gbx_st_transformcrs with 2 and 3 fields). Taking the first `case class` then describes
    # only the narrowest overload: that is how `[source_crs]` was dropped from
    # gbx_st_transformcrs. When it happens, prefer the WIDEST case class so the optional
    # trailing args are visible, and report it so the ambiguity stays auditable.
    sql_names = set(re.findall(r'override\s+def\s+name\s*:\s*String\s*=\s*["\']([^"\']+)["\']', content))
    if len(sql_names) == 1:
        candidates = re.findall(r'case\s+class\s+(\w+)', content)
        if len(candidates) > 1:
            widest, widest_n = class_name, len(extract_case_class_fields(content, class_name) or [])
            for cand in candidates:
                n = len(extract_case_class_fields(content, cand) or [])
                if n > widest_n:
                    widest, widest_n = cand, n
            if widest != class_name:
                MULTI_COMPANION_NOTES.append(
                    f"{function_name}: {len(candidates)} case classes share one SQL name; "
                    f"described the widest ({widest}, {widest_n} fields) not the first ({class_name})"
                )
                class_name = widest

    # Extract case class fields
    fields = extract_case_class_fields(content, class_name)
    if not fields:
        return None

    # Extract builder arities
    arities = extract_builder_arities(content, function_name)
    if not arities:
        # If we can't parse builder, use field count as exact arity
        arities = (len(fields), None)

    min_arity, max_arity = arities
    usage_args, optional_from = build_usage_args(fields, min_arity, max_arity or min_arity)

    return {
        "function_name": function_name,
        "class_name": class_name,
        "usage_args": usage_args,
        "optional_from": optional_from,
        "field_count": len(fields),
        "arities": arities
    }


def scan_expressions_directory(expressions_dir: str) -> Dict[str, Dict]:
    """
    Recursively scan a directory of Scala expression files and extract metadata.

    Returns: dict keyed by function_name with parsed metadata.
    """
    result = {}
    expressions_path = Path(expressions_dir)

    if not expressions_path.is_dir():
        return result

    for scala_file in expressions_path.rglob("*.scala"):
        # Skip utility files, tests, etc.
        if any(skip in scala_file.name for skip in ["Util", "Config", "Test", "Mock"]):
            continue

        parsed = parse_expression_file(str(scala_file))
        if parsed:
            result[parsed["function_name"]] = {
                "usage_args": parsed["usage_args"],
                "optional_from": parsed["optional_from"],
                "class_name": parsed["class_name"],
                "file": str(scala_file),
            }

    return result


def validate_usage_args(usage_args: str) -> bool:
    """Validate that usage_args is well-formed (params comma-separated, optional bracketed)."""
    # Basic validation: no unmatched brackets, each bracketed item is a single word
    if usage_args.count('[') != usage_args.count(']'):
        return False

    # Check that [x] appears only around single identifiers
    invalid_brackets = re.findall(r'\[\w+,|\],\[\w+\w+\]', usage_args)
    if invalid_brackets:
        return False

    return True


def collect_scala_overrides(geobrix_root: str) -> Dict[str, str]:
    """Map SQL name -> hand-written `override def usageArgs` value, if any.

    These are the human-authored baseline. A derived value must never be WORSE than one of
    them, so they are compared against, not ignored.
    """
    out: Dict[str, str] = {}
    root = Path(geobrix_root) / "src/main/scala"
    for f in root.rglob("*.scala"):
        txt = f.read_text()
        for m in re.finditer(
            r'object\s+(\w+)\s+extends\s+[\w\s.]*?WithExpressionInfo\s*\{(.*?)\n\}', txt, re.S
        ):
            body = m.group(2)
            nm = re.search(r'def\s+name\s*:\s*String\s*=\s*"([^"]+)"', body)
            ua = re.search(
                r'override\s+def\s+usageArgs\s*:\s*String\s*=\s*((?:"[^"]*"\s*\+?\s*)+)', body, re.S
            )
            if nm and ua:
                val = "".join(re.findall(r'"([^"]*)"', ua.group(1))).strip()
                # Several companions may share a SQL name; keep the most informative text.
                if val and len(val) > len(out.get(nm.group(1), "")):
                    out[nm.group(1)] = val
    return out


def check_no_regression(derived: Dict[str, Dict], overrides: Dict[str, str]) -> List[str]:
    """Reject a derived usage_args that loses information vs a hand-written override.

    Two losses are hard failures because both publish a wrong SQL contract:
      * dropping an optional-arg bracket that the override marked (renders optional as required)
      * dropping a parameter the override listed (hides a callable form)
    Returns a list of human-readable problems; empty means clean.
    """
    problems: List[str] = []
    for name, ov in overrides.items():
        d = derived.get(name, {}).get("usage_args")
        if not d:
            continue
        if "[" in ov and "[" not in d:
            problems.append(
                f"{name}: override marks an optional arg but derived does not\n"
                f"    override: {ov}\n    derived : {d}"
            )
        ov_n = len([p for p in ov.split(",") if p.strip()])
        d_n = len([p for p in d.split(",") if p.strip()])
        if d_n < ov_n:
            problems.append(
                f"{name}: derived drops {ov_n - d_n} parameter(s) the override lists\n"
                f"    override: {ov}\n    derived : {d}"
            )
    return problems


def main(geobrix_root: str = None) -> Dict[str, Dict]:
    """
    Main entry point: scan all expression directories and return parsed metadata.

    Returns: dict keyed by function_name with {"usage_args": ..., "optional_from": ...}
    """
    if geobrix_root is None:
        geobrix_root = os.environ.get('GEOBRIX_ROOT', '/Users/mjohns/IdeaProjects/geobrix')

    result = {}

    # Scan RasterX, VectorX expression directories
    for package in ['rasterx', 'vectorx']:
        expressions_dir = os.path.join(geobrix_root, f'src/main/scala/com/databricks/labs/gbx/{package}/expressions')
        print(f"Scanning {expressions_dir}...")
        parsed = scan_expressions_directory(expressions_dir)
        result.update(parsed)
        print(f"  Found {len(parsed)} functions")

    # GridX is structured differently: bng/, quadbin/, h3/, grid/ subdirectories
    gridx_root = os.path.join(geobrix_root, 'src/main/scala/com/databricks/labs/gbx/gridx')
    for subdir in ['bng', 'quadbin', 'h3', 'grid', 'custom']:
        gridx_dir = os.path.join(gridx_root, subdir)
        if os.path.isdir(gridx_dir):
            print(f"Scanning {gridx_dir}...")
            parsed = scan_expressions_directory(gridx_dir)
            result.update(parsed)
            print(f"  Found {len(parsed)} functions")

    # Validate all parsed usage_args
    invalid = [
        f"{n}: malformed usage_args {m['usage_args']!r}"
        for n, m in result.items()
        if not validate_usage_args(m["usage_args"])
    ]

    # Never publish a signature worse than the hand-written override it replaces.
    regressions = check_no_regression(result, collect_scala_overrides(geobrix_root))

    if MULTI_COMPANION_NOTES:
        print("\nNOTE: files with multiple companions under one SQL name:")
        for n in MULTI_COMPANION_NOTES:
            print(f"  - {n}")

    # FAIL LOUDLY. A silent warning is how an optional arg got published as required; the
    # generator treats a non-zero exit as fatal rather than emitting degraded metadata.
    if invalid or regressions:
        print("\nERROR: refusing to emit metadata — derived signatures are not trustworthy:")
        for p in invalid + regressions:
            print(f"  - {p}")
        raise SystemExit(
            f"{len(invalid)} malformed + {len(regressions)} regression(s) vs Scala overrides. "
            "Fix the parser or the Scala source; do not hand-edit function-info.json."
        )

    return result


if __name__ == "__main__":
    import sys
    import json

    root = sys.argv[1] if len(sys.argv) > 1 else None
    parsed = main(root)

    # Print a summary
    print(f"\nTotal functions parsed: {len(parsed)}")

    # Output as JSON for consumption by the function-info generator
    print(json.dumps(parsed, indent=2))
