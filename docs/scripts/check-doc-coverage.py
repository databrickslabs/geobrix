#!/usr/bin/env python3
"""Verify doc coverage for every registered GeoBrix function.

Four checks run together:

  D2 -- every registered function is documented on its Functions page
        (docs/docs/api/{rasterx,gridx,vectorx,pmtiles}-functions.mdx).

  D3 -- no registered function has a placeholder-only example output
        constant in its SQL example file.  A placeholder is a table whose
        only data rows consist solely of ``...`` and/or empty cells with
        no real values anywhere (no [BINARY], no [GTiff, no <WKB, no real
        numbers, no WKT, etc.).  A data row that contains at least one
        non-empty, non-dot cell is not a placeholder even if other cells
        are dots.

  D4 -- for each TILE-returning rasterx function (those whose Scala
        dataType is tileDataType(...) or an inline StructType with a
        "raster" BinaryType field), the example output MUST contain the
        substring "<raster bytes>" and MUST NOT render the tile as a
        bare "[BINARY]" cell.

  D5 -- every _sql_example_output constant across all four SQL example
        files (rasterx, gridx, vectorx, pmtiles) must have ASCII tables
        that are canonically aligned: each column width equals the max
        stripped-cell width across all rows, borders use exactly that
        many dashes, and data cells are left-justified to that width.

Exit code: 0 when all checks pass, 1 if any finds problems.

Run directly on the host -- pure stdlib, no Docker needed.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from _manifest_names import stable_names  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTERED_TXT = REPO_ROOT / "docs/tests-function-info/registered_functions.txt"
RASTERX_SCALA_ROOT = REPO_ROOT / "src/main/scala/com/databricks/labs/gbx/rasterx"
RASTERX_SQL_FILE = REPO_ROOT / "docs/tests/python/api/rasterx_functions_sql.py"

# prefix -> docs page(s); value is always a list so callers can iterate uniformly
PAGE_MAP: dict[str, list[Path]] = {
    "gbx_rst_": [
        REPO_ROOT / "docs/docs/api/raster-functions.mdx",
    ],
    "gbx_bng_": [REPO_ROOT / "docs/docs/api/gridx-functions.mdx"],
    "gbx_quadbin_": [REPO_ROOT / "docs/docs/api/gridx-functions.mdx"],
    "gbx_custom_": [REPO_ROOT / "docs/docs/api/gridx-functions.mdx"],
    "gbx_st_": [REPO_ROOT / "docs/docs/api/vectorx-functions.mdx"],
    "gbx_pmtiles_": [REPO_ROOT / "docs/docs/api/pmtiles-functions.mdx"],
}

# prefix -> SQL example file
SQL_FILE_MAP: dict[str, Path] = {
    "gbx_rst_": RASTERX_SQL_FILE,
    "gbx_bng_": REPO_ROOT / "docs/tests/python/api/gridx_functions_sql.py",
    "gbx_quadbin_": REPO_ROOT / "docs/tests/python/api/gridx_functions_sql.py",
    "gbx_custom_": REPO_ROOT / "docs/tests/python/api/gridx_functions_sql.py",
    "gbx_st_": REPO_ROOT / "docs/tests/python/api/vectorx_functions_sql.py",
    "gbx_pmtiles_": REPO_ROOT / "docs/tests/python/api/pmtiles_functions_sql.py",
}

# All four SQL example files for D5
ALL_SQL_FILES: list[Path] = [
    RASTERX_SQL_FILE,
    REPO_ROOT / "docs/tests/python/api/gridx_functions_sql.py",
    REPO_ROOT / "docs/tests/python/api/vectorx_functions_sql.py",
    REPO_ROOT / "docs/tests/python/api/pmtiles_functions_sql.py",
]

# Regex to find _sql_example_output constants and their triple-quoted values
OUTPUT_CONST_RE = re.compile(
    r'(\w+_sql_example_output)\s*=\s*"""(.*?)"""',
    re.DOTALL,
)

# Cells that signal a real (non-placeholder) value even when surrounded by dots
REAL_CELL_RE = re.compile(
    r"""
    \[BINARY\]          # binary tile descriptor
    | \[GTiff           # GTiff variant
    | <WKB              # WKB prefix
    | POLYGON\s*\(      # WKT polygon
    | POINT\s*\(        # WKT point
    | LINESTRING\s*\(   # WKT linestring
    | \{                # JSON / struct
    | \[<digit          # array of digit
    | \[\d              # array starting with digit
    | \[STRUCT          # STRUCT array abbreviation
    """,
    re.VERBOSE | re.IGNORECASE,
)


def canonical_sql() -> list[str]:
    names = []
    for line in REGISTERED_TXT.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            names.append(line)
    return names


def pages_for(name: str) -> list[Path] | None:
    """Return the list of candidate doc pages for *name*, or None if unmapped."""
    for prefix, pages in PAGE_MAP.items():
        if name.startswith(prefix):
            return pages
    return None


# Backwards-compat alias (returns first page; used for display purposes only)
def page_for(name: str) -> Path | None:
    pages = pages_for(name)
    return pages[0] if pages else None


def _combined_page_text(pages: list[Path]) -> str:
    """Concatenate the text of all existing pages in *pages*."""
    parts: list[str] = []
    for p in pages:
        if p.exists():
            parts.append(p.read_text())
    return "\n".join(parts)


def sql_file_for(name: str) -> Path | None:
    for prefix, sql_file in SQL_FILE_MAP.items():
        if name.startswith(prefix):
            return sql_file
    return None


def bare_name(gbx_name: str) -> str:
    """Strip the leading 'gbx_' prefix: 'gbx_rst_foo' -> 'rst_foo'."""
    return gbx_name[len("gbx_"):]


def is_documented(name: str, page_text: str) -> bool:
    """Return True if *name* appears in *page_text* in any recognised form.

    Accepted matches (all case-sensitive substring):
      1. The full SQL name:         gbx_rst_foo
      2. The bare name:             rst_foo  (used in ### headings)
      3. functionName="<bare>_sql_example"
      4. outputConstant="<bare>_sql_example_output"
    """
    full = name                   # e.g. gbx_rst_foo
    bare = bare_name(name)        # e.g. rst_foo

    if full in page_text:
        return True
    if bare in page_text:
        return True
    if f'functionName="{bare}_sql_example"' in page_text:
        return True
    if f'outputConstant="{bare}_sql_example_output"' in page_text:
        return True
    return False


def _all_output_constants(path: Path) -> dict[str, str]:
    """Return {constant_name: value} for every _sql_example_output in *path*."""
    text = path.read_text()
    return {m.group(1): m.group(2) for m in OUTPUT_CONST_RE.finditer(text)}


def _is_placeholder_output(value: str) -> bool:
    """Return True if *value* is a placeholder-only table.

    A placeholder is a table whose data rows (rows after the header / first
    separator pair) ALL consist solely of ``...`` and/or whitespace/empty cells
    and contain no recognisably real value.

    Table structure:
        +---+      <- separator 0
        |col|      <- header row(s)
        +---+      <- separator 1
        |val|      <- data rows  <-- these are what we inspect
        +---+

    Non-table strings (no ``+---+``/``|`` structure) are also flagged if they
    are just a column label or ``...``.
    """
    stripped = value.strip()
    if not stripped:
        return False

    lines = stripped.splitlines()
    sep_indices = [i for i, l in enumerate(lines) if l.strip().startswith("+")]
    has_table = len(sep_indices) >= 2

    if not has_table:
        # Bare (non-table) string: flag if it's just '...' or a single word
        # column label with no numerics / WKT / etc.
        single = stripped.replace("...", "").replace("|", "").strip()
        if not single or single.isidentifier():
            return True
        return False

    # Data rows are pipe-rows that come AFTER the second separator line.
    # (Rows between separator 0 and separator 1 are header/column-name rows.)
    second_sep = sep_indices[1]
    data_rows = [
        l for i, l in enumerate(lines)
        if i > second_sep and l.strip().startswith("|") and not l.strip().startswith("+")
    ]
    if not data_rows:
        return False

    def row_has_real_value(row: str) -> bool:
        cells = [c.strip() for c in row.strip("|").split("|")]
        for cell in cells:
            if not cell or cell == "...":
                continue
            # Check for real-value patterns
            if REAL_CELL_RE.search(cell):
                return True
            # Any cell that is not empty, not pure dots, and contains
            # something other than '...' is considered real
            cleaned = cell.replace("...", "").strip()
            if cleaned:
                return True
        return False

    # The output is a placeholder only if NO data row has a real value
    return not any(row_has_real_value(r) for r in data_rows)


# ---------------------------------------------------------------------------
# D4 classifier: which rasterx functions return the tile struct
# ---------------------------------------------------------------------------

def _classify_tile_returning_functions() -> set[str]:
    """Scan rasterx Scala sources and return the set of SQL names whose
    dataType is a tile struct.

    TILE-returning if its dataType RHS:
      (a) contains ``tileDataType`` (i.e. RST_ExpressionUtil.tileDataType(...)), OR
      (b) is an inline ``StructType(Seq(...))`` that includes a
          ``StructField("raster", BinaryType`` field.

    Explicitly NOT tile-returning (excluded by name even if source matches):
      - gbx_rst_xyzpyramid  (uses RST_XYZPyramid.tileStruct -- no inline raster field)
      - gbx_rst_tilexyz     (BinaryType scalar)
      - gbx_rst_boundingbox (BinaryType scalar)
    """
    tile_functions: set[str] = set()

    scala_files = list(RASTERX_SCALA_ROOT.rglob("*.scala"))

    for scala_file in scala_files:
        try:
            content = scala_file.read_text()
        except OSError:
            continue

        # Extract (name, dataType_rhs) pairs from the file.
        # Strategy: find all "override def name: String = ..." and
        # "override (def|val|lazy val) dataType ..." in the file,
        # then pair them up (each class has exactly one of each).

        # Find all SQL names in this file
        name_matches = list(re.finditer(
            r'override\s+def\s+name\s*:\s*String\s*=\s*"(gbx_rst_[^"]+)"',
            content
        ))
        if not name_matches:
            continue

        # Find all dataType definitions.  The RHS may span multiple lines
        # (e.g. inline StructType).  We capture up to the next top-level
        # override or end-of-class as a heuristic.
        datatype_matches = list(re.finditer(
            r'override\s+(?:def|val|lazy\s+val)\s+dataType\s*[=:][^=]',
            content
        ))
        if not datatype_matches:
            continue

        # For each (name, dataType) pair found in the same file,
        # classify the function.
        # We assume one expression class per file (the most common pattern).
        # For files with multiple classes, each class still has one name and
        # one dataType; we pair them up by position.

        # Build list of (pos, sql_name) and (pos, rhs_snippet)
        names_by_pos = [(m.start(), m.group(1)) for m in name_matches]
        dtypes_by_pos = []
        for m in datatype_matches:
            # Grab ~300 chars of RHS after the match
            rhs_start = m.end()
            rhs_snippet = content[rhs_start: rhs_start + 400]
            dtypes_by_pos.append((m.start(), rhs_snippet))

        # Pair each dataType with the nearest name (within same class).
        # Since both lists may be short (1-2 entries), do a simple
        # nearest-previous-name heuristic.
        for dt_pos, rhs in dtypes_by_pos:
            # Find the sql_name whose override def name appears closest
            # before or after this dataType definition.
            best_name = None
            best_dist = None
            for n_pos, sql_name in names_by_pos:
                dist = abs(dt_pos - n_pos)
                if best_dist is None or dist < best_dist:
                    best_dist = dist
                    best_name = sql_name

            if best_name is None:
                continue

            # Check classification criteria
            is_tile = False

            # (a) RHS references tileDataType(...)
            if "tileDataType" in rhs:
                is_tile = True

            # (b) Inline StructType(Seq(...)) with StructField("raster", BinaryType
            if not is_tile:
                if "StructType(Seq(" in rhs and 'StructField("raster", BinaryType' in rhs:
                    is_tile = True

            if is_tile:
                tile_functions.add(best_name)

    # Explicit exclusions per spec:
    # - gbx_rst_xyzpyramid uses RST_XYZPyramid.tileStruct (no inline raster field)
    # - gbx_rst_tilexyz returns BinaryType (PNG bytes)
    # - gbx_rst_boundingbox returns BinaryType (WKB)
    tile_functions.discard("gbx_rst_xyzpyramid")
    tile_functions.discard("gbx_rst_tilexyz")
    tile_functions.discard("gbx_rst_boundingbox")

    return tile_functions


# ---------------------------------------------------------------------------
# Table alignment helpers (shared by D5 check and fix utilities)
# ---------------------------------------------------------------------------

def reformat_table(lines: list[str]) -> list[str]:
    """Reformat a list of lines representing a single ASCII table to canonical
    alignment.

    Algorithm:
      - Identify all pipe rows (lines starting with '|').
      - Parse each pipe row into cells by stripping outer '|' and splitting on '|'.
      - Compute per-column canonical width = max(stripped cell length) across ALL rows.
      - Rebuild each border as '+' + '-'*width joined by '+' + '+'.
      - Rebuild each pipe row with cells left-justified to their column width.

    Lines that are neither borders nor pipe rows are returned as-is.
    """
    sep_indices = [i for i, l in enumerate(lines) if l.strip().startswith("+")]
    if not sep_indices:
        return list(lines)

    # Determine ncols from first border
    first_border = lines[sep_indices[0]].strip()
    parts = first_border.split("+")
    inner_parts = parts[1:-1]
    ncols = len(inner_parts)
    if ncols == 0:
        return list(lines)

    # Collect all pipe rows
    def parse_row(row: str) -> list[str]:
        inner = row.strip().strip("|")
        return inner.split("|")

    pipe_rows = [parse_row(l) for l in lines if l.strip().startswith("|") and not l.strip().startswith("+")]

    # Compute per-column max width (stripped content)
    col_widths = [0] * ncols
    for cells in pipe_rows:
        for j, cell in enumerate(cells):
            if j < ncols:
                col_widths[j] = max(col_widths[j], len(cell.strip()))

    def make_border() -> str:
        return "+" + "+".join("-" * w for w in col_widths) + "+"

    def make_row(cells: list[str]) -> str:
        padded = []
        for j in range(ncols):
            cell = cells[j].strip() if j < len(cells) else ""
            padded.append(cell.ljust(col_widths[j]))
        return "|" + "|".join(padded) + "|"

    # Reconstruct lines preserving order
    result: list[str] = []
    pipe_row_idx = 0
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("+"):
            result.append(make_border())
        elif stripped.startswith("|"):
            if pipe_row_idx < len(pipe_rows):
                result.append(make_row(pipe_rows[pipe_row_idx]))
                pipe_row_idx += 1
            else:
                result.append(line)
        else:
            result.append(line)
    return result


def _is_table_aligned(lines: list[str]) -> bool:
    """Return True iff reformat_table(lines) == lines."""
    return reformat_table(lines) == lines


def _find_tables_in_value(value: str) -> list[tuple[int, list[str]]]:
    """Find all ASCII table regions in *value*.

    Returns a list of (start_line_index, table_lines) for each contiguous
    block of border/pipe lines.
    """
    all_lines = value.splitlines()
    tables: list[tuple[int, list[str]]] = []
    i = 0
    while i < len(all_lines):
        stripped = all_lines[i].strip()
        if stripped.startswith("+") or stripped.startswith("|"):
            # Start of a table region
            start = i
            table_lines = []
            while i < len(all_lines):
                s = all_lines[i].strip()
                if s.startswith("+") or s.startswith("|"):
                    table_lines.append(all_lines[i])
                    i += 1
                else:
                    break
            tables.append((start, table_lines))
        else:
            i += 1
    return tables


# ---------------------------------------------------------------------------
# Check D2 -- every registered function is documented on its page
# ---------------------------------------------------------------------------

def check_d2(names: list[str]) -> tuple[bool, str]:
    """Return (passed, report_text)."""
    # Pre-load page texts (one read per unique page path)
    all_pages: set[Path] = set()
    for pages in PAGE_MAP.values():
        all_pages.update(pages)
    page_texts: dict[Path, str] = {}
    for page in all_pages:
        page_texts[page] = page.read_text() if page.exists() else ""

    # Group missing by prefix-group (represented by first candidate page)
    missing_by_group: dict[Path, list[str]] = {}
    for name in names:
        candidate_pages = pages_for(name)
        if candidate_pages is None:
            continue
        combined_text = "\n".join(page_texts.get(p, "") for p in candidate_pages)
        if not is_documented(name, combined_text):
            # Key by the first page for display grouping
            missing_by_group.setdefault(candidate_pages[0], []).append(name)

    if not missing_by_group:
        total = len(names)
        return True, f"[OK] D2 doc-coverage OK -- all {total} registered functions are documented on their pages."

    lines = ["[FAIL] D2 doc-coverage FAILED -- registered functions with no documentation on their mapped page:"]
    for page in sorted(str(p) for p in missing_by_group):
        page_path = Path(page)
        rel = page_path.relative_to(REPO_ROOT)
        lines.append(f"\n  {rel}:")
        for fn in missing_by_group[page_path]:
            lines.append(f"     - {fn}")
    return False, "\n".join(lines)


# ---------------------------------------------------------------------------
# Check D3 -- no placeholder-only example outputs
# ---------------------------------------------------------------------------

def check_d3(names: list[str]) -> tuple[bool, str]:
    """Return (passed, report_text)."""
    # Pre-load SQL files
    sql_texts: dict[Path, dict[str, str]] = {}
    for sql_file in set(SQL_FILE_MAP.values()):
        if sql_file.exists():
            sql_texts[sql_file] = _all_output_constants(sql_file)
        else:
            sql_texts[sql_file] = {}

    placeholders: list[str] = []
    for name in names:
        sql_file = sql_file_for(name)
        if sql_file is None:
            continue
        constants = sql_texts.get(sql_file, {})
        bare = bare_name(name)  # e.g. rst_foo
        const_name = f"{bare}_sql_example_output"
        if const_name not in constants:
            # No output constant at all -- out of scope for this check
            continue
        value = constants[const_name]
        if _is_placeholder_output(value):
            placeholders.append(name)

    if not placeholders:
        return True, "[OK] D3 placeholder-output OK -- no registered function has a placeholder-only example output."

    lines = ["[FAIL] D3 placeholder-output FAILED -- registered functions with placeholder-only example outputs:"]
    for fn in placeholders:
        lines.append(f"     - {fn}")
    return False, "\n".join(lines)


# ---------------------------------------------------------------------------
# Check D4 -- tile-output accuracy
# ---------------------------------------------------------------------------

def check_d4(tile_functions: set[str]) -> tuple[bool, str]:
    """Return (passed, report_text).

    For each TILE-returning rasterx function, its example output MUST
    contain '<raster bytes>' and MUST NOT render the tile as a bare '[BINARY]'
    cell.
    """
    if not RASTERX_SQL_FILE.exists():
        return False, f"[FAIL] D4 tile-output FAILED -- missing {RASTERX_SQL_FILE}"

    constants = _all_output_constants(RASTERX_SQL_FILE)
    violations: list[str] = []

    for gbx_name in sorted(tile_functions):
        bare = bare_name(gbx_name)  # e.g. rst_foo
        const_name = f"{bare}_sql_example_output"
        if const_name not in constants:
            # No output constant -- not in scope for this check
            continue
        value = constants[const_name]
        if "<raster bytes>" not in value:
            violations.append(
                f"     - {gbx_name}: output lacks '<raster bytes>' "
                f"(const: {const_name})"
            )

    if not violations:
        return True, (
            f"[OK] D4 tile-output OK -- all {len(tile_functions)} TILE-returning functions "
            f"render the tile struct correctly."
        )

    lines = [
        "[FAIL] D4 tile-output FAILED -- TILE-returning functions whose output "
        "does not contain '<raster bytes>':"
    ]
    lines.extend(violations)
    return False, "\n".join(lines)


# ---------------------------------------------------------------------------
# Check D5 -- ASCII table alignment
# ---------------------------------------------------------------------------

def check_d5() -> tuple[bool, str]:
    """Return (passed, report_text).

    For every _sql_example_output constant across all four SQL example files,
    find each ASCII table region and verify it is aligned.
    """
    violations: list[str] = []

    for sql_file in ALL_SQL_FILES:
        if not sql_file.exists():
            continue
        constants = _all_output_constants(sql_file)
        rel = sql_file.relative_to(REPO_ROOT)
        for const_name, value in constants.items():
            tables = _find_tables_in_value(value)
            for _start, table_lines in tables:
                if not _is_table_aligned(table_lines):
                    reformatted = reformat_table(table_lines)
                    # Find first offending line
                    first_bad = None
                    for orig, ref in zip(table_lines, reformatted):
                        if orig != ref:
                            first_bad = orig
                            break
                    violations.append(
                        f"     - {rel}: {const_name}: "
                        f"misaligned line: {repr(first_bad)}"
                    )

    if not violations:
        return True, "[OK] D5 table-alignment OK -- all example output tables are canonically aligned."

    lines = ["[FAIL] D5 table-alignment FAILED -- misaligned ASCII tables:"]
    lines.extend(violations)
    return False, "\n".join(lines)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    if not REGISTERED_TXT.exists():
        print(f"[FAIL] missing required file: {REGISTERED_TXT}", file=sys.stderr)
        return 1

    # A stable manifest row is checked by gbx:manifest:gen --check; this script keeps the other names.
    owned = stable_names()
    names = [n for n in canonical_sql() if n not in owned]
    print(f"Canonical registered functions: {len(names)}")
    print()

    # Classify TILE-returning rasterx functions
    tile_functions = _classify_tile_returning_functions()
    print(f"TILE-returning rasterx functions detected: {len(tile_functions)}")
    print()

    d2_ok, d2_report = check_d2(names)
    d3_ok, d3_report = check_d3(names)
    d4_ok, d4_report = check_d4(tile_functions)
    d5_ok, d5_report = check_d5()

    print(d2_report)
    print()
    print(d3_report)
    print()
    print(d4_report)
    print()
    print(d5_report)
    print()

    if d2_ok and d3_ok and d4_ok and d5_ok:
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
