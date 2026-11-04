"""The reader names GeoBrix registers, and a scan for names that are not.

Two sources define what ``spark.read.format("<name>")`` can resolve to:

* the manifest's ``datasources`` rows (``databricks.labs.gbx.manifest``): the lightweight (wheel) readers are the
  ``impl: python`` rows, the JAR's readers (none once the Scala readers are retired) the ``impl: scala`` rows.

A snippet that names anything else (``gpkg`` for ``gpkg_gbx``, say)
fails at run time with ``DATA_SOURCE_NOT_FOUND``. Spark and Databricks built-in
formats are allowed.

Names are found where a reader is chosen: ``.format("x")``, ``format="x"`` of
``load`` / ``save`` / ``saveAsTable``, the ``format`` argument of ``read_paths(...)``
and ``declare_ingest(...)`` (positional or keyword), ``read_files(format => 'x')``, SQL
``USING x`` (``CREATE TABLE`` / ``VIEW`` in any case), ``FROM x.`path```, the Lakeflow
connector's ``format`` table option (``"format": "x"``, ``format: x``) and the first
column of a ``format`` table. ``read_paths``, ``declare_ingest`` and the connector take
the lightweight readers only, with the ``_gbx`` suffix optional and case ignored, so
``gpkg`` is right there and wrong in ``.format(...)``. Not checked: that a heavyweight
name is not used where only a lightweight one works (and the reverse) -- the tier of
the surrounding snippet is not known to the scan.

Used by ``readers/test_registered_reader_names.py`` (the scan) and by the tests that
assert a reader name the docs display.
"""

import json
import os
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

# Formats Spark / Databricks provide; a snippet may use them next to GeoBrix readers.
# (``read_files(format => 'file')``, the FILE-reference listing the ``file`` docs use, is
# a ``read_files`` format only: see READ_FILES_FORMATS.)
BUILTIN_FORMATS = frozenset(
    name.lower()
    for name in (
        "avro",
        "binaryFile",
        "cloudFiles",
        "csv",
        "delta",
        "image",
        "jdbc",
        "json",
        "lakeflow_connect",
        "memory",
        "noop",
        "orc",
        "parquet",
        "text",
        "xml",
    )
)

READ_FILES_FORMATS = BUILTIN_FORMATS | {"file"}

_READER_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def heavy_reader_names():
    """Names of the JAR's registered data sources (the manifest's ``impl: scala`` rows)."""
    from databricks.labs.gbx import manifest

    return {d.name for d in manifest.load().datasources if d.impl == "scala"}


def light_reader_names():
    """Names of the lightweight (``*_gbx``) data sources ``register()`` installs."""
    from databricks.labs.gbx.manifest import datasource_classes

    return {source.name() for source in datasource_classes()}


def registered_reader_names():
    """Every GeoBrix reader name (heavyweight and lightweight), lower-cased as Spark matches them."""
    return {name.lower() for name in heavy_reader_names() | light_reader_names()}


# --- the scan ---------------------------------------------------------------

# ``.format("x")`` / ``.format('x')`` on a reader or writer (also across a line break).
_FORMAT_CALL = re.compile(r"""\.format\(\s*["']([^"'\n]*)["']""")
# ``format="x"`` as a keyword of ``.load(...)`` / ``.save(...)`` / ``.saveAsTable(...)``
# (found by call, since the other arguments may hold parentheses or span lines).
_FORMAT_KEYWORD_CALLS = ("load", "save", "saveAsTable")
# ``read_files(path, format => 'x')`` -- Databricks SQL named argument.
_READ_FILES_FORMAT = re.compile(r"""\bformat\s*=>\s*["']([^"'\n]*)["']""")
# ``SELECT * FROM x.`path` `` -- a path-style table reference names its reader.
_SQL_PATH_READ = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z_]\w*)\.`", re.IGNORECASE)
# ``CREATE TABLE t USING x`` (upper-case keyword only: prose says "using").
_SQL_USING = re.compile(r"\bUSING\s+([A-Za-z_]\w*)")
# ... and in any case, but only inside a CREATE TABLE / VIEW statement, before its
# ``AS`` / ``SELECT`` (so the "using" of the prose after a ``CREATE ... AS`` is not one).
_SQL_CREATE_USING = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?(?:EXTERNAL\s+)?(?:TABLE|VIEW)\b"
    r"(?:(?!\bAS\b|\bSELECT\b|;|\bUSING\b).){0,400}?\bUSING\s+([A-Za-z_]\w*)",
    re.IGNORECASE | re.DOTALL,
)
# The connector's ``format`` table option: ``"format": "x"`` in a pipeline spec, or
# ``format: x`` in prose / YAML.
_CONNECTOR_FORMAT = re.compile(r"""["']format["']\s*:\s*["']([^"'\n]*)["']""")
_CONNECTOR_FORMAT_PROSE = re.compile(
    r"`format:[ \t]*([A-Za-z]\w*)`|^[ \t]*format:[ \t]*([A-Za-z]\w*)[ \t]*$", re.MULTILINE
)

_PATTERNS = (
    ("format()", _FORMAT_CALL),
    ("read_files(format=>)", _READ_FILES_FORMAT),
    ("SQL path read", _SQL_PATH_READ),
    ("SQL USING", _SQL_USING),
    ("SQL USING", _SQL_CREATE_USING),
    ("connector format", _CONNECTOR_FORMAT),
    ("connector format", _CONNECTOR_FORMAT_PROSE),
)

# Calls whose ``format`` argument is a light reader name: (name, position of the
# positional ``format``, or None when only the keyword counts).
_FORMAT_ARGUMENT_CALLS = (
    ("read_paths", 1, "read_paths()"),
    ("declare_ingest", 2, "declare_ingest()"),
    *((name, None, f"{name}(format=)") for name in _FORMAT_KEYWORD_CALLS),
)

# These take a lightweight reader's name, with ``_gbx`` optional and any case.
LIGHT_NAME_KINDS = frozenset(
    {"read_paths()", "declare_ingest()", "connector format", "format table"}
)

_SKIP_DIRS = {
    "node_modules",
    "__pycache__",
    ".git",
    ".ipynb_checkpoints",
    "build",
    "target",
}

# (directory or file relative to the repo root, file suffixes scanned)
SCAN_ROOTS = (
    ("docs/docs", (".md", ".mdx")),
    ("docs/tests/python", (".py",)),
    ("docs/tests/scala", (".scala",)),
    ("docs/tests-dbr", (".py", ".scala")),
    ("examples", (".py", ".ipynb", ".md", ".sql", ".scala")),
    ("connectors", (".py", ".md", ".sql")),
    ("sample-data", (".py", ".md")),
    ("README.md", (".md",)),
)

# These quote bad names on purpose: this module and its test, and the connector test that
# checks an unknown ``format`` ("kml", "filegdb") is rejected.
_SELF = {
    Path(__file__).resolve(),
    Path(__file__).resolve().parent / "readers" / "test_registered_reader_names.py",
    REPO_ROOT / "connectors/lakeflow/sources/geobrix/tests/test_connector.py",
}

# Notebooks that still name a retired reader. Each file is listed on purpose, so the guard does not skip it
# silently: the entry goes when the notebook is rewritten to a lightweight reader, and
# ``test_deferred_notebooks_still_need_a_rewrite`` fails if a listed file is already clean.
# Until the notebooks are rewritten (Task 27), the scan reports their references apart from the docs.
DEFERRED_NOTEBOOKS = frozenset(
    {
        "examples/tests/classic_proj_grid_validation.ipynb",  # cell 5 reads with format("gdal")
    }
)


def _arguments(text, call_start):
    """Top-level arguments of the call whose ``(`` is at ``call_start``, as
    ``(keyword or None, value source, value offset)`` with ``#`` comments blanked;
    None when the parentheses do not close within a few lines (prose, not code)."""
    depth, quote, start = 0, None, call_start + 1
    parts, comments = [], []
    i, stop = start, min(len(text), start + 2000)
    while i < stop:
        c = text[i]
        if quote:
            if c == "\\":
                i += 1
            elif c == quote:
                quote = None
        elif c in "\"'":
            quote = c
        elif c == "#":  # a comment may hold commas, quotes and parentheses
            end = text.find("\n", i)
            end = stop if end < 0 else end
            comments.append((i, end))
            i = end
            continue
        elif c in "([{":
            depth += 1
        elif c in ")]}":
            if depth == 0:
                parts.append((start, i))
                break
            depth -= 1
        elif c == "," and depth == 0:
            parts.append((start, i))
            start = i + 1
        i += 1
    else:
        return None

    def source(a, b):
        chars = list(text[a:b])
        for ca, cb in comments:
            for k in range(max(ca, a), min(cb, b)):
                chars[k - a] = " "
        return "".join(chars)

    args = []
    for a, b in parts:
        part = source(a, b)
        if not part.strip():
            continue
        kw = re.match(r"\s*([A-Za-z_]\w*)\s*=(?!=)\s*", part)
        if kw:
            args.append((kw.group(1), part[kw.end():], a + kw.end()))
        else:
            args.append((None, part, a))
    return args


_STRING_LITERAL = re.compile(r"""\s*(["'])([^"'\n]*)\1\s*\Z""")


def _call_formats(text):
    """``(offset, kind, name)`` for the reader named by the ``format`` argument of
    ``read_paths`` / ``declare_ingest`` (positional or keyword) and the ``format=``
    keyword of ``load`` / ``save`` / ``saveAsTable``. An argument that is not a
    string literal (a variable, a signature's ``format``) is not a name."""
    found = []
    for func, position, kind in _FORMAT_ARGUMENT_CALLS:
        for call in re.finditer(rf"\b{func}\s*\(", text):
            args = _arguments(text, call.end() - 1)
            if not args:
                continue
            positional = [(a, o) for kw, a, o in args if kw is None]
            value = next(((a, o) for kw, a, o in args if kw == "format"), None)
            if value is None and position is not None and len(positional) > position:
                value = positional[position]
            literal = _STRING_LITERAL.match(value[0]) if value else None
            if literal:
                found.append((value[1] + literal.start(2), kind, literal.group(2)))
    return found


def _format_tables(text):
    """``(offset, name)`` for each backticked name in the first column of a table
    whose first header cell is ``format``."""
    found, offset, header, wanted = [], 0, True, False
    for line in text.splitlines(keepends=True):
        if line.startswith("|"):
            cells = line.strip().strip("|").split("|")
            first = cells[0].strip()
            if header:  # the first row of a table decides
                wanted, header = first == "`format`", False
            elif wanted and first and not set(first) <= set("-: "):
                for token in re.finditer(r"`([^`]+)`", cells[0]):
                    found.append((offset + line.index(token.group(1)), token.group(1)))
        else:
            header = True
        offset += len(line)
    return found


def reader_references(text):
    """``(line, kind, name)`` for every reader name ``text`` uses.

    Placeholders (``<reader_name>``, ``...``), fully-qualified class names and glob
    patterns are not names and are skipped.
    """
    found = set()

    def add(offset, kind, name):
        if _READER_NAME.match(name):
            found.add((text.count("\n", 0, offset) + 1, kind, name))

    for kind, pattern in _PATTERNS:
        for match in pattern.finditer(text):
            group = next((g for g in range(1, pattern.groups + 1) if match.group(g)), None)
            if group:
                add(match.start(group), kind, match.group(group))
    for offset, kind, name in _call_formats(text):
        add(offset, kind, name)
    for offset, name in _format_tables(text):
        add(offset, "format table", name)
    return sorted(found)


# --- retired reader names in prose and tables ------------------------------------

# The Scala readers are gone. ``reader_references`` finds a name where a reader is *chosen*; these patterns find
# one wherever it is *mentioned* (a table cell, a sentence, a code comment). ``gdal`` and ``ogr`` alone are the
# libraries, so only an inline code span of exactly that name, or a name with the ``_gdal`` / ``_ogr`` suffix,
# counts.
RETIRED_NAME_PATTERNS = (
    ("retired reader name", re.compile(r"\b[A-Za-z0-9]+(?:_[A-Za-z0-9]+)*_(?:gdal|ogr)\b|`\*_(?:gdal|ogr)`")),
    ("retired reader name", re.compile(r"`(?:gdal|ogr)`")),
    ("heavyweight reader", re.compile(r"\b(?:heavyweight|heavy)\s+(?:raster\s+|vector\s+)?readers?\b", re.IGNORECASE)),
)

# Docs pages that may name a retired reader. The release notes record the rename; ``api/benchmarking.mdx`` still
# describes the retired benchmark forms until its rewrite, and ``test_deferred_pages_still_need_a_rewrite`` fails
# when a listed page is already clean.
RETIRED_NAME_PAGES = ("docs/docs/release-notes",)
DEFERRED_PAGES = frozenset({"docs/docs/api/benchmarking.mdx"})


def retired_name_mentions(text):
    """``(line, kind, matched text)`` for each retired reader name ``text`` mentions."""
    found = set()
    for kind, pattern in RETIRED_NAME_PATTERNS:
        for match in pattern.finditer(text):
            found.add((text.count("\n", 0, match.start()) + 1, kind, match.group(0)))
    return sorted(found)


def docs_pages():
    """Every page under ``docs/docs`` (markdown and MDX), as repo-relative paths."""
    root = REPO_ROOT / "docs" / "docs"
    return sorted(
        p.relative_to(REPO_ROOT).as_posix()
        for p in root.rglob("*")
        if p.suffix in (".md", ".mdx") and not (set(p.parts) & _SKIP_DIRS)
    )


def retired_name_scan():
    """``(file, line, kind, text)`` for every retired reader name in a docs page, leaving out
    the pages that record the rename and the ``DEFERRED_PAGES``."""
    return [e for e in _retired_scan() if e[0] not in DEFERRED_PAGES]


def deferred_page_scan():
    """The retired names still on the ``DEFERRED_PAGES``."""
    return [e for e in _retired_scan() if e[0] in DEFERRED_PAGES]


def _retired_scan():
    found = []
    for rel in docs_pages():
        if rel.startswith(RETIRED_NAME_PAGES):
            continue
        text = (REPO_ROOT / rel).read_text(errors="replace")
        found.extend((rel, line, kind, hit) for line, kind, hit in retired_name_mentions(text))
    return found


def _chunks(path):
    """``(place, text)`` units to scan. A notebook is scanned per cell, since JSON line
    numbers mean nothing to its author; any other file is one unit."""
    if path.suffix != ".ipynb":
        return [(None, path.read_text(errors="replace"))]
    chunks = []
    for index, cell in enumerate(json.loads(path.read_text()).get("cells", []), 1):
        source = cell.get("source", "")
        chunks.append((f"cell {index}", source if isinstance(source, str) else "".join(source)))
    return chunks


def scan_files():
    """Every file the scan covers."""
    files = []
    for rel, suffixes in SCAN_ROOTS:
        root = REPO_ROOT / rel
        if root.is_file():
            files.append(root)
            continue
        for directory, subdirs, names in os.walk(root):
            subdirs[:] = [d for d in subdirs if d not in _SKIP_DIRS]
            files.extend(
                Path(directory) / n for n in names if n.endswith(suffixes)
            )
    return sorted(p for p in files if p.resolve() not in _SELF)


def scan():
    """``(references, unregistered)`` across the docs, examples and READMEs.

    Each is a list of ``(file, where, kind, name)``; ``where`` is ``line N`` or, in a
    notebook, ``cell N``. ``unregistered`` leaves out the ``DEFERRED_NOTEBOOKS``; ``deferred_scan()`` lists those.
    """
    references, unregistered = _scan()
    return references, [e for e in unregistered if e[0] not in DEFERRED_NOTEBOOKS]


def deferred_scan():
    """The unregistered names in the ``DEFERRED_NOTEBOOKS``, as ``(file, where, kind, name)``."""
    return [e for e in _scan()[1] if e[0] in DEFERRED_NOTEBOOKS]


def _scan():
    known = registered_reader_names() | BUILTIN_FORMATS
    references, unregistered = [], []
    for path in scan_files():
        rel = path.relative_to(REPO_ROOT).as_posix()
        for place, text in _chunks(path):
            for line, kind, name in reader_references(text):
                entry = (rel, place or f"line {line}", kind, name)
                references.append(entry)
                if not is_registered(kind, name, known):
                    unregistered.append(entry)
    return references, unregistered


def is_registered(kind, name, known=None):
    """True when ``name``, used the way ``kind`` says, resolves to a reader.

    ``read_paths`` / ``declare_ingest`` / the connector take a lightweight reader's
    name with the ``_gbx`` suffix optional; Spark's ``format()`` needs the exact name
    (or a built-in)."""
    known = known if known is not None else registered_reader_names() | BUILTIN_FORMATS
    lower = name.lower()
    if kind == "read_files(format=>)":
        return lower in READ_FILES_FORMATS
    if kind in LIGHT_NAME_KINDS:
        return (lower if lower.endswith("_gbx") else lower + "_gbx") in light_reader_names()
    return lower in known


def suggestions(name):
    """The registered readers whose name starts with ``name`` (``gpkg`` -> ``gpkg_gbx``)."""
    prefix = name.lower()
    return sorted(r for r in heavy_reader_names() | light_reader_names() if r.lower().startswith(prefix))
