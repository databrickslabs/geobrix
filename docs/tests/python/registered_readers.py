"""The reader names GeoBrix registers, and a scan for names that are not.

Two sources define what ``spark.read.format("<name>")`` can resolve to:

* heavyweight (JAR): the ``DataSourceRegister`` classes listed in
  ``src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister``;
  each class's ``shortName()`` is the name;
* lightweight (wheel): the ``name()`` of each class in
  ``databricks.labs.gbx.ds.register._SOURCES``.

A snippet that names anything else (``gpkg`` for ``gpkg_ogr`` / ``gpkg_gbx``, say)
fails at run time with ``DATA_SOURCE_NOT_FOUND``. Spark and Databricks built-in
formats are allowed.

Used by ``readers/test_registered_reader_names.py`` (the scan) and by the tests that
assert a reader name the docs display.
"""

import json
import os
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

SERVICES_FILE = (
    REPO_ROOT
    / "src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister"
)
SCALA_ROOT = REPO_ROOT / "src/main/scala"

# Formats Spark / Databricks provide; a snippet may use them next to GeoBrix readers.
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

_SHORT_NAME = re.compile(r'def\s+shortName\s*\(\s*\)\s*:\s*String\s*=\s*"([^"]+)"')
_READER_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def heavy_reader_names():
    """Short names of the JAR's registered data sources (services file -> ``shortName()``)."""
    names = set()
    for cls in SERVICES_FILE.read_text().split():
        package, _, simple = cls.rpartition(".")
        source = SCALA_ROOT / package.replace(".", "/") / f"{simple}.scala"
        match = _SHORT_NAME.search(source.read_text())
        assert match, f"{cls}: no shortName() found in {source}"
        names.add(match.group(1))
    return names


def light_reader_names():
    """Names of the lightweight (``*_gbx``) data sources ``register()`` installs."""
    from databricks.labs.gbx.ds.register import _SOURCES

    return {source.name() for source in _SOURCES}


def registered_reader_names():
    """Every GeoBrix reader name (heavyweight and lightweight), lower-cased as Spark matches them."""
    return {name.lower() for name in heavy_reader_names() | light_reader_names()}


# --- the scan ---------------------------------------------------------------

# ``.format("x")`` / ``.format('x')`` on a reader or writer (also across a line break).
_FORMAT_CALL = re.compile(r"""\.format\(\s*["']([^"'\n]*)["']""")
# ``.load(path, format="x")``
_LOAD_FORMAT = re.compile(r"""\.load\([^)\n]*\bformat\s*=\s*["']([^"'\n]*)["']""")
# ``SELECT * FROM x.`path` `` -- a path-style table reference names its reader.
_SQL_PATH_READ = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z_]\w*)\.`", re.IGNORECASE)
# ``CREATE TABLE t USING x`` (upper-case keyword only: prose says "using").
_SQL_USING = re.compile(r"\bUSING\s+([A-Za-z_]\w*)")

_PATTERNS = (
    ("format()", _FORMAT_CALL),
    ("load(format=)", _LOAD_FORMAT),
    ("SQL path read", _SQL_PATH_READ),
    ("SQL USING", _SQL_USING),
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

# This module and its test quote bad names on purpose.
_SELF = {
    Path(__file__).resolve(),
    Path(__file__).resolve().parent / "readers" / "test_registered_reader_names.py",
}


def reader_references(text):
    """``(line, kind, name)`` for every reader name ``text`` uses.

    Placeholders (``<reader_name>``, ``...``), fully-qualified class names and glob
    patterns are not names and are skipped.
    """
    found = []
    for kind, pattern in _PATTERNS:
        for match in pattern.finditer(text):
            name = match.group(1)
            if _READER_NAME.match(name):
                line = text.count("\n", 0, match.start(1)) + 1
                found.append((line, kind, name))
    return sorted(found)


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
    notebook, ``cell N``.
    """
    known = registered_reader_names() | BUILTIN_FORMATS
    references, unregistered = [], []
    for path in scan_files():
        rel = path.relative_to(REPO_ROOT).as_posix()
        for place, text in _chunks(path):
            for line, kind, name in reader_references(text):
                entry = (rel, place or f"line {line}", kind, name)
                references.append(entry)
                if name.lower() not in known:
                    unregistered.append(entry)
    return references, unregistered


def suggestions(name):
    """The registered readers whose name starts with ``name`` (``gpkg`` -> ``gpkg_gbx``, ``gpkg_ogr``)."""
    prefix = name.lower()
    return sorted(r for r in heavy_reader_names() | light_reader_names() if r.lower().startswith(prefix))
