"""Every reader name a doc, example or README snippet uses is a registered reader.

A snippet that says ``spark.read.format("gpkg")`` copy-pastes into a
``DATA_SOURCE_NOT_FOUND`` error: the readers are ``gpkg_ogr`` (heavyweight) and
``gpkg_gbx`` (lightweight), and there are no aliases. This is a static scan -- no Spark,
no JAR -- over the doc pages, the doc-test sources they import, the example notebooks and
the READMEs. The registered names come from the heavyweight services file and the
lightweight ``_SOURCES`` (see ``registered_readers.py``); Spark built-ins are allowed.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import registered_readers as rr  # noqa: E402


def test_registered_names_are_the_shipped_readers():
    heavy, light = rr.heavy_reader_names(), rr.light_reader_names()
    # One entry per line of the services file: a class whose shortName() is missed
    # would shrink the set silently.
    assert len(heavy) == len(rr.SERVICES_FILE.read_text().split())
    assert {"gdal", "gtiff_gdal", "shapefile_ogr", "geojson_ogr", "gpkg_ogr", "file_gdb_ogr"} <= heavy
    assert {"raster_gbx", "gtiff_gbx", "shapefile_gbx", "geojson_gbx", "gpkg_gbx", "file_gdb_gbx"} <= light
    # A bare format name is registered by neither tier (the failure this test guards).
    assert not ({"gpkg", "shapefile", "geojson", "filegdb", "file_gdb"} & (heavy | light))


@pytest.mark.parametrize(
    "text, expected",
    [
        ('spark.read.format("gpkg").load(p)', [("format()", "gpkg")]),
        ("spark.read.format('gpkg_ogr').load(p)", [("format()", "gpkg_ogr")]),
        ('spark.read\n    .format(\n        "shapefile")', [("format()", "shapefile")]),
        ('df.write.format("pmtiles").save(p)', [("format()", "pmtiles")]),
        ('spark.read.load(p, format="gpkg")', [("load(format=)", "gpkg")]),
        ("SELECT * FROM gpkg.`/data/a.gpkg`", [("SQL path read", "gpkg")]),
        ("select * from gpkg_ogr.`/data/a.gpkg`", [("SQL path read", "gpkg_ogr")]),
        ("CREATE TABLE t USING gpkg LOCATION '/x'", [("SQL USING", "gpkg")]),
        ("a table made using gpkg files", []),
        ('spark.read.format("<reader_name>")', []),
        ('spark.read.format("...")', []),
        ('.format("com.databricks.labs.gbx.ds.whitelist.WhitelistDataSource")', []),
    ],
)
def test_the_scan_finds_the_names_a_snippet_uses(text, expected):
    assert [(kind, name) for _, kind, name in rr.reader_references(text)] == expected


def test_the_scan_reads_notebook_cells(tmp_path):
    import json

    notebook = tmp_path / "nb.ipynb"
    notebook.write_text(
        json.dumps(
            {
                "cells": [
                    {"cell_type": "markdown", "source": ["no reader here"]},
                    {
                        "cell_type": "code",
                        "source": ["df = (spark.read\n", '    .format("gpkg")\n', "    .load(p))"],
                    },
                ]
            }
        )
    )
    assert rr._chunks(notebook)[1][0] == "cell 2"
    assert [n for _, _, n in rr.reader_references(rr._chunks(notebook)[1][1])] == ["gpkg"]


def test_the_scan_covers_the_docs_the_examples_and_the_readmes():
    """A moved directory must not turn the scan into a no-op."""
    files = {p.relative_to(rr.REPO_ROOT).as_posix() for p in rr.scan_files()}
    for prefix in (
        "docs/docs/",
        "docs/docs/ingestion/",
        "docs/tests/python/",
        "docs/tests/python/ingestion/",
        "examples/",
        "sample-data/",
    ):
        assert any(f.startswith(prefix) for f in files), f"nothing scanned under {prefix}"
    assert "README.md" in files
    references, _ = rr.scan()
    assert len(references) > 300, f"only {len(references)} reader references found"


def test_docs_and_examples_name_only_registered_readers():
    _, unregistered = rr.scan()
    report = [
        f"{file} ({where}): {kind} names '{name}'"
        + (f" -- did you mean {' / '.join(s)}?" if (s := rr.suggestions(name)) else "")
        for file, where, kind, name in unregistered
    ]
    assert not report, (
        "snippets name readers that are not registered "
        "(heavyweight: shortName() in the services file; lightweight: ds/register.py _SOURCES):\n  "
        + "\n  ".join(report)
    )
