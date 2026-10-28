"""Unit tests for VectorGbxReader._members and schema-divergence detection.

These are pure-unit tests (no Spark) that verify:
  - _members() enumerates matching vector files recursively
  - divergent schemas in a multi-shapefile directory raise ValueError
  - same-schema multi-shapefile directories still union without error
  - single shapefiles are unaffected by the schema check
  - flat-directory regression (existing flat behaviour unchanged)
  - bare-file path passthrough (unchanged)
  - .gdb directory passthrough (unchanged)
"""

import os
from unittest.mock import patch

import pytest

from databricks.labs.gbx.ds.vector import VectorGbxReader


def _make_reader(path: str, driver: str = "ESRI Shapefile") -> VectorGbxReader:
    """Build a VectorGbxReader pointing at *path* with *driver*, bypassing Spark."""
    return VectorGbxReader({"path": path, "driverName": driver})


def _touch_shp_bundle(directory: str, stem: str) -> None:
    """Create a minimal shapefile bundle (.shp + .dbf + .shx sidecars)."""
    for ext in (".shp", ".dbf", ".shx"):
        open(os.path.join(directory, stem + ext), "wb").close()


# ---------------------------------------------------------------------------
# Recursive enumeration
# ---------------------------------------------------------------------------


def test_members_recursive_shapefiles(tmp_path):
    """_members() must recurse into subdirectories and enumerate ALL .shp files."""
    sub1 = tmp_path / "sub1"
    sub2 = tmp_path / "sub2"
    sub1.mkdir()
    sub2.mkdir()

    _touch_shp_bundle(str(sub1), "a")
    _touch_shp_bundle(str(sub2), "b")

    reader = _make_reader(str(tmp_path))
    members = reader._members()

    basenames = {os.path.basename(m) for m in members}
    assert basenames == {
        "a.shp",
        "b.shp",
    }, f"Expected {{a.shp, b.shp}} but got {basenames}; full members={members}"


def test_members_recursive_deeply_nested(tmp_path):
    """Recursion must work for arbitrarily deep nesting."""
    deep = tmp_path / "a" / "b" / "c"
    deep.mkdir(parents=True)
    _touch_shp_bundle(str(deep), "deep")

    reader = _make_reader(str(tmp_path))
    members = reader._members()

    assert any(
        m.endswith("deep.shp") for m in members
    ), f"Expected deep.shp in members but got {members}"


# ---------------------------------------------------------------------------
# Regression — flat directory (unchanged behaviour)
# ---------------------------------------------------------------------------


def test_members_flat_directory_shapefiles(tmp_path):
    """Flat directory (no sub-dirs) still enumerates .shp files."""
    _touch_shp_bundle(str(tmp_path), "x")
    _touch_shp_bundle(str(tmp_path), "y")

    reader = _make_reader(str(tmp_path))
    members = reader._members()

    basenames = {os.path.basename(m) for m in members}
    assert basenames == {"x.shp", "y.shp"}


def test_members_sidecars_excluded(tmp_path):
    """Only .shp files (not .dbf/.shx sidecars) are returned for ESRI Shapefile driver."""
    _touch_shp_bundle(str(tmp_path), "only")

    reader = _make_reader(str(tmp_path))
    members = reader._members()

    # Only the .shp file should be in members; sidecars are filtered out
    assert all(
        m.endswith(".shp") or m.endswith(".shz") or m.endswith(".zip") for m in members
    ), f"Unexpected sidecar in members: {members}"
    assert any(m.endswith("only.shp") for m in members)


# ---------------------------------------------------------------------------
# Regression — bare file path (unchanged behaviour)
# ---------------------------------------------------------------------------


def test_members_bare_file_returns_self(tmp_path):
    """A path pointing to a plain file returns [self.path] unchanged."""
    shp = tmp_path / "single.shp"
    shp.write_bytes(b"")

    reader = _make_reader(str(shp))
    members = reader._members()

    assert members == [str(shp)]


# ---------------------------------------------------------------------------
# Regression — .gdb directory passthrough (unchanged behaviour)
# ---------------------------------------------------------------------------


def test_members_gdb_dir_returns_self(tmp_path):
    """A .gdb directory is a single FileGDB dataset and must be returned as-is."""
    gdb = tmp_path / "dataset.gdb"
    gdb.mkdir()
    # Add a file inside so it's a real dir with contents
    (gdb / "a00000001.gdbtable").write_bytes(b"")

    reader = _make_reader(str(gdb), driver="OpenFileGDB")
    members = reader._members()

    assert members == [str(gdb)], f"Expected [{gdb}] but got {members}"


# ---------------------------------------------------------------------------
# Regression — empty directory falls back to [self.path] (unchanged behaviour)
# ---------------------------------------------------------------------------


def test_members_empty_dir_falls_back_to_path(tmp_path):
    """An empty directory with no matching files returns [self.path] as fallback."""
    empty = tmp_path / "empty"
    empty.mkdir()

    reader = _make_reader(str(empty))
    members = reader._members()

    assert members == [str(empty)]


# ---------------------------------------------------------------------------
# Schema-divergence error on multi-member shapefile directory
# ---------------------------------------------------------------------------


def _fake_info(fields, ogr_types):
    """Minimal pyogrio read_info dict for schema-check unit testing."""
    return {
        "fields": fields,
        "ogr_types": ogr_types,
        "ogr_subtypes": ["OFSTNone"] * len(fields),
        "geometry_name": "geom_0",
        "crs": None,
    }


def test_schema_divergence_raises_valueerror(tmp_path):
    """A directory with two shapefiles of differing schemas must raise ValueError
    with the shared divergence message."""
    # Create two stub .shp bundles
    _touch_shp_bundle(str(tmp_path), "roads")
    _touch_shp_bundle(str(tmp_path), "rivers")

    # roads: one field 'name' (string); rivers: one field 'width' (real) — divergent
    info_roads = _fake_info(["name"], ["OFTString"])
    info_rivers = _fake_info(["width"], ["OFTReal"])

    def fake_info_for(self, path):
        if "roads" in path:
            return info_roads
        return info_rivers

    reader = _make_reader(str(tmp_path))

    with patch.object(VectorGbxReader, "_info_for", fake_info_for):
        with pytest.raises(ValueError) as exc:
            reader.schema()

    msg = str(exc.value)
    assert "shapefile reader: shapefiles under" in msg
    assert "have differing schemas" in msg
    assert "load them separately" in msg
    assert "roads" in msg or "rivers" in msg  # at least one diverging stem appears


def test_same_schema_multi_shapefile_no_error(tmp_path):
    """A directory with two shapefiles of IDENTICAL schemas must NOT raise — the
    union read proceeds normally."""
    _touch_shp_bundle(str(tmp_path), "roads")
    _touch_shp_bundle(str(tmp_path), "rivers")

    # Both have the same field set
    info_both = _fake_info(["name", "length"], ["OFTString", "OFTReal"])

    def fake_info_for(self, path):
        return info_both

    reader = _make_reader(str(tmp_path))

    with patch.object(VectorGbxReader, "_info_for", fake_info_for):
        # Should not raise; schema() returns the StructType from the first member
        schema = reader.schema()

    # Confirm the schema was returned (not None) and has the expected fields
    field_names = [f.name for f in schema.fields]
    assert "name" in field_names
    assert "length" in field_names


def test_single_shapefile_no_schema_check(tmp_path):
    """A single .shp file (one member) skips the divergence check entirely
    (a single member is unchanged)."""
    shp = tmp_path / "lone.shp"
    shp.write_bytes(b"")

    info = _fake_info(["id"], ["OFTInteger"])

    def fake_info_for(self, path):
        return info

    reader = _make_reader(str(shp))

    with patch.object(VectorGbxReader, "_info_for", fake_info_for):
        # Single member — no schema check; should return schema without error
        schema = reader.schema()

    field_names = [f.name for f in schema.fields]
    assert "id" in field_names


# ---------------------------------------------------------------------------
# Directory of .shp.zip and mixed .shp/.shp.zip enumeration
# ---------------------------------------------------------------------------


def _touch_zip_bundle(directory: str, stem: str) -> None:
    """Create a minimal .shp.zip file (valid zip containing shapefile sidecars)."""
    import zipfile

    zip_path = os.path.join(directory, stem + ".shp.zip")
    with zipfile.ZipFile(zip_path, "w") as zf:
        for ext in (".shp", ".dbf", ".shx"):
            zf.writestr(stem + ext, b"")


def test_members_dir_of_shp_zip(tmp_path):
    """_members() must enumerate .shp.zip files in a directory (zipped shapefiles)."""
    _touch_zip_bundle(str(tmp_path), "a")
    _touch_zip_bundle(str(tmp_path), "b")
    reader = _make_reader(str(tmp_path))
    members = reader._members()
    basenames = {os.path.basename(m) for m in members}
    assert "a.shp.zip" in basenames, f"Expected a.shp.zip in members; got {basenames}"
    assert "b.shp.zip" in basenames, f"Expected b.shp.zip in members; got {basenames}"
    assert len(basenames) == 2, f"Expected exactly 2 members; got {basenames}"


def test_members_mixed_shp_and_zip(tmp_path):
    """_members() must enumerate both unzipped .shp bundles AND .shp.zip files in a mixed directory."""
    _touch_shp_bundle(str(tmp_path), "roads")
    _touch_zip_bundle(str(tmp_path), "rivers")
    reader = _make_reader(str(tmp_path))
    members = reader._members()
    basenames = {os.path.basename(m) for m in members}
    assert "roads.shp" in basenames, f"Expected roads.shp in {basenames}"
    assert "rivers.shp.zip" in basenames, f"Expected rivers.shp.zip in {basenames}"
