"""Task 2 (SP2-CRS): production-path regression tests for CRS in lidar reader + LAZ encoder.

These tests call the ACTUAL production code paths so they would FAIL if either
production fix were reverted:

Encoder fix (imagery.py):
  Old code: ``ProjCRS.from_wkt("EPSG:4326")`` raised CRSError for authority
  strings → CRS silently dropped.  New code: routes through resolve_crs +
  to_pyproj_crs.  Before-FAIL: pass crs="EPSG:4326" (string) to write_xyz_laz →
  old code drops CRS → parse_crs() returns None → assertion fails.

Reader fix (lidar.py):
  Old code: ``crs_obj.to_wkt()`` gave a 689-char WKT for ESRI:54008
  ("PROJCRS["World_Sinusoidal"…]"), not "ESRI:54008".  New code: routes
  through crs_to_canonical(resolve_crs(crs_obj.to_wkt())).  Before-FAIL:
  revert lidar.py → ESRI header gives WKT blob → "ESRI:54008" assertion fails.
"""

import numpy as np
import pytest

# ─── shared helpers ──────────────────────────────────────────────────────────


def _tiny_xyz():
    """Minimal 3-point cloud; coordinates chosen to be ESRI:54008-plausible."""
    return (
        np.array([0.0, 1.0, 2.0], dtype=np.float64),
        np.array([0.0, 0.0, 0.0], dtype=np.float64),
        np.array([10.0, 10.0, 10.0], dtype=np.float64),
    )


def _write_las_with_esri54008_crs(path: str) -> None:
    """Write a minimal LAS 1.4 file whose header carries ESRI:54008 via OGC WKT VLR.

    laspy.LasHeader.add_crs only works for EPSG codes.  Non-EPSG projected CRS
    must be embedded via the OGC WKT VLR (record_id=2112) directly — the same
    mechanism real LiDAR software uses for MODIS sinusoidal grids.
    laspy.read().header.parse_crs() correctly reads this VLR back as a
    pyproj.CRS with ESRI:54008 authority.
    """
    import laspy
    import pyproj

    esri_crs = pyproj.CRS.from_authority("ESRI", "54008")
    hdr = laspy.LasHeader(point_format=0, version="1.4")
    hdr.offsets = np.zeros(3)
    hdr.scales = np.full(3, 0.01)
    las = laspy.LasData(header=hdr)
    x, y, z = _tiny_xyz()
    las.x = x
    las.y = y
    las.z = z
    wkt_bytes = esri_crs.to_wkt().encode("utf-8")
    vlr = laspy.VLR(
        user_id="LASF_Projection",
        record_id=2112,
        description="OGC Transformation Record",
        record_data=wkt_bytes,
    )
    las.vlrs.append(vlr)
    las.write(path)


# ─── encoder tests (imagery.py fix) ──────────────────────────────────────────

# String EPSG inputs (e.g. "EPSG:4326") are the critical regression cases:
# old code called ProjCRS.from_wkt("EPSG:4326") which raised CRSError → CRS dropped.
# Integer EPSG inputs (e.g. 4326) worked in both old and new code → positive control.
_ENCODER_CASES = [
    (
        "EPSG:4326",
        ("EPSG", "4326"),
    ),  # string path: FAILS under old code (fail-on-revert)
]


@pytest.mark.parametrize(
    "crs_input,expected_auth",
    _ENCODER_CASES,
    ids=["epsg_str"],
)
def test_laz_encoder_tags_epsg_crs_in_header(tmp_path, crs_input, expected_auth):
    """write_xyz_laz (→ _write_las in imagery.py) must tag the LAS header correctly.

    String inputs exercise the imagery.py fix: old code called
    ``ProjCRS.from_wkt(str(crs))`` which raises ``CRSError: Invalid WKT string``
    for authority strings like "EPSG:4326", silently dropping the CRS.
    This test FAILS for the string cases when imagery.py is reverted.
    """
    import laspy

    from databricks.labs.gbx.pyrx.imagery import write_xyz_laz

    x, y, z = _tiny_xyz()
    out = write_xyz_laz(str(tmp_path / "test.las"), x, y, z, crs=crs_input)

    with laspy.open(out) as reader:
        crs_back = reader.header.parse_crs()

    assert crs_back is not None, (
        f"CRS was not tagged in LAS header for input {crs_input!r}; "
        f"imagery.py likely reverted to ProjCRS.from_wkt() which raises on authority strings"
    )
    assert (
        crs_back.to_authority(min_confidence=100) == expected_auth
    ), f"Expected {expected_auth!r} but got {crs_back.to_authority(min_confidence=100)!r}"


# ─── reader tests (lidar.py fix) ─────────────────────────────────────────────


def test_lidar_reader_extracts_esri_crs_as_canonical(tmp_path):
    """LidarGbxReader._read_metadata must emit 'ESRI:54008' in the crs column.

    Exercises the real production path: LidarGbxReader._read_metadata is called
    on a LAS file whose header carries an ESRI:54008 CRS via OGC WKT VLR.

    Old code in lidar.py: ``crs = crs_obj.to_wkt()``
    → crs_obj.to_wkt() returns a 689-char WKT blob ("PROJCRS["World_Sinusoidal"…]")
      which is NOT equal to "ESRI:54008" → this test would FAIL.

    New code: ``crs = crs_to_canonical(resolve_crs(crs_obj.to_wkt()))``
    → correctly round-trips the WKT back to "ESRI:54008" via the PROJ registry.
    """
    from databricks.labs.gbx.ds.lidar import LidarGbxReader

    las_path = str(tmp_path / "esri_test.las")
    _write_las_with_esri54008_crs(las_path)

    # Call the real production reader (same code path as mode="metadata" in Spark)
    reader = LidarGbxReader({"path": las_path, "mode": "metadata"})
    batches = list(reader._read_metadata(las_path))

    assert len(batches) == 1, "expected exactly one metadata row"
    row = batches[0].to_pydict()
    crs = row["crs"][0]
    assert crs == "ESRI:54008", (
        f"Expected 'ESRI:54008' in crs column but got {crs!r}; "
        f"lidar.py may have reverted to crs_obj.to_wkt() which returns raw WKT"
    )


def test_lidar_reader_none_crs_stays_none(tmp_path):
    """A LAS file with no CRS VLR must produce a None crs column — not crash."""
    import laspy

    from databricks.labs.gbx.ds.lidar import LidarGbxReader

    # Write a plain LAS with no CRS
    hdr = laspy.LasHeader(point_format=0, version="1.4")
    hdr.offsets = np.zeros(3)
    hdr.scales = np.full(3, 0.01)
    las = laspy.LasData(header=hdr)
    x, y, z = _tiny_xyz()
    las.x = x
    las.y = y
    las.z = z
    las_path = str(tmp_path / "no_crs.las")
    las.write(las_path)

    reader = LidarGbxReader({"path": las_path, "mode": "metadata"})
    batches = list(reader._read_metadata(las_path))
    assert len(batches) == 1
    assert batches[0].to_pydict()["crs"][0] is None
