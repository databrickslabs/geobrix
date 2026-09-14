"""Tests for LidarDownloader — no live network; _get is fully mocked.

Coverage:
  N1  — nodes_in_aoi returns intersecting node from a flat (non-paged) EPT store
  N2  — nodes_in_aoi loads a -1 sub-page, re-examines the node, and selects it
  DL1 — download (driver path) writes <out_dir>/<project>/<key>.laz (idempotent)
  W1  — fetch_nodes worker generator yields clipped point rows with correct schema
        (exercises the mapInPandas body without a live Spark cluster)
"""

import io
import json

import numpy as np
import laspy
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Fake EPT store builders
# ---------------------------------------------------------------------------


def _fake_ept_store():
    """A tiny 1-node EPT store: ept.json + hierarchy page + one ept-data laz."""
    ept_json = json.dumps({
        "bounds": [0, 0, 0, 100, 100, 100], "span": 128, "points": 3,
    }).encode()
    hier = json.dumps({"0-0-0-0": 3}).encode()
    # one laz node with 3 points, all inside AOI [10,10,90,90]
    hdr = laspy.LasHeader(point_format=3, version="1.2")
    hdr.offsets = [0, 0, 0]; hdr.scales = [0.01, 0.01, 0.01]
    las = laspy.LasData(hdr)
    las.x = np.array([20.0, 50.0, 80.0]); las.y = np.array([20.0, 50.0, 80.0])
    las.z = np.array([5.0, 10.0, 15.0])
    buf = io.BytesIO(); las.write(buf); laz_bytes = buf.getvalue()
    def _get(url, **kw):
        if url.endswith("ept.json"): return ept_json
        if "ept-hierarchy/0-0-0-0.json" in url: return hier
        if "ept-data/0-0-0-0.laz" in url: return laz_bytes
        raise FileNotFoundError(url)
    return _get


def _fake_ept_store_paged():
    """EPT store where the root page marks child 1-0-0-0 as -1 (sub-page dereference).

    Root page  (ept-hierarchy/0-0-0-0.json): {"0-0-0-0": 3, "1-0-0-0": -1}
    Sub-page   (ept-hierarchy/1-0-0-0.json): {"1-0-0-0": 7}

    Both nodes intersect AOI [10,10,90,90] with bounds [0,0,0,100,100,100].
    """
    ept_json = json.dumps({
        "bounds": [0, 0, 0, 100, 100, 100], "span": 128, "points": 10,
    }).encode()
    root_hier = json.dumps({"0-0-0-0": 3, "1-0-0-0": -1}).encode()
    sub_hier = json.dumps({"1-0-0-0": 7}).encode()

    def _get(url, **kw):
        if url.endswith("ept.json"): return ept_json
        if "ept-hierarchy/0-0-0-0.json" in url: return root_hier
        if "ept-hierarchy/1-0-0-0.json" in url: return sub_hier
        raise FileNotFoundError(url)
    return _get


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


# N1 — flat (non-paged) hierarchy
def test_nodes_in_aoi_selects_intersecting_node():
    from databricks.labs.gbx.sample.lidar import Ept
    ept = Ept("https://x/store", _get=_fake_ept_store())
    nodes = ept.nodes_in_aoi((10, 10, 90, 90), max_depth=1)
    assert ("0-0-0-0", 3) in nodes


# N2 — -1 sub-page dereference
def test_nodes_in_aoi_sub_page_deref():
    """nodes_in_aoi fetches a sub-page when it sees -1 and re-selects the node."""
    from databricks.labs.gbx.sample.lidar import Ept
    ept = Ept("https://x/store", _get=_fake_ept_store_paged())
    nodes = ept.nodes_in_aoi((10, 10, 90, 90), max_depth=1)
    assert ("0-0-0-0", 3) in nodes
    assert ("1-0-0-0", 7) in nodes


# DL1 — driver path writes the .laz file
def test_download_writes_laz(tmp_path):
    from databricks.labs.gbx.sample.lidar import LidarDownloader
    dl = LidarDownloader(projects={"P": "https://x/store"}, _get=_fake_ept_store())
    dl.download(aoi_lonlat=None, aoi_3857=(10, 10, 90, 90), out_dir=str(tmp_path),
                spark=None, max_depth=1, write_laz=True)
    out = tmp_path / "P" / "0-0-0-0.laz"
    assert out.exists() and out.stat().st_size > 0


# W1 — worker generator body + schema, no Spark cluster needed
def test_fetch_nodes_generator_yields_clipped_rows():
    """Call fetch_nodes as a plain Python generator over a pandas iterator.

    Exercises the mapInPandas worker body and _OUT_SCHEMA column contract without
    spinning up a Spark session. _urllib_fetch is bypassed via _fetch injection.
    """
    from databricks.labs.gbx.sample.lidar import _make_fetch_nodes

    fake_get = _fake_ept_store()
    fetch_nodes = _make_fetch_nodes(
        bases={"P": "https://x/store/"},
        aoi=(10, 10, 90, 90),
        out_dir=None,
        write_laz=False,
        clip=True,
        _fetch=fake_get,
    )
    # Simulate a one-partition mapInPandas iterator
    input_pdf = pd.DataFrame({"project": ["P"], "node_key": ["0-0-0-0"]})
    results = list(fetch_nodes(iter([input_pdf])))

    assert len(results) == 1, "expected exactly one output DataFrame"
    df = results[0]

    expected_cols = {
        "project", "node_key",
        "x_3857", "y_3857", "z_m",
        "lon", "lat",
        "intensity", "classification",
        "return_number", "number_of_returns",
    }
    assert set(df.columns) == expected_cols, df.columns.tolist()
    assert len(df) == 3, f"expected 3 clipped points, got {len(df)}"
    assert list(df["project"]) == ["P", "P", "P"]
    assert list(df["node_key"]) == ["0-0-0-0"] * 3
    # coordinates round-trip through las.write / laspy.read within ~1e-4 m
    assert df["x_3857"].tolist() == pytest.approx([20.0, 50.0, 80.0], abs=0.01)
    assert df["y_3857"].tolist() == pytest.approx([20.0, 50.0, 80.0], abs=0.01)
    assert df["z_m"].tolist() == pytest.approx([5.0, 10.0, 15.0], abs=0.01)
