"""Tests for LidarDownloader — no live network; _get is fully mocked.

Coverage:
  N1  — nodes_in_aoi returns intersecting node from a fake EPT store
  DL1 — download writes a .laz file to out_dir/<project>/<key>.laz (driver path)
"""

import io
import json

import numpy as np
import laspy
import pytest


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


def test_nodes_in_aoi_selects_intersecting_node():
    from databricks.labs.gbx.sample.lidar import Ept
    ept = Ept("https://x/store", _get=_fake_ept_store())
    nodes = ept.nodes_in_aoi((10, 10, 90, 90), max_depth=1)
    assert ("0-0-0-0", 3) in nodes


def test_download_writes_laz(tmp_path):
    from databricks.labs.gbx.sample.lidar import LidarDownloader
    dl = LidarDownloader(projects={"P": "https://x/store"}, _get=_fake_ept_store())
    dl.download(aoi_lonlat=None, aoi_3857=(10, 10, 90, 90), out_dir=str(tmp_path),
                spark=None, max_depth=1, write_laz=True)
    out = tmp_path / "P" / "0-0-0-0.laz"
    assert out.exists() and out.stat().st_size > 0
