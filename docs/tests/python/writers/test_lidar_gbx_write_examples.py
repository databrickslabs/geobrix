"""Executes the lidar_gbx writer doc examples against synthesized data (Docker).

Synthesizes a tiny x,y,z + r,g,b + group,cluster point cloud in-process
(no external sample data), imports the examples module, and runs each verifier
against the real lightweight writer. Run via gbx:test:python-docs.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import lidar_gbx_write_examples as ex  # noqa: E402


def test_write_sharded(spark, tmp_path):
    ex.write_sharded(spark, str(tmp_path / "sharded"))


def test_write_singlefile(spark, tmp_path):
    ex.write_singlefile(spark, str(tmp_path / "single"))


def test_write_merge(spark, tmp_path):
    ex.write_merge(spark, str(tmp_path / "merge"))
