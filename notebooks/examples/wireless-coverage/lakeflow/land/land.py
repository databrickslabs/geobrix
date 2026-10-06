"""Wireless Coverage Lakeflow — land task.

Idempotent staging of LiDAR (.laz) and Overture water-mask data into the
pipeline input Volume.  Skips downloads when outputs are already present.

Entry point: main(argv) — invoked by the DAB job task::

    python land/land.py \\
        --catalog geospatial_docs \\
        --schema wireless_coverage_lf \\
        --volume data \\
        --full-aoi true \\
        --laz-dir "" \\
        --water-mask-dir ""

Path derivation mirrors transformations/_config.py so the land task and the
pipeline agree on where staged data lives.
"""

from __future__ import annotations

import argparse
import glob
import os
import sys
from typing import Optional, Tuple

# Constants mirror _config._FULL_SF / _DEMO_GGP exactly.
_FULL_SF = (-122.55, 37.70, -122.35, 37.85)
_DEMO_GGP = (-122.52, 37.76, -122.45, 37.78)  # Golden Gate Park window


def parse_args(argv):
    """Parse CLI arguments; exposed so tests can call it directly."""
    p = argparse.ArgumentParser(description="GeoBrix WC land task — stage inputs")
    p.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    p.add_argument("--schema", required=True, help="UC schema name")
    p.add_argument("--volume", required=True, help="UC volume name")
    p.add_argument(
        "--full-aoi",
        required=True,
        dest="full_aoi",
        help="'true' for full SF AOI; 'false' for demo GGP window",
    )
    p.add_argument(
        "--laz-dir",
        default="",
        dest="laz_dir",
        help="Override LAZ output directory (empty → derive from catalog/schema/volume)",
    )
    p.add_argument(
        "--water-mask-dir",
        default="",
        dest="water_mask_dir",
        help="Override water-mask output directory (empty → derive)",
    )
    return p.parse_args(argv)


def _resolve_paths(args) -> Tuple[str, str]:
    """Derive laz_dir and water_dir from args, mirroring _config.paths()."""
    root = f"/Volumes/{args.catalog}/{args.schema}/{args.volume}/wireless-coverage-lf"
    laz = args.laz_dir or f"{root}/lidar/sf/laz"
    water = args.water_mask_dir or f"{root}/overture-water"
    return laz, water


def _resolve_aoi(args) -> Tuple[float, float, float, float]:
    """Resolve AOI bbox from --full-aoi flag, mirroring _config.cfg()."""
    full = str(args.full_aoi).lower() == "true"
    return _FULL_SF if full else _DEMO_GGP


# ---------------------------------------------------------------------------
# Injection seam — monkeypatched in unit tests to avoid live network access.
# ---------------------------------------------------------------------------


def _download_lidar(
    aoi: Tuple[float, float, float, float],
    out_dir: str,
    spark,
) -> None:
    """Thin wrapper around LidarDownloader; seam for offline tests."""
    from databricks.labs.gbx.sample.lidar import LidarDownloader

    LidarDownloader().download(aoi_lonlat=aoi, out_dir=out_dir, spark=spark)


# ---------------------------------------------------------------------------
# Public staging functions
# ---------------------------------------------------------------------------


def stage_lidar(
    aoi: Tuple[float, float, float, float],
    out_dir: str,
    spark,
) -> None:
    """Stage LiDAR .laz files to *out_dir* unless already present (idempotent).

    Delegates to ``_download_lidar``; tests monkeypatch that seam to avoid
    live network access.
    """
    existing = glob.glob(os.path.join(out_dir, "**", "*.laz"), recursive=True)
    if existing:
        return
    _download_lidar(aoi, out_dir, spark)


def stage_water(
    bbox: Tuple[float, float, float, float],
    out_dir: str,
    spark,
) -> None:
    """Stage Overture base/water GeoParquet to *out_dir* unless already present.

    Discovers Overture base-theme assets, filters to type == 'water', and
    downloads with an AOI bbox pushdown.  Idempotent: skips when any .parquet
    file is already present under out_dir.
    """
    existing = glob.glob(os.path.join(out_dir, "**", "*.parquet"), recursive=True)
    if existing:
        return

    from pyspark.sql import functions as F

    from databricks.labs.gbx.sample.overture import OvertureClient

    client = OvertureClient()
    assets = client.discover(bbox, themes=["base"])
    water_assets = assets.filter(F.col("type") == "water")
    client.download(water_assets, out_dir, bbox=bbox)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: Optional[list] = None) -> None:
    """Parse arguments, build a SparkSession, and stage both inputs."""
    from pyspark.sql import SparkSession

    if argv is None:
        argv = sys.argv[1:]

    args = parse_args(argv)
    laz_dir, water_dir = _resolve_paths(args)
    aoi = _resolve_aoi(args)

    spark = SparkSession.builder.appName("wc-land").getOrCreate()

    os.makedirs(laz_dir, exist_ok=True)
    os.makedirs(water_dir, exist_ok=True)

    stage_lidar(aoi, laz_dir, spark)
    stage_water(aoi, water_dir, spark)


if __name__ == "__main__":
    main()
