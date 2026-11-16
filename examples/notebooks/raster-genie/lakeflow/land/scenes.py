"""Stack the per-band files of one Sentinel-2 scene into a single master COG.

One scene, one file: the bands an index needs plus SCL, on the 10 m grid of a reference band, with
512-pixel internal blocks, nearest-neighbour overviews at 2/4/8/16, GeoBrix's own compression options and
nodata 0. The overviews are nearest neighbour, not averages: a COG has one overview resampling for all
bands, and SCL holds class ids, which an average would blend into ids that mean nothing. A coarse read of
the file can therefore be masked with SCL.
Nodata is the only validity signal. The file carries no internal mask band, which the Unity Catalog
windowed reader (`gbx_rst_fromfile` with a geometry or a resolution) does not plan.

No Spark, no side effects beyond `out_path`.
"""

import datetime
import os
import shutil
from typing import Dict, List, Optional, Sequence

import numpy as np
from databricks.labs.gbx.pyrx.core.compression import creation_opts
from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_dir

#: Band order of every Sentinel-2 master COG: NIR (B08) is band 4, SWIR2 (B12) band 6 (1-based).
S2_BANDS = ("B02", "B03", "B04", "B08", "B11", "B12", "SCL")
#: Bands that hold class ids, resampled with nearest neighbour. Every other band is reflectance.
CATEGORICAL = frozenset({"SCL"})
#: A COG's block size and overview levels.
BLOCKSIZE = 512
OVERVIEW_LEVELS = [2, 4, 8, 16]
NODATA = 0
_DTYPE = "uint16"


def boa_offset_for(processing_baseline) -> int:
    """The additive offset of Sentinel-2 L2A reflectance: 1000 from processing baseline 04.00, else 0."""
    try:
        return 1000 if float(str(processing_baseline)) >= 4.0 else 0
    except ValueError:
        return 0


def stack_scene(
    band_paths: Dict[str, str],
    out_path: str,
    bands: Sequence[str] = S2_BANDS,
    ref: str = "B04",
    boa_offset: int = 0,
) -> str:
    """Write one multi-band master COG from per-band files and return `out_path`.

    `band_paths` maps a band name to its file. The grid of `ref` (a 10 m band) is the output grid:
    the other bands are resampled onto it, bilinear for reflectance and nearest for the bands in
    `CATEGORICAL`. Band descriptions are the band names, in `bands` order.

    `boa_offset` is subtracted from the reflectance bands, never below 1 so that a valid pixel does
    not become nodata. Pass `boa_offset_for(baseline)` so that an index (a ratio) is taken on true
    reflectance. Overviews are nearest neighbour, so SCL keeps its class ids at every level.
    """
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.shutil import copy as rio_copy
    from rasterio.vrt import WarpedVRT
    from rasterio.windows import Window

    bands = tuple(bands)
    missing = [b for b in bands if b not in band_paths]
    if missing:
        raise ValueError(
            f"stack_scene: no file for band(s) {', '.join(missing)}. "
            f"Fix: pass band_paths with a file for each of {', '.join(bands)}."
        )
    if ref not in bands:
        raise ValueError(
            f"stack_scene: ref band {ref} is not one of {', '.join(bands)}. "
            "Fix: pass a reference band that is in bands."
        )

    scratch = new_local_temp_dir("gbx_scene_")
    try:
        with rasterio.open(band_paths[ref]) as r:
            profile = dict(
                driver="GTiff",
                width=r.width,
                height=r.height,
                count=len(bands),
                dtype=_DTYPE,
                crs=r.crs,
                transform=r.transform,
                nodata=NODATA,
                tiled=True,
                blockxsize=BLOCKSIZE,
                blockysize=BLOCKSIZE,
                BIGTIFF="IF_SAFER",
            )
            height, width = r.height, r.width

        staged = os.path.join(scratch, "stack.tif")
        with rasterio.open(staged, "w", **profile) as dst:
            for i, name in enumerate(bands, start=1):
                categorical = name in CATEGORICAL
                offset = 0 if categorical else int(boa_offset)
                resampling = Resampling.nearest if categorical else Resampling.bilinear
                with rasterio.open(band_paths[name]) as src:
                    with WarpedVRT(
                        src,
                        crs=profile["crs"],
                        transform=profile["transform"],
                        width=width,
                        height=height,
                        resampling=resampling,
                        nodata=NODATA,
                        dtype=_DTYPE,
                    ) as vrt:
                        # One block row at a time: memory stays bounded for a full scene.
                        for row in range(0, height, BLOCKSIZE):
                            win = Window(0, row, width, min(BLOCKSIZE, height - row))
                            data = vrt.read(1, window=win)
                            if offset:
                                valid = data != NODATA
                                shifted = np.maximum(data.astype("int32") - offset, 1)
                                data = np.where(valid, shifted, NODATA).astype(_DTYPE)
                            dst.write(data, i, window=win)
                dst.set_band_description(i, name)

        cog = os.path.join(scratch, "scene.tif")
        opts = creation_opts(_DTYPE, compress="auto", driver="COG")
        rio_copy(
            staged,
            cog,
            driver="COG",
            blocksize=BLOCKSIZE,
            overview_resampling="NEAREST",
            overview_count=len(OVERVIEW_LEVELS),
            bigtiff="IF_SAFER",
            **opts,
        )
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        # A Volume is FUSE: write whole, sequentially, from a local file, then publish by rename.
        part = out_path + ".part"
        shutil.copyfile(cog, part)
        os.replace(part, out_path)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    return out_path


def _clean(rows: List[dict], max_cloud: float, max_nodata: float) -> List[dict]:
    """Scenes at or under the cloud limit, and under the no-data limit or with no no-data figure (kept, not dropped:
    the STAC figure is a whole-tile one, and a scene without it is judged by the coverage check on the result)."""
    return [
        r
        for r in rows
        if r["cloud_pct"] is not None
        and r["cloud_pct"] <= max_cloud
        and (r["nodata_pct"] is None or r["nodata_pct"] <= max_nodata)
    ]


def _best_on_day(rows: List[dict]) -> dict:
    """The scene of one tile on one day: the least cloud, ties (the same whole percent) by item id."""
    return min(rows, key=lambda r: (round(r["cloud_pct"]), r["cloud_pct"], r["item_id"]))


def _cover(rows: List[dict], tiles: Sequence[str], later_is_better: bool) -> Dict[str, dict]:
    """One scene per tile, on as few dates as possible: take the date that serves most tiles, then the date
    that serves most of the rest, and so on. A tie goes to the date closest to the fire (`later_is_better`
    for the window before it, earlier for the window after)."""
    chosen: Dict[str, dict] = {}
    remaining = set(tiles)
    sign = -1 if later_is_better else 1
    while remaining:
        by_day: Dict[str, Dict[str, List[dict]]] = {}
        for r in rows:
            if r["mgrs_tile"] in remaining:
                by_day.setdefault(r["date"][:10], {}).setdefault(r["mgrs_tile"], []).append(r)
        if not by_day:
            break
        day = min(
            by_day,
            key=lambda d: (-len(by_day[d]), sign * datetime.date.fromisoformat(d).toordinal(), d),
        )
        for tile, rs in by_day[day].items():
            chosen[tile] = _best_on_day(rs)
            remaining.discard(tile)
    return chosen


def choose_scene_pairs(
    pre: List[dict], post: List[dict], max_cloud: float, max_nodata: float
) -> Dict[str, object]:
    """Pick one pre and one post scene per MGRS tile.

    Rows are dicts with `item_id`, `mgrs_tile`, `date` (ISO), `orbit` (relative orbit), `cloud_pct`, `nodata_pct`
    (None when STAC has no figure). The rules, in order:

    1. A scene is a candidate when its cloud cover is at most `max_cloud` and its no-data share at most
       `max_nodata` (or unknown).
    2. The pre scenes are chosen to share as few dates as possible across the tiles, the date closest to the
       fire first.
    3. A tile's post scene is on the same relative orbit as its pre scene where that orbit has a candidate
       (the same view angle, so a difference between the two is the ground). Then the post scenes share as few
       dates as possible, the earliest first.

    Returns `{"pre": {tile: row}, "post": {tile: row}, "dropped": [tiles without a candidate on a side],
    "orbit_mismatch": [tiles whose post orbit is not their pre orbit]}`.
    """
    pre_c, post_c = _clean(pre, max_cloud, max_nodata), _clean(post, max_cloud, max_nodata)
    tiles = sorted({r["mgrs_tile"] for r in pre_c} & {r["mgrs_tile"] for r in post_c})
    dropped = sorted(({r["mgrs_tile"] for r in pre} | {r["mgrs_tile"] for r in post}) - set(tiles))
    pre_sel = _cover(pre_c, tiles, later_is_better=True)
    post_rows: List[dict] = []
    for t in tiles:
        same = [r for r in post_c if r["mgrs_tile"] == t and r["orbit"] == pre_sel[t]["orbit"]]
        post_rows += same or [r for r in post_c if r["mgrs_tile"] == t]
    post_sel = _cover(post_rows, tiles, later_is_better=False)
    mismatch = [t for t in tiles if post_sel[t]["orbit"] != pre_sel[t]["orbit"]]
    return {"pre": pre_sel, "post": post_sel, "dropped": dropped, "orbit_mismatch": mismatch}
