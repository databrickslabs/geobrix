"""Stack the per-band files of one Sentinel-2 scene into a single master COG.

One scene, one file: the bands an index needs plus SCL, on the 10 m grid of a reference band, with
512-pixel internal blocks, average overviews at 2/4/8/16, GeoBrix's own compression options and nodata 0.

The scene is **cloud-masked when the file is written**: at the native 10 m, every reflectance pixel whose
SCL class is no data (0), saturated (1), cloud shadow (3), cloud medium or high probability (8, 9), thin
cirrus (10) or snow (11) is set to nodata. Downstream SQL then needs no mask: a coarse read is already
clean. GDAL's average skips nodata, so a masked pixel does not pull an overview pixel toward zero; an
overview pixel is nodata only where all the pixels it covers are.

SCL stays in the file as band 7, for reference, and is valid at full resolution only: its overview
levels are averages of class ids and mean nothing. Nodata is the only validity signal. The file carries
no internal mask band, which the Unity Catalog windowed reader (`gbx_rst_fromfile` with a geometry or a
resolution) does not plan.

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
#: SCL classes masked out of the reflectance bands: no data, saturated, cloud shadow, cloud (medium, high
#: probability), thin cirrus, snow.
SCL_MASKED = (0, 1, 3, 8, 9, 10, 11)


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
    mask_scl: bool = True,
    stats: Optional[dict] = None,
) -> str:
    """Write one multi-band master COG from per-band files and return `out_path`.

    `band_paths` maps a band name to its file. The grid of `ref` (a 10 m band) is the output grid:
    the other bands are resampled onto it, bilinear for reflectance and nearest for the bands in
    `CATEGORICAL`. Band descriptions are the band names, in `bands` order.

    With `mask_scl` (the default) and an SCL band among `bands`, the reflectance bands are set to nodata
    wherever SCL is in `SCL_MASKED`, at the native 10 m, before the file is written. SCL itself is
    written as it is. A dict passed as `stats` is filled with the scene's counts: `data_px` (pixels whose
    SCL is not no data), `masked_px` (pixels masked for cloud, shadow, snow or saturation) and
    `masked_pct` (their share of `data_px`).

    `boa_offset` is subtracted from the reflectance bands, never below 1 so that a valid pixel does
    not become nodata. Pass `boa_offset_for(baseline)` so that an index (a ratio) is taken on true
    reflectance. Overviews are averages that skip nodata, so a reflectance overview is the mean of its
    valid pixels; SCL is valid at full resolution only.
    """
    import contextlib

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
        masking = mask_scl and "SCL" in bands
        data_px = masked_px = 0
        with contextlib.ExitStack() as stack:
            dst = stack.enter_context(rasterio.open(staged, "w", **profile))
            vrts = {}
            for name in bands:
                src = stack.enter_context(rasterio.open(band_paths[name]))
                vrts[name] = stack.enter_context(
                    WarpedVRT(
                        src,
                        crs=profile["crs"],
                        transform=profile["transform"],
                        width=width,
                        height=height,
                        resampling=(
                            Resampling.nearest
                            if name in CATEGORICAL
                            else Resampling.bilinear
                        ),
                        nodata=NODATA,
                        dtype=_DTYPE,
                    )
                )
            # One block row at a time: memory stays bounded for a full scene.
            for row in range(0, height, BLOCKSIZE):
                win = Window(0, row, width, min(BLOCKSIZE, height - row))
                bad = None
                if masking:
                    scl = vrts["SCL"].read(1, window=win)
                    bad = np.isin(scl, SCL_MASKED)
                    data_px += int((scl != NODATA).sum())
                    masked_px += int((bad & (scl != NODATA)).sum())
                for i, name in enumerate(bands, start=1):
                    categorical = name in CATEGORICAL
                    data = vrts[name].read(1, window=win)
                    offset = 0 if categorical else int(boa_offset)
                    if offset:
                        valid = data != NODATA
                        shifted = np.maximum(data.astype("int32") - offset, 1)
                        data = np.where(valid, shifted, NODATA).astype(_DTYPE)
                    if bad is not None and not categorical:
                        data = np.where(bad, NODATA, data).astype(_DTYPE)
                    dst.write(data, i, window=win)
            for i, name in enumerate(bands, start=1):
                dst.set_band_description(i, name)
        if stats is not None:
            stats.update(
                data_px=data_px,
                masked_px=masked_px,
                masked_pct=round(100.0 * masked_px / data_px, 2) if data_px else 0.0,
            )

        cog = os.path.join(scratch, "scene.tif")
        opts = creation_opts(_DTYPE, compress="auto", driver="COG")
        rio_copy(
            staged,
            cog,
            driver="COG",
            blocksize=BLOCKSIZE,
            overview_resampling="AVERAGE",
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
    the STAC figure is a whole-tile one, and a scene without it is judged by the coverage check on the result).
    """
    return [
        r
        for r in rows
        if r["cloud_pct"] is not None
        and r["cloud_pct"] <= max_cloud
        and (r["nodata_pct"] is None or r["nodata_pct"] <= max_nodata)
    ]


def _best_on_day(rows: List[dict]) -> dict:
    """The scene of one tile on one day: the least cloud, ties (the same whole percent) by item id."""
    return min(
        rows, key=lambda r: (round(r["cloud_pct"]), r["cloud_pct"], r["item_id"])
    )


def _cover(
    rows: List[dict], tiles: Sequence[str], later_is_better: bool
) -> Dict[str, dict]:
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
                by_day.setdefault(r["date"][:10], {}).setdefault(
                    r["mgrs_tile"], []
                ).append(r)
        if not by_day:
            break
        day = min(
            by_day,
            key=lambda d: (
                -len(by_day[d]),
                sign * datetime.date.fromisoformat(d).toordinal(),
                d,
            ),
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
    pre_c, post_c = _clean(pre, max_cloud, max_nodata), _clean(
        post, max_cloud, max_nodata
    )
    tiles = sorted({r["mgrs_tile"] for r in pre_c} & {r["mgrs_tile"] for r in post_c})
    dropped = sorted(
        ({r["mgrs_tile"] for r in pre} | {r["mgrs_tile"] for r in post}) - set(tiles)
    )
    pre_sel = _cover(pre_c, tiles, later_is_better=True)
    post_rows: List[dict] = []
    for t in tiles:
        same = [
            r
            for r in post_c
            if r["mgrs_tile"] == t and r["orbit"] == pre_sel[t]["orbit"]
        ]
        post_rows += same or [r for r in post_c if r["mgrs_tile"] == t]
    post_sel = _cover(post_rows, tiles, later_is_better=False)
    mismatch = [t for t in tiles if post_sel[t]["orbit"] != pre_sel[t]["orbit"]]
    return {
        "pre": pre_sel,
        "post": post_sel,
        "dropped": dropped,
        "orbit_mismatch": mismatch,
    }
