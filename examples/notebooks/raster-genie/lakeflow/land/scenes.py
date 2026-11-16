"""Stack the per-band files of one Sentinel-2 scene into a single master COG.

One scene, one file: the bands an index needs plus SCL, on the 10 m grid of a reference band, with
512-pixel internal blocks, overviews at 2/4/8/16, GeoBrix's own compression options and nodata 0.
Nodata is the only validity signal. The file carries no internal mask band, which the Unity Catalog
windowed reader (`gbx_rst_fromfile` with a geometry or a resolution) does not plan.

No Spark, no side effects beyond `out_path`.
"""

import os
import shutil
from typing import Dict, Sequence

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
    reflectance. Overviews are averages, so read SCL at full resolution.
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
