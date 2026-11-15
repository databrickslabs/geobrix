"""Value-level output fingerprints for the levels leg.

The timed query of a form answers ``(n, fp, n_out)``. Its ``fp`` is cheap: ``sum(length(raster))`` for a tile output,
the hash of the value for a scalar. That is a fingerprint of the ENCODING, not of the values. Two levels that write
the same pixels with another codec, tiling, overview pyramid or vertex order get other fingerprints (the L1/L3 pairs
of ``rst_align_to``, ``rst_cog_convert``, ``rst_binpoints``, ``rst_bng_rasterize_agg`` and ``rst_boundingbox`` all
differed that way at 4x), and two doubles that differ in the last bit hash apart (``rst_avg``).

The functions here hash what the output MEANS: the decoded pixels (rounded, nodata masked, with the grid), the
normalised geometry, the rounded doubles, the decoded MVT features. A form that has a ``vsql`` runs it once, outside
the timed region, and its result replaces the length fingerprint (``v1:<n>``). Equal values give equal fingerprints
whatever the encoder; a real difference (``rst_proximity``: GDAL's approximate distance against the exact one) still
changes it.

The UDFs are registered by name (``gbx_bench_vfp_<kind>``), each ``binary|array<double> -> bigint``: a value of
0 ... 2**31 - 2, so the order-independent ``sum`` of a column cannot overflow.
"""

from __future__ import annotations

import hashlib
from typing import Callable, Dict

VERSION = "v1"
_MOD = 2147483647  # 2**31 - 1, the modulus the other fingerprints use
_PIXEL_DECIMALS = 6  # pixels and doubles are compared at 1e-6: below it is rounding noise between kernels
_COORD_DECIMALS = 6


def _digest(*parts: bytes) -> int:
    h = hashlib.blake2b(digest_size=8)
    for p in parts:
        h.update(p)
    return int.from_bytes(h.digest(), "big") % _MOD


def raster_fp(raster) -> int:
    """The decoded content of a raster: shape, band dtypes, georeferencing (transform and canonical CRS), the nodata
    value and mask, and the rounded pixels. The codec, tiling, overviews and tags do not enter.
    """
    import numpy as np

    from databricks.labs.gbx._gdal_policy import open_bytes
    from databricks.labs.gbx.core.crs import crs_to_canonical

    with open_bytes(bytes(raster)) as ds:
        arr = ds.read(masked=True)
        tr = np.round(np.array(ds.transform[:6], dtype="float64"), _COORD_DECIMALS)
        dtypes = str(ds.dtypes)
        nodata = repr(ds.nodata)
        crs = str(crs_to_canonical(ds.crs))
    mask = np.ma.getmaskarray(arr)
    data = (
        np.round(arr.filled(0).astype("float64"), _PIXEL_DECIMALS) + 0.0
    )  # + 0.0: -0.0 == 0.0
    return _digest(
        repr(arr.shape).encode(),
        dtypes.encode(),
        nodata.encode(),
        crs.encode(),
        tr.tobytes(),
        np.packbits(mask).tobytes(),
        np.ascontiguousarray(data).tobytes(),
    )


def wkb_fp(wkb) -> int:
    """A geometry up to vertex order, ring start and orientation (``shapely.normalize``), at the coordinate precision."""
    import numpy as np
    import shapely

    # rounded by hand, not `set_precision`: a bench geometry may be invalid (the bow-tie repair input) and that raises
    g = shapely.from_wkb(bytes(wkb))
    g = shapely.normalize(
        shapely.transform(g, lambda c: np.round(c, _COORD_DECIMALS) + 0.0)
    )
    return _digest(shapely.to_wkb(g))


def doubles_fp(values) -> int:
    """An array of doubles at the pixel precision (the last bits of a sum differ with the summation order)."""
    import numpy as np

    a = np.round(np.asarray(list(values), dtype="float64"), _PIXEL_DECIMALS) + 0.0
    return _digest(repr(a.shape).encode(), np.ascontiguousarray(a).tobytes())


def mvt_fp(blob) -> int:
    """A vector tile by what it holds: per layer its extent and its features (properties and geometry), unordered."""
    import mapbox_vector_tile

    tile = mapbox_vector_tile.decode(bytes(blob))
    parts = []
    for name in sorted(tile):
        layer = tile[name]
        feats = sorted(
            repr((sorted(f.get("properties", {}).items()), f.get("geometry")))
            for f in layer.get("features", [])
        )
        parts.append(repr((name, layer.get("extent"), feats)))
    return _digest("\n".join(parts).encode())


KINDS: Dict[str, Callable] = {
    "raster": raster_fp,
    "wkb": wkb_fp,
    "doubles": doubles_fp,
    "mvt": mvt_fp,
}


def udf_name(kind: str) -> str:
    return f"gbx_bench_vfp_{kind}"


def register(spark) -> None:
    """Register ``gbx_bench_vfp_<kind>`` for every kind (idempotent; a Spark Connect session ships the function)."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType

    for kind, fn in KINDS.items():

        def make(fn=fn):
            def one(s):
                return None if s is None else fn(s)

            return one

        one = make()

        @F.udf(returnType=LongType())
        def _udf(x, _one=one):
            return _one(x)

        spark.udf.register(udf_name(kind), _udf)
