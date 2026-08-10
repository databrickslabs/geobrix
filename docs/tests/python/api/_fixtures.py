"""
Shared test fixture generators for API documentation tests.

The committed .tif artifacts in src/test/resources/binary/ are the
canonical fixtures; the generator functions here document how they were
produced and can be re-run if the file needs to be regenerated.

NOTE: sample-data/Volumes/main/default/geobrix_samples/ is gitignored
(see .gitignore lines 40-41), so committed test fixtures live under
src/test/resources/binary/ instead.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Multiband GeoTIFF fixture
# ---------------------------------------------------------------------------

# Path relative to repo root — tracked in git (not gitignored).
# band 1 = red, band 2 = NIR, band 3 = green
MULTIBAND = "src/test/resources/binary/geotiff-small/rgb_nir_small.tif"


def make_multiband_fixture(path: str | Path | None = None) -> Path:
    """
    Generate a small (8x8) 3-band GeoTIFF suitable for band-math tests.

    Bands
    -----
    1 – red   : values 50–120  (low reflectance)
    2 – NIR   : values 100–200 (high reflectance → positive NDVI)
    3 – green : values 80–150  (intermediate)

    Metadata tags are written per-band so that rst_bandmetadata returns a
    non-empty map.  CRS is EPSG:4326; the affine transform places the tile
    over a small area in the North Sea (no overlap with production sample data).

    Parameters
    ----------
    path : optional override path.  Defaults to MULTIBAND (relative to the
           repo root, resolved from this file's location).

    Returns
    -------
    pathlib.Path pointing at the written file.
    """
    import numpy as np
    import rasterio
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds

    repo_root = Path(__file__).parents[4]  # docs/tests/python/api/ → 4 levels up
    dest = Path(path) if path else repo_root / MULTIBAND
    dest.parent.mkdir(parents=True, exist_ok=True)

    width, height = 8, 8
    # Affine transform: small area in the North Sea to avoid conflicts
    # Positional: from_bounds(west, south, east, north, width, height)
    transform = from_bounds(2.0, 55.0, 2.01, 55.01, width, height)

    # Distinct per-band data (uint16) — NDVI = (NIR-red)/(NIR+red)
    rng = np.random.default_rng(42)
    red = rng.integers(50, 120, size=(height, width), dtype=np.uint16)
    nir = rng.integers(100, 200, size=(height, width), dtype=np.uint16)
    green = rng.integers(80, 150, size=(height, width), dtype=np.uint16)

    profile = {
        "driver": "GTiff",
        "dtype": "uint16",
        "width": width,
        "height": height,
        "count": 3,
        "crs": CRS.from_epsg(4326),
        "transform": transform,
    }

    with rasterio.open(dest, "w", **profile) as ds:
        ds.write(red, 1)
        ds.write(nir, 2)
        ds.write(green, 3)
        # Per-band metadata so rst_bandmetadata returns a non-empty map
        ds.update_tags(1, name="red", wavelength_nm="665", band_index="1")
        ds.update_tags(2, name="nir", wavelength_nm="865", band_index="2")
        ds.update_tags(3, name="green", wavelength_nm="560", band_index="3")

    return dest


if __name__ == "__main__":
    out = make_multiband_fixture()
    print(f"Written: {out}")
