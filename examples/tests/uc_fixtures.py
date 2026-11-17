"""The fixture files the UC gate stages for the `{vol}` rows of the oracle tables (`gbx_rst_fromfile` reads them as FILEs).

Run where rasterio is installed (the dev container): `python3 examples/tests/uc_fixtures.py --out DIR` writes the files of
`FILES` into DIR. The gate stages DIR to `<volume folder>/fixtures/`. The files are deterministic, so a staged copy of the
right size is the right file. Their layout is described beside the rows that read them
(`bench/oracle_tables/raster_constructor.py`, `_UC_FROMFILE`).
"""

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
for _p in (REPO_ROOT / "python" / "geobrix" / "src", REPO_ROOT / "python" / "geobrix"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

FILES = ("cog.tif", "cog_prefixed.tif", "striped.tif", "notiff.bin")


def striped_bytes() -> bytes:
    """A 600 x 600 GeoTIFF in strips (not tiled), in the grid of cog.tif."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff", width=600, height=600, count=1, dtype="uint8", crs="EPSG:32614",
            transform=from_origin(400000, 4000000, 10, 10), tiled=False,
        ) as ds:
            ds.write(np.ones((1, 600, 600), "uint8"))
        return mf.read()


def build(out: Path) -> dict:
    """Write the fixture files into `out`; returns {name: size}."""
    from test._cog_fixture import cog_bytes

    from databricks.labs.gbx.bench.oracle_tables.raster_constructor import FIXTURE_PREFIX_BYTES

    out.mkdir(parents=True, exist_ok=True)
    cog = cog_bytes()
    prefix = (bytes(range(256)) * (FIXTURE_PREFIX_BYTES // 256 + 1))[:FIXTURE_PREFIX_BYTES]
    files = {
        "cog.tif": cog,
        "cog_prefixed.tif": prefix + cog,
        "striped.tif": striped_bytes(),
        "notiff.bin": b"these bytes are no TIFF\n" * 40,
    }
    for name, data in files.items():
        (out / name).write_bytes(data)
    return {name: len(data) for name, data in files.items()}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--out", required=True, help="folder to write the fixture files into")
    a = p.parse_args(argv)
    for name, size in build(Path(a.out)).items():
        print(f"{name}: {size} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
