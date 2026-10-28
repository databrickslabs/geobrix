#!/usr/bin/env python3
"""rust_native_smoke.py [--probe PATH] [--order bindings-first|probe-first]: a Rust extension built against the
payload (the probe crate rust/gbxrs-native-probe), rasterio and GDAL's osgeo run on ONE GDAL in one Python process.

The payload must be active when it starts, by any of: the env wheel installed (its .pth activates it at interpreter
start); GBX_NATIVE_ROOT set with geobrix's source on the path (the script imports ``_gbx_native_path``, which
activates it); or PYTHONPATH, GDAL_DATA and PROJ_DATA set by the caller (scripts/native/verify-rust-native.sh, in the
build container). ``--probe`` defaults to ``<payload>/_native/<arch>/rs/_gbx_native_probe.abi3.so``, the payload being
``$GBX_NATIVE_ROOT`` or the installed ``databricks/labs/gbx``. ``--order probe-first`` loads the probe (and so the
payload's libgdal) before rasterio and osgeo are imported; ``bindings-first`` (the default) after.

Linux only. Prints ``ok: <check>`` per check, then one JSON line with the evidence, and stops at the first
``SMOKE FAILED: <what>`` (exit 1). The last line of a good run is ``R1_SMOKE OK ...``:
  1 the probe's GDAL, GEOS and PROJ (the libraries it runs) are those rasterio, shapely and pyproj run;
  2 the process maps one libgdal, one libgeos_c and one libproj, all from one directory;
  3 rasterio writes a GeoTIFF to /vsimem and the probe reads it back with the same width, height and band sum, and so
    does osgeo: a /vsimem file lives inside one libgdal, so this is the proof that the three share it;
  4 the probe's GEOS buffer area equals shapely's within 1e-9;
  5 the probe's PROJ EPSG:27700 -> EPSG:4326 of (530000, 180000) equals pyproj's (always_xy) within 1e-9.
"""

import argparse
import importlib.machinery
import importlib.util
import json
import os
import platform
import re
import sys
import uuid

MODULE = "_gbx_native_probe"
FAMILIES = {
    name: re.compile(rf"/{name}(-[0-9a-f]{{8}})?\.so[.\d]*$")
    for name in ("libgdal", "libgeos_c", "libproj")
}
TOL = 1e-9


def fail(msg):
    print(f"SMOKE FAILED: {msg}")
    sys.exit(1)


def ok(msg):
    print(f"ok: {msg}")


def default_probe():
    root = os.environ.get("GBX_NATIVE_ROOT")
    if not root:
        spec = importlib.util.find_spec("databricks.labs.gbx")
        if spec is None or not spec.origin:
            fail(
                "no --probe, no GBX_NATIVE_ROOT and no installed geobrix to find the payload"
            )
        root = os.path.dirname(spec.origin)
    return os.path.join(root, "_native", platform.machine(), "rs", f"{MODULE}.abi3.so")


def load_probe(path):
    if not os.path.isfile(path):
        fail(f"no probe extension at {path}")
    loader = importlib.machinery.ExtensionFileLoader(MODULE, path)
    spec = importlib.util.spec_from_loader(MODULE, loader)
    mod = importlib.util.module_from_spec(spec)
    loader.exec_module(mod)
    return mod


def mapped():
    with open("/proc/self/maps") as fh:
        paths = {ln.split(None, 5)[-1].strip() for ln in fh if "/" in ln}
    return {n: sorted(p for p in paths if rx.search(p)) for n, rx in FAMILIES.items()}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--probe", help="the probe extension (default: in the payload)")
    ap.add_argument(
        "--order", choices=("bindings-first", "probe-first"), default="bindings-first"
    )
    args = ap.parse_args()
    if not sys.platform.startswith("linux"):
        fail("Linux only: the payload loads on Linux")
    try:
        import _gbx_native_path  # noqa: F401 - activates the payload of GBX_NATIVE_ROOT
    except ImportError:
        pass  # the env wheel's .pth already ran, or the caller set the environment
    for var in ("GDAL_DATA", "PROJ_DATA"):
        if not os.environ.get(var):
            fail(f"{var} is not set: the payload is not active")
    path = args.probe or default_probe()

    probe = load_probe(path) if args.order == "probe-first" else None
    import numpy as np
    import pyproj
    import rasterio
    from rasterio.transform import from_origin
    import shapely
    from osgeo import gdal

    gdal.UseExceptions()
    probe = probe or load_probe(path)
    ev = {"arch": platform.machine(), "order": args.order, "probe": path}

    # 1 the libraries the probe runs are the bindings'
    g, geos, proj = probe.versions()
    ev["versions"] = {
        "probe": {"gdal": g, "geos": geos, "proj": proj},
        "bindings": {
            "rasterio_gdal": rasterio.__gdal_version__,
            "osgeo_gdal": gdal.VersionInfo("RELEASE_NAME"),
            "shapely_geos": shapely.geos_version_string,
            "pyproj_proj": pyproj.proj_version_str,
        },
    }
    if not (
        g == rasterio.__gdal_version__ == gdal.VersionInfo("RELEASE_NAME")
        and geos.split("-")[0] == shapely.geos_version_string
        and proj == pyproj.proj_version_str
    ):
        fail(f"the probe runs other libraries than the bindings: {ev['versions']}")
    ok(f"versions: GDAL {g}, GEOS {geos}, PROJ {proj} (the probe = rasterio, osgeo, shapely, pyproj)")

    # 2 one copy of each library in the process
    maps = mapped()
    ev["maps"] = maps
    dirs = {os.path.dirname(p) for files in maps.values() for p in files}
    if any(len(files) != 1 for files in maps.values()) or len(dirs) != 1:
        fail(f"the process does not map one libgdal, libgeos_c and libproj from one directory: {maps}")
    ok(f"one libgdal, libgeos_c and libproj mapped, from {dirs.pop()}")

    # 3 the shared-GDAL proof: rasterio writes /vsimem, the probe and osgeo read it
    w, h = 37, 23
    data = (np.arange(w * h, dtype="uint16").reshape(h, w) * 7) % 1000
    vsi = f"/vsimem/r1-smoke-{uuid.uuid4().hex}.tif"
    with rasterio.open(
        vsi, "w", driver="GTiff", width=w, height=h, count=1, dtype="uint16", crs="EPSG:27700",
        transform=from_origin(530000, 180000, 10, 10),
    ) as dst:
        dst.write(data, 1)
    want = [w, h, float(data.sum(dtype=np.float64))]
    try:
        got = list(probe.gdal_vsimem_info(vsi))
    except RuntimeError as exc:
        fail(f"the probe cannot open {vsi}, which rasterio wrote (not one GDAL): {exc}")
    ds = gdal.Open(vsi)
    band = np.frombuffer(ds.GetRasterBand(1).ReadRaster(), dtype="uint16")
    via_osgeo = [ds.RasterXSize, ds.RasterYSize, float(band.sum(dtype=np.float64))]
    ds = None
    gdal.Unlink(vsi)
    ev["vsimem"] = {"path": vsi, "rasterio_wrote": want, "probe_read": got, "osgeo_read": via_osgeo}
    if got != want or via_osgeo != want:
        fail(f"/vsimem round trip differs: {ev['vsimem']}")
    ok(f"/vsimem: rasterio wrote {w}x{h} sum {want[2]:.0f}; the probe and osgeo read the same")

    # 4 GEOS
    geom = shapely.Polygon([(0, 0), (10, 0), (10, 5), (3, 8), (0, 5)])
    a_rs = probe.geos_buffer_area(shapely.to_wkb(geom), 1.5)
    a_py = shapely.buffer(geom, 1.5, quad_segs=16).area
    ev["geos"] = {"probe": a_rs, "shapely": a_py, "diff": abs(a_rs - a_py)}
    if abs(a_rs - a_py) > TOL:
        fail(f"GEOS buffer area: {ev['geos']}")
    ok(f"GEOS buffer area {a_rs!r} == shapely's (diff {abs(a_rs - a_py):.3g})")

    # 5 PROJ
    xr, yr = probe.proj_transform("EPSG:27700", "EPSG:4326", 530000.0, 180000.0)
    t = pyproj.Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)
    xp, yp = t.transform(530000.0, 180000.0)
    ev["proj"] = {"probe": [xr, yr], "pyproj": [xp, yp], "diff": [abs(xr - xp), abs(yr - yp)]}
    if abs(xr - xp) > TOL or abs(yr - yp) > TOL:
        fail(f"PROJ EPSG:27700 -> EPSG:4326: {ev['proj']}")
    ok(f"PROJ EPSG:27700 -> EPSG:4326 (530000, 180000) = ({xr!r}, {yr!r}) == pyproj's")

    if mapped() != maps:
        fail(f"the libraries mapped changed during the run: {mapped()}")
    print(json.dumps(ev))
    print(
        f"R1_SMOKE OK arch={ev['arch']} order={args.order} gdal={g} geos={geos} proj={proj} "
        f"libgdal={maps['libgdal'][0]}"
    )


if __name__ == "__main__":
    main()
