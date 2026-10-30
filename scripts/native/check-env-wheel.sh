#!/bin/bash
# check-env-wheel.sh ENV VERSION: the install check of an env wheel, run by `gbx:build:wheel --verify` in that
# architecture's toolchain image (AlmaLinux 8, the manylinux_2_28 floor) with --network none. Mounted:
#   /dist/geobrix-<VERSION>+env<ENV>-py3-none-any.whl  the wheel
#   /wheelhouse                                         its core.txt wheels, fetched on the host
#   /wheelhouse-vizx                                    core.txt and extra-vizx.txt's wheels (check 9)
#   /env                                                python/geobrix/envs/env<ENV> (base.txt: the runtime's base)
# Each check prints `ok: <check>` or `CHECK FAILED: <what>`; the last line is
# `ENV_WHEEL_VERIFIED env=<ENV> arch=<arch> failures=<n>` and the exit status is 1 when any check failed.
#   1 a fresh cp312 venv installs the wheel offline, with the runtime's base versions as constraints, and every
#     base package it installs is at its base version, every one core.txt needs is installed, and one only an
#     extra names is listed (base_held.py)
#   2 the bindings (osgeo too) import before geobrix, through geobrix-native.pth alone, from the payload
#   3 without the .pth (GBX_NATIVE_PTH=0), `import databricks.labs.gbx` first activates the payload
#   4 CA: a user SSL_CERT_FILE survives `import rasterio` for GDAL and PROJ; without one, the system store
#   5 tripwire: the other environment's old extra name fails resolution, naming the other wheel
#   6 Python 3.11: pip refuses the wheel, and a forced install never activates the payload
#   7 selfcheck: _selfcheck.light() passes, every binding from the payload (GDAL's own osgeo and osgeo_utils too, and
#     osgeo_utils.gdal_calc and osgeo.gdal_array import), one libgdal mapped (rasterio's and osgeo's, the same
#     version), no foreign GDAL library mapped
#   8 natives: _selfcheck.natives() passes: netCDF-4 zstd/blosc/bzip2 through netCDF4 and GDAL (the filter plugins
#     registered with the payload's own netCDF-C, HDF5_PLUGIN_PATH unset here and in a child), OSTN15 against Ordnance
#     Survey's test points, OSGM15 and EGM96 heights, PROJ network off, GTiff JXL, REGEXP and muparser, libjpeg-turbo's
#     SIMD dispatcher, curl HTTP/2 with GDAL_HTTP_VERSION unset, projsync (its CDN listing is skipped here: --network none)
#   9 [vizx]: the extra installs offline (/wheelhouse-vizx) and brings no PyPI rasterio, pyogrio, pyproj, shapely,
#     geopandas or contextily (geopandas and contextily are vendored); after a vizx plot (a GeoDataFrame reprojected,
#     written and read back as GPKG, contextily's warp, gbx.vizx's plot_file and plot_static) the process maps one
#     libgdal, one libproj and one GEOS (libgeos_c), all the payload's: with the .pth, and with GBX_NATIVE_PTH=0
#     (activated at `import databricks.labs.gbx`; before it, geopandas does not import at all)
#  10 a runtime PROJ_DATA holding another PROJ's proj.db (DBR 19's /databricks/native/proj-data on a dedicated
#     cluster): pyproj from the payload still creates EPSG:4326, with PROJ_DATA the payload's share/proj
set -uo pipefail
ENV="$1"; VERSION="$2"; OTHER=$((11 - ENV)); ARCH="$(uname -m)"
PY=/opt/python/cp312-cp312/bin/python PY311=/opt/python/cp311-cp311/bin/python
WHL="/dist/geobrix-$VERSION+env$ENV-py3-none-any.whl"
FAILS=0
ok() { echo "ok: $*"; }
bad() { echo "CHECK FAILED: $*"; FAILS=$((FAILS + 1)); }
unset SSL_CERT_FILE CURL_CA_BUNDLE GDAL_CURL_CA_BUNDLE PROJ_CURL_CA_BUNDLE GDAL_DATA PROJ_DATA PROJ_LIB LD_LIBRARY_PATH HDF5_PLUGIN_PATH GDAL_HTTP_VERSION
pipq() { "$1" -m pip --disable-pip-version-check "${@:2}"; }

V=/tmp/v
if $PY -m venv "$V" && pipq "$V/bin/python" install -q --no-index --find-links /wheelhouse -c /env/base.txt "$WHL"; then
    # base_held.py: every base package installed is at its base version, every one core.txt needs is installed, and
    # one only an extra names (requests, held by [stac]) is not installed by the core install and is listed.
    got="$("$V/bin/python" /check/base_held.py /env 2>&1)"
    case "$got" in
        ok*) ok "1 offline install into a fresh venv, the runtime's base versions held ($(pipq "$V/bin/python" list 2>/dev/null | tail -n +3 | wc -l | tr -d ' ') distributions)${got#ok}" ;;
        *) bad "1 the runtime's base did not hold: $got" ;;
    esac
else
    bad "1 the wheel does not install offline from the wheelhouse"; echo "ENV_WHEEL_VERIFIED env=$ENV arch=$ARCH failures=$FAILS"; exit 1
fi

out="$("$V/bin/python" -c 'import sys, rasterio, shapely, pyproj, pyogrio, netCDF4, osgeo.gdal, _gbx_native_path as p
assert p.ACTIVE, "the .pth did not activate"
assert "databricks.labs.gbx" not in sys.modules
for m in (rasterio, shapely, pyproj, pyogrio, netCDF4, osgeo.gdal):
    assert "/databricks/labs/gbx/_vendor/" in m.__file__, m.__file__
site = next(i for i, x in enumerate(sys.path) if x.endswith("site-packages"))
vend = [i for i, x in enumerate(sys.path) if "/databricks/labs/gbx/_vendor/" in x]
assert vend == [site - 2, site - 1], (vend, site, sys.path)
print(rasterio.__gdal_version__, pyproj.proj_version_str, shapely.geos_version_string)' 2>&1)" \
    && ok "2 bindings before geobrix, by the .pth alone, from the payload, just before site-packages: GDAL/PROJ/GEOS $out" \
    || bad "2 .pth activation: $out"

out="$(GBX_NATIVE_PTH=0 "$V/bin/python" -c 'import _gbx_native_path as p
assert not p.ACTIVE
import databricks.labs.gbx
from databricks.labs.gbx import _native
import rasterio
assert _native.activated_by() == "import" and "/databricks/labs/gbx/_vendor/" in rasterio.__file__
print(_native.activated_by())' 2>&1)" \
    && ok "3 without the .pth, import databricks.labs.gbx first activates the payload (activated_by $out)" \
    || bad "3 import-time activation: $out"

echo "-----BEGIN CERTIFICATE-----" > /tmp/user-ca.pem
out="$(SSL_CERT_FILE=/tmp/user-ca.pem "$V/bin/python" -c 'import os, rasterio, pyogrio
g, p = os.environ.get("GDAL_CURL_CA_BUNDLE"), os.environ.get("PROJ_CURL_CA_BUNDLE")
assert g == p == "/tmp/user-ca.pem" and "CURL_CA_BUNDLE" not in os.environ, (g, p)
print(g)' 2>&1)" && sys="$("$V/bin/python" -c 'import os, rasterio; print(os.environ["GDAL_CURL_CA_BUNDLE"], os.environ["PROJ_CURL_CA_BUNDLE"])' 2>&1)" \
    && ok "4 CA: a user SSL_CERT_FILE survives import rasterio/pyogrio for GDAL and PROJ ($out); without one: $sys" \
    || bad "4 CA: $out $sys"

V2=/tmp/fresh; $PY -m venv "$V2"
for name in $([ "$ENV" = 6 ] && echo "light_env5 light_dbr17_all" || echo "light_env6 light_dbr19"); do
    out="$(pipq "$V2/bin/python" install --dry-run --no-index --find-links /dist --find-links /wheelhouse "geobrix[$name]==$VERSION+env$ENV" 2>&1)"
    if echo "$out" | grep -q ResolutionImpossible && echo "$out" | grep -q "geobrix==$VERSION+env$OTHER"; then
        ok "5 tripwire [$name]: ResolutionImpossible, $(echo "$out" | grep -m1 "depends on geobrix==" | sed 's/^ *//')"
    else
        bad "5 tripwire [$name]: $(echo "$out" | tail -3)"
    fi
done

V3=/tmp/v311; $PY311 -m venv "$V3"
out="$(pipq "$V3/bin/python" install --dry-run --no-deps --no-index "$WHL" 2>&1)"
echo "$out" | grep -qi "requires a different python" && ok "6a Python 3.11: pip refuses the wheel ($(echo "$out" | grep -i -m1 'requires a different python' | sed 's/^ *//'))" \
    || bad "6a Python 3.11 installs the wheel: $(echo "$out" | tail -2)"
if pipq "$V3/bin/python" install -q --no-deps --no-index --ignore-requires-python "$WHL" > /dev/null 2>&1; then
    out="$("$V3/bin/python" -c 'import sys, _gbx_native_path as p
assert not p.ACTIVE and not [x for x in sys.path if "_vendor" in x], sys.path
print(sys.implementation.cache_tag)' 2>&1)" && ok "6b Python 3.11, forced install: the payload stays inactive ($out)" || bad "6b Python 3.11: $out"
else
    bad "6b the forced install on Python 3.11 failed"
fi

out="$("$V/bin/python" - 2>&1 <<'PY'
import json, time
t = time.perf_counter()
import databricks.labs.gbx  # noqa: F401
imp = round(time.perf_counter() - t, 3)
from databricks.labs.gbx._native import _selfcheck as s
r = {"import_s": imp, "stack": s.stack(), "light": s.light(), "payload_mb": s.payload_size_mb(), "closure_mb": s.installed_closure_mb()}
st = r["stack"]
r["ok"] = (r["light"]["ok"] and all(st["from_payload"].values()) and st["foreign_libs"] == []
           and st["shared_libgdal"] and len(st["libgdal"]) == 1 and st["gdal_array"])
print("P8_RESULT " + json.dumps(r))
PY
)"
echo "$out" | grep -q '^P8_RESULT {"import_s".*"ok": true}$' && ok "7 selfcheck: _selfcheck.light() ok, 5/5 bindings + osgeo (gdal_array) + osgeo_utils (gdal_calc) from the payload, one libgdal, no foreign GDAL library" \
    || bad "7 selfcheck"
echo "$out" | grep '^P8_RESULT' || echo "$out" | tail -5

out="$("$V/bin/python" - 2>&1 <<'PY'
import json
import databricks.labs.gbx  # noqa: F401
from databricks.labs.gbx._native import _selfcheck as s
print("N5_RESULT " + json.dumps(s.natives()))
PY
)"
echo "$out" | grep -q '^N5_RESULT {"ok": true' && ok "8 natives: netCDF zstd/blosc/bzip2 via netCDF4 and GDAL, OSTN15/OSGM15/EGM96, JXL, REGEXP, muparser, JPEG SIMD, curl HTTP/2, projsync" \
    || bad "8 natives"
echo "$out" | grep '^N5_RESULT' || echo "$out" | tail -5

# 9: [vizx] in its own venv (the extra's packages from /wheelhouse-vizx, the base held but for VERIFY_UNHELD's
# packages: those the extra moves to a security fix, env5's requests).
if [ -n "${VERIFY_UNHELD:-}" ]; then grep -v -E "^($(echo "$VERIFY_UNHELD" | tr ' ' '|'))==" /env/base.txt; else cat /env/base.txt; fi > /tmp/base-vizx.txt
V4=/tmp/vizx
if $PY -m venv "$V4" && pipq "$V4/bin/python" install -q --no-index --find-links /wheelhouse --find-links /wheelhouse-vizx \
        -c /tmp/base-vizx.txt "geobrix[vizx] @ file://$WHL"; then
    pulled="$(pipq "$V4/bin/python" list --format=freeze 2>/dev/null | grep -i -E '^(rasterio|pyogrio|pyproj|shapely|geopandas|contextily|gdal)==' | tr '\n' ' ')"
    [ -z "$pulled" ] && ok "9a [vizx] installs offline and brings no PyPI GDAL-stack package ($(pipq "$V4/bin/python" list 2>/dev/null | tail -n +3 | wc -l | tr -d ' ') distributions)" \
        || bad "9a [vizx] installs PyPI's $pulled"
    cat > /tmp/vizx_probe.py <<'PY'
import json, os, re, sys, tempfile
import _gbx_native_path as p
before = None
if not p.ACTIVE:  # GBX_NATIVE_PTH=0: nothing GDAL-stack imports until geobrix activates the payload
    try:
        import geopandas  # noqa: F401
        before = "imported"
    except ModuleNotFoundError:
        before = "absent"
    import databricks.labs.gbx  # noqa: F401
import matplotlib
matplotlib.use("Agg")
import numpy as np, rasterio, geopandas as gpd, contextily as cx
from shapely.geometry import Point
from rasterio.transform import from_origin
from databricks.labs.gbx import vizx
d = tempfile.mkdtemp()
gdf = gpd.GeoDataFrame({"v": [1, 2, 3]}, geometry=[Point(-0.1, 51.5), Point(0.0, 51.4), Point(0.1, 51.6)], crs=4326)
gdf.to_crs(3857).to_file(f"{d}/p.gpkg")
back = gpd.read_file(f"{d}/p.gpkg")
img, ext = cx.warp_tiles(np.zeros((16, 16, 3), "uint8"), (0, 1600, 0, 1600), "EPSG:4326")
with rasterio.open(f"{d}/r.tif", "w", driver="GTiff", width=8, height=8, count=1, dtype="uint8", crs="EPSG:4326",
                   transform=from_origin(0, 8, 1, 1)) as ds:
    ds.write(np.arange(64, dtype="uint8").reshape(1, 8, 8))
vizx.plot_file(f"{d}/r.tif")
vizx.plot_static(back, basemap=False)
files = {l.split()[-1] for l in open("/proc/self/maps") if len(l.split()) > 5 and l.split()[-1].startswith("/")}
fam = {k: sorted(f for f in files if re.search(rf"/lib{k}(-[0-9a-f]{{8}})?\.so[.\d]*$", f)) for k in ("gdal", "proj", "geos_c")}
vendor = "/databricks/labs/gbx/_native/"
r = {"pth": p.ACTIVE, "geopandas_before_geobrix": before, "rows": len(back), "libs": fam,
     "from": {m: sys.modules[m].__file__ for m in ("geopandas", "contextily", "rasterio", "pyogrio", "pyproj", "shapely")}}
r["ok"] = (all(len(v) == 1 and vendor in v[0] for v in fam.values()) and len(back) == 3 and before in (None, "absent")
           and all("/databricks/labs/gbx/_vendor/" in f for f in r["from"].values()))
print("VIZX_RESULT " + json.dumps(r))
PY
    for mode in pth import; do
        out="$( (if [ $mode = import ]; then export GBX_NATIVE_PTH=0; fi; "$V4/bin/python" /tmp/vizx_probe.py) 2>&1)"
        if echo "$out" | grep -q '^VIZX_RESULT .*"ok": true}$'; then
            ok "9 [vizx] plot, activated by the $mode: one libgdal, one libproj, one GEOS, all the payload's ($(echo "$out" | grep -o '"libs": {[^}]*}' | sed 's#[^"]*/_native/##g'))"
        else
            bad "9 [vizx] plot, activated by the $mode: $(echo "$out" | grep '^VIZX_RESULT' || echo "$out" | tail -4)"
        fi
    done
else
    bad "9 [vizx] does not install offline from the wheelhouses"
fi

# 10: a foreign proj.db on PROJ_DATA: a copy of the payload's with another layout minor and PROJ.VERSION, as the
# runtime's own PROJ data is to the payload's PROJ.
out="$("$V/bin/python" - 2>&1 <<'PY'
import glob, os, shutil, sqlite3, subprocess, sys
src = glob.glob(f"{sys.prefix}/lib/python3.12/site-packages/databricks/labs/gbx/_native/share/proj/proj.db")[0]
os.makedirs("/tmp/foreign-proj", exist_ok=True)
db = shutil.copy(src, "/tmp/foreign-proj/proj.db")
con = sqlite3.connect(db)
con.execute("UPDATE metadata SET value = '1' WHERE key = 'DATABASE.LAYOUT.VERSION.MINOR'")
con.execute("UPDATE metadata SET value = '9.0.0' WHERE key = 'PROJ.VERSION'")
con.commit(); con.close()
probe = "import os, pyproj; print(pyproj.CRS.from_epsg(4326).to_epsg(), os.environ['PROJ_DATA'], pyproj.__file__)"
r = subprocess.run([sys.executable, "-c", probe], env={**os.environ, "PROJ_DATA": "/tmp/foreign-proj"},
                   capture_output=True, text=True)
print("PROJ10", r.stdout.strip() or r.stderr.strip()[-300:])
PY
)"
case "$out" in
    *"PROJ10 4326 "*"/_native/share/proj "*"/_vendor/"*) ok "10 PROJ_DATA with another PROJ's proj.db: the payload's share/proj serves, pyproj creates EPSG:4326" ;;
    *) bad "10 PROJ_DATA with another PROJ's proj.db: $(echo "$out" | tail -2)" ;;
esac

echo "ENV_WHEEL_VERIFIED env=$ENV arch=$ARCH failures=$FAILS"
[ "$FAILS" = 0 ]
