#!/bin/bash
# check-env-wheel.sh ENV VERSION: the install check of an env wheel, run by `gbx:build:wheel --verify` in that
# architecture's toolchain image (AlmaLinux 8, the manylinux_2_28 floor) with --network none. Mounted:
#   /dist/geobrix-<VERSION>+env<ENV>-py3-none-any.whl  the wheel
#   /wheelhouse                                         its core.txt wheels, fetched on the host
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

echo "ENV_WHEEL_VERIFIED env=$ENV arch=$ARCH failures=$FAILS"
[ "$FAILS" = 0 ]
