#!/bin/bash
# test-bindings.sh GDAL_NATIVE_DIR BINDINGS_DIR: the bindings probe. Run in the toolchain image with --network none and
#   /gdal-native (ro)        the exported stage-native tree the bindings were linked against
#   /bindings (rw)           build-bindings.sh's output (vendor/, dist/, build-seconds.txt); p11.json and test-results/ go here
#   /sdists (ro)             the five sdists: their test directories are the suites
#   /wheelhouse-test (ro)    the hashed test environment (test-requirements{,-newclick}-<arch>.txt)
#   /wheelhouse-pypi (ro)    the same five versions as PyPI wheels: the baseline
#   /verify (ro)             scripts/native
# It assembles the payload layout (`_native/<arch>/{lib,hdf5-plugins}`, `_native/share`, `_vendor/<arch>` side by side), checks the ELF
# closure of the vendored tree, makes three venvs (click 8.2.1; the newest click rasterio accepts; the PyPI wheels as the
# baseline), and p11_probe.py runs each binding's own suite against the vendored package and classifies every failure
# (flaky, click-attributable, environmental, regression). Prints `ok: <check>` / `TEST FAILED: <what>`.
set -euo pipefail

GN="${1:-/gdal-native}"
B="${2:-/bindings}"
PY=/opt/python/cp312-cp312/bin/python
ARCH="$(uname -m)"
T=/t
fail() { echo "TEST FAILED: $*"; exit 1; }
[ -d "$GN/lib" ] && [ -d "$B/vendor" ] || fail "no $GN/lib or $B/vendor"
# shellcheck disable=SC1091
source /verify/pins.conf

rm -rf "$T"
mkdir -p "$T/gbx/_native/$ARCH" "$T/gbx/_vendor" "$T/plug" "$T/work" "$T/src" "$T/distinfo"
cp -a "$GN/lib" "$T/gbx/_native/$ARCH/lib"
cp -a "$GN/hdf5-plugins" "$T/gbx/_native/$ARCH/hdf5-plugins"
cp -a "$GN/share" "$T/gbx/_native/share"
cp -a "$B/vendor" "$T/gbx/_vendor/$ARCH"
cp /verify/p11_probe.py "$T/plug/"
echo "ok: layout: _native/$ARCH/lib ($(ls "$T/gbx/_native/$ARCH/lib" | wc -l) files), _native/share, _vendor/$ARCH ($(ls "$T/gbx/_vendor/$ARCH" | tr '\n' ' '))"

"$PY" /verify/p11_probe.py closure --root "$T/gbx" --arch "$ARCH"

# the three environments; every one is installed from the hashed wheelhouse, offline
mkvenv() { # DIR LOCK
    "$PY" -m venv "$1"
    "$1/bin/python" -m pip install -q --disable-pip-version-check --no-index --find-links /wheelhouse-test --require-hashes -r "$2"
}
mkvenv "$T/venv" "/verify/test-requirements-$ARCH.txt"
mkvenv "$T/venv-newclick" "/verify/test-requirements-newclick-$ARCH.txt"
mkvenv "$T/venv-pypi" "/verify/test-requirements-$ARCH.txt"
"$T/venv-pypi/bin/python" -m pip install -q --disable-pip-version-check --no-index --no-deps /wheelhouse-pypi/*.whl
echo "ok: three venvs from the hashed wheelhouse: click $("$T/venv/bin/python" -c 'import click; print(click.__version__)') / $("$T/venv-newclick/bin/python" -c 'import click; print(click.__version__)') / PyPI wheels ($(ls /wheelhouse-pypi | cut -d- -f1,2 | tr '\n' ' '))"

# the metadata of our rasterio distribution, on the path beside the vendored code: the vendored tree has no dist-info
# (pip must never see it), so without this `rio` finds no entry points and every test_rio_* fails for that reason alone
rw="$(ls "$B"/dist/rasterio-*.whl)"
unzip -q -o "$rw" '*.dist-info/*' -d "$T/distinfo"

# the suites: each binding's own test directory, out of its sdist (pyproj, rasterio, netCDF4 with the config file that
# carries their pytest settings; shapely and pyogrio run from the installed package)
for dep in RASTERIO PYOGRIO PYPROJ SHAPELY NETCDF4; do
    f_var="${dep}_SDIST_FILE"; f="${!f_var}"; n="${dep,,}"
    mkdir -p "$T/src/$n" && tar -xzf "/sdists/$f" -C "$T/src/$n" --strip-components=1
done
mkdir -p "$T/work/rasterio" "$T/work/pyproj" "$T/work/netcdf4"
cp -a "$T/src/rasterio/tests" "$T/src/rasterio/pyproject.toml" "$T/work/rasterio/"
# rasterio's sdist ships no tests/data: take the data of the project's tag archive (prefetch.sh checked its sha256)
tar -xzf /sdists/rasterio-"$RASTERIO_VERSION"-testdata.tar.gz -C "$T/work/rasterio" --strip-components=1 --wildcards 'rasterio-*/tests/data'
[ -f "$T/work/rasterio/tests/data/RGB.byte.tif" ] || fail "rasterio tests/data is missing RGB.byte.tif"
cp -a "$T/src/pyproj/test" "$T/src/pyproj/pytest.ini" "$T/work/pyproj/"
cp -a "$T/src/netcdf4/test" "$T/src/netcdf4/pyproject.toml" "$T/work/netcdf4/"
echo "ok: test directories copied out of the sdists (rasterio tests/ with tests/data from the tag archive, pyproj test/, netCDF4 test/)"

exec "$PY" /verify/p11_probe.py run --arch "$ARCH" --native "$GN" --bindings "$B"
