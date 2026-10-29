#!/bin/bash
# Sourced by build-deps.sh and build-gdal.sh inside the build container: the shared environment and the guard that
# turns an ignored CMake option into a build failure. Every version and file name comes from /recipes/pins.conf.

# shellcheck disable=SC1091
source /recipes/pins.conf

P=/opt/gbx-native
JOBS="$(nproc)"
mkdir -p "$P/lib" "$P/include" "$P/share/licenses" "$P/share/gbx"

# Everything is found under $P first; the linker records $P/lib so the tools built along the way run during the build
# (sqlite3 makes PROJ's database). Staging replaces every RUNPATH (set-rpath.py), so none of it ships.
export PKG_CONFIG_PATH="$P/lib/pkgconfig:$P/share/pkgconfig"
export CMAKE_PREFIX_PATH="$P"
export CPPFLAGS="-I$P/include"
# -g1: function names and line tables (enough to symbolize a crash backtrace), no local variables. It does not change
# the generated code. stage-payload.sh splits it into the debuginfo tarball and ships the stripped files; every file
# carries a build-id (the linker's default here), which is how a debugger finds its debug file.
export CFLAGS="-O2 -g1 -fPIC"
export CXXFLAGS="-O2 -g1 -fPIC"
export LDFLAGS="-L$P/lib -Wl,-rpath,$P/lib -Wl,--build-id=sha1"
export LIBRARY_PATH="$P/lib"

# check_unused <cmake log> <-DNAME=VALUE ...>: fail when cmake reports one of the given options as unused.
check_unused() {
    local log="$1" opt n unused; shift
    unused="$(awk '/Manually-specified variables were not used by the project/{f=1; next}
                   f && /^ +[A-Za-z0-9_]+ *$/ {gsub(/ /, ""); print; next}
                   f && NF == 0 {if (seen++) f = 0}' "$log")"
    for opt in "$@"; do
        case "$opt" in -D*) ;; *) continue ;; esac
        n="${opt#-D}"; n="${n%%=*}"; n="${n%%:*}"
        if printf '%s\n' "$unused" | grep -qx "$n"; then echo "BUILD FAILED: ${DEP:-cmake} ignored -D$n"; exit 1; fi
    done
}
