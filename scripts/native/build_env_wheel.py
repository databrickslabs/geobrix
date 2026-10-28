#!/usr/bin/env python3
"""Build geobrix-<version>+env<N>-py3-none-any.whl with both architectures' native payload.

Run by `gbx:build:wheel` (scripts/commands/gbx-build-wheel.sh) inside
`uv run --no-project --with-requirements python/geobrix/requirements-build.txt`, so `python -m build`,
setuptools and `wheel` are the hashed build requirements. No container lock (no dev container).

Inputs:
  build/native/out/<arch>/stage-native/gdal-native/  GDAL and the libraries it needs, MANIFEST.json, and
                                                      GDAL's own Python bindings (python/osgeo, osgeo_utils) (N3)
  build/native/out/<arch>/bindings/{vendor,wheels}/   the five bindings built against it (N4)
  python/geobrix/envs/env<N>/                         core.txt, extra-*.txt, vendor-pure.txt, base.txt
  python/geobrix/envs/pyproject.env.toml              the env wheels' pyproject (ruling NP-3)
  --vendor-pure <dir>  the vendor-pure.txt wheels, fetched and hash-checked on the host by the command
  --jar <path>         the GeoBrix JAR (shipped in _jar/, spec 3.6)
  --extra-payload <d>  a tree merged into the payload, only under _native/<arch>/ (the Rust kernels:
                       _native/<arch>/rs/_gbxrs.abi3.so); repeatable
  --allow-dirty        build although the inputs have uncommitted changes (GIT_DIRTY = True)

Before any step: the inputs exist, Docker runs and both toolchain images are there, and the inputs
under git (the package source, the env files, scripts/native) have no uncommitted change (else
`--allow-dirty`), so the wheel's GIT_SHA says what it was built from.

Steps (each prints `== <n> ...`; a failure prints `BUILD FAILED: <what>`, a missing input
`PRECONDITION MISSING: <what>`, and exits 1):
  1 a scratch tree: the package source, the template pyproject plus the extras and tripwires,
    env/ (the env files), and __version__ stamped `<version>+env<N>` (exactly one replacement);
  2 _build_info.py: ENV, VERSION, the GDAL/PROJ/GEOS versions from MANIFEST.json, the native
    revision, the git sha, whether the tree was dirty, SOURCE_DATE_EPOCH;
  3 the payload per architecture: _native/<arch>/lib (no pkgconfig/cmake, no symlinks) and
    MANIFEST.json, _native/share and _native/licenses (identical for both architectures, or fail),
    _vendor/<arch> (the five bindings, and gdal-native's osgeo with its RUNPATH set for that place
    by set-rpath.py in the architecture's toolchain image), _vendor/common/osgeo_utils (identical
    for both architectures, or fail), the extra payload, _jar/;
  4 the vendored pure-Python wheels into _vendor/common without their dist-info (licences to
    _vendor/licenses); every Requires-Dist of a vendored package must be another vendored package
    at a version it accepts, or a core.txt name whose base version (base.txt) or floor it accepts
    and whose cap is no higher than its own, or click at the base version;
  5 python -m build --wheel --no-isolation, with SOURCE_DATE_EPOCH = HEAD's commit time;
  6 wheel unpack, add geobrix-native.pth (`import _gbx_native_path`), wheel pack (same epoch, so the
    same tree gives the same bytes);
  7 verify the wheel (metadata against the env files, the payload, the JAR, the licence guard, the
    URLs in the metadata, the size) and extract its payload to build/env-wheel/env<N>/payload/,
    the GBX_NATIVE_ROOT layout;
  8 per architecture, in that architecture's toolchain image (--network none): auditwheel show on a
    throwaway wheel of _native/<arch> + _vendor/<arch> is consistent with manylinux_2_28_<arch> and
    names no external library (the closure check of the payload as packed).
Only then the wheel goes to dist/env<N>/, which keeps no other wheel.

`--verify <arch>` builds nothing: it installs dist/env<N>'s wheel in a fresh offline venv in that
architecture's toolchain image and runs scripts/native/check-env-wheel.sh (the install, the .pth and
import-time activation, the CA rule, the tripwire, Python 3.11, and probe P8's `_selfcheck`). The
venv's wheelhouse (core.txt with base.txt as constraints) is fetched on the host first, through
`--host-python`'s pip, its output redacted.
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
import zipfile
from email.parser import Parser
from importlib.metadata import version as installed_version
from pathlib import Path

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import Version

sys.path.insert(0, str(Path(__file__).resolve().parent))
from redact import redact  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
PYDIR = REPO / "python" / "geobrix"
PKG = Path("databricks/labs/gbx")
ARCHES = ("x86_64", "aarch64")
PLATFORMS = {"x86_64": "linux/amd64", "aarch64": "linux/arm64"}
TRIPWIRES = {  # spec 5.4: the other environment's old extra names -> geobrix==<version>+env<other>
    6: (
        "light_dbr17",
        "light_dbr18",
        "light_env5",
        "light_dbr17_all",
        "light_dbr18_all",
        "light_env5_all",
    ),
    5: ("light_dbr19", "light_env6", "light_dbr19_all", "light_env6_all"),
}
RELY_ON_BASE = {
    "click"
}  # spec 5.2: preinstalled on every runtime; core.txt may still declare it
GIT_INPUTS = (  # what the wheel is built from, under git
    "python/geobrix/src",
    "python/geobrix/envs",
    "python/geobrix/README.md",
    "python/geobrix/pyproject.toml",
    "python/geobrix/requirements-build.txt",
    "scripts/native",
)
SIZE_BUDGET_MB = 100  # the env wheels measured 82 MB (2026-10-28, osgeo included); 100 is 25% past 80
METADATA_URL_HOSTS = {"databricks.com", "github.com", "databrickslabs.github.io"}
URL = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://([^/\s\"'<>]+)")
BANNED = re.compile(
    re.search(
        r"^BANNED='([^']+)'",
        (REPO / "scripts/native/verify-native.sh").read_text(),
        re.M,
    ).group(1),
    re.I,
)
LIB_NAME = re.compile(r"\.so(\.|$)")
# GDAL's MIT licence covers osgeo and osgeo_utils except two scripts nothing here uses, which are
# left out: gdal_merge.py (LGPL-2.0-or-later) and gdal2tiles.py (it embeds JavaScript under CC BY 3.0
# NZ and Apache-2.0). No other file of theirs, binaries included, may name a GPL-family licence.
OSGEO_UTILS_LEFT_OUT = ("gdal_merge.py", "gdal2tiles.py")
GPL_TEXT = re.compile(
    rb"General Public License|\b[AL]?GPL(?:-|v| v)?[0-9]|SPDX-License-Identifier:[^\n]*GPL"
)
HASHED_NEED = re.compile(rb"lib[A-Za-z0-9_+]+-[0-9a-f]{8}\.so[.0-9]*")
PY312_LINUX = {
    "python_version": "3.12",
    "python_full_version": "3.12.3",
    "sys_platform": "linux",
    "platform_system": "Linux",
    "os_name": "posix",
    "implementation_name": "cpython",
    "platform_python_implementation": "CPython",
    "extra": "",
}


def fail(msg):
    print(f"BUILD FAILED: {msg}", flush=True)
    sys.exit(1)


def missing(msg):
    print(f"PRECONDITION MISSING: {msg}; gbx:build:wheel not run", flush=True)
    sys.exit(1)


def step(n, what):
    print(f"== {n} {what}", flush=True)


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def tree_digest(root):
    """relative path -> sha256 of every file under root (fails on a symlink: a wheel holds none)."""
    out = {}
    for p in sorted(Path(root).rglob("*")):
        if p.is_symlink():
            fail(f"{p} is a symlink (a wheel cannot hold one)")
        if p.is_file():
            out[p.relative_to(root).as_posix()] = sha256(p)
    return out


def reqs_of(path):
    lines = [ln.split("#", 1)[0].strip() for ln in Path(path).read_text().splitlines()]
    return [Requirement(ln) for ln in lines if ln]


def canon(r):
    extras = f"[{','.join(sorted(r.extras))}]" if r.extras else ""
    return f"{canonicalize_name(r.name)}{extras}{r.specifier}"


def wheel_metadata(whl):
    with zipfile.ZipFile(whl) as zf:
        name = next(
            n
            for n in zf.namelist()
            if n.endswith(".dist-info/METADATA") and n.count("/") == 1
        )
        return Parser().parsestr(zf.read(name).decode())


def run(cmd, log, env=None):
    with open(log, "w") as fh:
        res = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT, env=env)
    if res.returncode != 0:
        tail = Path(log).read_text(errors="replace").splitlines()[-25:]
        print(redact("\n".join(tail)))
        fail(f"{' '.join(map(str, cmd[:4]))} ... exited {res.returncode} (log: {log})")


def git(*a):
    res = subprocess.run(["git", "-C", str(REPO), *a], capture_output=True, text=True)
    return res.stdout.strip()


def source_version():
    """`__version__` as written in the package source."""
    text = (PYDIR / "src" / PKG / "__init__.py").read_text()
    return re.search(r'^__version__ = "([^"]+)"$', text, re.M).group(1)


def env_version(public, env):
    """The env wheel's version: the public version (pre-releases included) + `+env<N>`."""
    v = Version(public)
    if v.local:
        fail(f"__version__ {public} already has a local label")
    return f"{v}+env{env}"


def stamp_version(text, env):
    """`__init__.py` text with `__version__` stamped `<version>+env<N>` (exactly one replacement)."""
    m = re.findall(r'^__version__ = "([^"]+)"$', text, re.M)
    if len(m) != 1:
        fail(
            f"__init__.py has {len(m)} `__version__ = ...` lines; exactly one is stamped"
        )
    version = env_version(m[0], env)
    return (
        text.replace(f'__version__ = "{m[0]}"', f'__version__ = "{version}"'),
        version,
    )


def requires_python_subset(env_spec, core_spec):
    """True when every Python the env range admits, the core range admits too (and it admits one)."""
    env_set, core_set = SpecifierSet(env_spec), SpecifierSet(core_spec)
    probe = [Version(f"3.{m}.{p}") for m in range(6, 20) for p in (0, 3, 99)]
    admitted = [v for v in probe if env_set.contains(v)]
    return bool(admitted) and all(core_set.contains(v) for v in admitted)


def _next_release(v, keep):
    rel = list(v.release[:keep]) or [v.release[0]]
    rel[-1] += 1
    return Version(".".join(map(str, rel)))


def _bounds(spec):
    """(floor, cap) of a SpecifierSet: the version it starts from, the version it stays below
    (`<`, `~=`, `==` and `==X.*`; `<=` and `!=` are not used by the requirements checked here).
    """
    floor = cap = None
    for s in spec:
        wild = s.version.endswith(".*")
        v = Version(s.version[:-2] if wild else s.version)
        if s.operator in (">=", "==", "~="):
            floor = v if floor is None or v > floor else floor
        top = None
        if s.operator == "<":
            top = v
        elif s.operator == "~=":
            top = _next_release(v, len(v.release) - 1)
        elif s.operator == "==":
            top = _next_release(v, len(v.release)) if wild else v
        if top is not None:
            cap = top if cap is None or top < cap else cap
    return floor, cap


def check_against_core(name, req, core_req, base):
    """A vendored package's requirement on a core.txt (or rely-on-base) package, by version."""
    want = req.specifier
    if not want:
        return
    core_floor, core_cap = _bounds(core_req.specifier) if core_req else (None, None)
    start = base or core_floor
    if start is None:
        fail(
            f"vendored {name} requires {req}, and core.txt has no floor for it (nor base.txt a version)"
        )
    if not want.contains(start, prereleases=True):
        fail(
            f"vendored {name} requires {req}; the environment starts at {req.name} {start}"
        )
    _floor, cap = _bounds(want)
    if cap is not None and (core_req is None or core_cap is None or core_cap > cap):
        fail(
            f"vendored {name} requires {req}; core.txt lets pip install {req.name} at or above {cap}"
        )


def toolchain_image(bindings_dir, arch):
    """The toolchain image the bindings were built on (bindings/build.txt names its tag)."""
    rec = (
        (bindings_dir / "build.txt").read_text()
        if (bindings_dir / "build.txt").is_file()
        else ""
    )
    m = re.search(r"image=geobrix-native-build:bindings-([0-9a-f]{12})-" + arch, rec)
    if not m:
        missing(
            f"{bindings_dir}/build.txt names no bindings image (the toolchain the payload was built in)"
        )
    return f"geobrix-native-build:toolchain-{m.group(1)}-{arch}"


def check_docker(images):
    if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        missing(
            "Docker is not running (the auditwheel and install checks run in the toolchain images)"
        )
    for image in images:
        if (
            subprocess.run(
                ["docker", "image", "inspect", image], capture_output=True
            ).returncode
            != 0
        ):
            missing(
                f"the toolchain image {image} (gbx:build:native --target toolchain)"
            )


def check_inputs(args):
    natives, bindings = {}, {}
    for a in ARCHES:
        n = args.native_out / a / "stage-native" / "gdal-native"
        b = args.native_out / a / "bindings"
        if not (n / "MANIFEST.json").is_file():
            missing(
                f"{n}/MANIFEST.json (gbx:build:native --arch {a} --target stage-native)"
            )
        if not (b / "vendor").is_dir() or not list((b / "wheels").glob("*.whl")):
            missing(
                f"{b}/vendor and {b}/wheels (gbx:build:native --arch {a} --target bindings)"
            )
        natives[a], bindings[a] = n, b
    if not args.jar.is_file():
        missing(
            f"the JAR {args.jar} (build it with gbx:docker:exec 'mvn -B -q package -PskipScoverage -DskipTests', or pass --jar)"
        )
    for d in args.extra_payload:
        if not d.is_dir():
            missing(f"--extra-payload {d} is not a directory")
    check_docker([toolchain_image(bindings[a], a) for a in ARCHES])
    dirty = git("status", "--porcelain", "--", *GIT_INPUTS)
    if dirty and not args.allow_dirty:
        n = len(dirty.splitlines())
        fail(
            f"{n} uncommitted change(s) under {', '.join(GIT_INPUTS)}: commit them, or pass --allow-dirty"
        )
    epoch = git("log", "-1", "--format=%ct", "HEAD")
    if not epoch.isdigit():
        missing("git log -1 --format=%ct HEAD gave no commit time (SOURCE_DATE_EPOCH)")
    return natives, bindings, bool(dirty), int(epoch)


def build_tree(args, env, version):
    """Step 1: the scratch source tree with its pyproject."""
    tree = args.work / "tree"
    shutil.rmtree(tree, ignore_errors=True)
    ignore = shutil.ignore_patterns(
        "__pycache__", "*.pyc", "*.egg-info", "_build_info.py", "_vendor", "_jar"
    )
    shutil.copytree(PYDIR / "src", tree / "src", ignore=ignore)
    for a in ARCHES:  # the source tree never holds a payload; the build adds it
        if (tree / "src" / PKG / "_native" / a).exists():
            fail(f"python/geobrix/src/{PKG}/_native/{a} exists in the source tree")
    shutil.copy2(PYDIR / "README.md", tree / "README.md")

    envdir = PYDIR / "envs" / f"env{env}"
    (tree / "env").mkdir()
    for f in ["core.txt", *sorted(p.name for p in envdir.glob("extra-*.txt"))]:
        shutil.copy2(envdir / f, tree / "env" / f)
    other = 11 - env
    trip = tree / "env" / f"tripwire-env{other}.txt"
    trip.write_text(
        "# A light_* name of the other environment's family: this wheel is not the one to install.\n"
        f"geobrix=={version.split('+')[0]}+env{other}\n"
    )

    template = (PYDIR / "envs" / "pyproject.env.toml").read_text()
    committed = tomllib.loads((PYDIR / "pyproject.toml").read_text())["project"]
    mine = tomllib.loads(template)["project"]
    for key in ("name", "license", "authors"):
        if mine.get(key) != committed.get(key):
            fail(
                f"envs/pyproject.env.toml [project] {key} = {mine.get(key)!r}, the committed pyproject says {committed.get(key)!r}"
            )
    if not requires_python_subset(
        mine["requires-python"], committed["requires-python"]
    ):
        fail(
            f"requires-python {mine['requires-python']!r} admits a Python {committed['requires-python']!r} does not"
        )
    lines = [template.rstrip("\n"), ""]
    extras = sorted(p.stem.removeprefix("extra-") for p in envdir.glob("extra-*.txt"))
    for name in extras:
        lines.append(
            f'optional-dependencies.{name} = {{file = ["env/extra-{name}.txt"]}}'
        )
    for name in TRIPWIRES[env]:
        lines.append(f'optional-dependencies.{name} = {{file = ["env/{trip.name}"]}}')
    (tree / "pyproject.toml").write_text("\n".join(lines) + "\n")
    tomllib.loads((tree / "pyproject.toml").read_text())  # still TOML

    init = tree / "src" / PKG / "__init__.py"
    text, stamped = stamp_version(init.read_text(), env)
    if stamped != version:
        fail(f"stamped {stamped}, wanted {version}")
    init.write_text(text)
    return tree, extras


def write_build_info(tree, env, version, natives, dirty, epoch):
    """Step 2."""
    manifests = {
        a: json.loads((natives[a] / "MANIFEST.json").read_text()) for a in ARCHES
    }
    pick = {
        k: {a: manifests[a]["versions"][k] for a in ARCHES}
        for k in ("gdal", "proj", "geos")
    }
    pick["revision"] = {a: manifests[a]["native_revision"] for a in ARCHES}
    for k, per_arch in pick.items():
        if len(set(per_arch.values())) != 1:
            fail(f"the two architectures' MANIFEST.json disagree on {k}: {per_arch}")
    a0 = ARCHES[0]
    (tree / "src" / PKG / "_build_info.py").write_text(
        '"""Written by gbx:build:wheel into the env wheel (never in the source tree)."""\n\n'
        f"ENV = {env}\n"
        f'VERSION = "{version}"\n'
        f'GDAL_VERSION = "{pick["gdal"][a0]}"\n'
        f'PROJ_VERSION = "{pick["proj"][a0]}"\n'
        f'GEOS_VERSION = "{pick["geos"][a0]}"\n'
        f"NATIVE_REVISION = {pick['revision'][a0]}\n"
        f'GIT_SHA = "{git("rev-parse", "HEAD")}"\n'
        f"GIT_DIRTY = {dirty}\n"
        f"SOURCE_DATE_EPOCH = {epoch}\n"
    )


def copy_files(src, dst, skip_dirs=()):
    for p in sorted(Path(src).rglob("*")):
        rel = p.relative_to(src)
        if rel.parts and rel.parts[0] in skip_dirs:
            continue
        if p.is_symlink():
            fail(f"{p} is a symlink (a wheel cannot hold one)")
        if p.is_file():
            (dst / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, dst / rel)


def binding_licences(wheels_dir):
    """<binding> -> {file name: bytes} from each binding wheel's dist-info."""
    out = {}
    for whl in sorted(Path(wheels_dir).glob("*.whl")):
        with zipfile.ZipFile(whl) as zf:
            name = canonicalize_name(whl.name.split("-")[0])
            for n in zf.namelist():
                parts = n.split("/")
                if parts[0].endswith(".dist-info") and re.match(
                    r"(LICEN[CS]E|COPYING|AUTHORS|NOTICE)", parts[-1], re.I
                ):
                    out.setdefault(name, {})[
                        "/".join(parts[1:]).removeprefix("licenses/")
                    ] = zf.read(n)
    return out


def set_rpath(bindings_dir, arch, tree, tree_at):
    """RUNPATH of every ELF file under ``tree`` (which sits at ``tree_at`` in the wheel) relative to
    _native/<arch>/lib: set-rpath.py in the architecture's toolchain image (patchelf), --network none."""
    image = toolchain_image(bindings_dir, arch)
    res = subprocess.run(
        ["docker", "run", "--rm", "--network", "none", "--platform", PLATFORMS[arch],
         "--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{tree}:/tree",
         "-v", f"{REPO / 'scripts' / 'native'}:/recipes:ro", image,
         "/opt/python/cp312-cp312/bin/python", "/recipes/set-rpath.py",
         "--tree", "/tree", "--tree-at", tree_at, "--lib-at", f"_native/{arch}/lib"],
        capture_output=True, text=True,
    )  # fmt: skip
    if res.returncode != 0:
        print(redact((res.stdout + res.stderr).strip()))
        fail(f"set-rpath.py on {tree_at} (in {image})")


def add_osgeo(natives, bindings, vendor, a):
    """GDAL's own bindings from gdal-native: osgeo into _vendor/<arch> beside the five."""
    if (vendor / "osgeo").exists():
        fail(f"{bindings[a]}/vendor already holds osgeo")
    copy_files(natives[a] / "python" / "osgeo", vendor / "osgeo")
    set_rpath(bindings[a], a, vendor / "osgeo", f"_vendor/{a}/osgeo")


def add_osgeo_utils(natives, pkg):
    """osgeo_utils (pure Python, identical for both architectures) into _vendor/common."""
    trees = {a: tree_digest(natives[a] / "python" / "osgeo_utils") for a in ARCHES}
    if trees[ARCHES[0]] != trees[ARCHES[1]]:
        fail("python/osgeo_utils differs between the architectures")
    dst = pkg / "_vendor" / "common" / "osgeo_utils"
    copy_files(natives[ARCHES[0]] / "python" / "osgeo_utils", dst)
    for name in OSGEO_UTILS_LEFT_OUT:
        (dst / name).unlink(missing_ok=True)
    for root in (dst, *(pkg / "_vendor" / a / "osgeo" for a in ARCHES)):
        files = sorted(p for p in root.rglob("*") if p.is_file())
        gpl = [p.relative_to(pkg) for p in files if GPL_TEXT.search(p.read_bytes())]
        if gpl:
            fail(f"{gpl} name a GPL-family licence (GDAL's MIT licence does not cover them)")


def add_payload(args, tree, natives, bindings, version):
    """Step 3."""
    pkg = tree / "src" / PKG
    nat = pkg / "_native"
    shares = {a: tree_digest(natives[a] / "share") for a in ARCHES}
    if shares[ARCHES[0]] != shares[ARCHES[1]]:
        diff = sorted(set(shares[ARCHES[0]].items()) ^ set(shares[ARCHES[1]].items()))[
            :5
        ]
        fail(f"share/ differs between the architectures, e.g. {diff}")
    lics = {a: tree_digest(natives[a] / "licenses") for a in ARCHES}
    blics = {a: binding_licences(bindings[a] / "wheels") for a in ARCHES}
    if lics[ARCHES[0]] != lics[ARCHES[1]] or blics[ARCHES[0]] != blics[ARCHES[1]]:
        fail("the licence files differ between the architectures")
    for a in ARCHES:
        manifest = json.loads((natives[a] / "MANIFEST.json").read_text())
        lib = nat / a / "lib"
        copy_files(natives[a] / "lib", lib, skip_dirs=("pkgconfig", "cmake"))
        shutil.copy2(natives[a] / "MANIFEST.json", nat / a / "MANIFEST.json")
        want = set(manifest["sonames"].values()) | {manifest["jni"]}
        have = {p.name for p in lib.iterdir()}
        if want != have:
            fail(
                f"{a}: lib/ holds {sorted(have ^ want)} beyond or short of MANIFEST.json's libraries"
            )
        vendor = pkg / "_vendor" / a
        copy_files(bindings[a] / "vendor", vendor)
        if list(vendor.rglob("*.dist-info")):
            fail(f"{bindings[a]}/vendor holds a dist-info")
        add_osgeo(natives, bindings, vendor, a)
        # The bindings record hashed SONAMEs, which change with every gdal-native rebuild: a binding
        # tree linked against another build names libraries this lib/ does not have.
        for so in vendor.rglob("*.so"):
            stale = {m.decode() for m in HASHED_NEED.findall(so.read_bytes())} - have
            if stale:
                fail(
                    f"{so.relative_to(pkg)} needs {sorted(stale)}, not in {a}/lib: the bindings were built against another gdal-native"
                )
    add_osgeo_utils(natives, pkg)
    copy_files(natives[ARCHES[0]] / "share", nat / "share")
    copy_files(natives[ARCHES[0]] / "licenses", nat / "licenses")
    for name, files in blics[ARCHES[0]].items():
        for rel, data in files.items():
            (nat / "licenses" / name / rel).parent.mkdir(parents=True, exist_ok=True)
            (nat / "licenses" / name / rel).write_bytes(data)
    # osgeo and osgeo_utils are GDAL's: its licence, listed under their names like the bindings'
    gdal_licence = nat / "licenses" / "gdal" / "LICENSE.TXT"
    if not gdal_licence.is_file():
        fail("gdal-native/licenses/gdal/LICENSE.TXT is missing")
    for name in ("osgeo", "osgeo_utils"):
        (nat / "licenses" / name).mkdir()
        shutil.copy2(gdal_licence, nat / "licenses" / name / "LICENSE.TXT")

    for d in args.extra_payload:
        for p in sorted(d.rglob("*")):
            if not p.is_file():
                continue
            rel = p.relative_to(d)
            if (
                len(rel.parts) < 3
                or rel.parts[0] != "_native"
                or rel.parts[1] not in ARCHES
                or rel.parts[2] in ("lib", "MANIFEST.json")
            ):
                fail(
                    f"--extra-payload {d}: {rel} is not under _native/<arch>/ (beside lib/)"
                )
            if (pkg / rel).exists():
                fail(f"--extra-payload {d}: {rel} is already in the payload")
            (pkg / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(p, pkg / rel)
    jar = pkg / "_jar" / f"geobrix-{version.split('+')[0]}-jar-with-dependencies.jar"
    jar.parent.mkdir(parents=True)
    with zipfile.ZipFile(args.jar) as zf:
        if (
            "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister"
            not in zf.namelist()
        ):
            fail(
                f"{args.jar} has no Spark DataSourceRegister service file: not the GeoBrix JAR"
            )
    shutil.copy2(args.jar, jar)


def pins_of(path):
    """name -> Version of the `name==version` lines of a requirements file."""
    return {
        canonicalize_name(r.name): Version(next(iter(r.specifier)).version)
        for r in reqs_of(path)
    }


def check_requirements(env, vendored):
    """Every vendored package's Requires-Dist, against the vendored set, core.txt and base.txt."""
    envdir = PYDIR / "envs" / f"env{env}"
    core = {canonicalize_name(r.name): r for r in reqs_of(envdir / "core.txt")}
    base = pins_of(envdir / "base.txt")
    # The base pins are what the runtime has: core.txt must admit them.
    for dep, v in base.items():
        if dep in core and not core[dep].specifier.contains(v, prereleases=True):
            fail(
                f"env{env}/core.txt {core[dep]} does not admit the runtime's {dep} {v} (base.txt)"
            )
    for name, (_v, meta) in sorted(vendored.items()):
        for line in meta.get_all("Requires-Dist") or []:
            r = Requirement(line)
            if r.marker is not None and not r.marker.evaluate(PY312_LINUX):
                continue
            dep = canonicalize_name(r.name)
            if dep in vendored:
                if not r.specifier.contains(vendored[dep][0], prereleases=True):
                    fail(
                        f"vendored {name} requires {r}, the vendored {dep} is {vendored[dep][0]}"
                    )
            elif dep in core or dep in RELY_ON_BASE:
                check_against_core(name, r, core.get(dep), base.get(dep))
            else:
                fail(
                    f"vendored {name} requires {r}: neither vendored, in env{env}/core.txt, nor preinstalled (click)"
                )


def add_vendor_pure(args, tree, env, bindings):
    """Step 4."""
    envdir = PYDIR / "envs" / f"env{env}"
    pkg = tree / "src" / PKG
    common, licdir = pkg / "_vendor" / "common", pkg / "_vendor" / "licenses"
    pins = {}
    for line in (envdir / "vendor-pure.txt").read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            spec, digest = line.split(" --hash=sha256:")
            name, ver = spec.split("==")
            pins[canonicalize_name(name)] = (ver, digest.strip())
    vendored = {}  # canonical name -> (version, metadata)
    for name, (ver, digest) in pins.items():
        hits = [
            w
            for w in args.vendor_pure.glob("*.whl")
            if canonicalize_name(w.name.split("-")[0]) == name
            and w.name.split("-")[1] == ver
        ]
        if len(hits) != 1:
            missing(
                f"{args.vendor_pure}: one {name}=={ver} wheel (found {[h.name for h in hits]})"
            )
        whl = hits[0]
        if sha256(whl) != digest:
            fail(f"{whl.name} sha256 {sha256(whl)}, vendor-pure.txt says {digest}")
        if not whl.name.endswith("-py3-none-any.whl"):
            fail(f"{whl.name} is not a pure-Python wheel")
        with zipfile.ZipFile(whl) as zf:
            for n in zf.namelist():
                top = n.split("/")[0]
                if top.endswith(".dist-info"):
                    if re.match(
                        r"(LICEN[CS]E|COPYING|AUTHORS|NOTICE)", n.split("/")[-1], re.I
                    ):
                        out = licdir / name / n.split("/")[-1]
                        out.parent.mkdir(parents=True, exist_ok=True)
                        out.write_bytes(zf.read(n))
                    continue
                if "/" not in n and not n.endswith(".py"):
                    continue  # a top-level README or CHANGELOG: not importable, not shipped
                if not n.endswith("/"):
                    (common / n).parent.mkdir(parents=True, exist_ok=True)
                    (common / n).write_bytes(zf.read(n))
        vendored[name] = (Version(ver), wheel_metadata(whl))
    for whl in sorted(bindings[ARCHES[0]].joinpath("wheels").glob("*.whl")):
        meta = wheel_metadata(whl)
        vendored[canonicalize_name(meta["Name"])] = (Version(meta["Version"]), meta)

    check_requirements(env, vendored)
    return sorted(vendored)


def build_wheel(args, tree, epoch):
    """Step 5."""
    for pkg_name in ("build", "setuptools", "wheel"):
        pinned = re.search(
            rf"^{pkg_name}==(\S+)", (PYDIR / "requirements-build.txt").read_text(), re.M
        ).group(1)
        if installed_version(pkg_name) != pinned:
            fail(
                f"{pkg_name} {installed_version(pkg_name)} is running, requirements-build.txt pins {pinned} (run through gbx:build:wheel)"
            )
    out = args.work / "build-out"
    shutil.rmtree(out, ignore_errors=True)
    env = {**os.environ, "SOURCE_DATE_EPOCH": str(epoch)}
    run(
        [
            sys.executable,
            "-m",
            "build",
            "--wheel",
            "--no-isolation",
            "--outdir",
            str(out),
            str(tree),
        ],
        args.work / "build.log",
        env,
    )
    wheels = list(out.glob("*.whl"))
    if len(wheels) != 1:
        fail(f"python -m build made {[w.name for w in wheels]}, wanted one wheel")
    return wheels[0]


def add_pth(args, whl, epoch):
    """Step 6."""
    unpacked, final = args.work / "unpacked", args.work / "final"
    for d in (unpacked, final):
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True)
    env = {**os.environ, "SOURCE_DATE_EPOCH": str(epoch)}
    run(
        [sys.executable, "-m", "wheel", "unpack", str(whl), "-d", str(unpacked)],
        args.work / "unpack.log",
        env,
    )
    (root,) = unpacked.iterdir()
    (root / "geobrix-native.pth").write_text("import _gbx_native_path\n")
    run(
        [sys.executable, "-m", "wheel", "pack", str(root), "-d", str(final)],
        args.work / "pack.log",
        env,
    )
    (packed,) = final.glob("*.whl")
    return packed


def norm_extra(name):
    return re.sub(r"[-_.]+", "-", name).lower()  # PEP 685


def verify_metadata(zf, env, version, extras):
    envdir = PYDIR / "envs" / f"env{env}"
    other = 11 - env
    meta_text = zf.read(f"geobrix-{version}.dist-info/METADATA").decode()
    meta = Parser().parsestr(meta_text)
    if meta["Version"] != version:
        fail(f"METADATA Version {meta['Version']}, wanted {version}")
    template = tomllib.loads((PYDIR / "envs" / "pyproject.env.toml").read_text())[
        "project"
    ]
    if SpecifierSet(meta["Requires-Python"] or "") != SpecifierSet(
        template["requires-python"]
    ):
        fail(
            f"METADATA Requires-Python {meta['Requires-Python']}, the template says {template['requires-python']}"
        )
    unmarked, by_extra = set(), {}
    for line in meta.get_all("Requires-Dist") or []:
        r = Requirement(line)
        m = re.fullmatch(r'extra == "([^"]+)"', str(r.marker or ""))
        bucket = by_extra.setdefault(norm_extra(m.group(1)), set()) if m else unmarked
        bucket.add(canon(r))
    want = {canon(r) for r in reqs_of(envdir / "core.txt")}
    if unmarked != want:
        fail(
            f"METADATA Requires-Dist differs from env{env}/core.txt: {sorted(unmarked ^ want)}"
        )
    declared = {norm_extra(e) for e in meta.get_all("Provides-Extra") or []}
    if declared != {norm_extra(e) for e in (*extras, *TRIPWIRES[env])}:
        fail(f"Provides-Extra {sorted(declared)}")
    for name in extras:
        want = {canon(r) for r in reqs_of(envdir / f"extra-{name}.txt")}
        if by_extra.get(norm_extra(name)) != want:
            fail(f"extra [{name}] differs from extra-{name}.txt")
    trip = {f"geobrix=={version.split('+')[0]}+env{other}"}
    for name in TRIPWIRES[env]:
        if by_extra.get(norm_extra(name)) != trip:
            fail(f"tripwire [{name}] is {by_extra.get(norm_extra(name))}")
    if {norm_extra(n) for n in TRIPWIRES[other]} & set(by_extra):
        fail("a same-env old extra name is declared")
    hosts = {h.lower() for h in URL.findall(meta_text)}
    if not hosts <= METADATA_URL_HOSTS:
        n = len(hosts - METADATA_URL_HOSTS)
        fail(f"METADATA names {n} host(s) outside the project's own")


def verify_payload(zf, version, epoch):
    names = set(zf.namelist())
    for a in ARCHES:
        manifest = json.loads(zf.read(f"{PKG}/_native/{a}/MANIFEST.json"))
        for lib in [*manifest["sonames"].values(), manifest["jni"]]:
            if f"{PKG}/_native/{a}/lib/{lib}" not in names:
                fail(f"{a}: {lib} is not in the wheel")
        for binding in ("rasterio", "osgeo"):
            if f"{PKG}/_vendor/{a}/{binding}/__init__.py" not in names:
                fail(f"{a}: the vendored {binding} is not in the wheel")
    for need in (
        f"{PKG}/_vendor/common/osgeo_utils/gdal_calc.py",
        f"{PKG}/_native/licenses/osgeo/LICENSE.TXT",
        f"{PKG}/_native/licenses/osgeo_utils/LICENSE.TXT",
        f"{PKG}/_jar/geobrix-{version.split('+')[0]}-jar-with-dependencies.jar",
        "geobrix-native.pth",
        "_gbx_native_path.py",
        f"{PKG}/_build_info.py",
        f"{PKG}/_native/share/proj/proj.db",
        f"{PKG}/_native/__init__.py",
        f"{PKG}/_native/_rules.py",
        f"{PKG}/_native/_selfcheck.py",
    ):
        if need not in names:
            fail(f"{need} is not in the wheel")
    inner = [
        n
        for n in names
        if n.startswith(f"{PKG}/_native/") and n.endswith("/__init__.py")
    ]
    if inner != [f"{PKG}/_native/__init__.py"]:
        fail(f"the wheel has an __init__.py inside the payload: {inner}")
    leaf = lambda n: n.rsplit("/", 1)[-1]  # noqa: E731
    banned = [n for n in names if LIB_NAME.search(leaf(n)) and BANNED.search(leaf(n))]
    if banned:
        fail(f"the licence guard matches {banned}")
    left_in = [
        n
        for n in names
        if n.startswith(f"{PKG}/_vendor/common/osgeo_utils/")
        and leaf(n) in OSGEO_UTILS_LEFT_OUT
    ]
    if left_in:
        fail(f"{left_in} is in the wheel (GDAL's MIT licence does not cover it)")
    if any(re.search(r"(pkgconfig|cmake)/", n) for n in names):
        fail("the wheel holds pkgconfig or cmake files")
    stamp = zip_date_time(epoch)
    off = [i.filename for i in zf.infolist() if i.date_time != stamp]
    if off:
        fail(
            f"{len(off)} member(s) are not stamped SOURCE_DATE_EPOCH {epoch}, e.g. {off[:3]}"
        )


def zip_date_time(epoch):
    """The zip timestamp `wheel` writes for SOURCE_DATE_EPOCH: UTC, seconds rounded down to even."""
    import time

    t = time.gmtime(epoch)[:6]
    return (*t[:5], t[5] - t[5] % 2)


def extract_payload(zf, payload):
    """The wheel's _native, _vendor and _jar, as GBX_NATIVE_ROOT expects them."""
    shutil.rmtree(payload, ignore_errors=True)
    for n in zf.namelist():
        if n.endswith("/") or not n.startswith(
            (f"{PKG}/_native/", f"{PKG}/_vendor/", f"{PKG}/_jar/")
        ):
            continue
        if Path(n).parent == PKG / "_native" and n.endswith(".py"):  # the loader's code
            continue
        out = payload / Path(n).relative_to(PKG)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(zf.read(n))


def verify(args, whl, env, version, extras, epoch):
    """Step 7: returns the wheel's size in MB."""
    if whl.name != f"geobrix-{version}-py3-none-any.whl":
        fail(f"the wheel is {whl.name}, wanted geobrix-{version}-py3-none-any.whl")
    with zipfile.ZipFile(whl) as zf:
        verify_metadata(zf, env, version, extras)
        verify_payload(zf, version, epoch)
        extract_payload(zf, args.work / "payload")
    size = whl.stat().st_size / 1e6
    if size > SIZE_BUDGET_MB:
        fail(f"the wheel is {size:.1f} MB, over the {SIZE_BUDGET_MB} MB budget")
    return size


AUDITWHEEL_SCRIPT = r"""
set -euo pipefail
A="$1"; W=/tmp/aw/gbxprobe-0
mkdir -p "$W/gbxprobe/_native" "$W/gbxprobe/_vendor" "$W/gbxprobe-0.dist-info"
cp -a "/payload/_native/$A" "$W/gbxprobe/_native/$A"
cp -a "/payload/_vendor/$A" "$W/gbxprobe/_vendor/$A"
: > "$W/gbxprobe/__init__.py"
printf 'Metadata-Version: 2.1\nName: gbxprobe\nVersion: 0\n' > "$W/gbxprobe-0.dist-info/METADATA"
printf 'Wheel-Version: 1.0\nGenerator: gbx-build-wheel\nRoot-Is-Purelib: false\nTag: cp312-cp312-linux_%s\n' "$A" > "$W/gbxprobe-0.dist-info/WHEEL"
$PY -m wheel pack "$W" --dest-dir /tmp/aw > /dev/null
auditwheel show --json "/tmp/aw/gbxprobe-0-cp312-cp312-linux_$A.whl" > /tmp/aw/aw.json 2> /dev/null
$PY - "$A" <<'EOF'
import json, sys
r = json.load(open("/tmp/aw/aw.json"))
tag = f"manylinux_2_28_{sys.argv[1]}"
ok = r.get("overall_tag") == tag and not r.get("external_libs")
print(("ok: " if ok else "BUILD FAILED: ") + f"auditwheel {sys.argv[1]}: overall_tag {r.get('overall_tag')}, external libraries {sorted(r.get('external_libs') or [])}")
sys.exit(0 if ok else 1)
EOF
"""


def auditwheel(args, bindings):
    """Step 8."""
    for a in ARCHES:
        image = toolchain_image(bindings[a], a)
        res = subprocess.run(
            ["docker", "run", "--rm", "--network", "none", "--platform", PLATFORMS[a],
             "-v", f"{args.work / 'payload'}:/payload:ro", image, "bash", "-c", AUDITWHEEL_SCRIPT, "aw", a],
            capture_output=True, text=True,
        )  # fmt: skip
        out = (res.stdout + res.stderr).strip()
        print("    " + out.replace("\n", "\n    "))
        if res.returncode != 0:
            fail(f"auditwheel {a} (in {image})")


def verify_install(args, env, version, arch):
    """`--verify <arch>`: the install check of the built wheel in that architecture's toolchain image."""
    whl = args.out / f"geobrix-{version}-py3-none-any.whl"
    if not whl.is_file():
        missing(f"{whl} (build it: gbx:build:wheel --env {env})")
    bindings = args.native_out / arch / "bindings"
    image = toolchain_image(bindings, arch)
    check_docker([image])
    envdir = PYDIR / "envs" / f"env{env}"
    house = REPO / "build" / "env-wheel" / "wheelhouse" / f"env{env}" / arch
    key = hashlib.sha256(
        (envdir / "core.txt").read_bytes() + (envdir / "base.txt").read_bytes()
    ).hexdigest()
    if (house / ".inputs-sha256").is_file() and (
        house / ".inputs-sha256"
    ).read_text().strip() == key:
        print(f"    wheelhouse {house}: current", flush=True)
    else:
        print(
            f"    wheelhouse {house}: fetching core.txt with base.txt as constraints (host pip)",
            flush=True,
        )
        shutil.rmtree(house, ignore_errors=True)
        house.mkdir(parents=True)
        res = subprocess.run(
            [args.host_python, "-m", "pip", "download", "-q", "--disable-pip-version-check",
             "-r", str(envdir / "core.txt"), "-c", str(envdir / "base.txt"), "--only-binary", ":all:",
             "--platform", f"manylinux_2_28_{arch}", "--platform", f"manylinux2014_{arch}",
             "--python-version", "3.12", "-d", str(house)],
            capture_output=True, text=True,
        )  # fmt: skip
        if res.returncode != 0:
            print(redact((res.stdout + res.stderr)[-3000:]))
            fail(
                f"pip download of the env{env} wheelhouse for {arch} exited {res.returncode}"
            )
        (house / ".inputs-sha256").write_text(key + "\n")
    res = subprocess.run(
        ["docker", "run", "--rm", "--network", "none", "--platform", PLATFORMS[arch],
         "-v", f"{args.out}:/dist:ro", "-v", f"{house}:/wheelhouse:ro", "-v", f"{envdir}:/env:ro",
         "-v", f"{REPO / 'scripts' / 'native'}:/check:ro", image,
         "bash", "/check/check-env-wheel.sh", str(env), version.split("+")[0]],
        capture_output=True, text=True,
    )  # fmt: skip
    out = (res.stdout + res.stderr).strip()
    report = args.work / f"verify-{arch}.txt"
    report.write_text(out + "\n")
    print("    " + redact(out).replace("\n", "\n    "))
    if res.returncode != 0:
        fail(f"the install check of {whl.name} on {arch} (report: {report})")


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--env", type=int, choices=(5, 6), required=True)
    p.add_argument("--jar", type=Path)
    p.add_argument("--vendor-pure", type=Path)
    p.add_argument("--extra-payload", type=Path, action="append", default=[])
    p.add_argument("--allow-dirty", action="store_true")
    p.add_argument("--verify", choices=ARCHES)
    p.add_argument("--host-python", default="python3")
    p.add_argument("--native-out", type=Path, default=REPO / "build" / "native" / "out")
    p.add_argument("--work", type=Path)
    p.add_argument("--out", type=Path)
    args = p.parse_args()
    env = args.env
    args.work = (args.work or REPO / "build" / "env-wheel" / f"env{env}").resolve()
    args.out = (args.out or REPO / "dist" / f"env{env}").resolve()
    args.work.mkdir(parents=True, exist_ok=True)
    version = env_version(source_version(), env)

    if args.verify:
        step(
            "verify",
            f"install check of geobrix {version} on {args.verify} (toolchain image, --network none)",
        )
        verify_install(args, env, version, args.verify)
        print(
            f"ENV_WHEEL_VERIFIED env={env} arch={args.verify} wheel={args.out / f'geobrix-{version}-py3-none-any.whl'}"
        )
        return
    if args.jar is None or args.vendor_pure is None:
        p.error("--jar and --vendor-pure are required to build")
    natives, bindings, dirty, epoch = check_inputs(args)
    step(1, f"scratch tree for geobrix {version}: {args.work / 'tree'}")
    tree, extras = build_tree(args, env, version)
    step(2, f"_build_info.py (GIT_DIRTY {dirty}, SOURCE_DATE_EPOCH {epoch})")
    write_build_info(tree, env, version, natives, dirty, epoch)
    step(
        3,
        f"payload: {', '.join(ARCHES)} natives and bindings, share, licences, the JAR",
    )
    add_payload(args, tree, natives, bindings, version)
    step(4, "vendored pure-Python packages")
    vendored = add_vendor_pure(args, tree, env, bindings)
    print(
        f"    vendored: {', '.join(vendored)}; every Requires-Dist covered, by version"
    )
    step(5, "python -m build --wheel --no-isolation")
    whl = build_wheel(args, tree, epoch)
    step(6, "geobrix-native.pth")
    whl = add_pth(args, whl, epoch)
    step(7, "verify the wheel and extract its payload")
    size = verify(args, whl, env, version, extras, epoch)
    step(8, "auditwheel per architecture (toolchain image, --network none)")
    auditwheel(args, bindings)
    args.out.mkdir(parents=True, exist_ok=True)
    for old in args.out.glob("*.whl"):
        old.unlink()
    shutil.move(str(whl), args.out / whl.name)
    print(
        f"WHEEL_BUILT env={env} wheel={args.out / whl.name} mb={size:.1f} sha256={sha256(args.out / whl.name)} "
        f"source_date_epoch={epoch} git_dirty={dirty} payload={args.work / 'payload'}"
    )


if __name__ == "__main__":
    main()
