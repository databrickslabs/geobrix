#!/usr/bin/env python3
"""Build geobrix-<version>+env<N>-py3-none-any.whl with both architectures' native payload.

Run by `gbx:build:wheel` (scripts/commands/gbx-build-wheel.sh) inside
`uv run --no-project --with-requirements python/geobrix/requirements-build.txt`, so `python -m build`,
setuptools and `wheel` are the hashed build requirements. No container lock (no dev container).

Inputs:
  build/native/out/<arch>/stage-native/gdal-native/  GDAL and the libraries it needs, MANIFEST.json (N3)
  build/native/out/<arch>/bindings/{vendor,wheels}/   the five bindings built against it (N4)
  python/geobrix/envs/env<N>/                         core.txt, extra-*.txt, vendor-pure.txt
  python/geobrix/envs/pyproject.env.toml              the env wheels' pyproject (ruling NP-3)
  --vendor-pure <dir>  the vendor-pure.txt wheels, fetched and hash-checked on the host by the command
  --jar <path>         the GeoBrix JAR (shipped in _jar/, spec 3.6)
  --extra-payload <d>  a tree merged into the payload, only under _native/<arch>/ (the Rust kernels:
                       _native/<arch>/rs/_gbxrs.abi3.so); repeatable

Steps (each prints `== <n> ...`; a failure prints `BUILD FAILED: <what>`, a missing input
`PRECONDITION MISSING: <what>`, and exits 1):
  1 a scratch tree: the package source, the template pyproject plus the extras and tripwires,
    env/ (the env files), and __version__ stamped `<version>+env<N>` (exactly one replacement);
  2 _build_info.py: ENV, VERSION, the GDAL/PROJ/GEOS versions from MANIFEST.json, the native
    revision, the git sha;
  3 the payload per architecture: _native/<arch>/lib (no pkgconfig/cmake, no symlinks) and
    MANIFEST.json, _native/share and _native/licenses (identical for both architectures, or fail),
    _vendor/<arch>, the extra payload, _jar/;
  4 the vendored pure-Python wheels into _vendor/common without their dist-info (licences to
    _vendor/licenses); every Requires-Dist of a vendored package must be another vendored package
    (at a version it accepts), a core.txt name, or click;
  5 python -m build --wheel --no-isolation;
  6 wheel unpack, add geobrix-native.pth (`import _gbx_native_path`), wheel pack;
  7 verify the wheel (metadata against the env files, the payload, the JAR, the licence guard, the
    URLs in the metadata, the size) and extract its payload to build/env-wheel/env<N>/payload/,
    the GBX_NATIVE_ROOT layout;
  8 per architecture, in that architecture's toolchain image (--network none): auditwheel show on a
    throwaway wheel of _native/<arch> + _vendor/<arch> is consistent with manylinux_2_28_<arch> and
    names no external library (the closure check of the payload as packed).
Only then the wheel goes to dist/env<N>/, which keeps no other wheel.
"""

import argparse
import hashlib
import json
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
from packaging.utils import canonicalize_name
from packaging.version import Version

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
SIZE_BUDGET_MB = 100  # the env wheels measured 80 MB (2026-10-28); a payload that grows 25% past that fails
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


def run(cmd, log, cwd=None):
    with open(log, "w") as fh:
        res = subprocess.run(cmd, cwd=cwd, stdout=fh, stderr=subprocess.STDOUT)
    if res.returncode != 0:
        tail = Path(log).read_text(errors="replace").splitlines()[-25:]
        print("\n".join(URL.sub("<url>", ln) for ln in tail))
        fail(f"{' '.join(map(str, cmd[:4]))} ... exited {res.returncode} (log: {log})")


def base_version():
    text = (PYDIR / "src" / PKG / "__init__.py").read_text()
    return re.search(r'^__version__ = "([^"]+)"$', text, re.M).group(1)


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
    return natives, bindings


def build_tree(args, env, ver, natives):
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
        f"# A light_* name of the other environment's family: this wheel is not the one to install.\n"
        f"geobrix=={ver.base_version}+env{other}\n"
    )

    template = (PYDIR / "envs" / "pyproject.env.toml").read_text()
    committed = tomllib.loads((PYDIR / "pyproject.toml").read_text())["project"]
    mine = tomllib.loads(template)["project"]
    for key in ("name", "license", "requires-python", "authors"):
        if mine.get(key) != committed.get(key):
            fail(
                f"envs/pyproject.env.toml [project] {key} = {mine.get(key)!r}, the committed pyproject says {committed.get(key)!r}"
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
    text, stamp = init.read_text(), f'__version__ = "{ver.base_version}"'
    if text.count(stamp) != 1:
        fail(f"{init} has {text.count(stamp)} lines `{stamp}`; exactly one is stamped")
    init.write_text(text.replace(stamp, f'__version__ = "{ver.base_version}+env{env}"'))
    return tree, extras


def write_build_info(tree, env, version, natives):
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

    def git(*a):
        res = subprocess.run(
            ["git", "-C", str(REPO), *a], capture_output=True, text=True
        )
        return res.stdout.strip()

    dirty = bool(
        git(
            "status",
            "--porcelain",
            "--",
            "python/geobrix/src",
            "python/geobrix/envs",
            "scripts/native",
        )
    )
    (tree / "src" / PKG / "_build_info.py").write_text(
        '"""Written by gbx:build:wheel into the env wheel (never in the source tree)."""\n\n'
        f"ENV = {env}\n"
        f'VERSION = "{version}"\n'
        f'GDAL_VERSION = "{pick["gdal"][ARCHES[0]]}"\n'
        f'PROJ_VERSION = "{pick["proj"][ARCHES[0]]}"\n'
        f'GEOS_VERSION = "{pick["geos"][ARCHES[0]]}"\n'
        f"NATIVE_REVISION = {pick['revision'][ARCHES[0]]}\n"
        f'GIT_SHA = "{git("rev-parse", "HEAD")}"\n'
        f"GIT_DIRTY = {dirty}\n"
    )
    return manifests


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


def add_payload(args, tree, natives, bindings):
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
        # The bindings record hashed SONAMEs, which change with every gdal-native rebuild: a binding
        # tree linked against another build names libraries this lib/ does not have.
        for so in vendor.rglob("*.so"):
            stale = {m.decode() for m in HASHED_NEED.findall(so.read_bytes())} - have
            if stale:
                fail(
                    f"{so.relative_to(pkg)} needs {sorted(stale)}, not in {a}/lib: the bindings were built against another gdal-native"
                )
    copy_files(natives[ARCHES[0]] / "share", nat / "share")
    copy_files(natives[ARCHES[0]] / "licenses", nat / "licenses")
    for name, files in blics[ARCHES[0]].items():
        for rel, data in files.items():
            (nat / "licenses" / name / rel).parent.mkdir(parents=True, exist_ok=True)
            (nat / "licenses" / name / rel).write_bytes(data)

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
    jar = pkg / "_jar" / f"geobrix-{base_version()}-jar-with-dependencies.jar"
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


def add_vendor_pure(args, tree, env, bindings):
    """Step 4."""
    pkg = tree / "src" / PKG
    common, licdir = pkg / "_vendor" / "common", pkg / "_vendor" / "licenses"
    pins = {}
    for line in (
        (PYDIR / "envs" / f"env{env}" / "vendor-pure.txt").read_text().splitlines()
    ):
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

    core = {
        canonicalize_name(r.name)
        for r in reqs_of(PYDIR / "envs" / f"env{env}" / "core.txt")
    }
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
            elif dep not in core and dep not in RELY_ON_BASE:
                fail(
                    f"vendored {name} requires {r}: neither vendored, in env{env}/core.txt, nor preinstalled (click)"
                )
    return sorted(vendored)


def build_wheel(args, tree):
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
    )
    wheels = list(out.glob("*.whl"))
    if len(wheels) != 1:
        fail(f"python -m build made {[w.name for w in wheels]}, wanted one wheel")
    return wheels[0]


def add_pth(args, whl):
    """Step 6."""
    unpacked, final = args.work / "unpacked", args.work / "final"
    for d in (unpacked, final):
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True)
    run(
        [sys.executable, "-m", "wheel", "unpack", str(whl), "-d", str(unpacked)],
        args.work / "unpack.log",
    )
    (root,) = unpacked.iterdir()
    (root / "geobrix-native.pth").write_text("import _gbx_native_path\n")
    run(
        [sys.executable, "-m", "wheel", "pack", str(root), "-d", str(final)],
        args.work / "pack.log",
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
    trip = {f"geobrix=={Version(version).base_version}+env{other}"}
    for name in TRIPWIRES[env]:
        if by_extra.get(norm_extra(name)) != trip:
            fail(f"tripwire [{name}] is {by_extra.get(norm_extra(name))}")
    if {norm_extra(n) for n in TRIPWIRES[other]} & set(by_extra):
        fail("a same-env old extra name is declared")
    hosts = {h.lower() for h in URL.findall(meta_text)}
    if not hosts <= METADATA_URL_HOSTS:
        n = len(hosts - METADATA_URL_HOSTS)
        fail(f"METADATA names {n} host(s) outside {sorted(METADATA_URL_HOSTS)}")


def verify_payload(zf, version):
    names = set(zf.namelist())
    for a in ARCHES:
        manifest = json.loads(zf.read(f"{PKG}/_native/{a}/MANIFEST.json"))
        for lib in [*manifest["sonames"].values(), manifest["jni"]]:
            if f"{PKG}/_native/{a}/lib/{lib}" not in names:
                fail(f"{a}: {lib} is not in the wheel")
        if f"{PKG}/_vendor/{a}/rasterio/__init__.py" not in names:
            fail(f"{a}: the vendored rasterio is not in the wheel")
    base = Version(version).base_version
    for need in (
        f"{PKG}/_jar/geobrix-{base}-jar-with-dependencies.jar",
        "geobrix-native.pth",
        "_gbx_native_path.py",
        f"{PKG}/_build_info.py",
        f"{PKG}/_native/share/proj/proj.db",
        f"{PKG}/_native/__init__.py",
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
    if any(re.search(r"(pkgconfig|cmake)/", n) for n in names):
        fail("the wheel holds pkgconfig or cmake files")


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


def verify(args, whl, env, version, extras):
    """Step 7: returns the wheel's size in MB."""
    if whl.name != f"geobrix-{version}-py3-none-any.whl":
        fail(f"the wheel is {whl.name}, wanted geobrix-{version}-py3-none-any.whl")
    with zipfile.ZipFile(whl) as zf:
        verify_metadata(zf, env, version, extras)
        verify_payload(zf, version)
        extract_payload(zf, args.work / "payload")
    size = whl.stat().st_size / 1e6
    if size > SIZE_BUDGET_MB:
        fail(f"the wheel is {size:.1f} MB, over the {SIZE_BUDGET_MB} MB budget")
    return size


def auditwheel(args, bindings):
    """Step 8."""
    if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        missing("Docker is not running (step 8 runs auditwheel in the toolchain image)")
    script = r"""
set -euo pipefail
A="$1"; W=/tmp/aw/gbxprobe-0
mkdir -p "$W/gbxprobe/_native" "$W/gbxprobe/_vendor" "$W/gbxprobe-0.dist-info"
cp -a "/payload/_native/$A" "$W/gbxprobe/_native/$A"
cp -a "/payload/_vendor/$A" "$W/gbxprobe/_vendor/$A"
: > "$W/gbxprobe/__init__.py"
printf 'Metadata-Version: 2.1\nName: gbxprobe\nVersion: 0\n' > "$W/gbxprobe-0.dist-info/METADATA"
printf 'Wheel-Version: 1.0\nGenerator: gbx-build-wheel\nRoot-Is-Purelib: false\nTag: cp312-cp312-linux_%s\n' "$A" > "$W/gbxprobe-0.dist-info/WHEEL"
$PY -m wheel pack "$W" --dest-dir /tmp/aw > /dev/null
auditwheel show --json "/tmp/aw/gbxprobe-0-cp312-cp312-linux_$A.whl" > /tmp/aw/aw.json
$PY - "$A" <<'EOF'
import json, sys
r = json.load(open("/tmp/aw/aw.json"))
tag = f"manylinux_2_28_{sys.argv[1]}"
ok = r.get("overall_tag") == tag and not r.get("external_libs")
print(("ok: " if ok else "BUILD FAILED: ") + f"auditwheel {sys.argv[1]}: overall_tag {r.get('overall_tag')}, external libraries {sorted(r.get('external_libs') or [])}")
sys.exit(0 if ok else 1)
EOF
"""
    for a in ARCHES:
        rec = (
            (bindings[a] / "build.txt").read_text()
            if (bindings[a] / "build.txt").is_file()
            else ""
        )
        m = re.search(r"image=geobrix-native-build:bindings-([0-9a-f]{12})-" + a, rec)
        if not m:
            missing(
                f"{bindings[a]}/build.txt names no bindings image (the toolchain the payload was built in)"
            )
        image = f"geobrix-native-build:toolchain-{m.group(1)}-{a}"
        if (
            subprocess.run(
                ["docker", "image", "inspect", image], capture_output=True
            ).returncode
            != 0
        ):
            missing(
                f"the toolchain image {image} (gbx:build:native --arch {a} --target toolchain)"
            )
        res = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--platform",
                PLATFORMS[a],
                "-v",
                f"{args.work / 'payload'}:/payload:ro",
                image,
                "bash",
                "-c",
                script,
                "aw",
                a,
            ],
            capture_output=True,
            text=True,
        )
        out = (res.stdout + res.stderr).strip()
        print("    " + out.replace("\n", "\n    "))
        if res.returncode != 0:
            fail(f"auditwheel {a} (in {image})")


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--env", type=int, choices=(5, 6), required=True)
    p.add_argument("--jar", type=Path, required=True)
    p.add_argument("--vendor-pure", type=Path, required=True)
    p.add_argument("--extra-payload", type=Path, action="append", default=[])
    p.add_argument("--native-out", type=Path, default=REPO / "build" / "native" / "out")
    p.add_argument("--work", type=Path)
    p.add_argument("--out", type=Path)
    args = p.parse_args()
    env = args.env
    args.work = (args.work or REPO / "build" / "env-wheel" / f"env{env}").resolve()
    args.out = (args.out or REPO / "dist" / f"env{env}").resolve()
    args.work.mkdir(parents=True, exist_ok=True)
    ver = Version(base_version())
    version = f"{ver.base_version}+env{env}"

    natives, bindings = check_inputs(args)
    step(1, f"scratch tree for geobrix {version}: {args.work / 'tree'}")
    tree, extras = build_tree(args, env, ver, natives)
    step(2, "_build_info.py")
    write_build_info(tree, env, version, natives)
    step(
        3,
        f"payload: {', '.join(ARCHES)} natives and bindings, share, licences, the JAR",
    )
    add_payload(args, tree, natives, bindings)
    step(4, "vendored pure-Python packages")
    vendored = add_vendor_pure(args, tree, env, bindings)
    print(f"    vendored: {', '.join(vendored)}; every Requires-Dist covered")
    step(5, "python -m build --wheel --no-isolation")
    whl = build_wheel(args, tree)
    step(6, "geobrix-native.pth")
    whl = add_pth(args, whl)
    step(7, "verify the wheel and extract its payload")
    size = verify(args, whl, env, version, extras)
    step(8, "auditwheel per architecture (toolchain image, --network none)")
    auditwheel(args, bindings)
    args.out.mkdir(parents=True, exist_ok=True)
    for old in args.out.glob("*.whl"):
        old.unlink()
    shutil.move(str(whl), args.out / whl.name)
    print(
        f"WHEEL_BUILT env={env} wheel={args.out / whl.name} mb={size:.1f} sha256={sha256(args.out / whl.name)} payload={args.work / 'payload'}"
    )


if __name__ == "__main__":
    main()
