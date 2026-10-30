#!/usr/bin/env python3
"""The DCS image's per-architecture lock and wheelhouse.

    dcs_lock.py relock ARCH            rewrite python/geobrix/envs/env6/dcs-base-<arch>.txt (the base image's
                                       packages, headed by its digest) and dcs-<arch>.lock (hashed; the base held,
                                       click included), both or neither
    dcs_lock.py wheelhouse ARCH DEST   put into DEST the wheels the lock installs beyond the base image, and
                                       GeoBrix's own rasterio wheel (its sha256 must be the lock's)
    dcs_lock.py check ARCH             the lock against the base file and, when built, our rasterio wheel
    dcs_lock.py moves ARCH             what the lock changes against the base image
    dcs_lock.py base-image ARCH        the pinned base image reference (repo@sha256:...) the base list was made from
    dcs_lock.py rasterio-pin           the rasterio distribution version the lock must carry (from scripts/native/pins.conf)

ARCH is x86_64 or aarch64. The lock is made on the host with uv (`uv pip compile --generate-hashes`), offering
the rasterio wheel that scripts/native built as a local candidate, and the image installs it with
`pip install --no-index --find-links <wheelhouse> --require-hashes -r dcs.lock`: no package index is needed in the
image build. The wheels come from the package index pip and uv are configured with; no URL, host or credential is
printed (output goes through scripts/native/redact.py). Standard library only (the host python3 may be 3.10).
"""

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections import OrderedDict
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_ROOT = HERE.parent.parent.parent
ENV_DIR = "python/geobrix/envs/env6"
ARCHES = ("aarch64", "x86_64")
BASE_IMAGES = {  # arch -> (docker platform, image)
    "aarch64": ("linux/arm64", "databricksruntime/environment:v6-standard-arm"),
    "x86_64": ("linux/amd64", "databricksruntime/environment:v6-standard"),
}
# What the core install moves on purpose (core.txt says so): httpx2 2.12 and later need idna >= 3.18, and the base
# image ships 3.11. Every other package of the base image is held at its version.
DECLARED_MOVES = ("idna",)
GLIBC_MINORS = range(17, 29)  # manylinux_2_17 .. manylinux_2_28: the image is Ubuntu 24.04 (glibc 2.39)


class LockError(Exception):
    pass


def canon(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def parse_lock(text):
    """name -> (version, [sha256, ...]) for the `name==version \\` entries of a hashed requirements file."""
    out = OrderedDict()
    current = None
    for line in text.splitlines():
        m = re.match(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==(\S+?)(?:\s+\\)?$", line)
        if m:
            current = canon(m.group(1))
            out[current] = (m.group(2), [])
            continue
        h = re.match(r"^\s+--hash=sha256:([0-9a-f]{64})", line)
        if h and current:
            out[current][1].append(h.group(1))
    return out


def parse_base(text):
    """name -> version for the `name==version` lines of `pip list --format=freeze`."""
    out = {}
    for line in text.splitlines():
        if "==" in line and not line.startswith("#"):
            name, version = line.strip().split("==", 1)
            out[canon(name)] = version
    return out


def constraints(base_text, drop=DECLARED_MOVES):
    """The base image's packages as constraints, less the ones the core install declares it moves."""
    skip = {canon(n) for n in drop}
    keep = [ln for ln in base_text.splitlines() if "==" in ln and canon(ln.split("==", 1)[0]) not in skip]
    return "\n".join(keep) + "\n"


def needed(lock, base):
    """name -> (version, hashes) for the lock entries the base image does not already have at that version."""
    return OrderedDict((n, e) for n, e in lock.items() if base.get(n) != e[0])


def moves(lock, base):
    """name -> (base version, lock version) for the packages the lock changes."""
    return {n: (base[n], e[0]) for n, e in lock.items() if n in base and base[n] != e[0]}


def check(lock, base, rasterio, wheel_sha=None):
    """Problems with a lock, as a list of messages (empty is sound)."""
    problems = []
    for name, (version, hashes) in lock.items():
        if not hashes:
            problems.append(f"{name}=={version} has no hash (`--require-hashes` refuses the file)")
    for name, (was, now) in moves(lock, base).items():
        if name not in {canon(n) for n in DECLARED_MOVES}:
            problems.append(f"{name} moves from the base image's {was} to {now}; the base is held")
    entry = lock.get("rasterio")
    if entry is None:
        problems.append("no rasterio in the lock")
    else:
        if entry[0] != rasterio:
            problems.append(f"rasterio is {entry[0]}, not GeoBrix's build {rasterio}")
        if wheel_sha and wheel_sha not in entry[1]:
            problems.append(f"the built rasterio wheel (sha256 {wheel_sha[:12]}) is not the lock's: relock")
    return problems


# ---- files and tools ---------------------------------------------------------------------------------------------


def _redactor():
    spec = importlib.util.spec_from_file_location("redact", DEFAULT_ROOT / "scripts/native/redact.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.redact


def _env_dir(root):
    return Path(root) / ENV_DIR


def _read(root, arch):
    env = _env_dir(root)
    try:
        return (env / f"dcs-{arch}.lock").read_text(), (env / f"dcs-base-{arch}.txt").read_text()
    except OSError as exc:
        raise LockError(f"{exc.filename}: not found; run `gbx:dcs:build --relock`")


def rasterio_pin(root):
    """`<RASTERIO_VERSION>+gbx<GBX_NATIVE_REVISION>` from scripts/native/pins.conf."""
    values = dict(
        re.findall(r"^(RASTERIO_VERSION|GBX_NATIVE_REVISION)=(\S+)$", (Path(root) / "scripts/native/pins.conf").read_text(), re.M)
    )
    return f"{values['RASTERIO_VERSION']}+gbx{values['GBX_NATIVE_REVISION']}"


def rasterio_wheel(root, arch, version):
    dist = Path(root) / "build/native/out" / arch / "bindings/dist"
    hits = sorted(dist.glob(f"rasterio-{version}-*-linux_{arch}.whl"))
    if len(hits) != 1:
        raise LockError(f"expected one rasterio-{version} wheel for {arch} in {dist}, found {len(hits)}; run gbx:build:native")
    return hits[0]


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def platforms(arch):
    names = [f"manylinux_2_{n}_{arch}" for n in reversed(GLIBC_MINORS)] + [f"manylinux2014_{arch}"]
    if arch == "x86_64":
        names += ["manylinux2010_x86_64", "manylinux1_x86_64"]
    return names


def _run_redacted(cmd, **kw):
    """Run a tool; its output is printed only through the redactor. Returns the exit status and the tail."""
    redact = _redactor()
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, **kw)
    out = redact(res.stdout or "")
    return res.returncode, out


def download(arch, dest, requirements):
    """`pip download` the hashed requirements for the image's platform into dest (the host's own pip configuration)."""
    with tempfile.TemporaryDirectory() as tmp:
        req = Path(tmp) / "requirements.txt"
        req.write_text(requirements)
        cmd = [sys.executable, "-m", "pip", "download", "-q", "--disable-pip-version-check", "--no-deps", "--only-binary", ":all:",
               "--python-version", "3.12", "--implementation", "cp", "--abi", "cp312", "--abi", "abi3", "--abi", "none",
               "--require-hashes", "-r", str(req), "-d", str(dest)]
        for p in platforms(arch):
            cmd += ["--platform", p]
        rc, out = _run_redacted(cmd)
    if rc != 0:
        raise LockError(f"pip download exited {rc}:\n{out[-1500:]}")


def _wheel_files(cache, name, version):
    dist = re.sub(r"[-_.]+", "_", name)
    ver = version.replace("-", "_")
    return sorted(p for p in Path(cache).glob("*.whl") if re.fullmatch(rf"{re.escape(dist)}-{re.escape(ver)}-.*\.whl", p.name, re.I))


def hashed_requirements(entries):
    lines = []
    for name, (version, hashes) in entries.items():
        lines.append(f"{name}=={version} " + " ".join(f"--hash=sha256:{h}" for h in hashes))
    return "\n".join(lines) + "\n"


def wheelhouse(root, arch, dest, download=download):
    """Fill dest with the wheels the lock installs beyond the base image, and GeoBrix's rasterio wheel."""
    lock_text, base_text = _read(root, arch)
    lock, base = parse_lock(lock_text), parse_base(base_text)
    base_image(base_text)  # refuses a base list that is not pinned to an image digest
    want = rasterio_pin(root)
    built = rasterio_wheel(root, arch, want)
    problems = check(lock, base, want, sha256(built))
    if problems:
        raise LockError("; ".join(problems))
    need = needed(lock, base)
    need.pop("rasterio", None)
    cache = Path(root) / "build/dcs/wheelhouse" / arch
    cache.mkdir(parents=True, exist_ok=True)
    missing = OrderedDict((n, e) for n, e in need.items() if not _wheel_files(cache, n, e[0]))
    if missing:
        download(arch, cache, hashed_requirements(missing))
    dest = Path(dest)
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    for name, (version, _) in need.items():
        found = _wheel_files(cache, name, version)
        if not found:
            raise LockError(f"no wheel for {name}=={version} for {arch} after the download")
        shutil.copy(found[0], dest / found[0].name)
    shutil.copy(built, dest / built.name)
    return sorted(p.name for p in dest.iterdir())


BASE_REPO = "databricksruntime/environment"
BASE_HEADER = re.compile(r"^# base-image: (" + re.escape(BASE_REPO) + r"@sha256:[0-9a-f]{64})$", re.M)


def base_image(base_text):
    """The base image reference (`repo@sha256:...`) a base list was made from: the build uses exactly it."""
    m = BASE_HEADER.search(base_text)
    if not m:
        raise LockError("the base list names no image digest (`# base-image: <repo>@sha256:...`); run `gbx:dcs:build --relock`")
    return m.group(1)


def local_digest(image):
    """The digest of a base image tag that is in the local image store, as `repo@sha256:...` (no registry query)."""
    rc, out = _run_redacted(["docker", "image", "inspect", image, "--format", "{{json .RepoDigests}}"])
    try:
        refs = [r for r in json.loads(out.strip() or "[]") if r.startswith(BASE_REPO + "@sha256:")] if rc == 0 else []
    except ValueError:
        refs = []
    if len(refs) != 1:
        raise LockError(f"{image} is not in the local image store with one digest (docker pull it first)")
    return refs[0]


def relock(root, arch):
    """Rewrite the base list and the lock for one architecture (needs docker, uv and the built rasterio wheel).

    Both files are written together, and only after the compile and the lock check have passed: a failure
    leaves the previous pair untouched.
    """
    platform, image = BASE_IMAGES[arch]
    env = _env_dir(root)
    want = rasterio_pin(root)
    built = rasterio_wheel(root, arch, want)
    ref = local_digest(image)
    rc, listing = _run_redacted(
        ["docker", "run", "--rm", "--platform", platform, "--entrypoint", "/databricks/python3/bin/python", ref,
         "-m", "pip", "list", "--disable-pip-version-check", "--format=freeze"]
    )
    if rc != 0:
        raise LockError(f"could not list the packages of {image}:\n{listing[-800:]}")
    lines = sorted((ln.strip() for ln in listing.splitlines() if "==" in ln), key=str.lower)
    base_text = f"# base-image: {ref}\n" + "\n".join(lines) + "\n"
    # The constraints file sits beside dcs.in under a fixed name: uv prints the path in the lock's `via` notes, so a
    # temporary directory's name would end up in a tracked file.
    cons = env / f".dcs-constraints-{arch}.txt"
    with tempfile.TemporaryDirectory() as tmp:
        new_lock = Path(tmp) / f"dcs-{arch}.lock"
        cons.write_text(constraints(base_text))
        try:
            rc, out = _run_redacted(
                ["uv", "pip", "compile", "--quiet", "--generate-hashes", "--only-binary", ":all:", "--python-version", "3.12",
                 "--python-platform", f"{arch}-manylinux_2_28", "-c", cons.name, "--find-links", str(built.parent),
                 "--custom-compile-command", "gbx:dcs:build --relock", "-o", str(new_lock), "dcs.in"],
                cwd=str(env),
            )
        finally:
            cons.unlink()
        if rc != 0:
            raise LockError(f"uv pip compile exited {rc}:\n{out[-1500:]}")
        lock_text = new_lock.read_text()
        problems = check(parse_lock(lock_text), parse_base(base_text), want, sha256(built))
        if problems:
            raise LockError("; ".join(problems))
    staged = {name: env / (name + ".new") for name in (f"dcs-{arch}.lock", f"dcs-base-{arch}.txt")}
    staged[f"dcs-{arch}.lock"].write_text(lock_text)
    staged[f"dcs-base-{arch}.txt"].write_text(base_text)
    for name, path in staged.items():
        os.replace(path, env / name)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=str(DEFAULT_ROOT))
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("relock", "check", "moves"):
        sub.add_parser(name).add_argument("arch", choices=ARCHES)
    sub.add_parser("rasterio-pin")
    sub.add_parser("base-image").add_argument("arch", choices=ARCHES)
    w = sub.add_parser("wheelhouse")
    w.add_argument("arch", choices=ARCHES)
    w.add_argument("dest")
    args = ap.parse_args(argv)
    try:
        if args.cmd == "base-image":
            print(base_image(_read(args.root, args.arch)[1]))
            return 0
        if args.cmd == "rasterio-pin":
            print(rasterio_pin(args.root))
            return 0
        if args.cmd == "relock":
            relock(args.root, args.arch)
        if args.cmd == "wheelhouse":
            names = wheelhouse(args.root, args.arch, args.dest)
            print(f"wheelhouse: {len(names)} wheels for {args.arch}")
        if args.cmd in ("relock", "check", "moves"):
            lock_text, base_text = _read(args.root, args.arch)
            lock, base = parse_lock(lock_text), parse_base(base_text)
            want = rasterio_pin(args.root)
            if args.cmd == "check":
                problems = check(lock, base, want)
                for p in problems:
                    print(f"LOCK PROBLEM: {p}")
                return 1 if problems else 0
            moved = moves(lock, base)
            new = [f"{n}=={e[0]}" for n, e in needed(lock, base).items() if n not in base]
            print(f"{args.arch}: {len(lock)} packages locked; moved against the base: {moved or 'none'}; added: {len(new)}")
    except LockError as exc:
        print(f"DCS LOCK FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
