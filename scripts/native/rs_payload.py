#!/usr/bin/env python3
"""The Rust kernel payload (build/rs/payload) and its provenance: what each architecture's _gbxrs was built from.

    rs_payload.py stamp ARCH [--root DIR]   gbx:build:rs, after a build: write build/rs/out/ARCH/provenance.json
    rs_payload.py check [--root DIR]        exit 1 unless both architectures' module is there, built from the
                                            checkout's sources in today's toolchain image (prints what is not)

The provenance of a module: the sha256 of every file under git in rust/gbxrs (read from the checkout, which
gbx:build:rs copies into the build), the toolchain image tag it was built in (pins.py tag-hash: pins.conf, the
toolchain stage, rust-toolchain.toml and cargo-offline.toml), and the module's own sha256. gbx:build:wheel ships the
payload only when every one still matches, so a wheel never carries a kernel older than the crate.

Stdlib only.
"""

import argparse
import datetime
import hashlib
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from pins import tag_hash  # noqa: E402

DEFAULT_ROOT = HERE.parent.parent
ARCHES = ("aarch64", "x86_64")
CRATE = "rust/gbxrs"
MODULE = "_gbxrs"
FIX = "bash scripts/commands/gbx-build-rs.sh --arch both"


def payload_dir(root):
    """The tree gbx:build:wheel merges into the payload: _native/<arch>/rs/_gbxrs.abi3.so."""
    return Path(root) / "build" / "rs" / "payload"


def stamp_path(root, arch):
    return Path(root) / "build" / "rs" / "out" / arch / "provenance.json"


def module_path(root, arch):
    """The architecture's built module, or None (none, or more than one)."""
    found = sorted((payload_dir(root) / "_native" / arch / "rs").glob(f"{MODULE}*.so"))
    return found[0] if len(found) == 1 else None


def _sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def sources(root):
    """rel -> sha256 of the crate's files under git, as the checkout has them ("deleted" for one that is gone)."""
    out = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--", CRATE], capture_output=True, check=True
    ).stdout.decode()
    rels = sorted(p for p in out.split("\0") if p)
    if not rels:
        raise SystemExit(f"rs_payload.py: no file under git in {CRATE}")
    return {rel: _sha(Path(root) / rel) if (Path(root) / rel).is_file() else "deleted" for rel in rels}


def stamp(root, arch, tag=None):
    so = module_path(root, arch)
    if so is None:
        raise SystemExit(f"rs_payload.py: no single {MODULE}*.so in {payload_dir(root)}/_native/{arch}/rs")
    head = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True)
    data = {
        "written_by": "bash scripts/commands/gbx-build-rs.sh",
        "crate": CRATE,
        "arch": arch,
        "date": datetime.datetime.now(datetime.timezone.utc).date().isoformat(),
        "head": head.stdout.strip() or None,
        "toolchain_image": tag or tag_hash(Path(root), arch),
        "module": so.relative_to(payload_dir(root)).as_posix(),
        "module_sha256": _sha(so),
        "sources": sources(root),
    }
    p = stamp_path(root, arch)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2) + "\n")
    return p


def read_stamp(root, arch):
    p = stamp_path(root, arch)
    return json.loads(p.read_text()) if p.is_file() else None


def problems(root, tags=None):
    """(missing, stale): why the payload may not ship, one line per architecture; both empty when it may."""
    missing, stale = [], []
    now = sources(root)
    for arch in ARCHES:
        so, st = module_path(root, arch), read_stamp(root, arch)
        if so is None:
            missing.append(f"{arch}: no {MODULE}*.so in {payload_dir(root)}/_native/{arch}/rs")
            continue
        if st is None:
            missing.append(f"{arch}: {so.name} has no provenance ({stamp_path(root, arch)}), so its sources are unknown")
            continue
        why = []
        if st.get("module_sha256") != _sha(so) or st.get("module") != so.relative_to(payload_dir(root)).as_posix():
            why.append(f"{so.name} is not the module the stamp names")
        tag = (tags or {}).get(arch) or tag_hash(Path(root), arch)
        if st.get("toolchain_image") != tag:
            why.append(f"built in toolchain image {st.get('toolchain_image')}, today's is {tag}")
        then = st.get("sources", {})
        changed = sorted(k for k in set(then) | set(now) if then.get(k) != now.get(k))
        if changed:
            why.append(f"{CRATE} changed since the build: {', '.join(changed)}")
        if why:
            stale.append(f"{arch}: " + "; ".join(why))
    return missing, stale


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("stamp")
    s.add_argument("arch", choices=ARCHES)
    sub.add_parser("check")
    for p in sub.choices.values():
        p.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    args = ap.parse_args(argv)
    if args.cmd == "stamp":
        print(f"RS_STAMP arch={args.arch} {stamp(args.root, args.arch)}")
        return 0
    missing, stale = problems(args.root)
    for line in missing + stale:
        print(f"{'MISSING' if line in missing else 'STALE'}: {line}")
    if missing or stale:
        print(f"Fix: {FIX}")
        return 1
    print(f"rs payload ok: {', '.join(ARCHES)} built from the checkout's {CRATE}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
