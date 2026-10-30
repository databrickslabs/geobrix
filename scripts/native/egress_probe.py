#!/usr/bin/env python3
"""Before CI's cold native build: can this runner reach every host the build downloads from directly?

    egress_probe.py [--timeout SECONDS] [--root DIR]

pip and the Rust crates go through the JFrog mirror (the repository wires its PyPI, Maven, npm and cargo remotes, no
Docker or generic one), so the pinned downloads of scripts/native/prefetch.sh go to their upstream hosts: every
*_URL / *_URL2 of pins.conf (source tarballs, the bindings' sdists, rustup-init and cargo-deny, the PROJ grids,
rasterio's test data). Each download needs one reachable origin: its URL's host or its URL2 mirror's. A host is
reachable when it answers HTTPS at all (any HTTP status); a refused, reset or timed-out connection, or a failed TLS
handshake, is blocked. One `::error::` line per download that has no reachable origin, naming the host and the kind of
download, then exit 1: seconds, instead of failing 20 minutes into the build. The quay.io image pull and the image
build's dnf and rustup toolchain install are probed by the workflow step that runs this. Stdlib only.
"""

import argparse
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from pins import load_pins  # noqa: E402

WHERE = 'the "Direct fetches" list in .github/workflows/build-gdal-native.yml'


def kind(key):
    if key.startswith("RUSTUP_"):
        return "the Rust toolchain (rustup-init; the image build installs the toolchain from the same host)"
    if key.startswith("CARGO_DENY_"):
        return "the cargo-deny binary"
    if "_SDIST_" in key:
        return "a Python binding's sdist"
    if key.startswith("PROJGRID_"):
        return "a PROJ grid"
    if "TESTDATA" in key:
        return "rasterio's test data"
    return "a pinned source tarball"


def downloads(pins):
    """{download: [(key, host), ...]}: each pinned download with its origin and mirror hosts."""
    out = {}
    for key, value in sorted(pins.items()):
        if not key.endswith(("_URL", "_URL2")) or not value.startswith(("https://", "http://")):
            continue
        dep = key[: -len("_URL2")] if key.endswith("_URL2") else key[: -len("_URL")]
        out.setdefault(dep, []).append((key, urlsplit(value).hostname))
    return out


def reachable_https(host, timeout):
    req = urllib.request.Request(f"https://{host}/", method="HEAD", headers={"User-Agent": "geobrix-egress-probe"})
    try:
        urllib.request.urlopen(req, timeout=timeout).close()
        return True
    except urllib.error.HTTPError:  # the host answered
        return True
    except (urllib.error.URLError, OSError):
        return False


def probe(pins, reachable):
    """The `::error::` lines for downloads with no reachable origin (empty: all reachable)."""
    dl = downloads(pins)
    hosts = sorted({h for origins in dl.values() for _, h in origins})
    with ThreadPoolExecutor(max_workers=8) as ex:
        ok = dict(zip(hosts, ex.map(reachable, hosts)))
    errors = []
    for dep, origins in sorted(dl.items()):
        if not any(ok[h] for _, h in origins):
            names = ", ".join(sorted({h for _, h in origins}))
            errors.append(
                f"::error::direct egress blocked: {names} ({kind(origins[0][0])}, pins.conf {dep}_URL): the cold native"
                f" build downloads it outside JFrog. See {WHERE}."
            )
    print(
        f"egress probe: {sum(ok.values())} of {len(hosts)} hosts reachable;"
        f" {len(dl) - len(errors)} of {len(dl)} pinned downloads have an origin"
    )
    return errors


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--timeout", type=float, default=20)
    ap.add_argument("--root", type=Path, default=HERE.parent.parent)
    args = ap.parse_args(argv)
    errors = probe(load_pins(args.root / "scripts/native/pins.conf"), lambda h: reachable_https(h, args.timeout))
    for e in errors:
        print(e)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
