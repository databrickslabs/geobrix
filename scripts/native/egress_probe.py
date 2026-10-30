#!/usr/bin/env python3
"""Before CI's cold native build: can this runner fetch every file the build downloads directly?

    egress_probe.py [--arch x86_64|aarch64] [--timeout SECONDS] [--root DIR]

The repository wires JFrog remotes for PyPI, Maven, npm and cargo only. pip, the Rust crates and (in CI) the five
binding sdists (fetch-lib.sh fetch_sdist, GBX_SDISTS_FROM=index) go through them. The rest of scripts/native/prefetch.sh
goes to its upstream hosts: every *_URL / *_URL2 of pins.conf (the source tarballs, rustup-init, cargo-deny, the PROJ
grids) and rasterio's test data (test-data.conf). Each download needs one working origin, its URL or its URL2 mirror:
a ranged GET of the file itself (redirects followed, so a release asset's storage host is checked too) that answers
2xx. HTTP 403, 407 and 451 are a policy proxy's refusal (blocked); another status means the pinned URL itself fails; a
refused, reset or timed-out connection, or a failed TLS handshake, is blocked. An HTTP 429, a 5xx or a timeout is
retried once first, as prefetch.sh's `curl --retry` retries it, so a transient answer does not fail the job. One
`::error::` line per download with no working origin, naming the host and the kind of download, then exit 1: seconds,
not 20 minutes into the build. The quay.io image pull and the image build's dnf are probed by the workflow step that
runs this; the image build's `rustup toolchain install` uses static.rust-lang.org, rustup-init's host, probed here.
Stdlib only.
"""

import argparse
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from pins import load_pins  # noqa: E402

WHERE = 'the "Direct fetches" list in .github/workflows/build-gdal-native.yml'
POLICY = (403, 407, 451)  # what a policy proxy answers for a host or path it refuses
RETRY_DELAY = 1  # seconds before the one retry of a 429, a 5xx or a timeout (curl --retry's first wait)
TRIPLES = {"RUSTUP": "{arch}-unknown-linux-gnu", "CARGO_DENY": "{arch}-unknown-linux-musl"}  # as prefetch.sh


def kind(dep):
    if dep == "RUSTUP":
        return "rustup-init; the image build installs the Rust toolchain from the same host"
    if dep == "CARGO_DENY":
        return "the cargo-deny binary"
    if dep.startswith("PROJGRID_"):
        return "a PROJ grid"
    if dep == "RASTERIO_TESTDATA":
        return "rasterio's test data (test-data.conf)"
    return "a pinned source tarball"


def downloads(pins, testdata, arch):
    """{dep: [url, ...]}: each direct download of prefetch.sh with its origin and mirror."""
    out = {}
    for key, value in sorted({**pins, **testdata}.items()):
        if not key.endswith(("_URL", "_URL2")) or not value.startswith(("https://", "http://")):
            continue
        dep = key[: -len("_URL2")] if key.endswith("_URL2") else key[: -len("_URL")]
        if "{triple}" in value:
            value = value.replace("{triple}", TRIPLES[dep].format(arch=arch))
        out.setdefault(dep, []).append(value)
    return out


def check_url(url, timeout):
    """(ok, why): a ranged GET of the pinned file; a 429, a 5xx or a timeout is retried once."""
    for attempt in (1, 2):
        req = urllib.request.Request(url, headers={"Range": "bytes=0-0", "User-Agent": "geobrix-egress-probe"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                resp.read(1)
            return True, "ok"
        except urllib.error.HTTPError as e:
            if e.code in POLICY:
                return False, f"refused (HTTP {e.code}: a policy proxy)"
            why, transient = f"the pinned URL answered HTTP {e.code}", e.code == 429 or e.code >= 500
        except (urllib.error.URLError, OSError) as e:
            why = "no connection (refused, reset, timed out or TLS failed)"
            transient = isinstance(e, TimeoutError) or isinstance(getattr(e, "reason", None), TimeoutError)
        if not transient or attempt == 2:
            return False, why
        time.sleep(RETRY_DELAY)


def probe(dl, check):
    """The `::error::` lines for downloads with no working origin (empty: all have one)."""
    urls = sorted({u for us in dl.values() for u in us})
    with ThreadPoolExecutor(max_workers=8) as ex:
        res = dict(zip(urls, ex.map(check, urls)))
    errors = []
    for dep, us in sorted(dl.items()):
        if not any(res[u][0] for u in us):
            hosts = ", ".join(f"{urlsplit(u).hostname} ({res[u][1]})" for u in us)
            src = "test-data.conf" if dep == "RASTERIO_TESTDATA" else "pins.conf"
            errors.append(
                f"::error::direct egress blocked: {hosts}: {kind(dep)}, {src} {dep}_URL. The cold native build"
                f" downloads it outside JFrog. See {WHERE}."
            )
    hosts = {urlsplit(u).hostname for u in urls}
    print(
        f"egress probe: {len(dl) - len(errors)} of {len(dl)} direct downloads have a working origin"
        f" ({sum(ok for ok, _ in res.values())} of {len(urls)} URLs, {len(hosts)} hosts)"
    )
    return errors


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--arch", choices=("x86_64", "aarch64"), default="x86_64")
    ap.add_argument("--timeout", type=float, default=30)
    ap.add_argument("--root", type=Path, default=HERE.parent.parent)
    args = ap.parse_args(argv)
    native = args.root / "scripts" / "native"
    dl = downloads(load_pins(native / "pins.conf"), load_pins(native / "test-data.conf"), args.arch)
    errors = probe(dl, lambda u: check_url(u, args.timeout))
    for e in errors:
        print(e)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
