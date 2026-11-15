#!/usr/bin/env python3
"""Admit the env6 base's click 8.2.1 in an unpacked rasterio sdist, and stamp the native revision.

    relax-click.py <unpacked sdist> [--revision N]

rasterio >= 1.4.4 declares `click>=4.0,!=8.2.*` (pallets/click#2939), while the Databricks env6 / DBR 19 base ships
click 8.2.1. This removes that one `!=8.2.*` from the click requirement, in pyproject.toml or, where the requirement
lives there, setup.cfg or setup.py, and fails unless exactly one requirement was edited. It also appends
`+gbx<revision>` (a PEP 440 local label, for provenance) to the version in rasterio/__init__.py. N is
GBX_NATIVE_REVISION from the environment unless --revision gives it. Prints the before and after of every line it
changes. Exit 1 and `RELAX FAILED: ...` on anything unexpected.
"""

import argparse
import os
import re
import sys
from pathlib import Path

# "click>=4.0,!=8.2.*"  /  'click!=8.2.*,>=4.0'  /  "click >= 4.0, != 8.2.*": the whole quoted requirement, so the
# edit cannot touch click-plugins or cligj (a quote follows the specifier, never a hyphen or a letter).
CLICK_REQ = re.compile(
    r"""(?P<q>["'])click\s*(?P<spec>[^"']*?!=\s*8\.2\.\*[^"']*)(?P=q)"""
)
VERSION = re.compile(
    r"""(?m)^(?P<head>__version__\s*=\s*)(?P<q>["'])(?P<v>[^"'+]+)(?P=q)\s*$"""
)


def fail(msg):
    print(f"RELAX FAILED: {msg}")
    sys.exit(1)


def drop_exclusion(spec):
    """'>=4.0,!=8.2.*' -> '>=4.0'; a lone '!=8.2.*' leaves the bare name."""
    parts = [p.strip() for p in spec.split(",")]
    kept = [p for p in parts if not re.fullmatch(r"!=\s*8\.2\.\*", p)]
    return ",".join(kept)


def relax_click(root):
    edits, writes = [], []
    for name in ("pyproject.toml", "setup.cfg", "setup.py"):
        path = root / name
        if not path.is_file():
            continue
        out, changed = [], False
        for line in path.read_text().splitlines(keepends=True):
            new = CLICK_REQ.sub(
                lambda m: f"{m['q']}click{drop_exclusion(m['spec'])}{m['q']}", line
            )
            if new != line:
                changed = True
                edits.append((name, line.rstrip("\n"), new.rstrip("\n")))
            out.append(new)
        if changed:
            writes.append((path, "".join(out)))
    if len(edits) != 1:
        fail(
            f"expected exactly one click requirement with '!=8.2.*' in pyproject.toml, setup.cfg or setup.py, found {len(edits)}"
        )
    for path, text in writes:
        path.write_text(text)
    for name, before, after in edits:
        print(f"{name}\n  before: {before.strip()}\n  after:  {after.strip()}")


def stamp_version(root, revision):
    path = root / "rasterio" / "__init__.py"
    if not path.is_file():
        fail(f"no {path}")
    text = path.read_text()
    found = list(VERSION.finditer(text))
    if len(found) != 1:
        fail(
            f"expected one plain __version__ line in rasterio/__init__.py, found {len(found)}"
        )
    m = found[0]
    new = f"{m['head']}{m['q']}{m['v']}+gbx{revision}{m['q']}"
    path.write_text(text[: m.start()] + new + text[m.end() :])
    print(f"rasterio/__init__.py\n  before: {m.group(0).strip()}\n  after:  {new}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("sdist_dir", type=Path)
    ap.add_argument("--revision", default=os.environ.get("GBX_NATIVE_REVISION"))
    args = ap.parse_args()
    if not args.revision or not args.revision.isdigit():
        fail("GBX_NATIVE_REVISION (or --revision) must be a number")
    if not args.sdist_dir.is_dir():
        fail(f"{args.sdist_dir} is not a directory")
    relax_click(args.sdist_dir)
    stamp_version(args.sdist_dir, args.revision)


if __name__ == "__main__":
    main()
