#!/usr/bin/env python3
"""Hash of the committed sources that Unity Catalog integration depends on.

`gbx:uc:tests` stores this hash in its evidence file, and the QC check
`uc-integration-evidence` compares it with the current one. A later commit that touches none of
UC_PATHS (a docs change, a CI change) keeps the evidence valid; a commit that touches any of them
makes it stale.

The hash is sha256 over `git ls-tree -r HEAD -- <UC_PATHS>`: the mode, blob id and path of every
committed file under those paths. Uncommitted edits are not part of it. `--check-clean` exits 1
when a tracked file under UC_PATHS differs from HEAD, because evidence must describe committed code.

Usage:
    python3 scripts/ci/uc_sources_hash.py [--root DIR] [--rev REV]   print the hash
    python3 scripts/ci/uc_sources_hash.py [--root DIR] --check-clean  exit 1 when a UC source is dirty
"""

import argparse
import hashlib
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

# The one list of UC-relevant paths. Each is a path (file or folder) relative to the repository root:
# the Python package (the UC functions and install code), the example statements the gate runs,
# the Scala sources (the JAR behind the Scala functions), and the Maven build file.
UC_PATHS = (
    "python/geobrix/src",
    "examples/uc-functions",
    "src/main/scala",
    "pom.xml",
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _git(root: Path, *args: str) -> str:
    res = subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {res.stderr.strip()}")
    return res.stdout


def sources_hash(root: Path = REPO_ROOT, rev: str = "HEAD", paths=UC_PATHS) -> str:
    """sha256 (hex) of `git ls-tree -r <rev> -- <paths>`."""
    listing = _git(root, "ls-tree", "-r", rev, "--", *paths)
    return hashlib.sha256(listing.encode()).hexdigest()


def dirty_files(root: Path = REPO_ROOT, paths=UC_PATHS) -> List[str]:
    """Tracked files under `paths` that differ from HEAD (staged or not). Untracked files are not listed."""
    out = _git(root, "diff", "--name-only", "HEAD", "--", *paths)
    return [line for line in out.splitlines() if line]


def head_sha(root: Path = REPO_ROOT) -> str:
    return _git(root, "rev-parse", "HEAD").strip()


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--root", default=str(REPO_ROOT), help="repository root (default: this checkout)")
    ap.add_argument("--rev", default="HEAD", help="commit to hash (default HEAD)")
    ap.add_argument("--check-clean", action="store_true", help="exit 1 when a tracked UC source differs from HEAD")
    args = ap.parse_args(argv)
    root = Path(args.root)
    if args.check_clean:
        dirty = dirty_files(root)
        if dirty:
            print("uncommitted changes under the UC sources:", file=sys.stderr)
            for f in dirty:
                print(f"  {f}", file=sys.stderr)
            print("Fix: commit or stash them; the evidence must describe committed code.", file=sys.stderr)
            return 1
        print("clean")
        return 0
    print(sources_hash(root, args.rev))
    return 0


if __name__ == "__main__":
    sys.exit(main())
