#!/usr/bin/env python3
"""
Stage a sample-data dataset (a directory of the minimal bundle) to a Unity Catalog Volume.

Uploads every file under  <src-root>/<path>  to  <volume-root>/geobrix-examples/<path>  with the
Files API (streaming PUT), checks each remote size against the local one, and removes remote
files under that dataset directory that the local copy does not have (a store left over from an
older layout would read wrongly).

Options:
  --profile NAME      Databricks config profile (required; never chosen for you)
  --path REL          dataset directory under geobrix-examples (default: global/era5-weather)
  --src-root DIR      local geobrix-examples root
                      (default: sample-data/Volumes/main/default/test-data/geobrix-examples)
  --volume-root DIR   Volume to stage into (default: /Volumes/geospatial_docs/geobrix/sample-data)
  --dry-run           list what would be uploaded and removed locally; no workspace call
  --help, -h          this text

Reads no config file: the profile and the Volume come from the options. An auth error stops the
run (exit 3); log in with `databricks auth login --profile NAME` yourself.
"""
from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from _cli import exit_if_help  # sibling module (script dir is on sys.path)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_SRC_ROOT = (
    PROJECT_ROOT
    / "sample-data/Volumes/main/default/test-data/geobrix-examples"
)
DEFAULT_VOLUME_ROOT = "/Volumes/geospatial_docs/geobrix/sample-data"
DEFAULT_PATH = "global/era5-weather"
WORKERS = 8


def local_files(src: Path) -> list[Path]:
    """Every file under ``src``, sorted."""
    return sorted(p for p in src.rglob("*") if p.is_file())


def remote_files(w, root: str) -> list[str]:
    """Every file path under the Volume directory ``root`` ([] when it does not exist yet)."""
    from databricks.sdk.errors import NotFound

    out: list[str] = []
    todo = [root]
    while todo:
        directory = todo.pop()
        try:
            entries = list(w.files.list_directory_contents(directory))
        except NotFound:
            continue
        for entry in entries:
            (todo if entry.is_directory else out).append(entry.path)
    return sorted(out)


def _upload(w, local: Path, remote: str) -> str | None:
    """Upload one file and check its size; returns an error text, or None."""
    with open(local, "rb") as fh:
        w.files.upload(remote, fh, overwrite=True)
    size = w.files.get_metadata(remote).content_length
    if size != local.stat().st_size:
        return f"{remote}: local {local.stat().st_size} B, remote {size} B"
    return None


def main(argv=None) -> int:
    exit_if_help(__doc__, argv)
    ap = argparse.ArgumentParser(description="Stage a sample-data dataset to a Volume")
    ap.add_argument("--profile")
    ap.add_argument("--path", default=DEFAULT_PATH)
    ap.add_argument("--src-root", type=Path, default=DEFAULT_SRC_ROOT)
    ap.add_argument("--volume-root", default=DEFAULT_VOLUME_ROOT)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    rel = args.path.strip("/")
    src = args.src_root / rel
    dest = f"{args.volume_root.rstrip('/')}/geobrix-examples/{rel}"
    files = local_files(src) if src.is_dir() else []
    if not files:
        print(f"Nothing to stage: no files under {src}. Generate it first "
              "(gbx:data:generate-minimal-bundle --zarr-only).", file=sys.stderr)
        return 2
    total = sum(f.stat().st_size for f in files)
    print(f"Staging {len(files)} files ({total / 1e6:.2f} MB) from {src}")
    print(f"  to {dest}")
    if args.dry_run:
        for f in files:
            print(f"  would upload {f.relative_to(src).as_posix()}")
        return 0
    if not args.profile:
        print("Pass --profile NAME (the workspace is never chosen for you).", file=sys.stderr)
        return 2

    from databricks.sdk import WorkspaceClient

    try:
        w = WorkspaceClient(profile=args.profile)
        w.current_user.me()  # one cheap call: an auth problem stops here
    except Exception as e:  # noqa: BLE001 - report the auth failure and stop
        print(f"Auth failed for profile {args.profile!r}: {type(e).__name__}. "
              f"Log in yourself (databricks auth login --profile {args.profile}) and run again.",
              file=sys.stderr)
        return 3

    keep = {f"{dest}/{f.relative_to(src).as_posix()}" for f in files}
    for directory in sorted({str(Path(p).parent) for p in keep}):
        try:
            w.files.create_directory(directory)
        except Exception:  # noqa: BLE001 - already there, or the upload creates it
            pass
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        errors = [
            e
            for e in pool.map(
                lambda f: _upload(w, f, f"{dest}/{f.relative_to(src).as_posix()}"), files
            )
            if e
        ]
    if errors:
        print("Upload verification failed (bytes did not land):", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        return 1

    stale = [p for p in remote_files(w, dest) if p not in keep]
    for path in stale:
        print(f"Removing stale file: {path}")
        w.files.delete(path)
    remote = remote_files(w, dest)
    if set(remote) != keep:
        print(f"Verification failed: {len(remote)} remote files, expected {len(keep)}.", file=sys.stderr)
        return 1
    print(f"Done: {len(remote)} files in {dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
