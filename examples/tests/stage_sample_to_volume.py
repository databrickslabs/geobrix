#!/usr/bin/env python3
"""
Stage a sample-data dataset (a directory of the minimal bundle) to a Unity Catalog Volume.

Uploads every file under  <src-root>/<path>  to  <volume-root>/geobrix-examples/<path>  with the
Files API (streaming PUT) and checks each remote size against the local one. Finder litter
(.DS_Store, ._* files) is not uploaded. A symlink whose target is outside --src-root, or any
symlinked directory, is refused (exit 2) before a client is built.

What it deletes: only stale files INSIDE the uploaded part of each *.zarr store (a chunk left by
an older layout of the store would read wrongly). That is the whole store when the store, or a
directory above it, is uploaded, and only the uploaded subtree when --path points inside a store
(--path <store>.zarr/<array> prunes that array and never touches zarr.json or the other arrays).
Nothing else is ever deleted: files beside a store, other datasets, the rest of the Volume. A path
that holds no *.zarr store (for example --path nyc) is uploaded over what is there and deletes
nothing.

Options:
  --profile NAME      Databricks config profile (required; never chosen for you)
  --path REL          dataset directory under geobrix-examples, relative, no ".." and no
                      empty or "." segment (default: global/era5-weather)
  --src-root DIR      local geobrix-examples root
                      (default: sample-data/Volumes/main/default/test-data/geobrix-examples)
  --volume-root DIR   Volume to stage into, /Volumes/<catalog>/<schema>/<volume>
                      (default: /Volumes/geospatial_docs/geobrix/sample-data)
  --dry-run           list what would be uploaded and pruned; no workspace call, no SDK needed
  --help, -h          this text

Reads no config file: the profile and the Volume come from the options. Exit codes: 0 done,
1 an upload or check failed (nothing deleted), 2 bad arguments or nothing to stage, 3 the
profile cannot authenticate (log in to it yourself), 4 the workspace could not be reached.
"""

from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from _cli import exit_if_help  # sibling module (script dir is on sys.path)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_SRC_ROOT = (
    PROJECT_ROOT / "sample-data/Volumes/main/default/test-data/geobrix-examples"
)
DEFAULT_VOLUME_ROOT = "/Volumes/geospatial_docs/geobrix/sample-data"
DEFAULT_PATH = "global/era5-weather"
WORKERS = 8
AUTH_ERRORS = (
    "Unauthenticated",
    "PermissionDenied",
)  # databricks.sdk.errors class names


def clean_rel(path: str) -> str:
    """The dataset path, or ValueError. Relative, and no empty, "." or ".." segment."""
    if not path or path.startswith("/") or "\\" in path:
        raise ValueError(
            f"--path {path!r} must be a relative dataset directory such as {DEFAULT_PATH}"
        )
    rel = path[:-1] if path.endswith("/") else path
    if any(segment in ("", ".", "..") for segment in rel.split("/")):
        raise ValueError(
            f"--path {path!r} has an empty, '.' or '..' segment; "
            f"use a plain directory such as {DEFAULT_PATH}"
        )
    return rel


def clean_volume_root(given: str) -> str:
    """The Volume root as /Volumes/<catalog>/<schema>/<volume>[/dir], or ValueError."""
    root = given.rstrip("/")
    parts = root.split("/")
    if (
        not root.startswith("/Volumes/")
        or len(parts) < 5
        or any(part in ("", ".", "..") for part in parts[1:])
    ):
        raise ValueError(
            f"--volume-root {given!r} must be /Volumes/<catalog>/<schema>/<volume>"
        )
    return root


def is_litter(name: str) -> bool:
    """Finder files that are never data (a Zarr store's own dotfiles, .zarray and the like, are)."""
    return name == ".DS_Store" or name.startswith("._")


def local_files(src: Path, src_root: Path) -> list[Path]:
    """Every file under ``src`` that is data, sorted; ValueError for a link that cannot be staged.

    ``src_root`` is the resolved local geobrix-examples root. A symlinked file is followed only
    when its target is inside it, so nothing outside the bundle is uploaded; a symlinked
    directory is refused (it is not followed, and skipping it silently would drop files).
    """
    files: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(src, followlinks=False):
        base = Path(dirpath)
        for name in dirnames:
            if (base / name).is_symlink():
                raise ValueError(
                    f"{base / name} is a symlinked directory; stage the real directory instead"
                )
        for name in filenames:
            path = base / name
            if is_litter(name):
                continue
            if path.is_symlink():
                try:
                    path.resolve().relative_to(src_root)
                except ValueError:
                    raise ValueError(
                        f"{path} is a symlink to {path.resolve()}, outside --src-root "
                        f"{src_root}; refusing to stage it"
                    ) from None
            if path.is_file():
                files.append(path)
    return sorted(files)


def prune_scopes(rel: str, files: list[Path], src: Path) -> list[str]:
    """The directories whose stale files are pruned, relative to geobrix-examples.

    The uploaded part of each *.zarr store: the whole store when it is uploaded whole (or sits
    below --path), and only the uploaded subtree when --path points inside a store
    (<store>.zarr/<array>). Never above --path: the store's zarr.json and the sibling arrays of
    a sub-path upload are not part of it.
    """
    depth = len(rel.split("/"))
    scopes = set()
    for f in files:
        segments = f"{rel}/{f.relative_to(src).as_posix()}".split("/")
        for i, segment in enumerate(segments[:-1]):  # the last one is the file itself
            if segment.endswith(".zarr"):
                scopes.add("/".join(segments[: max(i + 1, depth)]))
                break
    return sorted(scopes)


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
    try:
        with open(local, "rb") as fh:
            w.files.upload(remote, fh, overwrite=True)
        size = w.files.get_metadata(remote).content_length
    except (
        Exception
    ) as e:  # noqa: BLE001 - one failed file is reported, not a traceback
        return f"{remote}: {type(e).__name__}: {str(e)[:200]}"
    if size != local.stat().st_size:
        return f"{remote}: local {local.stat().st_size} B, remote {size} B"
    return None


def _is_auth_error(error: Exception) -> bool:
    """A credentials problem: a config ValueError, or the SDK's 401 / 403 errors (by class name)."""
    return isinstance(error, ValueError) or any(
        cls.__name__ in AUTH_ERRORS for cls in type(error).__mro__
    )


def main(argv=None) -> int:
    exit_if_help(__doc__, argv)
    ap = argparse.ArgumentParser(description="Stage a sample-data dataset to a Volume")
    ap.add_argument("--profile")
    ap.add_argument("--path", default=DEFAULT_PATH)
    ap.add_argument("--src-root", type=Path, default=DEFAULT_SRC_ROOT)
    ap.add_argument("--volume-root", default=DEFAULT_VOLUME_ROOT)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    # Everything that decides where files go is checked before any file or workspace is touched.
    try:
        rel = clean_rel(args.path)
        volume_root = clean_volume_root(args.volume_root)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    src_root = args.src_root.resolve()
    src = (src_root / rel).resolve()
    try:
        src.relative_to(src_root)
    except ValueError:
        print(f"Error: --path {rel!r} leaves --src-root {src_root}.", file=sys.stderr)
        return 2
    base = f"{volume_root}/geobrix-examples"
    dest = f"{base}/{rel}"
    try:
        files = local_files(src, src_root) if src.is_dir() else []
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    if not files:
        print(
            f"Nothing to stage: no files under {src}. Generate it first "
            "(gbx:data:generate-minimal-bundle --zarr-only).",
            file=sys.stderr,
        )
        return 2
    scopes = prune_scopes(rel, files, src)
    total = sum(f.stat().st_size for f in files)
    print(f"Staging {len(files)} files ({total / 1e6:.2f} MB) from {src}")
    print(f"  to {dest}")
    if scopes:
        print("  stale files are removed only inside: " + ", ".join(scopes))
    else:
        print(
            "  no *.zarr store here: files are uploaded over what is there, nothing is deleted"
        )
    if args.dry_run:
        for f in files:
            print(f"  would upload {f.relative_to(src).as_posix()}")
        return 0
    if not args.profile:
        print(
            "Pass --profile NAME (the workspace is never chosen for you).",
            file=sys.stderr,
        )
        return 2

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print(
            "Install databricks-sdk (Python 3.11 or newer) and run again.",
            file=sys.stderr,
        )
        return 2
    try:
        w = WorkspaceClient(profile=args.profile)
    except (
        Exception
    ) as e:  # noqa: BLE001 - the profile cannot be turned into credentials
        print(
            f"Cannot use profile {args.profile!r} ({type(e).__name__}). "
            f"Log in yourself (databricks auth login --profile {args.profile}) and run again.",
            file=sys.stderr,
        )
        return 3
    try:
        w.current_user.me()  # one cheap call: an auth or network problem stops here
    except Exception as e:  # noqa: BLE001
        if _is_auth_error(e):
            print(
                f"Profile {args.profile!r} cannot authenticate ({type(e).__name__}). "
                f"Log in yourself (databricks auth login --profile {args.profile}) and run again.",
                file=sys.stderr,
            )
            return 3
        print(
            f"Could not reach the workspace of profile {args.profile!r}: "
            f"{type(e).__name__}: {str(e)[:200]}",
            file=sys.stderr,
        )
        return 4

    remote = {f: f"{dest}/{f.relative_to(src).as_posix()}" for f in files}
    for directory in sorted({str(Path(p).parent) for p in remote.values()}):
        try:
            w.files.create_directory(directory)
        except Exception:  # noqa: BLE001 - already there, or the upload creates it
            pass
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        errors = [e for e in pool.map(lambda f: _upload(w, f, remote[f]), files) if e]
    if errors:
        print("Upload failed or did not land (nothing was deleted):", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        return 1

    # Plan every prune first: a scope whose listing does not contain what was just uploaded is
    # listed in a form we did not build, and then nothing may be deleted anywhere.
    plan = []
    for scope in scopes:
        scope_dest = f"{base}/{scope}"
        mine = {p for p in remote.values() if p.startswith(scope_dest + "/")}
        listing = remote_files(w, scope_dest)
        missing = sorted(mine - set(listing))
        in_a_store = any(segment.endswith(".zarr") for segment in scope.split("/"))
        if missing or not in_a_store:
            print(
                f"Error: the listing of {scope_dest} does not contain the files just uploaded "
                f"(first: {missing[:1]}); nothing was deleted.",
                file=sys.stderr,
            )
            return 1
        plan.append((scope_dest, mine, [p for p in listing if p not in mine]))
    for scope_dest, _, stale in plan:
        for path in stale:
            # unreachable: the listing is under the scope
            if not path.startswith(scope_dest + "/"):
                print(f"Error: refusing to delete {path}.", file=sys.stderr)
                return 1
            print(f"Removing stale file: {path}")
            w.files.delete(path)
    for scope_dest, mine, _ in plan:
        left = remote_files(w, scope_dest)
        if set(left) != mine:
            print(
                f"Verification failed: {scope_dest} holds {len(left)} files, expected {len(mine)}.",
                file=sys.stderr,
            )
            return 1
    print(f"Done: {len(files)} files in {dest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
