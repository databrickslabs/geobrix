"""Stage the JAR this wheel carries on a Unity Catalog Volume, for a dedicated cluster (or a standard
cluster whose metastore admin allowlists the path). The light tier never needs it."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import os
import re
import shutil
import zipfile

from databricks.labs.gbx.uc.ddl import check_volume_dir

_IN_WHEEL = re.compile(
    r"databricks/labs/gbx/_jar/geobrix-[^/]+-jar-with-dependencies\.jar"
)
INSTRUCTION = (
    "geobrix: staged {dst} (sha256 {sha}).\n"
    "Next: add it as a cluster library on a dedicated cluster (Compute > your cluster > Libraries > "
    "Install new > Volumes > {dst}) and restart the cluster; a running cluster cannot load a JAR. "
    "On standard access mode a metastore admin must first add {dst} to the artifact allowlist. "
    "Then install the same env wheel on the cluster and call databricks.labs.gbx.register(spark): "
    "GeoBrix checks that the JAR and the wheel are the same version."
)


_WHEEL_NAME = re.compile(r"geobrix-([^-+]+)(?:\+[^-]+)?-py3-none-any\.whl")


def _recorded_sha256(z: zipfile.ZipFile, member: str):
    """The hex sha256 the wheel's RECORD lists for ``member`` (``sha256=<urlsafe base64>``), or None."""
    records = [n for n in z.namelist() if re.fullmatch(r"[^/]+\.dist-info/RECORD", n)]
    if len(records) != 1:
        return None
    for row in csv.reader(io.StringIO(z.read(records[0]).decode("utf-8"))):
        if row and row[0] == member:
            algo, _, digest = (row[1] if len(row) > 1 else "").partition("=")
            if algo == "sha256" and digest:
                try:
                    return base64.urlsafe_b64decode(
                        digest + "=" * (-len(digest) % 4)
                    ).hex()
                except ValueError:
                    return None
    return None


def jar_from_wheel(whl_path: str) -> bytes:
    """The bytes of the JAR an env wheel carries, after two checks: the JAR is named for the wheel's
    version, and its sha256 is the one the wheel's RECORD lists."""
    base = os.path.basename(whl_path)
    with zipfile.ZipFile(whl_path) as z:
        hits = [n for n in z.namelist() if _IN_WHEEL.fullmatch(n)]
        if len(hits) != 1:
            raise ValueError(
                f"{base} carries {len(hits)} geobrix JARs, expected 1. "
                "Fix: use an env wheel (geobrix-<version>+env5 or +env6)."
            )
        member = hits[0]
        named = _WHEEL_NAME.fullmatch(base)
        if named and os.path.basename(member) != (
            f"geobrix-{named.group(1)}-jar-with-dependencies.jar"
        ):
            raise ValueError(
                f"{base} carries {os.path.basename(member)}, not the JAR of version "
                f"{named.group(1)}. Fix: rebuild the wheel (gbx:data:stage-wheel) so it bundles its own JAR."
            )
        data = z.read(member)
        want = _recorded_sha256(z, member)
        if want is None or hashlib.sha256(data).hexdigest() != want:
            raise ValueError(
                f"the JAR in {base} does not match the sha256 its RECORD lists"
                + (" (RECORD lists none)" if want is None else "")
                + ". Fix: rebuild the wheel and stage the new one."
            )
        return data


def _already_staged(src: str, dst: str, sha: str) -> bool:
    """The staged JAR exists with the source's size and hashes to ``sha``, and its sidecar says so: the sidecar alone
    is not proof (an interrupted copy or a deleted JAR leaves it behind)."""
    if not (os.path.exists(dst) and os.path.exists(dst + ".sha256")):
        return False
    if os.path.getsize(dst) != os.path.getsize(src):
        return False
    with open(dst + ".sha256") as fh:
        if fh.read().split()[:1] != [sha]:
            return False
    with open(dst, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest() == sha


def stage_jar(vol_dir: str, *, overwrite: bool = False) -> str:
    """Copy the JAR this wheel carries into ``vol_dir`` with a ``.sha256`` sidecar and print the cluster
    step. One geobrix JAR per folder (one per cluster). Returns the staged path."""
    from databricks.labs.gbx import _native

    src = _native.jar_path()
    if src is None:
        raise RuntimeError(
            "this wheel carries no JAR. Fix: install the env wheel (geobrix-<version>+env5 or +env6)."
        )
    vol_dir = check_volume_dir(vol_dir.rstrip("/"))
    os.makedirs(vol_dir, exist_ok=True)
    name = os.path.basename(src)
    others = sorted(
        f
        for f in os.listdir(vol_dir)
        if f.endswith("-jar-with-dependencies.jar") and f != name
    )
    if others:
        raise RuntimeError(
            f"{vol_dir} already holds {others[0]}. Fix: keep one geobrix JAR per folder (one geobrix JAR per "
            "cluster): remove it, or stage into another folder."
        )
    with open(src, "rb") as fh:
        sha = hashlib.sha256(fh.read()).hexdigest()
    dst = os.path.join(vol_dir, name)
    if overwrite or not _already_staged(src, dst, sha):
        shutil.copyfile(src, dst)  # a Volume (FUSE) takes no rename: copy in place
        with open(dst + ".sha256", "w") as fh:
            fh.write(f"{sha}  {name}\n")
    print(INSTRUCTION.format(dst=dst, sha=sha))
    return dst
