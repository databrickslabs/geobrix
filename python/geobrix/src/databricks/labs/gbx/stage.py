"""Stage the JAR this wheel carries on a Unity Catalog Volume, for a dedicated cluster (or a standard
cluster whose metastore admin allowlists the path). The light tier never needs it."""

from __future__ import annotations

import hashlib
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


def jar_from_wheel(whl_path: str) -> bytes:
    """The bytes of the JAR an env wheel carries."""
    with zipfile.ZipFile(whl_path) as z:
        hits = [n for n in z.namelist() if _IN_WHEEL.fullmatch(n)]
        if len(hits) != 1:
            raise ValueError(
                f"{os.path.basename(whl_path)} carries {len(hits)} geobrix JARs, expected 1. "
                "Fix: use an env wheel (geobrix-<version>+env5 or +env6)."
            )
        return z.read(hits[0])


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
    if (
        overwrite
        or not os.path.exists(dst + ".sha256")
        or open(dst + ".sha256").read().split()[:1] != [sha]
    ):
        shutil.copyfile(src, dst)  # a Volume (FUSE) takes no rename: copy in place
        with open(dst + ".sha256", "w") as fh:
            fh.write(f"{sha}  {name}\n")
    print(INSTRUCTION.format(dst=dst, sha=sha))
    return dst
