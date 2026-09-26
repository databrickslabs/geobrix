"""lidar_gbx writer (DataSource V2). Inverse of the lidar_gbx points reader.

Points DataFrame (x,y,z + optional r,g,b) -> sharded LAS/LAZ, one .laz per
Spark partition (one per cluster). Serverless-safe: write(iterator) encodes to a
worker-local temp then shutil.copyfile to the FUSE path (laspy seeks to backfill
the header, so a direct Volume write fails). Mirrors NetcdfRasterGbxWriter.
"""

from __future__ import annotations

import glob
import os
import shutil
import tempfile
import uuid
from dataclasses import dataclass, field
from typing import Iterator, List, Optional

import numpy as np
from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds import _scratch

_REQUIRED = ("x", "y", "z")
_RGB = ("r", "g", "b")
_V1_ALLOWED_DATA = set(_REQUIRED) | set(_RGB)
_V2_DATA_COLS = (
    "intensity",
    "return_number",
    "number_of_returns",
    "classification",
    "gps_time",
)
_V2_OPTIONS = ("overlap", "voxelSize", "thinOverlap", "dedupeExact")


@dataclass
class LidarCommitMessage(WriterCommitMessage):
    paths: List[str] = field(default_factory=list)
    frag_path: str = ""  # singleFile mode: this partition's feather fragment


def _reject_v2_options(options: dict) -> None:
    for opt in _V2_OPTIONS:
        val = options.get(opt)
        if val is None:
            continue
        if opt == "overlap" and str(val).lower() == "keep":
            continue  # keep is the v1 default (no-op)
        raise ValueError(
            f"lidar_gbx writer: option {opt!r}={val!r} is point-cloud unification "
            f"deferred to v2; v1 supports x,y,z + optional r,g,b only."
        )


def _resolve_write_cols(schema: StructType, options: dict) -> dict:
    names = [f.name for f in schema.fields]
    name_set = set(names)
    for c in _REQUIRED:
        if c not in name_set:
            raise ValueError(
                f"lidar_gbx writer requires columns x,y,z; missing {c!r} "
                f"(got {names})."
            )
    group_col = options.get("groupCol")
    cluster_col = options.get("clusterCol")
    name_col = options.get("nameCol")
    naming = {c for c in (group_col, cluster_col, name_col) if c}
    has_rgb = all(c in name_set for c in _RGB)
    partial_rgb = any(c in name_set for c in _RGB) and not has_rgb
    if partial_rgb:
        raise ValueError(
            "lidar_gbx writer: RGB requires all of r,g,b (or none); "
            f"got {sorted(c for c in _RGB if c in name_set)}."
        )
    # Any data column beyond x,y,z,r,g,b (and the naming columns) is v2.
    extra = [c for c in names if c not in _V1_ALLOWED_DATA and c not in naming]
    if extra:
        v2 = [c for c in extra if c in _V2_DATA_COLS]
        raise ValueError(
            f"lidar_gbx writer (v1): unsupported column(s) {extra}; v1 writes "
            f"x,y,z + optional r,g,b. Columns like {v2 or list(_V2_DATA_COLS)} are "
            f"deferred to v2 — drop them before writing."
        )
    return {
        "has_rgb": has_rgb,
        "group_col": group_col,
        "cluster_col": cluster_col,
        "name_col": name_col,
    }


class LidarGbxWriter(DataSourceWriter):
    def __init__(self, options: dict, schema: StructType, overwrite: bool):
        from databricks.labs.gbx.ds._listing import to_local_path

        self.merge = str(options.get("merge", "false")).lower() == "true"
        _reject_v2_options(options)
        if not self.merge:
            roles = _resolve_write_cols(schema, options)
        else:
            roles = {
                "has_rgb": False,
                "group_col": None,
                "cluster_col": None,
                "name_col": None,
            }
        self.has_rgb = roles["has_rgb"]
        self.group_col = roles["group_col"]
        self.cluster_col = roles["cluster_col"]
        self.name_col = roles["name_col"]
        self.path = to_local_path(options.get("path"))
        self.overwrite = overwrite
        self.crs = options.get("crs")
        self.single_file = str(options.get("singleFile", "false")).lower() == "true"
        self.keep_parts = str(options.get("keepParts", "false")).lower() == "true"
        self.file_name = options.get("fileName")
        self.part_prefix = options.get("partPrefix") or "part"
        if self.merge:
            self.single_file = False
        if not self.merge and overwrite and os.path.isdir(self.path):
            for stale in glob.glob(os.path.join(self.path, "*.la[sz]")):
                try:
                    os.remove(stale)
                except OSError:
                    pass
        self.scratch_dir = (
            _scratch.new_scratch_dir(self.path) if self.single_file else ""
        )

    def _stem(self, row) -> str:
        """Deterministic <group>_<cluster>, else nameCol basename, else uuid."""
        if self.group_col and self.cluster_col:
            g = row[self.group_col]
            c = row[self.cluster_col]
            if g is not None and c is not None:
                pre = f"{self.file_name}_" if self.file_name else ""
                return f"{pre}{g}_{c}"
        if self.name_col and row[self.name_col] is not None:
            return os.path.basename(str(row[self.name_col]))
        return f"{self.part_prefix}-{uuid.uuid4().hex[:8]}"

    def _encode_part(self, tmp_path: str, xs, ys, zs, rs, gs, bs) -> str:
        from databricks.labs.gbx.pyrx.imagery import write_xyz_laz, write_xyzrgb_laz

        x = np.asarray(xs, dtype=np.float64)
        y = np.asarray(ys, dtype=np.float64)
        z = np.asarray(zs, dtype=np.float64)
        if self.has_rgb:
            return write_xyzrgb_laz(
                tmp_path,
                x,
                y,
                z,
                np.asarray(rs, dtype=np.uint8),
                np.asarray(gs, dtype=np.uint8),
                np.asarray(bs, dtype=np.uint8),
                crs=self.crs,
            )
        return write_xyz_laz(tmp_path, x, y, z, crs=self.crs)

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        if self.merge:
            return LidarCommitMessage(paths=[], frag_path="")
        if self.single_file:
            return self._write_single(iterator)  # Task 3

        os.makedirs(self.path, exist_ok=True)
        xs: list = []
        ys: list = []
        zs: list = []
        rs: list = []
        gs: list = []
        bs: list = []
        stem: Optional[str] = None
        for row in iterator:
            if stem is None:
                stem = self._stem(row)
            xs.append(row["x"])
            ys.append(row["y"])
            zs.append(row["z"])
            if self.has_rgb:
                rs.append(row["r"])
                gs.append(row["g"])
                bs.append(row["b"])
        if not xs:
            return LidarCommitMessage(paths=[], frag_path="")
        # local-temp -> shutil.copyfile (FUSE: laspy seeks the header).
        tmp = tempfile.NamedTemporaryFile(suffix=".laz", delete=False)
        tmp.close()
        written_tmp: Optional[str] = None
        try:
            written_tmp = self._encode_part(tmp.name, xs, ys, zs, rs, gs, bs)
            ext = os.path.splitext(written_tmp)[1]  # .laz or .las fallback
            out = os.path.join(self.path, f"{stem}{ext}")
            shutil.copyfile(written_tmp, out)
        finally:
            for p in {tmp.name, written_tmp}:
                if p and os.path.exists(p):
                    try:
                        os.unlink(p)
                    except OSError:
                        pass
        return LidarCommitMessage(paths=[out])

    def _write_single(self, iterator: Iterator) -> WriterCommitMessage:
        """Placeholder for Task 3 singleFile mode."""
        return LidarCommitMessage(paths=[], frag_path="")

    def commit(self, messages: list) -> None:
        return None  # parts mode: executors already wrote the files (Task 3 adds modes)

    def abort(self, messages: list) -> None:
        for msg in messages:
            if isinstance(msg, LidarCommitMessage):
                for p in msg.paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
                if msg.frag_path:
                    try:
                        os.remove(msg.frag_path)
                    except OSError:
                        pass
        if self.scratch_dir:
            _scratch.remove_scratch_dir(self.scratch_dir)
