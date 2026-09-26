"""lidar_gbx writer (DataSource V2). Inverse of the lidar_gbx points reader.

Points DataFrame (x,y,z + optional r,g,b) -> sharded LAS/LAZ, one .laz per
Spark partition (one per cluster). Serverless-safe: write(iterator) encodes to a
worker-local temp then shutil.copyfile to the FUSE path (laspy seeks to backfill
the header, so a direct Volume write fails). Mirrors NetcdfRasterGbxWriter.

Modes
-----
parts (default)  One .laz per Spark partition, written on executors.
singleFile       Executors capture x,y,z[,r,g,b] as feather fragments; the driver
                 merges all fragments into a single .laz.
merge            Post-hoc: folds existing .laz/.las part files in the directory
                 into one .laz without re-reading the DataFrame.
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


# ---------------------------------------------------------------------------
# Compute-aware merge budget helpers
# ---------------------------------------------------------------------------

_MERGE_FALLBACK_BYTES = 512 * 1024**2  # conservative floor if the RAM probe fails


def _probe_infra():
    """Return (avail_mb, gpu_count) from gpu_infra(); (0, 0) on any probe failure."""
    try:
        from databricks.labs.gbx.pyrx.mvs import gpu_infra

        infra = gpu_infra()
        avail_mb = int(
            infra.get("host_ram_available_mb") or infra.get("host_ram_mb") or 0
        )
        gpu = int(infra.get("gpu_count", 0))
        return avail_mb, gpu
    except Exception:  # noqa: BLE001
        return 0, 0


def _merge_budget_bytes(merge_max_mb) -> int:
    """Max single-copy point bytes allowed for a driver-side merge, sized to the
    executing compute's ACTUAL available RAM (gpu_infra host_ram_available_mb).
    The merge peak is ~2x the concatenated point bytes, so the point-byte budget
    is (available - reserve) / 2. AIR (gpu_count>0) reserves more headroom (it also
    holds GPU/host buffers) but its large RAM still yields a big budget. An explicit
    mergeMaxMB option or GBX_LIDAR_MERGE_MAX_MB env var overrides. Probe failure ->
    conservative floor."""
    import os

    override = merge_max_mb or os.environ.get("GBX_LIDAR_MERGE_MAX_MB")
    if override:
        return max(int(float(override) * 1024**2), 0)
    avail_mb, gpu = _probe_infra()
    if avail_mb <= 0:
        return _MERGE_FALLBACK_BYTES
    reserve_mb = (6 if gpu > 0 else 2) * 1024  # 6 GiB AIR, 2 GiB GC/classic
    usable_mb = max(0, avail_mb - reserve_mb)
    budget = (usable_mb * 1024**2) // 2  # /2 for the ~2x merge peak
    return max(budget, _MERGE_FALLBACK_BYTES)  # never below the safe floor


# ---------------------------------------------------------------------------
# Module-level helpers (merge core)
# ---------------------------------------------------------------------------


def _laz_point_count(path: str) -> int:
    """Open a LAS/LAZ file and return its header point count."""
    import laspy

    with laspy.open(path) as r:
        return int(r.header.point_count)


def _laz_written_path(path: str) -> str:
    """Return path if it exists, else the .las sibling (LAZ-write fallback)."""
    if os.path.exists(path):
        return path
    las = path[:-4] + ".las"
    if os.path.exists(las):
        return las
    return path  # neither exists; caller will fail on the missing file


def _swap_ext(target: str, real: str) -> str:
    """Return target with real's file extension (.laz or .las on fallback)."""
    ext = os.path.splitext(real)[1]
    return os.path.splitext(target)[0] + ext


def _merge_laz_parts(inputs: List[str], tmp_path: str, has_rgb: bool, crs) -> int:
    """Concatenate x/y/z(/RGB) from part .laz/.las files, recompute header from
    merged bounds, write ONE .laz at tmp_path via the write_xyz(rgb)_laz path.
    Returns the merged point count. Driver-side (holds all points in RAM — gated
    by caller via _gate_merge / _gate_merge_paths).
    """
    import laspy

    from databricks.labs.gbx.pyrx.imagery import write_xyz_laz, write_xyzrgb_laz

    xs: list = []
    ys: list = []
    zs: list = []
    rs: list = []
    gs: list = []
    bs: list = []
    for p in inputs:
        las = laspy.read(p)
        xs.append(np.asarray(las.x))
        ys.append(np.asarray(las.y))
        zs.append(np.asarray(las.z))
        if has_rgb:
            rs.append((np.asarray(las.red) >> 8).astype(np.uint8))
            gs.append((np.asarray(las.green) >> 8).astype(np.uint8))
            bs.append((np.asarray(las.blue) >> 8).astype(np.uint8))
    x = np.concatenate(xs)
    y = np.concatenate(ys)
    z = np.concatenate(zs)
    if has_rgb:
        write_xyzrgb_laz(
            tmp_path,
            x,
            y,
            z,
            np.concatenate(rs),
            np.concatenate(gs),
            np.concatenate(bs),
            crs=crs,
        )
    else:
        write_xyz_laz(tmp_path, x, y, z, crs=crs)
    return int(len(x))


# ---------------------------------------------------------------------------
# Writer class
# ---------------------------------------------------------------------------


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
        self.merge_max_mb = options.get("mergeMaxMB")
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
            return self._write_single(iterator)

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

    # ------------------------------------------------------------------
    # singleFile mode: executor captures fragment, driver merges
    # ------------------------------------------------------------------

    def _write_single(self, iterator: Iterator) -> WriterCommitMessage:
        """Per partition, capture x,y,z[,r,g,b] as a feather fragment in scratch.

        The driver's _commit_single collects all fragments and merges them into
        one .laz via _merge_laz_parts.
        """
        import pyarrow as pa
        import pyarrow.feather as feather

        os.makedirs(self.scratch_dir, exist_ok=True)
        xs: list = []
        ys: list = []
        zs: list = []
        rs: list = []
        gs: list = []
        bs: list = []
        for row in iterator:
            xs.append(row["x"])
            ys.append(row["y"])
            zs.append(row["z"])
            if self.has_rgb:
                rs.append(row["r"])
                gs.append(row["g"])
                bs.append(row["b"])

        if not xs:
            return LidarCommitMessage(paths=[], frag_path="")

        cols = {
            "x": pa.array(xs, type=pa.float64()),
            "y": pa.array(ys, type=pa.float64()),
            "z": pa.array(zs, type=pa.float64()),
        }
        if self.has_rgb:
            cols["r"] = pa.array(rs, type=pa.uint8())
            cols["g"] = pa.array(gs, type=pa.uint8())
            cols["b"] = pa.array(bs, type=pa.uint8())

        tbl = pa.table(cols)
        frag = os.path.join(self.scratch_dir, f"frag-{uuid.uuid4().hex}.arrow")
        feather.write_feather(tbl, frag)
        return LidarCommitMessage(frag_path=frag)

    def _gate_merge(self, frags: List[str]) -> None:
        """Refuse a driver merge on feather fragments that would exceed the RAM budget.

        Estimates bytes as total feather rows * bytes/point (32 XYZ+RGB, 24 XYZ).
        Budget is compute-aware (sized from the node's actual available RAM via
        gpu_infra). mergeMaxMB option or GBX_LIDAR_MERGE_MAX_MB env var overrides.
        """
        import pyarrow.feather as feather

        total_rows = sum(
            feather.read_table(frag, columns=["x"]).num_rows for frag in frags
        )
        bpp = 32 if self.has_rgb else 24
        est = total_rows * bpp
        budget = _merge_budget_bytes(self.merge_max_mb)
        avail_mb, gpu = _probe_infra()
        if est > budget:
            raise ValueError(
                f"lidar_gbx merge: estimated {est / 1e6:.0f} MB exceeds the "
                f"compute-aware budget of {budget / 1e6:.0f} MB "
                f"(host-RAM-based; {gpu}xGPU detected, RAM avail {avail_mb} MiB). "
                f"Use mergeMaxMB=<MB> to override or run on a node with more RAM."
            )
        print(
            f"[merge] {gpu}xGPU · RAM avail {avail_mb} MiB → "
            f"budget {budget / 1e6:.0f} MB, merging {est / 1e6:.0f} MB"
        )

    def _gate_merge_paths(
        self, parts: List[str], has_rgb: Optional[bool] = None
    ) -> None:
        """Refuse a driver merge on on-disk .laz/.las files that would exceed RAM.

        Estimates bytes as summed point_count * bytes/point (32 XYZ+RGB, 24 XYZ).
        has_rgb overrides self.has_rgb (needed when self.has_rgb is False in merge
        mode but the on-disk files actually contain RGB). Budget is compute-aware
        (sized from the node's actual available RAM via gpu_infra). mergeMaxMB
        option or GBX_LIDAR_MERGE_MAX_MB env var overrides.
        """
        _has_rgb = has_rgb if has_rgb is not None else self.has_rgb
        bpp = 32 if _has_rgb else 24
        est = sum(_laz_point_count(p) for p in parts) * bpp
        budget = _merge_budget_bytes(self.merge_max_mb)
        avail_mb, gpu = _probe_infra()
        if est > budget:
            raise ValueError(
                f"lidar_gbx merge: estimated {est / 1e6:.0f} MB exceeds the "
                f"compute-aware budget of {budget / 1e6:.0f} MB "
                f"(host-RAM-based; {gpu}xGPU detected, RAM avail {avail_mb} MiB). "
                f"Use mergeMaxMB=<MB> to override or run on a node with more RAM."
            )
        print(
            f"[merge] {gpu}xGPU · RAM avail {avail_mb} MiB → "
            f"budget {budget / 1e6:.0f} MB, merging {est / 1e6:.0f} MB"
        )

    def _frags_to_temp_laz(self, frags: List[str]) -> List[str]:
        """Decode each feather fragment (x,y,z[,r,g,b]) into a temp .laz file.

        Returns the list of written paths (may be .las on LAZ-fallback).
        Cleans up any already-created temps on failure before re-raising.
        """
        import pyarrow.feather as feather

        tmps: List[str] = []
        try:
            for frag in frags:
                tbl = feather.read_table(frag)
                names = set(tbl.schema.names)
                xs = tbl.column("x").to_pylist()
                ys = tbl.column("y").to_pylist()
                zs = tbl.column("z").to_pylist()
                if self.has_rgb and "r" in names:
                    rs = tbl.column("r").to_pylist()
                    gs = tbl.column("g").to_pylist()
                    bs = tbl.column("b").to_pylist()
                else:
                    rs, gs, bs = [], [], []

                tmp = tempfile.NamedTemporaryFile(suffix=".laz", delete=False)
                tmp.close()
                written = self._encode_part(tmp.name, xs, ys, zs, rs, gs, bs)
                # If laspy fell back to .las, the .laz placeholder is empty; remove it.
                if written != tmp.name and os.path.exists(tmp.name):
                    try:
                        os.unlink(tmp.name)
                    except OSError:
                        pass
                tmps.append(written)
        except Exception:
            for p in tmps:
                if os.path.exists(p):
                    try:
                        os.unlink(p)
                    except OSError:
                        pass
            raise
        return tmps

    def _commit_single(self, messages: list) -> None:
        """Driver-side singleFile merge: decode fragments -> merge -> publish."""
        from databricks.labs.gbx.ds.vector import _resolve_single_file_output
        from databricks.labs.gbx.ds.writer import _publish_merged

        frags = [
            m.frag_path
            for m in messages
            if isinstance(m, LidarCommitMessage) and m.frag_path
        ]
        if not frags:
            _scratch.remove_scratch_dir(self.scratch_dir)
            return None

        target = _resolve_single_file_output(self.path, self.file_name, ".laz")
        temp_laz_inputs: List[str] = []
        tmp_merged: Optional[str] = None
        try:
            self._gate_merge(frags)
            temp_laz_inputs = self._frags_to_temp_laz(frags)
            expected = sum(_laz_point_count(p) for p in temp_laz_inputs)

            merged_tmp = tempfile.NamedTemporaryFile(suffix=".laz", delete=False)
            merged_tmp.close()
            tmp_merged = merged_tmp.name

            _merge_laz_parts(temp_laz_inputs, tmp_merged, self.has_rgb, self.crs)
            real = _laz_written_path(tmp_merged)
            _publish_merged(real, _swap_ext(target, real), expected, _laz_point_count)
        finally:
            # Clean up temp per-fragment .laz files.
            for p in temp_laz_inputs:
                if os.path.exists(p):
                    try:
                        os.unlink(p)
                    except OSError:
                        pass
            # Clean up merged temp (both .laz and possible .las fallback).
            if tmp_merged:
                for p in {tmp_merged, tmp_merged[:-4] + ".las"}:
                    if os.path.exists(p):
                        try:
                            os.unlink(p)
                        except OSError:
                            pass
            _scratch.remove_scratch_dir(self.scratch_dir)
        return None

    def _commit_merge(self) -> None:
        """Post-hoc directory merge: fold existing .laz/.las parts into one .laz."""
        import laspy

        from databricks.labs.gbx.ds.vector import _resolve_single_file_output
        from databricks.labs.gbx.ds.writer import _glob_merge_inputs, _publish_merged

        target = _resolve_single_file_output(self.path, self.file_name, ".laz")
        target_las = os.path.splitext(target)[0] + ".las"

        # Search for both .laz and .las parts, excluding both target variants.
        parts = sorted(
            set(
                _glob_merge_inputs(self.path, target, "laz")
                + _glob_merge_inputs(self.path, target_las, "las")
            )
        )
        if not parts:
            raise ValueError(
                f"lidar_gbx merge: no .laz/.las files under {self.path} to merge."
            )

        # Infer RGB from the first part's point format.
        with laspy.open(parts[0]) as r0:
            has_rgb = r0.header.point_format.id in (2, 3, 5, 7, 8, 10)

        self._gate_merge_paths(parts, has_rgb=has_rgb)
        expected = sum(_laz_point_count(p) for p in parts)

        tmp = tempfile.NamedTemporaryFile(suffix=".laz", delete=False)
        tmp.close()
        tmp_merged = tmp.name
        try:
            _merge_laz_parts(parts, tmp_merged, has_rgb, self.crs)
            real = _laz_written_path(tmp_merged)
            _publish_merged(real, _swap_ext(target, real), expected, _laz_point_count)
        finally:
            for p in {tmp_merged, tmp_merged[:-4] + ".las"}:
                if os.path.exists(p):
                    try:
                        os.unlink(p)
                    except OSError:
                        pass

        # DATA-SAFETY: only delete parts AFTER validate+copy+verify all passed.
        if not self.keep_parts:
            for p in parts:
                try:
                    os.remove(p)
                except OSError:
                    pass

    def commit(self, messages: list) -> None:
        if self.merge:
            return self._commit_merge()
        if self.single_file:
            return self._commit_single(messages)
        return None

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
