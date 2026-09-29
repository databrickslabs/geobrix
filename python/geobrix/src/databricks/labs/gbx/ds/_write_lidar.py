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
import re
import shutil
import uuid
from dataclasses import dataclass, field
from typing import Iterator, List, Optional

import numpy as np
from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds import _scratch
from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_file

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
_V2_OPTIONS = ("thinOverlap",)  # still deferred
_GRADUATED = ("overlap", "voxelSize", "dedupeExact")  # v2 seam-unification (merge mode)


@dataclass
class LidarCommitMessage(WriterCommitMessage):
    paths: List[str] = field(default_factory=list)
    frag_path: str = ""  # singleFile mode: this partition's feather fragment


def _reject_v2_options(options: dict) -> None:
    for opt in _V2_OPTIONS:
        if options.get(opt) is not None:
            raise ValueError(
                f"lidar_gbx writer: option {opt!r} is deferred (not in v2 seam scope)."
            )
    ov = options.get("overlap")
    if ov is not None and str(ov).lower() not in ("keep", "drop"):
        raise ValueError(
            f"lidar_gbx writer: overlap={ov!r} invalid; use 'keep' (default) or 'drop'."
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


def _infer_crs_from_parts(inputs: List[str]) -> Optional[str]:
    """Return the canonical CRS string inferred from the first input file's header.

    Reads the LAS/LAZ VLR CRS, routes it through ``resolve_crs`` + ``crs_to_canonical``
    so ESRI/OGC authorities are preserved (e.g. ESRI:54008 stays 'ESRI:54008' rather
    than falling back to a raw WKT blob). Returns None when no CRS can be inferred.
    """
    if not inputs:
        return None
    try:
        import laspy

        from databricks.labs.gbx.core.crs import crs_to_canonical, resolve_crs

        _parsed = laspy.read(inputs[0]).header.parse_crs()
        if _parsed is not None:
            return crs_to_canonical(resolve_crs(_parsed.to_wkt()))
    except Exception:  # noqa: BLE001
        pass
    return None


_UUID_PART_RE = re.compile(r"-[0-9a-f]{8}$")  # part-<8hex> uuid fallback stem


def _cluster_id_of(path: str, part_prefix: str = "part") -> Optional[str]:
    """Cluster id = the last '_'-token of the stem (from *_<group>_<cluster>.laz).
    None when the part is not cluster-identifiable (uuid fallback / no token)."""
    stem = os.path.splitext(os.path.basename(path))[0]
    if stem.startswith(part_prefix + "-") or _UUID_PART_RE.search(stem):
        return None
    if "_" not in stem:
        return None
    return stem.rsplit("_", 1)[-1]


def _part_cluster_meta(parts: List[str], part_prefix: str = "part") -> dict:
    """Map parts -> cluster id, and aggregate per-cluster header center + bbox.
    Header-only (laspy.open .mins/.maxs) — no point read. Raises when any part
    is not cluster-identifiable (overlap=drop cannot assign ownership)."""
    import laspy

    per_part: dict = {}
    center: dict = {}
    bbox: dict = {}  # cid -> [xmin, xmax, ymin, ymax]
    for p in parts:
        cid = _cluster_id_of(p, part_prefix)
        if cid is None:
            raise ValueError(
                f"lidar_gbx overlap=drop: part {os.path.basename(p)!r} is not "
                "cluster-identifiable (needs *_<group>_<cluster>.laz naming); "
                "cannot assign seam ownership."
            )
        per_part[p] = cid
        with laspy.open(p) as r:
            mins, maxs = r.header.mins, r.header.maxs
        if cid not in bbox:
            bbox[cid] = [mins[0], maxs[0], mins[1], maxs[1]]
        else:  # union when >1 part shares a cluster id
            b = bbox[cid]
            b[0], b[1] = min(b[0], mins[0]), max(b[1], maxs[0])
            b[2], b[3] = min(b[2], mins[1]), max(b[3], maxs[1])
    for cid, b in bbox.items():
        center[cid] = ((b[0] + b[1]) / 2.0, (b[2] + b[3]) / 2.0)
        bbox[cid] = (b[0], b[1], b[2], b[3])
    return {"per_part": per_part, "center": center, "bbox": bbox}


def _merge_laz_parts(inputs: List[str], tmp_path: str, has_rgb: bool, crs) -> int:
    """Concatenate x/y/z(/RGB) from part .laz/.las files, recompute header from
    merged bounds, write ONE .laz at tmp_path via the write_xyz(rgb)_laz path.
    Returns the merged point count. Driver-side (holds all points in RAM — gated
    by caller via _gate_merge / _gate_merge_paths).

    When crs is None the CRS is inferred from the first input part's header so
    the merged file always preserves the parts' projected CRS.
    """
    import laspy

    from databricks.labs.gbx.pyrx.imagery import write_xyz_laz, write_xyzrgb_laz

    # Infer CRS from the first part when the caller did not supply one.
    if crs is None and inputs:
        crs = _infer_crs_from_parts(inputs)

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


def _nearest_owner(x: float, y: float, centers: dict, bboxes: dict) -> str:
    """Cluster id owning point (x,y): nearest center among clusters whose bbox
    covers (x,y). Ties -> lowest id (string-sorted). Covering set is never empty
    (the point's own cluster bbox always contains it)."""
    cand = [
        cid
        for cid, (xmin, xmax, ymin, ymax) in bboxes.items()
        if xmin <= x <= xmax and ymin <= y <= ymax
    ]
    best, best_d = None, None
    for cid in sorted(cand):
        cx, cy = centers[cid]
        d = (x - cx) ** 2 + (y - cy) ** 2
        if best_d is None or d < best_d:
            best, best_d = cid, d
    return best


def _merge_laz_parts_seam(
    inputs: List[str],
    tmp_path: str,
    has_rgb: bool,
    crs,
    *,
    overlap_drop: bool,
    dedupe_exact: bool,
    voxel_size,
    part_prefix: str = "part",
) -> int:
    """Streaming seam-aware merge. Processes one part at a time; keeps a bounded
    working set; writes one .laz via write_xyz(rgb)_laz; returns the kept count.

    This task implements ``overlap_drop`` (seam-point ownership via
    ``_nearest_owner``). ``dedupe_exact`` and ``voxel_size`` are accepted for
    signature stability but are pass-through (wired in Tasks 4/5).
    """
    import laspy

    from databricks.labs.gbx.pyrx.imagery import write_xyz_laz, write_xyzrgb_laz

    if crs is None and inputs:
        crs = _infer_crs_from_parts(inputs)

    meta = _part_cluster_meta(inputs, part_prefix) if overlap_drop else None

    # Kept-point accumulators (Tasks 4/5 replace the plain list with set/dict).
    kx, ky, kz, kr, kg, kb = [], [], [], [], [], []

    def _emit(x, y, z, r, g, b):
        kx.append(x)
        ky.append(y)
        kz.append(z)
        if has_rgb:
            kr.append(r)
            kg.append(g)
            kb.append(b)

    for p in inputs:
        las = laspy.read(p)
        xs = np.asarray(las.x)
        ys = np.asarray(las.y)
        zs = np.asarray(las.z)
        if has_rgb:
            rs = (np.asarray(las.red) >> 8).astype(np.uint8)
            gs = (np.asarray(las.green) >> 8).astype(np.uint8)
            bs = (np.asarray(las.blue) >> 8).astype(np.uint8)
        cid_p = meta["per_part"][p] if overlap_drop else None
        for i in range(len(xs)):
            x, y, z = float(xs[i]), float(ys[i]), float(zs[i])
            if overlap_drop:
                owner = _nearest_owner(x, y, meta["center"], meta["bbox"])
                if owner != cid_p:
                    continue  # another cluster owns this seam point
            r = int(rs[i]) if has_rgb else 0
            g = int(gs[i]) if has_rgb else 0
            b = int(bs[i]) if has_rgb else 0
            _emit(x, y, z, r, g, b)

    # (Task 5 inserts the cells->kx materialization here, before the guard.)
    if not kx:
        return 0  # nothing survived; caller (Task 6) retains parts, skips publish
    if has_rgb:
        write_xyzrgb_laz(
            tmp_path,
            np.asarray(kx),
            np.asarray(ky),
            np.asarray(kz),
            np.asarray(kr, np.uint8),
            np.asarray(kg, np.uint8),
            np.asarray(kb, np.uint8),
            crs=crs,
        )
    else:
        write_xyz_laz(tmp_path, np.asarray(kx), np.asarray(ky), np.asarray(kz), crs=crs)
    return len(kx)


# ---------------------------------------------------------------------------
# Writer class
# ---------------------------------------------------------------------------


class LidarGbxWriter(DataSourceWriter):
    def __init__(self, options: dict, schema: StructType, overwrite: bool):
        from databricks.labs.gbx.ds._listing import to_local_path

        self.merge = str(options.get("merge", "false")).lower() == "true"
        _reject_v2_options(options)
        self.overlap = str(options.get("overlap", "keep")).lower()
        self.dedupe_exact = str(options.get("dedupeExact", "false")).lower() == "true"
        _vs = options.get("voxelSize")
        self.voxel_size = float(_vs) if _vs not in (None, "") else None
        self._seam = (
            self.overlap == "drop" or self.dedupe_exact or self.voxel_size is not None
        )
        if self._seam and not self.merge:
            raise ValueError(
                "lidar_gbx writer: overlap=drop / dedupeExact / voxelSize requires merge "
                "mode (.option('merge','true')) — they unify existing named .laz parts."
            )
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
        # DataSource options are always strings; convert a numeric EPSG string
        # to int so write_xyz(rgb)_laz reaches the integer EPSG constructor rather
        # than the WKT path, which fails silently for bare EPSG codes.
        _crs_raw = options.get("crs")
        if isinstance(_crs_raw, str):
            try:
                self.crs = int(_crs_raw)
            except (ValueError, TypeError):
                self.crs = _crs_raw  # WKT / authority string — pass through
        else:
            self.crs = _crs_raw
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
        # Group rows by their resolved stem within this partition.
        # repartitionByRange is sampling-based and does NOT guarantee one
        # (group,cluster) per partition, so multiple keys may arrive together.
        # Bucketing by stem here gives deterministic per-key naming regardless
        # of the partitioner.
        buckets: dict = {}  # stem -> {xs, ys, zs, rs, gs, bs}
        for row in iterator:
            stem = self._stem(row)
            if stem not in buckets:
                buckets[stem] = {
                    "xs": [],
                    "ys": [],
                    "zs": [],
                    "rs": [],
                    "gs": [],
                    "bs": [],
                }
            b = buckets[stem]
            b["xs"].append(row["x"])
            b["ys"].append(row["y"])
            b["zs"].append(row["z"])
            if self.has_rgb:
                b["rs"].append(row["r"])
                b["gs"].append(row["g"])
                b["bs"].append(row["b"])
        if not buckets:
            return LidarCommitMessage(paths=[], frag_path="")
        # Write one .laz per distinct stem; local-temp -> shutil.copyfile (FUSE).
        written_paths: list = []
        for stem, b in buckets.items():
            if not b["xs"]:
                continue
            tmp = new_local_temp_file(suffix=".laz")
            written_tmp: Optional[str] = None
            try:
                written_tmp = self._encode_part(
                    tmp,
                    b["xs"],
                    b["ys"],
                    b["zs"],
                    b["rs"],
                    b["gs"],
                    b["bs"],
                )
                ext = os.path.splitext(written_tmp)[1]  # .laz or .las fallback
                out = os.path.join(self.path, f"{stem}{ext}")
                shutil.copyfile(written_tmp, out)
                written_paths.append(out)
            finally:
                for p in {tmp, written_tmp}:
                    if p and os.path.exists(p):
                        try:
                            os.unlink(p)
                        except OSError:
                            pass
        return LidarCommitMessage(paths=written_paths)

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
        Budget is compute-aware via the budget_for('driver_merge') authority.
        mergeMaxMB option or GBX_LIDAR_MERGE_MAX_MB env var overrides.
        """
        import pyarrow.feather as feather

        from databricks.labs.gbx.pyrx.core.budget import budget_for

        total_rows = sum(
            feather.read_table(frag, columns=["x"]).num_rows for frag in frags
        )
        est = total_rows * (32 if self.has_rgb else 24)
        d = budget_for("driver_merge", est, override_mb=self.merge_max_mb)
        if d.action == "error":
            raise ValueError(d.reason)
        print(f"[merge] {d.reason}", flush=True)

    def _gate_merge_paths(
        self, parts: List[str], has_rgb: Optional[bool] = None
    ) -> None:
        """Refuse a driver merge on on-disk .laz/.las files that would exceed RAM.

        Estimates bytes as summed point_count * bytes/point (32 XYZ+RGB, 24 XYZ).
        has_rgb overrides self.has_rgb (needed when self.has_rgb is False in merge
        mode but the on-disk files actually contain RGB). Budget is compute-aware
        via the budget_for('driver_merge') authority. mergeMaxMB option or
        GBX_LIDAR_MERGE_MAX_MB env var overrides.
        """
        from databricks.labs.gbx.pyrx.core.budget import budget_for

        _has_rgb = has_rgb if has_rgb is not None else self.has_rgb
        est = sum(_laz_point_count(p) for p in parts) * (32 if _has_rgb else 24)
        d = budget_for("driver_merge", est, override_mb=self.merge_max_mb)
        if d.action == "error":
            raise ValueError(d.reason)
        print(f"[merge] {d.reason}", flush=True)

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

                tmp = new_local_temp_file(suffix=".laz")
                written = self._encode_part(tmp, xs, ys, zs, rs, gs, bs)
                # If laspy fell back to .las, the .laz placeholder is empty; remove it.
                if written != tmp and os.path.exists(tmp):
                    try:
                        os.unlink(tmp)
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

            tmp_merged = new_local_temp_file(suffix=".laz")

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

        # Scan every part's point format to detect heterogeneous RGB/XYZ mixes.
        # A merged LAS has a single point format; mixing RGB and XYZ-only parts
        # either raises AttributeError (las.red on XYZ-only) or silently drops
        # color — both wrong. Fail loud with a clear message instead.
        _RGB_FORMATS = frozenset((2, 3, 5, 7, 8, 10))
        fmt_ids: list = []
        for p in parts:
            with laspy.open(p) as _r:
                fmt_ids.append(_r.header.point_format.id)
        rgb_flags = [fid in _RGB_FORMATS for fid in fmt_ids]
        if any(rgb_flags) and not all(rgb_flags):
            _rgb_fmts = sorted({fmt_ids[i] for i, f in enumerate(rgb_flags) if f})
            _xyz_fmts = sorted({fmt_ids[i] for i, f in enumerate(rgb_flags) if not f})
            raise ValueError(
                f"lidar_gbx merge: parts have heterogeneous point formats "
                f"(RGB fmt {_rgb_fmts} and XYZ-only fmt {_xyz_fmts}); "
                "merge requires a homogeneous set — merge RGB and XYZ parts separately."
            )
        has_rgb = all(rgb_flags)

        self._gate_merge_paths(parts, has_rgb=has_rgb)
        expected = sum(_laz_point_count(p) for p in parts)

        tmp_merged = new_local_temp_file(suffix=".laz")
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
