"""Generate the benchmark row pool on Databricks, byte-identical to the local generator.

``rows/r<j>.tif`` is a pure function of ``(j, seed, srids, tile_px, bands, dtype)``
(:func:`databricks.labs.gbx.bench.datagen.row_tile_srid_seed` and ``make_tile_bytes``), so the
tiles can be made where the Volume is instead of uploaded from a workstation.

The fan-out is a DataFrame of tile indices repartitioned by the index column: that is the only
parallelism a Serverless notebook has. Each task writes its tiles to the Volume through the
FUSE mount, from a local temp file. A tile whose file already has the expected size is skipped,
so a rerun is idempotent. Nothing on the Volume is ever deleted.

The driver works through the range in chunks and prints a ``[GENCORPUS]`` progress line after
each, plus a heartbeat line while a chunk runs, so a long run is not a black box.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import tempfile
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, Iterator, List, Sequence

from databricks.labs.gbx.bench import datagen as _dg
from databricks.labs.gbx.core._retry import _retry_transient

TAG = "[GENCORPUS]"
DEFAULT_SRIDS = (4326, 3857, 32618, 27700)


@dataclass(frozen=True)
class RowSpec:
    """The row pool's generation parameters (the local ``gbx:bench:gen-data`` defaults)."""

    seed: int = 1234
    srids: tuple = DEFAULT_SRIDS
    tile_px: int = 1024
    bands: int = 4
    dtype: str = "float32"

    def as_dict(self) -> dict:
        d = asdict(self)
        d["srids"] = list(self.srids)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "RowSpec":
        d = dict(d)
        d["srids"] = tuple(int(s) for s in d.get("srids", DEFAULT_SRIDS))
        return cls(**d)


# --- pure parts --------------------------------------------------------------------


def index_range(rows_from: int, rows_to: int) -> range:
    """The tile indices ``rows_from..rows_to``, both ends included."""
    if rows_from < 0 or rows_to < rows_from:
        raise ValueError(
            f"need 0 <= rows_from <= rows_to (got {rows_from}..{rows_to}); both ends are included"
        )
    return range(rows_from, rows_to + 1)


def chunks(indices: Sequence[int], size: int) -> Iterator[Sequence[int]]:
    """Consecutive slices of *indices* of at most *size* items."""
    if size < 1:
        raise ValueError("chunk size must be at least 1")
    for i in range(0, len(indices), size):
        yield indices[i : i + size]


def row_tile_bytes(j: int, spec: RowSpec) -> bytes:
    """The bytes of ``rows/r<j>.tif``: what ``datagen.generate_corpus`` writes for index *j*."""
    srid, tile_seed = _dg.row_tile_srid_seed(j, spec.seed, spec.srids)
    return _dg.make_tile_bytes(
        spec.tile_px, spec.bands, spec.dtype, srid, 0.0, tile_seed
    )


def expected_sizes(spec: RowSpec) -> Dict[int, int]:
    """File size by SRID. The tiles are uncompressed, so a tile's size depends on its CRS only
    (the GeoTIFF keys differ in length); one tile per SRID is generated to learn each.
    """
    sizes: Dict[int, int] = {}
    for j in range(len(spec.srids)):
        srid, _ = _dg.row_tile_srid_seed(j, spec.seed, spec.srids)
        if srid not in sizes:
            sizes[srid] = len(row_tile_bytes(j, spec))
    return sizes


def tile_path(out_dir, j: int) -> Path:
    return Path(out_dir) / "rows" / f"r{j}.tif"


def _expected_size_of(j: int, spec: RowSpec, sizes: Dict[int, int]) -> int:
    srid, _ = _dg.row_tile_srid_seed(j, spec.seed, spec.srids)
    return sizes[srid]


def _size_or_none(path: Path):
    try:
        return _retry_transient(lambda: path.stat().st_size, attempts=3, backoff=0.2)
    except OSError:
        return None


def write_row_tile(
    j: int,
    out_dir,
    spec: RowSpec,
    sizes: Dict[int, int],
    skip_existing: bool = True,
) -> tuple:
    """Write ``rows/r<j>.tif`` under *out_dir*; returns ``(status, nbytes, seconds)``.

    ``skipped`` when the file already has the expected size (and *skip_existing*); otherwise the
    tile is generated into a local temp file and copied over the Volume path (sequential I/O, no
    seek on the Volume). A short file left by an interrupted run is overwritten in place; nothing
    is deleted from *out_dir*.
    """
    t0 = time.monotonic()
    dest = tile_path(out_dir, j)
    want = _expected_size_of(j, spec, sizes)
    if skip_existing and _size_or_none(dest) == want:
        return "skipped", want, time.monotonic() - t0
    data = row_tile_bytes(j, spec)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td) / dest.name
        tmp.write_bytes(data)
        _retry_transient(lambda: shutil.copyfile(tmp, dest))
    got = _size_or_none(dest)
    if got != len(data):
        raise OSError(f"{dest}: wrote {len(data)} bytes, found {got}")
    return "written", len(data), time.monotonic() - t0


def generate_partition(
    js: Iterable[int],
    out_dir,
    spec_dict: dict,
    sizes: Dict[int, int],
    skip_existing: bool = True,
) -> Iterator[tuple]:
    """The work of one task: ``(j, status, nbytes, seconds)`` per index."""
    spec = RowSpec.from_dict(spec_dict)
    for j in js:
        j = int(j)
        status, nbytes, secs = write_row_tile(j, out_dir, spec, sizes, skip_existing)
        yield j, status, nbytes, secs


# --- the Spark fan-out ---------------------------------------------------------------

_RESULT_SCHEMA = "j long, status string, nbytes long, secs double"


def _run_chunk(
    spark, js, out_dir, spec, sizes, partitions, skip_existing
) -> List[tuple]:
    out_dir_s, spec_d, sizes_d = str(out_dir), spec.as_dict(), dict(sizes)

    def _task(batches):
        import pyarrow as pa

        for batch in batches:
            rows = list(
                generate_partition(
                    batch.column("j").to_pylist(),
                    out_dir_s,
                    spec_d,
                    sizes_d,
                    skip_existing,
                )
            )
            yield pa.RecordBatch.from_pylist(
                [dict(j=r[0], status=r[1], nbytes=r[2], secs=r[3]) for r in rows],
                schema=pa.schema(
                    [
                        ("j", pa.int64()),
                        ("status", pa.string()),
                        ("nbytes", pa.int64()),
                        ("secs", pa.float64()),
                    ]
                ),
            )

    df = spark.createDataFrame([(int(j),) for j in js], "j long")
    # Serverless parallelism comes only from repartition(N, column).
    df = df.repartition(partitions, "j").mapInArrow(_task, _RESULT_SCHEMA)
    return [tuple(r) for r in df.collect()]


def run_generation(
    spark,
    out_dir,
    indices: Sequence[int],
    spec: RowSpec,
    *,
    partitions: int = 64,
    chunk: int = 512,
    skip_existing: bool = True,
    say: Callable[[str], None] = None,
    heartbeat_s: float = 60.0,
) -> dict:
    """Generate the tiles *indices* under ``<out_dir>/rows`` and return the counts.

    Prints ``[GENCORPUS] progress`` after each chunk and ``[GENCORPUS] running`` every
    *heartbeat_s* while a chunk is in flight.
    """

    def _say(msg):
        (say or (lambda m: print(m, flush=True)))(msg)

    indices = list(indices)
    total = len(indices)
    sizes = expected_sizes(spec)
    state = {"done": 0, "written": 0, "skipped": 0, "bytes": 0, "chunk": 0}
    t0 = time.monotonic()
    n_chunks = -(-total // chunk) if total else 0
    _say(
        f"{TAG} start {total} tiles r{indices[0]}..r{indices[-1]} -> {out_dir}/rows "
        f"({n_chunks} chunks of up to {chunk}, {partitions} partitions per chunk, "
        f"skip_existing={skip_existing})"
    )

    stop = threading.Event()

    def _beat():
        while not stop.wait(heartbeat_s):
            _say(
                f"{TAG} running: done {state['done']}/{total} "
                f"(chunk {state['chunk']}/{n_chunks} in flight, "
                f"elapsed {time.monotonic() - t0:.0f}s)"
            )

    beat = threading.Thread(target=_beat, daemon=True)
    beat.start()
    try:
        for js in chunks(indices, chunk):
            state["chunk"] += 1
            res = _run_chunk(spark, js, out_dir, spec, sizes, partitions, skip_existing)
            if len(res) != len(js):
                raise RuntimeError(
                    f"chunk {state['chunk']}: {len(js)} indices in, {len(res)} results back"
                )
            for _j, status, nbytes, _secs in res:
                state[status] += 1
                state["bytes"] += nbytes
            state["done"] += len(res)
            el = time.monotonic() - t0
            rate = state["done"] / el if el else 0.0
            eta = (total - state["done"]) / rate if rate else 0.0
            _say(
                f"{TAG} progress: done {state['done']}/{total} "
                f"(written {state['written']}, skipped {state['skipped']}) "
                f"chunk {state['chunk']}/{n_chunks} elapsed {el:.0f}s "
                f"rate {rate:.2f}/s eta {eta:.0f}s"
            )
    finally:
        stop.set()
        beat.join(timeout=5)
    return dict(
        total=total,
        done=state["done"],
        written=state["written"],
        skipped=state["skipped"],
        bytes=state["bytes"],
        seconds=round(time.monotonic() - t0, 1),
    )


# --- verification --------------------------------------------------------------------


def inventory(
    out_dir, indices: Iterable[int], spec: RowSpec, sizes: Dict[int, int]
) -> dict:
    """Which of *indices* are on the Volume at the expected size, short, or missing."""
    rows = Path(out_dir) / "rows"
    present = {}
    for e in _retry_transient(lambda: list(os.scandir(rows))):
        if e.name.startswith("r") and e.name.endswith(".tif"):
            present[e.name] = _retry_transient(lambda e=e: e.stat().st_size)
    complete, short, missing, total_bytes = 0, [], [], 0
    idx = list(indices)
    for j in idx:
        sz = present.get(f"r{j}.tif")
        if sz is None:
            missing.append(j)
        elif sz == _expected_size_of(j, spec, sizes):
            complete += 1
            total_bytes += sz
        else:
            short.append(j)
    return dict(
        expected=len(idx),
        complete=complete,
        short=short,
        missing=missing,
        bytes=total_bytes,
        tif_files_in_dir=len(present),
    )


def sha_sample(out_dir, indices: Iterable[int]) -> dict:
    """``{str(j): {sha256, size}}`` for the tiles *indices* (None when the file is absent)."""
    out = {}
    for j in indices:
        p = tile_path(out_dir, j)
        if not p.is_file():
            out[str(j)] = {"sha256": None, "size": None}
            continue
        h = hashlib.sha256()
        n = 0
        with open(p, "rb") as f:
            for blk in iter(lambda: f.read(1 << 20), b""):
                h.update(blk)
                n += len(blk)
        out[str(j)] = {"sha256": h.hexdigest(), "size": n}
    return out


# --- smoke -----------------------------------------------------------------------------


def _probe_and_sha(spark, spec: RowSpec, indices: Sequence[int]) -> list:
    """On workers: the install check and the sha256 of each tile generated there (no Volume I/O)."""
    spec_d = spec.as_dict()

    def _task(batches):
        import pyarrow as pa

        for batch in batches:
            out = []
            for j in batch.column("j").to_pylist():
                import databricks.labs.gbx as _g

                b = row_tile_bytes(int(j), RowSpec.from_dict(spec_d))
                out.append(
                    dict(
                        j=int(j),
                        sha=hashlib.sha256(b).hexdigest(),
                        info=json.dumps(
                            dict(
                                version=getattr(_g, "__version__", "?"),
                                arch=platform.machine(),
                            )
                        ),
                    )
                )
            yield pa.RecordBatch.from_pylist(
                out,
                schema=pa.schema(
                    [("j", pa.int64()), ("sha", pa.string()), ("info", pa.string())]
                ),
            )

    df = spark.createDataFrame([(int(j),) for j in indices], "j long")
    df = df.repartition(len(indices), "j").mapInArrow(
        _task, "j long, sha string, info string"
    )
    return [tuple(r) for r in df.collect()]


def smoke(
    spark,
    spec: RowSpec,
    indices: Sequence[int],
    want_sha: Dict[str, str],
    say: Callable[[str], None] = None,
) -> dict:
    """Prove the workers have geobrix and make the same bytes as the host: no Volume I/O."""
    _say = say or (lambda m: print(m, flush=True))
    result = dict(smoke=True, import_check={"ok": False}, mismatch=[], ok=False)
    try:
        rows = _probe_and_sha(spark, spec, indices)
    except Exception as e:  # noqa: BLE001 - reported in the payload
        result["import_check"] = dict(
            ok=False, error=(type(e).__name__ + ": " + str(e))[-600:]
        )
        _say(f"{TAG} smoke: worker import FAILED: {result['import_check']['error']}")
        return result
    info = json.loads(rows[0][2])
    result["import_check"] = dict(ok=True, **info)
    result["mismatch"] = sorted(j for j, sha, _ in rows if want_sha.get(str(j)) != sha)
    result["ok"] = not result["mismatch"]
    _say(
        f"{TAG} smoke: workers geobrix {info['version']} on {info['arch']}; "
        f"{len(rows) - len(result['mismatch'])}/{len(rows)} tiles equal the host bytes"
    )
    return result


# --- the notebook entry ------------------------------------------------------------------


def notebook_main(spark, cfg: dict, say: Callable[[str], None] = None) -> dict:
    """What the runner notebook calls; returns the JSON-able exit payload."""
    _say = say or (lambda m: print(m, flush=True))
    spec = RowSpec.from_dict(cfg["spec"])
    _say(f"{TAG} run {cfg['run_id']} mode {cfg['mode']} on {platform.machine()}")
    if cfg["mode"] == "smoke":
        return smoke(spark, spec, cfg["smoke_indices"], cfg["smoke_sha"], say=_say)
    idx = index_range(cfg["rows_from"], cfg["rows_to"])
    summary = run_generation(
        spark,
        cfg["out_dir"],
        idx,
        spec,
        partitions=cfg["partitions"],
        chunk=cfg["chunk"],
        skip_existing=cfg["skip_existing"],
        say=_say,
    )
    sizes = expected_sizes(spec)
    inv = inventory(cfg["out_dir"], idx, spec, sizes)
    _say(
        f"{TAG} inventory: {inv['complete']}/{inv['expected']} complete, "
        f"{len(inv['short'])} short, {len(inv['missing'])} missing, "
        f"{inv['bytes']} bytes; {inv['tif_files_in_dir']} r*.tif in the directory"
    )
    shas = sha_sample(cfg["out_dir"], cfg.get("verify_indices", []))
    for j, v in shas.items():
        _say(f"{TAG} sha r{j}.tif {v['size']} {v['sha256']}")
    inv_small = dict(inv, short=inv["short"][:50], missing=inv["missing"][:50])
    return dict(
        ok=not inv["short"] and not inv["missing"],
        summary=summary,
        inventory=inv_small,
        sha=shas,
    )


# --- the launcher's pure parts -------------------------------------------------------------

CANONICAL_SAMPLE = (0, 999, 1000, 5000, 9999)


def default_verify_indices(rows_from: int, rows_to: int) -> List[int]:
    """The tiles whose sha256 is checked: the range's ends and middle, and the canonical sample
    (0, 999, 1000, 5000, 9999) where it falls inside the range."""
    pick = {rows_from, (rows_from + rows_to) // 2, rows_to}
    pick.update(j for j in CANONICAL_SAMPLE if rows_from <= j <= rows_to)
    return sorted(pick)


_PROGRESS_RE = None


def last_progress(text: str):
    """The last ``[GENCORPUS] progress|running|start`` line in a run's exported notebook text
    (JSON/HTML escapes undone), or None."""
    import re

    global _PROGRESS_RE
    if _PROGRESS_RE is None:
        _PROGRESS_RE = re.compile(
            r"\[GENCORPUS\] (?:progress|running|start|inventory)[^\n\\<\"]*"
        )
    found = _PROGRESS_RE.findall(text or "")
    return found[-1].strip() if found else None


def progress_done(line):
    """``(done, total)`` from a progress/running line, else None."""
    import re

    m = re.search(r"done (\d+)/(\d+)", line or "")
    return (int(m.group(1)), int(m.group(2))) if m else None


def compare_payload(
    payload: dict, local_sha: Dict[str, str], expected: int
) -> List[str]:
    """What is wrong with a finished run's exit payload (empty list = verified): the count of
    complete tiles, and each sampled sha256 against the local file's."""
    problems = []
    inv = payload.get("inventory") or {}
    if inv.get("complete") != expected:
        problems.append(
            f"{inv.get('complete')} of {expected} tiles complete "
            f"(short {inv.get('short')}, missing {inv.get('missing')})"
        )
    shas = payload.get("sha") or {}
    for j, want in sorted(local_sha.items(), key=lambda kv: int(kv[0])):
        got = (shas.get(j) or {}).get("sha256")
        if got != want:
            problems.append(f"r{j}.tif sha256 {got} != local {want}")
    for j in shas:
        if j not in local_sha:
            problems.append(f"r{j}.tif was hashed on Databricks but not locally")
    return problems


# --- the runner notebook -----------------------------------------------------------------

_CELL_ENV = """import importlib.metadata as _md, platform as _pf
print("[GENCORPUS] geobrix", _md.version("geobrix"), "driver", _pf.machine(), flush=True)
"""

_CELL_MAIN = '''import json
from databricks.labs.gbx.bench import datagen_remote as _dr
CFG = json.loads(r"""{cfg_json}""")
RESULT = _dr.notebook_main(spark, CFG)
'''

# The exit MUST be its own trailing cell: dbutils.notebook.exit ends the run.
_CELL_EXIT = """import json
dbutils.notebook.exit(json.dumps(RESULT))
"""


def _cell(source: str) -> dict:
    return {
        "cell_type": "code",
        "metadata": {},
        "outputs": [],
        "execution_count": None,
        "source": source.splitlines(keepends=True),
    }


def build_gen_notebook(cfg: dict) -> dict:
    """The runner notebook (ipynb dict). The wheel comes from the Serverless environment spec."""
    cfg_json = json.dumps(cfg, sort_keys=True)
    assert '"""' not in cfg_json
    return {
        "cells": [
            _cell(_CELL_ENV),
            _cell(_CELL_MAIN.replace("{cfg_json}", cfg_json)),
            _cell(_CELL_EXIT),
        ],
        "metadata": {},
        "nbformat": 4,
        "nbformat_minor": 5,
    }
