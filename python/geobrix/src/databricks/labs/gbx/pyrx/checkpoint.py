"""Volume-backed checkpointing for long multi-stage pipelines (orthomosaic series).

A per-group manifest JSON on a durable store (UC Volume) records, per (stage, key),
an input-signature hash + the output path. A stage skips work when its manifest
entry matches the current input signature AND the output still exists — so a re-run
after a cancel/fail processes only what is missing. The cluster is the checkpoint
unit for per-cluster stages (dense); the group is the unit for whole-group stages
(mosaic, corrected, cog, pmtiles).

Serverless-safe: stdlib only, no Spark. Volume-safe: the manifest is written to a
LOCAL temp then copied to the (FUSE) Volume path — never renamed across FUSE.
"""
import hashlib
import json
import os
import shutil
import tempfile
import threading
import time
from pathlib import Path


def input_signature(*parts) -> str:
    """Stable 16-hex-char signature of the inputs that define an output.

    ``parts`` are JSON-serialisable values (knob dicts, sorted file lists, sizes,
    upstream signatures). Ordering is significant — the caller passes a stable order.
    """
    h = hashlib.sha256()
    for part in parts:
        h.update(json.dumps(part, sort_keys=True, default=str).encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()[:16]


class Manifest:
    """Per-group checkpoint manifest persisted as JSON on a (Volume) path.

    Thread-safe for in-run concurrent updates (e.g. the dense GPU pool's threads).
    One process writes at a time across runs, so cross-run contention is not a
    concern. A missing or corrupt manifest loads as empty (first run).
    """

    def __init__(self, path):
        self.path = str(path)
        self._lock = threading.Lock()
        self._data = self._load()

    def _load(self) -> dict:
        try:
            with open(self.path) as f:
                d = json.load(f)
            return d if isinstance(d, dict) else {}
        except (FileNotFoundError, ValueError, OSError):
            return {}

    def get_sig(self, stage, key):
        entry = self._data.get(stage, {}).get(str(key))
        return entry.get("sig") if entry else None

    def is_done(self, stage, key, sig) -> bool:
        entry = self._data.get(stage, {}).get(str(key))
        if not entry or entry.get("sig") != sig:
            return False
        out = entry.get("output")
        return bool(out) and Path(out).exists()

    def mark_done(self, stage, key, sig, output, **meta):
        with self._lock:
            self._data.setdefault(stage, {})[str(key)] = {
                "sig": sig,
                "output": str(output),
                "ts": time.time(),
                **meta,
            }
            self._save()

    def _save(self):
        # FUSE-safe: write a LOCAL temp then copy to the Volume path (no rename).
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(tempfile.gettempdir()) / f"{Path(self.path).name}.{os.getpid()}.tmp"
        with open(tmp, "w") as f:
            json.dump(self._data, f, indent=1)
        shutil.copy(str(tmp), self.path)
        tmp.unlink(missing_ok=True)


def checkpoint_skip(manifest, stage, key, sig, *, force=False) -> bool:
    """True when this (stage, key) can be skipped: not forced and already done."""
    return (not force) and manifest.is_done(stage, key, sig)
