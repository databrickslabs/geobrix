"""Chunk and shard key encodings for Zarr v2 and v3 arrays."""

from __future__ import annotations

from dataclasses import dataclass

from databricks.labs.gbx.zarr.errors import ZarrUnsupported


@dataclass(frozen=True)
class KeyEncoding:
    zarr_format: int
    separator: str
    prefix: str

    def chunk_key(self, coords: tuple[int, ...]) -> str:
        if not coords:
            return self.prefix or "0"
        body = self.separator.join(str(int(c)) for c in coords)
        return f"{self.prefix}{self.separator}{body}" if self.prefix else body

    def parse(self, key: str, ndim: int) -> tuple[int, ...]:
        if ndim == 0:
            if key != (self.prefix or "0"):
                raise ValueError(f"not a 0-d chunk key: {key!r}")
            return ()
        body = key
        if self.prefix:
            head = self.prefix + self.separator
            if not key.startswith(head):
                raise ValueError(f"not a chunk key: {key!r}")
            body = key[len(head) :]
        parts = body.split(self.separator)
        if len(parts) != ndim or not all(p.isdigit() for p in parts):
            raise ValueError(f"not a {ndim}-d chunk key: {key!r}")
        return tuple(int(p) for p in parts)


def encoding_for(zarr_format: int, doc: dict) -> KeyEncoding:
    if zarr_format == 2:
        return KeyEncoding(2, doc.get("dimension_separator") or ".", "")
    enc = doc.get("chunk_key_encoding") or {"name": "default"}
    sep = (enc.get("configuration") or {}).get("separator")
    if enc.get("name") == "default":
        return KeyEncoding(3, sep or "/", "c")
    if enc.get("name") == "v2":
        return KeyEncoding(3, sep or ".", "")
    raise ZarrUnsupported(
        f"chunk key encoding {enc.get('name')!r} is not supported",
        fix="re-encode the store with the 'default' or 'v2' chunk key encoding",
    )


def chunks_per_shard(
    shard_shape: tuple[int, ...], chunk_shape: tuple[int, ...]
) -> tuple[int, ...]:
    out = []
    for s, c in zip(shard_shape, chunk_shape):
        if s % c:
            raise ZarrUnsupported(
                f"shard shape {tuple(shard_shape)} is not a multiple of chunk shape "
                f"{tuple(chunk_shape)}",
                fix="rewrite the array with a shard shape that is a multiple of its chunk shape",
            )
        out.append(s // c)
    return tuple(out)


def shard_of(chunk_coords: tuple[int, ...], cps: tuple[int, ...]) -> tuple[int, ...]:
    return tuple(c // n for c, n in zip(chunk_coords, cps))


def inner_index(chunk_coords: tuple[int, ...], cps: tuple[int, ...]) -> int:
    """Position of a chunk within its shard, C-order."""
    idx = 0
    for c, n in zip(chunk_coords, cps):
        idx = idx * n + (c % n)
    return idx


def inner_coords(
    shard_coords: tuple[int, ...], index: int, cps: tuple[int, ...]
) -> tuple[int, ...]:
    """Chunk coordinates of the ``index``-th (C-order) chunk of a shard."""
    local = []
    for n in reversed(cps):
        local.append(index % n)
        index //= n
    local.reverse()
    return tuple(s * n + loc for s, n, loc in zip(shard_coords, cps, local))
