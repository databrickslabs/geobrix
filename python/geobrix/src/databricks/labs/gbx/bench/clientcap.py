"""A cap on what a bench input builds on the client.

``spark.createDataFrame(rows)`` ships every row from the driver process (through Spark Connect on Serverless
and standard clusters, where the client is the notebook's own process). A probe that built 4 GB of tile bytes
this way sat for 20 minutes and was cancelled. An input that large is built on the executors instead
(``spark.read.format("binaryFile")``, ``spark.range``): this module refuses the client build before it starts.
"""

from __future__ import annotations

from typing import Optional

MAX_CLIENT_BYTES = (
    64 * 2**20
)  # 64 MiB: a client-side input beyond this is built on the executors


class ClientInputTooLarge(ValueError):
    """A DataFrame an input builds on the client would exceed the cap."""


def _fix(what: str, size: int, cap: int) -> str:
    return (
        f"{what}: {size / 2**20:.0f} MiB would be built on the client, above the {cap / 2**20:.0f} MiB cap. "
        "Fix: build it on the executors (spark.read.format('binaryFile') over files, spark.range for "
        "generated rows), or lower the row count."
    )


def check_estimate(
    what: str, rows: int, row_bytes: int, cap: int = MAX_CLIENT_BYTES
) -> int:
    """Refuse a client-side input of ``rows`` rows of about ``row_bytes`` bytes each above ``cap``; return its size."""
    size = int(rows) * int(row_bytes)
    if size > cap:
        raise ClientInputTooLarge(_fix(what, size, cap))
    return size


def size_of(value) -> int:
    """The bytes a value takes when shipped: bytes and strings by length, containers by their members, 8 otherwise."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return len(value)
    if isinstance(value, str):
        return len(value)
    if isinstance(value, dict):
        return sum(size_of(k) + size_of(v) for k, v in value.items())
    if isinstance(value, (list, tuple)):
        return sum(size_of(v) for v in value)
    return 8


def client_dataframe(spark, data, schema, *, what: str, cap: Optional[int] = None):
    """``spark.createDataFrame(data, schema)`` for a list of rows, refused when the rows exceed the cap."""
    data = list(data)
    cap = MAX_CLIENT_BYTES if cap is None else cap
    size = size_of(data)
    if size > cap:
        raise ClientInputTooLarge(_fix(what, size, cap))
    return spark.createDataFrame(data, schema)
