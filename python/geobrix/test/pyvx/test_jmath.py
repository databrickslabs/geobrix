"""``_jmath.hypot`` is Java's ``Math.hypot`` bit for bit: the fixture holds 1164 inputs (coordinate differences at many
magnitudes, near-equal and very unequal pairs, subnormals, huge values, random bit patterns, zeros, infinities and NaN)
and the results Java 17 gave for them (``HypotCheck.java``)."""

import math
import struct
from pathlib import Path

from databricks.labs.gbx.pyvx._jmath import hypot

_FIXTURE = Path(__file__).parent / "data" / "java_hypot.txt"


def _f(h: str) -> float:
    return struct.unpack("<d", struct.pack("<Q", int(h, 16)))[0]


def _rows():
    for line in _FIXTURE.read_text().splitlines():
        if line and not line.startswith("#"):
            x, y, want = line.split()
            yield _f(x), _f(y), int(want, 16)


def test_hypot_matches_java_bit_for_bit():
    rows = list(_rows())
    assert len(rows) > 1000
    bad = []
    for x, y, want in rows:
        got = hypot(x, y)
        bits = struct.unpack("<Q", struct.pack("<d", got))[0]
        if bits != want and not (math.isnan(got) and math.isnan(_f(f"{want:016x}"))):
            bad.append((x, y, got, _f(f"{want:016x}")))
    assert not bad, bad[:5]


def test_cpython_hypot_is_not_java_hypot():
    # Why the port exists: CPython's correctly rounded hypot differs from fdlibm's in the last bit on some inputs.
    differs = sum(
        1
        for x, y, want in _rows()
        if not math.isnan(math.hypot(x, y))
        and struct.unpack("<Q", struct.pack("<d", math.hypot(x, y)))[0] != want
    )
    assert differs > 0
