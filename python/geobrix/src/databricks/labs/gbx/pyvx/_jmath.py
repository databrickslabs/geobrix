"""Java's ``Math.hypot``, bit for bit: a port of fdlibm ``__ieee754_hypot`` (``FdLibm.Hypot`` in the JDK).

JTS 1.20 measures every distance with ``Math.hypot`` (``Coordinate.distance``). The conforming triangulation compares
those distances (inside a circle, within a tolerance), so the Python level must compute the same doubles as the JVM
to make the same choices. CPython's ``math.hypot`` is more accurate than fdlibm, and so differs in the last bit now
and then: it cannot stand in.
"""

import math
import struct

_D = struct.Struct("<d")
_Q = struct.Struct("<Q")


def _bits(x: float) -> int:
    return _Q.unpack(_D.pack(x))[0]


def _from_bits(bits: int) -> float:
    return _D.unpack(_Q.pack(bits & 0xFFFFFFFFFFFFFFFF))[0]


def _hi(x: float) -> int:
    return _bits(x) >> 32


def _lo(x: float) -> int:
    return _bits(x) & 0xFFFFFFFF


def _with_hi(x: float, hi: int) -> float:
    return _from_bits(((hi & 0xFFFFFFFF) << 32) | (_bits(x) & 0xFFFFFFFF))


def hypot(x: float, y: float) -> float:
    """``sqrt(x*x + y*y)`` without undue overflow or underflow, rounded exactly as Java's ``Math.hypot``."""
    ha = _hi(x) & 0x7FFFFFFF
    hb = _hi(y) & 0x7FFFFFFF
    if hb > ha:
        a, b = y, x
        ha, hb = hb, ha
    else:
        a, b = x, y
    a = _with_hi(a, ha)  # |a|
    b = _with_hi(b, hb)  # |b|
    if (ha - hb) > 0x3C00000:  # a/b > 2**60
        return a + b
    k = 0
    if ha > 0x5F300000:  # a > 2**500
        if ha >= 0x7FF00000:  # Inf or NaN
            w = a + b  # for sNaN
            if ((ha & 0xFFFFF) | _lo(a)) == 0:
                w = a
            if ((hb ^ 0x7FF00000) | _lo(b)) == 0:
                w = b
            return w
        # scale a and b by 2**-600
        ha -= 0x25800000
        hb -= 0x25800000
        k += 600
        a = _with_hi(a, ha)
        b = _with_hi(b, hb)
    if hb < 0x20B00000:  # b < 2**-500
        if hb <= 0x000FFFFF:  # subnormal b or 0
            if (hb | _lo(b)) == 0:
                return a
            t1 = _from_bits(0x7FD00000 << 32)  # 2**1022
            b *= t1
            a *= t1
            k -= 1022
            # fdlibm (and the JDK's port) keep the words read before this scaling: so does this port
        else:  # scale a and b by 2**600
            ha += 0x25800000
            hb += 0x25800000
            k -= 600
            a = _with_hi(a, ha)
            b = _with_hi(b, hb)
    # medium size a and b
    w = a - b
    if w > b:
        t1 = _from_bits((ha & 0xFFFFFFFF) << 32)
        t2 = a - t1
        w = math.sqrt(t1 * t1 - (b * (-b) - t2 * (a + t1)))
    else:
        a = a + a
        y1 = _from_bits((hb & 0xFFFFFFFF) << 32)
        y2 = b - y1
        t1 = _from_bits(((ha + 0x00100000) & 0xFFFFFFFF) << 32)
        t2 = a - t1
        w = math.sqrt(t1 * y1 - (w * (-w) - (t1 * y2 + t2 * b)))
    if k != 0:
        return _from_bits((_hi(1.0) + (k << 20)) << 32) * w
    return w
