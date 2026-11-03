"""Parameter and geometry reads shared by the light grid functions and the Unity Catalog handlers.

Import-light on purpose: numpy and shapely only, no pyspark and no pandas, so a UC Python sandbox can run them.
"""

from __future__ import annotations

import numpy as np

from ._dilate import MODES as _MODES
from ._geom import parse_geom


def _dilate_check_mode(mode):
    """Raise ValueError for an unknown dilation mode (parameter error).

    ``None`` is allowed and treated as the default mode by callers.
    Other dilation functions reuse this helper — keep it at module scope.
    """
    if mode is not None and mode not in _MODES:
        raise ValueError(f"unknown mode {mode!r}; expected one of {_MODES}")


def _parse_finite(geom):
    """Parse a geometry ONCE: ``(geom, finite)``, ``finite`` False when it has a NaN or infinite coordinate.

    ``geom`` is the parsed geometry, which the grid engine takes as is (no second parse); an unreadable one
    comes back unchanged with ``finite`` True, so the engine's own data-error rule (NULL) applies.
    """
    from shapely import get_coordinates

    try:
        g = parse_geom(geom)
    except Exception:  # noqa: BLE001 - unreadable geometry DATA is the caller's NULL
        return geom, True
    if g is None:
        return geom, True
    return g, bool(np.isfinite(get_coordinates(g)).all())


def _norm_res(res):
    """Normalize a SQL resolution arg (Int index or resolutionMap string).

    _bng.get_resolution validates Int ±1..±6 / resolutionMap keys and rejects
    metres-as-Int; the _bng wrapper fns call it again, so pass the raw value
    through with only numpy-scalar unwrapping.
    """
    if isinstance(res, (np.integer,)):
        return int(res)
    if isinstance(res, (float, np.floating)) and float(res).is_integer():
        # A NULL in an INT column makes pandas hand the whole batch over as floats (3.0).
        return int(res)
    return res


def _whole_res(fn, res) -> int:
    """A non-NULL resolution as the int an integer-resolution grid (quadbin, custom) takes.

    A whole number is that int, whether it arrives as an int, a float (``3.0``: a NULL in an
    INT column made pandas hand the batch over as floats) or text (``'3'``, ``' 3 '``,
    ``'3.0'``: SQL callers pass the resolution as a string). Anything else, ``3.5`` or ``'abc'``,
    raises naming the value instead of being cut to a whole number. BNG does not come through
    here: its resolutions are also strings (``'1km'``) that mean something else.
    """
    value = res
    if isinstance(res, str):
        try:
            value = float(res.strip())
        except ValueError:
            value = None
    if value is None or not float(value).is_integer():
        raise ValueError(f"{fn}: resolution must be a whole number; got {res!r}")
    return int(value)
