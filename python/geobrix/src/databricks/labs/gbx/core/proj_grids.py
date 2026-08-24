"""Driver-side registry of user-supplied PROJ grid-shift directories.

This holds ONLY driver-side state. Workers never read it — the light tier
embeds the dir list into UDF closures (see pyrx._udf), and the heavy tier
folds it into ExpressionConfig. Storing it here keeps the light path
`_jvm`-free and tier-neutral.
"""

from typing import List, Sequence, Union

_REGISTERED: List[str] = []


def _normalize(dirs: Union[str, Sequence[str]]) -> List[str]:
    if isinstance(dirs, str):
        return [dirs]
    if isinstance(dirs, (list, tuple)):
        for d in dirs:
            if not isinstance(d, str):
                raise TypeError(f"proj grid dir must be a str, got {type(d).__name__}")
        return list(dirs)
    raise TypeError(f"dirs must be a str or a sequence of str, got {type(dirs).__name__}")


def _dedupe(dirs: Sequence[str]) -> List[str]:
    seen: set = set()
    out: List[str] = []
    for d in dirs:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out


def set_registered_dirs(dirs: Union[str, Sequence[str]], *, replace: bool = False) -> List[str]:
    """Accumulate (default) or replace the registered grid dirs; de-dupe, keep order."""
    global _REGISTERED
    incoming = _normalize(dirs)
    base = [] if replace else _REGISTERED
    _REGISTERED = _dedupe(list(base) + incoming)
    return list(_REGISTERED)


def get_registered_dirs() -> List[str]:
    """Return an ordered, de-duplicated copy of the registered grid dirs."""
    return list(_REGISTERED)
