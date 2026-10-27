"""Every light extra that installs rio-tiler 9.x must floor typing_extensions at 4.13.0.

rio-tiler 9.x declares ``class AssetWithOptions(TypedDict, extra_items=True)`` (PEP 728)
in ``rio_tiler/types.py``, but its own metadata only asks for ``typing-extensions>=4.10``.
``extra_items`` arrived in typing_extensions 4.13.0 (changelog, 4.13.0rc1: "Update PEP 728
implementation to a newer version of the PEP"); 4.12.2 raises ``TypeError:
_TypedDictMeta.__new__() got an unexpected keyword argument 'extra_items'``.

Serverless env 5, DBR 17.3 and DBR 18 preinstall typing_extensions 4.12.2. Because pip keeps
an installed package that satisfies the requirement, a ``>=4.12`` floor left 4.12.2 in place and
``import rio_tiler`` failed on those runtimes. The CI locks pin typing-extensions 4.16.0, so
no CI test ever saw the failure: this guard reads the extras instead.

The floor is the PEP 728 minimum on purpose. On the 4.12.2 bases pip upgrades to the newest
release under any floor of 4.13 or higher, so raising it gains nothing there; DBR 19, DBR 19 ML
and env 6 preinstall 4.15.0, so a floor above 4.15.0 would move it there for no gain. The floor
has no upper bound: a cap would downgrade a later base.
"""

import re
import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version

PKG = Path(__file__).resolve().parents[2]  # python/geobrix
EXTRAS = tomllib.loads((PKG / "pyproject.toml").read_text())["project"][
    "optional-dependencies"
]
LOCKS = [
    "requirements-light-env5-ci.txt",
    "requirements-light-env5-all-ci.txt",
    "requirements-light-env6-ci.txt",
    "requirements-light-env6-all-ci.txt",
]

# First typing_extensions release whose TypedDict accepts ``extra_items=``.
EXTRA_ITEMS_FLOOR = Version("4.13.0")


def _closure(extra, seen=None):
    """Requirements of `extra`, following nested ``geobrix[a,b]`` references."""
    seen = set() if seen is None else seen
    if extra in seen:
        return []
    seen.add(extra)
    out = []
    for text in EXTRAS[extra]:
        req = Requirement(text)
        if canonicalize_name(req.name) == "geobrix":
            for nested in sorted(req.extras):
                out.extend(_closure(nested, seen))
        else:
            out.append(req)
    return out


def _lower_bound(req):
    bounds = [Version(s.version) for s in req.specifier if s.operator in (">=", "==")]
    return max(bounds) if bounds else None


def _pins_rio_tiler_9(reqs):
    return any(
        canonicalize_name(r.name) == "rio-tiler"
        and (_lower_bound(r) or Version("0")) >= Version("9")
        for r in reqs
    )


def _typing_extensions_floors(extra):
    return [
        _lower_bound(r)
        for r in _closure(extra)
        if canonicalize_name(r.name) == "typing-extensions" and _lower_bound(r)
    ]


def _rio_tiler_9_extras():
    return sorted(e for e in EXTRAS if _pins_rio_tiler_9(_closure(e)))


def test_the_runtime_extras_are_covered():
    # A rename of the extras must not turn the guard below into a vacuous pass.
    covered = set(_rio_tiler_9_extras())
    assert {
        "light_dbr17",
        "light_dbr18",
        "light_env5",
        "light_dbr19",
        "light_env6",
        "light_dbr17_all",
        "light_env5_all",
        "light_dbr19_all",
        "light_env6_all",
    } <= covered


def _floor(extra):
    floors = _typing_extensions_floors(extra)
    msg = f"[{extra}] installs rio-tiler 9.x but sets no typing_extensions floor"
    assert floors, msg
    return max(floors)


def test_every_extra_that_installs_rio_tiler_9_floors_typing_extensions_at_pep_728():
    for extra in _rio_tiler_9_extras():
        assert _floor(extra) >= EXTRA_ITEMS_FLOOR, (
            f"[{extra}] typing_extensions>={_floor(extra)} admits 4.12.2, which cannot "
            "import rio-tiler 9.x (PEP 728 extra_items needs >=4.13.0)"
        )


def test_the_floor_is_not_raised_past_pep_728():
    # 4.13.0 is the first release with extra_items. A floor above 4.15.0 moves the
    # preinstalled package on DBR 19 / DBR 19 ML / env 6 for no gain.
    for extra in _rio_tiler_9_extras():
        assert _floor(extra) == EXTRA_ITEMS_FLOOR, f"[{extra}] {_floor(extra)}"


CAP_OPERATORS = ("<", "<=", "!=", "~=", "==", "===")


def test_the_floor_has_no_upper_bound():
    # A cap such as <4.14, or an exact pin such as ==4.13.0, would downgrade the 4.15.0
    # that DBR 19 / env 6 ship.
    for extra in _rio_tiler_9_extras():
        for req in _closure(extra):
            if canonicalize_name(req.name) != "typing-extensions":
                continue
            caps = [s for s in req.specifier if s.operator in CAP_OPERATORS]
            assert not caps, f"[{extra}] {req}"


def test_the_ci_locks_resolve_typing_extensions_at_or_above_the_floor():
    # The locks resolve to the newest release, which hid the missing floor in CI.
    for lock in LOCKS:
        pin = re.search(
            r"^typing-extensions==(\S+) \\$", (PKG / lock).read_text(), re.M
        )
        assert pin, f"{lock} does not pin typing-extensions"
        assert Version(pin.group(1)) >= EXTRA_ITEMS_FLOOR, lock


def test_the_installed_typing_extensions_accepts_extra_items():
    import typing_extensions as te

    class Probe(te.TypedDict, extra_items=True):
        name: str

    assert Probe.__required_keys__ == frozenset({"name"})
