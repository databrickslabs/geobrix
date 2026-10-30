"""The env wheels carry what they claim.

Reads the two built wheels (``gbx:build:wheel --env 5`` and ``--env 6``) as zip files: the metadata
against ``python/geobrix/envs/envN``, the tripwire extras, both architectures' payload, the JAR,
the startup activation at the wheel root, the build record, the licence guard on every library
name, Requires-Python 3.12 only, one zip timestamp for every member (SOURCE_DATE_EPOCH: the
build is deterministic), and that the native payload is byte-identical in the two wheels.
"""

import base64
import hashlib
import json
import re
import time
import zipfile
from email.parser import Parser
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name

REPO = Path(__file__).resolve().parents[4]
ENVS = REPO / "python" / "geobrix" / "envs"
PKG = "databricks/labs/gbx"
ARCHES = ("x86_64", "aarch64")
TRIPWIRES = {  # the other environment's old extra names, per wheel
    6: (
        "light_dbr17",
        "light_dbr18",
        "light_env5",
        "light_dbr17_all",
        "light_dbr18_all",
        "light_env5_all",
        "photogrammetry_gpu_env5",
        "models_gpu_env5",
        "gpu_env5_all",
    ),
    5: ("light_dbr19", "light_env6", "light_dbr19_all", "light_env6_all"),
}
# The licence guard is verify-native.sh's: one list for the native build and the wheel.
BANNED = re.compile(
    re.search(
        r"^BANNED='([^']+)'",
        (REPO / "scripts/native/verify-native.sh").read_text(),
        re.M,
    ).group(1),
    re.I,
)


def _version():
    text = (REPO / "python/geobrix/src/databricks/labs/gbx/__init__.py").read_text()
    return re.search(r'^__version__ = "([^"]+)"', text, re.M).group(1)


def _norm(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def _canon(r):
    """A requirement without its marker, the name canonicalised: the form both sides compare in."""
    extras = f"[{','.join(sorted(r.extras))}]" if r.extras else ""
    return f"{canonicalize_name(r.name)}{extras}{r.specifier}"


def _reqs(path):
    lines = [ln.split("#", 1)[0].strip() for ln in path.read_text().splitlines()]
    return {_canon(Requirement(ln)) for ln in lines if ln}


def _wheel(env):
    return (
        REPO / "dist" / f"env{env}" / f"geobrix-{_version()}+env{env}-py3-none-any.whl"
    )


def _check_one(env, zf):
    v, other = _version(), 11 - env
    names = set(zf.namelist())
    meta = Parser().parsestr(
        zf.read(f"geobrix-{v}+env{env}.dist-info/METADATA").decode()
    )
    assert meta["Version"] == f"{v}+env{env}"
    assert SpecifierSet(meta["Requires-Python"]) == SpecifierSet(
        ">=3.12,<3.13"
    )  # the payload is cp312-only

    unmarked, by_extra = set(), {}
    for line in meta.get_all("Requires-Dist") or []:
        r = Requirement(line)
        m = re.fullmatch(r'extra == "([^"]+)"', str(r.marker or ""))
        (by_extra.setdefault(_norm(m.group(1)), set()) if m else unmarked).add(
            _canon(r)
        )
    assert unmarked == _reqs(ENVS / f"env{env}" / "core.txt")

    extras = {
        p.stem.removeprefix("extra-"): p
        for p in (ENVS / f"env{env}").glob("extra-*.txt")
    }
    trip = {_norm(n) for n in TRIPWIRES[env]}
    assert {_norm(e) for e in meta.get_all("Provides-Extra")} == {
        _norm(e) for e in extras
    } | trip
    for name, path in extras.items():
        assert by_extra[_norm(name)] == _reqs(path), name
    for name in trip:
        assert by_extra[name] == {f"geobrix=={v}+env{other}"}, name
    assert not {_norm(n) for n in TRIPWIRES[other]} & set(
        by_extra
    ), "a same-env old name is declared"

    for a in ARCHES:
        manifest = json.loads(zf.read(f"{PKG}/_native/{a}/MANIFEST.json"))
        assert f"{PKG}/_native/{a}/lib/{manifest['jni']}" in names
        gdal = [
            h
            for o, h in manifest["sonames"].items()
            if re.fullmatch(r"libgdal\.so\.\d+", o)
        ]
        assert len(gdal) == 1 and f"{PKG}/_native/{a}/lib/{gdal[0]}" in names
        assert f"{PKG}/_vendor/{a}/rasterio/__init__.py" in names
    assert (
        f"{PKG}/_jar/geobrix-{v}-jar-with-dependencies.jar" in names
    )  # the JAR is in the wheel
    assert zf.read("geobrix-native.pth").decode().strip() == "import _gbx_native_path"
    assert "_gbx_native_path.py" in names
    build_info = {}
    exec(zf.read(f"{PKG}/_build_info.py").decode(), build_info)
    assert build_info["ENV"] == env and build_info["VERSION"] == f"{v}+env{env}"
    # Deterministic: every member carries SOURCE_DATE_EPOCH (HEAD's commit time), as wheel writes it
    # (UTC, seconds rounded down to even), so one tree gives one sha256.
    t = time.gmtime(build_info["SOURCE_DATE_EPOCH"])[:6]
    stamp = (*t[:5], t[5] - t[5] % 2)
    assert {i.date_time for i in zf.infolist()} == {stamp}

    libs = [n for n in names if re.search(r"\.so(\.|$)", n.rsplit("/", 1)[-1])]
    assert libs and not [n for n in libs if BANNED.search(n.rsplit("/", 1)[-1])]

    payload = (f"{PKG}/_native/",) + tuple(f"{PKG}/_vendor/{a}/" for a in ARCHES)
    shas = {}
    for row in zf.read(f"geobrix-{v}+env{env}.dist-info/RECORD").decode().splitlines():
        path, digest, _size = row.rsplit(",", 2)
        if path.startswith(payload):
            shas[path] = digest
    for path in list(shas)[
        :3
    ]:  # RECORD is the bytes: spot-check it against the members
        raw = (
            base64.urlsafe_b64encode(hashlib.sha256(zf.read(path)).digest())
            .rstrip(b"=")
            .decode()
        )
        assert shas[path] == f"sha256={raw}"
    return shas


def test_the_env_wheels_carry_what_they_claim():
    missing = [f"dist/env{e}" for e in (5, 6) if not _wheel(e).is_file()]
    if missing:
        pytest.skip(
            f"PRECONDITION MISSING: {', '.join(missing)} (run gbx:build:wheel --env 5 and --env 6)"
        )
    shas = {}
    for env in (5, 6):
        with zipfile.ZipFile(_wheel(env)) as zf:
            shas[env] = _check_one(env, zf)
    assert (
        shas[5] and shas[5] == shas[6]
    ), "the native payload differs between the env5 and env6 wheels"
