"""`gbx.uc.install(deps="volume")`: the ENVIRONMENT options (``--no-index``, ``--find-links=<Volume>/<arch>``), the
preflight that every needed wheel is staged, and the Fix a CREATE gets when the sandbox cannot install a package.
"""

import json
import re

import pytest

from databricks.labs.gbx import _native, manifest, uc
from databricks.labs.gbx.uc import ddl

from .test_stage_deps import UC_PINS, WHEEL, _lists, _wheel

# --- gbx.uc.install(deps="volume") ------------------------------------------------------------------------------------


def _env_of(stmt):
    m = re.search(r"dependencies = '(\[.*?\])'", stmt)
    return None if m is None else json.loads(m.group(1).replace("\\'", "'"))


def test_volume_mode_puts_the_options_first_and_keeps_the_shared_pins():
    vol = "/Volumes/c/s/v/geobrix"
    stmts = uc.install("c", "s", vol, dry_run=True, deps="volume", procedures=True)
    envs = {tuple(e) for e in map(_env_of, stmts) if e}
    pins = tuple(f"{n}=={v}" for n, v in UC_PINS.items())
    assert envs == {
        (
            "--no-index",
            f"--find-links={vol}/x86_64",
            f"--find-links={vol}/aarch64",
            f"geobrix @ file://{vol}/{WHEEL}",
        )
        + pins
    }
    one = uc.install(
        "c",
        "s",
        vol,
        dry_run=True,
        deps="volume",
        arch="aarch64",
        wheels_dir="/Volumes/c/s/w/deps",
    )
    assert {tuple(e[:2]) for e in map(_env_of, one) if e} == {
        ("--no-index", "--find-links=/Volumes/c/s/w/deps/aarch64")
    }
    shared = uc.install("c", "s", vol, dry_run=True, deps="shared")
    assert all(
        not e or e[0].startswith("geobrix @ ") for e in map(_env_of, shared)
    )  # the default stays PyPI


def test_volume_mode_arguments_are_checked_first():
    with pytest.raises(ValueError, match="deps='volume' only"):
        uc.install(
            "c", "s", "/Volumes/c/s/v", dry_run=True, wheels_dir="/Volumes/c/s/v"
        )
    with pytest.raises(ValueError, match="arch"):
        uc.install(
            "c", "s", "/Volumes/c/s/v", dry_run=True, deps="volume", arch="arm64"
        )
    with pytest.raises(ValueError, match="volume_dir"):
        uc.install(
            "c",
            "s",
            "/Volumes/c/s/v",
            dry_run=True,
            deps="volume",
            wheels_dir="dbfs:/Volumes/c/s/v",
        )
    with pytest.raises(ValueError, match="only --no-index"):
        ddl.private_ddl(next(iter(manifest.load().private.values())), "c", "s", "file:///w.whl", UC_PINS, "0.6",
                        pip_options=["--index-url=https://x"])  # fmt: skip


class _Spark:
    def __init__(self, fail=None):
        self.stmts, self.fail = [], fail

    def sql(self, s):
        self.stmts.append(s)
        if self.fail and s.startswith("CREATE"):
            raise Exception(self.fail)  # noqa: TRY002 - what a CREATE raises in a test
        raise _Ran()


class _Ran(Exception):
    pass


def _ready(tmp_path, monkeypatch, staged):
    vol = tmp_path / "vol"
    (vol / "aarch64").mkdir(parents=True)
    for n, v in staged.items():
        _wheel(str(vol / "aarch64"), n, v)
    monkeypatch.setattr(ddl, "check_volume_dir", lambda p: p)
    monkeypatch.setattr(
        ddl, "_PIP_OPTION", re.compile(r"--no-index|--find-links=\S+")
    )  # a tmp folder
    monkeypatch.setattr(uc, "_require_env6", lambda: None)
    monkeypatch.setattr(uc, "_single_wheel", lambda d: f"{d}/{WHEEL}")
    monkeypatch.setattr(
        _native, "deps_dir", lambda root=None: None
    )  # this wheel's lists: none, the UC pins only
    return str(vol)


def test_volume_mode_refuses_a_folder_without_every_pin_before_any_statement(
    tmp_path, monkeypatch
):
    vol = _ready(tmp_path, monkeypatch, {"h3": UC_PINS["h3"]})
    spark = _Spark()
    with pytest.raises(
        RuntimeError,
        match=rf"aarch64 lacks 1 wheel.*quadbin=={UC_PINS['quadbin']}.*Fix:.*--arch aarch64",
    ):
        uc.install(
            "c", "s", vol, spark=spark, deps="volume", only=["gbx_quadbin_polyfill"]
        )
    assert spark.stmts == []
    with pytest.raises(RuntimeError, match="no x86_64/ or aarch64/ wheel folder"):
        uc.install(
            "c",
            "s",
            vol,
            spark=spark,
            deps="volume",
            wheels_dir=str(tmp_path / "empty"),
        )
    vol = _ready(tmp_path / "ok", monkeypatch, UC_PINS)
    with pytest.raises(_Ran):  # the preflight passed: the first statement ran
        uc.install(
            "c", "s", vol, spark=_Spark(), deps="volume", only=["gbx_quadbin_polyfill"]
        )


def test_volume_mode_preflight_wants_the_wheels_own_list_too(tmp_path, monkeypatch):
    vol = _ready(tmp_path, monkeypatch, UC_PINS)
    core = {n: (v, "a" * 64) for n, v in UC_PINS.items()} | {
        "numexpr": ("2.14.2", "b" * 64)
    }
    _lists(tmp_path, monkeypatch, core)
    with pytest.raises(RuntimeError, match="numexpr==2.14.2"):
        uc.install(
            "c", "s", vol, spark=_Spark(), deps="volume", only=["gbx_quadbin_polyfill"]
        )


@pytest.mark.parametrize(
    "deps, message, said",
    [
        (
            "shared",
            "pip: Connection to pypi.org timed out. (connect timeout=15)",
            'deps="volume"',
        ),
        (
            "per-function",
            "[NO_MATCHING_DISTRIBUTION] No matching distribution found for h3==4.5.0",
            "gbx.stage_deps",
        ),
        (
            "volume",
            "ERROR: No matching distribution found for quadbin==0.2.2",
            "stage deps --uc",
        ),
    ],
)
def test_a_create_that_cannot_get_its_packages_raises_with_a_fix(
    tmp_path, monkeypatch, deps, message, said
):
    vol = _ready(tmp_path, monkeypatch, UC_PINS)
    with pytest.raises(RuntimeError, match="Fix:") as exc:
        uc.install(
            "c",
            "s",
            vol,
            spark=_Spark(message),
            deps=deps,
            only=["gbx_quadbin_polyfill"],
        )
    assert said in str(exc.value) and message.split(":")[0] in str(exc.value.__cause__)


def test_another_create_failure_is_raised_as_it_is(tmp_path, monkeypatch):
    vol = _ready(tmp_path, monkeypatch, UC_PINS)
    with pytest.raises(Exception, match="PERMISSION_DENIED") as exc:
        uc.install(
            "c",
            "s",
            vol,
            spark=_Spark("PERMISSION_DENIED: CREATE FUNCTION"),
            only=["gbx_quadbin_polyfill"],
        )
    assert not isinstance(exc.value, RuntimeError)
