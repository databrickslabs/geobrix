"""An install without PyPI access: the env wheel's dependency lists (_native/deps/), `python -m databricks.labs.gbx.stage
deps` (download, hash check, offline completeness check, wheels.txt) and `gbx.stage_deps` (sha-checked copy to a
Volume, install lines). `gbx.uc.install(deps="volume")` is in test_uc_volume_deps.py.
"""

import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from databricks.labs.gbx import _native, manifest, stage
from databricks.labs.gbx._native import _deps

UC_PINS = dict(manifest.load().uc_dependencies)
BASE = {"numpy": "2.3.4", "pandas": "2.3.3"}


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _wheel(folder, name, version, requires=(), tag="py3-none-any"):
    """A minimal valid wheel: its METADATA (with ``requires``), WHEEL and RECORD."""
    os.makedirs(folder, exist_ok=True)
    dist = f"{name.replace('-', '_')}-{version}"
    path = os.path.join(folder, f"{dist}-{tag}.whl")
    meta = f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n" + "".join(
        f"Requires-Dist: {r}\n" for r in requires
    )
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(f"{dist}.dist-info/METADATA", meta)
        zf.writestr(
            f"{dist}.dist-info/WHEEL",
            f"Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: {tag}\n",
        )
        zf.writestr(f"{dist}.dist-info/RECORD", "")
    return path


def _lists(tmp_path, monkeypatch, core, extras=None, env=6):
    """The installed wheel's _native/deps/ replaced by lists of ``core`` (name -> (version, sha)) and ``extras``."""
    root = tmp_path / "payload"
    d = root / "_native" / "deps"
    d.mkdir(parents=True)
    (d / _deps.base_name(env)).write_text(
        "".join(f"{n}=={v}\n" for n, v in BASE.items())
    )
    for a in _deps.ARCHES:
        (d / _deps.list_name(env, a)).write_text(_deps.format_list("test", core))
        for extra, pins in (extras or {}).items():
            (d / _deps.list_name(env, a, extra)).write_text(
                _deps.format_list("test", pins)
            )
        if env == 6:
            (d / _deps.list_name(env, a, _deps.UC)).write_text(
                _deps.format_list("test", {n: core[n] for n in UC_PINS if n in core})
            )
    monkeypatch.setattr(_native, "deps_dir", lambda root_=None: str(d))
    monkeypatch.setattr(stage, "_installed_env", lambda: env)
    return d


# --- the list format (_native/_deps.py, shared with the builder) ---------------------------------------------------


def test_the_list_format_round_trips_and_refuses_an_unhashed_or_repeated_pin():
    pins = {"h3": ("4.5.0", "a" * 64), "typing-extensions": ("4.16.0", "b" * 64)}
    text = _deps.format_list("geobrix 0.6.0 aarch64\nline two", pins)
    assert text.startswith("# geobrix 0.6.0 aarch64\n# line two\n")
    assert _deps.parse_list(text) == pins
    with pytest.raises(ValueError, match="hash"):
        _deps.parse_list("h3==4.5.0\n")
    with pytest.raises(ValueError, match="twice"):
        _deps.parse_list(
            f"h3==4.5.0 --hash=sha256:{'a' * 64}\nH3==4.5.0 --hash=sha256:{'a' * 64}\n"
        )
    assert _deps.wheel_name_version("typing_extensions-4.16.0-py3-none-any.whl") == (
        "typing-extensions",
        "4.16.0",
    )
    assert _deps.wheel_name_version(
        "x/h3-4.5.0-cp312-cp312-manylinux_2_28_aarch64.whl"
    ) == ("h3", "4.5.0")
    assert _deps.platforms("aarch64")[0] == "manylinux_2_28_aarch64"
    with pytest.raises(ValueError, match="arch"):
        _deps.platforms("arm64")


def test_dep_pins_adds_the_extras_and_names_the_known_ones(tmp_path, monkeypatch):
    core = {"h3": ("4.5.0", "a" * 64), "quadbin": ("0.2.2", "b" * 64)}
    _lists(
        tmp_path,
        monkeypatch,
        core,
        {"zarr": {"zarrs": ("0.2.3", "c" * 64)}, "stac": {}},
    )
    assert stage.dep_pins("aarch64") == core
    assert stage.dep_pins("x86_64", ("zarr",))["zarrs"] == ("0.2.3", "c" * 64)
    with pytest.raises(ValueError, match=r"no extra \[nope\].*\['stac', 'zarr'\]"):
        stage.dep_pins("aarch64", ("nope",))


def test_a_plain_wheel_has_no_lists_and_says_how_to_fix_it(monkeypatch):
    monkeypatch.setattr(_native, "deps_dir", lambda root=None: None)
    with pytest.raises(RuntimeError, match="Fix: install an env wheel"):
        stage.dep_pins("aarch64", env=6)
    monkeypatch.setattr("databricks.labs.gbx.__version__", "0.6.0")
    with pytest.raises(RuntimeError, match="not an env wheel"):
        stage._installed_env()


# --- the offline completeness check: a real pip, local wheels only ---------------------------------------------------

needs_pip = pytest.mark.skipif(
    importlib.util.find_spec("pip") is None, reason="this interpreter has no pip"
)


@needs_pip
def test_the_offline_check_counts_the_base_as_installed_and_refuses_a_missing_wheel(
    tmp_path,
):
    folder = str(tmp_path / "aarch64")
    gbx = _wheel(
        str(tmp_path),
        "geobrix",
        "0.6.0+env6",
        ["h3>=4.1,<5", "numexpr>=2.14", "numpy>=2.3.4,<2.4"],
    )
    _wheel(folder, "h3", "4.5.0", ["numpy>=1.26"])
    _wheel(folder, "numexpr", "2.14.2", ["numpy>=1.26"])
    got = stage.check_offline(folder, "aarch64", [gbx, "h3==4.5.0"], BASE)
    assert got == [
        "geobrix==0.6.0+env6",
        "h3==4.5.0",
        "numexpr==2.14.2",
    ]  # numpy: the base has it
    os.remove(os.path.join(folder, "numexpr-2.14.2-py3-none-any.whl"))
    with pytest.raises(
        RuntimeError, match=r"(?s)do not cover the install on aarch64.*numexpr.*Fix:"
    ):
        stage.check_offline(folder, "aarch64", [gbx], BASE)
    with pytest.raises(
        RuntimeError, match="numpy"
    ):  # without the base, numpy would come from the folder
        _wheel(folder, "numexpr", "2.14.2", ["numpy>=1.26"])
        stage.check_offline(folder, "aarch64", [gbx], {})


@needs_pip
def test_the_offline_check_takes_only_wheels_for_the_target_platform(tmp_path):
    folder = str(tmp_path / "x86_64")
    gbx = _wheel(str(tmp_path), "geobrix", "0.6.0+env6", ["h3>=4.1,<5"])
    _wheel(folder, "h3", "4.5.0", tag="cp312-cp312-manylinux_2_28_aarch64")
    with pytest.raises(RuntimeError, match="h3"):
        stage.check_offline(folder, "x86_64", [gbx], BASE)
    _wheel(folder, "h3", "4.5.0", tag="cp312-cp312-manylinux_2_28_x86_64")
    assert stage.check_offline(folder, "x86_64", [gbx], BASE) == [
        "geobrix==0.6.0+env6",
        "h3==4.5.0",
    ]


# --- python -m databricks.labs.gbx.stage deps ----------------------------------------------------------------------


def _index(tmp_path, arch):
    """The wheels a fake `pip download` serves: h3 and quadbin at the UC pins, and numexpr."""
    folder = str(tmp_path / "index" / arch)
    paths = [
        _wheel(folder, "h3", UC_PINS["h3"], tag=f"cp312-cp312-manylinux_2_28_{arch}"),
        _wheel(folder, "quadbin", UC_PINS["quadbin"]),
        _wheel(folder, "numexpr", "2.14.2", tag=f"cp312-cp312-manylinux_2_28_{arch}"),
    ]
    with_sha = {}
    for p in paths:
        n, v = _deps.wheel_name_version(p)
        with_sha[n] = (v, _sha(open(p, "rb").read()), p)
    return with_sha


def _fake_pip(index, seen, dry_run_ok=True):
    """`run` for download_deps: a `pip download` copies the listed wheels from ``index`` (by arch); a dry run writes a
    report; anything else runs for real (the stub venv)."""

    def run(cmd, **kw):
        if "pip" in cmd and "download" in cmd:
            seen.append(cmd)
            arch = next(c for c in cmd if c.startswith("manylinux_2_28_"))[
                len("manylinux_2_28_") :
            ]
            req, dest = cmd[cmd.index("-r") + 1], cmd[cmd.index("-d") + 1]
            for n, (v, sha) in _deps.parse_list(open(req).read()).items():
                assert index[arch][n][1] == sha
                shutil.copy(index[arch][n][2], dest)
            return subprocess.CompletedProcess(cmd, 0, "", "")
        if "pip" in cmd and "--dry-run" in cmd:
            seen.append(cmd)
            if not dry_run_ok:
                return subprocess.CompletedProcess(
                    cmd, 1, "", "ERROR: No matching distribution found for numexpr"
                )
            with open(cmd[cmd.index("--report") + 1], "w") as fh:
                json.dump(
                    {
                        "install": [
                            {"metadata": {"name": "h3", "version": UC_PINS["h3"]}}
                        ]
                    },
                    fh,
                )
            return subprocess.CompletedProcess(cmd, 0, "", "")
        return subprocess.run(cmd, **kw)

    return run


def test_deps_downloads_both_arches_hash_checked_checks_offline_and_writes_wheels_txt(
    tmp_path, monkeypatch, capsys
):
    index = {a: _index(tmp_path, a) for a in _deps.ARCHES}
    core = {n: (v, s) for n, (v, s, _) in index["aarch64"].items()}
    _lists(tmp_path, monkeypatch, core)
    # the lists are per arch: the x86_64 one pins the x86_64 wheels
    d = _native.deps_dir()
    (open(os.path.join(d, "env6-x86_64.txt"), "w")).write(
        _deps.format_list(
            "test", {n: (v, s) for n, (v, s, _) in index["x86_64"].items()}
        )
    )
    whl = _wheel(str(tmp_path / "dl"), "geobrix", "0.6.0+env6", ["h3>=4.1,<5"])
    monkeypatch.setattr("databricks.labs.gbx.__version__", "0.6.0+env6")
    seen = []
    out = tmp_path / "out"
    path = stage.download_deps(
        str(out), uc=True, wheel=whl, python=sys.executable, run=_fake_pip(index, seen)
    )
    downloads = [c for c in seen if "download" in c]
    assert len(downloads) == 2  # both arches by default
    for c in downloads:
        assert {"--no-deps", "--require-hashes", "--only-binary=:all:"} <= set(c)
        assert (
            c[c.index("--python-version") + 1] == "3.12"
            and c[c.index("--implementation") + 1] == "cp"
        )
    dry = [c for c in seen if "--dry-run" in c]
    assert len(dry) == 2 and all("--no-index" in c and "--python" in c for c in dry)
    assert all(
        f"h3=={UC_PINS['h3']}" in c for c in dry
    )  # --uc: the UC pins are part of the check
    rows = stage.read_wheels_txt(str(out))
    rels = sorted(r for _, r in rows)
    assert rels[0] == "aarch64/h3-4.5.0-cp312-cp312-manylinux_2_28_aarch64.whl"
    assert "geobrix-0.6.0+env6-py3-none-any.whl" in rels and len(rels) == 7
    assert all(_sha(open(out / r, "rb").read()) == s for s, r in rows)
    assert path == str(out / "wheels.txt")
    assert "offline install check passed" in capsys.readouterr().out


def test_deps_refuses_a_wheel_that_does_not_match_its_pin_or_a_stray_wheel(
    tmp_path, monkeypatch
):
    index = {a: _index(tmp_path, a) for a in _deps.ARCHES}
    core = {n: (v, s) for n, (v, s, _) in index["aarch64"].items()}
    _lists(tmp_path, monkeypatch, core)
    out = tmp_path / "out"
    _wheel(str(out / "aarch64"), "stray", "1.0")
    monkeypatch.setattr("databricks.labs.gbx.__version__", "0.6.0+env6")
    with pytest.raises(
        RuntimeError, match=r"stray-1\.0.*Fix: download into an empty --out folder"
    ):
        stage.download_deps(
            str(out),
            arches=("aarch64",),
            python=sys.executable,
            run=_fake_pip(index, []),
        )
    with pytest.raises(ValueError, match="not the installed env wheel"):
        stage.download_deps(
            str(out), wheel=str(tmp_path / "geobrix-0.5.2-py3-none-any.whl")
        )


def test_deps_raises_with_pips_answer_when_the_offline_check_fails(
    tmp_path, monkeypatch
):
    index = {"aarch64": _index(tmp_path, "aarch64")}
    _lists(
        tmp_path, monkeypatch, {n: (v, s) for n, (v, s, _) in index["aarch64"].items()}
    )
    monkeypatch.setattr("databricks.labs.gbx.__version__", "0.6.0+env6")
    monkeypatch.setattr(stage, "_requirement_lines", lambda extras: ["h3>=4.1,<5"])
    with pytest.raises(
        RuntimeError, match="(?s)No matching distribution found for numexpr.*Fix:"
    ):
        stage.download_deps(
            str(tmp_path / "out"),
            arches=("aarch64",),
            python=sys.executable,
            run=_fake_pip(index, [], False),
        )


def test_the_cli_defaults_to_both_arches_and_reports_a_failure(
    tmp_path, monkeypatch, capsys
):
    got = {}
    monkeypatch.setattr(
        stage, "download_deps", lambda out, **kw: got.update(out=out, **kw)
    )
    assert (
        stage.main(["deps", "--out", str(tmp_path), "--extras", "zarr, vizx", "--uc"])
        == 0
    )
    assert (
        got["arches"] == ("x86_64", "aarch64")
        and got["extras"] == ("zarr", "vizx")
        and got["uc"]
    )

    def boom(out, **kw):
        raise RuntimeError("no lists. Fix: x")

    monkeypatch.setattr(stage, "download_deps", boom)
    assert stage.main(["deps", "--out", str(tmp_path), "--arch", "aarch64"]) == 1
    assert "geobrix: no lists. Fix: x" in capsys.readouterr().err


# --- gbx.stage_deps -------------------------------------------------------------------------------------------------

WHEEL = "geobrix-0.6.0+env6-py3-none-any.whl"


def _src(tmp_path):
    src = tmp_path / "src"
    files = {WHEEL: b"wheel", "aarch64/h3-4.5.0-cp312-cp312-manylinux_2_28_aarch64.whl": b"h3a",
             "x86_64/h3-4.5.0-cp312-cp312-manylinux_2_28_x86_64.whl": b"h3x"}  # fmt: skip
    for rel, data in files.items():
        (src / rel).parent.mkdir(parents=True, exist_ok=True)
        (src / rel).write_bytes(data)
    (src / "wheels.txt").write_text(
        "# test\n" + "".join(f"{_sha(d)}  {r}\n" for r, d in files.items())
    )
    return src, files


def test_stage_deps_copies_through_fuse_checks_each_sha_and_prints_the_install_lines(
    tmp_path, monkeypatch, capsys
):
    src, files = _src(tmp_path)
    vol = tmp_path / "vol"
    monkeypatch.setattr(stage, "check_volume_dir", lambda p: p)
    monkeypatch.setattr(stage, "_on_databricks", lambda: True)
    assert stage.stage_deps(str(vol), str(src)) == str(vol)
    assert all((vol / r).read_bytes() == d for r, d in files.items())
    req = (vol / "requirements-aarch64.txt").read_text()
    assert req == f"--no-index\n--find-links {vol}/aarch64\n{vol}/{WHEEL}\n"
    out = capsys.readouterr().out
    assert "3 copied" in out
    assert f"%pip install --no-index --find-links {vol}/aarch64 {vol}/{WHEEL}" in out
    assert (
        f"--find-links={vol}/x86_64" in out and f"{vol}/requirements-x86_64.txt" in out
    )
    assert "deps='volume'" in out
    stage.stage_deps(str(vol), str(src))
    assert "0 copied, 3 already there" in capsys.readouterr().out
    (vol / "geobrix-0.5.2+env6-py3-none-any.whl").write_bytes(b"old")
    with pytest.raises(RuntimeError, match="its own folder"):
        stage.stage_deps(str(vol), str(src))


def test_stage_deps_refuses_a_source_that_changed_since_the_download(
    tmp_path, monkeypatch
):
    src, _ = _src(tmp_path)
    monkeypatch.setattr(stage, "check_volume_dir", lambda p: p)
    (src / "aarch64/h3-4.5.0-cp312-cp312-manylinux_2_28_aarch64.whl").write_bytes(
        b"tampered"
    )
    with pytest.raises(RuntimeError, match="does not match the sha256.*Fix:"):
        stage.stage_deps(str(tmp_path / "vol"), str(src), client=object())
    with pytest.raises(RuntimeError, match="no wheels.txt.*Fix:"):
        stage.stage_deps(str(tmp_path / "vol"), str(tmp_path))


class _Files:
    """The Files API of a WorkspaceClient, in memory; ``drop`` truncates every upload (bytes that never landed)."""

    def __init__(self, drop=False):
        self.store, self.drop, self.dirs = {}, drop, set()

    def create_directory(self, path):
        self.dirs.add(path)

    def upload(self, path, fh, overwrite=False):
        data = fh.read()
        self.store[path] = data[:1] if self.drop else data

    def download(self, path):
        import io

        return type("R", (), {"contents": io.BytesIO(self.store[path])})()

    def get_metadata(self, path):
        if path not in self.store:
            raise FileNotFoundError(path)
        return type("M", (), {"content_length": len(self.store[path])})()

    def list_directory_contents(self, path):
        return [
            type("E", (), {"path": p})()
            for p in self.store
            if os.path.dirname(p) == path
        ]


def test_stage_deps_off_databricks_uploads_through_the_files_api_and_checks_what_landed(
    tmp_path, monkeypatch
):
    src, files = _src(tmp_path)
    vol = "/Volumes/c/s/v/geobrix/0.6.0/deps"
    w = type("W", (), {"files": _Files()})()
    stage.stage_deps(vol, str(src), client=w)
    assert all(w.files.store[f"{vol}/{r}"] == d for r, d in files.items())
    assert (
        f"{vol}/requirements-aarch64.txt" in w.files.store
        and f"{vol}/wheels.txt" in w.files.store
    )
    bad = type("W", (), {"files": _Files(drop=True)})()
    with pytest.raises(RuntimeError, match="after the copy.*Fix:"):
        stage.stage_deps(vol, str(src), client=bad)


def test_stage_deps_is_exported_beside_stage_jar():
    import databricks.labs.gbx as gbx

    assert gbx.stage_deps is stage.stage_deps and "stage_deps" in gbx.__all__


# --- the real readers: _native.deps_dir and the installed wheel's Requires-Dist ------------------------------------

REPO = Path(__file__).resolve().parents[4]
SRC_PKG = REPO / "python" / "geobrix" / "src" / "databricks" / "labs" / "gbx"
DIST_ENV6 = sorted((REPO / "dist" / "env6").glob("geobrix-*+env6-py3-none-any.whl"))
PAYLOAD_ENV6 = REPO / "build" / "env-wheel" / "env6" / "payload"


def _installed_native(root, lists=True):
    """The package's own ``_native/__init__.py``, copied to an installed layout (``<site>/databricks/labs/gbx/_native``)
    with (``lists``) a ``deps/`` folder beside it, and loaded from there: payload_root follows its ``__file__``.
    """
    pkg = root / "site" / "databricks" / "labs" / "gbx"
    shutil.copytree(
        SRC_PKG / "_native",
        pkg / "_native",
        ignore=shutil.ignore_patterns("__pycache__", "deps"),
    )
    if lists:
        (pkg / "_native" / "deps").mkdir()
        (pkg / "_native" / "deps" / "env6-base.txt").write_text("numpy==2.3.4\n")
    spec = importlib.util.spec_from_file_location(
        f"gbx_native_copy_{abs(hash(str(root)))}", pkg / "_native" / "__init__.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return pkg, mod


def test_deps_dir_is_the_installed_packages_own_folder(tmp_path, monkeypatch):
    monkeypatch.delenv("GBX_NATIVE_ROOT", raising=False)
    pkg, mod = _installed_native(tmp_path / "env")
    assert mod.payload_root() == str(pkg)
    assert mod.deps_dir() == str(pkg / "_native" / "deps")
    _, plain = _installed_native(tmp_path / "plain", lists=False)
    assert plain.deps_dir() is None  # a wheel without the lists (the plain wheel)


def test_deps_dir_follows_gbx_native_root_and_the_source_tree_has_none(
    tmp_path, monkeypatch
):
    """A source checkout serves the payload through GBX_NATIVE_ROOT (build/env-wheel/env<N>/payload); the source
    tree itself never holds the lists (the build writes them into its scratch tree)."""
    assert _native.deps_dir(str(SRC_PKG)) is None
    payload = tmp_path / "payload"
    monkeypatch.setenv("GBX_NATIVE_ROOT", str(payload))
    assert _native.deps_dir() is None  # no _native/deps there yet
    (payload / "_native" / "deps").mkdir(parents=True)
    assert _native.deps_dir() == str(payload / "_native" / "deps")
    assert (
        _native.deps_dir(str(tmp_path / "elsewhere")) is None
    )  # an explicit root wins


@pytest.mark.skipif(
    not (PAYLOAD_ENV6 / "_native" / "deps").is_dir(),
    reason="no extracted env6 payload (gbx:build:wheel --env 6)",
)
def test_the_built_env6_payload_serves_its_lists_through_gbx_native_root(
    monkeypatch,
):
    monkeypatch.setenv("GBX_NATIVE_ROOT", str(PAYLOAD_ENV6))
    d = _native.deps_dir()
    assert d == str(PAYLOAD_ENV6 / "_native" / "deps")
    for a in _deps.ARCHES:
        core = _deps.parse_list(open(os.path.join(d, _deps.list_name(6, a))).read())
        uc = _deps.parse_list(
            open(os.path.join(d, _deps.list_name(6, a, _deps.UC))).read()
        )
        assert {n: v for n, (v, _) in uc.items()} == UC_PINS
        assert all(core[n] == p for n, p in uc.items())


def _dist_info(root, version, requires):
    """An installed ``geobrix`` distribution: a dist-info with ``requires`` as Requires-Dist, on ``root``."""
    info = root / f"geobrix-{version}.dist-info"
    info.mkdir(parents=True)
    info.joinpath("METADATA").write_text(
        f"Metadata-Version: 2.4\nName: geobrix\nVersion: {version}\n"
        + "".join(f"Requires-Dist: {r}\n" for r in requires)
    )
    return root


REQUIRES = [
    "h3<5,>=4.1",
    "numexpr>=2.14.1",
    'zarr<3.5,>=3.4.1; extra == "zarr"',
    'pystac-client<1,>=0.7; extra == "stac"',
    'geobrix==0.6.0+env5; extra == "light-env5"',  # a tripwire
]


def test_requirement_lines_are_the_installed_wheels_unmarked_and_chosen_extras(
    tmp_path, monkeypatch
):
    monkeypatch.syspath_prepend(
        str(_dist_info(tmp_path / "site", "0.6.0+env6", REQUIRES))
    )
    assert stage._requirement_lines(()) == ["h3<5,>=4.1", "numexpr>=2.14.1"]
    assert stage._requirement_lines(("zarr",)) == [
        "h3<5,>=4.1",
        "numexpr>=2.14.1",
        "zarr<3.5,>=3.4.1",
    ]
    assert stage._requirement_lines(("zarr", "stac")) == [
        "h3<5,>=4.1",
        "numexpr>=2.14.1",
        "zarr<3.5,>=3.4.1",
        "pystac-client<1,>=0.7",
    ]


@pytest.mark.skipif(not DIST_ENV6, reason="no built env6 wheel in dist/env6")
def test_requirement_lines_of_the_built_env6_wheel_are_core_txt_and_the_extra(
    tmp_path, monkeypatch
):
    """The real wheel's METADATA: the unmarked lines are envs/env6/core.txt, an extra adds its extra-<name>.txt."""
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name

    with zipfile.ZipFile(DIST_ENV6[-1]) as zf:
        for n in zf.namelist():
            if ".dist-info/" in n:
                zf.extract(n, tmp_path / "site")
    monkeypatch.syspath_prepend(str(tmp_path / "site"))

    def canon(lines):
        return {
            (canonicalize_name(r.name), str(r.specifier))
            for r in map(Requirement, lines)
        }

    def of(path):
        text = (REPO / "python/geobrix/envs/env6" / path).read_text().splitlines()
        return canon(
            ln.split("#", 1)[0].strip() for ln in text if ln.split("#", 1)[0].strip()
        )

    assert canon(stage._requirement_lines(())) == of("core.txt")
    assert canon(stage._requirement_lines(("zarr",))) == of("core.txt") | of(
        "extra-zarr.txt"
    )


def test_the_wheel_less_download_checks_the_installed_requirements_with_the_uc_pins(
    tmp_path, monkeypatch, capsys
):
    """No --wheel: the offline check installs the installed wheel's own requirements (the real _requirement_lines),
    from lists the real deps_dir finds under GBX_NATIVE_ROOT, and --uc adds the UC pins.
    """
    index = {"aarch64": _index(tmp_path, "aarch64")}
    zarr = _wheel(str(tmp_path / "index" / "aarch64"), "zarr", "3.4.1")
    core = {n: (v, s) for n, (v, s, _) in index["aarch64"].items()}
    index["aarch64"]["zarr"] = ("3.4.1", _sha(open(zarr, "rb").read()), zarr)
    payload = tmp_path / "payload"
    d = payload / "_native" / "deps"
    d.mkdir(parents=True)
    d.joinpath("env6-base.txt").write_text(
        "".join(f"{n}=={v}\n" for n, v in BASE.items())
    )
    d.joinpath("env6-aarch64.txt").write_text(_deps.format_list("test", core))
    d.joinpath("env6-aarch64-zarr.txt").write_text(
        _deps.format_list("test", {"zarr": index["aarch64"]["zarr"][:2]})
    )
    monkeypatch.setenv("GBX_NATIVE_ROOT", str(payload))
    monkeypatch.syspath_prepend(
        str(_dist_info(tmp_path / "site", "0.6.0+env6", REQUIRES))
    )
    monkeypatch.setattr("databricks.labs.gbx.__version__", "0.6.0+env6")
    seen, checked = [], []
    fake = _fake_pip(index, seen)

    def run(cmd, **kw):
        if "--dry-run" in cmd:
            checked.append((open(cmd[cmd.index("-r") + 1]).read(), cmd))
        return fake(cmd, **kw)

    out = tmp_path / "out"
    stage.download_deps(
        str(out),
        arches=("aarch64",),
        extras=("zarr",),
        uc=True,
        python=sys.executable,
        run=run,
    )
    ((requires, cmd),) = checked
    assert requires == "h3<5,>=4.1\nnumexpr>=2.14.1\nzarr<3.5,>=3.4.1\n"
    assert cmd[-2:] == [f"h3=={UC_PINS['h3']}", f"quadbin=={UC_PINS['quadbin']}"]
    rels = sorted(r for _, r in stage.read_wheels_txt(str(out)))
    assert rels == sorted(
        f"aarch64/{os.path.basename(p)}" for _, _, p in index["aarch64"].values()
    )
    assert "holds no env wheel" in capsys.readouterr().out
