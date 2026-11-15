"""scripts/ci/light_native.py: the payload the light CI shards test on (layout and install), on a fake native build."""

import importlib.util
import sys

import pytest
from conftest import REPO_ROOT

spec = importlib.util.spec_from_file_location("light_native", REPO_ROOT / "scripts" / "ci" / "light_native.py")
light_native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(light_native)


def _fake_build(tmp_path):
    out = tmp_path / "out"
    jvm = tmp_path / "jvm"
    (jvm / "_native" / "x86_64" / "lib").mkdir(parents=True)
    (jvm / "_native" / "x86_64" / "lib" / "libgdalalljni.so").write_bytes(b"jni")
    (jvm / "_native" / "share" / "proj").mkdir(parents=True)
    for name in light_native.BINDINGS:
        (out / "bindings" / "vendor" / name).mkdir(parents=True)
        (out / "bindings" / "vendor" / name / "__init__.py").write_text("")
    py = out / "stage-native" / "gdal-native" / "python"
    (py / "osgeo").mkdir(parents=True)
    (py / "osgeo" / "gdal.py").write_text("")
    (py / "osgeo_utils").mkdir()
    (py / "osgeo_utils" / "__init__.py").write_text("")
    rs = tmp_path / "lib_gbxrs.so"
    rs.write_bytes(b"rs")
    return out, jvm, rs


def test_assemble_lays_out_the_env_wheel_payload(tmp_path):
    out, jvm, rs = _fake_build(tmp_path)
    root = tmp_path / "payload"
    light_native.assemble(out, jvm, rs, root)
    v = root / "_vendor" / "x86_64"
    assert all((v / n / "__init__.py").is_file() for n in light_native.BINDINGS)
    assert (v / "osgeo" / "gdal.py").is_file()
    assert (root / "_vendor" / "common" / "osgeo_utils" / "__init__.py").is_file()
    assert (root / "_native" / "x86_64" / "rs" / "_gbxrs.abi3.so").read_bytes() == b"rs"
    assert (root / "_native" / "x86_64" / "lib" / "libgdalalljni.so").is_file()
    # osgeo's RUNPATH ($ORIGIN/../../lib) resolves to _vendor/lib: it must reach the payload's libraries
    assert (v / "osgeo" / ".." / ".." / "lib" / "libgdalalljni.so").resolve().is_file()


@pytest.mark.parametrize(
    "missing", ["bindings/vendor/rasterio", "stage-native/gdal-native/python/osgeo"]
)
def test_assemble_fails_loudly_when_a_piece_is_missing(tmp_path, capsys, missing):
    import shutil

    out, jvm, rs = _fake_build(tmp_path)
    shutil.rmtree(out / missing)
    with pytest.raises(SystemExit) as e:
        light_native.assemble(out, jvm, rs, tmp_path / "payload")
    assert e.value.code == 1
    assert "LIGHT PAYLOAD FAILED" in capsys.readouterr().out


def test_install_writes_the_startup_activation_and_exports_the_root(tmp_path, monkeypatch):
    out, jvm, rs = _fake_build(tmp_path)
    root = tmp_path / "payload"
    light_native.assemble(out, jvm, rs, root)
    site = tmp_path / "site"
    site.mkdir()
    env = tmp_path / "env"
    monkeypatch.setattr(light_native.sysconfig, "get_paths", lambda: {"purelib": str(site)})
    monkeypatch.setenv("GITHUB_ENV", str(env))
    light_native.install(root)
    assert (site / "geobrix-native.pth").read_text() == "import _gbx_native_path\n"
    assert (site / "_gbx_native_path.py").is_file()
    assert env.read_text() == f"GBX_NATIVE_ROOT={root.resolve()}\n"


def test_check_names_what_is_missing(tmp_path, monkeypatch, capsys):
    # no payload activation in this interpreter: the check must fail and say so, never pass quietly
    monkeypatch.delenv("GBX_NATIVE_ROOT", raising=False)
    monkeypatch.setitem(sys.modules, "_gbx_native_path", type(sys)("_gbx_native_path"))
    sys.modules["_gbx_native_path"].ACTIVE = False
    with pytest.raises(SystemExit) as e:
        light_native.check(tmp_path)
    assert e.value.code == 1
    out = capsys.readouterr().out
    assert "GBX_NATIVE_ROOT" in out and "not active" in out
