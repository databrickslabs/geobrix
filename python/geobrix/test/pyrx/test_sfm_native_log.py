"""pycolmap's native (glog) output is held to warnings and errors unless the caller asks for verbose."""

import sys
import types

import pytest

from databricks.labs.gbx.rasterx import _native_log, sfm

WARNING = 1


class _Logging:
    def __init__(self, level=0):
        self.minloglevel = level


def _fake_pycolmap(level=0):
    return types.SimpleNamespace(logging=_Logging(level))


@pytest.fixture(autouse=True)
def _no_env(monkeypatch):
    monkeypatch.delenv(_native_log.VERBOSE_ENV, raising=False)


def test_default_limits_native_log_to_warnings():
    pc = _fake_pycolmap(0)
    _native_log.quiet_native_log(pc)
    assert pc.logging.minloglevel == WARNING


def test_verbose_true_leaves_the_level_untouched():
    pc = _fake_pycolmap(0)
    assert _native_log.quiet_native_log(pc, verbose=True) is None
    assert pc.logging.minloglevel == 0


def test_env_switch_turns_the_native_log_back_on(monkeypatch):
    monkeypatch.setenv(_native_log.VERBOSE_ENV, "1")
    pc = _fake_pycolmap(0)
    _native_log.quiet_native_log(pc)
    assert pc.logging.minloglevel == 0


def test_a_stricter_level_is_never_lowered():
    pc = _fake_pycolmap(2)
    _native_log.quiet_native_log(pc)
    assert pc.logging.minloglevel == 2


def test_context_restores_the_level_even_on_error():
    pc = _fake_pycolmap(0)
    with pytest.raises(RuntimeError):
        with _native_log.native_log(pc):
            assert pc.logging.minloglevel == WARNING
            raise RuntimeError("boom")
    assert pc.logging.minloglevel == 0


def test_pycolmap_without_logging_api_is_tolerated():
    _native_log.quiet_native_log(types.SimpleNamespace())  # must not raise


def test_match_worker_sets_level_before_pycolmap_work(monkeypatch):
    """The mapInPandas matcher imports pycolmap on the worker: the level is set before any call."""
    import pandas as pd

    seen = []
    pc = _fake_pycolmap(0)

    def _extract(*a, **k):
        seen.append(pc.logging.minloglevel)
        raise RuntimeError("stop")

    pc.extract_features = _extract
    monkeypatch.setitem(sys.modules, "pycolmap", pc)
    row = {"src1": "a", "src2": "b"}
    list(sfm._match_pairs_dist(iter([pd.DataFrame([row])])))
    assert seen == [WARNING]


def test_match_worker_verbose_leaves_level(monkeypatch):
    import pandas as pd

    seen = []
    pc = _fake_pycolmap(0)

    def _extract(*a, **k):
        seen.append(pc.logging.minloglevel)
        raise RuntimeError("stop")

    pc.extract_features = _extract
    monkeypatch.setitem(sys.modules, "pycolmap", pc)
    row = {"src1": "a", "src2": "b"}
    list(sfm._match_pairs_dist(iter([pd.DataFrame([row])]), verbose=True))
    assert seen == [0]


def test_extract_child_script_limits_the_log_by_default(tmp_path, monkeypatch):
    """The per-image child process is a separate interpreter: the level is passed on its argv."""
    import pandas as pd

    captured = {}

    def _run(cmd, **k):
        captured["cmd"] = cmd
        captured["script"] = open(cmd[1]).read()
        return types.SimpleNamespace(returncode=1, stderr="")

    monkeypatch.setattr("subprocess.run", _run)
    img = tmp_path / "a.jpg"
    img.write_bytes(b"x")
    list(sfm._extract_features_to_df(iter([pd.DataFrame({"source": [str(img)]})])))
    assert "minloglevel" in captured["script"]
    assert captured["cmd"][-1] == "0"  # native_verbose flag: off

    list(
        sfm._extract_features_to_df(
            iter([pd.DataFrame({"source": [str(img)]})]), verbose=True
        )
    )
    assert captured["cmd"][-1] == "1"
