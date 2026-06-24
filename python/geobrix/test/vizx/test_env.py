import builtins

import pytest

from databricks.labs.gbx.vizx._env import assert_viz_available


def test_assert_viz_available_passes_when_present():
    # matplotlib + geopandas are installed in the light/dev/CI env.
    assert assert_viz_available() is None


def test_assert_viz_available_raises_actionable_error(monkeypatch):
    real_import = builtins.__import__

    def fake(name, *a, **k):
        if name == "geopandas":
            raise ImportError("No module named 'geopandas'")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake)
    with pytest.raises(ImportError) as ei:
        assert_viz_available()
    msg = str(ei.value)
    assert "geopandas" in msg and "geobrix[vizx]" in msg
