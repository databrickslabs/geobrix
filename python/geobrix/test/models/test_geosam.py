import numpy as np
import pytest

from databricks.labs.gbx.models import geosam  # must import without torch present


def test_module_imports_without_model_deps():
    assert hasattr(geosam, "load_geosam") and hasattr(geosam, "segment")


def test_segment_uses_injected_backend(monkeypatch):
    class FakeSam:
        def generate(self, arr):
            m = np.zeros(arr.shape[:2], np.int32)
            m[0:2, 0:2] = 1
            return m

    h = geosam.GeoSamHandle(backend=FakeSam(), model_type="vit_b")
    out = geosam.segment(h, np.zeros((4, 4, 3), np.uint8))
    assert out.shape == (4, 4) and out.max() == 1


def test_load_without_deps_raises_clear_error(monkeypatch):
    monkeypatch.setattr(
        geosam,
        "_import_backend",
        lambda *a, **k: (_ for _ in ()).throw(ImportError("no torch")),
    )
    with pytest.raises(geosam.ModelDepsMissing, match=r"geobrix\[models_gpu_env5\]"):
        geosam.load_geosam(device="cpu")
