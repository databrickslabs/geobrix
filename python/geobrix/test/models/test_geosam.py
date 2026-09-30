import numpy as np
import pytest

from databricks.labs.gbx.models import geosam  # must import without torch present


def test_module_imports_without_model_deps():
    assert hasattr(geosam, "load_geosam") and hasattr(geosam, "segment")


def test_segment_uses_injected_backend(monkeypatch):
    class FakeSam:
        """Mirrors the real SamGeo.generate contract: it performs automatic mask
        generation but RETURNS None, storing the label mask on self.objects (a 2D
        array, 0=background, unique positive int per object) -- not the return
        value. segment() must read handle.backend.objects, not generate()'s
        return, or this test would pass against the old (wrong) implementation."""

        def generate(self, arr, **kwargs):
            m = np.zeros(arr.shape[:2], np.int32)
            m[0:2, 0:2] = 1
            self.objects = m
            return None

    h = geosam.GeoSamHandle(backend=FakeSam(), model_type="vit_b")
    out = geosam.segment(h, np.zeros((4, 4, 3), np.uint8))
    assert out.shape == (4, 4) and out.max() == 1
    assert out.dtype == np.int32


def test_segment_all_background_when_objects_none(monkeypatch):
    """On an all-background chip, SamGeo may leave handle.backend.objects as None
    instead of a 2D label mask. segment() must treat that as an all-background
    result -- an HxW int32 zeros mask sized from the INPUT image -- rather than
    letting np.asarray(None).astype("int32") produce an invalid 0-d array."""

    class FakeSam:
        def generate(self, arr, **kwargs):
            self.objects = None
            return None

    h = geosam.GeoSamHandle(backend=FakeSam(), model_type="vit_b")
    out = geosam.segment(h, np.zeros((5, 7, 3), np.uint8))
    assert out.shape == (5, 7)
    assert out.dtype == np.int32
    assert out.max() == 0


def test_load_without_deps_raises_clear_error(monkeypatch):
    monkeypatch.setattr(
        geosam,
        "_import_backend",
        lambda *a, **k: (_ for _ in ()).throw(ImportError("no torch")),
    )
    with pytest.raises(geosam.ModelDepsMissing, match=r"geobrix\[models_gpu_env5\]"):
        geosam.load_geosam(device="cpu")
