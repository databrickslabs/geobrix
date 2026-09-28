"""Tests for gbx.models.serving.

RULING R4: mlflow is ABSENT from the light CI env (not in
requirements-light-env6-ci.txt; confirmed not importable in the geobrix-dev container
either). serving.py imports mlflow lazily inside each function that needs it, so the
module itself always imports cleanly. These tests split the same way:

- predict-logic and I/O-contract tests exercise plain data (_predict_geojson,
  PREDICT_INPUT_COLUMNS/PREDICT_OUTPUT_COLUMNS) and never touch mlflow.
- register/create/query tests monkeypatch the _mlflow_log_and_register /
  _serving_create / _serving_query seams, so they never import mlflow either.
- The two tests that need a REAL mlflow object (a PythonModel instance, a
  ModelSignature) are guarded with pytest.importorskip("mlflow") -- they SKIP in this
  environment and exist for a future env where mlflow is installed.
"""

import base64
import json

import numpy as np
import pandas as pd
import pytest
import shapely.geometry
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.models import serving

_FAKE_GEOM_WKB = shapely.wkb.dumps(shapely.geometry.box(0, 0, 1, 1))


def _image_b64(arr):
    """Base64-encode ``arr`` (HxWx3 uint8) as GTiff bytes via rasterio's MemoryFile --
    the same construction ``conftest.py``'s ``_open_rgb`` uses for the runner fixtures
    -- so this test depends only on deps the models tests already own, not the
    transitive-only ``imageio``. These tests monkeypatch ``runner.segment_raster``, so
    the bytes are never actually decoded as a raster; only base64 round-tripping and
    the mlflow-free predict-logic contract are exercised."""
    height, width = arr.shape[:2]
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=3,
        dtype="uint8",
        crs="EPSG:32633",
        transform=from_origin(0, height, 1, 1),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(np.moveaxis(arr, -1, 0))
        return base64.b64encode(mf.read()).decode()


def _fake_segment_raster(*_a, **_k):
    return pd.DataFrame({"label": [1], "geom": [_FAKE_GEOM_WKB], "score": [0.9]})


def test_predict_geojson_returns_feature_collection(monkeypatch):
    """R4: the predict LOGIC lives in _predict_geojson, mlflow-free. Patches the
    runner module ATTRIBUTE (not a name copied at def time) so the lazy in-function
    `from databricks.labs.gbx.models import runner` call inside _predict_geojson picks
    up the patch."""
    monkeypatch.setattr(
        "databricks.labs.gbx.models.runner.segment_raster", _fake_segment_raster
    )
    out = serving._predict_geojson(
        {"image_b64": _image_b64(np.zeros((8, 8, 3), np.uint8))}
    )
    fc = json.loads(out["geojson"])
    assert fc["type"] == "FeatureCollection"
    assert len(fc["features"]) == 1
    assert fc["features"][0]["properties"]["label"] == 1


def test_predict_geojson_accepts_dataframe_input(monkeypatch):
    """mlflow serving hands a PythonModel.predict a pandas DataFrame (one row per
    dataframe_records item), not a plain dict -- _predict_geojson must handle both."""
    monkeypatch.setattr(
        "databricks.labs.gbx.models.runner.segment_raster", _fake_segment_raster
    )
    model_input = pd.DataFrame(
        {"image_b64": [_image_b64(np.zeros((8, 8, 3), np.uint8))]}
    )
    out = serving._predict_geojson(model_input)
    fc = json.loads(out["geojson"])
    assert fc["type"] == "FeatureCollection"


def test_io_contract_columns():
    """R4 mlflow-free surrogate for the signature Review-Focus case: the plain-data
    I/O contract that geosam_signature() converts lazily."""
    assert [n for n, _ in serving.PREDICT_INPUT_COLUMNS] == ["image_b64"]
    assert [n for n, _ in serving.PREDICT_OUTPUT_COLUMNS] == ["geojson"]


def test_pyfunc_predict_returns_geojson(monkeypatch):
    """Brief's original Step-1 case (needs a real mlflow.pyfunc.PythonModel)."""
    pytest.importorskip("mlflow")
    monkeypatch.setattr(
        "databricks.labs.gbx.models.runner.segment_raster", _fake_segment_raster
    )
    pf = serving.build_geosam_pyfunc()
    out = pf.predict(None, {"image_b64": _image_b64(np.zeros((8, 8, 3), np.uint8))})
    fc = json.loads(out["geojson"])
    assert fc["type"] == "FeatureCollection"


def test_signature_matches_predict_output_keys():
    """Brief's Review-Focus case (needs a real mlflow.models.ModelSignature)."""
    pytest.importorskip("mlflow")
    sig = serving.geosam_signature()
    assert "geojson" in [
        c["name"] for c in sig.outputs.to_dict()
    ]  # Review Focus: I/O contract
    assert "image_b64" in [c["name"] for c in sig.inputs.to_dict()]


def test_register_uses_uc_registry(monkeypatch):
    calls = {}
    monkeypatch.setattr(
        serving, "_mlflow_log_and_register", lambda **k: calls.update(k) or "3"
    )
    v = serving.register_to_unity_gateway(
        object(), "cat.sch.geosam", profile="oauth-fe"
    )
    assert v == "3" and calls["name"] == "cat.sch.geosam"
    assert "geobrix" in " ".join(calls["pip_reqs"])  # geobrix rides into the served env


def test_create_endpoint_uses_serving_seam(monkeypatch):
    calls = {}

    def _fake_create(endpoint_name, config, *, profile):
        calls["endpoint_name"] = endpoint_name
        calls["config"] = config
        calls["profile"] = profile
        return {"state": {"ready": "READY", "config_update": "NOT_UPDATING"}}

    monkeypatch.setattr(serving, "_serving_create", _fake_create)
    out = serving.create_endpoint(
        "cat.sch.geosam", "3", profile="oauth-fe", endpoint_name="geosam-endpoint"
    )
    assert out == {"state": {"ready": "READY", "config_update": "NOT_UPDATING"}}
    assert calls["endpoint_name"] == "geosam-endpoint"
    assert calls["profile"] == "oauth-fe"
    entity = calls["config"]["served_entities"][0]
    assert entity["entity_name"] == "cat.sch.geosam"
    assert entity["entity_version"] == "3"
    assert entity["workload_type"] == "GPU_MEDIUM"
    assert entity["scale_to_zero_enabled"] is True


def test_query_uses_serving_seam(monkeypatch):
    calls = {}

    def _fake_query(endpoint_name, image_b64, *, profile):
        calls["endpoint_name"] = endpoint_name
        calls["image_b64"] = image_b64
        calls["profile"] = profile
        return {"predictions": [{"geojson": "{}"}]}

    monkeypatch.setattr(serving, "_serving_query", _fake_query)
    out = serving.query("geosam-endpoint", b"raw-image-bytes", profile="oauth-fe")
    assert out == {"predictions": [{"geojson": "{}"}]}
    assert calls["endpoint_name"] == "geosam-endpoint"
    assert calls["profile"] == "oauth-fe"
    assert base64.b64decode(calls["image_b64"]) == b"raw-image-bytes"
