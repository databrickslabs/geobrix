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
import os

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


def test_profile_auth_skipped_in_databricks_runtime(monkeypatch):
    """On-cluster (notebook/job/Serverless/model-serving) auth is AMBIENT:
    _apply_profile_auth must NOT set DATABRICKS_CONFIG_PROFILE. Setting it to a dev
    profile name points MLflow/the deploy client at a ~/.databrickscfg entry that does
    not exist in the job container -- the real cause of the nb3 register_to_unity_gateway
    crash on Serverless GPU ('Reading Databricks credential configuration failed')."""
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.2")
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    serving._apply_profile_auth("oauth-fe")
    assert "DATABRICKS_CONFIG_PROFILE" not in os.environ


def test_profile_auth_applied_off_cluster(monkeypatch):
    """Off-cluster (local dev registering to a remote workspace) the caller's profile IS
    applied so MLflow/the deploy client can authenticate to that workspace."""
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.delenv("DB_IS_DRIVER", raising=False)
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    serving._apply_profile_auth("oauth-fe")
    assert os.environ["DATABRICKS_CONFIG_PROFILE"] == "oauth-fe"


def test_profile_auth_noop_when_profile_none(monkeypatch):
    """profile=None (on-cluster caller that omits it entirely) never mutates the env."""
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    serving._apply_profile_auth(None)
    assert "DATABRICKS_CONFIG_PROFILE" not in os.environ


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


def test_register_bundles_wheel_via_code_paths(monkeypatch):
    """mlflow's private-wheel pattern (the fix): code_paths=[wheel_path] bundles the
    wheel INTO the model (mlflow copies it to code/<basename> -- no subdirs), and
    pip_requirements must reference that BUNDLED path, not the original
    file:///Volumes/... URI, which the Model Serving container build cannot reach --
    that unreachable-URI install is the bug this fixes."""
    calls = {}
    monkeypatch.setattr(
        serving, "_mlflow_log_and_register", lambda **k: calls.update(k) or "3"
    )
    serving.register_to_unity_gateway(object(), "cat.sch.geosam", profile="oauth-fe")
    assert calls["code_paths"] == [serving.DEFAULT_WHEEL_PATH]
    pip_reqs_str = " ".join(calls["pip_reqs"])
    assert "code/geobrix-0.5.2-py3-none-any.whl[models_gpu_env5]" in pip_reqs_str
    assert "segment-geospatial" in pip_reqs_str
    assert "file:///Volumes" not in pip_reqs_str


def test_register_wheel_path_override_changes_pip_reqs(monkeypatch):
    """A caller-supplied wheel_path (e.g. a different staged version) must flow through
    to BOTH code_paths and the bundled-basename pip requirement -- not just one."""
    calls = {}
    monkeypatch.setattr(
        serving, "_mlflow_log_and_register", lambda **k: calls.update(k) or "3"
    )
    serving.register_to_unity_gateway(
        object(),
        "cat.sch.geosam",
        profile="oauth-fe",
        wheel_path="/Volumes/x/y/geobrix-9.9.9-py3-none-any.whl",
    )
    assert calls["code_paths"] == ["/Volumes/x/y/geobrix-9.9.9-py3-none-any.whl"]
    assert "code/geobrix-9.9.9-py3-none-any.whl[models_gpu_env5]" in " ".join(
        calls["pip_reqs"]
    )


def test_register_pip_reqs_override_still_passes_code_paths(monkeypatch):
    """An explicit pip_reqs= override replaces the computed default, but code_paths
    (the wheel bundling itself) is always passed regardless."""
    calls = {}
    monkeypatch.setattr(
        serving, "_mlflow_log_and_register", lambda **k: calls.update(k) or "3"
    )
    serving.register_to_unity_gateway(
        object(),
        "cat.sch.geosam",
        profile="oauth-fe",
        pip_reqs=["some-other-pkg"],
    )
    assert calls["pip_reqs"] == ["some-other-pkg"]
    assert calls["code_paths"] == [serving.DEFAULT_WHEEL_PATH]


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
    # workload_size (workloadSizeId) is REQUIRED by the Serving API -- omitting it fails
    # create with '400 workloadSizeId is undefined' (observed on the nb3 GPU run).
    assert entity["workload_size"] == "Small"
    assert entity["scale_to_zero_enabled"] is True


def test_await_ready_returns_final_info_once_ready():
    """A get_info that reports NOT_READY/IN_PROGRESS a couple times, then
    READY+NOT_UPDATING, must return that final info. No real sleep -- _sleep is a
    no-op seam."""
    responses = [
        {"state": {"ready": "NOT_READY", "config_update": "IN_PROGRESS"}},
        {"state": {"ready": "NOT_READY", "config_update": "IN_PROGRESS"}},
        {"state": {"ready": "READY", "config_update": "NOT_UPDATING"}},
    ]
    calls = {"n": 0}

    def _get_info():
        info = responses[calls["n"]]
        calls["n"] += 1
        return info

    out = serving._await_ready(_get_info, _sleep=lambda s: None)
    assert out == {"state": {"ready": "READY", "config_update": "NOT_UPDATING"}}
    assert calls["n"] == 3


def test_await_ready_raises_on_terminal_failure():
    def _get_info():
        return {"state": {"ready": "NOT_READY", "config_update": "UPDATE_FAILED"}}

    with pytest.raises(RuntimeError):
        serving._await_ready(_get_info, _sleep=lambda s: None)


def test_await_ready_raises_on_timeout():
    """A get_info that never becomes ready must raise TimeoutError once timeout_s
    elapses -- driven by a fake _clock so the test does not actually sleep."""
    clock = {"t": 0.0}

    def _get_info():
        return {"state": {"ready": "NOT_READY", "config_update": "IN_PROGRESS"}}

    def _sleep(s):
        clock["t"] += s

    def _clock():
        return clock["t"]

    with pytest.raises(TimeoutError):
        serving._await_ready(
            _get_info, timeout_s=5.0, poll_s=10.0, _sleep=_sleep, _clock=_clock
        )


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
