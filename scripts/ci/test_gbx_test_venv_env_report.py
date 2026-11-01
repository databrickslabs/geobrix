"""Tests for scripts/ci/venv_env_report.py: local-version pins and the expected payload divergence.

The container carries the env wheel's payload GDAL stack (rasterio 1.5.2+gbx1, gdal 3.13.3,
proj 9.9.0, geos 3.15.0); the host uses PyPI wheels. A local version cannot be installed from
any index, so the host pins the public base, and the stack is reported as an expected
divergence, not drift.
"""

import importlib.util

from conftest import REPO_ROOT

_spec = importlib.util.spec_from_file_location(
    "venv_env_report", REPO_ROOT / "scripts" / "ci" / "venv_env_report.py"
)
report = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(report)

PAYLOAD_NATIVES = {"gdal": "3.13.3", "proj": "9.9.0", "geos": "3.15.0"}
HOST_NATIVES = {"gdal": "3.12.2", "proj": "9.5.1", "geos": "3.13.1"}


def _env(python="3.12.3", natives=None, **packages):
    return {
        "python": python,
        "packages": {k.replace("_", "-"): v for k, v in packages.items()},
        "natives": natives or dict(HOST_NATIVES),
    }


def _statuses(rows):
    return {r[0]: r[3] for r in rows}


def test_local_version_pins_are_stripped_for_the_host():
    container = _env(rasterio="1.5.2+gbx1", numpy="2.1.3", natives=PAYLOAD_NATIVES)
    lock = (
        "rasterio==1.4.0 \\\n    --hash=sha256:aa\n"
        "numpy==2.3.4 \\\n    --hash=sha256:bb\n"
        'marked==1.0 ; python_version < "3.13" \\\n    --hash=sha256:cc\n'
    )

    lines = report.match_requirements(lock, container)

    assert "rasterio==1.5.2" in lines  # the public base: +gbx1 is on no index
    assert not [x for x in lines if "+" in x.split(";")[0]]
    assert "numpy==2.1.3" in lines
    assert 'marked==1.0 ; python_version < "3.13"' in lines


def test_pin_drift_pins_the_public_base_and_skips_a_package_already_there():
    container = _env(rasterio="1.5.2+gbx1", numpy="2.1.3", scipy="1.16.3")
    host = _env(rasterio="1.5.2", numpy="2.3.4", scipy="1.16.3")

    assert report.pin_drift(host, container) == ["numpy==2.1.3"]

    stale = _env(rasterio="1.5.0", numpy="2.1.3", scipy="1.16.3")
    assert report.pin_drift(stale, container) == ["rasterio==1.5.2"]


def test_payload_stack_divergence_is_expected_not_drift():
    container = _env(rasterio="1.5.2+gbx1", numpy="2.1.3", natives=PAYLOAD_NATIVES)
    host = _env(rasterio="1.5.2", numpy="2.1.3")

    rows, notes = report.compare(host, container)

    status = _statuses(rows)
    assert status["rasterio"] == "EXPECTED"
    assert {status[k] for k in ("gdal", "proj", "geos")} == {"EXPECTED"}
    assert status["python"] == "OK"
    assert not any(r[3] == "DRIFT" for r in rows)
    warn = [n for n in notes if n.startswith("WARN")]
    assert len(warn) == 1
    for word in ("rasterio", "gdal", "proj", "geos", "container"):
        assert word in warn[0]


def test_other_drift_still_fails_beside_the_expected_divergence():
    container = _env(rasterio="1.5.2+gbx1", numpy="2.1.3", natives=PAYLOAD_NATIVES)
    host = _env(rasterio="1.5.2", numpy="2.3.4")

    rows, _ = report.compare(host, container)

    status = _statuses(rows)
    assert status["numpy"] == "DRIFT"
    assert status["rasterio"] == "EXPECTED"


def test_a_native_that_differs_without_the_payload_is_drift():
    # No local-version payload in the container: a different GDAL is real drift.
    container = _env(rasterio="1.5.2", natives=PAYLOAD_NATIVES)
    host = _env(rasterio="1.5.2")

    rows, notes = report.compare(host, container)

    assert _statuses(rows)["gdal"] == "DRIFT"
    assert not [n for n in notes if n.startswith("WARN")]


def test_a_native_the_host_cannot_reach_is_drift_even_beside_the_payload():
    container = _env(rasterio="1.5.2+gbx1", natives=PAYLOAD_NATIVES)
    host = _env(rasterio="1.5.2", natives={**HOST_NATIVES, "proj": None})

    rows, _ = report.compare(host, container)

    assert _statuses(rows)["proj"] == "DRIFT"


def test_a_local_version_outside_the_payload_stack_is_drift():
    container = _env(torch="2.5.0+cpu", rasterio="1.5.2+gbx1", natives=PAYLOAD_NATIVES)
    host = _env(torch="2.5.0", rasterio="1.5.2")

    rows, _ = report.compare(host, container)

    assert _statuses(rows)["torch"] == "DRIFT"


def test_the_expected_rows_are_printed_and_the_drift_exit_code_ignores_them(
    tmp_path, capsys
):
    import json

    container = _env(rasterio="1.5.2+gbx1", natives=PAYLOAD_NATIVES)
    host = _env(rasterio="1.5.2")
    h, c = tmp_path / "h.json", tmp_path / "c.json"
    h.write_text(json.dumps(host))
    c.write_text(json.dumps(container))

    rc = report.main(["compare", str(h), str(c)])

    out = capsys.readouterr().out
    assert rc == 0
    assert "EXPECTED" in out and "WARN" in out and "container" in out
    assert "DRIFT" not in out
