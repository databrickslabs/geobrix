"""Env-v5 (Python 3.12) compatibility guard for the [stac] extra (pin sanity only;
the live import is exercised by examples/tests/stac_env_v5_smoke.py on Serverless)."""

import pathlib
import re


def _stac_deps():
    txt = (pathlib.Path(__file__).resolve().parents[2] / "pyproject.toml").read_text()
    block = re.search(r"\nstac = \[(.*?)\]", txt, re.S)
    assert block, "[stac] extra not found in pyproject.toml"
    return block.group(1)


def test_stac_extra_declares_pystac_and_pc():
    deps = _stac_deps()
    assert "pystac-client" in deps and "planetary-computer" in deps


def test_stac_pins_support_py312():
    deps = _stac_deps()
    assert re.search(r"pystac-client>=0\.7", deps)
    assert re.search(r"planetary-computer>=1\.0", deps)
