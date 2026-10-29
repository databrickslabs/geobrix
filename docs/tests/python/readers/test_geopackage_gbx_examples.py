"""Executes the gpkg_gbx reader doc examples against real sample data (Docker)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import geopackage_gbx_examples as ex  # noqa: E402


def test_read_gpkg_gbx(spark):
    ex.read_gpkg_gbx(spark)


def test_both_tiers_name_the_geometry_column_as_the_layer_does(spark):
    """The page says both tiers produce the same schema: the geometry column keeps the
    layer's name (`geom` in the sample file; `geom_0` only for a layer that names none)."""
    from databricks.labs.gbx.ds.register import register

    register(spark)
    light = spark.read.format("gpkg_gbx").load(ex.SAMPLE)
    heavy = spark.read.format("gpkg_ogr").load(ex.SAMPLE)
    assert light.columns == heavy.columns
    assert light.columns[-3:] == ["geom", "geom_srid", "geom_srid_proj"]
    assert "geom_0" not in light.columns


def test_regexp_on_a_geopackage_goes_through_indirect_sqlite():
    """REGEXP needs a GDAL built with PCRE2 (the env wheel's): the example runs against the built payload, in a
    process of its own, with GeoBrix imported first so the payload's pyogrio and GDAL serve it."""
    import json
    import os
    import platform
    import subprocess

    import pytest

    repo = Path(__file__).resolve().parents[4]
    payload = repo / "build" / "env-wheel" / "env6" / "payload"
    if not sys.platform.startswith("linux") or not (payload / "_native" / platform.machine()).is_dir():
        pytest.skip(f"PRECONDITION MISSING: {payload} (REGEXP needs the env wheel's GDAL; gbx:build:wheel --env 6)")
    here = Path(__file__).parent
    code = (
        "import json, databricks.labs.gbx, geopackage_gbx_examples as ex; "
        "print(json.dumps(list(ex.regexp_gpkg()['boroname'])))"
    )
    env = {**os.environ, "GBX_NATIVE_ROOT": str(payload)}
    env["PYTHONPATH"] = os.pathsep.join([str(repo / "python/geobrix/src"), str(here), str(here.parent)])
    env.pop("GBX_NATIVE_DISABLE", None)
    res = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    assert res.returncode == 0, res.stderr[-1500:]
    got = json.loads(res.stdout.splitlines()[-1])
    assert got == ["Manhattan"]
    assert ex.REGEXP_GPKG_output == str(got)
