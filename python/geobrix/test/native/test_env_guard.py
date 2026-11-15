"""The env wheel refuses the other environment's runtime, naming the file to install.

The rows are the signals measured on each surface: Serverless sets
DATABRICKS_ENV_VERSION, classic sets DATABRICKS_RUNTIME_VERSION, and the UC sandbox has only
/databricks/DBR_VERSION, which follows the function's declared environment. With no Databricks
signal at all the guard never raises.
"""

import pytest

from databricks.labs.gbx import _env_guard

CASES = [  # environ, /databricks/DBR_VERSION, protobuf, wheel env, file named in the error (None = ok)
    (
        {"DATABRICKS_ENV_VERSION": "6", "DATABRICKS_RUNTIME_VERSION": "client.6.4"},
        "client.6.4",
        6,
        6,
        None,
    ),
    (
        {"DATABRICKS_ENV_VERSION": "5", "DATABRICKS_RUNTIME_VERSION": "client.5.12"},
        "client.5.12",
        5,
        6,
        "geobrix-0.6.0+env5-py3-none-any.whl",
    ),
    (
        {"DATABRICKS_RUNTIME_VERSION": "18.3"},
        "18.3",
        5,
        6,
        "geobrix-0.6.0+env5-py3-none-any.whl",
    ),
    (
        {"DATABRICKS_RUNTIME_VERSION": "19.12"},
        "19.12",
        6,
        5,
        "geobrix-0.6.0+env6-py3-none-any.whl",
    ),
    (
        {},
        "client.5.12",
        5,
        6,
        "geobrix-0.6.0+env5-py3-none-any.whl",
    ),  # UC sandbox, function declared '5'
    ({}, None, 5, 6, None),  # no Databricks signal: never raises
]


@pytest.mark.parametrize("environ, dbr_version, protobuf, wheel_env, named", CASES)
def test_the_guard(
    tmp_path, monkeypatch, environ, dbr_version, protobuf, wheel_env, named
):
    path = tmp_path / "DBR_VERSION"
    if dbr_version is not None:
        path.write_text(dbr_version + "\n")
    monkeypatch.setattr(_env_guard, "_protobuf_major", lambda: protobuf)
    check = lambda: _env_guard.check(  # noqa: E731
        build_env=wheel_env,
        version=f"0.6.0+env{wheel_env}",
        environ=environ,
        dbr_version_path=str(path),
    )
    if named is None:
        check()
    else:
        with pytest.raises(RuntimeError, match=named.replace("+", r"\+")):
            check()
