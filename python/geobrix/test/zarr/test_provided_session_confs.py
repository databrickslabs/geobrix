"""The confs the zarr suites give the session Databricks provides (``conftest.PROVIDED_CONFS``)."""

from test import _provided_spark as ps
from test.zarr.conftest import PROVIDED_CONFS, provided_session_confs


class _Conf:
    def __init__(self, values, refused=()):
        self.values, self.refused = dict(values), set(refused)

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value):
        if key in self.refused:
            raise RuntimeError(f"[CONFIG_NOT_AVAILABLE] {key}")
        self.values[key] = value

    def unset(self, key):
        self.values.pop(key, None)


class _Session:
    def __init__(self, conf):
        self.conf = conf


def test_the_confs_hold_for_the_block_and_the_session_gets_its_own_back():
    conf = _Conf({"spark.sql.shuffle.partitions": "200", "other": "x"})
    with provided_session_confs(_Session(conf), PROVIDED_CONFS):
        assert {k: conf.values[k] for k in PROVIDED_CONFS} == PROVIDED_CONFS
    assert conf.values == {"spark.sql.shuffle.partitions": "200", "other": "x"}


def test_a_conf_the_session_refuses_stays_the_platforms():
    conf = _Conf({}, refused={"spark.databricks.delta.autoCompact.enabled"})
    with provided_session_confs(_Session(conf), PROVIDED_CONFS):
        assert "spark.databricks.delta.autoCompact.enabled" not in conf.values
        assert conf.values["spark.sql.shuffle.partitions"] == "2"
    assert conf.values == {}


def test_the_local_session_gets_nothing():
    conf = _Conf({"spark.sql.shuffle.partitions": "2"})
    with provided_session_confs(_Session(conf), {}):
        pass
    assert conf.values == {"spark.sql.shuffle.partitions": "2"}


def test_a_delta_path_quoted_part_by_part_is_qualified_too(tmp_path):
    """GeoBrix quotes every part of a table name (``_manifest._q``), so the ledger reaches Spark as
    ```delta`.`<path>```; that spelling gets the same rule. Here, not in test/test_provided_spark.py: the
    zarr modules import pyarrow, which the heavy CI job does not install."""
    from databricks.labs.gbx.zarr._manifest import _q
    from databricks.labs.gbx.zarr._register import _ledger_table

    root = tmp_path / "root_s"
    root.mkdir()
    name = _q(_ledger_table(str(root)))
    assert ps._qualify_sql(f"DESCRIBE DETAIL {name}") == (
        f"DESCRIBE DETAIL `delta`.`file:{root / '_gbx_owner'}`"
    )
    assert ps._qualify_sql("SELECT * FROM `delta`.`/Volumes/c/s/v/t`") == (
        "SELECT * FROM `delta`.`/Volumes/c/s/v/t`"
    )
