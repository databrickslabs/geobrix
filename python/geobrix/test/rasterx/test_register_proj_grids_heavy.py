import pytest
from databricks.labs.gbx import crs_grids


def test_heavy_apply_sets_jvm_registry(monkeypatch):
    calls = {}

    class _Reg:
        def set(self, jlist, replace):
            calls["dirs"] = list(jlist)
            calls["replace"] = replace

    class _JVM:
        class com:
            class databricks:
                class labs:
                    class gbx:
                        class operations:
                            ProjGridRegistry = _Reg()

    class _Spark:
        _jvm = _JVM()

    crs_grids.register_proj_grids(spark=_Spark(), dirs=["/Volumes/a"], replace=True)
    assert calls["dirs"] == ["/Volumes/a"]
    assert calls["replace"] is True
