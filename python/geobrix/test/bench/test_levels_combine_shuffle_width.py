"""The combine L3 form with a wider shuffle: a bench-only variant, set on classic sessions only."""

from databricks.labs.gbx.bench import levels_forms as lf


class _Conf:
    def __init__(self, store):
        self.store = store

    def get(self, key, default=None):
        return self.store.get(key, default)

    def set(self, key, value):
        self.store[key] = str(value)

    def unset(self, key):
        self.store.pop(key, None)


class _Df:
    def __init__(self, seen, conf):
        self._seen, self._conf = seen, conf

    def collect(self):
        self._seen.append(self._conf.get("spark.sql.shuffle.partitions"))
        return [(4, 10, 4)]


class _Spark:
    def __init__(self, store):
        self.conf = _Conf(store)
        self.seen = []
        self.sql_text = []

    def sql(self, text):
        self.sql_text.append(text)
        return _Df(self.seen, self.conf)


def _form(variant):
    (f,) = [f for f in lf.FORMS["H"] if f.variant == variant]
    return f


def test_variant_is_the_l3_form_with_its_own_run():
    base, wide = _form("h_combine_l3"), _form("h_combine_l3_sp1000")
    assert (wide.fn, wide.level, wide.input) == (base.fn, base.level, base.input)
    assert wide.setup == base.setup  # the same light function is registered
    assert wide.run.endswith(":h_combine_l3_sp1000") and not wide.sql


def test_classic_session_runs_the_query_at_1000_and_restores(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    store = {"spark.sql.shuffle.partitions": "200"}
    spark = _Spark(store)
    n, fp, n_out, df = lf.h_combine_l3_sp1000(spark, "t_src")
    assert (n, fp, n_out) == (4, 10, 4)
    assert spark.seen == ["1000"]
    assert store["spark.sql.shuffle.partitions"] == "200"
    assert "t_src" in spark.sql_text[0] and "gbx_rst_combineavg" in spark.sql_text[0]


def test_unset_conf_is_unset_again_after(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "19.0")
    store = {}
    lf.h_combine_l3_sp1000(_Spark(store), "t_src")
    assert "spark.sql.shuffle.partitions" not in store


def test_serverless_never_sets_the_conf(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.5")
    store = {"spark.sql.shuffle.partitions": "auto"}
    spark = _Spark(store)
    lf.h_combine_l3_sp1000(spark, "t_src")
    assert spark.seen == ["auto"]
    assert store["spark.sql.shuffle.partitions"] == "auto"
