import pickle

import pytest

from databricks.labs.gbx.zarr import errors as E


@pytest.mark.parametrize(
    "cls",
    [
        E.ZarrWriteConflict,
        E.ZarrStoreDrift,
        E.ZarrReadOnly,
        E.ZarrGridMismatch,
        E.ZarrVersionUnavailable,
        E.ZarrAccessUnavailable,
        E.ZarrUnsupported,
        E.ZarrOwnershipError,
    ],
)
def test_errors_carry_fix(cls):
    e = cls("something broke", fix="do the thing")
    assert (
        isinstance(e, E.ZarrError)
        and str(e) == "something broke Fix: do the thing"
        and e.fix == "do the thing"
    )


def test_conflict_keys_and_winner():
    e = E.ZarrWriteConflict(
        "overlap", fix="re-read and retry", keys=("t2m/c/0/0/0",), winner="ab" * 16
    )
    assert e.keys == ("t2m/c/0/0/0",) and e.winner == "ab" * 16


def test_conflict_defaults_to_no_keys_and_no_winner():
    e = E.ZarrWriteConflict("overlap", fix="re-read and retry")
    assert e.keys == () and e.winner is None


def test_errors_survive_pickling():
    """Raised in a worker process, an error must reach the driver with its fix and
    conflict details intact (the keyword-only ``fix`` defeats the default reduce)."""
    e = E.ZarrWriteConflict("overlap", fix="retry", keys=("a", "b"), winner="cd" * 16)
    back = pickle.loads(pickle.dumps(e))
    assert type(back) is E.ZarrWriteConflict and str(back) == str(e)
    assert (back.fix, back.keys, back.winner) == ("retry", ("a", "b"), "cd" * 16)
    plain = pickle.loads(pickle.dumps(E.ZarrReadOnly("ro", fix="open for write")))
    assert type(plain) is E.ZarrReadOnly and plain.fix == "open for write"


def test_lazy_package_exports_errors():
    import databricks.labs.gbx.zarr as z

    assert z.ZarrWriteConflict is E.ZarrWriteConflict


def test_lazy_package_rejects_unknown_names():
    import databricks.labs.gbx.zarr as z

    with pytest.raises(AttributeError):
        z.not_a_public_name


def test_fixtures_build(era5_v3_sharded, era5_v2, era5_v3_unsharded):
    import zarr

    g = zarr.open_group(str(era5_v3_sharded), mode="r")
    assert g["t2m"].shape == (48, 73, 144) and g["t2m"].shards == (24, 74, 144)
    assert zarr.open_group(str(era5_v2), mode="r")["u10"].metadata.zarr_format == 2


def test_spark_delta_fixture(spark_delta):
    spark_delta.sql(
        "CREATE TABLE zfix (a INT) USING DELTA "
        "TBLPROPERTIES ('delta.enableChangeDataFeed'='true')"
    )
    try:
        spark_delta.sql("INSERT INTO zfix VALUES (1)")
        assert (
            spark_delta.sql(
                "SELECT count(*) c FROM table_changes('zfix', 0)"
            ).collect()[0]["c"]
            == 1
        )
    finally:
        spark_delta.sql("DROP TABLE zfix")
