"""Which Zarr stores GeoBrix leaves with consolidated metadata, and that it is fresh.

xarray's ``open_zarr`` asks for consolidated metadata first and, when a store has none,
warns ``Failed to open Zarr store with consolidated metadata ... much slower``. A store a
vendor wrote with ``to_zarr`` is consolidated, so what GeoBrix writes should be too, and
a copy made after an append must hold the metadata of the arrays as they are now, not as
they were. "Fresh" is judged against zarr-python itself: the consolidated copy must equal
what ``zarr.consolidate_metadata`` makes from the per-node documents in the store.

A managed (transactional) store is a manifest table plus pack files, not a Zarr store on
disk, so there is nothing at its root to consolidate: the stores it produces are the copy
of ``gbx_zarr_export(method="copy")`` and a publish into a registered base.
"""

import json
import uuid
import warnings

import numpy as np
import pandas as pd
import pytest

zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")

from databricks.labs.gbx.zarr._export import (  # noqa: E402
    _consolidated,
    gbx_zarr_export,
)
from databricks.labs.gbx.zarr._register import gbx_zarr_register  # noqa: E402
from databricks.labs.gbx.zarr._sync import publish  # noqa: E402
from databricks.labs.gbx.zarr._testing import (  # noqa: E402
    era5_like_dataset,
    make_era5_like,
)
from databricks.labs.gbx.zarr._write import gbx_zarr_write  # noqa: E402

CHUNKS = {"time": 12, "latitude": 5, "longitude": 6}
DIMS = ("time", "latitude", "longitude")
_NAMES = ("zarr.json", ".zarray", ".zattrs", ".zgroup")


def _dataset(hours=48, start="2023-01-01", seed=0):
    rng = np.random.default_rng(seed)
    shape = (hours, 10, 12)
    return xr.Dataset(
        {"t2m": (DIMS, (280 + rng.normal(size=shape)).astype("float32"))},
        coords={
            "time": ("time", pd.date_range(start, periods=hours, freq="h")),
            "latitude": ("latitude", np.linspace(45.0, 36.0, 10)),
            "longitude": ("longitude", np.arange(12) * 1.0),
        },
    )


@pytest.fixture
def drop(spark_delta):
    made = []
    yield made.append
    for t in made:
        spark_delta.sql(f"DROP TABLE IF EXISTS {t}")


def _root_document(path):
    """``(key, parsed)`` of the root's consolidated copy, or ``(key, None)`` when it has none."""
    z3, z2 = path / "zarr.json", path / ".zmetadata"
    if z2.is_file():
        return ".zmetadata", json.loads(z2.read_text())
    doc = json.loads(z3.read_text())
    return "zarr.json", doc.get("consolidated_metadata")


def assert_consolidated_and_fresh(path):
    """The store at ``path`` carries consolidated metadata equal to what zarr makes from
    its per-node documents."""
    key, got = _root_document(path)
    assert got is not None, f"{path} has no consolidated metadata"
    docs = {}
    for f in sorted(p for p in path.rglob("*") if p.name in _NAMES):
        docs[f.relative_to(path).as_posix()] = f.read_bytes()
    if key == "zarr.json":  # the per-node root document, without its consolidated block
        root = json.loads(docs["zarr.json"])
        root.pop("consolidated_metadata", None)
        docs["zarr.json"] = json.dumps(root).encode()
    want = json.loads(_consolidated(docs)[key])
    want = want if key == ".zmetadata" else want["consolidated_metadata"]
    assert got == want, f"{path} consolidated metadata is stale"


def assert_opens_without_warning(path, **kw):
    """``xr.open_zarr(path)`` (``consolidated=None``) raises no consolidated-metadata warning."""
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        xr.open_zarr(str(path), chunks=None, **kw).close()
    assert not [w for w in seen if "consolidated" in str(w.message)], [
        str(w.message) for w in seen
    ]


# --- the control: an unconsolidated store warns, so the checks above can fail ---------


def test_xarray_warns_on_a_store_without_consolidated_metadata(tmp_path):
    store = make_era5_like(tmp_path / "plain.zarr", zarr_format=3)
    assert _root_document(store)[1] is None
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        xr.open_zarr(str(store), chunks=None).close()
    assert [w for w in seen if "consolidated" in str(w.message)]
    with pytest.raises(AssertionError):
        assert_opens_without_warning(store)
    with pytest.raises(AssertionError):
        assert_consolidated_and_fresh(store)


# --- a plain store ----------------------------------------------------------------------


def test_a_plain_create_and_append_are_consolidated_and_fresh(spark_delta, tmp_path):
    out = tmp_path / "out.zarr"
    first, second = _dataset(48), _dataset(24, start="2023-01-03", seed=2)
    gbx_zarr_write(
        first, str(out), transactional=False, chunks=CHUNKS, spark=spark_delta
    )
    assert_consolidated_and_fresh(out)
    assert_opens_without_warning(out)

    gbx_zarr_write(
        second,
        str(out),
        transactional=False,
        mode="append",
        append_dim="time",
        spark=spark_delta,
    )
    assert_consolidated_and_fresh(out)  # the shape in it is the appended one
    assert_opens_without_warning(out)
    assert xr.open_zarr(str(out), consolidated=True).sizes["time"] == 72


# --- a managed store: its copy ----------------------------------------------------------


def test_the_copy_of_a_managed_store_is_consolidated_and_fresh_after_an_append(
    spark_delta, drop, tmp_path
):
    table = f"gbx_zarr_consolidated_{uuid.uuid4().hex[:8]}"
    drop(table)
    gbx_zarr_write(
        _dataset(48),
        table,
        root=str(tmp_path / "root"),
        chunks=CHUNKS,
        spark=spark_delta,
    )
    gbx_zarr_write(
        _dataset(24, start="2023-01-03", seed=2),
        table,
        mode="append",
        spark=spark_delta,
    )
    copy = tmp_path / "copy.zarr"
    gbx_zarr_export(spark_delta, table, str(copy))
    assert_consolidated_and_fresh(copy)
    assert_opens_without_warning(copy)
    assert xr.open_zarr(str(copy), consolidated=True).sizes["time"] == 72


# --- a registered store, published into --------------------------------------------------


@pytest.mark.parametrize("fmt", [3, 2])
def test_a_publish_leaves_a_consolidated_base_fresh(spark_delta, drop, tmp_path, fmt):
    store = make_era5_like(tmp_path / "base.zarr", zarr_format=fmt)
    zarr.consolidate_metadata(str(store))
    assert_consolidated_and_fresh(store)
    table = f"gbx_zarr_consolidated_{uuid.uuid4().hex[:8]}"
    drop(table)
    gbx_zarr_register(
        spark_delta,
        str(store),
        table,
        writable=True,
        overlay_root=str(tmp_path / "overlay"),
    )
    later = era5_like_dataset().isel(time=slice(0, 12))
    later = later.assign_coords(time=later.time.values + np.timedelta64(48, "h"))
    gbx_zarr_write(later, table, mode="append", spark=spark_delta)
    publish(spark_delta, table)
    assert_consolidated_and_fresh(store)
    assert xr.open_zarr(str(store), consolidated=True).sizes["time"] == 60
