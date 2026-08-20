"""Tests for OpenResourceLRU — per-partition byte-budgeted LRU cache."""

from databricks.labs.gbx.pyrx.grouped_exec import OpenResourceLRU


def test_byte_budget_evicts_oldest_over_budget():
    closed = []
    lru = OpenResourceLRU(
        max_bytes=100,
        max_count=1000,
        opener=lambda k: k,
        closer=lambda r: closed.append(r),
        weigher=lambda r, k: 40,
    )
    lru.get("a")
    lru.get("b")  # 80 bytes; both fit
    assert lru.bytes == 80 and lru.evictions == 0
    lru.get("c")  # 120 > 100 -> evict oldest "a" -> back to 80
    assert closed == ["a"] and lru.bytes == 80 and lru.evictions == 1


def test_many_small_files_stay_warm_under_budget():
    lru = OpenResourceLRU(
        max_bytes=4 * 1024**3,
        max_count=1000,
        opener=lambda k: k,
        closer=lambda r: None,
        weigher=lambda r, k: 32 * 1024**2,
    )  # 32 MiB each
    for i in range(100):
        lru.get(f"f{i}")  # 100 * 32 MiB = 3.125 GiB < 4 GiB
    assert lru.evictions == 0 and lru.opens == 100


def test_count_guard_bounds_handles_when_weight_nominal():
    lru = OpenResourceLRU(
        max_bytes=10**12,
        max_count=2,
        opener=lambda k: k,
        closer=lambda r: None,
        weigher=lambda r, k: 0,
    )  # streams ~ nominal
    lru.get("a")
    lru.get("b")
    lru.get("c")
    assert lru.evictions == 1  # count guard fired at 3 > 2


def test_never_evicts_the_current_entry():
    closed = []
    lru = OpenResourceLRU(
        max_bytes=10,
        max_count=1000,
        opener=lambda k: k,
        closer=lambda r: closed.append(r),
        weigher=lambda r, k: 999,
    )  # single entry exceeds budget
    got = lru.get("big")
    assert got == "big" and closed == []  # current entry kept despite over-budget


def test_close_all_closes_remaining():
    closed = []
    lru = OpenResourceLRU(
        max_bytes=100,
        opener=lambda k: k,
        closer=lambda r: closed.append(r),
        weigher=lambda r, k: 10,
    )
    lru.get("x")
    lru.get("y")
    lru.close_all()
    assert sorted(closed) == ["x", "y"] and lru.bytes == 0


# ---------------------------------------------------------------------------
# Task 4: real-size weigher (not STREAM_NOMINAL_BYTES)
# ---------------------------------------------------------------------------


def test_lru_weighs_by_real_size():
    """LRU uses the weigher's return value (60 bytes each); oldest evicted when over budget."""
    closed = []
    lru = OpenResourceLRU(
        max_bytes=100,
        max_count=8,
        opener=lambda k: {"k": k},
        closer=lambda s: closed.append(s),
        weigher=lambda s, k: 60,  # 60 bytes each
    )
    lru.get("a")
    lru.get("b")  # 120 > 100 → oldest ("a") evicted
    assert closed == [{"k": "a"}]


# ---------------------------------------------------------------------------
# Task 4: staged-temp cleanup on eviction (temp-leak fix)
# ---------------------------------------------------------------------------


def _make_tiny_gtiff_bytes():
    """Return bytes of a minimal valid GeoTIFF (4×3 float32, no Spark needed)."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
        nodata=-9999.0,
    )
    data = np.arange(12, dtype="float32").reshape(3, 4)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
        return mf.read()


def test_make_opener_closer_deletes_staged_temp(tmp_path, monkeypatch):
    """Closer removes the staged temp file on eviction (temp-leak fix).

    Uses the real _OpenerContext closer+opener seam:
    - monkeypatch _stage_local_if_needed to return a known temp with is_temp=True
    - call ctx.open() so it registers the temp in _staged_temps
    - call ctx.close() on the returned dataset
    - assert the temp file no longer exists
    """
    from databricks.labs.gbx.pyrx.core import preparer
    from databricks.labs.gbx.pyrx.grouped_exec import _OpenerContext

    tif_bytes = _make_tiny_gtiff_bytes()

    # Create the "staged copy" that _stage_local_if_needed would produce.
    staged = tmp_path / "staged_copy.tif"
    staged.write_bytes(tif_bytes)

    # Patch staging to return our known temp with is_temp=True.
    monkeypatch.setattr(
        preparer, "_stage_local_if_needed", lambda p: (str(staged), True)
    )

    ctx = _OpenerContext()
    # ctx.fr_holder[0] = None → open() uses the staging fallback path.

    ds = ctx.open("any_uri")
    assert staged.exists(), "staged temp should exist before close is called"

    ctx.close(ds)
    assert not staged.exists(), (
        "close must delete the staged temp file; "
        "temp-leak fix regression: os.remove not called in close"
    )


def test_make_opener_lru_eviction_deletes_staged_temp(tmp_path, monkeypatch):
    """LRU eviction (max_count=1) triggers close which deletes the staged temp.

    Opens two entries sequentially; the first is evicted when the second is added.
    Both entries use staged temps; after eviction the first temp must be gone,
    the second must still exist (it is the current/live entry).
    """
    from databricks.labs.gbx.pyrx.core import preparer
    from databricks.labs.gbx.pyrx.grouped_exec import OpenResourceLRU, _OpenerContext

    tif_bytes = _make_tiny_gtiff_bytes()

    # Two separate staged copies — one per source uri.
    staged_a = tmp_path / "staged_a.tif"
    staged_b = tmp_path / "staged_b.tif"
    staged_a.write_bytes(tif_bytes)
    staged_b.write_bytes(tif_bytes)

    staged_map = {"uri_a": str(staged_a), "uri_b": str(staged_b)}

    def fake_stage(p):
        return staged_map[p], True

    monkeypatch.setattr(preparer, "_stage_local_if_needed", fake_stage)

    ctx = _OpenerContext()
    lru = OpenResourceLRU(
        max_bytes=10**12,
        max_count=1,
        opener=ctx.open,
        closer=ctx.close,
        weigher=ctx.weigh,
    )

    lru.get("uri_a")  # opens staged_a; no eviction yet (only 1 entry)
    lru.get(
        "uri_b"
    )  # opens staged_b; evicts uri_a (max_count=1), close deletes staged_a

    assert not staged_a.exists(), "evicted entry's staged temp must be deleted by close"
    assert (
        staged_b.exists()
    ), "current (non-evicted) entry's staged temp must still exist"


# ---------------------------------------------------------------------------
# Connect-aware LRU sizing tests
# ---------------------------------------------------------------------------


def test_connect_aware_lru_sizing_classic_spark(monkeypatch):
    """Classic Spark (non-Connect) returns classic defaults."""
    from databricks.labs.gbx.pyrx.grouped_exec import _connect_aware_lru_sizing

    # Clear any env overrides
    monkeypatch.delenv("GBX_STREAM_MAX_BYTES", raising=False)
    monkeypatch.delenv("GBX_LRU_MAX_BYTES", raising=False)

    # Fake a classic Spark session
    class FakeClassicSpark:
        pass

    fake_spark = FakeClassicSpark()
    fake_spark.__class__.__module__ = "pyspark.sql.session"

    stream_max, lru_max, max_count = _connect_aware_lru_sizing(fake_spark)
    assert stream_max == 256 * 1024**2, f"classic stream_max={stream_max}"
    assert lru_max == 4 * 1024**3, f"classic lru_max={lru_max}"
    assert max_count == 4


def test_connect_aware_lru_sizing_connect_spark(monkeypatch):
    """Spark Connect returns reduced sizing."""
    from databricks.labs.gbx.pyrx.grouped_exec import _connect_aware_lru_sizing

    # Clear any env overrides
    monkeypatch.delenv("GBX_STREAM_MAX_BYTES", raising=False)
    monkeypatch.delenv("GBX_LRU_MAX_BYTES", raising=False)

    # Fake a Spark Connect session
    class FakeConnectSpark:
        pass

    fake_spark = FakeConnectSpark()
    fake_spark.__class__.__module__ = "pyspark.sql.connect.session"

    stream_max, lru_max, max_count = _connect_aware_lru_sizing(fake_spark)
    assert stream_max == 64 * 1024**2, f"connect stream_max={stream_max}"
    assert lru_max == 512 * 1024**2, f"connect lru_max={lru_max}"
    assert max_count == 4


def test_connect_aware_lru_sizing_none_spark(monkeypatch):
    """None spark returns classic defaults (safe fallback)."""
    from databricks.labs.gbx.pyrx.grouped_exec import _connect_aware_lru_sizing

    # Clear any env overrides
    monkeypatch.delenv("GBX_STREAM_MAX_BYTES", raising=False)
    monkeypatch.delenv("GBX_LRU_MAX_BYTES", raising=False)

    stream_max, lru_max, max_count = _connect_aware_lru_sizing(None)
    assert stream_max == 256 * 1024**2
    assert lru_max == 4 * 1024**3
    assert max_count == 4


def test_connect_aware_lru_sizing_env_override_classic(monkeypatch):
    """Env GBX_STREAM_MAX_BYTES override works on classic Spark."""
    from databricks.labs.gbx.pyrx.grouped_exec import _connect_aware_lru_sizing

    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", str(100 * 1024**2))
    monkeypatch.delenv("GBX_LRU_MAX_BYTES", raising=False)

    class FakeClassicSpark:
        pass

    fake_spark = FakeClassicSpark()
    fake_spark.__class__.__module__ = "pyspark.sql.session"

    stream_max, lru_max, max_count = _connect_aware_lru_sizing(fake_spark)
    assert stream_max == 100 * 1024**2, "env override should win"
    assert lru_max == 4 * 1024**3, "classic default when no env"
    assert max_count == 4


def test_connect_aware_lru_sizing_env_override_connect(monkeypatch):
    """Env GBX_LRU_MAX_BYTES override works on Connect."""
    from databricks.labs.gbx.pyrx.grouped_exec import _connect_aware_lru_sizing

    monkeypatch.delenv("GBX_STREAM_MAX_BYTES", raising=False)
    monkeypatch.setenv("GBX_LRU_MAX_BYTES", str(1024 * 1024 * 1024))

    class FakeConnectSpark:
        pass

    fake_spark = FakeConnectSpark()
    fake_spark.__class__.__module__ = "pyspark.sql.connect.session"

    stream_max, lru_max, max_count = _connect_aware_lru_sizing(fake_spark)
    assert stream_max == 64 * 1024**2, "connect default when no env"
    assert lru_max == 1024 * 1024 * 1024, "env override should win"
    assert max_count == 4


def test_connect_aware_lru_sizing_both_env_overrides(monkeypatch):
    """Both env vars override defaults."""
    from databricks.labs.gbx.pyrx.grouped_exec import _connect_aware_lru_sizing

    monkeypatch.setenv("GBX_STREAM_MAX_BYTES", str(50 * 1024**2))
    monkeypatch.setenv("GBX_LRU_MAX_BYTES", str(512 * 1024**2))

    class FakeConnectSpark:
        pass

    fake_spark = FakeConnectSpark()
    fake_spark.__class__.__module__ = "pyspark.sql.connect.session"

    stream_max, lru_max, max_count = _connect_aware_lru_sizing(fake_spark)
    assert stream_max == 50 * 1024**2
    assert lru_max == 512 * 1024**2
    assert max_count == 4


# ---------------------------------------------------------------------------
# _open_via_file_ref stream_max_bytes parameter tests
# ---------------------------------------------------------------------------


def test_open_via_file_ref_respects_stream_max_bytes(monkeypatch):
    """_open_via_file_ref uses passed stream_max_bytes threshold."""
    from databricks.labs.gbx.pyrx.grouped_exec import _open_via_file_ref

    # Fake FileRef with size just under threshold
    class FakeFileRef:
        def __init__(self, size):
            self.size = size
            self.stream_opened = False
            self.fuse_opened = False

        def open(self):
            self.stream_opened = True

            class FakeStream:
                pass

            return FakeStream()

        def as_local_file(self):
            self.fuse_opened = True
            return "/local/path"

    # Fake rasterio with open and profile tracking
    class FakeRasterio:
        def __init__(self):
            self.opened_with_stream = False

        def open(self, target):
            if isinstance(target, str):
                # FUSE path
                self.opened_with_stream = False
            else:
                # Stream path
                self.opened_with_stream = True

            class FakeDataset:
                profile = {"tiled": True}

                def close(self):
                    pass

            return FakeDataset()

    # Test 1: File UNDER threshold with tiled=True → uses stream
    fr = FakeFileRef(50 * 1024**2)
    rstio = FakeRasterio()
    ds, stream = _open_via_file_ref(fr, rstio, stream_max_bytes=100 * 1024**2)
    assert fr.stream_opened, "should open stream for file < threshold"
    assert stream is not None, "stream path should return stream"
    assert rstio.opened_with_stream, "rasterio should have been passed the stream"

    # Test 2: File OVER threshold → uses FUSE
    fr2 = FakeFileRef(150 * 1024**2)
    rstio2 = FakeRasterio()
    ds2, stream2 = _open_via_file_ref(fr2, rstio2, stream_max_bytes=100 * 1024**2)
    assert fr2.fuse_opened, "should use FUSE for file > threshold"
    assert stream2 is None, "FUSE path should return None stream"
