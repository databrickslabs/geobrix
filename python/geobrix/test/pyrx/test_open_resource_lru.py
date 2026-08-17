"""Tests for OpenResourceLRU — per-partition byte-budgeted LRU cache."""
from databricks.labs.gbx.pyrx.grouped_exec import OpenResourceLRU


def test_byte_budget_evicts_oldest_over_budget():
    closed = []
    lru = OpenResourceLRU(max_bytes=100, max_count=1000,
                          opener=lambda k: k, closer=lambda r: closed.append(r),
                          weigher=lambda r, k: 40)
    lru.get("a"); lru.get("b")     # 80 bytes; both fit
    assert lru.bytes == 80 and lru.evictions == 0
    lru.get("c")                    # 120 > 100 -> evict oldest "a" -> back to 80
    assert closed == ["a"] and lru.bytes == 80 and lru.evictions == 1


def test_many_small_files_stay_warm_under_budget():
    lru = OpenResourceLRU(max_bytes=4 * 1024**3, max_count=1000,
                          opener=lambda k: k, closer=lambda r: None,
                          weigher=lambda r, k: 32 * 1024**2)  # 32 MiB each
    for i in range(100):
        lru.get(f"f{i}")           # 100 * 32 MiB = 3.125 GiB < 4 GiB
    assert lru.evictions == 0 and lru.opens == 100


def test_count_guard_bounds_handles_when_weight_nominal():
    lru = OpenResourceLRU(max_bytes=10**12, max_count=2,
                          opener=lambda k: k, closer=lambda r: None,
                          weigher=lambda r, k: 0)  # streams ~ nominal
    lru.get("a"); lru.get("b"); lru.get("c")
    assert lru.evictions == 1      # count guard fired at 3 > 2


def test_never_evicts_the_current_entry():
    closed = []
    lru = OpenResourceLRU(max_bytes=10, max_count=1000,
                          opener=lambda k: k, closer=lambda r: closed.append(r),
                          weigher=lambda r, k: 999)  # single entry exceeds budget
    got = lru.get("big")
    assert got == "big" and closed == []  # current entry kept despite over-budget


def test_close_all_closes_remaining():
    closed = []
    lru = OpenResourceLRU(max_bytes=100, opener=lambda k: k,
                          closer=lambda r: closed.append(r), weigher=lambda r, k: 10)
    lru.get("x"); lru.get("y")
    lru.close_all()
    assert sorted(closed) == ["x", "y"] and lru.bytes == 0
