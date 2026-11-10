"""Tests for the authoritative per-function benchmark store + change resolution.

A temp dir is passed as ``root=`` so nothing touches the real ``test-logs/``.
Hash / staleness tests create fake source files under the temp root and use a
tiny ``FnSpec``-like stub (``.name`` + ``.sources``) so they need no real registry.
"""

from types import SimpleNamespace

from databricks.labs.gbx.bench import store


def _spec(name, sources):
    return SimpleNamespace(name=name, sources=tuple(sources))


def _write_source(root, rel, content):
    fp = root / rel
    fp.parent.mkdir(parents=True, exist_ok=True)
    fp.write_text(content)
    return rel


def test_write_read_roundtrip(tmp_path):
    cells = [{"mode": "pure-core", "verdict": "match"}]
    heavy = [{"ms": 12.3}]
    light = [{"ms": 4.5}]
    store.write_record(
        "rst_slope",
        sources=("a.py",),
        cells=cells,
        heavy_rows=heavy,
        light_rows=light,
        commit="abc123",
        validated_at="2026-06-07T00:00:00Z",
        corpus="essential",
        which="full",
        root=tmp_path,
    )
    rec = store.read_record("rst_slope", root=tmp_path)
    assert rec["fn"] == "rst_slope"
    assert rec["validated_commit"] == "abc123"
    assert rec["validated_at"] == "2026-06-07T00:00:00Z"
    assert rec["corpus"] == "essential"
    assert rec["set"] == "full"
    assert rec["cells"] == cells
    assert rec["heavy_rows"] == heavy
    assert rec["light_rows"] == light
    assert "sources_hash" in rec


def test_read_record_missing_returns_none(tmp_path):
    assert store.read_record("nope", root=tmp_path) is None


def test_write_record_overwrites_latest_only(tmp_path):
    common = dict(
        sources=("a.py",),
        cells=[],
        heavy_rows=[],
        light_rows=[],
        validated_at="t",
        corpus="essential",
        which="full",
        root=tmp_path,
    )
    store.write_record("rst_avg", commit="first", **common)
    store.write_record("rst_avg", commit="second", **common)
    rec = store.read_record("rst_avg", root=tmp_path)
    assert rec["validated_commit"] == "second"
    # latest-only: a single file per function
    assert store.store_function_names(root=tmp_path) == {"rst_avg"}


def test_sources_hash_stable_and_changes(tmp_path):
    _write_source(tmp_path, "core/mod.py", "x = 1\n")
    h1 = store.sources_hash(("core/mod.py",), root=tmp_path)
    h2 = store.sources_hash(("core/mod.py",), root=tmp_path)
    assert h1 == h2  # stable for unchanged content
    _write_source(tmp_path, "core/mod.py", "x = 2\n")
    h3 = store.sources_hash(("core/mod.py",), root=tmp_path)
    assert h3 != h1  # changes when content changes


def test_sources_hash_order_independent(tmp_path):
    _write_source(tmp_path, "a.py", "a\n")
    _write_source(tmp_path, "b.py", "b\n")
    assert store.sources_hash(("a.py", "b.py"), root=tmp_path) == store.sources_hash(
        ("b.py", "a.py"), root=tmp_path
    )


def test_sources_hash_missing_file(tmp_path):
    # A missing source hashes deterministically (the <MISSING> sentinel) rather
    # than raising, so a record can still be written/compared.
    h = store.sources_hash(("absent.py",), root=tmp_path)
    assert isinstance(h, str) and len(h) == 64


def test_is_stale(tmp_path):
    rel = _write_source(tmp_path, "core/t.py", "orig\n")
    spec = _spec("rst_slope", (rel,))
    store.write_record(
        "rst_slope",
        sources=spec.sources,
        cells=[],
        heavy_rows=[],
        light_rows=[],
        commit="c",
        validated_at="t",
        corpus="essential",
        which="full",
        root=tmp_path,
    )
    rec = store.read_record("rst_slope", root=tmp_path)
    assert store.is_stale(spec, rec, root=tmp_path) is False  # matching hash
    _write_source(tmp_path, "core/t.py", "MUTATED\n")
    assert store.is_stale(spec, rec, root=tmp_path) is True  # source changed
    assert store.is_stale(spec, None, root=tmp_path) is True  # missing record


def test_affected_functions_single_and_shared():
    nodata = "python/.../core/_nodata.py"
    specs = [
        _spec("rst_slope", ("python/.../core/terrain.py", nodata)),
        _spec("rst_ndvi", ("python/.../core/indices.py", nodata)),
        _spec("rst_transform", ("python/.../core/warp.py",)),
    ]
    # a path in exactly one fn's sources selects only it
    assert store.affected_functions(["python/.../core/warp.py"], specs) == [
        "rst_transform"
    ]
    # a SHARED path (in multiple fns' sources) selects ALL of them, sorted
    assert store.affected_functions([nodata], specs) == ["rst_ndvi", "rst_slope"]
    # nothing matches -> empty
    assert store.affected_functions(["unrelated.py"], specs) == []


def test_unmapped_changed():
    specs = [
        _spec("rst_slope", ("terrain.py", "_nodata.py")),
        _spec("rst_transform", ("warp.py",)),
    ]
    out = store.unmapped_changed(["warp.py", "docs/readme.md", "terrain.py"], specs)
    assert out == ["docs/readme.md"]  # only the uncovered path
    # all covered -> empty
    assert store.unmapped_changed(["warp.py", "terrain.py"], specs) == []


def test_read_all_and_store_function_names(tmp_path):
    assert store.read_all(root=tmp_path) == []  # missing dir -> empty
    assert store.store_function_names(root=tmp_path) == set()
    common = dict(
        sources=("a.py",),
        cells=[],
        heavy_rows=[],
        light_rows=[],
        commit="c",
        validated_at="t",
        corpus="essential",
        which="full",
        root=tmp_path,
    )
    store.write_record("rst_avg", **common)
    store.write_record("rst_slope", **common)
    allrecs = store.read_all(root=tmp_path)
    assert {r["fn"] for r in allrecs} == {"rst_avg", "rst_slope"}
    # sorted by file name
    assert [r["fn"] for r in allrecs] == ["rst_avg", "rst_slope"]
    assert store.store_function_names(root=tmp_path) == {"rst_avg", "rst_slope"}


def test_orphan_records(tmp_path, monkeypatch):
    # A store with one real fn (in select(set="full")) and one bogus fn (removed
    # from the registry). orphan_records returns only the bogus name.
    common = dict(
        sources=("a.py",),
        cells=[],
        heavy_rows=[],
        light_rows=[],
        commit="c",
        validated_at="t",
        corpus="essential",
        which="full",
        root=tmp_path,
    )
    store.write_record("rst_slope", **common)
    store.write_record("rst_notreal", **common)

    # Stub the registry so the test doesn't depend on the live full set: only
    # rst_slope is "registered".
    from databricks.labs.gbx.bench import spec as _spec

    monkeypatch.setattr(
        _spec,
        "select",
        lambda **kw: [SimpleNamespace(name="rst_slope", sources=("a.py",))],
    )

    orphans = store.orphan_records(root=tmp_path)
    assert orphans == ["rst_notreal"]


def test_orphan_records_empty_store(tmp_path):
    assert store.orphan_records(root=tmp_path) == []


def test_affected_functions_against_real_registry():
    # Smoke check the helpers tie into the real FnSpec.sources contract: editing
    # the shared _nodata.py affects more than one function, and editing the GDAL spec
    # engine affects the functions it runs.
    from databricks.labs.gbx.bench import spec as s

    specs = s.select(set="full")
    nodata_path = "python/geobrix/src/databricks/labs/gbx/pyrx/core/_nodata.py"
    affected = store.affected_functions([nodata_path], specs)
    assert "rst_avg" in affected and "rst_convolve" in affected
    assert len(affected) > 2
    engine_path = "python/geobrix/src/databricks/labs/gbx/engine/run.py"
    affected = store.affected_functions([engine_path], specs)
    assert "rst_slope" in affected and "rst_ndvi" in affected
