"""Row-pool generation on Databricks: the pure parts, and the Spark fan-out on a local session.

The tiles written there must be the bytes ``bench.datagen.generate_corpus`` writes for the same
index and seed, so a corpus generated on Databricks equals the one generated locally.
"""

import hashlib
import json
from pathlib import Path

import pytest

from databricks.labs.gbx.bench import datagen as dg
from databricks.labs.gbx.bench import datagen_remote as dr

SRIDS = [4326, 3857, 32618, 27700]


def _spec(**kw):
    base = dict(seed=77, srids=tuple(SRIDS), tile_px=32, bands=2, dtype="float32")
    base.update(kw)
    return dr.RowSpec(**base)


@pytest.fixture(scope="module")
def local_corpus(tmp_path_factory):
    """The local generator's row pool for the small spec: 10 tiles."""
    out = tmp_path_factory.mktemp("local_corpus")
    dg.generate_corpus(
        out_dir=out,
        seed=77,
        tile_px=[32],
        bands=[1],
        dtypes=["float32"],
        srids=SRIDS,
        nodata_fracs=[0.0],
        row_rows=10,
        row_tile_px=32,
        row_bands=2,
        row_dtype="float32",
        jobs=1,
    )
    return out


def _sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


# --- index ranges -----------------------------------------------------------------


def test_index_range_is_inclusive_at_both_ends():
    r = dr.index_range(1000, 1099)
    assert (r[0], r[-1], len(r)) == (1000, 1099, 100)
    assert len(dr.index_range(5, 5)) == 1


@pytest.mark.parametrize("a,b", [(10, 9), (-1, 5), (3, -2)])
def test_index_range_refuses_an_empty_or_negative_range(a, b):
    with pytest.raises(ValueError):
        dr.index_range(a, b)


def test_chunks_cover_the_range_once_in_order():
    got = list(dr.chunks(range(10), 4))
    assert [list(c) for c in got] == [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]


# --- determinism: the same bytes as the local generator ----------------------------


def test_row_tile_bytes_equal_the_local_generators_file(local_corpus):
    spec = _spec()
    for j in (0, 1, 2, 3, 7, 9):
        assert (
            dr.row_tile_bytes(j, spec)
            == (local_corpus / "rows" / f"r{j}.tif").read_bytes()
        ), j


def test_expected_sizes_are_each_srids_file_size(local_corpus):
    spec = _spec()
    sizes = dr.expected_sizes(spec)
    assert set(sizes) == set(SRIDS)
    for j in range(10):
        srid, _ = dg.row_tile_srid_seed(j, spec.seed, spec.srids)
        assert (local_corpus / "rows" / f"r{j}.tif").stat().st_size == sizes[srid]


def test_spec_round_trips_through_json():
    spec = _spec()
    assert dr.RowSpec.from_dict(json.loads(json.dumps(spec.as_dict()))) == spec


# --- writing one tile: idempotent, never deletes -----------------------------------


def test_write_row_tile_writes_then_skips_then_repairs_a_short_file(
    tmp_path, local_corpus
):
    spec = _spec()
    sizes = dr.expected_sizes(spec)
    (tmp_path / "rows").mkdir()
    keep = tmp_path / "rows" / "other.txt"
    keep.write_text("not ours")

    assert dr.write_row_tile(3, tmp_path, spec, sizes)[0] == "written"
    dest = tmp_path / "rows" / "r3.tif"
    assert dest.read_bytes() == (local_corpus / "rows" / "r3.tif").read_bytes()

    mtime = dest.stat().st_mtime_ns
    assert dr.write_row_tile(3, tmp_path, spec, sizes)[0] == "skipped"
    assert dest.stat().st_mtime_ns == mtime  # untouched

    # a short (interrupted) file is not "existing with the expected size": it is rewritten
    dest.write_bytes(b"short")
    assert dr.write_row_tile(3, tmp_path, spec, sizes)[0] == "written"
    assert dest.read_bytes() == (local_corpus / "rows" / "r3.tif").read_bytes()

    # skip_existing=False rewrites a complete file
    assert dr.write_row_tile(3, tmp_path, spec, sizes, skip_existing=False)[0] == (
        "written"
    )
    assert keep.read_text() == "not ours"  # nothing else was touched


def test_write_row_tile_creates_the_rows_dir(tmp_path):
    spec = _spec()
    dr.write_row_tile(0, tmp_path / "corpus", spec, dr.expected_sizes(spec))
    assert (tmp_path / "corpus" / "rows" / "r0.tif").is_file()


def test_generate_partition_yields_one_result_per_index(tmp_path):
    spec = _spec()
    res = list(
        dr.generate_partition([4, 5], tmp_path, spec.as_dict(), dr.expected_sizes(spec))
    )
    assert [(r[0], r[1]) for r in res] == [(4, "written"), (5, "written")]
    assert all(r[2] > 0 for r in res)


# --- the Spark fan-out (local session) ---------------------------------------------


def test_run_generation_matches_local_files_reports_progress_and_is_idempotent(
    spark, tmp_path, local_corpus
):
    spec = _spec()
    lines = []
    out = tmp_path / "vol"
    summary = dr.run_generation(
        spark,
        out,
        dr.index_range(2, 9),
        spec,
        partitions=3,
        chunk=3,
        say=lines.append,
        heartbeat_s=3600,
    )
    assert summary["total"] == 8 and summary["done"] == 8
    assert summary["written"] == 8 and summary["skipped"] == 0
    for j in range(2, 10):
        assert _sha(out / "rows" / f"r{j}.tif") == _sha(
            local_corpus / "rows" / f"r{j}.tif"
        )
    assert not (out / "rows" / "r0.tif").exists()  # outside the range

    # tagged progress: every chunk (3 chunks of 3, 3, 2), with done out of total
    prog = [x for x in lines if x.startswith("[GENCORPUS] progress")]
    assert len(prog) == 3
    assert "done 8/8" in prog[-1] and "done 3/8" in prog[0]

    # a second run writes nothing
    again = dr.run_generation(
        spark,
        out,
        dr.index_range(2, 9),
        spec,
        partitions=2,
        chunk=100,
        say=lambda _x: None,
        heartbeat_s=3600,
    )
    assert again["written"] == 0 and again["skipped"] == 8


def test_run_generation_heartbeat_prints_while_a_chunk_runs(spark, tmp_path):
    spec = _spec(tile_px=256)  # long enough for a 0.05 s heartbeat to fire
    lines = []
    dr.run_generation(
        spark,
        tmp_path,
        dr.index_range(0, 7),
        spec,
        partitions=2,
        chunk=8,
        say=lines.append,
        heartbeat_s=0.05,
    )
    assert any(x.startswith("[GENCORPUS] running") for x in lines)


# --- verification ------------------------------------------------------------------


def test_inventory_counts_complete_files_and_flags_short_ones(tmp_path):
    spec = _spec()
    sizes = dr.expected_sizes(spec)
    for j in (0, 1, 2):
        dr.write_row_tile(j, tmp_path, spec, sizes)
    (tmp_path / "rows" / "r2.tif").write_bytes(b"short")
    inv = dr.inventory(tmp_path, dr.index_range(0, 3), spec, sizes)
    assert inv["expected"] == 4
    assert inv["complete"] == 2
    assert inv["short"] == [2]
    assert inv["missing"] == [3]
    assert inv["bytes"] == sizes[SRIDS[0]] + sizes[SRIDS[1]]


def test_sha_sample_hashes_the_named_indices(tmp_path, local_corpus):
    got = dr.sha_sample(local_corpus, [0, 9])
    assert got["0"]["sha256"] == _sha(local_corpus / "rows" / "r0.tif")
    assert got["9"]["size"] == (local_corpus / "rows" / "r9.tif").stat().st_size


def test_sha_sample_names_a_missing_file(tmp_path):
    assert dr.sha_sample(tmp_path, [5])["5"] == {"sha256": None, "size": None}


# --- the smoke: worker bytes against the host's ------------------------------------


def test_smoke_compares_the_workers_bytes_to_the_expected_sha(spark, local_corpus):
    spec = _spec()
    want = {str(j): _sha(local_corpus / "rows" / f"r{j}.tif") for j in (0, 1, 2, 3)}
    ok = dr.smoke(spark, spec, [0, 1, 2, 3], want, say=lambda _x: None)
    assert ok["ok"] and ok["mismatch"] == [] and ok["import_check"]["ok"]

    bad = dict(want, **{"2": "0" * 64})
    res = dr.smoke(spark, spec, [0, 1, 2, 3], bad, say=lambda _x: None)
    assert not res["ok"] and res["mismatch"] == [2]


# --- the notebook ------------------------------------------------------------------


def _cfg(**kw):
    base = dict(
        mode="run",
        out_dir="/Volumes/c/s/v/bench-corpus",
        spec=_spec().as_dict(),
        rows_from=0,
        rows_to=9,
        partitions=8,
        chunk=64,
        skip_existing=True,
        verify_indices=[0, 9],
        run_id="gen-test",
    )
    base.update(kw)
    return base


@pytest.mark.parametrize("mode", ["run", "smoke"])
def test_notebook_cells_compile_and_exit_in_their_own_last_cell(mode):
    nb = dr.build_gen_notebook(
        _cfg(mode=mode, smoke_indices=[0, 1], smoke_sha={"0": "a", "1": "b"})
    )
    cells = ["".join(c["source"]) for c in nb["cells"]]
    for src in cells:
        compile(src, "<cell>", "exec")
    assert "dbutils.notebook.exit" in cells[-1]
    assert sum("dbutils.notebook.exit" in c for c in cells) == 1
    assert "gen-test" in "".join(cells)


def test_nothing_in_the_remote_path_uses_the_forbidden_serverless_apis():
    src = Path(dr.__file__).read_text()
    for banned in ("sparkContext", ".rdd", "_jvm", "_jsc", "spark.conf"):
        assert banned not in src.replace("The Serverless forbidden", ""), banned


# --- the launcher's pure parts -------------------------------------------------------


def test_default_verify_indices_cover_the_ends_the_middle_and_the_canonical_sample():
    assert dr.default_verify_indices(1000, 1099) == [1000, 1049, 1099]
    full = dr.default_verify_indices(0, 9999)
    for j in (0, 999, 1000, 5000, 9999):
        assert j in full
    assert dr.default_verify_indices(7, 7) == [7]


def test_last_progress_reads_the_latest_tagged_line_from_exported_text():
    html = (
        "junk [GENCORPUS] start 8 tiles x\\n"
        '"[GENCORPUS] progress: done 3/8 (written 3, skipped 0) elapsed 4s\\n"'
        "<b>[GENCORPUS] running: done 5/8 (chunk 2/3 in flight, elapsed 9s)</b>"
    )
    line = dr.last_progress(html)
    assert line.startswith("[GENCORPUS] running: done 5/8")
    assert dr.progress_done(line) == (5, 8)
    assert dr.last_progress("nothing here") is None and dr.progress_done(None) is None


def test_compare_payload_flags_a_short_count_and_every_sha_difference():
    ok = dict(
        inventory=dict(complete=3, short=[], missing=[]),
        sha={"0": {"sha256": "a"}, "2": {"sha256": "b"}},
    )
    assert dr.compare_payload(ok, {"0": "a", "2": "b"}, 3) == []
    bad = dict(
        inventory=dict(complete=2, short=[1], missing=[]),
        sha={"0": {"sha256": "a"}, "2": {"sha256": "x"}},
    )
    problems = dr.compare_payload(bad, {"0": "a", "2": "b"}, 3)
    assert len(problems) == 2
    assert "2 of 3" in problems[0] and "r2.tif" in problems[1]


def test_exported_text_decodes_the_notebook_model_in_the_export_html():
    import base64
    import urllib.parse

    model = {
        "commands": [
            {
                "results": {
                    "data": [
                        {"data": "[GENCORPUS] progress: done 3/8 (written 3)\n"},
                        {"data": "[GENCORPUS] running: done 5/8 (chunk 2/3)\n"},
                    ]
                }
            },
            {"results": {"data": "raw string result"}},
        ]
    }
    blob = base64.b64encode(urllib.parse.quote(json.dumps(model)).encode()).decode()
    html = f"<script>window.x = 1; __DATABRICKS_NOTEBOOK_MODEL = '{blob}';</script>"
    text = dr.exported_text(html)
    assert dr.progress_done(dr.last_progress(text)) == (5, 8)
    # a page with no model is searched as it is
    assert dr.exported_text("plain [GENCORPUS] start 1") == "plain [GENCORPUS] start 1"
