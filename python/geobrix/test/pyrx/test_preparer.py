import os

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core import cog as gbxcog
from databricks.labs.gbx.pyrx.core.preparer import (
    _resolve_sources,
    _subdataset_uri,
    cog_output_name,
    prepare_cog,
    prepare_cog_measured,
    prepare_cogs,
)


def _write_src(path, w=512, h=512):
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 60, 0.01, 0.01),
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(np.arange(w * h, dtype="uint8").reshape(1, h, w))


def test_cog_output_name_appends_cog():
    assert cog_output_name("myfile1.tiff") == "myfile1.tiff.cog"
    assert cog_output_name("scene.tif") == "scene.tif.cog"
    assert cog_output_name("no_ext") == "no_ext.cog"


def test_subdataset_uri_bare_path_when_none():
    assert _subdataset_uri("/data/x.tif", None) == "/data/x.tif"


def test_subdataset_uri_builds_netcdf_uri():
    assert _subdataset_uri("/data/x.nc", "temp") == 'NETCDF:"/data/x.nc":temp'


def test_prepare_cog_produces_valid_cog_named_dot_cog(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out_path, status = prepare_cog(str(src), str(out), blocksize=256)
    assert status == "ok"
    assert out_path == str(out / "scene.tiff.cog")
    assert os.path.exists(out_path)
    with open(out_path, "rb") as fh:
        info = gbxcog.sniff_header(fh.read())
    assert info.is_cog is True and info.overview_levels >= 1


def test_prepare_cog_skips_when_exists_default(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out.mkdir()
    # Pre-create the target so skip_if_exists (default True) short-circuits.
    target = out / "scene.tiff.cog"
    target.write_bytes(b"sentinel-not-a-real-cog")
    out_path, status = prepare_cog(str(src), str(out), blocksize=256)
    assert status == "skipped"
    assert out_path == str(target)
    # Untouched — still the sentinel bytes (no reconvert).
    assert target.read_bytes() == b"sentinel-not-a-real-cog"


def test_prepare_cog_force_rebuild_when_skip_false(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out.mkdir()
    target = out / "scene.tiff.cog"
    target.write_bytes(b"sentinel")
    out_path, status = prepare_cog(
        str(src), str(out), blocksize=256, skip_if_exists=False
    )
    assert status == "ok"
    with open(out_path, "rb") as fh:
        info = gbxcog.sniff_header(fh.read())
    assert info.is_cog is True  # real COG now, sentinel overwritten


def test_prepare_cog_error_isolation_returns_status(tmp_path):
    out = tmp_path / "out"
    # Nonexistent source → convert fails; must return ('error:...') not raise.
    out_path, status = prepare_cog(str(tmp_path / "missing.tif"), str(out))
    assert out_path is None
    assert status.startswith("error:")


def test_prepare_cog_measured_ok_has_rss(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    r = prepare_cog_measured(str(src), str(out), blocksize=256)
    assert r["status"] == "ok"
    assert r["output_path"] == str(out / "scene.tiff.cog")
    assert isinstance(r["peak_rss_mib"], float) and r["peak_rss_mib"] > 0


def test_prepare_cog_measured_error_passthrough(tmp_path):
    out = tmp_path / "out"
    r = prepare_cog_measured(str(tmp_path / "missing.tif"), str(out))
    assert r["output_path"] is None
    assert r["status"].startswith("error:")
    # RSS is still reported (measured around the attempt).
    assert isinstance(r["peak_rss_mib"], float)


def test_prepare_cog_measured_skipped_status(tmp_path):
    src = tmp_path / "in" / "scene.tiff"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out.mkdir()
    (out / "scene.tiff.cog").write_bytes(b"sentinel")
    r = prepare_cog_measured(str(src), str(out))
    assert r["status"] == "skipped"
    assert r["output_path"] == str(out / "scene.tiff.cog")


def test_prepare_cog_out_name_overrides_basename(tmp_path):
    src = tmp_path / "in" / "staged_tmp12345.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    # out_name mimics prepare_cogs passing the ORIGINAL basename while the input
    # path is a staged temp.
    out_path, status = prepare_cog(
        str(src), str(out), blocksize=256, out_name="scene_original.tif"
    )
    assert status == "ok"
    assert out_path == str(out / "scene_original.tif.cog")
    assert os.path.exists(out_path)


def test_prepare_cog_out_name_none_uses_basename(tmp_path):
    src = tmp_path / "in" / "plain.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out_path, status = prepare_cog(str(src), str(out), blocksize=256)
    assert status == "ok"
    assert out_path == str(out / "plain.tif.cog")


def _touch_tif(p):
    p.parent.mkdir(parents=True, exist_ok=True)
    _write_src(str(p))


def test_resolve_single_file(tmp_path):
    f = tmp_path / "a.tif"
    _touch_tif(f)
    assert _resolve_sources(str(f)) == [(str(f), None)]


def test_resolve_dir_lists_rasters_recursive(tmp_path):
    _touch_tif(tmp_path / "a.tif")
    _touch_tif(tmp_path / "sub" / "b.tif")
    (tmp_path / "note.json").write_text("{}")  # non-raster excluded
    got = sorted(p for p, e in _resolve_sources(str(tmp_path)))
    assert got == sorted([str(tmp_path / "a.tif"), str(tmp_path / "sub" / "b.tif")])


def test_resolve_dir_non_recursive(tmp_path):
    _touch_tif(tmp_path / "a.tif")
    _touch_tif(tmp_path / "sub" / "b.tif")
    got = sorted(p for p, e in _resolve_sources(str(tmp_path), recursive=False))
    assert got == [str(tmp_path / "a.tif")]  # sub/ not descended


def test_resolve_list_mixes_files_and_dirs_dedup(tmp_path):
    _touch_tif(tmp_path / "d" / "a.tif")
    f = tmp_path / "d" / "a.tif"  # same file, also named explicitly
    _touch_tif(tmp_path / "standalone.tif")
    resolved = _resolve_sources(
        [str(tmp_path / "d"), str(f), str(tmp_path / "standalone.tif")]
    )
    paths = [p for p, e in resolved]
    # a.tif appears once (dir + explicit), plus standalone.tif
    assert paths.count(str(f)) == 1
    assert str(tmp_path / "standalone.tif") in paths


def test_resolve_missing_path_is_not_found(tmp_path):
    resolved = _resolve_sources(str(tmp_path / "nope.tif"))
    assert resolved == [(str(tmp_path / "nope.tif"), "not-found")]


def test_resolve_explicit_file_bypasses_extension_filter(tmp_path):
    weird = tmp_path / "data.bin"  # not in DEFAULT_RASTER_EXTS
    _touch_tif(weird)  # but it IS a real GeoTIFF on disk
    # named explicitly → included despite extension
    assert _resolve_sources(str(weird)) == [(str(weird), None)]


def test_prepare_cogs_dir_summary_and_valid_cogs(tmp_path):
    d = tmp_path / "corpus"
    _touch_tif(d / "a.tif")
    _touch_tif(d / "b.tif")
    out = tmp_path / "out"
    summary = prepare_cogs(str(d), str(out), blocksize=256, verbose=False)
    assert summary["total"] == 2
    assert summary["ok"] == 2 and summary["skipped"] == 0 and summary["error"] == 0
    assert summary["out_dir"] == str(out)
    assert isinstance(summary["peak_rss_mib"], float)
    assert isinstance(summary["elapsed_s"], float)
    names = sorted(os.path.basename(r["output_path"]) for r in summary["results"])
    assert names == ["a.tif.cog", "b.tif.cog"]
    for r in summary["results"]:
        assert r["status"] == "ok"
        with open(r["output_path"], "rb") as fh:
            assert gbxcog.sniff_header(fh.read()).is_cog is True


def test_prepare_cogs_list_mixed_with_error_and_skip(tmp_path):
    d = tmp_path / "corpus"
    _touch_tif(d / "a.tif")
    out = tmp_path / "out"
    out.mkdir()
    # Pre-create b's output so it is skipped.
    _touch_tif(tmp_path / "b.tif")
    (out / "b.tif.cog").write_bytes(b"sentinel")
    missing = str(tmp_path / "ghost.tif")
    summary = prepare_cogs(
        [str(d), str(tmp_path / "b.tif"), missing],
        str(out),
        blocksize=256,
        verbose=False,
    )
    assert summary["ok"] == 1  # a.tif
    assert summary["skipped"] == 1  # b.tif (pre-existing output)
    assert summary["error"] == 1  # ghost.tif not-found
    assert summary["total"] == 3
    # not-found surfaced as an error record with null output_path
    nf = [r for r in summary["results"] if r["status"] == "error:not-found"]
    assert len(nf) == 1 and nf[0]["output_path"] is None


def test_prepare_cogs_verbose_prints_progress(tmp_path, capsys):
    d = tmp_path / "corpus"
    _touch_tif(d / "a.tif")
    out = tmp_path / "out"
    prepare_cogs(str(d), str(out), blocksize=256, verbose=True)
    captured = capsys.readouterr().out
    assert "[1/1]" in captured
    assert "done:" in captured


def test_prepare_cogs_verbose_false_silent(tmp_path, capsys):
    d = tmp_path / "corpus"
    _touch_tif(d / "a.tif")
    out = tmp_path / "out"
    prepare_cogs(str(d), str(out), blocksize=256, verbose=False)
    assert capsys.readouterr().out == ""


def test_prepare_cogs_staging_error_isolation(tmp_path, monkeypatch):
    """Staging failure on one source doesn't abort batch; others still succeed."""
    from databricks.labs.gbx.pyrx.core import preparer as preparer_module

    d = tmp_path / "corpus"
    _touch_tif(d / "good.tif")
    bad = d / "bad.tif"
    _touch_tif(bad)
    out = tmp_path / "out"

    # Monkeypatch _stage_local_if_needed to fail for "bad.tif" only.
    original_stage = preparer_module._stage_local_if_needed

    def mock_stage(path):
        if "bad.tif" in path:
            raise IOError("Simulated FUSE read error")
        return original_stage(path)

    monkeypatch.setattr(preparer_module, "_stage_local_if_needed", mock_stage)

    summary = prepare_cogs(str(d), str(out), blocksize=256, verbose=False)

    # Batch must not abort; good.tif converts, bad.tif errors, counts correct.
    assert summary["total"] == 2
    assert summary["ok"] == 1, f"Expected 1 ok, got {summary['ok']}"
    assert summary["error"] == 1, f"Expected 1 error, got {summary['error']}"
    assert summary["skipped"] == 0

    # good.tif succeeded.
    good_results = [r for r in summary["results"] if "good.tif" in r["source"]]
    assert len(good_results) == 1
    assert good_results[0]["status"] == "ok"
    assert good_results[0]["output_path"] is not None

    # bad.tif got error:stage, output_path None.
    bad_results = [r for r in summary["results"] if "bad.tif" in r["source"]]
    assert len(bad_results) == 1
    assert bad_results[0]["status"].startswith("error:stage")
    assert bad_results[0]["output_path"] is None


def test_prepare_cog_bigtiff_default_is_yes_valid_cog(tmp_path):
    """Default bigtiff='YES' produces a valid COG (BigTIFF-structured)."""
    src = tmp_path / "in" / "b.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out_path, status = prepare_cog(str(src), str(out), blocksize=256)  # default bigtiff
    assert status == "ok"
    with open(out_path, "rb") as fh:
        raw = fh.read()
    # magic 43 = BigTIFF
    assert raw[2] == 43 or raw[3] == 43
    assert gbxcog.sniff_header(raw).is_cog is True


def test_prepare_cog_bigtiff_no_is_classic(tmp_path):
    """bigtiff='NO' forces Classic TIFF (magic 42) for a small output."""
    src = tmp_path / "in" / "c.tif"
    src.parent.mkdir()
    _write_src(str(src))
    out = tmp_path / "out"
    out_path, status = prepare_cog(str(src), str(out), blocksize=256, bigtiff="NO")
    assert status == "ok"
    with open(out_path, "rb") as fh:
        raw = fh.read()
    assert raw[2] == 42 or raw[3] == 42  # Classic TIFF
    assert gbxcog.sniff_header(raw).is_cog is True


def test_cog_convert_file_rejects_bad_bigtiff(tmp_path):
    """cog_convert_file validates the bigtiff value."""
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file

    src = tmp_path / "s.tif"
    _write_src(str(src))
    try:
        cog_convert_file(str(src), str(tmp_path / "o.cog"), bigtiff="MAYBE")
        assert False, "expected ValueError for bad bigtiff"
    except ValueError:
        pass


# ---------------------------------------------------------------------------
# _stage_local_if_needed — probe-then-stage tests
# ---------------------------------------------------------------------------


def test_stage_local_passthrough_plain_local(tmp_path):
    """A plain local path (no FUSE prefix) passes through unchanged with is_temp=False."""
    from databricks.labs.gbx.pyrx.core.preparer import _stage_local_if_needed

    f = tmp_path / "local.tif"
    _write_src(str(f))
    path, is_temp = _stage_local_if_needed(str(f))
    assert path == str(f)
    assert is_temp is False


def test_stage_local_volumes_probe_success_no_copy(tmp_path, monkeypatch):
    """/Volumes-classified path where rasterio probe succeeds → (path, False), no copy."""
    from databricks.labs.gbx.pyrx.core import preparer as m

    f = tmp_path / "src.tif"
    _write_src(str(f))  # real GeoTIFF; probe will open it via rasterio for real

    # Treat the real tmp_path file as if it were a /Volumes path.
    monkeypatch.setattr(m, "_is_fuse_path", lambda p: True)

    path, is_temp = m._stage_local_if_needed(str(f))
    assert path == str(f), "expected direct-read passthrough"
    assert is_temp is False


def _probe_raiser(p):
    raise OSError("simulated FUSE probe failure")


def test_stage_local_volumes_probe_fails_falls_back_to_copy(tmp_path, monkeypatch):
    """/Volumes path where probe fails → falls back to sequential copy → (temp, True)."""
    from databricks.labs.gbx.pyrx.core import preparer as m

    f = tmp_path / "src.tif"
    _write_src(str(f))  # real, readable file for the copy fallback

    monkeypatch.setattr(m, "_is_fuse_path", lambda p: True)
    # Make the probe fail immediately (no retry sleep).
    monkeypatch.setattr(m, "_probe_direct_open", _probe_raiser)

    path, is_temp = m._stage_local_if_needed(str(f))
    assert is_temp is True, "expected temp copy after probe failure"
    assert path != str(f)
    assert os.path.exists(path)
    os.remove(path)


def test_stage_local_force_stage_env_always_copies(tmp_path, monkeypatch):
    """GBX_FORCE_STAGE=1 bypasses probe and always copies even when direct access works."""
    from databricks.labs.gbx.pyrx.core import preparer as m

    f = tmp_path / "src.tif"
    _write_src(str(f))

    monkeypatch.setattr(m, "_is_fuse_path", lambda p: True)
    monkeypatch.setenv("GBX_FORCE_STAGE", "1")

    path, is_temp = m._stage_local_if_needed(str(f))
    assert is_temp is True, "GBX_FORCE_STAGE=1 must force a copy"
    assert path != str(f)
    assert os.path.exists(path)
    os.remove(path)


# ---------------------------------------------------------------------------
# Task 4: GBX_STAGE_MAX_BYTES guard
# ---------------------------------------------------------------------------


def test_stage_local_respects_max_bytes(monkeypatch, tmp_path):
    """Files over GBX_STAGE_MAX_BYTES raise StageTooLargeError instead of copying."""
    from databricks.labs.gbx.pyrx.core import preparer as m

    big = tmp_path / "big.tif"
    big.write_bytes(b"x" * 1024)  # 1024-byte file

    monkeypatch.setenv("GBX_STAGE_MAX_BYTES", "512")  # cap below 1024 bytes
    monkeypatch.setenv("GBX_FORCE_STAGE", "1")  # skip probe, go straight to copy branch
    monkeypatch.setattr(m, "_is_fuse_path", lambda p: True)  # force FUSE path

    import pytest

    with pytest.raises(m.StageTooLargeError):
        m._stage_local_if_needed(str(big))
