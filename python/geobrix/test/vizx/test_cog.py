"""Offline tests for plot_cog (rasterio overview read over a contextily basemap)."""

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402


def _write_tif(tmp_path, bands=3, size=32, crs="EPSG:3857"):
    import rasterio
    from rasterio.transform import from_bounds

    path = tmp_path / "cog.tif"
    data = (np.random.rand(bands, size, size) * 1000).astype("uint16")
    transform = from_bounds(-1.36e7, 4.5e6, -1.35e7, 4.51e6, size, size)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=bands,
        dtype="uint16",
        crs=crs,
        transform=transform,
    ) as dst:
        dst.write(data)
    return str(path)


def _write_tif_4326(tmp_path, bands=3, size=32, name="cog_4326.tif", nodata=None):
    # A small geographic (EPSG:4326) GeoTIFF near Rochester, NY -- degree-scale
    # bounds, matching the real ortho COG's CRS class used to reproduce the bug.
    import rasterio
    from rasterio.transform import from_bounds

    path = tmp_path / name
    data = (np.random.rand(bands, size, size) * 255).astype("uint8")
    transform = from_bounds(-77.97, 43.23, -77.96, 43.24, size, size)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=bands,
        dtype="uint8",
        crs="EPSG:4326",
        transform=transform,
        nodata=nodata,
    ) as dst:
        dst.write(data)
    return str(path)


def test_plot_cog_renders_figure(tmp_path):
    from databricks.labs.gbx.vizx import plot_cog

    plt.close("all")
    path = _write_tif(tmp_path, bands=3)
    plot_cog(path)
    assert len(plt.get_fignums()) >= 1
    plt.close("all")


def test_plot_cog_band_select(tmp_path, monkeypatch):
    from databricks.labs.gbx.vizx import _cog

    path = _write_tif(tmp_path, bands=3)
    captured = {}
    # capture the array handed to the renderer to confirm a single band was read
    monkeypatch.setattr(
        _cog,
        "_render_cog",
        lambda data, transform, **kw: captured.update(shape=data.shape),
    )
    _cog.plot_cog(path, band=2)
    assert captured["shape"][0] == 1  # one band selected


def test_plot_cog_strips_dbfs_scheme(tmp_path):
    from databricks.labs.gbx.vizx import plot_cog

    plt.close("all")
    path = _write_tif(tmp_path, bands=1)
    plot_cog("dbfs:" + path)  # must not raise on the scheme prefix
    assert len(plt.get_fignums()) >= 1
    plt.close("all")


# --- to_crs reprojection (WarpedVRT) -------------------------------------


def test_plot_cog_to_crs_reprojects_multiband_to_mercator(tmp_path):
    # A geographic (EPSG:4326) 3-band COG rendered with to_crs="EPSG:3857" must
    # land on the Web Mercator (meters, millions) scale, not degree scale --
    # this is what lets a composite raster align with a 3857 vector layer.
    from databricks.labs.gbx.vizx import plot_cog

    plt.close("all")
    path = _write_tif_4326(tmp_path, bands=3)
    fig, ax = plt.subplots()
    plot_cog(path, to_crs="EPSG:3857", basemap=False, ax=ax)
    xlim = ax.get_xlim()
    assert max(abs(v) for v in xlim) > 1e6, f"expected mercator meters, got {xlim}"
    plt.close("all")


def test_plot_cog_to_crs_reprojects_single_band_to_mercator(tmp_path):
    # Same check via the single-`band=` read path (separate code path from the
    # all-bands _decimated_read path).
    from databricks.labs.gbx.vizx import plot_cog

    plt.close("all")
    path = _write_tif_4326(tmp_path, bands=3)
    fig, ax = plt.subplots()
    plot_cog(path, band=1, to_crs="EPSG:3857", basemap=False, ax=ax)
    xlim = ax.get_xlim()
    assert max(abs(v) for v in xlim) > 1e6, f"expected mercator meters, got {xlim}"
    plt.close("all")


def test_plot_cog_no_to_crs_stays_native_degree_scale(tmp_path):
    # Without to_crs (the default / prior behavior) a 4326 source stays on the
    # degree scale -- byte-identical to today's behavior.
    from databricks.labs.gbx.vizx import plot_cog

    plt.close("all")
    path = _write_tif_4326(tmp_path, bands=3)
    fig, ax = plt.subplots()
    plot_cog(path, basemap=False, ax=ax)
    xlim = ax.get_xlim()
    assert max(abs(v) for v in xlim) < 400, f"expected degree scale, got {xlim}"
    plt.close("all")


def test_plot_cog_to_crs_already_matching_is_noop(tmp_path):
    # to_crs equal to the source CRS must not warp (same coordinate scale as
    # plain plot_cog, and must not error).
    from databricks.labs.gbx.vizx import plot_cog

    plt.close("all")
    path = _write_tif(tmp_path, bands=3, crs="EPSG:3857")  # already 3857
    fig, ax = plt.subplots()
    plot_cog(path, to_crs="EPSG:3857", basemap=False, ax=ax)
    xlim = ax.get_xlim()
    assert max(abs(v) for v in xlim) > 1e6
    plt.close("all")


def test_plot_cog_to_crs_none_source_crs_does_not_warp(tmp_path):
    # A CRS-less source cannot be warped; to_crs must be ignored rather than
    # raising.
    import rasterio
    from rasterio.transform import from_origin

    from databricks.labs.gbx.vizx import plot_cog

    plt.close("all")
    path = tmp_path / "no_crs.tif"
    data = (np.random.rand(1, 8, 8) * 255).astype("uint8")
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=8,
        width=8,
        count=1,
        dtype="uint8",
        transform=from_origin(0, 8, 1, 1),
    ) as dst:
        dst.write(data)
    fig, ax = plt.subplots()
    plot_cog(str(path), to_crs="EPSG:3857", basemap=False, ax=ax)  # must not raise
    assert len(plt.get_fignums()) >= 1
    plt.close("all")


def test_plot_cog_to_crs_preserves_nodata_borders(tmp_path, monkeypatch):
    # nodata (black borders from an oblique/rotated COG) must survive the warp
    # -- WarpedVRT honors nodata, so the masked read stays masked post-warp.
    import rasterio

    from databricks.labs.gbx.vizx import _cog

    path = _write_tif_4326(tmp_path, bands=1, nodata=0)
    with rasterio.open(path) as src:
        arr = src.read(1)
    arr[:5, :] = 0  # force a nodata band across the top rows
    with rasterio.open(path, "r+") as dst:
        dst.write(arr, 1)

    captured = {}
    monkeypatch.setattr(
        _cog,
        "_render_cog",
        lambda data, transform, **kw: captured.update(data=data),
    )
    plt.close("all")
    _cog.plot_cog(path, band=1, to_crs="EPSG:3857", basemap=False)
    data = captured["data"]
    assert np.ma.isMaskedArray(data)
    assert data.mask.any()  # some nodata pixels remained masked through the warp
    plt.close("all")
