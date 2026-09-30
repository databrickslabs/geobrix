import numpy as np
import pytest

from databricks.labs.gbx.pyrx.ortho import apply_sim3, rasterize_enu_ortho, umeyama_sim3


def test_umeyama_recovers_known_transform():
    rng = np.random.default_rng(0)
    src = rng.normal(size=(20, 3))
    R_true = np.linalg.qr(rng.normal(size=(3, 3)))[0]
    if np.linalg.det(R_true) < 0:
        R_true[:, 0] *= -1
    s_true, t_true = 2.5, np.array([10.0, -5.0, 3.0])
    dst = s_true * (src @ R_true.T) + t_true
    s, R, t = umeyama_sim3(src, dst)
    assert abs(s - s_true) < 1e-6
    assert np.allclose(apply_sim3((s, R, t), src), dst, atol=1e-6)


def test_apply_sim3_identity():
    pts = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    out = apply_sim3((1.0, np.eye(3), np.zeros(3)), pts)
    assert np.allclose(out, pts)


def test_umeyama_degenerate_input_raises():
    # Fewer than 3 point pairs (2 coincident points from the original brief's
    # "degenerate" case) — orchestrator ruling: this must RAISE, not silently
    # return NaN from a 0/0 scale division.
    src_two = np.array([[1.0, 1.0, 1.0], [1.0, 1.0, 1.0]])
    dst_two = np.array([[0.0, 0.0, 0.0], [2.0, 2.0, 2.0]])
    with pytest.raises(ValueError):
        umeyama_sim3(src_two, dst_two)

    # >=3 pairs but zero source variance (all src points coincident).
    src_coincident = np.array([[1.0, 2.0, 3.0]] * 3)
    dst_distinct = np.array([[0.0, 0.0, 0.0], [1.0, 1.0, 1.0], [2.0, 2.0, 2.0]])
    with pytest.raises(ValueError):
        umeyama_sim3(src_coincident, dst_distinct)


def test_module_imports_without_pycolmap():
    # Importing the module + using the pure primitives must NOT require pycolmap.
    import importlib

    m = importlib.import_module("databricks.labs.gbx.pyrx.ortho")
    assert hasattr(m, "umeyama_sim3") and hasattr(m, "apply_sim3")


def test_place_clusters_shared_enu_requires_pycolmap_with_clear_message():
    # No pycolmap installed locally -> the lazy import must raise a clear,
    # actionable ImportError rather than a bare ModuleNotFoundError. Import
    # directly (not via sys.modules, which another test may have populated)
    # to decide whether this environment can even exercise the failure path.
    import importlib

    try:
        importlib.import_module("pycolmap")
    except ImportError:
        pass
    else:
        pytest.skip("pycolmap is importable in this environment")

    m = importlib.import_module("databricks.labs.gbx.pyrx.ortho")
    with pytest.raises(ImportError, match="place_clusters_shared_enu"):
        m.place_clusters_shared_enu(
            {0: ("/nonexistent/sparse", "/nonexistent/gps.json")}
        )


@pytest.mark.parametrize("_", [0])
def test_place_clusters_shared_enu_available(_):
    pytest.importorskip("pycolmap")
    from databricks.labs.gbx.pyrx.ortho import place_clusters_shared_enu

    assert callable(place_clusters_shared_enu)
    # A full functional test needs a synthetic pycolmap Reconstruction; building
    # one (cameras + posed images + an on-disk sparse model pycolmap can load)
    # is not cheaply feasible without pycolmap available to author and verify it
    # against, and this environment has no pycolmap install to check it with.
    # This asserts the gated symbol exists and the module imported without
    # pycolmap at top level (see test_module_imports_without_pycolmap).


def test_dense_clusters_to_products_available():
    import inspect

    from databricks.labs.gbx.pyrx.ortho import dense_clusters_to_products

    # Importing dense_clusters_to_products does not require pycolmap, so this
    # structural assertion runs unconditionally (light venv + CI too) -- spark
    # is an EXPLICIT first parameter (was a notebook global).
    sig = inspect.signature(dense_clusters_to_products)
    assert list(sig.parameters)[0] == "spark"

    pytest.importorskip("pycolmap")
    # A full run needs a tiny fused .ply + pycolmap models + a spark session; where
    # those are available, assert it returns (used_cids, merged_laz_path) and the
    # merged LAZ round-trips through the lidar_gbx reader.


def test_dense_orthomosaic_composes(monkeypatch):
    import databricks.labs.gbx.pyrx.ortho as ortho_mod

    calls = {}

    def fake_reconstruct(cluster_models, image_dir, **kwargs):
        calls["reconstruct_args"] = (cluster_models, image_dir)
        calls["reconstruct_kwargs"] = kwargs
        return {0: "a.ply"}

    def fake_products(spark, cluster_models, dense_plys, **kwargs):
        calls["products_args"] = (spark, cluster_models, dense_plys)
        calls["products_kwargs"] = kwargs
        return ([0], "merged.laz")

    monkeypatch.setattr(ortho_mod, "dense_reconstruct_clusters", fake_reconstruct)
    monkeypatch.setattr(ortho_mod, "dense_clusters_to_products", fake_products)

    spark = object()
    cluster_models = {0: ("/sparse/0", "/gps/0.json")}
    result = ortho_mod.dense_orthomosaic(
        spark,
        cluster_models,
        "/images",
        merged_ortho="/out/ortho.tif",
        merged_dsm="/out/dsm.tif",
        merged_laz="/out/merged.laz",
        cluster_paths={0: {"ortho": "/out/c0_ortho.tif", "dsm": "/out/c0_dsm.tif"}},
        work_root="/work",
        ply_root="/ply",
        checkpoint=None,
        force=True,
        gsd_cm=5.0,
        group="grp1",
        max_image_size=800,
        src_images=4,
        geom_consistency=True,
        num_iterations=3,
        window_step=2,
        allocation={"concurrency": 1},
        on_event=print,
    )

    assert result == ([0], "merged.laz")

    r_cluster_models, r_image_dir = calls["reconstruct_args"]
    assert r_cluster_models == cluster_models
    assert r_image_dir == "/images"
    r_kwargs = calls["reconstruct_kwargs"]
    assert r_kwargs["work_root"] == "/work"
    assert r_kwargs["ply_root"] == "/ply"
    assert r_kwargs["checkpoint"] is None
    assert r_kwargs["force"] is True
    assert r_kwargs["max_image_size"] == 800
    assert r_kwargs["src_images"] == 4
    assert r_kwargs["geom_consistency"] is True
    assert r_kwargs["num_iterations"] == 3
    assert r_kwargs["window_step"] == 2
    assert r_kwargs["allocation"] == {"concurrency": 1}
    assert r_kwargs["on_event"] is print

    p_spark, p_cluster_models, p_plys = calls["products_args"]
    assert p_spark is spark
    assert p_cluster_models == {0: cluster_models[0]}
    assert p_plys == {0: "a.ply"}
    p_kwargs = calls["products_kwargs"]
    assert p_kwargs["merged_ortho"] == "/out/ortho.tif"
    assert p_kwargs["merged_dsm"] == "/out/dsm.tif"
    assert p_kwargs["merged_laz"] == "/out/merged.laz"
    assert p_kwargs["cluster_paths"] == {
        0: {"ortho": "/out/c0_ortho.tif", "dsm": "/out/c0_dsm.tif"}
    }
    assert p_kwargs["gsd_cm"] == 5.0
    assert p_kwargs["group"] == "grp1"


def test_dense_orthomosaic_raises_on_empty(monkeypatch):
    import databricks.labs.gbx.pyrx.ortho as ortho_mod

    monkeypatch.setattr(ortho_mod, "dense_reconstruct_clusters", lambda *a, **k: {})

    products_called = []
    monkeypatch.setattr(
        ortho_mod,
        "dense_clusters_to_products",
        lambda *a, **k: products_called.append(True),
    )

    with pytest.raises(RuntimeError, match="no cluster produced points"):
        ortho_mod.dense_orthomosaic(
            object(),
            {0: ("/sparse/0", "/gps/0.json")},
            "/images",
            merged_ortho="/out/ortho.tif",
            merged_dsm="/out/dsm.tif",
            merged_laz="/out/merged.laz",
            cluster_paths={0: {"ortho": "/o.tif", "dsm": "/d.tif"}},
            work_root="/work",
        )
    assert products_called == []


def test_dense_orthomosaic_spark_is_explicit():
    import inspect

    from databricks.labs.gbx.pyrx.ortho import dense_orthomosaic

    sig = inspect.signature(dense_orthomosaic)
    assert list(sig.parameters)[0] == "spark"


def test_rasterize_enu_ortho_writes_geotiffs(tmp_path):
    # gps_tf=None -- the equirectangular path must run with no pycolmap.
    import rasterio

    rng = np.random.default_rng(0)
    n = 2000
    xe = rng.uniform(-10, 10, n)
    ye = rng.uniform(-10, 10, n)
    ze = rng.uniform(0, 5, n)
    r = rng.integers(0, 255, n).astype("uint8")
    g = r.copy()
    b = r.copy()
    op = tmp_path / "o.tif"
    dp = tmp_path / "d.tif"
    ortho, dsm = rasterize_enu_ortho(
        xe,
        ye,
        ze,
        r,
        g,
        b,
        ref_lat=37.8,
        ref_lon=-122.4,
        ref_alt=0.0,
        gps_tf=None,
        out_ortho=str(op),
        out_dsm=str(dp),
        gsd_cm=50.0,
    )
    # Coarse georef sanity: the cloud is centred near the ENU origin (+-10 m),
    # so the raster's top-left (west, north) bound must land within a few
    # hundred metres of ref_lon/ref_lat -- catches gross formula/sign bugs
    # (e.g. degrees-as-metres, swapped axes, wrong hemisphere) without
    # asserting exact fidelity (that needs the gps_tf ellipsoidal path, which
    # is untestable here without pycolmap).
    tol_deg = 0.005  # ~550 m at this latitude
    with rasterio.open(ortho) as ds:
        assert ds.count >= 3 and ds.crs is not None
        left, _bottom, _right, top = ds.bounds
        assert abs(left - (-122.4)) < tol_deg
        assert abs(top - 37.8) < tol_deg
    with rasterio.open(dsm) as ds:
        assert ds.count == 1
