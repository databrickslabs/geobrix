//! `_gbx_native_probe`: not shipped. Calls GDAL, GEOS and PROJ from Rust, linked to the payload's
//! libraries, so `scripts/native/rust_native_smoke.py` can show that a Rust kernel, rasterio and
//! osgeo run on ONE GDAL (a `/vsimem` file rasterio writes is read here) and that GEOS and PROJ give
//! the answers shapely and pyproj give.
//!
//! GDAL goes through `gdal-sys` alone, behind the one safe wrapper module [`gdal`]: its bindings are
//! generated from the payload's GDAL 3.13 headers (the `bindgen` feature), and the high-level `gdal`
//! 0.19.0 crate does not compile against them (GDAL 3.13 renamed `GDT_Byte` to `GDT_UInt8` and added
//! `bOperateInBufType` to `GDALRasterIOExtraArg`).
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use geos::Geom;

mod gdal;

fn err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// (width, height, sum of band 1) of a dataset another binding wrote, e.g. rasterio to `/vsimem/...`.
#[pyfunction]
fn gdal_vsimem_info(path: &str) -> PyResult<(usize, usize, f64)> {
    let ds = gdal::Dataset::open(path)?;
    let (width, height) = ds.size();
    Ok((width, height, ds.band_sum(1)?))
}

/// The area of `wkb` buffered by `dist` with 16 segments per quadrant (shapely's default `quad_segs`).
#[pyfunction]
fn geos_buffer_area(wkb: &[u8], dist: f64) -> PyResult<f64> {
    let g = geos::Geometry::new_from_wkb(wkb).map_err(err)?;
    g.buffer(dist, 16).map_err(err)?.area().map_err(err)
}

/// (x, y) of one point from `src` to `dst` (any CRS PROJ knows, e.g. "EPSG:27700"), in the
/// normalised axis order (easting/longitude first), as pyproj's `always_xy=True`.
#[pyfunction]
fn proj_transform(src: &str, dst: &str, x: f64, y: f64) -> PyResult<(f64, f64)> {
    let p = proj::Proj::new_known_crs(src, dst, None).map_err(err)?;
    p.convert((x, y)).map_err(err)
}

/// The GDAL, GEOS and PROJ versions of the libraries this process runs (not the headers it was built with).
#[pyfunction]
fn versions() -> PyResult<(String, String, String)> {
    let gdal = gdal::version_info("RELEASE_NAME")?;
    let geos = geos::version().map_err(err)?;
    let proj = proj::ProjBuilder::new().lib_info().map_err(err)?.version;
    Ok((gdal, geos, proj))
}

#[pymodule]
fn _gbx_native_probe(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(gdal_vsimem_info, m)?)?;
    m.add_function(wrap_pyfunction!(geos_buffer_area, m)?)?;
    m.add_function(wrap_pyfunction!(proj_transform, m)?)?;
    m.add_function(wrap_pyfunction!(versions, m)?)?;
    Ok(())
}
