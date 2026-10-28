//! `_gbx_native_probe`: not shipped. Calls GDAL, GEOS and PROJ from Rust, linked to the payload's
//! libraries, so `scripts/native/rust_native_smoke.py` can show that a Rust kernel, rasterio and
//! osgeo run on ONE GDAL (a `/vsimem` file rasterio writes is read here) and that GEOS and PROJ give
//! the answers shapely and pyproj give.
//!
//! GDAL goes through `gdal-sys` alone: its bindings are generated from the payload's GDAL 3.13
//! headers (the `bindgen` feature), and the high-level `gdal` 0.19.0 crate does not compile against
//! them (GDAL 3.13 renamed `GDT_Byte` to `GDT_UInt8` and added `bOperateInBufType` to
//! `GDALRasterIOExtraArg`). The few calls are wrapped here.
use std::ffi::{c_void, CStr, CString};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use geos::Geom;

fn err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// GDAL's last error message, as a Python RuntimeError.
fn gdal_error(what: &str) -> PyErr {
    // SAFETY: CPLGetLastErrorMsg returns a valid, NUL-terminated, thread-local string (never null).
    let msg = unsafe { CStr::from_ptr(gdal_sys::CPLGetLastErrorMsg()) };
    PyRuntimeError::new_err(format!("{what}: {}", msg.to_string_lossy()))
}

/// A raster dataset handle, closed when dropped.
struct Dataset(gdal_sys::GDALDatasetH);

impl Dataset {
    fn open(path: &str) -> PyResult<Self> {
        let c_path = CString::new(path).map_err(err)?;
        // SAFETY: registering drivers is idempotent; c_path outlives the call. GDALOpen is GDALOpenEx
        // with GDAL_OF_RASTER (gdal-sys has no bindings for the GDAL_OF_* macros).
        let h = unsafe {
            gdal_sys::GDALAllRegister();
            gdal_sys::GDALOpen(c_path.as_ptr(), gdal_sys::GDALAccess::GA_ReadOnly)
        };
        if h.is_null() {
            return Err(gdal_error(&format!("GDALOpen({path})")));
        }
        Ok(Dataset(h))
    }
}

impl Drop for Dataset {
    fn drop(&mut self) {
        // SAFETY: self.0 is a dataset GDALOpen returned, closed only here.
        unsafe { gdal_sys::GDALClose(self.0) };
    }
}

/// (width, height, sum of band 1) of a dataset another binding wrote, e.g. rasterio to `/vsimem/...`.
#[pyfunction]
fn gdal_vsimem_info(path: &str) -> PyResult<(usize, usize, f64)> {
    let ds = Dataset::open(path)?;
    // SAFETY: ds.0 is an open dataset; the band handle is owned by it and used while it is open; buf
    // holds width * height f64 values, which GDALRasterIO fills (GDT_Float64, packed: spacings 0).
    unsafe {
        let width = gdal_sys::GDALGetRasterXSize(ds.0);
        let height = gdal_sys::GDALGetRasterYSize(ds.0);
        let band = gdal_sys::GDALGetRasterBand(ds.0, 1);
        if band.is_null() {
            return Err(gdal_error("GDALGetRasterBand(1)"));
        }
        let mut buf = vec![0f64; (width as usize) * (height as usize)];
        let rc = gdal_sys::GDALRasterIO(
            band,
            gdal_sys::GDALRWFlag::GF_Read,
            0,
            0,
            width,
            height,
            buf.as_mut_ptr() as *mut c_void,
            width,
            height,
            gdal_sys::GDALDataType::GDT_Float64,
            0,
            0,
        );
        if rc != gdal_sys::CPLErr::CE_None {
            return Err(gdal_error("GDALRasterIO"));
        }
        Ok((width as usize, height as usize, buf.iter().sum()))
    }
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
    let key = CString::new("RELEASE_NAME").map_err(err)?;
    // SAFETY: GDALVersionInfo returns a NUL-terminated string GDAL owns (never null).
    let gdal = unsafe { CStr::from_ptr(gdal_sys::GDALVersionInfo(key.as_ptr())) }
        .to_string_lossy()
        .into_owned();
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
