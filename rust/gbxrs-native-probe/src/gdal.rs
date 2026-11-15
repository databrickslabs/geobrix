//! The one safe wrapper over `gdal-sys` (the pattern a GeoBrix kernel follows until a release of the
//! high-level `gdal` crate supports GDAL 3.13):
//!
//! - every GDAL call runs under a [`Quiet`] guard: GDAL's per-thread error state is reset first (so a
//!   NULL with no fresh error never reports a stale message), its messages stay off stderr, and the
//!   last one becomes the Python error;
//! - a dataset is a handle closed on drop;
//! - a band is read one row at a time into a buffer reserved with `try_reserve_exact`, so a huge
//!   raster is a Python error, never an allocation abort of the whole process;
//! - each `unsafe` block holds only the FFI call and says why it is sound.
//!
//! A kernel that writes also needs an explicit, checked close: `Drop` cannot report `GDALClose`'s error.
use std::ffi::{c_void, CStr, CString};

use pyo3::exceptions::PyRuntimeError;
use pyo3::{PyErr, PyResult};

/// GDAL's error state for the calls made while it lives: reset on entry, quiet, restored on drop.
struct Quiet;

impl Quiet {
    fn new() -> Self {
        // SAFETY: both act on GDAL's per-thread error state; CPLQuietErrorHandler is GDAL's own handler.
        unsafe {
            gdal_sys::CPLErrorReset();
            gdal_sys::CPLPushErrorHandler(Some(gdal_sys::CPLQuietErrorHandler));
        }
        Quiet
    }

    /// The last GDAL error since this guard began, as a Python RuntimeError.
    fn error(&self, what: &str) -> PyErr {
        // SAFETY: CPLGetLastErrorMsg returns a valid, NUL-terminated, per-thread string (never null).
        let msg = unsafe { CStr::from_ptr(gdal_sys::CPLGetLastErrorMsg()) };
        let msg = msg.to_string_lossy();
        if msg.is_empty() {
            PyRuntimeError::new_err(format!("{what} failed"))
        } else {
            PyRuntimeError::new_err(format!("{what}: {msg}"))
        }
    }
}

impl Drop for Quiet {
    fn drop(&mut self) {
        // SAFETY: pops the handler Quiet::new pushed, on the same thread.
        unsafe { gdal_sys::CPLPopErrorHandler() };
    }
}

/// A raster dataset opened read-only, closed when dropped.
pub struct Dataset(gdal_sys::GDALDatasetH);

impl Dataset {
    pub fn open(path: &str) -> PyResult<Self> {
        let c_path = CString::new(path).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let quiet = Quiet::new();
        // SAFETY: registering drivers is idempotent. GDALOpen is GDALOpenEx with GDAL_OF_RASTER (gdal-sys has
        // no bindings for the GDAL_OF_* macros); c_path outlives the call.
        let handle = unsafe {
            gdal_sys::GDALAllRegister();
            gdal_sys::GDALOpen(c_path.as_ptr(), gdal_sys::GDALAccess::GA_ReadOnly)
        };
        if handle.is_null() {
            return Err(quiet.error(&format!("GDALOpen({path})")));
        }
        Ok(Dataset(handle))
    }

    /// (width, height) in pixels.
    pub fn size(&self) -> (usize, usize) {
        // SAFETY: self.0 is an open dataset.
        let (w, h) = unsafe {
            (
                gdal_sys::GDALGetRasterXSize(self.0),
                gdal_sys::GDALGetRasterYSize(self.0),
            )
        };
        (w.max(0) as usize, h.max(0) as usize)
    }

    /// The sum of band `n` (1-based), read one row at a time as float64.
    pub fn band_sum(&self, n: i32) -> PyResult<f64> {
        let quiet = Quiet::new();
        // SAFETY: self.0 is an open dataset; the band handle it returns belongs to it.
        let band = unsafe { gdal_sys::GDALGetRasterBand(self.0, n) };
        if band.is_null() {
            return Err(quiet.error(&format!("GDALGetRasterBand({n})")));
        }
        let (w, h) = self.size();
        let mut row: Vec<f64> = Vec::new();
        row.try_reserve_exact(w)
            .map_err(|e| PyRuntimeError::new_err(format!("a row of {w} pixels: {e}")))?;
        row.resize(w, 0.0);
        let mut sum = 0.0;
        for y in 0..h {
            // SAFETY: band is valid while self is open; row holds w f64 values, which GDALRasterIO fills with
            // line y (GDT_Float64, packed: spacings 0). w and y come from GDAL's own int sizes.
            let rc = unsafe {
                gdal_sys::GDALRasterIO(
                    band,
                    gdal_sys::GDALRWFlag::GF_Read,
                    0,
                    y as i32,
                    w as i32,
                    1,
                    row.as_mut_ptr() as *mut c_void,
                    w as i32,
                    1,
                    gdal_sys::GDALDataType::GDT_Float64,
                    0,
                    0,
                )
            };
            if rc != gdal_sys::CPLErr::CE_None {
                return Err(quiet.error("GDALRasterIO"));
            }
            sum += row.iter().sum::<f64>();
        }
        Ok(sum)
    }
}

impl Drop for Dataset {
    fn drop(&mut self) {
        // SAFETY: self.0 is a dataset GDALOpen returned, closed only here (read-only: nothing to flush).
        unsafe { gdal_sys::GDALClose(self.0) };
    }
}

/// GDALVersionInfo(key), e.g. "RELEASE_NAME" -> "3.13.3".
pub fn version_info(key: &str) -> PyResult<String> {
    let c_key = CString::new(key).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    // SAFETY: GDALVersionInfo returns a NUL-terminated string GDAL owns (never null); c_key outlives the call.
    let v = unsafe { CStr::from_ptr(gdal_sys::GDALVersionInfo(c_key.as_ptr())) };
    Ok(v.to_string_lossy().into_owned())
}
