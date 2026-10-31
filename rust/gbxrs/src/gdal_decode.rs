//! `decode_window` (feature `gdal`): one window of a raster file, as a ZSTD GeoTIFF's bytes, through the payload's libgdal.
//!
//! GDAL goes through `gdal-sys` alone (the high-level `gdal` crate does not support GDAL 3.13), and the module follows the
//! pattern of `rust/gbxrs-native-probe/src/gdal.rs`: every call runs under a [`Quiet`] guard (GDAL's per-thread error state
//! reset first, its messages off stderr, the last one becomes the Python error), a dataset is a handle closed on drop, and each
//! `unsafe` block holds only the FFI call and says why it is sound.
//!
//! The open policy is the lightweight tier's (`_gdal_policy.open_data`), applied by what the caller hands in: `drivers` is the
//! manifest's allowed raster drivers, given to `GDALOpenEx` as GDAL's driver list, so a file no allowed driver identifies is
//! refused and never opened by the rest of the registry; the VRT raw-band and Python pixel-function settings are switched off
//! with it. The drivers GeoBrix refuses are never registered in the first place: `GDAL_SKIP` is in the process environment
//! before `GDALAllRegister` runs (`_gdal_policy.prepare()` sets it when `databricks.labs.gbx` is imported). With `drivers`
//! None (the policy switched off) the whole registry opens the file, as `open_data` does with the policy off.
use std::ffi::{c_char, c_int, CStr, CString};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

// GDAL_OF_RASTER | GDAL_OF_VERBOSE_ERROR (gdal-sys has no bindings for the GDAL_OF_* macros); GDAL_OF_READONLY is 0.
// The verbose flag makes a failed open say why (a file no allowed driver identifies, a missing file).
const OPEN_FLAGS: u32 = 0x02 | 0x40;

static INIT: Once = Once::new();
static SEQ: AtomicU64 = AtomicU64::new(0);

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

/// An open dataset, closed when dropped (a failed decode closes both of its datasets on every path).
struct Dataset(gdal_sys::GDALDatasetH);

impl Dataset {
    fn size(&self) -> (i64, i64) {
        // SAFETY: self.0 is an open dataset.
        unsafe {
            (
                gdal_sys::GDALGetRasterXSize(self.0) as i64,
                gdal_sys::GDALGetRasterYSize(self.0) as i64,
            )
        }
    }

    /// Closes the dataset now and reports the close's own error (the flush of a written GeoTIFF).
    fn close(mut self) -> bool {
        let handle = std::mem::replace(&mut self.0, std::ptr::null_mut());
        // SAFETY: handle is the open dataset, closed only here; Drop sees a null handle and does nothing.
        unsafe { gdal_sys::GDALClose(handle) == gdal_sys::CPLErr::CE_None }
    }
}

impl Drop for Dataset {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: self.0 is an open dataset, closed only here.
            unsafe { gdal_sys::GDALClose(self.0) };
        }
    }
}

/// A `/vsimem/` file, unlinked when dropped unless its buffer was taken.
struct MemFile(CString);

impl Drop for MemFile {
    fn drop(&mut self) {
        // SAFETY: VSIUnlink on a name this module made; a missing file is a harmless -1.
        unsafe { gdal_sys::VSIUnlink(self.0.as_ptr()) };
    }
}

fn c_string(text: &str, what: &str) -> PyResult<CString> {
    CString::new(text).map_err(|e| PyValueError::new_err(format!("{what}: {e}")))
}

/// Opens `path` read-only as a raster, with `drivers` as GDAL's driver list when given.
fn open(path: &str, drivers: Option<&[String]>, quiet: &Quiet) -> PyResult<Dataset> {
    let c_path = c_string(path, "the path")?;
    let names = match drivers {
        Some(list) => list
            .iter()
            .map(|d| c_string(d, "a driver name"))
            .collect::<PyResult<Vec<CString>>>()?,
        None => Vec::new(),
    };
    let mut list: Vec<*const c_char> = names.iter().map(|n| n.as_ptr()).collect();
    list.push(std::ptr::null());
    let allowed = if drivers.is_some() {
        list.as_ptr()
    } else {
        std::ptr::null()
    };
    // SAFETY: c_path and the driver list (null-terminated, its strings held by `names`) outlive the call; the open
    // options and sibling files are NULL.
    let handle = unsafe {
        gdal_sys::GDALOpenEx(
            c_path.as_ptr(),
            OPEN_FLAGS,
            allowed,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    if handle.is_null() {
        return Err(quiet.error(&format!("GDAL cannot open {path}")));
    }
    Ok(Dataset(handle))
}

fn decode(
    path: &str,
    window: [i64; 4],
    predictor: i32,
    drivers: Option<&[String]>,
) -> PyResult<Vec<u8>> {
    // SAFETY: registering drivers is idempotent (once here); the settings are process-global config options GDAL reads at
    // open, set before every policy-on open.
    INIT.call_once(|| unsafe { gdal_sys::GDALAllRegister() });
    if drivers.is_some() {
        for (key, value) in [
            ("GDAL_VRT_ENABLE_RAWRASTERBAND", "NO"),
            ("GDAL_VRT_ENABLE_PYTHON", "NO"),
        ] {
            let (k, v) = (CString::new(key).unwrap(), CString::new(value).unwrap());
            // SAFETY: both strings are NUL-terminated and outlive the call; GDAL copies them.
            unsafe { gdal_sys::CPLSetConfigOption(k.as_ptr(), v.as_ptr()) };
        }
    }
    let quiet = Quiet::new();
    let src = open(path, drivers, &quiet)?;
    let [col, row, w, h] = window;
    let (width, height) = src.size();
    if col < 0 || row < 0 || w <= 0 || h <= 0 || col + w > width || row + h > height {
        return Err(PyValueError::new_err(format!(
            "the window ({col}, {row}, {w}, {h}) is not inside the {width} x {height} raster {path}"
        )));
    }
    let args: Vec<CString> = [
        "-of".to_string(),
        "GTiff".to_string(),
        "-srcwin".to_string(),
        col.to_string(),
        row.to_string(),
        w.to_string(),
        h.to_string(),
        "-co".to_string(),
        "COMPRESS=ZSTD".to_string(),
        "-co".to_string(),
        format!("PREDICTOR={predictor}"),
    ]
    .iter()
    .map(|a| c_string(a, "a translate option"))
    .collect::<PyResult<_>>()?;
    let mut argv: Vec<*mut c_char> = args.iter().map(|a| a.as_ptr() as *mut c_char).collect();
    argv.push(std::ptr::null_mut());
    let out = MemFile(c_string(
        &format!(
            "/vsimem/gbxrs_{}_{}.tif",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ),
        "the output name",
    )?);
    // SAFETY: argv is null-terminated and its strings are held by `args`; GDAL copies the options.
    let options =
        unsafe { gdal_sys::GDALTranslateOptionsNew(argv.as_mut_ptr(), std::ptr::null_mut()) };
    if options.is_null() {
        return Err(quiet.error("GDALTranslateOptionsNew"));
    }
    let mut usage_error: c_int = 0;
    // SAFETY: out.0 is a NUL-terminated name, src.0 an open dataset, options the handle just made (freed right after).
    let dst = unsafe {
        let dst = gdal_sys::GDALTranslate(out.0.as_ptr(), src.0, options, &mut usage_error);
        gdal_sys::GDALTranslateOptionsFree(options);
        dst
    };
    if dst.is_null() {
        return Err(quiet.error("GDALTranslate"));
    }
    drop(src);
    // The GeoTIFF is complete only once its dataset is closed.
    if !Dataset(dst).close() {
        return Err(quiet.error("closing the decoded GeoTIFF"));
    }
    let mut len: gdal_sys::vsi_l_offset = 0;
    // SAFETY: out.0 names a file that exists; bTakeOwnership=0 leaves the buffer to the file, which MemFile unlinks.
    let buf = unsafe { gdal_sys::VSIGetMemFileBuffer(out.0.as_ptr(), &mut len, 0) };
    if buf.is_null() {
        return Err(quiet.error("reading the decoded GeoTIFF"));
    }
    // SAFETY: GDAL's buffer holds len bytes until the file is unlinked, which happens after this copy.
    Ok(unsafe { std::slice::from_raw_parts(buf, len as usize) }.to_vec())
}

/// The window `(col, row, w, h)` of the raster at `path` as a ZSTD GeoTIFF, with the TIFF `predictor`. `drivers` is the
/// manifest's allowed raster drivers (None: the policy is off and the whole registry may open the file).
#[pyfunction]
#[pyo3(signature = (path, col, row, w, h, predictor, drivers=None))]
#[allow(clippy::too_many_arguments)] // the Python signature: the window and predictor are scalars, `drivers` trails
pub fn decode_window<'py>(
    py: Python<'py>,
    path: &str,
    col: i64,
    row: i64,
    w: i64,
    h: i64,
    predictor: i32,
    drivers: Option<Vec<String>>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = py.detach(|| decode(path, [col, row, w, h], predictor, drivers.as_deref()))?;
    Ok(PyBytes::new(py, &bytes))
}
