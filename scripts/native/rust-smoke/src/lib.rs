use pyo3::prelude::*;

/// Proves the pinned toolchain builds an abi3 module offline in the manylinux_2_28 container.
#[pyfunction]
fn smoke() -> &'static str {
    "ok"
}

#[pymodule]
fn gbx_rust_smoke(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(smoke, m)?)?;
    Ok(())
}
