//! Real child interpreter using the production PyO3 module, not a mock exporter.
use pyo3::prelude::*;

fn main() -> PyResult<()> {
    Python::attach(|py| {
        let module = PyModule::new(py, "_remotemedia_buffers")?;
        _remotemedia_buffers::_remotemedia_buffers(&module)?;
        py.import("sys")?
            .getattr("modules")?
            .set_item("_remotemedia_buffers", module)?;
        let source = std::ffi::CString::new(include_str!("python_worker.py")).unwrap();
        py.run(&source, None, None)
    })
}
