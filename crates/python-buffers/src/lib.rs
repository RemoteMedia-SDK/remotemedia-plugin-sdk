//! Python exports retain immutable Rust owners; writable exports retain a loan.
//! No payload conversion or private-array adoption is implicit in this layer.
#![deny(unsafe_op_in_unsafe_fn)]

mod export;
mod recover;
#[cfg(feature = "worker")]
mod worker;

pub use export::{BufferData, OutputAllocator, OutputLoan, ReadBuffer};
use pyo3::prelude::*;
pub use recover::{recover_audio, recover_bytes};
use std::sync::Arc;

/// Explicit local-heap allocation for in-process producers. IPC workers receive
/// their negotiated mapped allocator from their execution context instead.
#[pyfunction]
fn local_allocator() -> OutputAllocator {
    OutputAllocator::new(Arc::new(remotemedia_types::HeapOutputAllocator))
}

#[pymodule]
pub fn _remotemedia_buffers(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ReadBuffer>()?;
    module.add_class::<OutputLoan>()?;
    module.add_class::<OutputAllocator>()?;
    module.add_function(wrap_pyfunction!(local_allocator, module)?)?;
    module.add("BUFFER_PROTOCOL_VERSION", 1)?;
    #[cfg(feature = "worker")]
    worker::register(module)?;
    Ok(())
}
