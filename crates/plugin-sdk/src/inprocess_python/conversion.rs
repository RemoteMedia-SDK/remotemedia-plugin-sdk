//! Explicit ownership choice for Python bulk conversions. Legacy payload copies
//! retain caller authority; the shared path only exports/recovers owning views.
use _remotemedia_buffers::{recover_audio, recover_bytes, BufferData, ReadBuffer};
use pyo3::{
    prelude::*,
    types::{PyBytes, PyList, PyString},
};
use remotemedia_types::{AudioSamples, CopyContext, CopyPolicy, CopyReason, SharedBytes};

#[derive(Clone, Copy)]
pub(super) enum Conversion<'a> {
    Shared,
    Legacy(&'a CopyContext),
}

pub(super) fn require_legacy(copies: &CopyContext) -> PyResult<()> {
    if copies.policy() != CopyPolicy::AllowCopies {
        return Err(pyo3::exceptions::PyBufferError::new_err(
            "Legacy in-process Python requires allow_copies; select shared-buffer execution for strict calls",
        ));
    }
    Ok(())
}

impl Conversion<'_> {
    pub fn is_shared(self) -> bool {
        matches!(self, Self::Shared)
    }

    pub fn export_bytes(self, py: Python<'_>, data: &SharedBytes) -> PyResult<Py<PyAny>> {
        match self {
            Self::Shared => {
                Ok(Py::new(py, ReadBuffer::new(BufferData::Bytes(data.clone()))?)?.into_any())
            }
            Self::Legacy(copies) => copies
                .materialize(CopyReason::BorrowedInput, data.len() as u64, || {
                    PyBytes::new(py, data).into_any().unbind()
                })
                .map_err(copy_error),
        }
    }

    pub fn import_bytes(self, obj: &Bound<'_, PyAny>) -> PyResult<SharedBytes> {
        match self {
            Self::Shared => recover_bytes(obj),
            Self::Legacy(copies) => {
                if let Ok(bytes) = obj.cast::<PyBytes>() {
                    return copies
                        .materialize(
                            CopyReason::VecMaterialization,
                            bytes.as_bytes().len() as u64,
                            || SharedBytes::from(bytes.as_bytes().to_vec()),
                        )
                        .map_err(copy_error);
                }
                import_sequence(obj, copies, |item| item.extract::<u8>()).map(Into::into)
            }
        }
    }

    pub fn export_audio(self, py: Python<'_>, samples: &AudioSamples) -> PyResult<Py<PyAny>> {
        match self {
            Self::Shared => {
                let samples = match samples {
                    AudioSamples::Shared(buffer) => buffer.clone(),
                    AudioSamples::Arc(buffer) => buffer.clone().into(),
                    AudioSamples::Vec(_) => {
                        return Err(pyo3::exceptions::PyBufferError::new_err(
                            "audio must be promoted before shared export",
                        ))
                    }
                };
                let owner = Py::new(py, ReadBuffer::new(BufferData::Audio(samples))?)?;
                Ok(py
                    .import("numpy")?
                    .call_method1("asarray", (owner,))?
                    .unbind())
            }
            Self::Legacy(copies) => copies
                .materialize_result(CopyReason::BorrowedInput, samples.len() as u64 * 4, || {
                    PyList::new(py, samples.as_slice().iter().copied())
                        .map(|list| list.into_any().unbind())
                })
                .map_err(copy_error)?,
        }
    }

    pub fn import_audio(self, obj: &Bound<'_, PyAny>) -> PyResult<AudioSamples> {
        match self {
            Self::Shared => recover_audio(obj).map(Into::into),
            Self::Legacy(copies) => {
                import_sequence(obj, copies, |item| item.extract::<f32>()).map(Into::into)
            }
        }
    }
}

/// Reserve final storage once, then convert only the declared number of elements.
/// A changing sequence fails explicitly instead of growing an unaccounted buffer.
/// Counts are logical u8/f32 payload storage, excluding Python object overhead.
fn import_sequence<T>(
    obj: &Bound<'_, PyAny>,
    copies: &CopyContext,
    mut extract: impl FnMut(&Bound<'_, PyAny>) -> PyResult<T>,
) -> PyResult<Vec<T>> {
    // Match PyO3's Vec extraction protocol, which also accepts NumPy arrays.
    // PySequence's safe cast instead requires collections.abc.Sequence membership.
    // SAFETY: obj retains a live Python object and this call holds the GIL.
    if obj.is_instance_of::<PyString>() || unsafe { pyo3::ffi::PySequence_Check(obj.as_ptr()) } == 0
    {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "Expected a Python payload sequence",
        ));
    }
    let length = obj.len()?;
    let bytes = length
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| {
            pyo3::exceptions::PyOverflowError::new_err("Python payload size overflow")
        })?;
    let operation = copies
        .begin(CopyReason::VecMaterialization, bytes as u64)
        .map_err(copy_error)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(length)
        .map_err(|error| pyo3::exceptions::PyMemoryError::new_err(error.to_string()))?;
    for item in obj.try_iter()? {
        if output.len() == length {
            return Err(changed_length());
        }
        output.push(extract(&item?)?);
    }
    if output.len() != length {
        return Err(changed_length());
    }
    operation.complete();
    Ok(output)
}

fn changed_length() -> PyErr {
    pyo3::exceptions::PyValueError::new_err("Python payload length changed during conversion")
}
fn copy_error(error: impl std::fmt::Display) -> PyErr {
    pyo3::exceptions::PyBufferError::new_err(error.to_string())
}
