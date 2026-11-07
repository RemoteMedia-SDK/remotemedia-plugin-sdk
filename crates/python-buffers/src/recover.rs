//! Recover only views rooted in our immutable exporter. A private array, dtype
//! conversion, writable loan, or non-contiguous range is never silently packed/copied.
use crate::{BufferData, ReadBuffer};
use pyo3::{
    buffer::{PyBuffer, PyUntypedBuffer},
    exceptions::PyBufferError,
    prelude::*,
    types::PyMemoryView,
};
use remotemedia_types::SharedBuffer;

fn owner(obj: &Bound<'_, PyAny>) -> PyResult<BufferData> {
    let mut current = obj.clone();
    for _ in 0..64 {
        if let Ok(buffer) = current.cast::<ReadBuffer>() {
            return Ok(buffer.get().data.clone());
        }
        current = if current.is_instance_of::<PyMemoryView>() {
            current.getattr("obj")?
        } else {
            let Ok(numpy) = obj.py().import("numpy") else {
                break;
            };
            if !current.get_type().is(&numpy.getattr("ndarray")?) {
                break;
            }
            current.getattr("base")?
        };
    }
    Err(PyBufferError::new_err("output must retain a published RemoteMedia buffer; private or writable storage requires an explicit copy"))
}

fn range(
    pointer: usize,
    byte_len: usize,
    base: usize,
    elements: usize,
    itemsize: usize,
) -> PyResult<std::ops::Range<usize>> {
    let error = || PyBufferError::new_err("view range does not belong to the retained buffer");
    let offset = pointer.checked_sub(base).ok_or_else(error)?;
    if !offset.is_multiple_of(itemsize) || !byte_len.is_multiple_of(itemsize) {
        return Err(error());
    }
    let start = offset / itemsize;
    let end = start
        .checked_add(byte_len / itemsize)
        .filter(|&end| end <= elements)
        .ok_or_else(error)?;
    Ok(start..end)
}

pub fn recover_audio(obj: &Bound<'_, PyAny>) -> PyResult<SharedBuffer<f32>> {
    let view = native_audio_buffer(obj)?;
    if !view.readonly() || !view.is_c_contiguous() {
        return Err(PyBufferError::new_err(
            "audio output must be readonly contiguous native f32",
        ));
    }
    let BufferData::Audio(data) = owner(obj)? else {
        return Err(PyBufferError::new_err(
            "audio output has a different backing element type",
        ));
    };
    let range = range(
        view.buf_ptr() as usize,
        view.len_bytes(),
        data.as_ptr() as usize,
        data.len(),
        4,
    )?;
    data.slice(range)
        .map_err(|e| PyBufferError::new_err(e.to_string()))
}

/// Validate wire endianness independently of the typed-buffer dependency.
/// The pinned PyO3 0.28.3 little-endian format check also accepts `>f`.
pub(crate) fn native_audio_buffer(obj: &Bound<'_, PyAny>) -> PyResult<PyBuffer<f32>> {
    let view = PyUntypedBuffer::get(obj)?;
    let native = match view.format().to_bytes() {
        b"f" | b"@f" | b"=f" => true,
        b"<f" => cfg!(target_endian = "little"),
        b">f" | b"!f" => cfg!(target_endian = "big"),
        _ => false,
    };
    if !native || view.item_size() != 4 {
        return Err(PyBufferError::new_err(
            "buffer contents are not compatible with native f32",
        ));
    }
    view.into_typed()
}

pub fn recover_bytes(obj: &Bound<'_, PyAny>) -> PyResult<SharedBuffer<u8>> {
    // Opaque tensor/image bytes may be viewed with a numeric NumPy dtype. We
    // recover their original byte range, without reinterpreting them as Rust T.
    let view = PyUntypedBuffer::get(obj)?;
    if !view.readonly() || !(view.is_c_contiguous() || view.is_fortran_contiguous()) {
        return Err(PyBufferError::new_err(
            "byte output must be readonly and contiguous",
        ));
    }
    let BufferData::Bytes(data) = owner(obj)? else {
        return Err(PyBufferError::new_err(
            "byte output has a different backing element type",
        ));
    };
    let range = range(
        view.buf_ptr() as usize,
        view.len_bytes(),
        data.as_ptr() as usize,
        data.len(),
        1,
    )?;
    data.slice(range)
        .map_err(|e| PyBufferError::new_err(e.to_string()))
}
