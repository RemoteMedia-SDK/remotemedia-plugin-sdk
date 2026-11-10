//! Recover immutable mapped storage or copy through the host's explicit ledger.
use super::*;

pub(super) fn adapt_audio(
    py: Python<'_>,
    request: &WorkerRequest,
    payload: &Bound<'_, PyAny>,
) -> PyResult<ReadBuffer> {
    // Validate format before policy/allocation; this never coerces dtype or
    // makes a contiguous private array. The buffer pins its exporter while the
    // GIL is released for the remote allocation request.
    let source = crate::recover::native_audio_buffer(payload)?;
    if let Ok(data) = recover_audio(payload) {
        if request.can_forward_payload(&PayloadStorage::Audio(data.clone()))? {
            return ReadBuffer::new(BufferData::Audio(data));
        }
    }
    // Copy under Python attachment through its buffer API. Never form a Rust
    // shared slice over an externally mutable ndarray. Strides pack in C order.
    let output = py.detach(|| {
        request.materialize_audio(source.item_count(), |output| {
            Python::attach(|py| source.copy_to_slice(py, output))
        })
    })??;
    ReadBuffer::new(BufferData::Audio(output))
}
