use super::wire::{BufferReference, OutputMessage};
use crate::{
    pool::reader_descriptor, writer_descriptor, MappedElement, PeerId, Result, SessionId,
    SharedMemoryError,
};
use remotemedia_types::{PayloadStorage, RuntimeData, SharedBuffer, TensorEncoding};

/// Keep this owner until the broker acknowledges its output sequence. The broker
/// acquires its own owner before acknowledging, so final worker release is safe.
pub struct PreparedOutput {
    message: OutputMessage,
    _storage: PayloadStorage,
}
impl PreparedOutput {
    pub fn message(&self) -> &OutputMessage {
        &self.message
    }
}

pub fn prepare_output(
    data: RuntimeData,
    session: SessionId,
    peer: PeerId,
    encoding: TensorEncoding,
) -> Result<PreparedOutput> {
    let (metadata, storage) = data.into_shared_parts(encoding)?;
    let buffer = match &storage {
        PayloadStorage::None => None,
        PayloadStorage::Audio(data) => Some(reference(data, session, peer)?),
        PayloadStorage::Bytes(data) => Some(reference(data, session, peer)?),
    };
    Ok(PreparedOutput {
        message: OutputMessage { metadata, buffer },
        _storage: storage,
    })
}

fn reference<T: MappedElement>(
    data: &SharedBuffer<T>,
    session: SessionId,
    peer: PeerId,
) -> Result<BufferReference> {
    let range = data.storage_range();
    let size = std::mem::size_of::<T>() as u64;
    let offset = range.start as u64 * size;
    let byte_len = data.len() as u64 * size;
    if let Some(descriptor) = reader_descriptor(data) {
        if descriptor.session != session || descriptor.recipient != peer {
            return Err(SharedMemoryError::WrongSession);
        }
        return Ok(BufferReference::Input {
            lease: descriptor.lease,
            offset,
            byte_len,
        });
    }
    if let Some(descriptor) = writer_descriptor(data) {
        if descriptor.session != session || descriptor.producer != peer {
            return Err(SharedMemoryError::WrongSession);
        }
        return Ok(BufferReference::Producer {
            allocation: descriptor.allocation,
            offset,
            byte_len,
        });
    }
    Err(SharedMemoryError::InvalidLayout) // No private output adoption or heap fallback.
}
