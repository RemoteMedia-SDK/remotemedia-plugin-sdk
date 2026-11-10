//! Payload-free messages for authenticated local worker channels.
use crate::descriptor::PROTOCOL_VERSION;
use crate::{
    import, BufferDescriptor, Delivery, ElementType, PeerId, Result, SessionId, SharedMemoryError,
    SharedPool,
};
use remotemedia_types::{
    CopyContext, CopyReason, PayloadKind, PayloadStorage, RuntimeData, RuntimeMetadata,
    SharedBuffer, TensorEncoding,
};
use serde::{Deserialize, Serialize};
use std::fs::File;

/// Only metadata and a lease descriptor are serialized. A file/OS handle must be
/// granted separately and bound to this exact message by the authenticated channel.
/// Version 1 numeric layouts are little-endian and contiguous. Inline text/JSON,
/// file references and control messages have no bulk descriptor or file grant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageDescriptor {
    pub version: u16,
    pub session: SessionId,
    pub recipient: PeerId,
    pub metadata: RuntimeMetadata,
    pub buffer: Option<BufferDescriptor>,
}

impl MessageDescriptor {
    pub fn validate(
        &self,
        session: SessionId,
        recipient: PeerId,
        capacity: Option<u64>,
        tensor_encoding: TensorEncoding,
    ) -> Result<()> {
        if self.version != PROTOCOL_VERSION
            || self.session != session
            || !cfg!(target_endian = "little")
        {
            return Err(SharedMemoryError::WrongSession);
        }
        if self.recipient != recipient {
            return Err(SharedMemoryError::InvalidLease);
        }
        let (kind, bytes) = match (&self.buffer, capacity) {
            (Some(buffer), Some(capacity)) => {
                if buffer.recipient != recipient {
                    return Err(SharedMemoryError::InvalidLease);
                }
                buffer.validate(session, capacity)?;
                let kind = match buffer.element {
                    ElementType::Bytes => PayloadKind::Bytes,
                    ElementType::F32Le => PayloadKind::AudioF32,
                };
                (Some(kind), buffer.byte_len)
            }
            (None, None) => (None, 0),
            _ => return Err(SharedMemoryError::InvalidLayout),
        };
        self.metadata
            .validate_payload(kind, bytes, tensor_encoding)?;
        Ok(())
    }
}

/// Dropping an unsent message cancels its lease. Serialized descriptor bytes do
/// not themselves mark dispatch; the channel must transfer this owning delivery.
pub struct MessageDelivery {
    descriptor: MessageDescriptor,
    delivery: Option<Delivery>,
    storage: PayloadStorage,
}

impl MessageDelivery {
    pub fn descriptor(&self) -> &MessageDescriptor {
        &self.descriptor
    }

    /// Exact exported view, including host provenance. Compatibility staging
    /// retains the new mapped owner, never the private owner it copied from.
    #[cfg(feature = "worker-protocol")]
    pub(crate) fn storage(&self) -> PayloadStorage {
        self.storage.clone()
    }

    /// # Safety
    /// The transport must obey Delivery::into_transport's grant/immutability and
    /// final-unmap/death reclamation contract. It must authenticate the recipient
    /// process incarnation and bind the handle to this exact descriptor.
    pub unsafe fn into_transport(self) -> (MessageDescriptor, Option<File>) {
        // SAFETY: these are exactly the underlying delivery's preconditions.
        let file = self
            .delivery
            .map(|delivery| unsafe { delivery.into_transport().0 });
        // The dispatched lease now pins storage independently of this owner.
        drop(self.storage);
        (self.descriptor, file)
    }
}

impl SharedPool {
    /// Prepare a strict transfer. Private heap buffers fail; there is no implicit
    /// copy into the pool. Consuming `data` promotes legacy Vec audio without a
    /// copy, but such heap storage still cannot be exported as an IPC mapping.
    pub fn prepare_message(
        &self,
        data: RuntimeData,
        recipient: PeerId,
        tensor_encoding: TensorEncoding,
    ) -> Result<MessageDelivery> {
        self.prepare_message_inner(data, recipient, tensor_encoding, None)
    }

    /// Explicit policy boundary. Eligible storage is forwarded without copying.
    /// Private or foreign-pool storage may be copied once into this pool only
    /// under allow_copies; the context attributes the completed payload bytes.
    pub fn prepare_message_with_copy_context(
        &self,
        data: RuntimeData,
        recipient: PeerId,
        tensor_encoding: TensorEncoding,
        context: &CopyContext,
    ) -> Result<MessageDelivery> {
        self.prepare_message_inner(data, recipient, tensor_encoding, Some(context))
    }

    fn prepare_message_inner(
        &self,
        data: RuntimeData,
        recipient: PeerId,
        tensor_encoding: TensorEncoding,
        context: Option<&CopyContext>,
    ) -> Result<MessageDelivery> {
        let (metadata, storage) = data.into_shared_parts(tensor_encoding)?;
        let (delivery, storage) = match storage {
            PayloadStorage::None => (None, PayloadStorage::None),
            PayloadStorage::Audio(data) => {
                let (delivery, data) = self.export_with_copy_context(data, recipient, context)?;
                (Some(delivery), PayloadStorage::Audio(data))
            }
            PayloadStorage::Bytes(data) => {
                let (delivery, data) = self.export_with_copy_context(data, recipient, context)?;
                (Some(delivery), PayloadStorage::Bytes(data))
            }
        };
        let descriptor = MessageDescriptor {
            version: PROTOCOL_VERSION,
            session: self.session_id(),
            recipient,
            metadata,
            buffer: delivery.as_ref().map(|d| d.descriptor().clone()),
        };
        Ok(MessageDelivery {
            descriptor,
            delivery,
            storage,
        })
    }

    fn export_with_copy_context<T: crate::MappedElement>(
        &self,
        data: SharedBuffer<T>,
        recipient: PeerId,
        context: Option<&CopyContext>,
    ) -> Result<(Delivery, SharedBuffer<T>)> {
        let (reason, original_error) = match self.export(&data, recipient) {
            Ok(delivery) => return Ok((delivery, data)),
            Err(SharedMemoryError::PrivateStorage) => (
                CopyReason::PrivateIpcIngress,
                SharedMemoryError::PrivateStorage,
            ),
            Err(SharedMemoryError::WrongSession) => (
                CopyReason::ForeignPoolIngress,
                SharedMemoryError::WrongSession,
            ),
            Err(error) => return Err(error),
        };
        let Some(context) = context else {
            return Err(original_error);
        };
        let bytes = std::mem::size_of_val(data.as_slice()) as u64;
        context.check(reason, bytes)?;
        let mut loan = self.allocate::<T>(data.len())?;
        context.materialize(reason, bytes, || loan.copy_from_slice(&data))?;
        let staged = loan.freeze();
        Ok((self.export(&staged, recipient)?, staged))
    }
}

struct ReleaseOnDrop(Option<Box<dyn FnOnce() + Send + Sync>>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            release();
        }
    }
}

/// Validate metadata and create an owning RuntimeData view without decoding any
/// payload bytes. `release` runs exactly once on failure or after the final view
/// drops (including views retained beyond message/session destruction).
///
/// # Safety
/// All `import` preconditions apply: the transport authenticates the peer and
/// binds the file to this accepted lease, preserving immutable storage through
/// final unmap. `release` must be bound to that transport-owned lease, not to
/// unchecked fields supplied by an arbitrary sender. Inline messages may supply
/// a no-op callback. This function does not authenticate OS handles itself.
pub unsafe fn import_message(
    descriptor: MessageDescriptor,
    file: Option<File>,
    session: SessionId,
    recipient: PeerId,
    tensor_encoding: TensorEncoding,
    release: impl FnOnce() + Send + Sync + 'static,
) -> Result<RuntimeData> {
    let release = ReleaseOnDrop(Some(Box::new(release)));
    let capacity = file
        .as_ref()
        .map(|file| file.metadata().map(|m| m.len()))
        .transpose()?;
    descriptor.validate(session, recipient, capacity, tensor_encoding)?;
    let storage = match (descriptor.buffer, file) {
        (Some(buffer), Some(file)) => {
            // SAFETY: caller guarantees the same authenticated immutable grant
            // required by import. The release guard is moved into its owner and
            // dropped only after unmap, including import failure paths.
            match buffer.element {
                ElementType::F32Le => PayloadStorage::Audio(unsafe {
                    import(file, buffer, session, move || drop(release))?
                }),
                ElementType::Bytes => PayloadStorage::Bytes(unsafe {
                    import(file, buffer, session, move || drop(release))?
                }),
            }
        }
        (None, None) => PayloadStorage::None,
        _ => return Err(SharedMemoryError::InvalidLayout),
    };
    Ok(descriptor.metadata.attach(storage, tensor_encoding)?)
}
