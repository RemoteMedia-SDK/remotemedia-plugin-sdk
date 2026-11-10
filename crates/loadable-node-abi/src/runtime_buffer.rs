//! Safe Rust adapters for the same-process ownership ABI. No RuntimeData layout,
//! Rust allocator, Vec or Arc layout crosses the dynamic-library boundary.
#![deny(unsafe_op_in_unsafe_fn)]
use crate::buffer::*;
use abi_stable::{
    sabi_trait::TD_CanDowncast,
    std_types::{RArc, ROption, RSlice, RString},
};
use remotemedia_types::{
    PayloadKind, PayloadStorage, RuntimeData, RuntimeMetadata, SharedBuffer, TensorEncoding,
};
use std::{marker::PhantomData, sync::OnceLock};

mod allocator;
pub use allocator::{export_allocator, import_allocator};

pub const MAX_METADATA_BYTES: usize = 1024 * 1024;
type Result<T> = std::result::Result<T, RString>;

pub fn encode(data: RuntimeData) -> Result<FfiMessage> {
    let (metadata, storage) = data
        .into_shared_parts(TensorEncoding::Runtime)
        .map_err(error)?;
    let metadata = serde_json::to_vec(&metadata).map_err(error)?;
    if metadata.len() > MAX_METADATA_BYTES {
        return Err("native metadata exceeds 1 MiB".into());
    }
    let buffer = match storage {
        PayloadStorage::None => None,
        PayloadStorage::Audio(data) => Some(export_buffer(data)?),
        PayloadStorage::Bytes(data) => Some(export_buffer(data)?),
    };
    Ok(FfiMessage {
        version: BUFFER_ABI_VERSION,
        metadata: metadata.into(),
        buffer: buffer.into(),
    })
}

pub fn decode(message: FfiMessage) -> Result<RuntimeData> {
    if message.version != BUFFER_ABI_VERSION {
        return Err("incompatible native buffer protocol".into());
    }
    if message.metadata.len() > MAX_METADATA_BYTES {
        return Err("native metadata exceeds 1 MiB".into());
    }
    let metadata: RuntimeMetadata = serde_json::from_slice(&message.metadata).map_err(error)?;
    let (kind, bytes) = match &message.buffer {
        ROption::RNone => (None, 0),
        ROption::RSome(buffer) => (
            Some(match buffer.owner.element() {
                BufferElement::AudioF32 => PayloadKind::AudioF32,
                BufferElement::Bytes => PayloadKind::Bytes,
            }),
            buffer.byte_len,
        ),
    };
    metadata
        .validate_payload(kind, bytes, TensorEncoding::Runtime)
        .map_err(error)?;
    let storage = match message.buffer.into_option() {
        None => PayloadStorage::None,
        Some(buffer) => match buffer.owner.element() {
            BufferElement::AudioF32 => PayloadStorage::Audio(import_buffer::<f32>(buffer)?),
            BufferElement::Bytes => PayloadStorage::Bytes(import_buffer::<u8>(buffer)?),
        },
    };
    metadata
        .attach(storage, TensorEncoding::Runtime)
        .map_err(error)
}

fn error(error: impl std::fmt::Display) -> RString {
    error.to_string().into()
}

/// Available for downcast ONLY in the binary/library that originally created
/// this object. abi_stable rejects a cross-library Rust-type downcast even when
/// both libraries use the same crate version. This recovers mapped provenance
/// when a plugin forwards an input or publishes a host-allocated output.
#[derive(Clone)]
struct SourceOwner {
    storage: OnceLock<PayloadStorage>,
}

// SAFETY: only immutable shared owners are installed, once. Every cloned ABI
// handle retains the RArc and thus storage; no API exposes mutable publication.
unsafe impl BufferOwner for SourceOwner {
    fn element(&self) -> BufferElement {
        match self.storage.get().expect("published owner") {
            PayloadStorage::Audio(_) => BufferElement::AudioF32,
            PayloadStorage::Bytes(_) => BufferElement::Bytes,
            PayloadStorage::None => unreachable!("inline data has no owner"),
        }
    }
    fn view(&self) -> RSlice<'_, u8> {
        match self.storage.get().expect("published owner") {
            PayloadStorage::Audio(data) => audio_bytes(data).into(),
            PayloadStorage::Bytes(data) => data.as_ref().into(),
            PayloadStorage::None => unreachable!("inline data has no owner"),
        }
    }
}

fn audio_bytes(data: &[f32]) -> &[u8] {
    // SAFETY: f32 has no padding; every byte is initialized. The byte view is
    // immutable and has exactly the lifetime of the typed slice.
    unsafe { std::slice::from_raw_parts(data.as_ptr().cast(), std::mem::size_of_val(data)) }
}

trait Element: Copy + Send + Sync + 'static {
    const KIND: BufferElement;
    fn wrap(data: SharedBuffer<Self>) -> PayloadStorage;
    fn get(storage: &PayloadStorage) -> Option<&SharedBuffer<Self>>;
}
impl Element for u8 {
    const KIND: BufferElement = BufferElement::Bytes;
    fn wrap(data: SharedBuffer<Self>) -> PayloadStorage {
        PayloadStorage::Bytes(data)
    }
    fn get(storage: &PayloadStorage) -> Option<&SharedBuffer<Self>> {
        if let PayloadStorage::Bytes(data) = storage {
            Some(data)
        } else {
            None
        }
    }
}
impl Element for f32 {
    const KIND: BufferElement = BufferElement::AudioF32;
    fn wrap(data: SharedBuffer<Self>) -> PayloadStorage {
        PayloadStorage::Audio(data)
    }
    fn get(storage: &PayloadStorage) -> Option<&SharedBuffer<Self>> {
        if let PayloadStorage::Audio(data) = storage {
            Some(data)
        } else {
            None
        }
    }
}

struct ImportedOwner<T> {
    buffer: FfiBuffer,
    address: usize,
    elements: usize,
    marker: PhantomData<T>,
}
impl<T: Element> AsRef<[T]> for ImportedOwner<T> {
    fn as_ref(&self) -> &[T] {
        // SAFETY: import_buffer checked range, alignment and primitive type.
        // buffer retains its immutable owner for this entire slice lifetime.
        unsafe { std::slice::from_raw_parts(self.address as *const T, self.elements) }
    }
}

fn validate_buffer<T: Element>(buffer: &FfiBuffer) -> Result<(usize, usize)> {
    if buffer.owner.element() != T::KIND {
        return Err("native buffer element mismatch".into());
    }
    let view = buffer.owner.view();
    let end = buffer
        .offset
        .checked_add(buffer.byte_len)
        .ok_or_else(|| RString::from("native range overflow"))?;
    let size = std::mem::size_of::<T>() as u64;
    if end > view.len() as u64
        || end > isize::MAX as u64
        || !buffer.offset.is_multiple_of(size)
        || !buffer.byte_len.is_multiple_of(size)
    {
        return Err("invalid native buffer range".into());
    }
    let address = (view.as_ptr() as usize)
        .checked_add(buffer.offset as usize)
        .ok_or_else(|| RString::from("native address overflow"))?;
    if !address.is_multiple_of(std::mem::align_of::<T>()) {
        return Err("unaligned native buffer".into());
    }
    Ok((address, (buffer.byte_len / size) as usize))
}

fn import_buffer<T: Element>(buffer: FfiBuffer) -> Result<SharedBuffer<T>> {
    let (address, elements) = validate_buffer::<T>(&buffer)?;
    if let Ok(source) = buffer.owner.obj.downcast_as::<SourceOwner>() {
        let storage = source
            .storage
            .get()
            .ok_or_else(|| RString::from("unpublished native owner"))?;
        let data = T::get(storage).ok_or_else(|| RString::from("native owner type mismatch"))?;
        let start = buffer.offset as usize / std::mem::size_of::<T>();
        return data.slice(start..start + elements).map_err(error);
    }
    Ok(SharedBuffer::from_owner(ImportedOwner::<T> {
        buffer,
        address,
        elements,
        marker: PhantomData,
    }))
}

fn export_buffer<T: Element>(data: SharedBuffer<T>) -> Result<FfiBuffer> {
    let range = data.storage_range();
    let size = std::mem::size_of::<T>() as u64;
    let byte_len = data.len() as u64 * size;
    if let Some(owner) = data.owner::<ImportedOwner<T>>() {
        return Ok(FfiBuffer {
            owner: owner.buffer.owner.clone(),
            offset: owner
                .buffer
                .offset
                .checked_add(range.start as u64 * size)
                .ok_or_else(|| RString::from("native range overflow"))?,
            byte_len,
        });
    }
    if let Some(owner) = data.owner::<allocator::ImportedLoan<T>>() {
        return Ok(FfiBuffer {
            owner: owner.publish().clone(),
            offset: range.start as u64 * size,
            byte_len,
        });
    }
    let owner = SourceOwner {
        storage: OnceLock::from(T::wrap(data)),
    };
    Ok(FfiBuffer {
        owner: BufferOwner_TO::from_ptr(RArc::new(owner), TD_CanDowncast),
        offset: 0,
        byte_len,
    })
}
