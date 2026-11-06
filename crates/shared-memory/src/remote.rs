//! Exclusive out-of-process production. The broker retains a producer reservation
//! separately from local readers and delivery leases, including after publication.
use crate::pool::Inner;
use crate::{
    AllocationId, MappedElement, PeerId, Result, SessionId, SharedMemoryError, SharedPool,
    WriteDescriptor,
};
use memmap2::{Mmap, MmapMut, MmapOptions};
use remotemedia_types::{BufferLoan, SharedBuffer};
use std::{fs::File, marker::PhantomData, sync::Arc};

/// Undispatched exclusive grant. Dropping it cancels the reservation; dispatch
/// transfers reclamation responsibility to the authenticated worker connection.
pub struct WriteGrant {
    pool: Arc<Inner>,
    descriptor: Option<WriteDescriptor>,
    file: Option<File>,
}

impl WriteGrant {
    pub fn descriptor(&self) -> &WriteDescriptor {
        self.descriptor.as_ref().unwrap()
    }

    /// # Safety
    /// Transfer this private file only to the named producer incarnation. It may
    /// map/write only the initialized grant range and must not truncate the file,
    /// duplicate the grant or delegate it. Keep the reservation until final unmap
    /// or verified process death. A send error or disconnect is not proof of either.
    pub unsafe fn into_transport(mut self) -> (File, WriteDescriptor) {
        (self.file.take().unwrap(), self.descriptor.take().unwrap())
    }
}

impl Drop for WriteGrant {
    fn drop(&mut self) {
        if let Some(descriptor) = self.descriptor.take() {
            drop(self.file.take());
            let _ = self.pool.broker().release_producer(&descriptor);
        }
    }
}

impl SharedPool {
    /// Reserve initialized exclusive output storage for one worker. The host's
    /// initialization mapping is removed before this handle can be dispatched.
    pub fn allocate_remote<T: MappedElement>(
        &self,
        elements: usize,
        producer: PeerId,
    ) -> Result<WriteGrant> {
        let bytes = elements
            .checked_mul(std::mem::size_of::<T>())
            .filter(|&bytes| bytes <= isize::MAX as usize)
            .ok_or(SharedMemoryError::InvalidLayout)?;
        let (id, map) = self.reserve_initialized(bytes, T::ELEMENT)?;
        drop(map);
        let result = (|| {
            let file = self.inner.files[id.slot as usize].try_clone()?;
            let descriptor = self.inner.broker().handoff_writer(id, producer)?;
            Ok(WriteGrant {
                pool: self.inner.clone(),
                descriptor: Some(descriptor),
                file: Some(file),
            })
        })();
        if result.is_err() {
            let _ = self.inner.broker().release_local(id);
        }
        result
    }

    /// Acquire the host's first read owner over a worker-produced allocation.
    /// The worker may retain immutable views; its reservation remains independent.
    /// Repeated publication, changed descriptors and wrong primitive types fail.
    ///
    /// # Safety
    /// The authenticated producer must have consumed its exclusive loan and
    /// revoked every writable alias before requesting publication. No subsequent
    /// writer may touch this generation. The transport must bind this descriptor
    /// to that exact grant, never trust an arbitrary publication assertion.
    pub unsafe fn publish_remote<T: MappedElement>(
        &self,
        descriptor: &WriteDescriptor,
    ) -> Result<SharedBuffer<T>> {
        if descriptor.element != T::ELEMENT {
            return Err(SharedMemoryError::InvalidLayout);
        }
        let mut broker = self.inner.broker();
        broker.validate_writer(descriptor)?;
        let file = &self.inner.files[descriptor.allocation.slot as usize];
        // SAFETY: caller established immutable publication of this exact grant.
        let map = unsafe { MmapOptions::new().map(file)? };
        broker.publish_remote(descriptor)?;
        drop(broker);
        Ok(SharedBuffer::from_owner(Publication::<T> {
            map: Some(map),
            pool: self.inner.clone(),
            id: descriptor.allocation,
            elements: descriptor.byte_len as usize / std::mem::size_of::<T>(),
            marker: PhantomData,
        }))
    }

    /// # Safety
    /// The producer must have unmapped its exclusive loan and every frozen view,
    /// or the original OS process handle must prove death. Do not release merely
    /// because it published an output, was cancelled, timed out or disconnected.
    pub unsafe fn release_producer(&self, descriptor: &WriteDescriptor) -> Result<()> {
        self.inner.broker().release_producer(descriptor)
    }
}

pub(crate) struct Publication<T> {
    map: Option<Mmap>,
    pub(crate) pool: Arc<Inner>,
    pub(crate) id: AllocationId,
    elements: usize,
    marker: PhantomData<T>,
}
impl<T: MappedElement> AsRef<[T]> for Publication<T> {
    fn as_ref(&self) -> &[T] {
        // SAFETY: page-aligned, initialized typed reservation; immutable
        // publication and the broker's local owner prevent concurrent reuse.
        unsafe {
            std::slice::from_raw_parts(self.map.as_ref().unwrap().as_ptr().cast(), self.elements)
        }
    }
}
impl<T> Drop for Publication<T> {
    fn drop(&mut self) {
        drop(self.map.take());
        let _ = self.pool.broker().release_local(self.id);
    }
}

struct Release(Option<Box<dyn FnOnce() + Send + Sync>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            release();
        }
    }
}
struct RemoteWriter<T> {
    // Unmap before notifying the producer's broker, including abandoned loans.
    map: MmapMut,
    descriptor: WriteDescriptor,
    _release: Release,
    marker: PhantomData<T>,
}
impl<T: MappedElement> AsRef<[T]> for RemoteWriter<T> {
    fn as_ref(&self) -> &[T] {
        // SAFETY: import validated the page-aligned initialized grant. The
        // immutable buffer retains this writer and its reservation after freeze.
        unsafe {
            std::slice::from_raw_parts(
                self.map.as_ptr().cast(),
                self.descriptor.byte_len as usize / std::mem::size_of::<T>(),
            )
        }
    }
}
impl<T: MappedElement> AsMut<[T]> for RemoteWriter<T> {
    fn as_mut(&mut self) -> &mut [T] {
        // SAFETY: BufferLoan is exclusive and consuming freeze removes AsMut.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.map.as_mut_ptr().cast(),
                self.descriptor.byte_len as usize / std::mem::size_of::<T>(),
            )
        }
    }
}

/// Recover provenance only from a frozen worker loan. Slices preserve the full
/// reservation identity; their `storage_range()` supplies the restricted view.
pub fn writer_descriptor<T: MappedElement>(buffer: &SharedBuffer<T>) -> Option<&WriteDescriptor> {
    buffer
        .owner::<RemoteWriter<T>>()
        .map(|owner| &owner.descriptor)
}

/// Import an initialized exclusive producer grant. Freeze keeps the same mapping
/// and address, and the final owner drop acknowledges unmap through `release`.
/// Failed imports also acknowledge after discarding any partial mapping.
///
/// # Safety
/// An authenticated broker must have bound this file to the exact descriptor and
/// reserved it exclusively for this process. No other party may read, write,
/// truncate or recycle it until publication/final unmap. Import each grant once;
/// do not create extra writable mappings or expose arbitrary file I/O. `release` must acknowledge only
/// this grant and must never run separately while an owner remains alive.
pub unsafe fn import_writer<T: MappedElement>(
    file: File,
    descriptor: WriteDescriptor,
    session: SessionId,
    producer: PeerId,
    release: impl FnOnce() + Send + Sync + 'static,
) -> Result<BufferLoan<T>> {
    let release = Release(Some(Box::new(release)));
    descriptor.validate(session, producer, file.metadata()?.len())?;
    if descriptor.element != T::ELEMENT {
        return Err(SharedMemoryError::InvalidLayout);
    }
    // SAFETY: caller bound the fixed file to an exclusive initialized grant.
    let map = unsafe { MmapOptions::new().map_mut(&file)? };
    Ok(BufferLoan::from_owner(RemoteWriter::<T> {
        map,
        descriptor,
        _release: release,
        marker: PhantomData,
    }))
}
