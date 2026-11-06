use crate::broker::Broker;
use crate::{
    AllocationId, BufferDescriptor, ElementType, PeerId, Result, SessionId, SharedMemoryError,
};
use memmap2::{Mmap, MmapMut, MmapOptions};
use remotemedia_types::{BufferAllocationError, BufferLoan, OutputAllocator, SharedBuffer};
use std::fs::File;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};

mod sealed {
    pub trait Sealed {}
    impl Sealed for u8 {}
    impl Sealed for f32 {}
}

/// Only plain primitives with valid representations for every bit pattern.
pub trait MappedElement: sealed::Sealed + Copy + Send + Sync + 'static {
    const ELEMENT: ElementType;
}
impl MappedElement for u8 {
    const ELEMENT: ElementType = ElementType::Bytes;
}
impl MappedElement for f32 {
    const ELEMENT: ElementType = ElementType::F32Le;
}

#[derive(Debug, Clone)]
pub struct SizeClass {
    pub bytes: usize,
    pub slots: usize,
}

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub classes: Vec<SizeClass>,
    pub max_bytes: usize,
    pub max_leases: usize,
}

pub(crate) struct Inner {
    pub(crate) session: SessionId,
    pub(crate) files: Vec<File>,
    broker: Mutex<Broker>,
}

impl Inner {
    pub(crate) fn broker(&self) -> MutexGuard<'_, Broker> {
        // State transitions do not call user code while locked. A poisoned lock
        // is nevertheless never recovered into potentially reusable storage.
        self.broker.lock().expect("shared-memory broker poisoned")
    }
}

/// Fixed, bounded size classes of private file-backed shared mappings.
/// Windows and Unix use the same memmap2 mapping contract. Anonymous temporary
/// file handles are passed out of band; descriptors never contain a file path.
/// Creation and allocation may block/allocate and are not audio-callback APIs.
#[derive(Clone)]
pub struct SharedPool {
    pub(crate) inner: Arc<Inner>,
}

impl SharedPool {
    pub fn session_id(&self) -> SessionId {
        self.inner.session
    }

    /// `session` must identify this pool incarnation uniquely on its channel.
    pub fn new(session: SessionId, config: PoolConfig) -> Result<Self> {
        let mut total = 0usize;
        let mut count = 0usize;
        for class in &config.classes {
            if class.bytes == 0 || class.slots == 0 || class.bytes > isize::MAX as usize {
                return Err(SharedMemoryError::InvalidLayout);
            }
            total = class
                .bytes
                .checked_mul(class.slots)
                .and_then(|n| n.checked_add(total))
                .ok_or(SharedMemoryError::InvalidLayout)?;
            count = count
                .checked_add(class.slots)
                .ok_or(SharedMemoryError::InvalidLayout)?;
        }
        if total > config.max_bytes || count > u32::MAX as usize {
            return Err(SharedMemoryError::Exhausted);
        }
        let capacities: Vec<_> = config
            .classes
            .iter()
            .flat_map(|c| std::iter::repeat_n(c.bytes as u64, c.slots))
            .collect();
        let broker = Broker::new(session, &capacities, config.max_leases)?;
        let mut files = Vec::with_capacity(count);
        for capacity in capacities {
            // tempfile uses private permissions and removes the directory entry
            // (Unix) or deletes on last handle close (Windows). No shared path is
            // ever accepted from an IPC message.
            let file = tempfile::tempfile()?;
            file.set_len(capacity)?;
            files.push(file);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                session,
                files,
                broker: Mutex::new(broker),
            }),
        })
    }

    pub fn allocate<T: MappedElement>(&self, elements: usize) -> Result<BufferLoan<T>> {
        let bytes = elements
            .checked_mul(std::mem::size_of::<T>())
            .filter(|&n| n <= isize::MAX as usize)
            .ok_or(SharedMemoryError::InvalidLayout)?;
        let (id, map) = self.reserve_initialized(bytes, T::ELEMENT)?;
        Ok(BufferLoan::from_owner(Writer::<T> {
            map: Some(map),
            pool: self.inner.clone(),
            id,
            elements,
            marker: PhantomData,
        }))
    }

    pub(crate) fn reserve_initialized(
        &self,
        bytes: usize,
        element: ElementType,
    ) -> Result<(AllocationId, MmapMut)> {
        let id = self.inner.broker().allocate(bytes as u64, element)?;
        // SAFETY: the private broker grants the only writer. The previous
        // generation has no local owner or delivery lease, so no readers exist.
        // Files have fixed size and no writable File escapes through safe APIs.
        let map = unsafe { MmapOptions::new().map_mut(&self.inner.files[id.slot as usize]) };
        let mut map = match map {
            Ok(map) => map,
            Err(err) => {
                self.inner.broker().release_local(id)?;
                return Err(err.into());
            }
        };
        // All exposed elements are initialized; never expose a previous frame.
        map[..bytes].fill(0);
        Ok((id, map))
    }

    /// Reserve a distinct recipient lease on the exact published storage range.
    /// Dropping an unsent Delivery cancels it. Once dispatched, loss of an ACK
    /// leaves it reserved until final release or confirmed process termination.
    pub fn export<T: MappedElement>(
        &self,
        buffer: &SharedBuffer<T>,
        recipient: PeerId,
    ) -> Result<Delivery> {
        let range = buffer.storage_range();
        let size = std::mem::size_of::<T>() as u64;
        let offset = range.start as u64 * size;
        let bytes = buffer.len() as u64 * size;
        let (file, descriptor) = if let Some(owner) = buffer.owner::<Writer<T>>() {
            if !Arc::ptr_eq(&owner.pool, &self.inner) {
                return Err(SharedMemoryError::WrongSession);
            }
            let file = self.inner.files[owner.id.slot as usize].try_clone()?;
            let mut broker = self.inner.broker();
            broker.publish(owner.id)?;
            (file, broker.grant(owner.id, recipient, offset, bytes)?)
        } else if let Some(owner) = buffer.owner::<crate::remote::Publication<T>>() {
            if !Arc::ptr_eq(&owner.pool, &self.inner) {
                return Err(SharedMemoryError::WrongSession);
            }
            let file = self.inner.files[owner.id.slot as usize].try_clone()?;
            let descriptor = self
                .inner
                .broker()
                .grant(owner.id, recipient, offset, bytes)?;
            (file, descriptor)
        } else if let Some(owner) = buffer.owner::<Reader<T>>() {
            if owner.descriptor.session != self.inner.session {
                return Err(SharedMemoryError::WrongSession);
            }
            let file = self
                .inner
                .files
                .get(owner.descriptor.allocation.slot as usize)
                .ok_or(SharedMemoryError::StaleAllocation)?
                .try_clone()?;
            let offset = owner
                .descriptor
                .offset
                .checked_add(offset)
                .ok_or(SharedMemoryError::InvalidLayout)?;
            let descriptor =
                self.inner
                    .broker()
                    .forward(&owner.descriptor, recipient, offset, bytes)?;
            (file, descriptor)
        } else {
            return Err(SharedMemoryError::PrivateStorage);
        };
        Ok(Delivery {
            pool: self.inner.clone(),
            file: Some(file),
            descriptor: Some(descriptor),
        })
    }

    pub fn accept(&self, descriptor: &BufferDescriptor) -> Result<()> {
        self.inner.broker().accept(descriptor)
    }

    /// Forward an accepted lease without mapping or reading its payload locally.
    /// The requested byte range is relative to the source descriptor.
    pub fn forward(
        &self,
        source: &BufferDescriptor,
        recipient: PeerId,
        offset: u64,
        byte_len: u64,
    ) -> Result<Delivery> {
        let offset = source
            .offset
            .checked_add(offset)
            .ok_or(SharedMemoryError::InvalidLayout)?;
        let file = self
            .inner
            .files
            .get(source.allocation.slot as usize)
            .ok_or(SharedMemoryError::StaleAllocation)?
            .try_clone()?;
        let descriptor = self
            .inner
            .broker()
            .forward(source, recipient, offset, byte_len)?;
        Ok(Delivery {
            pool: self.inner.clone(),
            file: Some(file),
            descriptor: Some(descriptor),
        })
    }

    /// Reclaim a remotely delivered lease after its final owner is gone.
    ///
    /// # Safety
    /// The authenticated recipient must have unmapped this lease and all views,
    /// or its exact process incarnation must have terminated. A timeout or loss
    /// of connection is insufficient. Premature release permits concurrent reuse.
    pub unsafe fn release_remote(&self, descriptor: &BufferDescriptor) -> Result<()> {
        self.inner.broker().release_remote(descriptor)
    }

    /// Release all leases held by a confirmed-dead process incarnation.
    ///
    /// # Safety
    /// Verify termination using the original OS process handle/child, not PID
    /// lookup, heartbeat expiry, or an untrusted control message.
    pub unsafe fn release_dead_peer(&self, peer: PeerId) {
        self.inner.broker().release_dead_peer(peer);
    }

    /// Stop new allocations and forwarding; existing readers and releases work.
    pub fn close(&self) {
        self.inner.broker().close();
    }
}

fn allocation_error(error: SharedMemoryError) -> BufferAllocationError {
    match error {
        SharedMemoryError::InvalidLayout => BufferAllocationError::InvalidLength,
        SharedMemoryError::Exhausted => BufferAllocationError::Exhausted,
        _ => BufferAllocationError::Unavailable,
    }
}

impl OutputAllocator for SharedPool {
    fn allocate_bytes(
        &self,
        len: usize,
    ) -> std::result::Result<BufferLoan<u8>, BufferAllocationError> {
        self.allocate(len).map_err(allocation_error)
    }
    fn allocate_audio(
        &self,
        samples: usize,
    ) -> std::result::Result<BufferLoan<f32>, BufferAllocationError> {
        self.allocate(samples).map_err(allocation_error)
    }
}

struct Writer<T> {
    map: Option<MmapMut>,
    pool: Arc<Inner>,
    id: AllocationId,
    elements: usize,
    marker: PhantomData<T>,
}
impl<T: MappedElement> AsRef<[T]> for Writer<T> {
    fn as_ref(&self) -> &[T] {
        // SAFETY: map base is page-aligned; allocation checked the element count
        // and initialized every byte. T is sealed to u8/f32 with all bit patterns valid.
        unsafe {
            std::slice::from_raw_parts(self.map.as_ref().unwrap().as_ptr().cast(), self.elements)
        }
    }
}
impl<T: MappedElement> AsMut<[T]> for Writer<T> {
    fn as_mut(&mut self) -> &mut [T] {
        // SAFETY: BufferLoan exclusively owns this writer; after freeze there is
        // no AsMut path and external owners cannot be reopened by Arc uniqueness.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.map.as_mut().unwrap().as_mut_ptr().cast(),
                self.elements,
            )
        }
    }
}
impl<T> Drop for Writer<T> {
    fn drop(&mut self) {
        drop(self.map.take());
        let _ = self.pool.broker().release_local(self.id);
    }
}

pub struct Delivery {
    pool: Arc<Inner>,
    file: Option<File>,
    descriptor: Option<BufferDescriptor>,
}
impl Delivery {
    pub fn descriptor(&self) -> &BufferDescriptor {
        self.descriptor.as_ref().unwrap()
    }

    /// Hand the private backing handle and descriptor to a trusted transport.
    ///
    /// # Safety
    /// The transport must preserve the pending lease until acknowledgement or
    /// verified non-delivery. Recipients must never write/truncate the handle and
    /// must map read-only. Do not expose it to arbitrary file I/O or other peers.
    pub unsafe fn into_transport(mut self) -> (File, BufferDescriptor) {
        (self.file.take().unwrap(), self.descriptor.take().unwrap())
    }
}
impl Drop for Delivery {
    fn drop(&mut self) {
        if let Some(descriptor) = self.descriptor.take() {
            drop(self.file.take());
            let _ = self.pool.broker().release_remote(&descriptor);
        }
    }
}

struct Reader<T> {
    map: Mmap,
    descriptor: BufferDescriptor,
    // Fields drop in declaration order: unmap before notifying the broker.
    _release: ReleaseGuard,
    marker: PhantomData<T>,
}

#[cfg(feature = "worker-protocol")]
pub(crate) fn reader_descriptor<T: MappedElement>(
    buffer: &SharedBuffer<T>,
) -> Option<&BufferDescriptor> {
    buffer.owner::<Reader<T>>().map(|owner| &owner.descriptor)
}
impl<T: MappedElement> AsRef<[T]> for Reader<T> {
    fn as_ref(&self) -> &[T] {
        let start = self.descriptor.offset as usize;
        let len = self.descriptor.byte_len as usize / std::mem::size_of::<T>();
        // SAFETY: import checked type, alignment, bounds and fixed mapping size.
        // The transport's lease contract keeps these bytes immutable until Drop.
        unsafe { std::slice::from_raw_parts(self.map[start..].as_ptr().cast(), len) }
    }
}
struct ReleaseGuard(Option<Box<dyn FnOnce() + Send + Sync>>);
impl Drop for ReleaseGuard {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            release();
        }
    }
}

/// Import a read-only owner retaining its lease through all clones and slices.
/// The release callback also runs on a failed import, after any partial mapping
/// is dropped, so a rejected descriptor cannot strand its delivery lease.
///
/// # Safety
/// The authenticated channel must bind `file` to this accepted descriptor and
/// keep its generation immutable until `release` runs. No party may truncate or
/// mutate the file while any imported view exists. The callback must release only
/// this lease, and the recipient must not separately release it while views live.
pub unsafe fn import<T: MappedElement>(
    file: File,
    descriptor: BufferDescriptor,
    session: SessionId,
    release: impl FnOnce() + Send + Sync + 'static,
) -> Result<SharedBuffer<T>> {
    let release = ReleaseGuard(Some(Box::new(release)));
    descriptor.validate(session, file.metadata()?.len())?;
    if descriptor.element != T::ELEMENT {
        return Err(SharedMemoryError::InvalidLayout);
    }
    // SAFETY: fixed-size, immutable generation guaranteed by the caller above.
    let map = unsafe { MmapOptions::new().map(&file)? };
    if !(map.as_ptr() as usize + descriptor.offset as usize)
        .is_multiple_of(std::mem::align_of::<T>())
    {
        return Err(SharedMemoryError::InvalidLayout);
    }
    Ok(SharedBuffer::from_owner(Reader::<T> {
        map,
        descriptor,
        _release: release,
        marker: PhantomData,
    }))
}
