//! Host-selected loans remain unpublished while the authenticated worker writes.
//! The pending loan is never read or exposed until immutable publication. Its
//! original owner and provenance are moved into the returned shared view.
use super::*;
use remotemedia_types::{OutputAllocator, PayloadStorage};

pub(crate) enum PendingPublication {
    Bytes(BufferLoan<u8>),
    Audio(BufferLoan<f32>),
}

impl PendingPublication {
    /// # Safety
    /// The worker has revoked every writable view of this exact producer grant.
    pub(crate) unsafe fn publish(
        self,
        pool: &SharedPool,
        descriptor: &WriteDescriptor,
    ) -> Result<PayloadStorage> {
        pool.inner.broker().publish_remote(descriptor)?;
        Ok(match self {
            Self::Bytes(loan) => PayloadStorage::Bytes(loan.freeze()),
            Self::Audio(loan) => PayloadStorage::Audio(loan.freeze()),
        })
    }
}

impl SharedPool {
    /// Allocate through the caller's selected provider, then transfer its exact
    /// loan. Private/foreign storage fails instead of falling back to this pool.
    pub(crate) fn allocate_remote_with(
        &self,
        allocator: &dyn OutputAllocator,
        element: crate::ElementType,
        elements: usize,
        producer: PeerId,
    ) -> Result<WriteGrant> {
        use crate::ElementType;
        let (mut grant, publication) = match element {
            ElementType::Bytes => {
                let loan = allocator
                    .allocate_bytes(elements)
                    .map_err(allocation_error)?;
                if loan.len() != elements {
                    return Err(SharedMemoryError::InvalidLayout);
                }
                (
                    self.grant_loan(&loan, producer)?,
                    PendingPublication::Bytes(loan),
                )
            }
            ElementType::F32Le => {
                let loan = allocator
                    .allocate_audio(elements)
                    .map_err(allocation_error)?;
                if loan.len() != elements {
                    return Err(SharedMemoryError::InvalidLayout);
                }
                (
                    self.grant_loan(&loan, producer)?,
                    PendingPublication::Audio(loan),
                )
            }
        };
        grant.publication = Some(publication);
        Ok(grant)
    }

    fn grant_loan<T: MappedElement>(
        &self,
        loan: &BufferLoan<T>,
        producer: PeerId,
    ) -> Result<WriteGrant> {
        let id = self.loan_identity(loan)?;
        let file = self.inner.files[id.slot as usize].try_clone()?;
        let descriptor = self.inner.broker().handoff_writer(id, producer)?;
        Ok(WriteGrant {
            pool: self.inner.clone(),
            descriptor: Some(descriptor),
            file: Some(file),
            publication: None,
        })
    }
}

fn allocation_error(error: remotemedia_types::BufferAllocationError) -> SharedMemoryError {
    match error {
        remotemedia_types::BufferAllocationError::InvalidLength => SharedMemoryError::InvalidLayout,
        remotemedia_types::BufferAllocationError::Exhausted => SharedMemoryError::Exhausted,
        remotemedia_types::BufferAllocationError::Unavailable => SharedMemoryError::PrivateStorage,
    }
}
