//! Producer allocation independent of the host runtime and transport backend.

use crate::BufferLoan;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BufferAllocationError {
    #[error("output allocation length exceeds addressable memory")]
    InvalidLength,
    #[error("output allocator has insufficient capacity")]
    Exhausted,
    #[error("output allocation is unavailable in this execution context")]
    Unavailable,
}

/// Whether a provider has been validated for prepared real-time producers.
///
/// Prepared producers allocate loans on a worker thread and hand them to a
/// real-time callback, which only writes through the loan's mutable slice and
/// returns it. `Verified` means that slice access and the hand-back never
/// allocate, block or lock on the callback thread for this provider, as shown
/// by allocation-counting tests. Everything else is `Unverified`, and prepared
/// spawns reject it unless the caller opts in with
/// [`AssumePreparedRealtime`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedRealtimeSupport {
    Verified,
    Unverified,
}

/// Supplies initialized, exclusive output storage in the selected memory domain.
/// Providers must return an error on exhaustion, never overwrite a live buffer.
/// Allocation itself is not a real-time-safe operation; prepare loans beforehand.
/// The returned loan owns its release behavior independently of this allocator.
pub trait OutputAllocator: Send + Sync {
    fn allocate_bytes(&self, len: usize) -> Result<BufferLoan<u8>, BufferAllocationError>;
    fn allocate_audio(&self, samples: usize) -> Result<BufferLoan<f32>, BufferAllocationError>;

    /// Prepared real-time validation status. Unknown providers are unverified.
    fn prepared_realtime_support(&self) -> PreparedRealtimeSupport {
        PreparedRealtimeSupport::Unverified
    }
}

/// Explicit caller opt-in: treat `provider` as validated for prepared
/// real-time use. The caller takes responsibility for that validation.
pub struct AssumePreparedRealtime(pub std::sync::Arc<dyn OutputAllocator>);

impl OutputAllocator for AssumePreparedRealtime {
    fn allocate_bytes(&self, len: usize) -> Result<BufferLoan<u8>, BufferAllocationError> {
        self.0.allocate_bytes(len)
    }
    fn allocate_audio(&self, samples: usize) -> Result<BufferLoan<f32>, BufferAllocationError> {
        self.0.allocate_audio(samples)
    }
    fn prepared_realtime_support(&self) -> PreparedRealtimeSupport {
        PreparedRealtimeSupport::Verified
    }
}

/// Local heap allocation. This does not provide cross-process shareable storage.
#[derive(Debug, Default)]
pub struct HeapOutputAllocator;

fn allocate<T: Default + Clone>(len: usize) -> Result<BufferLoan<T>, BufferAllocationError> {
    len.checked_mul(std::mem::size_of::<T>())
        .filter(|&bytes| bytes <= isize::MAX as usize)
        .ok_or(BufferAllocationError::InvalidLength)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(len)
        .map_err(|_| BufferAllocationError::Exhausted)?;
    values.resize(len, T::default());
    Ok(values.into())
}

impl OutputAllocator for HeapOutputAllocator {
    fn allocate_bytes(&self, len: usize) -> Result<BufferLoan<u8>, BufferAllocationError> {
        allocate(len)
    }

    fn allocate_audio(&self, samples: usize) -> Result<BufferLoan<f32>, BufferAllocationError> {
        allocate(samples)
    }

    /// Prepared loans are plain heap storage: slice access never allocates.
    fn prepared_realtime_support(&self) -> PreparedRealtimeSupport {
        PreparedRealtimeSupport::Verified
    }
}
