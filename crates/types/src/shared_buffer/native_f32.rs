//! Checked native-endian byte views. No allocation is retyped for deallocation.
use super::{SharedBuffer, Storage};
use std::sync::Arc;

struct NativeF32(SharedBuffer<u8>);

impl AsRef<[f32]> for NativeF32 {
    fn as_ref(&self) -> &[f32] {
        // SAFETY: every f32 bit pattern is valid. align_to constructs only aligned
        // references; the shared immutable owner outlives this returned borrow.
        let (prefix, samples, suffix) = unsafe { self.0.as_slice().align_to::<f32>() };
        assert!(
            prefix.is_empty() && suffix.is_empty(),
            "byte owner changed its layout"
        );
        samples
    }
}

impl SharedBuffer<u8> {
    /// Retain aligned native-endian bytes as immutable f32 samples, without a
    /// payload copy. A non-multiple length or unaligned address returns the
    /// original owner unchanged. This performs no endian or numerical conversion.
    ///
    /// Views and slices retain all original budget reservations and backing-byte
    /// charges. They cannot reopen a mutable loan. Allocation/forwarding proofs
    /// are deliberately not transferred across the element-type change; callers
    /// must satisfy the destination's typed/domain admission contract separately.
    pub fn try_into_native_f32(self) -> Result<SharedBuffer<f32>, Self> {
        // SAFETY: f32 permits all bit patterns; align_to partitions by alignment
        // and complete element widths without reading or changing payload bytes.
        let (prefix, samples, suffix) = unsafe { self.as_slice().align_to::<f32>() };
        if !prefix.is_empty() || !suffix.is_empty() {
            return Err(self);
        }
        let len = samples.len();
        let identity = self.storage_identity();
        let backing_bytes = self.backing_bytes();
        let mut provenance = crate::provenance::Provenance::default();
        provenance.budget = self.provenance.budget.clone();
        Ok(SharedBuffer {
            storage: Storage::View {
                owner: Arc::new(NativeF32(self)),
                identity,
                backing_bytes,
            },
            range: 0..len,
            provenance,
        })
    }
}
