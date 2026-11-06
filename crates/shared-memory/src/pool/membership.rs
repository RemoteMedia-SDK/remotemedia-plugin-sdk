//! Host-side domain checks use concrete pool ownership, never a session label.
use super::{MappedElement, SharedBuffer, SharedPool, Writer};
use remotemedia_types::{AudioSamples, RuntimeData};
use std::sync::Arc;

impl SharedPool {
    /// Whether this immutable view retains a host owner from this exact pool.
    /// This reads no payload, creates no mapping/lease and works after close.
    /// Raw imported readers are not certified: their descriptor's session label
    /// alone cannot establish ownership by this particular broker instance.
    pub fn owns_buffer<T: MappedElement>(&self, buffer: &SharedBuffer<T>) -> bool {
        if let Some(owner) = buffer.owner::<Writer<T>>() {
            return Arc::ptr_eq(&self.inner, &owner.pool);
        }
        buffer
            .owner::<crate::remote::Publication<T>>()
            .is_some_and(|owner| Arc::ptr_eq(&self.inner, &owner.pool))
    }

    /// Check host-owned bulk storage without cloning legacy audio or reading
    /// media bytes. Metadata-only messages need no storage-domain capability.
    pub fn owns_payload(&self, data: &RuntimeData) -> bool {
        match data {
            RuntimeData::Audio {
                samples: AudioSamples::Shared(samples),
                ..
            } => self.owns_buffer(samples),
            RuntimeData::Audio { .. } => false,
            RuntimeData::Video { pixel_data, .. } => self.owns_buffer(pixel_data),
            RuntimeData::Image { data, .. }
            | RuntimeData::Tensor { data, .. }
            | RuntimeData::Numpy { data, .. }
            | RuntimeData::Binary(data) => self.owns_buffer(data),
            RuntimeData::Text(_)
            | RuntimeData::Json(_)
            | RuntimeData::File { .. }
            | RuntimeData::ControlMessage { .. } => true,
        }
    }
}
