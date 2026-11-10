use remotemedia_types::{
    BoundOutputAllocator, BufferAllocationError, BufferLoan, ForwardingScope, HeapOutputAllocator,
    OutputAllocator, RuntimeData, SharedBuffer, TensorEncoding,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[test]
fn allocation_binding_is_exact_and_survives_views_and_unique_reuse() {
    let provider = Arc::new(HeapOutputAllocator);
    let selected = BoundOutputAllocator::new(provider.clone());
    let other = BoundOutputAllocator::new(provider);
    let mut loan = selected.allocate_bytes(16).unwrap();
    loan.fill(17);
    let pointer = loan.as_ptr();
    let published = loan.freeze();
    let data = RuntimeData::Binary(published.slice(3..8).unwrap());
    assert!(selected.owns_payload(&data));
    assert!(!other.owns_payload(&data));
    assert!(!selected.owns_payload(&RuntimeData::Binary(vec![17; 5].into())));
    drop(data);
    let mut recycled = published.try_into_loan().unwrap();
    recycled[0] = 22;
    let data = RuntimeData::Binary(recycled.freeze());
    assert!(selected.owns_payload(&data));
    let (meta, storage) = data.into_shared_parts(TensorEncoding::Runtime).unwrap();
    let data = meta.attach(storage, TensorEncoding::Runtime).unwrap();
    assert!(selected.owns_payload(&data));
    let RuntimeData::Binary(buffer) = data else {
        unreachable!()
    };
    assert_eq!(buffer.as_ptr(), pointer);
    // Even a no-copy Vec extraction intentionally loses the bound-provider proof.
    let values = buffer.into_vec();
    assert_eq!(values.as_ptr(), pointer);
    assert!(!selected.owns_payload(&RuntimeData::Binary(values.into())));
}

#[test]
fn forwarding_is_view_scoped_and_survives_nested_nodes_and_thread_handoffs() {
    let first = ForwardingScope::default();
    let nested = ForwardingScope::default();
    let unrelated = ForwardingScope::default();
    let original: SharedBuffer<u8> = vec![1, 2, 3, 4, 5].into();
    let accepted = first.accept(RuntimeData::Binary(original.slice(1..4).unwrap()));
    assert!(first.accepts_output(&accepted));
    assert!(!first.accepts_output(&RuntimeData::Binary(original.clone())));
    let accepted = nested.accept(accepted);
    assert!(first.accepts_output(&accepted));
    assert!(nested.accepts_output(&accepted));
    assert!(!unrelated.accepts_output(&accepted));
    let RuntimeData::Binary(buffer) = accepted else {
        unreachable!()
    };
    assert!(buffer.slice(0..4).is_err());
    let view = buffer.slice(1..2).unwrap();
    assert!(view.shares_storage(&original));
    drop(buffer);
    drop(original);
    std::thread::spawn(move || {
        let data = RuntimeData::Binary(view);
        assert!(first.accepts_output(&data));
        assert!(nested.accepts_output(&data));
        assert!(!unrelated.accepts_output(&data));
        assert_eq!(data.bulk_payload_bytes(), 1);
    })
    .join()
    .unwrap();
}

#[test]
fn serialization_and_empty_private_storage_cannot_forge_provenance() {
    let allocator = BoundOutputAllocator::new(Arc::new(HeapOutputAllocator));
    let scope = ForwardingScope::default();
    for size in [0, 16] {
        let data = scope.accept(RuntimeData::Binary(
            allocator.allocate_bytes(size).unwrap().freeze(),
        ));
        let encoded = rmp_serde::to_vec_named(&data).unwrap();
        let decoded: RuntimeData = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded, data);
        assert!(!scope.accepts_output(&decoded));
        assert!(!allocator.owns_payload(&decoded));
        assert!(scope.accepts_output(&data));
        assert!(allocator.owns_payload(&data));
    }
    assert!(allocator.owns_payload(&RuntimeData::Text("metadata".into())));
}

#[test]
fn legacy_arc_audio_admission_preserves_the_sample_allocation() {
    let samples: Arc<[f32]> = Arc::from([0.25, 0.5]);
    let pointer = samples.as_ptr();
    let scope = ForwardingScope::default();
    let data = scope.accept(RuntimeData::Audio {
        samples: remotemedia_types::AudioSamples::Arc(samples),
        sample_rate: 48000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    });
    assert!(scope.accepts_output(&data));
    let RuntimeData::Audio { samples, .. } = data else {
        unreachable!()
    };
    assert_eq!(samples.as_ptr(), pointer);
    assert!(matches!(
        samples,
        remotemedia_types::AudioSamples::Shared(_)
    ));
}

struct Owned {
    data: Vec<u8>,
    released: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for Owned {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
impl AsMut<[u8]> for Owned {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}
impl Drop for Owned {
    fn drop(&mut self) {
        self.released.fetch_add(1, Ordering::SeqCst);
    }
}
struct External(Arc<AtomicUsize>);
impl OutputAllocator for External {
    fn allocate_bytes(&self, len: usize) -> Result<BufferLoan<u8>, BufferAllocationError> {
        if len > 8 {
            return Err(BufferAllocationError::Exhausted);
        }
        Ok(BufferLoan::from_owner(Owned {
            data: vec![0; len],
            released: self.0.clone(),
        }))
    }
    fn allocate_audio(&self, _: usize) -> Result<BufferLoan<f32>, BufferAllocationError> {
        Err(BufferAllocationError::Unavailable)
    }
}
#[test]
fn binding_preserves_provider_exhaustion_and_last_owner_release() {
    let releases = Arc::new(AtomicUsize::new(0));
    let allocator = BoundOutputAllocator::new(Arc::new(External(releases.clone())))
        .with_budget(remotemedia_types::BufferBudget::new(1, 8));
    assert_eq!(
        allocator.allocate_bytes(9).unwrap_err(),
        BufferAllocationError::Exhausted
    );
    assert_eq!(
        allocator.allocate_audio(1).unwrap_err(),
        BufferAllocationError::Unavailable
    );
    let mut loan = allocator.allocate_bytes(8).unwrap();
    loan[1] = 9;
    let original = loan.freeze();
    assert!(original.owner::<Owned>().is_some());
    let view = original.slice(1..2).unwrap();
    let scope = ForwardingScope::default();
    let output = scope.accept(RuntimeData::Binary(view));
    assert!(allocator.owns_payload(&output));
    drop(original);
    drop(allocator);
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    drop(output);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
    // Retaining the scope itself never pins payload storage.
    drop(scope);
}

#[test]
fn nested_admission_branches_release_payload_once_after_final_view() {
    let releases = Arc::new(AtomicUsize::new(0));
    let owner = SharedBuffer::from_owner(Owned {
        data: vec![3; 8],
        released: releases.clone(),
    });
    let first = ForwardingScope::default();
    let mut output = first.accept(RuntimeData::Binary(owner));
    let branch = output.clone();
    for _ in 0..1024 {
        output = ForwardingScope::default().accept(output);
    }
    assert!(first.accepts_output(&output));
    drop(output);
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    assert!(first.accepts_output(&branch));
    drop(branch);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}
