use remotemedia_types::{
    BoundOutputAllocator, BufferBudget, CopyContext, CopyPolicy, ForwardingScope,
    HeapOutputAllocator, OutputAllocator, RuntimeData, SharedBuffer,
};
use std::sync::Arc;

fn audio(samples: SharedBuffer<f32>) -> RuntimeData {
    RuntimeData::Audio {
        samples: samples.into(),
        sample_rate: 48000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    }
}

#[test]
fn native_view_retains_bits_address_and_original_backing_budget() {
    let mut bytes = Vec::with_capacity(256);
    for value in [0.0f32, -0.0, f32::from_bits(0x7fc01234), f32::INFINITY] {
        bytes.extend_from_slice(&value.to_ne_bytes());
    }
    let capacity = bytes.capacity();
    let source: SharedBuffer<u8> = bytes.into();
    let address = source.as_ptr() as usize;
    let view = source.clone().try_into_native_f32().unwrap();
    assert_eq!(view.as_ptr() as usize, address);
    assert_eq!(view[1].to_bits(), (-0.0f32).to_bits());
    assert_eq!(view[2].to_bits(), 0x7fc01234);
    let too_small = BufferBudget::new(1, 16);
    assert!(too_small.admit(audio(view.clone())).is_err());
    let budget = BufferBudget::new(1, capacity as u64);
    let first = budget.admit(RuntimeData::Binary(source.clone())).unwrap();
    let second = budget.admit(audio(view.slice(1..2).unwrap())).unwrap();
    assert_eq!(budget.usage().owners, 1);
    assert_eq!(budget.usage().bytes, capacity as u64);
    drop((first, source, view));
    assert_eq!(budget.usage().owners, 1);
    std::thread::spawn(move || drop(second)).join().unwrap();
    assert_eq!(budget.usage().owners, 0);
}

#[test]
fn typed_views_keep_credit_but_do_not_forge_typed_allocation_or_forwarding_proof() {
    let budget = BufferBudget::new(1, 32);
    let allocator =
        BoundOutputAllocator::new(Arc::new(HeapOutputAllocator)).with_budget(budget.clone());
    let scope = ForwardingScope::default();
    let source = scope.accept(RuntimeData::Binary(
        allocator.allocate_bytes(32).unwrap().freeze(),
    ));
    assert!(allocator.owns_payload(&source));
    let RuntimeData::Binary(bytes) = source else {
        unreachable!()
    };
    let view = bytes.try_into_native_f32().unwrap();
    assert_eq!(budget.usage().bytes, 32);
    let converted = audio(view.clone());
    assert!(!allocator.owns_payload(&converted));
    assert!(!scope.accepts_output(&converted));
    assert!(view.clone().try_into_loan().is_err());
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "typed-view");
    assert!(view.clone().try_into_vec(&strict).is_err());
    drop(converted);
    drop(view);
    assert_eq!(budget.usage().owners, 0);
}

#[test]
fn incompatible_layout_returns_original_owner_and_separate_views_cannot_join() {
    let source: SharedBuffer<u8> = vec![0; 32].into();
    let unaligned = source.slice(1..9).unwrap();
    let address = unaligned.as_ptr();
    let returned = unaligned.try_into_native_f32().unwrap_err();
    assert_eq!(returned.as_ptr(), address);
    assert!(source.slice(0..3).unwrap().try_into_native_f32().is_err());
    let first = source.slice(0..16).unwrap().try_into_native_f32().unwrap();
    let other = source.slice(8..24).unwrap().try_into_native_f32().unwrap();
    assert!(first
        .slice(0..2)
        .unwrap()
        .try_join(&other.slice(2..4).unwrap())
        .is_none());
    let joined = first
        .slice(0..2)
        .unwrap()
        .try_join(&first.slice(2..4).unwrap())
        .unwrap();
    assert_eq!(joined.len(), 4);
    assert!(joined.shares_storage(&first));
    assert!(source
        .slice(0..0)
        .unwrap()
        .try_into_native_f32()
        .unwrap()
        .is_empty());
}
