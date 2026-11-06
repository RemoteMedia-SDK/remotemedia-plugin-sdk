use remotemedia_types::{
    AudioSamples, BoundOutputAllocator, BufferBudget, BufferBudgetUsage, HeapOutputAllocator,
    OutputAllocator, RuntimeData, SharedBuffer,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[path = "support/budget_checks.rs"]
mod checks;
use checks::{hold_concurrently, usage};
fn input(bytes: usize) -> RuntimeData {
    RuntimeData::Binary(vec![7; bytes].into())
}
fn audio(samples: AudioSamples) -> RuntimeData {
    RuntimeData::Audio {
        samples,
        sample_rate: 48000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    }
}

#[test]
fn repeated_admission_and_empty_slices_share_full_backing_reservation() {
    let budget = BufferBudget::new(1, 64);
    let mut values = Vec::with_capacity(64);
    values.extend_from_slice(&[7; 8]);
    let original = SharedBuffer::from(values);
    let first = budget.admit(RuntimeData::Binary(original.clone())).unwrap();
    let second = budget
        .admit(RuntimeData::Binary(original.slice(8..8).unwrap()))
        .unwrap();
    let third = budget.admit(first.clone()).unwrap();
    usage(&budget, 1, 64);
    let RuntimeData::Binary(shared) = &first else {
        unreachable!()
    };
    assert!(shared.shares_storage(&original));
    assert!(budget.admit(input(1)).is_err());
    drop((first, second));
    usage(&budget, 1, 64);
    drop(third);
    usage(&budget, 0, 0);
    // The external source owner is outside this consumer's admission lifetime.
    assert_eq!(&*original, &[7; 8]);
    drop(budget.admit(RuntimeData::Binary(original)).unwrap());
    usage(&budget, 0, 0);
}

#[test]
fn input_and_output_share_one_limit_without_recharging_published_output() {
    let budget = BufferBudget::new(2, 16);
    let allocator =
        BoundOutputAllocator::new(Arc::new(HeapOutputAllocator)).with_budget(budget.clone());
    assert_eq!(
        allocator.budget().unwrap().limits(),
        BufferBudgetUsage {
            owners: 2,
            bytes: 16
        }
    );
    let admitted = budget.admit(input(8)).unwrap();
    let output = RuntimeData::Binary(allocator.allocate_bytes(8).unwrap().freeze());
    let output = budget.admit(output).unwrap();
    assert!(allocator.owns_payload(&output));
    usage(&budget, 2, 16);
    assert!(allocator.allocate_bytes(0).is_err());
    assert!(budget.admit(input(1)).is_err());
    drop(admitted);
    let replacement = budget.admit(input(8)).unwrap();
    usage(&budget, 2, 16);
    drop((replacement, output));
    usage(&budget, 0, 0);
}

#[test]
fn separate_scopes_keep_independent_credits_and_prior_views() {
    let first = BufferBudget::new(1, 8);
    let second = BufferBudget::new(1, 8);
    let original = input(8);
    let upstream = first.admit(original.clone()).unwrap();
    let downstream = second.admit(upstream.clone()).unwrap();
    let looped = first.admit(downstream).unwrap();
    // Same raw storage admitted separately shares the second scope's credit,
    // but does not inherit a first-scope receipt from a different routing path.
    let bypass = second.admit(original).unwrap();
    usage(&first, 1, 8);
    usage(&second, 1, 8);
    drop((upstream, looped));
    usage(&first, 0, 0);
    usage(&second, 1, 8);
    drop(bypass);
    usage(&second, 0, 0);
}

#[test]
fn legacy_audio_moves_storage_and_metadata_does_not_consume_capacity() {
    let zero = BufferBudget::new(0, 0);
    assert!(zero.admit(RuntimeData::Text("control".into())).is_ok());
    assert!(zero
        .admit(RuntimeData::Json(serde_json::json!({"ready":true})))
        .is_ok());
    assert!(zero.admit(input(0)).is_err());
    let budget = BufferBudget::new(1, 64);
    let mut samples = Vec::with_capacity(16);
    samples.extend_from_slice(&[0.5; 4]);
    let pointer = samples.as_ptr();
    let data = budget.admit(audio(AudioSamples::Vec(samples))).unwrap();
    let RuntimeData::Audio { samples, .. } = &data else {
        unreachable!()
    };
    assert_eq!(samples.as_ptr(), pointer);
    assert!(matches!(samples, AudioSamples::Shared(_)));
    usage(&budget, 1, 64);
    drop(data);
    let samples: Arc<[f32]> = Arc::from([0.25; 4]);
    let pointer = samples.as_ptr();
    let first = budget
        .admit(audio(AudioSamples::Arc(samples.clone())))
        .unwrap();
    let second = budget.admit(audio(AudioSamples::Arc(samples))).unwrap();
    let RuntimeData::Audio { samples, .. } = &first else {
        unreachable!()
    };
    assert_eq!(samples.as_ptr(), pointer);
    usage(&budget, 1, 16);
    drop((first, second));
    usage(&budget, 0, 0);
}

struct Owner {
    values: Vec<u8>,
    drops: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        &self.values
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn weak_admission_cache_never_pins_payloads_or_old_allocation_generations() {
    let budget = BufferBudget::new(1, 8);
    let drops = Arc::new(AtomicUsize::new(0));
    for _ in 0..512 {
        let data = RuntimeData::Binary(SharedBuffer::from_owner(Owner {
            values: vec![3; 8],
            drops: drops.clone(),
        }));
        let admitted = budget.admit(data).unwrap();
        let RuntimeData::Binary(buffer) = &admitted else {
            unreachable!()
        };
        assert!(buffer.owner::<Owner>().is_some());
        drop(admitted);
        usage(&budget, 0, 0);
    }
    assert_eq!(drops.load(Ordering::SeqCst), 512);
}

#[test]
fn concurrent_admission_of_the_same_owner_uses_one_credit() {
    let budget = BufferBudget::new(1, 8);
    let original = input(8);
    let admitting = budget.clone();
    let results = hold_concurrently(
        8,
        move || admitting.admit(original.clone()),
        || usage(&budget, 1, 8),
    );
    for result in results {
        assert!(result.is_ok());
    }
    usage(&budget, 0, 0);
}
