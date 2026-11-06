use remotemedia_types::{
    BoundOutputAllocator, BufferAllocationError, BufferBudget, BufferBudgetUsage, BufferLoan,
    CopyContext, CopyPolicy, CopyReason, ForwardingScope, HeapOutputAllocator, OutputAllocator,
    RuntimeData, SharedBuffer, TensorEncoding,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Barrier,
};

fn bound(budget: &BufferBudget) -> BoundOutputAllocator {
    BoundOutputAllocator::new(Arc::new(HeapOutputAllocator)).with_budget(budget.clone())
}
fn usage(budget: &BufferBudget, owners: u64, bytes: u64) {
    assert_eq!(budget.usage(), BufferBudgetUsage { owners, bytes });
}

#[test]
fn live_loans_views_and_reuse_hold_one_reservation_until_last_owner() {
    let budget = BufferBudget::new(1, 64);
    let allocator = bound(&budget);
    let mut loan = allocator.allocate_audio(16).unwrap();
    loan.fill(0.5);
    let pointer = loan.as_ptr();
    usage(&budget, 1, 64);
    assert_eq!(
        allocator.allocate_bytes(0).unwrap_err(),
        BufferAllocationError::Exhausted
    );
    let buffer = loan.freeze();
    let view = buffer.slice(2..3).unwrap();
    let buffer = buffer.try_into_loan().unwrap_err();
    usage(&budget, 1, 64);
    drop(view);
    let mut loan = buffer.try_into_loan().unwrap();
    loan[0] = 0.75;
    let buffer = loan.freeze();
    assert_eq!(buffer.as_ptr(), pointer);
    let data = ForwardingScope::default().accept(RuntimeData::Audio {
        samples: buffer.into(),
        sample_rate: 48000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    });
    let (metadata, storage) = data.into_shared_parts(TensorEncoding::Runtime).unwrap();
    let data = metadata.attach(storage, TensorEncoding::Runtime).unwrap();
    assert!(allocator.owns_payload(&data));
    let retained = data.clone();
    drop(data);
    drop(allocator);
    usage(&budget, 1, 64);
    std::thread::spawn(move || drop(retained)).join().unwrap();
    usage(&budget, 0, 0);
    assert!(bound(&budget).allocate_audio(16).is_ok());
}

#[test]
fn bytes_and_owner_limits_are_independent_including_zero_sized_outputs() {
    let budget = BufferBudget::new(3, 8);
    let allocator = bound(&budget);
    let first = allocator.allocate_bytes(8).unwrap();
    let empty = allocator.allocate_bytes(0).unwrap();
    usage(&budget, 2, 8);
    assert_eq!(
        allocator.allocate_bytes(1).unwrap_err(),
        BufferAllocationError::Exhausted
    );
    let second_empty = allocator.allocate_bytes(0).unwrap();
    assert_eq!(
        allocator.allocate_bytes(0).unwrap_err(),
        BufferAllocationError::Exhausted
    );
    drop(first);
    usage(&budget, 2, 0);
    let replacement = allocator.allocate_audio(2).unwrap();
    usage(&budget, 3, 8);
    drop((empty, second_empty, replacement));
    usage(&budget, 0, 0);
    assert_eq!(
        bound(&BufferBudget::new(0, 0))
            .allocate_bytes(0)
            .unwrap_err(),
        BufferAllocationError::Exhausted
    );
}

enum Mode {
    Rounded,
    Fails,
    Panics,
    Slice,
}
struct Provider {
    calls: Arc<AtomicUsize>,
    mode: Mode,
    budget: BufferBudget,
}
impl OutputAllocator for Provider {
    fn allocate_bytes(&self, len: usize) -> Result<BufferLoan<u8>, BufferAllocationError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        usage(&self.budget, 1, len as u64); // reservation precedes provider invocation
        match self.mode {
            Mode::Rounded => {
                let mut values = Vec::with_capacity(128);
                values.resize(len, 7);
                Ok(values.into())
            }
            Mode::Fails => Err(BufferAllocationError::Unavailable),
            Mode::Panics => panic!("provider failed"),
            Mode::Slice => Ok(SharedBuffer::from(Arc::<[u8]>::from(vec![9; len]))
                .try_into_loan()
                .unwrap()),
        }
    }
    fn allocate_audio(&self, _: usize) -> Result<BufferLoan<f32>, BufferAllocationError> {
        Err(BufferAllocationError::Unavailable)
    }
}
fn provider(budget: &BufferBudget, mode: Mode) -> (BoundOutputAllocator, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let allocator = BoundOutputAllocator::new(Arc::new(Provider {
        calls: calls.clone(),
        mode,
        budget: budget.clone(),
    }))
    .with_budget(budget.clone());
    (allocator, calls)
}

#[test]
fn reserves_before_provider_and_rolls_back_failure_unwind_and_rounding_rejection() {
    for mode in [Mode::Rounded, Mode::Fails, Mode::Panics] {
        let budget = BufferBudget::new(1, 16);
        let (allocator, calls) = provider(&budget, mode);
        assert_eq!(
            allocator.allocate_bytes(17).unwrap_err(),
            BufferAllocationError::Exhausted
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| allocator.allocate_bytes(8)));
        assert!(result.is_err() || result.unwrap().is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        usage(&budget, 0, 0);
        assert!(bound(&budget).allocate_bytes(16).is_ok());
    }
    let budget = BufferBudget::new(u64::MAX, u64::MAX);
    let (allocator, calls) = provider(&budget, Mode::Fails);
    assert_eq!(
        allocator.allocate_bytes(usize::MAX).unwrap_err(),
        BufferAllocationError::InvalidLength
    );
    assert_eq!(
        allocator.allocate_audio(usize::MAX).unwrap_err(),
        BufferAllocationError::InvalidLength
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    usage(&budget, 0, 0);
}

#[test]
fn rounded_capacity_is_charged_for_the_entire_lifetime_of_an_empty_slice() {
    let budget = BufferBudget::new(1, 128);
    let (allocator, _) = provider(&budget, Mode::Rounded);
    let original = allocator.allocate_bytes(8).unwrap().freeze();
    usage(&budget, 1, 128);
    let empty = original.slice(8..8).unwrap();
    assert!(original.shares_storage(&empty));
    drop(original);
    usage(&budget, 1, 128);
    assert!(allocator.allocate_bytes(1).is_err());
    drop(empty);
    usage(&budget, 0, 0);
}

#[test]
fn nested_budget_failure_does_not_leak_the_inner_reservation() {
    let inner = BufferBudget::new(1, 128);
    let outer = BufferBudget::new(1, 16);
    let (allocator, _) = provider(&inner, Mode::Rounded);
    let allocator = BoundOutputAllocator::new(Arc::new(allocator)).with_budget(outer.clone());
    assert_eq!(
        allocator.allocate_bytes(8).unwrap_err(),
        BufferAllocationError::Exhausted
    );
    usage(&inner, 0, 0);
    usage(&outer, 0, 0);
    let inner_allocator = Arc::new(bound(&inner));
    let allocator = BoundOutputAllocator::new(inner_allocator.clone()).with_budget(outer.clone());
    let output = allocator.allocate_bytes(8).unwrap().freeze();
    let held = output.slice(2..3).unwrap();
    drop((output, allocator, inner_allocator));
    usage(&inner, 1, 8);
    usage(&outer, 1, 8);
    drop(held);
    usage(&inner, 0, 0);
    usage(&outer, 0, 0);
}

#[test]
fn bare_vec_and_arc_extraction_cannot_escape_the_live_budget() {
    for slice in [false, true] {
        let budget = BufferBudget::new(1, 8);
        let allocator = if slice {
            provider(&budget, Mode::Slice).0
        } else {
            bound(&budget)
        };
        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "budget-output");
        let original = allocator.allocate_bytes(8).unwrap().freeze();
        if slice {
            assert!(original.try_into_arc(&strict).is_err());
        } else {
            assert!(original.try_into_vec(&strict).is_err());
        }
        usage(&budget, 0, 0);
        let compatible = CopyContext::new(CopyPolicy::AllowCopies, "budget-output");
        let retained = allocator.allocate_bytes(8).unwrap().freeze();
        let pointer = retained.as_ptr();
        if slice {
            let result = retained.try_into_arc(&compatible).unwrap();
            assert_ne!(result.as_ptr(), pointer);
            assert_eq!(result.len(), 8);
        } else {
            let result = retained.try_into_vec(&compatible).unwrap();
            assert_ne!(result.as_ptr(), pointer);
            assert_eq!(result.len(), 8);
        }
        usage(&budget, 0, 0);
        let reason = if slice {
            CopyReason::ArcMaterialization
        } else {
            CopyReason::VecMaterialization
        };
        let counts = compatible
            .report()
            .entries
            .into_iter()
            .find(|(r, _)| *r == reason)
            .unwrap()
            .1;
        assert_eq!((counts.completed, counts.copied_bytes), (1, 8));
        assert_eq!(
            strict
                .report()
                .entries
                .into_iter()
                .find(|(r, _)| *r == reason)
                .unwrap()
                .1
                .rejected,
            1
        );
    }
}

#[test]
fn concurrent_allocators_never_exceed_shared_capacity_and_recover_after_release() {
    let budget = BufferBudget::new(4, 32);
    let allocator = Arc::new(bound(&budget));
    let rendezvous = Arc::new(Barrier::new(17));
    let threads: Vec<_> = (0..16)
        .map(|_| {
            let allocator = allocator.clone();
            let barrier = rendezvous.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let result = allocator.allocate_bytes(8);
                barrier.wait();
                barrier.wait();
                result.is_ok()
            })
        })
        .collect();
    rendezvous.wait();
    rendezvous.wait();
    usage(&budget, 4, 32);
    rendezvous.wait();
    let successes = threads
        .into_iter()
        .map(|t| usize::from(t.join().unwrap()))
        .sum::<usize>();
    assert_eq!(successes, 4);
    usage(&budget, 0, 0);
    assert!(allocator.allocate_bytes(32).is_ok());
}
