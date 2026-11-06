use remotemedia_types::{
    BufferAllocationError, BufferLoan, HeapOutputAllocator, OutputAllocator, SharedBuffer,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct CountingAllocator;
thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}
fn record_allocation() {
    let _ = ALLOCATIONS.try_with(|count| {
        if let Some(n) = count.get() {
            count.set(Some(n + 1));
        }
    });
}
// Delegate the exact layouts/pointers to System; count only this test thread.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record_allocation();
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn recovering_existing_storage_does_not_initialize_copy_accounting_or_allocate() {
    let vector: SharedBuffer<u8> = vec![1; 16].into();
    let vector_pointer = vector.as_ptr();
    let slice: Arc<[u8]> = Arc::from([1u8; 16]);
    let slice_pointer = slice.as_ptr();
    let shared_slice = SharedBuffer::from(slice);
    let legacy = remotemedia_types::AudioSamples::Vec(vec![1.0; 4]);
    ALLOCATIONS.with(|n| n.set(Some(0)));
    let vector = vector.into_vec();
    let slice = shared_slice.into_arc();
    let legacy = legacy.into_vec();
    let allocations = ALLOCATIONS.with(|n| n.replace(None).unwrap());
    assert_eq!(allocations, 0);
    assert_eq!(vector.as_ptr(), vector_pointer);
    assert_eq!(slice.as_ptr(), slice_pointer);
    assert_eq!(legacy, [1.0; 4]);
}

#[test]
fn prepared_loan_fill_freeze_and_reuse_allocate_nothing() {
    let allocator = remotemedia_types::BoundOutputAllocator::new(Arc::new(HeapOutputAllocator));
    let mut loan = allocator.allocate_audio(64).unwrap();
    assert!(loan.iter().all(|&x| x == 0.0));
    let pointer = loan.as_ptr();
    assert_eq!((pointer as usize) % std::mem::align_of::<f32>(), 0);
    ALLOCATIONS.with(|n| n.set(Some(0)));
    loan.fill(0.5);
    let published = loan.freeze();
    let alias = published.slice(2..4).unwrap();
    let published = published.try_into_loan().unwrap_err();
    drop(alias);
    let mut recycled = published.try_into_loan().unwrap();
    recycled[0] = 0.75;
    let result = recycled.freeze();
    let allocations = ALLOCATIONS.with(|n| n.replace(None).unwrap());
    assert_eq!(
        allocations, 0,
        "prepared publication must not allocate metadata"
    );
    assert_eq!(result.as_ptr(), pointer);
    assert_eq!(result[0], 0.75);
}

#[test]
fn repeated_forwarding_admission_and_validation_allocate_nothing() {
    use remotemedia_types::{ForwardingScope, RuntimeData};
    let scope = ForwardingScope::default();
    let mut data = scope.accept(RuntimeData::Binary(vec![7; 32].into()));
    ALLOCATIONS.with(|n| n.set(Some(0)));
    for _ in 0..64 {
        data = scope.accept(data);
        assert!(scope.accepts_output(&data));
    }
    let allocations = ALLOCATIONS.with(|n| n.replace(None).unwrap());
    assert_eq!(allocations, 0);
}

struct OwnedLoan {
    values: Vec<u8>,
    releases: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for OwnedLoan {
    fn as_ref(&self) -> &[u8] {
        &self.values
    }
}
impl AsMut<[u8]> for OwnedLoan {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.values
    }
}
impl Drop for OwnedLoan {
    fn drop(&mut self) {
        self.releases.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn external_loan_retains_owner_and_releases_after_last_view() {
    let releases = Arc::new(AtomicUsize::new(0));
    let mut loan = BufferLoan::from_owner(OwnedLoan {
        values: vec![0; 8],
        releases: releases.clone(),
    });
    let pointer = loan.as_ptr() as usize;
    loan[3] = 42;
    let buffer = loan.freeze();
    let view = buffer.slice(3..4).unwrap();
    drop(buffer);
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    std::thread::spawn(move || {
        assert_eq!(view.as_ptr() as usize, pointer + 3);
        assert_eq!(view[0], 42);
        // Local uniqueness does not prove an external provider has no remote
        // readers. Reuse must go through that provider's lease accounting.
        assert_eq!(view.try_into_loan().unwrap_err().as_slice(), &[42]);
    })
    .join()
    .unwrap();
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

#[test]
fn abandoned_output_releases_its_owner() {
    let releases = Arc::new(AtomicUsize::new(0));
    drop(BufferLoan::from_owner(OwnedLoan {
        values: vec![0],
        releases: releases.clone(),
    }));
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

#[test]
fn immutable_provider_cannot_be_reopened_for_writing() {
    let buffer = SharedBuffer::from_owner(vec![1u8, 2]);
    let unchanged = buffer.try_into_loan().unwrap_err();
    assert_eq!(unchanged.as_slice(), &[1, 2]);
}

#[test]
fn allocation_rejects_overflow_and_initializes_empty_and_byte_loans() {
    assert_eq!(
        HeapOutputAllocator.allocate_audio(usize::MAX).unwrap_err(),
        BufferAllocationError::InvalidLength
    );
    assert_eq!(
        HeapOutputAllocator.allocate_bytes(usize::MAX).unwrap_err(),
        BufferAllocationError::InvalidLength
    );
    assert!(HeapOutputAllocator
        .allocate_audio(0)
        .unwrap()
        .freeze()
        .is_empty());
    assert_eq!(
        HeapOutputAllocator
            .allocate_bytes(7)
            .unwrap()
            .freeze()
            .as_slice(),
        &[0; 7]
    );
}
