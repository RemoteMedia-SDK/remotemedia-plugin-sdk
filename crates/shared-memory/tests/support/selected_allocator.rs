use super::*;
use remotemedia_shared_memory::peer::CallResources;
use remotemedia_types::{BoundOutputAllocator, BufferAllocationError, BufferLoan, OutputAllocator};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

pub(super) struct Selected {
    pub(super) bound: BoundOutputAllocator,
    pub(super) pointer: AtomicUsize,
    pub(super) cancel: Mutex<Option<(HostHandle, u64)>>,
}
impl Selected {
    pub(super) fn new(pool: &SharedPool) -> Arc<Self> {
        Arc::new(Self {
            bound: BoundOutputAllocator::new(Arc::new(pool.clone())),
            pointer: AtomicUsize::new(0),
            cancel: Mutex::new(None),
        })
    }
    fn record<T>(&self, loan: BufferLoan<T>) -> BufferLoan<T> {
        self.pointer.store(loan.as_ptr() as usize, Ordering::SeqCst);
        if let Some((handle, call)) = self.cancel.lock().unwrap().take() {
            handle.cancel(call).unwrap();
        }
        loan
    }
}
impl OutputAllocator for Selected {
    fn allocate_bytes(&self, len: usize) -> Result<BufferLoan<u8>, BufferAllocationError> {
        self.bound.allocate_bytes(len).map(|loan| self.record(loan))
    }
    fn allocate_audio(&self, len: usize) -> Result<BufferLoan<f32>, BufferAllocationError> {
        self.bound.allocate_audio(len).map(|loan| self.record(loan))
    }
}
fn selected_call(
    handle: &HostHandle,
    allocator: Arc<dyn OutputAllocator>,
    operation: &str,
    input: Option<RuntimeData>,
) -> u64 {
    handle
        .start_call_with_resources(
            CallKind::Process,
            CallContext {
                session_id: Some("selected".into()),
                node_id: "producer".into(),
            },
            serde_json::json!({"operation":operation}),
            input
                .into_iter()
                .map(|data| ("input".into(), data))
                .collect(),
            CallResources {
                output_allocator: Some(allocator),
                ..Default::default()
            },
        )
        .unwrap()
}

#[test]
fn remote_production_preserves_the_actual_bound_loan_and_not_another_binding() {
    for audio in [false, true] {
        verify_selected(audio);
    }
}
fn verify_selected(audio: bool) {
    let pool = pool(2);
    let allocator = Selected::new(&pool);
    let other = BoundOutputAllocator::new(Arc::new(pool.clone()));
    let (input, _) = provenance_input(&other, audio);
    let (mut peer, handle) = spawn(&pool);
    let call = selected_call(&handle, allocator.clone(), "transform", Some(input));
    let output = output(&mut peer, call).into_data();
    complete(&mut peer, call).unwrap();
    assert!(allocator.bound.owns_payload(&output));
    assert!(!other.owns_payload(&output));
    handle.close();
    match &output {
        RuntimeData::Audio { samples, .. } => {
            assert_eq!(
                samples.as_ptr() as usize,
                allocator.pointer.load(Ordering::SeqCst)
            );
            assert_eq!(&samples[..], &[1.5; 16]);
        }
        RuntimeData::Binary(bytes) => {
            assert_eq!(
                bytes.as_ptr() as usize,
                allocator.pointer.load(Ordering::SeqCst)
            );
            assert_eq!(&bytes[..], &[8; 16]);
        }
        _ => panic!("bulk output required"),
    }
    drop(output);
    let _first = pool.allocate::<u8>(4096).unwrap();
    let _second = pool.allocate::<u8>(4096).unwrap();
}

#[test]
fn explicit_incompatible_or_exhausted_providers_fail_without_pool_fallback() {
    let pool = pool(1);
    let foreign = super::pool(1);
    let selected = Selected::new(&pool);
    let (mut peer, handle) = spawn(&pool);
    for allocator in [
        Arc::new(remotemedia_types::HeapOutputAllocator) as Arc<dyn OutputAllocator>,
        Arc::new(foreign.clone()),
    ] {
        let call = selected_call(&handle, allocator, "source", None);
        assert!(complete(&mut peer, call).is_err());
        assert!(pool.allocate::<u8>(4096).is_ok());
        assert!(foreign.allocate::<u8>(4096).is_ok());
    }
    let held = pool.allocate::<u8>(4096).unwrap();
    let call = selected_call(&handle, selected.clone(), "source", None);
    assert!(complete(&mut peer, call).is_err());
    drop(held);
    let call = selected_call(&handle, selected.clone(), "source", None);
    let output = output(&mut peer, call).into_data();
    complete(&mut peer, call).unwrap();
    assert!(selected.bound.owns_payload(&output));
    handle.close();
}

#[test]
fn cancellation_inside_provider_does_not_deadlock_or_dispatch_its_loan() {
    let pool = pool(1);
    let selected = Selected::new(&pool);
    let (mut peer, handle) = spawn(&pool);
    let call = selected_call(&handle, selected.clone(), "source", None);
    *selected.cancel.lock().unwrap() = Some((handle.clone(), call));
    let (sender, receiver) = std::sync::mpsc::channel();
    let pump = std::thread::spawn(move || {
        sender.send(complete(&mut peer, call)).unwrap();
        peer
    });
    assert!(receiver
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap()
        .is_err());
    let mut peer = pump.join().unwrap();
    assert!(pool.allocate::<u8>(4096).is_ok());
    let next = selected_call(&handle, selected.clone(), "source", None);
    let output = output(&mut peer, next).into_data();
    complete(&mut peer, next).unwrap();
    assert!(selected.bound.owns_payload(&output));
    handle.close();
}

#[test]
fn unpublished_selected_loans_reclaim_after_abandonment_or_joined_worker_death() {
    for operation in ["abandon_loan", "hold_loan"] {
        let pool = pool(1);
        let selected = Selected::new(&pool);
        let (mut peer, handle) = spawn(&pool);
        let call = selected_call(&handle, selected.clone(), operation, None);
        if operation == "hold_loan" {
            let started = output(&mut peer, call).into_data();
            assert!(matches!(started, RuntimeData::Text(value) if value == "writing"));
            assert!(pool.allocate::<u8>(1).is_err());
        } else {
            complete(&mut peer, call).unwrap();
        }
        handle.close();
        let loan = pool.allocate::<u8>(4096).unwrap();
        assert!(loan.iter().all(|&byte| byte == 0));
        assert_ne!(selected.pointer.load(Ordering::SeqCst), 0);
    }
}
