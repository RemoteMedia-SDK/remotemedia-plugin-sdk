use remotemedia_types::{BufferBudget, BufferBudgetUsage};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::{Arc, Barrier};

pub fn usage(budget: &BufferBudget, owners: u64, bytes: u64) {
    assert_eq!(budget.usage(), BufferBudgetUsage { owners, bytes });
}

/// Keep concurrent results alive while inspecting the shared budget. Release
/// the rendezvous even when an action or assertion panics, so failures can exit.
pub fn hold_concurrently<T: Send + 'static>(
    threads: usize,
    action: impl Fn() -> T + Send + Sync + 'static,
    inspect: impl FnOnce(),
) -> Vec<T> {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let action = Arc::new(action);
    let workers: Vec<_> = (0..threads)
        .map(|_| {
            let barrier = barrier.clone();
            let action = action.clone();
            std::thread::spawn(move || {
                let value = catch_unwind(AssertUnwindSafe(|| action()));
                barrier.wait();
                barrier.wait();
                value
            })
        })
        .collect();
    barrier.wait();
    let inspected = catch_unwind(AssertUnwindSafe(inspect));
    barrier.wait();
    let results = workers
        .into_iter()
        .map(|worker| {
            worker
                .join()
                .unwrap()
                .unwrap_or_else(|error| resume_unwind(error))
        })
        .collect();
    inspected.unwrap_or_else(|error| resume_unwind(error));
    results
}
