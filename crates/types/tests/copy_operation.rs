use remotemedia_types::{CopyContext, CopyPolicy, CopyReason};

#[test]
fn owning_operation_retains_ledger_and_accounts_completion_failure_and_unwind() {
    let context = CopyContext::new(CopyPolicy::AllowCopies, "worker/output");
    let success = context.begin(CopyReason::BorrowedInput, 16).unwrap();
    let failure = context.begin(CopyReason::BorrowedInput, 32).unwrap();
    assert_eq!(context.report().entries[3].1.in_flight, 2);
    std::thread::spawn(move || success.complete())
        .join()
        .unwrap();
    drop(failure);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _operation = context.begin(CopyReason::BorrowedInput, 64).unwrap();
        panic!("test unwinding");
    }));
    assert!(result.is_err());
    let report = context.report();
    assert_eq!(report.boundary, "worker/output");
    let count = report.entries[3].1;
    assert_eq!(
        (
            count.completed,
            count.copied_bytes,
            count.failed,
            count.failed_requested_bytes,
            count.in_flight
        ),
        (1, 16, 2, 96, 0)
    );
}

#[test]
fn strict_authorization_rejects_before_start_and_empty_attempts_have_no_events() {
    let context = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict/output");
    assert!(context.begin(CopyReason::BorrowedInput, 4).is_err());
    context
        .begin(CopyReason::BorrowedInput, 0)
        .unwrap()
        .complete();
    drop(context.begin(CopyReason::BorrowedInput, 0).unwrap());
    context
        .begin(CopyReason::BorrowedInput, 0)
        .unwrap()
        .complete_bytes(0)
        .unwrap();
    assert!(context
        .begin(CopyReason::BorrowedInput, 0)
        .unwrap()
        .complete_bytes(4)
        .is_err());
    let count = context.report().entries[3].1;
    assert_eq!(
        (
            count.rejected,
            count.rejected_bytes,
            count.completed,
            count.failed,
            count.in_flight
        ),
        (1, 4, 0, 0, 0)
    );
}

#[test]
fn partial_capture_completes_on_worker_with_actual_bytes_and_balanced_events() {
    let context = CopyContext::new(CopyPolicy::AllowCopies, "capture/partial");
    let full = context.begin(CopyReason::BorrowedInput, 64).unwrap();
    let partial = context.begin(CopyReason::BorrowedInput, 64).unwrap();
    let unused = context.begin(CopyReason::BorrowedInput, 64).unwrap();
    let cancelled = context.begin(CopyReason::BorrowedInput, 64).unwrap();
    assert_eq!(context.report().entries[3].1.in_flight, 4);
    std::thread::spawn(move || {
        full.complete_bytes(64).unwrap();
        partial.complete_bytes(12).unwrap();
        unused.complete_bytes(0).unwrap();
        drop(cancelled);
    })
    .join()
    .unwrap();
    let count = context.report().entries[3].1;
    assert_eq!(count.completed, 3);
    assert_eq!(count.copied_bytes, 76);
    assert_eq!(count.failed, 1);
    assert_eq!(count.failed_requested_bytes, 64);
    assert_eq!(count.in_flight, 0);
}

#[test]
fn overrun_is_a_failed_authorization_not_a_successful_copy() {
    let context = CopyContext::new(CopyPolicy::AllowCopies, "capture/overrun");
    let error = context
        .begin(CopyReason::BorrowedInput, 16)
        .unwrap()
        .complete_bytes(20)
        .unwrap_err();
    assert_eq!(error.authorized_bytes, 16);
    assert_eq!(error.actual_bytes, 20);
    let count = context.report().entries[3].1;
    assert_eq!(count.completed, 0);
    assert_eq!(count.copied_bytes, 0);
    assert_eq!(count.failed, 1);
    assert_eq!(count.failed_requested_bytes, 16);
    assert_eq!(count.in_flight, 0);
}
