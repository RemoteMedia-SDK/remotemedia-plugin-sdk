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
