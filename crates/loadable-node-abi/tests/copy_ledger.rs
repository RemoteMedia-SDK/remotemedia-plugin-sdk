#![cfg(feature = "runtime")]
use loadable_node_abi::copy_ledger::runtime::{export, import};
use remotemedia_types::{
    CopyContext, CopyCounts, CopyPolicy, CopyReason, SharedBuffer, COPY_REASONS,
};
use std::sync::Arc;

fn counts(context: &CopyContext, reason: CopyReason) -> CopyCounts {
    context
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == reason)
        .unwrap()
        .1
}

#[test]
fn device_transfer_abi_preserves_partial_completion_and_retained_authority() {
    let host = CopyContext::new(CopyPolicy::AllowCopies, "device/partial");
    let imported = import(export(host.clone()));
    let operation = imported.begin(CopyReason::DeviceTransfer, 64).unwrap();
    let cancelled = imported.begin(CopyReason::DeviceTransfer, 32).unwrap();
    drop(imported);
    std::thread::spawn(move || {
        operation.complete_bytes(12).unwrap();
        drop(cancelled);
    })
    .join()
    .unwrap();
    assert_eq!(
        counts(&host, CopyReason::DeviceTransfer),
        CopyCounts {
            completed: 1,
            copied_bytes: 12,
            failed: 1,
            failed_requested_bytes: 32,
            ..Default::default()
        }
    );
    assert!(host.report().entries[..8]
        .iter()
        .all(|(_, count)| *count == Default::default()));
    assert_eq!(
        import(export(host.clone())).report().entries,
        host.report().entries
    );
}

#[test]
fn strict_bridge_rejects_all_reasons_before_operation_and_preserves_identity() {
    let host = CopyContext::new(
        CopyPolicy::RequireZeroCopy,
        "session/s/node/n/materialization",
    );
    let remote = import(export(host.clone()));
    for reason in COPY_REASONS {
        let error = remote
            .materialize(reason, 7, || panic!("must reject before operation"))
            .unwrap_err();
        assert_eq!(error.boundary, host.report().boundary);
        assert_eq!(error.reason, reason);
        remote.materialize(reason, 0, || ()).unwrap();
        assert_eq!(
            counts(&host, reason),
            CopyCounts {
                rejected: 1,
                rejected_bytes: 7,
                ..Default::default()
            }
        );
    }
    assert_eq!(remote.report().policy, CopyPolicy::RequireZeroCopy);
    assert_eq!(remote.report().entries, host.report().entries);
}

#[test]
fn bridge_records_in_flight_success_error_and_local_unwind() {
    let host = CopyContext::new(CopyPolicy::AllowCopies, "host");
    let remote = import(export(host.clone()));
    let reason = CopyReason::VecMaterialization;
    remote
        .materialize(reason, 11, || {
            assert_eq!(counts(&host, reason).in_flight, 1)
        })
        .unwrap();
    assert_eq!(
        remote
            .materialize_result(reason, 13, || Err::<(), _>("partial"))
            .unwrap(),
        Err("partial")
    );
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = remote.materialize(reason, 17, || panic!("local operation unwind"));
    }))
    .is_err());
    assert_eq!(
        counts(&host, reason),
        CopyCounts {
            completed: 1,
            copied_bytes: 11,
            failed: 2,
            failed_requested_bytes: 30,
            ..Default::default()
        }
    );
}

#[test]
fn owning_copy_operations_keep_the_imported_abi_ledger_alive_after_context_drop() {
    let host = CopyContext::new(CopyPolicy::AllowCopies, "worker/output");
    let remote = import(export(host.clone()));
    let success = remote.begin(CopyReason::BorrowedInput, 16).unwrap();
    let failure = remote.begin(CopyReason::BorrowedInput, 32).unwrap();
    drop(remote);
    assert_eq!(counts(&host, CopyReason::BorrowedInput).in_flight, 2);
    std::thread::spawn(move || {
        success.complete();
        drop(failure);
    })
    .join()
    .unwrap();
    assert_eq!(
        counts(&host, CopyReason::BorrowedInput),
        CopyCounts {
            completed: 1,
            copied_bytes: 16,
            failed: 1,
            failed_requested_bytes: 32,
            ..Default::default()
        }
    );
}

#[test]
fn bridge_is_retained_reexportable_and_concurrent_without_double_accounting() {
    let host = CopyContext::new(CopyPolicy::AllowCopies, "retained");
    let first = import(export(host.clone()));
    let retained = Arc::new(import(export(first.clone())));
    drop(first);
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let context = retained.clone();
            std::thread::spawn(move || {
                for _ in 0..25 {
                    context
                        .materialize(CopyReason::BorrowedInput, 4, || ())
                        .unwrap();
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(
        counts(&host, CopyReason::BorrowedInput),
        CopyCounts {
            completed: 100,
            copied_bytes: 400,
            ..Default::default()
        }
    );
    drop(host);
    assert_eq!(counts(&retained, CopyReason::BorrowedInput).completed, 100);
    retained
        .materialize(CopyReason::BorrowedInput, 8, || ())
        .unwrap();
    assert_eq!(
        counts(&retained, CopyReason::BorrowedInput).copied_bytes,
        408
    );
}

#[test]
fn serde_decode_uses_the_imported_ledger_and_restores_scope() {
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let host = CopyContext::new(policy, "decoder");
        let remote = import(export(host.clone()));
        let mut decoder = serde_json::Deserializer::from_str("[1,2,3,4]");
        let decoded = remote.deserialize::<SharedBuffer<u8>, _>(&mut decoder);
        let actual = counts(&host, CopyReason::PortableDeserialization);
        if policy == CopyPolicy::AllowCopies {
            assert_eq!(&*decoded.unwrap(), &[1, 2, 3, 4]);
            assert_eq!(actual.copied_bytes, 4);
        } else {
            assert!(decoded.is_err());
            assert_eq!(actual.rejected_bytes, 1);
        }
        assert_eq!(actual.in_flight, 0);
    }
}
