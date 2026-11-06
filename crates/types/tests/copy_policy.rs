use remotemedia_types::{
    AudioSamples, CopyContext, CopyCounts, CopyPolicy, CopyReason, SharedBuffer,
};
use std::sync::Arc;

fn counts(context: &CopyContext, reason: CopyReason) -> CopyCounts {
    context
        .report()
        .entries
        .into_iter()
        .find(|entry| entry.0 == reason)
        .unwrap()
        .1
}

#[test]
fn strict_reuses_unique_vec_and_existing_arc_without_recording_copies() {
    let context = CopyContext::new(CopyPolicy::RequireZeroCopy, "node.output");
    let mut values = Vec::with_capacity(128);
    values.extend_from_slice(&[1.0f32, 2.0]);
    let pointer = values.as_ptr();
    let output = SharedBuffer::from(values).try_into_vec(&context).unwrap();
    assert_eq!(output.as_ptr(), pointer);
    assert_eq!(output.capacity(), 128);
    let owner: Arc<[f32]> = Arc::from([1.0, 2.0]);
    let recovered = SharedBuffer::from(owner.clone())
        .try_into_arc(&context)
        .unwrap();
    assert!(Arc::ptr_eq(&owner, &recovered));
    let empty = SharedBuffer::from(vec![1u8]).slice(1..1).unwrap();
    assert!(empty.try_into_vec(&context).unwrap().is_empty());
    assert!(context
        .report()
        .entries
        .iter()
        .all(|(_, counts)| *counts == CopyCounts::default()));
}

#[test]
fn strict_rejects_alias_slice_and_arc_materialization_before_cloning() {
    let context = CopyContext::new(CopyPolicy::RequireZeroCopy, "node.output");
    let buffer = SharedBuffer::from(vec![1.0f32; 4]);
    let error = buffer.clone().try_into_vec(&context).unwrap_err();
    assert_eq!((error.boundary.as_str(), error.bytes), ("node.output", 16));
    assert!(buffer.slice(1..3).unwrap().try_into_vec(&context).is_err());
    assert!(buffer.try_into_arc(&context).is_err());
    assert_eq!(
        counts(&context, CopyReason::VecMaterialization).rejected_bytes,
        24
    );
    assert_eq!(
        counts(&context, CopyReason::ArcMaterialization).rejected_bytes,
        16
    );
    assert!(context
        .report()
        .entries
        .iter()
        .all(|(_, counts)| counts.completed == 0 && counts.in_flight == 0));
    struct NeverClone;
    impl Clone for NeverClone {
        fn clone(&self) -> Self {
            panic!("strict attempted Clone")
        }
    }
    // Use a nonzero-sized element so this is a payload materialization.
    #[derive(Clone)]
    struct Element {
        _marker: NeverClone,
        _byte: u8,
    }
    let buffer = SharedBuffer::from(vec![Element {
        _marker: NeverClone,
        _byte: 1,
    }]);
    let alias = buffer.clone();
    assert!(buffer.try_into_vec(&context).is_err());
    drop(alias);
}

#[test]
fn compatibility_records_only_the_visible_copied_bytes() {
    let context = CopyContext::new(CopyPolicy::AllowCopies, "node.output");
    let buffer = SharedBuffer::from(vec![1.0f32, 2.0, 3.0, 4.0]);
    let vec = buffer.clone().try_into_vec(&context).unwrap();
    assert_ne!(vec.as_ptr(), buffer.as_ptr());
    let arc = buffer.slice(1..3).unwrap().try_into_arc(&context).unwrap();
    assert_eq!(&*arc, &[2.0, 3.0]);
    assert_eq!(
        counts(&context, CopyReason::VecMaterialization).copied_bytes,
        16
    );
    assert_eq!(
        counts(&context, CopyReason::ArcMaterialization).copied_bytes,
        8
    );
    assert_eq!(
        counts(&context, CopyReason::VecMaterialization).completed,
        1
    );
    assert_eq!(
        serde_json::to_value(context.report()).unwrap()["policy"],
        "allow_copies"
    );
    assert!(serde_json::from_str::<CopyPolicy>("\"implicit_fallback\"").is_err());
}

#[test]
fn reports_in_flight_and_failed_materialization_without_claiming_success() {
    let context = CopyContext::new(CopyPolicy::AllowCopies, "panic-test");
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        context.materialize(CopyReason::VecMaterialization, 4, || {
            assert_eq!(
                counts(&context, CopyReason::VecMaterialization).in_flight,
                1
            );
            panic!("copy primitive failed");
        })
    }));
    assert!(failed.is_err());
    let report = counts(&context, CopyReason::VecMaterialization);
    assert_eq!(
        (
            report.failed,
            report.failed_requested_bytes,
            report.in_flight
        ),
        (1, 4, 0)
    );
    assert_eq!((report.completed, report.copied_bytes), (0, 0));
}

#[test]
fn boundary_ledger_is_shared_across_threads() {
    let context = CopyContext::new(CopyPolicy::AllowCopies, "parallel");
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let context = context.clone();
            std::thread::spawn(move || {
                for _ in 0..100 {
                    let source: SharedBuffer<u8> = vec![7; 8].into();
                    let _alias = source.clone();
                    assert_eq!(source.try_into_vec(&context).unwrap(), [7; 8]);
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let report = counts(&context, CopyReason::VecMaterialization);
    assert_eq!(
        (report.completed, report.copied_bytes, report.in_flight),
        (800, 6400, 0)
    );
    assert!(!report.saturated);
}

#[test]
fn fallible_materialization_preserves_failure_and_rejects_before_execution() {
    let context = CopyContext::new(CopyPolicy::AllowCopies, "fallible");
    let result = context
        .materialize_result(CopyReason::PortableSerialization, 12, || {
            assert_eq!(
                counts(&context, CopyReason::PortableSerialization).in_flight,
                1
            );
            Err::<(), _>("partial output")
        })
        .unwrap();
    assert_eq!(result, Err("partial output"));
    let failed = counts(&context, CopyReason::PortableSerialization);
    assert_eq!(
        (
            failed.failed,
            failed.failed_requested_bytes,
            failed.in_flight
        ),
        (1, 12, 0)
    );
    assert_eq!((failed.completed, failed.copied_bytes), (0, 0));
    context
        .materialize_result(CopyReason::PortableSerialization, 4, || Ok::<_, ()>(()))
        .unwrap()
        .unwrap();
    assert_eq!(
        counts(&context, CopyReason::PortableSerialization).copied_bytes,
        4
    );
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict-fallible");
    let rejected = strict
        .materialize_result(
            CopyReason::PortableSerialization,
            12,
            || -> Result<(), ()> { panic!("rejected operation must not execute") },
        )
        .unwrap_err();
    assert_eq!(rejected.bytes, 12);
    assert_eq!(
        counts(&strict, CopyReason::PortableSerialization).rejected,
        1
    );
    // Empty operations still preserve their errors without inventing payload bytes.
    assert_eq!(
        strict
            .materialize_result(CopyReason::PortableSerialization, 0, || Err::<(), _>(
                "empty failure"
            ))
            .unwrap(),
        Err("empty failure")
    );
    assert_eq!(counts(&strict, CopyReason::PortableSerialization).failed, 0);
}

#[test]
fn legacy_audio_apis_are_counted_without_double_counting_shared_conversions() {
    let context = CopyContext::legacy();
    let before = context.report();
    let samples = AudioSamples::Vec(vec![1.0; 4]);
    let _alias = samples.clone();
    let _arc = samples.into_arc();
    let _borrowed = AudioSamples::from(&[1.0f32; 3][..]);
    let shared: SharedBuffer<u8> = vec![1; 5].into();
    let _alias = shared.clone();
    let _owned = shared.into_vec();
    for (reason, expected_bytes) in [
        (CopyReason::LegacyAudioClone, 16),
        (CopyReason::ArcMaterialization, 16),
        (CopyReason::BorrowedInput, 12),
        (CopyReason::VecMaterialization, 5),
    ] {
        let old = before
            .entries
            .iter()
            .find(|entry| entry.0 == reason)
            .unwrap()
            .1;
        let new = counts(context, reason);
        assert_eq!(new.completed - old.completed, 1);
        assert_eq!(new.copied_bytes - old.copied_bytes, expected_bytes);
    }
}
