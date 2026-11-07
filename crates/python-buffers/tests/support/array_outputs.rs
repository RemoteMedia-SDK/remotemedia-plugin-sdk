use super::*;
use remotemedia_shared_memory::peer::CallResources;
use remotemedia_types::{BoundOutputAllocator, CopyContext, CopyCounts, CopyPolicy, CopyReason};
use std::sync::Arc;

fn start(
    worker: &Worker,
    allocator: Arc<BoundOutputAllocator>,
    copies: Option<CopyContext>,
    operation: &str,
    input: Option<RuntimeData>,
) -> u64 {
    worker
        .handle
        .start_call_with_resources(
            CallKind::Process,
            CallContext {
                session_id: Some("python-session".into()),
                node_id: "numpy-node".into(),
            },
            serde_json::json!({"operation":operation}),
            input
                .into_iter()
                .map(|value| ("input".into(), value))
                .collect(),
            CallResources {
                output_copies: copies,
                output_allocator: Some(allocator),
                ..Default::default()
            },
        )
        .unwrap()
}

fn counts(context: &CopyContext) -> CopyCounts {
    context
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::BorrowedInput)
        .unwrap()
        .1
}

#[test]
fn published_audio_slices_forward_in_strict_mode_without_another_pool_slot() {
    let pool = pool(1);
    let allocator = Arc::new(BoundOutputAllocator::new(Arc::new(pool.clone())));
    use remotemedia_types::OutputAllocator;
    let mut loan = allocator.allocate_audio(8).unwrap();
    loan.copy_from_slice(&[0., 1., 2., 3., 4., 5., 6., 7.]);
    let pointer = loan.as_ptr() as usize;
    let input = RuntimeData::Audio {
        samples: loan.freeze().into(),
        sample_rate: 48000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    };
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "python/output");
    let mut worker = Worker::spawn_protocol(&pool, true);
    let call = start(
        &worker,
        allocator.clone(),
        Some(copies.clone()),
        "adapt_forward",
        Some(input),
    );
    let output = worker.output(call).into_data();
    worker.complete(call).unwrap();
    assert!(allocator.owns_payload(&output));
    let RuntimeData::Audio { samples, .. } = &output else {
        panic!("audio")
    };
    assert_eq!(samples.as_ptr() as usize, pointer + 8);
    assert_eq!(&samples[..], &[2., 3., 4., 5.]);
    assert_eq!(counts(&copies), CopyCounts::default());
    let call = start(
        &worker,
        allocator.clone(),
        Some(copies.clone()),
        "adapt_forward_endian",
        Some(output.clone()),
    );
    assert!(worker
        .complete(call)
        .unwrap_err()
        .contains("not compatible"));
    assert_eq!(counts(&copies), CopyCounts::default());
    drop(worker);
    assert!(pool.allocate::<u8>(4).is_err());
    drop(output);
    assert!(pool.allocate::<u8>(4).is_ok());
}

#[test]
fn private_readonly_strided_and_heap_published_arrays_copy_once_under_output_policy() {
    for (operation, expected) in [
        ("adapt_private", vec![0., 1., 2., 3., 4., 5., 6., 7.]),
        ("adapt_readonly", vec![0., 1., 2., 3., 4., 5., 6., 7.]),
        ("adapt_strided", vec![7., 5., 3., 1.]),
        ("adapt_heap", vec![0., 1., 2., 3., 4., 5., 6., 7.]),
    ] {
        let pool = pool(1);
        let allocator = Arc::new(BoundOutputAllocator::new(Arc::new(pool.clone())));
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "python/output");
        let mut worker = Worker::spawn_protocol(&pool, true);
        let call = start(
            &worker,
            allocator.clone(),
            Some(copies.clone()),
            operation,
            None,
        );
        let output = worker.output(call).into_data();
        worker.complete(call).unwrap();
        assert!(allocator.owns_payload(&output));
        let RuntimeData::Audio { samples, .. } = output else {
            panic!("audio")
        };
        assert_eq!(&samples[..], expected.as_slice());
        let remotemedia_types::AudioSamples::Shared(samples) = samples else {
            panic!("shared audio")
        };
        let retained = samples.slice(1..3).unwrap();
        drop(samples);
        assert_eq!(
            (counts(&copies).completed, counts(&copies).copied_bytes),
            (1, expected.len() as u64 * 4)
        );
        let call = start(
            &worker,
            allocator.clone(),
            Some(copies.clone()),
            operation,
            None,
        );
        assert!(worker.complete(call).is_err());
        assert_eq!(counts(&copies).failed, 1);
        drop(retained);
        let call = start(&worker, allocator, Some(copies.clone()), operation, None);
        let retained = worker.output(call).into_data();
        worker.complete(call).unwrap();
        drop(worker);
        assert_eq!(
            (counts(&copies).completed, counts(&copies).in_flight),
            (2, 0)
        );
        assert!(pool.allocate::<u8>(4).is_err());
        drop(retained);
        assert!(pool.allocate::<u8>(4).is_ok());
    }
}

#[test]
fn required_copies_need_negotiation_and_authority_but_invalid_dtypes_never_copy() {
    let pool = pool(1);
    let allocator = Arc::new(BoundOutputAllocator::new(Arc::new(pool.clone())));
    let mut worker = Worker::spawn_protocol(&pool, true);
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "python/output");
    for operation in [
        "adapt_private",
        "adapt_readonly",
        "adapt_strided",
        "adapt_heap",
    ] {
        let call = start(
            &worker,
            allocator.clone(),
            Some(strict.clone()),
            operation,
            None,
        );
        assert!(worker
            .complete(call)
            .unwrap_err()
            .contains("copy policy denied"));
        assert!(pool.allocate::<u8>(4).is_ok());
    }
    assert_eq!(
        (counts(&strict).rejected, counts(&strict).rejected_bytes),
        (4, 112)
    );
    let call = start(&worker, allocator.clone(), None, "adapt_private", None);
    assert!(worker
        .complete(call)
        .unwrap_err()
        .contains("copy policy denied"));
    let allow = CopyContext::new(CopyPolicy::AllowCopies, "python/output");
    for operation in ["adapt_f64", "adapt_endian"] {
        let call = start(
            &worker,
            allocator.clone(),
            Some(allow.clone()),
            operation,
            None,
        );
        let error = worker.complete(call).unwrap_err();
        assert!(error.contains("not compatible"), "{operation}: {error}");
        assert_eq!(counts(&allow), CopyCounts::default(), "{operation}");
    }
    assert_eq!(counts(&allow), CopyCounts::default());
    let call = start(
        &worker,
        allocator.clone(),
        Some(strict.clone()),
        "adapt_empty",
        None,
    );
    drop(worker.output(call));
    worker.complete(call).unwrap();
    assert_eq!(counts(&strict).completed, 0);
    drop(worker);
    let mut legacy = Worker::spawn(&pool);
    let call = start(
        &legacy,
        allocator,
        Some(allow.clone()),
        "adapt_private",
        None,
    );
    assert!(legacy
        .complete(call)
        .unwrap_err()
        .contains("not negotiated"));
    assert_eq!(counts(&allow), CopyCounts::default());
}
