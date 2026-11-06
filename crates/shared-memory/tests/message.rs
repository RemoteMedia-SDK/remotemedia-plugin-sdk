use remotemedia_shared_memory::{
    import_message, MessageDelivery, MessageDescriptor, PeerId, PoolConfig, SessionId,
    SharedMemoryError, SharedPool, SizeClass,
};
use remotemedia_types::{ImageFormat, PixelFormat, RuntimeData, RuntimeMetadata, TensorEncoding};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

const SESSION: SessionId = SessionId([19; 16]);
const RECIPIENT: PeerId = PeerId(7);
const ENCODING: TensorEncoding = TensorEncoding::Runtime;

fn pool() -> SharedPool {
    SharedPool::new(
        SESSION,
        PoolConfig {
            classes: vec![SizeClass {
                bytes: 4096,
                slots: 1,
            }],
            max_bytes: 4096,
            max_leases: 4,
        },
    )
    .unwrap()
}

fn audio(pool: &SharedPool) -> RuntimeData {
    let mut loan = pool.allocate::<f32>(16).unwrap();
    loan.fill(0.75);
    RuntimeData::Audio {
        samples: loan.freeze().into(),
        sample_rate: 48000,
        channels: 2,
        stream_id: Some("speech".into()),
        timestamp_us: Some(8),
        arrival_ts_us: Some(9),
        metadata: Some(serde_json::json!({"speaker":2})),
    }
}

fn receive(pool: &SharedPool, data: RuntimeData) -> (RuntimeData, Arc<AtomicUsize>) {
    let prepared = pool.prepare_message(data, RECIPIENT, ENCODING).unwrap();
    receive_prepared(pool, prepared)
}

fn receive_prepared(
    pool: &SharedPool,
    prepared: MessageDelivery,
) -> (RuntimeData, Arc<AtomicUsize>) {
    let metadata = serde_json::to_vec(prepared.descriptor()).unwrap();
    assert!(
        metadata.len() < 1024,
        "descriptor accidentally contains payload"
    );
    // SAFETY: this fixture owns both ends, transfers only the matching private
    // file and keeps the broker lease until the import's final unmap callback.
    let (_, file) = unsafe { prepared.into_transport() };
    let descriptor: MessageDescriptor = serde_json::from_slice(&metadata).unwrap();
    let lease = descriptor.buffer.clone();
    if let Some(lease) = &lease {
        pool.accept(lease).unwrap();
    }
    let released = Arc::new(AtomicUsize::new(0));
    let counter = released.clone();
    let owner = pool.clone();
    // SAFETY: same owned grant/recipient binding as above. The callback is not
    // exposed to user code and cannot run while any imported view exists.
    let data = unsafe {
        import_message(descriptor, file, SESSION, RECIPIENT, ENCODING, move || {
            if let Some(lease) = lease {
                owner.release_remote(&lease).unwrap();
            }
            counter.fetch_add(1, Ordering::SeqCst);
        })
    }
    .unwrap();
    (data, released)
}

#[test]
fn private_ingress_is_rejected_or_copied_once_with_visible_accounting() {
    use remotemedia_types::{CopyContext, CopyPolicy, CopyReason};
    for is_audio in [false, true] {
        let pool = pool();
        let input = if is_audio {
            RuntimeData::Audio {
                samples: vec![1.0f32; 4].into(),
                sample_rate: 48000,
                channels: 1,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata: None,
            }
        } else {
            RuntimeData::Binary(vec![1u8; 4].into())
        };
        let bytes = if is_audio { 16 } else { 4 };
        assert!(!pool.owns_payload(&input));
        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "session.node.input");
        assert!(matches!(
            pool.prepare_message_with_copy_context(input.clone(), RECIPIENT, ENCODING, &strict),
            Err(SharedMemoryError::CopyRejected(_))
        ));
        let report = strict.report();
        let counts = report
            .entries
            .iter()
            .find(|entry| entry.0 == CopyReason::PrivateIpcIngress)
            .unwrap()
            .1;
        assert_eq!((counts.rejected_bytes, counts.copied_bytes), (bytes, 0));
        let allow = CopyContext::new(CopyPolicy::AllowCopies, "session.node.input");
        let delivery = pool
            .prepare_message_with_copy_context(input.clone(), RECIPIENT, ENCODING, &allow)
            .unwrap();
        let id = delivery.descriptor().buffer.as_ref().unwrap().allocation;
        let (output, released) = receive_prepared(&pool, delivery);
        assert_eq!(output, input);
        let forwarded = pool
            .prepare_message_with_copy_context(output.clone(), PeerId(8), ENCODING, &allow)
            .unwrap();
        assert_eq!(
            forwarded.descriptor().buffer.as_ref().unwrap().allocation,
            id
        );
        let counts = allow
            .report()
            .entries
            .into_iter()
            .find(|entry| entry.0 == CopyReason::PrivateIpcIngress)
            .unwrap()
            .1;
        assert_eq!((counts.completed, counts.copied_bytes), (1, bytes));
        drop(forwarded);
        drop(output);
        assert_eq!(released.load(Ordering::SeqCst), 1);
        assert!(pool.allocate::<u8>(4096).is_ok());
    }
}

#[test]
fn foreign_pool_copy_is_explicit_and_validation_or_budget_errors_never_trigger_fallback() {
    use remotemedia_types::{CopyContext, CopyPolicy, CopyReason};
    let target = pool();
    // A distinct pool incarnation must not be mistaken for exportable storage,
    // even if both pools happen to use the same external session label.
    let source = pool();
    let mut loan = source.allocate::<u8>(4).unwrap();
    loan.copy_from_slice(&[1, 2, 3, 4]);
    let data = RuntimeData::Binary(loan.freeze());
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "foreign.input");
    assert!(matches!(
        target.prepare_message_with_copy_context(data.clone(), RECIPIENT, ENCODING, &strict),
        Err(SharedMemoryError::CopyRejected(_))
    ));
    assert_eq!(
        strict
            .report()
            .entries
            .into_iter()
            .find(|entry| entry.0 == CopyReason::ForeignPoolIngress)
            .unwrap()
            .1
            .rejected_bytes,
        4
    );
    let allow = CopyContext::new(CopyPolicy::AllowCopies, "foreign.input");
    let occupied = target.allocate::<u8>(4).unwrap();
    assert!(matches!(
        target.prepare_message_with_copy_context(data.clone(), RECIPIENT, ENCODING, &allow),
        Err(SharedMemoryError::Exhausted)
    ));
    assert!(allow
        .report()
        .entries
        .iter()
        .all(|(_, count)| count.completed == 0));
    drop(occupied);
    let delivery = target
        .prepare_message_with_copy_context(data, RECIPIENT, ENCODING, &allow)
        .unwrap();
    let (copied, _) = receive_prepared(&target, delivery);
    assert_eq!(copied, RuntimeData::Binary(vec![1, 2, 3, 4].into()));
    assert_eq!(
        allow
            .report()
            .entries
            .into_iter()
            .find(|entry| entry.0 == CopyReason::ForeignPoolIngress)
            .unwrap()
            .1
            .copied_bytes,
        4
    );
    drop(copied);
    let invalid = RuntimeData::Audio {
        samples: vec![1.0f32; 3].into(),
        sample_rate: 48000,
        channels: 2,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    };
    assert!(matches!(
        target.prepare_message_with_copy_context(invalid, RECIPIENT, ENCODING, &allow),
        Err(SharedMemoryError::PayloadLayout(_))
    ));
    assert!(allow
        .report()
        .entries
        .iter()
        .filter(|(reason, _)| *reason != CopyReason::ForeignPoolIngress)
        .all(|(_, count)| count.completed == 0));
}

#[test]
fn mapped_audio_message_survives_session_close_and_forwards_same_storage() {
    let pool = pool();
    let produced = audio(&pool);
    assert!(pool.owns_payload(&produced));
    let (input, released) = receive(&pool, produced);
    assert!(!pool.owns_payload(&input)); // Raw imported descriptor is not host ownership.
    let RuntimeData::Audio {
        samples,
        sample_rate,
        channels,
        timestamp_us,
        ..
    } = input
    else {
        panic!()
    };
    assert_eq!((sample_rate, channels, timestamp_us), (48000, 2, Some(8)));
    let retained = samples.into_shared().slice(4..8).unwrap();
    assert_eq!(retained.as_ref(), &[0.75; 4]);
    let forwarded = pool.export(&retained, PeerId(8)).unwrap();
    assert_eq!(forwarded.descriptor().offset, 16);
    assert_eq!(forwarded.descriptor().byte_len, 16);
    pool.close();
    assert_eq!(released.load(Ordering::SeqCst), 0);
    assert_eq!(retained.as_ref(), &[0.75; 4]);
    drop(forwarded);
    drop(retained);
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[test]
fn all_byte_payloads_roundtrip_metadata_with_mapped_owners() {
    for variant in 0..5 {
        let pool = pool();
        let mut loan = pool.allocate::<u8>(24).unwrap();
        for (index, byte) in loan.iter_mut().enumerate() {
            *byte = index as u8;
        }
        let data = loan.freeze();
        let input = match variant {
            0 => RuntimeData::Binary(data),
            1 => RuntimeData::Video {
                pixel_data: data,
                width: 4,
                height: 4,
                format: PixelFormat::NV12,
                codec: None,
                frame_number: 42,
                timestamp_us: 1000,
                is_keyframe: true,
                stream_id: Some("camera".into()),
                arrival_ts_us: Some(1001),
            },
            2 => RuntimeData::Image {
                data,
                format: ImageFormat::Raw {
                    pixel_format: PixelFormat::Rgb24,
                },
                width: 4,
                height: 2,
                timestamp_us: Some(2),
                stream_id: Some("still".into()),
                metadata: Some(serde_json::json!({"tag":"still"})),
            },
            3 => RuntimeData::Tensor {
                data,
                shape: vec![2, 3],
                dtype: 0,
                metadata: Some(serde_json::json!({"layer":3})),
            },
            _ => RuntimeData::Numpy {
                data,
                shape: vec![2, 3],
                dtype: "f4".into(),
                strides: vec![4, 8],
                c_contiguous: false,
                f_contiguous: true,
            },
        };
        assert!(pool.owns_payload(&input));
        let expected = input.clone();
        let (output, released) = receive(&pool, input);
        assert_eq!(output, expected);
        drop(expected);
        let (_, storage) = output.into_shared_parts(ENCODING).unwrap();
        let remotemedia_types::PayloadStorage::Bytes(buffer) = storage else {
            panic!()
        };
        assert_eq!(buffer.as_ref(), &(0..24).collect::<Vec<u8>>());
        assert!(pool.export(&buffer, PeerId(8)).is_ok());
        assert!(matches!(
            pool.allocate::<u8>(1),
            Err(SharedMemoryError::Exhausted)
        ));
        drop(buffer);
        assert_eq!(released.load(Ordering::SeqCst), 1);
        assert!(pool.allocate::<u8>(24).is_ok());
    }
}

#[test]
fn invalid_metadata_releases_accepted_grant_once_without_exposing_view() {
    let pool = pool();
    let prepared = pool
        .prepare_message(audio(&pool), RECIPIENT, ENCODING)
        .unwrap();
    // SAFETY: exact local grant, retained until the rejection callback below.
    let (mut descriptor, file) = unsafe { prepared.into_transport() };
    let lease = descriptor.buffer.clone().unwrap();
    pool.accept(&lease).unwrap();
    if let RuntimeMetadata::Audio { channels, .. } = &mut descriptor.metadata {
        *channels = 3;
    }
    let owner = pool.clone();
    let released = Arc::new(AtomicUsize::new(0));
    let counter = released.clone();
    // SAFETY: release is bound to the original accepted grant, not the malformed
    // incoming metadata. No reader exists if pre-mapping validation fails.
    let result = unsafe {
        import_message(descriptor, file, SESSION, RECIPIENT, ENCODING, move || {
            owner.release_remote(&lease).unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
        })
    };
    assert!(result.is_err());
    assert_eq!(released.load(Ordering::SeqCst), 1);
    assert!(pool.allocate::<u8>(1).is_ok());
}

#[test]
fn envelope_rejects_mismatched_session_peer_version_handle_and_payload_kind() {
    let pool = pool();
    let prepared = pool
        .prepare_message(audio(&pool), RECIPIENT, ENCODING)
        .unwrap();
    let descriptor = prepared.descriptor();
    assert!(descriptor
        .validate(SESSION, RECIPIENT, Some(4096), ENCODING)
        .is_ok());
    assert!(descriptor
        .validate(SessionId([0; 16]), RECIPIENT, Some(4096), ENCODING)
        .is_err());
    assert!(descriptor
        .validate(SESSION, PeerId(8), Some(4096), ENCODING)
        .is_err());
    assert!(descriptor
        .validate(SESSION, RECIPIENT, None, ENCODING)
        .is_err());
    assert!(descriptor
        .validate(SESSION, RECIPIENT, Some(4), ENCODING)
        .is_err());
    for mutate in 0..5 {
        let mut bad = descriptor.clone();
        match mutate {
            0 => bad.version += 1,
            1 => bad.buffer.as_mut().unwrap().recipient = PeerId(9),
            2 => bad.buffer.as_mut().unwrap().session = SessionId([1; 16]),
            3 => bad.metadata = RuntimeMetadata::Binary,
            _ => bad.buffer = None,
        }
        assert!(bad
            .validate(SESSION, RECIPIENT, Some(4096), ENCODING)
            .is_err());
    }
}

#[test]
fn unsent_messages_cancel_and_private_payloads_do_not_materialize() {
    let pool = pool();
    let prepared = pool
        .prepare_message(audio(&pool), RECIPIENT, ENCODING)
        .unwrap();
    assert!(matches!(
        pool.allocate::<u8>(1),
        Err(SharedMemoryError::Exhausted)
    ));
    drop(prepared);
    assert!(pool.allocate::<u8>(1).is_ok());
    assert!(pool
        .prepare_message(RuntimeData::Binary(vec![1; 24].into()), RECIPIENT, ENCODING)
        .is_err());
    let (text, released) = receive(&pool, RuntimeData::Text("control only".into()));
    assert_eq!(text, RuntimeData::Text("control only".into()));
    assert_eq!(released.load(Ordering::SeqCst), 1);
}
