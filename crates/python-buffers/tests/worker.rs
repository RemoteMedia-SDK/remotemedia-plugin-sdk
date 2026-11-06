#![cfg(all(feature = "test-worker", any(windows, target_os = "linux")))]
use remotemedia_shared_memory::{
    peer::{CallContext, CallKind, HostHandle, Limits, PeerConnection, PeerEvent, ReceivedOutput},
    PeerId, PoolConfig, SessionId, SharedPool, SizeClass,
};
use remotemedia_types::{
    ControlMessageType, ImageFormat, PayloadStorage, PixelFormat, RuntimeData, TensorEncoding,
};
use std::{process::Command, sync::mpsc, time::Duration};

struct Worker {
    peer: PeerConnection,
    handle: HostHandle,
    watchdog: mpsc::Sender<()>,
}
impl Worker {
    fn spawn(pool: &SharedPool) -> Self {
        // An optional interpreter path exercises the exact same fixture against
        // an installed wheel. Otherwise use the child embedding the real module.
        let mut command = if let Ok(python) = std::env::var("MAPPED_WORKER_PYTHON") {
            let mut command = Command::new(python);
            command.arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/support/python_worker.py"
            ));
            command
        } else {
            Command::new(env!("CARGO_BIN_EXE_python-buffer-test-worker"))
        };
        // SAFETY: our fixture claims the private endpoint once, only exports
        // owning buffers, and does not fork or delegate handles/mappings.
        let (mut peer, handle) =
            unsafe { PeerConnection::spawn(&mut command, pool.clone(), Limits::default()) }
                .unwrap();
        let (watchdog, wait) = mpsc::channel();
        let timeout_handle = handle.clone();
        std::thread::spawn(move || {
            if wait.recv_timeout(Duration::from_secs(20)).is_err() {
                timeout_handle.close();
            }
        });
        assert!(
            matches!(peer.next_event().unwrap(), PeerEvent::Ready { modes } if modes.len() == 8)
        );
        Self {
            peer,
            handle,
            watchdog,
        }
    }
    fn start(&self, kind: CallKind, operation: &str, input: Option<RuntimeData>) -> u64 {
        self.handle
            .start_call(
                kind,
                CallContext {
                    session_id: Some("python-session".into()),
                    node_id: "numpy-node".into(),
                },
                serde_json::json!({"operation": operation}),
                input
                    .into_iter()
                    .map(|data| ("input".into(), data))
                    .collect(),
            )
            .unwrap()
    }
    fn output(&mut self, call: u64) -> ReceivedOutput {
        match self.peer.next_event().unwrap() {
            PeerEvent::Output(output) => {
                assert_eq!(output.call(), call);
                output
            }
            _ => panic!("expected output"),
        }
    }
    fn complete(&mut self, expected: u64) -> Result<(), String> {
        match self.peer.next_event().unwrap() {
            PeerEvent::Complete { call, result } => {
                assert_eq!(call, expected);
                result
            }
            _ => panic!("expected completion"),
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.handle.close();
        let _ = self.watchdog.send(());
    }
}
fn pool(slots: usize) -> SharedPool {
    SharedPool::new(
        SessionId([81; 16]),
        PoolConfig {
            classes: vec![SizeClass { bytes: 4096, slots }],
            max_bytes: 4096 * slots,
            max_leases: 64,
        },
    )
    .unwrap()
}
fn bytes(pool: &SharedPool) -> RuntimeData {
    let mut loan = pool.allocate::<u8>(16).unwrap();
    for (i, value) in loan.iter_mut().enumerate() {
        *value = i as u8;
    }
    RuntimeData::Binary(loan.freeze())
}
fn binary(data: &RuntimeData) -> &[u8] {
    match data {
        RuntimeData::Binary(bytes) => bytes,
        _ => panic!("expected binary"),
    }
}

#[test]
fn numpy_produces_directly_and_transforms_audio_and_bytes() {
    for audio in [false, true] {
        let pool = pool(3);
        let mut worker = Worker::spawn(&pool);
        let input = if audio {
            let mut loan = pool.allocate::<f32>(16).unwrap();
            for (i, sample) in loan.iter_mut().enumerate() {
                *sample = i as f32;
            }
            RuntimeData::Audio {
                samples: loan.freeze().into(),
                sample_rate: 48000,
                channels: 2,
                stream_id: Some("voice".into()),
                timestamp_us: Some(31),
                arrival_ts_us: None,
                metadata: None,
            }
        } else {
            bytes(&pool)
        };
        let expected = input
            .clone()
            .into_shared_parts(TensorEncoding::Runtime)
            .unwrap()
            .0;
        let call = worker.start(CallKind::Process, "transform", Some(input));
        let output = worker.output(call).into_data();
        worker.complete(call).unwrap();
        let (metadata, storage) = output.into_shared_parts(TensorEncoding::Runtime).unwrap();
        assert_eq!(metadata, expected);
        match &storage {
            PayloadStorage::Audio(data) => {
                assert!(data
                    .iter()
                    .enumerate()
                    .all(|(i, &value)| value == i as f32 * 2.0));
                drop(pool.export(data, PeerId(900)).unwrap());
            }
            PayloadStorage::Bytes(data) => {
                assert!(data
                    .iter()
                    .enumerate()
                    .all(|(i, &value)| value == i as u8 * 2));
                drop(pool.export(data, PeerId(900)).unwrap());
            }
            _ => unreachable!(),
        }
        let source = worker.start(CallKind::Tick, "source", None);
        let produced = worker.output(source).into_data();
        worker.complete(source).unwrap();
        drop(worker);
        assert_eq!(binary(&produced), &(0..16).collect::<Vec<_>>());
        assert_eq!(storage.layout().1, if audio { 64 } else { 16 });
    }
}

#[test]
fn pass_through_preserves_original_allocation_and_metadata() {
    let pool = pool(1);
    let mut worker = Worker::spawn(&pool);
    let input = bytes(&pool);
    let descriptor = pool
        .prepare_message(input.clone(), PeerId(901), TensorEncoding::Runtime)
        .unwrap();
    let call = worker.start(CallKind::Process, "pass", Some(input));
    let output = worker.output(call).into_data();
    worker.complete(call).unwrap();
    let forwarded = pool
        .prepare_message(output.clone(), PeerId(902), TensorEncoding::Runtime)
        .unwrap();
    assert_eq!(
        descriptor.descriptor().buffer.as_ref().unwrap().allocation,
        forwarded.descriptor().buffer.as_ref().unwrap().allocation
    );
    assert!(pool.allocate::<u8>(16).is_err());
    drop(descriptor);
    drop(forwarded);
    drop(worker);
    assert_eq!(binary(&output), &(0..16).collect::<Vec<_>>());
    drop(output);
    assert!(pool.allocate::<u8>(16).is_ok());
}

#[test]
fn every_bulk_variant_and_control_metadata_roundtrip_through_python_views() {
    let pool = pool(1);
    let mut worker = Worker::spawn(&pool);
    let mut loan = pool.allocate::<u8>(24).unwrap();
    for (index, value) in loan.iter_mut().enumerate() {
        *value = index as u8;
    }
    let data = loan.freeze();
    let cases = vec![
        RuntimeData::Video {
            pixel_data: data.clone(),
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
        RuntimeData::Image {
            data: data.clone(),
            format: ImageFormat::Raw {
                pixel_format: PixelFormat::Rgb24,
            },
            width: 4,
            height: 2,
            timestamp_us: Some(2),
            stream_id: Some("still".into()),
            metadata: Some(serde_json::json!({"tag":"still"})),
        },
        RuntimeData::Tensor {
            data: data.clone(),
            shape: vec![2, 3],
            dtype: 0,
            metadata: Some(serde_json::json!({"layer":3})),
        },
        RuntimeData::Numpy {
            data,
            shape: vec![2, 3],
            dtype: "f4".into(),
            strides: vec![4, 8],
            c_contiguous: false,
            f_contiguous: true,
        },
        RuntimeData::Json(serde_json::json!({"data_type":"audio","value":[1,2,3]})),
        RuntimeData::Text("text".into()),
        RuntimeData::ControlMessage {
            message_type: ControlMessageType::CancelSpeculation {
                from_timestamp: 10,
                to_timestamp: 20,
            },
            segment_id: Some("seg".into()),
            timestamp_ms: 9,
            metadata: serde_json::json!({"reason":"cancel"}),
        },
        RuntimeData::File {
            path: "source.bin".into(),
            filename: Some("source.bin".into()),
            mime_type: Some("application/octet-stream".into()),
            size: Some(30),
            offset: Some(2),
            length: Some(8),
            stream_id: Some("file".into()),
        },
    ];
    for expected in cases {
        let original = pool
            .prepare_message(expected.clone(), PeerId(901), TensorEncoding::Runtime)
            .unwrap();
        let call = worker.start(CallKind::Process, "pass", Some(expected.clone()));
        let output = worker.output(call).into_data();
        worker.complete(call).unwrap();
        assert_eq!(output, expected);
        let forwarded = pool
            .prepare_message(output, PeerId(902), TensorEncoding::Runtime)
            .unwrap();
        assert_eq!(
            original.descriptor().buffer.as_ref().map(|b| b.allocation),
            forwarded.descriptor().buffer.as_ref().map(|b| b.allocation)
        );
    }
}

#[test]
fn blocked_python_output_releases_gil_for_control_and_cancellation() {
    let pool = pool(1);
    let mut worker = Worker::spawn(&pool);
    let stream = worker.start(CallKind::Stream, "stream", None);
    let first = worker.output(stream); // Keep the ack withheld while Python waits.
    let rpc = worker.start(CallKind::Rpc, "rpc", None);
    let reply = worker.output(rpc).into_data();
    assert!(
        matches!(reply, RuntimeData::Json(value) if value == serde_json::json!({
            "session_id":"python-session", "node_id":"numpy-node"
        }))
    );
    worker.complete(rpc).unwrap();
    let first = first.into_data();
    let second = worker.output(stream).into_data();
    worker.complete(stream).unwrap();
    assert_eq!(
        binary(&second).as_ptr() as usize - binary(&first).as_ptr() as usize,
        6
    );
    let wait = worker.start(CallKind::Stream, "wait", None);
    drop(worker.output(wait).into_data());
    worker.handle.cancel(wait).unwrap();
    worker.complete(wait).unwrap();
    drop(worker);
    assert_eq!(binary(&first), &[2, 3, 4, 5]);
    assert_eq!(binary(&second), &[8, 9, 10, 11]);
}

#[test]
fn private_strided_and_invalid_layout_outputs_fail_without_poisoning_connection() {
    let pool = pool(2);
    let mut worker = Worker::spawn(&pool);
    for operation in ["private", "strided", "bad_shape"] {
        let call = worker.start(CallKind::Process, operation, Some(bytes(&pool)));
        assert!(worker.complete(call).unwrap_err().contains("BufferError"));
    }
    let source = worker.start(CallKind::Tick, "source", None);
    let output = worker.output(source).into_data();
    worker.complete(source).unwrap();
    assert_eq!(binary(&output), &(0..16).collect::<Vec<_>>());
}

#[test]
fn escaped_numpy_input_pins_mapping_after_call_completion() {
    let pool = pool(1);
    let mut worker = Worker::spawn(&pool);
    let hold = worker.start(CallKind::Process, "hold", Some(bytes(&pool)));
    worker.complete(hold).unwrap();
    assert!(pool.allocate::<u8>(16).is_err());
    let source = worker.start(CallKind::Tick, "source", None);
    assert!(worker.complete(source).unwrap_err().contains("BufferError"));
    let release = worker.start(CallKind::Control, "release", None);
    worker.complete(release).unwrap();
    let source = worker.start(CallKind::Tick, "source", None);
    let output = worker.output(source).into_data();
    worker.complete(source).unwrap();
    drop(worker);
    assert_eq!(binary(&output), &(0..16).collect::<Vec<_>>());
}
