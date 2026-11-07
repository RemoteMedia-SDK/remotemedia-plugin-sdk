#![cfg(all(feature = "test-peer", any(windows, target_os = "linux")))]
use remotemedia_shared_memory::{
    peer::{CallContext, CallKind, HostHandle, Limits, PeerConnection, PeerEvent},
    PeerId, PoolConfig, SessionId, SharedPool, SizeClass,
};
use remotemedia_types::{PayloadStorage, RuntimeData, TensorEncoding};
use std::process::Command;

#[path = "support/output_copies.rs"]
mod output_copies;
#[path = "support/selected_allocator.rs"]
mod selected_allocator;
#[path = "support/source_completion.rs"]
mod source_completion;
#[path = "support/pressure.rs"]
mod pressure;

fn pool(slots: usize) -> SharedPool {
    SharedPool::new(
        SessionId([57; 16]),
        PoolConfig {
            classes: vec![SizeClass { bytes: 4096, slots }],
            max_bytes: 4096 * slots,
            max_leases: 64,
        },
    )
    .unwrap()
}
fn spawn(pool: &SharedPool) -> (PeerConnection, HostHandle) {
    spawn_protocol(pool, false)
}

#[test]
fn process_resources_release_after_concurrent_close_or_startup_rejection() {
    use remotemedia_shared_memory::channel::{ProcessResources, WorkerProcess};
    use std::sync::Arc;
    for reject_startup in [false, true] {
        let owner = Arc::new(());
        let mut command = Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
        command.arg("peer");
        let process =
            WorkerProcess::spawn_with_resources(&mut command, ProcessResources::new(owner.clone()))
                .unwrap();
        let connected = unsafe {
            PeerConnection::from_process(
                process,
                pool(1),
                Limits::default(),
                if reject_startup { 99 } else { 0 },
                false,
            )
        };
        if reject_startup {
            assert!(connected.is_err());
        } else {
            let (mut peer, host) = connected.unwrap();
            assert!(matches!(
                peer.next_event().unwrap(),
                PeerEvent::Ready { .. }
            ));
            assert_eq!(Arc::strong_count(&owner), 2);
            std::thread::scope(|scope| {
                for _ in 0..8 {
                    scope.spawn(|| host.close());
                }
            });
            assert_eq!(
                Arc::strong_count(&owner),
                1,
                "closed peer retained process resources"
            );
            drop(peer);
        }
        assert_eq!(Arc::strong_count(&owner), 1);
    }
}

#[test]
fn returned_input_slices_preserve_exact_host_owner_and_provenance_across_two_workers() {
    use remotemedia_types::{BoundOutputAllocator, ForwardingScope};
    use std::sync::Arc;
    for audio in [false, true] {
        let pool = pool(1);
        let allocator = BoundOutputAllocator::new(Arc::new(pool.clone()));
        let first_scope = ForwardingScope::default();
        let second_scope = ForwardingScope::default();
        let (input, expected_address) = provenance_input(&allocator, audio);
        let (mut first, first_handle) = spawn(&pool);
        let (mut second, second_handle) = spawn(&pool);
        let call = start(
            &first_handle,
            CallKind::Process,
            "slice",
            vec![("input".into(), first_scope.accept(input))],
        );
        let sliced = output(&mut first, call).into_data();
        complete(&mut first, call).unwrap();
        assert!(first_scope.accepts_output(&sliced));
        assert!(allocator.owns_payload(&sliced));
        let call = start(
            &second_handle,
            CallKind::Process,
            "echo",
            vec![("input".into(), second_scope.accept(sliced))],
        );
        let retained = output(&mut second, call).into_data();
        complete(&mut second, call).unwrap();
        first_handle.close();
        second_handle.close();
        drop((first, second, first_handle, second_handle));
        assert!(first_scope.accepts_output(&retained));
        assert!(second_scope.accepts_output(&retained));
        assert!(allocator.owns_payload(&retained));
        match &retained {
            RuntimeData::Audio { samples, .. } => {
                assert_eq!(samples.as_ptr() as usize, expected_address);
                assert_eq!(&samples[..], &[0.75; 12]);
                let remotemedia_types::AudioSamples::Shared(samples) = samples else {
                    panic!("shared samples required")
                };
                assert_eq!(
                    pool.export(samples, PeerId(91))
                        .unwrap()
                        .descriptor()
                        .offset,
                    40
                );
            }
            RuntimeData::Binary(bytes) => {
                assert_eq!(bytes.as_ptr() as usize, expected_address);
                assert_eq!(&bytes[..], &[7; 12]);
                assert_eq!(
                    pool.export(bytes, PeerId(91)).unwrap().descriptor().offset,
                    10
                );
            }
            _ => panic!("expected bulk owner"),
        }
        assert!(pool.allocate::<u8>(1).is_err());
        drop(retained);
        assert!(pool.allocate::<u8>(4096).is_ok());
    }
}
fn spawn_protocol(pool: &SharedPool, snapshots: bool) -> (PeerConnection, HostHandle) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
    command.arg("peer");
    // SAFETY: the fixture uses WorkerEndpoint and never delegates or forks.
    let (mut peer, handle) = unsafe {
        let limits = Limits {
            outputs_per_call: 1,
            ..Limits::default()
        };
        if snapshots {
            PeerConnection::spawn_with_snapshots(&mut command, pool.clone(), limits)
        } else {
            PeerConnection::spawn(&mut command, pool.clone(), limits)
        }
    }
    .unwrap();
    assert!(matches!(peer.next_event().unwrap(), PeerEvent::Ready { modes } if modes.len() == 6));
    (peer, handle)
}

#[test]
fn returning_an_input_needs_no_additional_recipient_lease() {
    let pool = SharedPool::new(
        SessionId([58; 16]),
        PoolConfig {
            classes: vec![SizeClass {
                bytes: 4096,
                slots: 1,
            }],
            max_bytes: 4096,
            max_leases: 1,
        },
    )
    .unwrap();
    let input = bytes(&pool, &[9; 16]);
    let address = binary(&input).as_ptr();
    let (mut peer, handle) = spawn(&pool);
    let call = start(
        &handle,
        CallKind::Process,
        "echo",
        vec![("input".into(), input)],
    );
    // The worker holds the only recipient lease until this output is acknowledged.
    let output = output(&mut peer, call);
    assert_eq!(binary(output.data()).as_ptr(), address);
    let retained = output.into_data();
    complete(&mut peer, call).unwrap();
    handle.close();
    assert_eq!(binary(&retained), &[9; 16]);
    assert!(pool.allocate::<u8>(1).is_err());
    drop(retained);
    assert!(pool.allocate::<u8>(4096).is_ok());
}

#[test]
fn live_snapshot_reads_preserve_versions_and_leases_across_replacement_and_death() {
    use remotemedia_traits::ports::{OutputPort, SnapshotPort};
    use std::{collections::HashMap, sync::Arc};
    for audio in [false, true] {
        let pool = pool(2);
        let publication = OutputPort::empty();
        let make = |value: u8| {
            if audio {
                let mut loan = pool.allocate::<f32>(16).unwrap();
                loan.fill(value as f32);
                RuntimeData::Audio {
                    samples: loan.freeze().into(),
                    sample_rate: 48000,
                    channels: 2,
                    stream_id: None,
                    timestamp_us: Some(99),
                    arrival_ts_us: None,
                    metadata: None,
                }
            } else {
                bytes(&pool, &[value; 16])
            }
        };
        let allocation = |data: &RuntimeData| {
            let (_, payload) = data
                .clone()
                .into_shared_parts(TensorEncoding::Runtime)
                .unwrap();
            let grant = match payload {
                PayloadStorage::Audio(data) => pool.export(&data, PeerId(995)).unwrap(),
                PayloadStorage::Bytes(data) => pool.export(&data, PeerId(995)).unwrap(),
                _ => panic!(),
            };
            grant.descriptor().allocation
        };
        let admitted = remotemedia_types::ForwardingScope::default();
        let original = admitted.accept(make(7));
        let original_id = allocation(&original);
        publication.publish(original, 100);
        let (mut peer, handle) = spawn_protocol(&pool, true);
        let call = handle
            .start_call_with_snapshots(
                CallKind::Process,
                CallContext {
                    session_id: Some("snapshots".into()),
                    node_id: "reader".into(),
                },
                serde_json::json!({"operation":"snapshot_twice"}),
                vec![],
                None,
                HashMap::from([(
                    "latest".into(),
                    Arc::new(publication.input()) as Arc<dyn SnapshotPort>,
                )]),
            )
            .unwrap();
        let first = output(&mut peer, call);
        assert!(admitted.accepts_output(first.data()));
        assert_eq!(allocation(first.data()), original_id);
        let replacement = make(9);
        let replacement_id = allocation(&replacement);
        publication.publish(replacement, 200);
        let first = first.into_data();
        let second = output(&mut peer, call).into_data();
        let old_again = output(&mut peer, call).into_data();
        complete(&mut peer, call).unwrap();
        assert_eq!(allocation(&second), replacement_id);
        assert_eq!(allocation(&old_again), original_id);
        assert!(!admitted.accepts_output(&second));
        assert!(admitted.accepts_output(&old_again));
        publication.publish(RuntimeData::Text("released".into()), 300);
        drop((first, second, old_again));
        // The worker retains the first read beyond call completion.
        let spare = pool.allocate::<u8>(16).unwrap();
        assert!(pool.allocate::<u8>(1).is_err());
        handle.close();
        assert!(pool.allocate::<u8>(1).is_ok());
        drop(spare);
    }
}

#[test]
fn snapshot_negotiation_empty_reads_and_copy_policy_errors_are_explicit() {
    use remotemedia_traits::ports::{OutputPort, SnapshotPort};
    use remotemedia_types::{CopyContext, CopyPolicy};
    use std::{collections::HashMap, sync::Arc};
    let pool = pool(2);
    let publication = OutputPort::empty();
    let context = || CallContext {
        session_id: Some("snapshots".into()),
        node_id: "reader".into(),
    };
    let bindings = || {
        HashMap::from([(
            "latest".into(),
            Arc::new(publication.input()) as Arc<dyn SnapshotPort>,
        )])
    };
    let arguments = || serde_json::json!({"operation":"snapshot_once"});
    let (mut old, old_handle) = spawn(&pool);
    assert!(old_handle
        .start_call_with_snapshots(
            CallKind::Process,
            context(),
            arguments(),
            vec![],
            None,
            bindings()
        )
        .is_err());
    let plain = start(&old_handle, CallKind::Process, "echo", vec![]);
    complete(&mut old, plain).unwrap();
    old_handle.close();
    let (mut peer, handle) = spawn_protocol(&pool, true);
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "snapshot.input");
    let call = handle
        .start_call_with_snapshots(
            CallKind::Process,
            context(),
            arguments(),
            vec![],
            Some(&strict),
            bindings(),
        )
        .unwrap();
    assert!(
        matches!(output(&mut peer, call).into_data(), RuntimeData::Text(s) if s == "unpublished")
    );
    complete(&mut peer, call).unwrap();
    publication.publish(RuntimeData::Binary(vec![1, 2, 3, 4].into()), 1);
    let rejected = handle
        .start_call_with_snapshots(
            CallKind::Process,
            context(),
            arguments(),
            vec![],
            Some(&strict),
            bindings(),
        )
        .unwrap();
    assert!(complete(&mut peer, rejected).is_err());
    let allow = CopyContext::new(CopyPolicy::AllowCopies, "snapshot.compatibility");
    let compatible = handle
        .start_call_with_snapshots(
            CallKind::Process,
            context(),
            arguments(),
            vec![],
            Some(&allow),
            bindings(),
        )
        .unwrap();
    let result = output(&mut peer, compatible).into_data();
    assert_eq!(binary(&result), &[1, 2, 3, 4]);
    complete(&mut peer, compatible).unwrap();
    assert_eq!(
        strict
            .report()
            .entries
            .iter()
            .map(|(_, c)| c.rejected_bytes)
            .sum::<u64>(),
        4
    );
    assert_eq!(
        allow
            .report()
            .entries
            .iter()
            .map(|(_, c)| c.copied_bytes)
            .sum::<u64>(),
        4
    );
    handle.close();
    assert_eq!(binary(&result), &[1, 2, 3, 4]);
}
fn start(
    handle: &HostHandle,
    kind: CallKind,
    operation: &str,
    inputs: Vec<(String, RuntimeData)>,
) -> u64 {
    handle
        .start_call(
            kind,
            CallContext {
                session_id: Some("session-a".into()),
                node_id: "node-a".into(),
            },
            serde_json::json!({"operation": operation, "correlation": 731}),
            inputs,
        )
        .unwrap()
}
fn output(peer: &mut PeerConnection, call: u64) -> remotemedia_shared_memory::peer::ReceivedOutput {
    match peer.next_event().unwrap() {
        PeerEvent::Output(output) => {
            assert_eq!(output.call(), call);
            output
        }
        _ => panic!("expected output"),
    }
}
fn complete(peer: &mut PeerConnection, call: u64) -> Result<(), String> {
    match peer.next_event().unwrap() {
        PeerEvent::Complete {
            call: actual,
            result,
            ..
        } => {
            assert_eq!(actual, call);
            result
        }
        _ => panic!("expected completion"),
    }
}
fn bytes(pool: &SharedPool, values: &[u8]) -> RuntimeData {
    let mut loan = pool.allocate::<u8>(values.len()).unwrap();
    loan.copy_from_slice(values);
    RuntimeData::Binary(loan.freeze())
}
fn binary(data: &RuntimeData) -> &[u8] {
    match data {
        RuntimeData::Binary(data) => data,
        _ => panic!("expected binary"),
    }
}

#[test]
fn counted_private_ingress_then_two_worker_forwarding_copies_only_once() {
    let admitted = remotemedia_types::ForwardingScope::default();
    let input = admitted.accept(RuntimeData::Binary(vec![1, 2, 3, 4].into()));
    use remotemedia_types::{CopyContext, CopyPolicy, CopyReason};
    let pool = pool(2);
    let (mut first, first_handle) = spawn(&pool);
    let (mut second, second_handle) = spawn(&pool);
    let context = || CallContext {
        session_id: Some("session-a".into()),
        node_id: "node-a".into(),
    };
    let arguments = || serde_json::json!({"operation":"echo", "correlation":731});
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "first.input");
    assert!(first_handle
        .start_call_with_copy_context(
            CallKind::Process,
            context(),
            arguments(),
            vec![("input".into(), input.clone())],
            &strict
        )
        .is_err());
    let allow = CopyContext::new(CopyPolicy::AllowCopies, "first.input");
    let call = first_handle
        .start_call_with_copy_context(
            CallKind::Process,
            context(),
            arguments(),
            vec![("input".into(), input)],
            &allow,
        )
        .unwrap();
    let original = output(&mut first, call).into_data();
    assert!(
        !admitted.accepts_output(&original),
        "staging must not inherit the private input's proof"
    );
    complete(&mut first, call).unwrap();
    let RuntimeData::Binary(ref bytes) = original else {
        panic!("binary required")
    };
    let original_id = pool
        .export(bytes, PeerId(981))
        .unwrap()
        .descriptor()
        .allocation;
    let forwarding = CopyContext::new(CopyPolicy::RequireZeroCopy, "second.input");
    let call = second_handle
        .start_call_with_copy_context(
            CallKind::Process,
            context(),
            arguments(),
            vec![("input".into(), original)],
            &forwarding,
        )
        .unwrap();
    let retained = output(&mut second, call).into_data();
    complete(&mut second, call).unwrap();
    let RuntimeData::Binary(ref bytes) = retained else {
        panic!("binary required")
    };
    assert_eq!(
        pool.export(bytes, PeerId(982))
            .unwrap()
            .descriptor()
            .allocation,
        original_id
    );
    assert_eq!(binary(&retained), &[1, 2, 3, 4]);
    let copied = allow
        .report()
        .entries
        .into_iter()
        .find(|entry| entry.0 == CopyReason::PrivateIpcIngress)
        .unwrap()
        .1;
    assert_eq!((copied.completed, copied.copied_bytes), (1, 4));
    assert!(forwarding
        .report()
        .entries
        .iter()
        .all(|(_, count)| *count == Default::default()));
    assert_eq!(
        strict
            .report()
            .entries
            .into_iter()
            .find(|entry| entry.0 == CopyReason::PrivateIpcIngress)
            .unwrap()
            .1
            .rejected_bytes,
        4
    );
    first_handle.close();
    second_handle.close();
    drop((first, second));
    assert_eq!(binary(&retained), &[1, 2, 3, 4]);
}

#[test]
fn production_dispatch_transforms_audio_and_binary_and_retains_output_after_death() {
    verify_transform(spawn);
}

fn verify_transform(spawn: impl Fn(&SharedPool) -> (PeerConnection, HostHandle)) {
    for audio in [false, true] {
        let pool = pool(2);
        let (mut peer, handle) = spawn(&pool);
        let input = if audio {
            let mut loan = pool.allocate::<f32>(16).unwrap();
            for (i, sample) in loan.iter_mut().enumerate() {
                *sample = i as f32 * 0.5;
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
            bytes(&pool, &(0..16).collect::<Vec<_>>())
        };
        let expected_metadata = input
            .clone()
            .into_shared_parts(TensorEncoding::Runtime)
            .unwrap()
            .0;
        let call = start(
            &handle,
            CallKind::Process,
            "transform",
            vec![("input".into(), input)],
        );
        let output = output(&mut peer, call).into_data();
        complete(&mut peer, call).unwrap();
        let (metadata, storage) = output.into_shared_parts(TensorEncoding::Runtime).unwrap();
        assert_eq!(metadata, expected_metadata);
        // A received producer output remains forwardable by its original pool.
        let delivery = match &storage {
            PayloadStorage::Audio(data) => pool.export(data, PeerId(900)).unwrap(),
            PayloadStorage::Bytes(data) => pool.export(data, PeerId(900)).unwrap(),
            _ => unreachable!(),
        };
        drop(delivery);
        handle.close();
        drop(peer);
        match &storage {
            PayloadStorage::Audio(data) => {
                assert!(data.iter().enumerate().all(|(i, &v)| v == i as f32))
            }
            PayloadStorage::Bytes(data) => assert_eq!(&**data, &(1..=16).collect::<Vec<_>>()),
            _ => unreachable!(),
        }
        let spare = pool.allocate::<u8>(16).unwrap();
        assert!(pool.allocate::<u8>(16).is_err());
        drop(storage);
        assert!(pool.allocate::<u8>(16).is_ok());
        drop(spare);
    }
}

#[cfg(all(target_os = "linux", feature = "test-linux-namespace"))]
fn spawn_namespaced(pool: &SharedPool) -> (PeerConnection, HostHandle) {
    use remotemedia_shared_memory::channel::NamespaceCommand;
    use std::path::Path;
    let mut command = NamespaceCommand::new(
        Path::new(env!("CARGO_BIN_EXE_remotemedia-worker-supervisor")),
        Path::new("/usr/bin/unshare"),
        env!("CARGO_BIN_EXE_shared-memory-test-peer"),
    )
    .unwrap();
    command.command_mut().arg("peer");
    // SAFETY: matching trusted supervisor/fixture, no mapping delegation.
    let (mut peer, handle) = unsafe {
        PeerConnection::spawn_namespaced(&mut command, pool.clone(), Limits::default(), 0)
    }
    .unwrap();
    assert!(matches!(
        peer.next_event().unwrap(),
        PeerEvent::Ready { .. }
    ));
    (peer, handle)
}

#[cfg(all(target_os = "linux", feature = "test-linux-namespace"))]
#[test]
fn namespace_peer_transforms_and_keeps_host_outputs_alive_after_shutdown() {
    verify_transform(spawn_namespaced);
}

#[cfg(all(target_os = "linux", feature = "test-linux-namespace"))]
#[test]
fn namespace_peer_reclaims_retained_input_only_after_confirmed_shutdown() {
    let pool = pool(1);
    let (mut peer, handle) = spawn_namespaced(&pool);
    let input = bytes(&pool, &[9; 16]);
    let call = start(
        &handle,
        CallKind::Process,
        "echo",
        vec![("input".into(), input.clone())],
    );
    let echoed = output(&mut peer, call).into_data();
    complete(&mut peer, call).unwrap();
    // Independent mappings may use different virtual addresses. Compare backing
    // allocation and byte range, not pointer equality across mapping imports.
    let original = pool
        .prepare_message(input.clone(), PeerId(998), TensorEncoding::Runtime)
        .unwrap();
    let returned = pool
        .prepare_message(echoed.clone(), PeerId(999), TensorEncoding::Runtime)
        .unwrap();
    let original_descriptor = original.descriptor().buffer.as_ref().unwrap();
    let returned_descriptor = returned.descriptor().buffer.as_ref().unwrap();
    assert_eq!(
        original_descriptor.allocation,
        returned_descriptor.allocation
    );
    assert_eq!(original_descriptor.offset, returned_descriptor.offset);
    assert_eq!(original_descriptor.byte_len, returned_descriptor.byte_len);
    drop((original, returned));
    drop(echoed);
    let call = start(
        &handle,
        CallKind::Process,
        "hold",
        vec![("input".into(), input)],
    );
    complete(&mut peer, call).unwrap();
    assert!(pool.allocate::<u8>(16).is_err());
    handle.close();
    assert!(pool.allocate::<u8>(16).is_ok());
}

#[test]
fn slow_stream_consumer_does_not_block_control_and_slices_share_one_publication() {
    let pool = pool(1);
    let (mut peer, handle) = spawn(&pool);
    let stream = start(&handle, CallKind::Stream, "stream", vec![]);
    let first = output(&mut peer, stream);
    assert_eq!(binary(first.data()), &[2, 3, 4, 5]);
    let rpc = start(&handle, CallKind::Rpc, "rpc", vec![]);
    let response = output(&mut peer, rpc).into_data();
    assert!(
        matches!(response, RuntimeData::Json(value) if value == serde_json::json!({"session":"session-a", "node":"node-a", "correlation":731}))
    );
    complete(&mut peer, rpc).unwrap();
    let first = first.into_data();
    let second = output(&mut peer, stream).into_data();
    complete(&mut peer, stream).unwrap();
    assert_eq!(binary(&second), &[8, 9, 10, 11]);
    // Both outputs are slices of the same cached publication/mapping.
    assert_eq!(
        binary(&second).as_ptr() as usize - binary(&first).as_ptr() as usize,
        6
    );
    handle.close();
    assert_eq!(binary(&first), &[2, 3, 4, 5]);
    drop(first);
    assert!(pool.allocate::<u8>(16).is_err());
    drop(second);
    assert!(pool.allocate::<u8>(16).is_ok());
}

#[test]
fn retained_input_blocks_reuse_until_control_releases_it_and_budget_failure_is_recoverable() {
    let pool = pool(1);
    let (mut peer, handle) = spawn(&pool);
    let call = start(
        &handle,
        CallKind::Process,
        "hold",
        vec![("input".into(), bytes(&pool, &[1, 2, 3]))],
    );
    complete(&mut peer, call).unwrap();
    assert!(pool.allocate::<u8>(3).is_err());
    let source = start(&handle, CallKind::Tick, "source", vec![]);
    assert!(complete(&mut peer, source).is_err());
    let release = start(&handle, CallKind::Control, "release", vec![]);
    complete(&mut peer, release).unwrap();
    let source = start(&handle, CallKind::Tick, "source", vec![]);
    let data = output(&mut peer, source).into_data();
    complete(&mut peer, source).unwrap();
    assert_eq!(binary(&data), &(0..16).collect::<Vec<_>>());
    handle.close();
}

#[test]
fn cancellation_reaches_running_call_without_blocking_other_calls() {
    let pool = pool(1);
    let (mut peer, handle) = spawn(&pool);
    let waiting = start(&handle, CallKind::Stream, "wait", vec![]);
    assert!(
        matches!(output(&mut peer, waiting).into_data(), RuntimeData::Text(value) if value == "started")
    );
    handle.cancel(waiting).unwrap();
    let control = start(&handle, CallKind::Control, "unblock", vec![]);
    let mut completed = Vec::new();
    for _ in 0..2 {
        match peer.next_event().unwrap() {
            PeerEvent::Complete { call, result, .. } => {
                result.unwrap();
                completed.push(call);
            }
            _ => panic!("unexpected output"),
        }
    }
    completed.sort();
    assert_eq!(completed, vec![waiting, control]);
    handle.close();
}

#[test]
fn cancellation_releases_output_credit_without_invalidating_consumer_view() {
    let pool = pool(1);
    let (mut peer, handle) = spawn(&pool);
    let stream = start(&handle, CallKind::Stream, "stream", vec![]);
    let retained = output(&mut peer, stream);
    handle.cancel(stream).unwrap();
    assert!(complete(&mut peer, stream)
        .unwrap_err()
        .contains("cancelled"));
    assert_eq!(binary(retained.data()), &[2, 3, 4, 5]);
    handle.close();
    assert!(pool.allocate::<u8>(16).is_err());
    drop(retained);
    assert!(pool.allocate::<u8>(16).is_ok());
}

#[test]
fn private_buffers_are_rejected_without_poisoning_connection_and_concurrent_calls_keep_order() {
    let pool = pool(1);
    let (mut peer, handle) = spawn(&pool);
    let context = CallContext {
        session_id: None,
        node_id: "private".into(),
    };
    assert!(handle
        .start_call(
            CallKind::Process,
            context,
            serde_json::json!({}),
            vec![("input".into(), RuntimeData::Binary(vec![1].into()))]
        )
        .is_err());
    let private = start(&handle, CallKind::Process, "private", vec![]);
    assert!(complete(&mut peer, private).is_err());
    let threads: Vec<_> = (0..12)
        .map(|_| {
            let handle = handle.clone();
            std::thread::spawn(move || start(&handle, CallKind::Control, "release", vec![]))
        })
        .collect();
    let mut sent: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    let mut received = Vec::new();
    for _ in 0..12 {
        match peer.next_event().unwrap() {
            PeerEvent::Complete { call, result, .. } => {
                result.unwrap();
                received.push(call);
            }
            _ => panic!("unexpected event"),
        }
    }
    sent.sort();
    received.sort();
    assert_eq!(sent, received);
    let crash = start(&handle, CallKind::Process, "crash", vec![]);
    assert!(peer.next_event().is_err());
    assert!(handle.cancel(crash).is_err());
    assert!(pool.allocate::<u8>(1).is_ok());
}

#[test]
fn named_multi_input_pass_through_keeps_read_lease_views_after_worker_exit() {
    let pool = pool(2);
    let (mut peer, handle) = spawn(&pool);
    let left = bytes(&pool, &[0, 1, 2, 3, 4, 5]);
    let RuntimeData::Binary(left) = left else {
        unreachable!()
    };
    let call = start(
        &handle,
        CallKind::MultiInput,
        "echo",
        vec![
            (
                "left".into(),
                RuntimeData::Binary(left.slice(2..5).unwrap()),
            ),
            ("right".into(), bytes(&pool, &[9, 10])),
        ],
    );
    drop(left);
    let first = output(&mut peer, call).into_data();
    let second = output(&mut peer, call).into_data();
    complete(&mut peer, call).unwrap();
    handle.close();
    let mut values = vec![binary(&first).to_vec(), binary(&second).to_vec()];
    values.sort();
    assert_eq!(values, vec![vec![2, 3, 4], vec![9, 10]]);
    assert!(pool.allocate::<u8>(1).is_err());
    drop(first);
    drop(second);
    assert!(pool.allocate::<u8>(1).is_ok());
}

#[test]
fn full_duplex_large_metadata_cannot_deadlock_input_acknowledgements() {
    let pool = pool(1);
    let (mut peer, handle) = spawn(&pool);
    let (done, wait) = std::sync::mpsc::channel();
    let watchdog_handle = handle.clone();
    let watchdog = std::thread::spawn(move || {
        if wait
            .recv_timeout(std::time::Duration::from_secs(20))
            .is_err()
        {
            watchdog_handle.close();
        }
    });
    for _ in 0..4 {
        let large = start(&handle, CallKind::Process, "large", vec![]);
        // Deliberately fill both directions before consuming the output. A
        // receive task that writes acknowledgements directly into the same
        // blocked output pipe can deadlock while this call batch is being sent.
        let mut inputs = vec![("mapped".into(), bytes(&pool, &[7]))];
        inputs.extend((0..16).map(|i| (i.to_string(), RuntimeData::Text("x".repeat(128 * 1024)))));
        let control = start(&handle, CallKind::Control, "release", inputs);
        let mut completed = Vec::new();
        let mut saw_output = false;
        while completed.len() < 2 {
            match peer.next_event().unwrap() {
                PeerEvent::Output(output) => {
                    assert_eq!(output.call(), large);
                    assert!(
                        matches!(output.into_data(), RuntimeData::Text(value) if value.len() == 512 * 1024)
                    );
                    saw_output = true;
                }
                PeerEvent::Complete { call, result, .. } => {
                    result.unwrap();
                    completed.push(call);
                }
                _ => panic!("unexpected handshake"),
            }
        }
        completed.sort();
        assert_eq!(completed, vec![large, control]);
        assert!(saw_output);
    }
    done.send(()).unwrap();
    watchdog.join().unwrap();
    handle.close();
}

#[test]
fn forged_release_terminates_only_its_peer_and_keeps_other_owners_live() {
    let pool = pool(2);
    let (mut good, good_handle) = spawn(&pool);
    let source = start(&good_handle, CallKind::Tick, "source", vec![]);
    let retained = output(&mut good, source).into_data();
    complete(&mut good, source).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
    command.args(["peer", "forged-release"]);
    // SAFETY: the negative fixture neither maps nor delegates the granted file;
    // it injects an out-of-scope lease, never a premature acknowledgement.
    let (mut bad, bad_handle) =
        unsafe { PeerConnection::spawn(&mut command, pool.clone(), Limits::default()) }.unwrap();
    assert!(matches!(bad.next_event().unwrap(), PeerEvent::Ready { .. }));
    start(
        &bad_handle,
        CallKind::Process,
        "unused",
        vec![("input".into(), bytes(&pool, &[99]))],
    );
    assert!(bad
        .next_event()
        .err()
        .unwrap()
        .to_string()
        .contains("unknown or duplicate input release"));
    let reclaimed = pool.allocate::<u8>(16).unwrap();
    assert!(pool.allocate::<u8>(16).is_err());
    assert_eq!(binary(&retained), &(0..16).collect::<Vec<_>>());
    let rpc = start(&good_handle, CallKind::Rpc, "rpc", vec![]);
    drop(output(&mut good, rpc));
    complete(&mut good, rpc).unwrap();
    good_handle.close();
    drop(reclaimed);
    drop(retained);
    assert!(pool.allocate::<u8>(16).is_ok());
}

#[test]
fn snapshot_extension_cannot_be_silently_downgraded() {
    for outputs in [false, true] {
        let pool = pool(1);
        let mut command = Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
        // This existing raw fixture returns the old Ready shape without snapshots.
        command.args(["peer", "forged-release"]);
        // SAFETY: fixture never maps or delegates any handle.
        let (mut peer, handle) = unsafe {
            if outputs {
                PeerConnection::spawn_with_snapshot_outputs(&mut command, pool, Limits::default())
            } else {
                PeerConnection::spawn_with_snapshots(&mut command, pool, Limits::default())
            }
        }
        .unwrap();
        let error = peer.next_event().err().expect("downgrade must fail");
        assert!(error.to_string().contains("capability handshake"));
        handle.close();
    }
}

#[test]
fn cancelled_call_cannot_read_snapshots_and_other_calls_remain_live() {
    use remotemedia_traits::ports::{OutputPort, SnapshotPort};
    use std::{collections::HashMap, sync::Arc};
    let pool = pool(1);
    let port = OutputPort::empty();
    port.publish(bytes(&pool, &[7; 4]), 100);
    let (mut peer, handle) = spawn_protocol(&pool, true);
    let call = handle
        .start_call_with_snapshots(
            CallKind::Stream,
            CallContext {
                session_id: Some("snapshots".into()),
                node_id: "reader".into(),
            },
            serde_json::json!({"operation":"snapshot_after_cancel"}),
            vec![],
            None,
            HashMap::from([(
                "latest".into(),
                Arc::new(port.input()) as Arc<dyn SnapshotPort>,
            )]),
        )
        .unwrap();
    drop(output(&mut peer, call));
    handle.cancel(call).unwrap();
    let unblock = start(&handle, CallKind::Control, "unblock", vec![]);
    let mut completions = vec![];
    for _ in 0..2 {
        match peer.next_event().unwrap() {
            PeerEvent::Complete { call, result, .. } => {
                result.unwrap();
                completions.push(call);
            }
            _ => panic!("unexpected cancelled snapshot output"),
        }
    }
    completions.sort();
    let mut expected = vec![call, unblock];
    expected.sort();
    assert_eq!(completions, expected);
    let echo = start(&handle, CallKind::Process, "echo", vec![]);
    complete(&mut peer, echo).unwrap();
    handle.close();
}

#[test]
fn blocked_snapshot_reader_does_not_stall_control_or_cancellation() {
    use remotemedia_traits::ports::{SnapshotPort, TimestampedSnapshot};
    use std::{
        any::Any,
        collections::HashMap,
        sync::{mpsc, Arc, Condvar, Mutex},
        time::Duration,
    };
    struct SlowReader {
        entered: mpsc::Sender<()>,
        released: Arc<(Mutex<bool>, Condvar)>,
    }
    impl SnapshotPort for SlowReader {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn snapshot_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
            None
        }
        fn supports_runtime_data(&self) -> bool {
            true
        }
        fn runtime_snapshot(
            &self,
        ) -> Result<Option<Arc<TimestampedSnapshot<RuntimeData>>>, remotemedia_types::Error>
        {
            self.entered.send(()).unwrap();
            let (lock, changed) = self.released.as_ref();
            let ready = lock.lock().unwrap();
            let (ready, timeout) = changed
                .wait_timeout_while(ready, Duration::from_secs(5), |ready| !*ready)
                .unwrap();
            assert!(
                *ready && !timeout.timed_out(),
                "host snapshot reader was never released"
            );
            Ok(Some(Arc::new(TimestampedSnapshot {
                value: RuntimeData::Text("late".into()),
                pts_us: 1,
                written_at_us: 2,
                seq: 0,
            })))
        }
    }
    for cancelled in [false, true] {
        let pool = pool(1);
        let (mut peer, handle) = spawn_protocol(&pool, true);
        let (entered, reading) = mpsc::channel();
        let released = Arc::new((Mutex::new(false), Condvar::new()));
        let reader: Arc<dyn SnapshotPort> = Arc::new(SlowReader {
            entered,
            released: released.clone(),
        });
        let call = handle
            .start_call_with_snapshots(
                CallKind::Process,
                CallContext {
                    session_id: Some("slow-read".into()),
                    node_id: "reader".into(),
                },
                serde_json::json!({"operation":"snapshot_once"}),
                vec![],
                None,
                HashMap::from([("latest".into(), reader)]),
            )
            .unwrap();
        let (events, receive) = mpsc::channel();
        let pump = std::thread::spawn(move || {
            let mut completed = 0;
            while completed < 2 {
                let event = peer.next_event().unwrap();
                if matches!(event, PeerEvent::Complete { .. }) {
                    completed += 1;
                }
                events.send(event).unwrap();
            }
            peer
        });
        reading.recv_timeout(Duration::from_secs(2)).unwrap();
        if cancelled {
            handle.cancel(call).unwrap();
        }
        let control = start(&handle, CallKind::Rpc, "rpc", vec![]);
        match receive.recv_timeout(Duration::from_secs(2)).unwrap() {
            PeerEvent::Output(value) => {
                assert_eq!(value.call(), control);
                drop(value);
            }
            _ => panic!("control stalled behind snapshot read"),
        }
        match receive.recv_timeout(Duration::from_secs(2)).unwrap() {
            PeerEvent::Complete {
                call: actual,
                result,
                ..
            } => {
                assert_eq!(actual, control);
                result.unwrap();
            }
            _ => panic!("missing control completion"),
        }
        *released.0.lock().unwrap() = true;
        released.1.notify_all();
        if !cancelled {
            match receive.recv_timeout(Duration::from_secs(2)).unwrap() {
                PeerEvent::Output(value) => {
                    assert_eq!(value.call(), call);
                    drop(value);
                }
                _ => panic!("missing snapshot result"),
            }
        }
        match receive.recv_timeout(Duration::from_secs(2)).unwrap() {
            PeerEvent::Complete {
                call: actual,
                result,
                ..
            } => {
                assert_eq!(actual, call);
                assert_eq!(result.is_err(), cancelled);
            }
            _ => panic!("missing snapshot completion"),
        }
        let mut peer = pump.join().unwrap();
        let echo = start(&handle, CallKind::Process, "echo", vec![]);
        complete(&mut peer, echo).unwrap();
        handle.close();
    }
}

fn provenance_input(
    allocator: &remotemedia_types::BoundOutputAllocator,
    audio: bool,
) -> (RuntimeData, usize) {
    use remotemedia_types::OutputAllocator;
    if audio {
        let mut loan = allocator.allocate_audio(32).unwrap();
        loan.fill(0.75);
        let buffer = loan.freeze();
        let address = buffer.as_ptr() as usize + 10 * 4;
        (
            RuntimeData::Audio {
                samples: buffer.slice(8..24).unwrap().into(),
                sample_rate: 48000,
                channels: 2,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata: None,
            },
            address,
        )
    } else {
        let mut loan = allocator.allocate_bytes(32).unwrap();
        loan.fill(7);
        let buffer = loan.freeze();
        let address = buffer.as_ptr() as usize + 10;
        (RuntimeData::Binary(buffer.slice(8..24).unwrap()), address)
    }
}
