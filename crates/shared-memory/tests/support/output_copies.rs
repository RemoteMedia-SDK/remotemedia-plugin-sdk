use super::selected_allocator::Selected;
use super::*;
use remotemedia_shared_memory::peer::CallResources;
use remotemedia_types::{CopyContext, CopyCounts, CopyPolicy, CopyReason};
use std::sync::{atomic::Ordering, Arc};

fn copying_peer(pool: &SharedPool) -> (PeerConnection, HostHandle) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
    command.arg("peer");
    // SAFETY: the fixture retains no delegated handles and reports copy results.
    let (mut peer, handle) = unsafe {
        PeerConnection::spawn_with_output_copies(
            &mut command,
            pool.clone(),
            Limits {
                outputs_per_call: 1,
                ..Limits::default()
            },
            0,
        )
    }
    .unwrap();
    assert!(matches!(
        peer.next_event().unwrap(),
        PeerEvent::Ready { .. }
    ));
    (peer, handle)
}

fn begin(
    handle: &HostHandle,
    provider: Arc<Selected>,
    copies: Option<CopyContext>,
    arguments: serde_json::Value,
) -> u64 {
    handle
        .start_call_with_resources(
            CallKind::Process,
            CallContext {
                session_id: Some("copies".into()),
                node_id: "producer".into(),
            },
            arguments,
            vec![],
            CallResources {
                copies: Some(CopyContext::new(CopyPolicy::AllowCopies, "input/only")),
                output_copies: copies,
                output_allocator: Some(provider),
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
fn host_bounds_pending_copy_grants_and_checks_extent_overflow_before_authorization() {
    for behavior in ["credit", "overflow"] {
        let pool = pool(2);
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "output");
        let mut command = Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
        command.args(["peer", "invalid-copy"]);
        // SAFETY: the fixture leaves grants unmapped and releases them in order.
        let (mut peer, handle) = unsafe {
            PeerConnection::spawn_with_output_copies(
                &mut command,
                pool.clone(),
                Limits {
                    outputs_per_call: 1,
                    ..Limits::default()
                },
                0,
            )
        }
        .unwrap();
        assert!(matches!(
            peer.next_event().unwrap(),
            PeerEvent::Ready { .. }
        ));
        let call = begin(
            &handle,
            Selected::new(&pool),
            Some(copies.clone()),
            serde_json::json!({"behavior":behavior}),
        );
        complete(&mut peer, call).unwrap();
        let count = counts(&copies);
        assert_eq!(
            (count.failed, count.failed_requested_bytes, count.in_flight),
            (1, 16, 0)
        );
        let _first = pool.allocate::<u8>(16).unwrap();
        let _second = pool.allocate::<u8>(16).unwrap();
        handle.close();
    }
}

#[test]
fn legacy_handshake_serialization_omits_the_optional_output_copy_extension() {
    use remotemedia_shared_memory::peer::wire::{HostMessage, WorkerMessage, WIRE_VERSION};
    let hello = HostMessage::Hello {
        version: WIRE_VERSION,
        session: SessionId([1; 16]),
        peer: PeerId(1),
        encoding: TensorEncoding::Runtime,
        limits: Limits::default(),
        snapshot_protocol: 0,
        output_copy_protocol: 0,
    };
    let ready = WorkerMessage::Ready {
        version: WIRE_VERSION,
        session: SessionId([1; 16]),
        peer: PeerId(1),
        modes: vec![CallKind::Process],
        snapshot_protocol: 0,
        output_copy_protocol: 0,
    };
    let hello = serde_json::to_value(hello).unwrap();
    let ready = serde_json::to_value(ready).unwrap();
    assert!(!hello["Hello"]
        .as_object()
        .unwrap()
        .contains_key("output_copy_protocol"));
    assert!(!ready["Ready"]
        .as_object()
        .unwrap()
        .contains_key("output_copy_protocol"));
    assert!(matches!(
        serde_json::from_value::<HostMessage>(hello).unwrap(),
        HostMessage::Hello {
            output_copy_protocol: 0,
            ..
        }
    ));
    assert!(matches!(
        serde_json::from_value::<WorkerMessage>(ready).unwrap(),
        WorkerMessage::Ready {
            output_copy_protocol: 0,
            ..
        }
    ));
}

#[test]
fn malformed_copy_transitions_are_rejected_and_pending_attempts_are_failed() {
    for (behavior, expected) in [
        ("wrong-call", "wrong call or is duplicated"),
        ("duplicate", "wrong call or is duplicated"),
        ("complete", "pending operation acknowledgement"),
        ("release", "before copy completion"),
        ("pending-output", "before successful copy completion"),
        ("failed-output", "before successful copy completion"),
    ] {
        let pool = pool(1);
        let provider = Selected::new(&pool);
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "output");
        let mut command = Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
        command.args(["peer", "invalid-copy"]);
        // SAFETY: the negative fixture never maps or delegates its grants.
        let (mut peer, handle) = unsafe {
            PeerConnection::spawn_with_output_copies(
                &mut command,
                pool.clone(),
                Limits::default(),
                0,
            )
        }
        .unwrap();
        assert!(matches!(
            peer.next_event().unwrap(),
            PeerEvent::Ready { .. }
        ));
        begin(
            &handle,
            provider,
            Some(copies.clone()),
            serde_json::json!({"behavior":behavior}),
        );
        let error = peer
            .next_event()
            .err()
            .expect("invalid transition must fail");
        assert!(error.to_string().contains(expected), "{behavior}: {error}");
        let count = counts(&copies);
        assert_eq!(count.in_flight, 0, "{behavior}");
        // The duplicate case first reports success; its second report cannot
        // count it twice. Every other case fails the original attempt exactly once.
        assert_eq!(
            (count.completed, count.failed),
            if behavior == "duplicate" {
                (1, 0)
            } else {
                (0, 1)
            },
            "{behavior}"
        );
        assert!(pool.allocate::<u8>(16).is_ok(), "{behavior}");
        handle.close();
    }
}

#[test]
fn strict_or_missing_output_authority_rejects_before_allocation_and_does_not_borrow_input_permission(
) {
    let pool = pool(1);
    let provider = Selected::new(&pool);
    let (mut peer, handle) = copying_peer(&pool);
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "node/output");
    for context in [None, Some(strict.clone())] {
        let call = begin(
            &handle,
            provider.clone(),
            context,
            serde_json::json!({"operation":"copy"}),
        );
        assert!(complete(&mut peer, call)
            .unwrap_err()
            .contains("copy policy denied"));
        assert_eq!(provider.pointer.load(Ordering::SeqCst), 0);
    }
    assert_eq!(
        (
            counts(&strict).rejected,
            counts(&strict).rejected_bytes,
            counts(&strict).in_flight
        ),
        (1, 16, 0)
    );
    let call = begin(
        &handle,
        provider,
        Some(strict.clone()),
        serde_json::json!({"operation":"copy","len":0}),
    );
    assert!(binary(&output(&mut peer, call).into_data()).is_empty());
    complete(&mut peer, call).unwrap();
    assert_eq!(counts(&strict).completed, 0);
    handle.close();
}

#[test]
fn accounted_audio_and_bytes_preserve_selected_owner_after_worker_shutdown() {
    for audio in [false, true] {
        let pool = pool(1);
        let provider = Selected::new(&pool);
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "node/output");
        let (mut peer, handle) = copying_peer(&pool);
        let call = begin(
            &handle,
            provider.clone(),
            Some(copies.clone()),
            serde_json::json!({"operation":"copy","audio":audio}),
        );
        let retained = output(&mut peer, call).into_data();
        complete(&mut peer, call).unwrap();
        assert!(provider.bound.owns_payload(&retained));
        let pointer = match &retained {
            RuntimeData::Audio { samples, .. } => {
                assert_eq!(&samples[..], &[0.25; 4]);
                samples.as_ptr() as usize
            }
            RuntimeData::Binary(bytes) => {
                assert_eq!(&bytes[..], &[7; 16]);
                bytes.as_ptr() as usize
            }
            _ => panic!("bulk output required"),
        };
        assert_eq!(pointer, provider.pointer.load(Ordering::SeqCst));
        assert_eq!(
            (
                counts(&copies).completed,
                counts(&copies).copied_bytes,
                counts(&copies).failed,
                counts(&copies).in_flight
            ),
            (1, 16, 0, 0)
        );
        handle.close();
        assert!(provider.bound.owns_payload(&retained));
        assert!(pool.allocate::<u8>(16).is_err());
        drop(retained);
        assert!(pool.allocate::<u8>(16).is_ok());
    }
}

#[test]
fn callback_failure_and_retained_slice_exhaustion_are_accounted_and_recoverable() {
    let pool = pool(1);
    let provider = Selected::new(&pool);
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "node/output");
    let (mut peer, handle) = copying_peer(&pool);
    let call = begin(
        &handle,
        provider.clone(),
        Some(copies.clone()),
        serde_json::json!({"operation":"copy","behavior":"fail"}),
    );
    assert!(complete(&mut peer, call)
        .unwrap_err()
        .contains("copy callback sentinel"));
    let call = begin(
        &handle,
        provider.clone(),
        Some(copies.clone()),
        serde_json::json!({"operation":"copy","behavior":"credit"}),
    );
    let RuntimeData::Binary(data) = output(&mut peer, call).into_data() else {
        panic!("binary")
    };
    complete(&mut peer, call).unwrap();
    let retained = data.slice(4..8).unwrap();
    drop(data);
    let call = begin(
        &handle,
        provider.clone(),
        Some(copies.clone()),
        serde_json::json!({"operation":"copy"}),
    );
    assert!(complete(&mut peer, call)
        .unwrap_err()
        .contains("insufficient capacity"));
    drop(retained);
    let call = begin(
        &handle,
        provider,
        Some(copies.clone()),
        serde_json::json!({"operation":"copy"}),
    );
    drop(output(&mut peer, call));
    complete(&mut peer, call).unwrap();
    let count = counts(&copies);
    assert_eq!(
        (
            count.completed,
            count.copied_bytes,
            count.failed,
            count.failed_requested_bytes,
            count.in_flight
        ),
        (2, 32, 2, 32, 0)
    );
    handle.close();
}

#[test]
fn peer_death_closes_the_authorized_attempt_as_failed() {
    let pool = pool(1);
    let provider = Selected::new(&pool);
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "node/output");
    let (mut peer, handle) = copying_peer(&pool);
    let call = begin(
        &handle,
        provider,
        Some(copies.clone()),
        serde_json::json!({"operation":"copy","behavior":"die"}),
    );
    drop(output(&mut peer, call));
    assert!(peer.next_event().is_err());
    assert_eq!(
        (
            counts(&copies).failed,
            counts(&copies).failed_requested_bytes,
            counts(&copies).in_flight
        ),
        (1, 16, 0)
    );
    assert!(pool.allocate::<u8>(16).is_ok());
    handle.close();
}

#[test]
fn output_copy_capability_never_silently_downgrades() {
    let pool = pool(1);
    let mut command = Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
    command.args(["peer", "forged-release"]);
    // SAFETY: fixture does not map grants and returns a legacy Ready message.
    let (mut peer, handle) = unsafe {
        PeerConnection::spawn_with_output_copies(&mut command, pool.clone(), Limits::default(), 0)
    }
    .unwrap();
    assert!(peer
        .next_event()
        .err()
        .expect("downgrade rejected")
        .to_string()
        .contains("capability handshake"));
    handle.close();
    let (mut peer, handle) = spawn(&pool);
    let call = begin(
        &handle,
        Selected::new(&pool),
        Some(CopyContext::new(CopyPolicy::AllowCopies, "output")),
        serde_json::json!({"operation":"copy"}),
    );
    assert!(complete(&mut peer, call)
        .unwrap_err()
        .contains("not negotiated"));
    handle.close();
}

#[test]
fn cancellation_during_copy_retains_storage_until_the_callback_exits() {
    let pool = pool(1);
    let provider = Selected::new(&pool);
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "node/output");
    let (mut peer, handle) = copying_peer(&pool);
    let call = begin(
        &handle,
        provider.clone(),
        Some(copies.clone()),
        serde_json::json!({"operation":"copy","behavior":"cancel"}),
    );
    assert!(
        matches!(output(&mut peer, call).into_data(), RuntimeData::Text(text) if text == "copy-started")
    );
    handle.cancel(call).unwrap();
    assert_eq!(counts(&copies).in_flight, 1);
    assert!(pool.allocate::<u8>(16).is_err());
    let unblock = start(&handle, CallKind::Control, "unblock", vec![]);
    let mut completed = std::collections::HashSet::new();
    while completed.len() < 2 {
        match peer.next_event().unwrap() {
            PeerEvent::Complete {
                call: finished,
                result,
            } => {
                if finished == call {
                    assert!(result.unwrap_err().contains("copy callback cancelled"));
                } else {
                    assert_eq!(finished, unblock);
                    result.unwrap();
                }
                assert!(completed.insert(finished));
            }
            _ => panic!("unexpected output while cancelling copy"),
        }
    }
    assert_eq!(
        (
            counts(&copies).failed,
            counts(&copies).failed_requested_bytes,
            counts(&copies).in_flight
        ),
        (1, 16, 0)
    );
    let retry = begin(
        &handle,
        provider,
        Some(copies.clone()),
        serde_json::json!({"operation":"copy"}),
    );
    drop(output(&mut peer, retry));
    complete(&mut peer, retry).unwrap();
    assert_eq!(counts(&copies).completed, 1);
    handle.close();
}

#[test]
fn cancellation_inside_copy_allocator_accounts_failure_and_reclaims_the_ungranted_loan() {
    let pool = pool(1);
    let provider = Selected::new(&pool);
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "node/output");
    let (mut peer, handle) = copying_peer(&pool);
    let call = begin(
        &handle,
        provider.clone(),
        Some(copies.clone()),
        serde_json::json!({"operation":"copy"}),
    );
    *provider.cancel.lock().unwrap() = Some((handle.clone(), call));
    assert!(complete(&mut peer, call).is_err());
    assert_eq!(
        (
            counts(&copies).failed,
            counts(&copies).failed_requested_bytes,
            counts(&copies).in_flight
        ),
        (1, 16, 0)
    );
    assert!(pool.allocate::<u8>(16).is_ok());
    let retry = begin(
        &handle,
        provider,
        Some(copies.clone()),
        serde_json::json!({"operation":"copy"}),
    );
    drop(output(&mut peer, retry));
    complete(&mut peer, retry).unwrap();
    assert_eq!(counts(&copies).completed, 1);
    handle.close();
}
