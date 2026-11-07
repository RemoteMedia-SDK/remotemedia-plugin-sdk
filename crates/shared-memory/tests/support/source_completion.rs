use super::*;
use remotemedia_shared_memory::{channel::WorkerProcess, peer::Protocols};

fn connect(pool: &SharedPool, enabled: bool, behavior: &str) -> (PeerConnection, HostHandle) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
    command.args(["peer", "source-completion", behavior]);
    let process = WorkerProcess::spawn(&mut command).unwrap();
    // SAFETY: the fixture obeys ownership rules or sends unmapped negative metadata.
    unsafe {
        PeerConnection::from_process_with_protocols(
            process,
            pool.clone(),
            Limits::default(),
            Protocols {
                source_completion: enabled,
                ..Protocols::default()
            },
        )
    }
    .unwrap()
}

fn completed(peer: &mut PeerConnection, expected: u64) -> (Result<(), String>, bool) {
    match peer.next_event().unwrap() {
        PeerEvent::Complete {
            call,
            result,
            source_finished,
        } => {
            assert_eq!(call, expected);
            (result, source_finished)
        }
        _ => panic!("expected terminal completion"),
    }
}

#[test]
fn source_request_commits_only_after_successful_acknowledged_output() {
    for mode in ["finish", "error", "cancel"] {
        let pool = pool(1);
        let (mut peer, host) = connect(&pool, true, "normal");
        assert!(matches!(
            peer.next_event().unwrap(),
            PeerEvent::Ready { .. }
        ));
        let quiet = start(&host, CallKind::Tick, "quiet", vec![]);
        assert_eq!(completed(&mut peer, quiet), (Ok(()), false));
        let call = start(&host, CallKind::Tick, mode, vec![]);
        let PeerEvent::Output(output) = peer.next_event().unwrap() else {
            panic!("expected first output")
        };
        if mode == "cancel" {
            host.cancel(call).unwrap();
        }
        let RuntimeData::Binary(first) = output.into_data() else {
            panic!("expected binary")
        };
        let second = if mode != "cancel" {
            let PeerEvent::Output(output) = peer.next_event().unwrap() else {
                panic!("expected tail")
            };
            let RuntimeData::Binary(second) = output.into_data() else {
                panic!("expected binary")
            };
            assert_eq!(second.as_ptr(), first.as_ptr().wrapping_add(2));
            Some(second)
        } else {
            None
        };
        let (result, finished) = completed(&mut peer, call);
        assert_eq!(result.is_ok(), mode == "finish");
        assert_eq!(finished, mode == "finish");
        let next = start(&host, CallKind::Tick, "quiet", vec![]);
        assert_eq!(completed(&mut peer, next), (Ok(()), false));
        host.close();
        assert_eq!(&*first, &[0, 1, 2, 3, 4, 5, 6, 7]);
        assert!(pool.allocate::<u8>(8).is_err());
        drop(first);
        if let Some(second) = second {
            assert_eq!(&*second, &[2, 3, 4, 5]);
            assert!(pool.allocate::<u8>(8).is_err());
            drop(second);
        }
        assert!(pool.allocate::<u8>(8).is_ok());
    }
}

#[test]
fn source_completion_requires_negotiation_and_a_tick_call() {
    for (enabled, kind) in [(false, CallKind::Tick), (true, CallKind::Process)] {
        let (mut peer, host) = connect(&pool(1), enabled, "normal");
        assert!(matches!(
            peer.next_event().unwrap(),
            PeerEvent::Ready { .. }
        ));
        let call = start(&host, kind, "finish", vec![]);
        let (result, finished) = completed(&mut peer, call);
        assert!(result.unwrap_err().contains("negotiated tick"));
        assert!(!finished);
        host.close();
    }
    let (mut peer, host) = connect(&pool(1), true, "missing-echo");
    assert!(peer
        .next_event()
        .err()
        .unwrap()
        .to_string()
        .contains("capability handshake"));
    host.close();
}

#[test]
fn forged_source_completion_is_rejected_before_terminal_delivery() {
    for (enabled, kind, behavior, expected) in [
        (
            false,
            CallKind::Tick,
            "forged-success",
            "invalid source completion",
        ),
        (
            true,
            CallKind::Process,
            "forged-success",
            "invalid source completion",
        ),
        (
            true,
            CallKind::Tick,
            "forged-error",
            "invalid source completion",
        ),
        (
            true,
            CallKind::Tick,
            "forged-pending",
            "pending operation acknowledgement",
        ),
    ] {
        let (mut peer, host) = connect(&pool(1), enabled, behavior);
        assert!(matches!(
            peer.next_event().unwrap(),
            PeerEvent::Ready { .. }
        ));
        start(&host, kind, "finish", vec![]);
        let pending = if behavior == "forged-pending" {
            let PeerEvent::Output(output) = peer.next_event().unwrap() else {
                panic!("expected held output")
            };
            Some(output)
        } else {
            None
        };
        let error = peer
            .next_event()
            .err()
            .expect("invalid completion must reject");
        assert!(error.to_string().contains(expected), "{behavior}: {error}");
        drop(pending);
        host.close();
    }
}

#[test]
fn cancellation_crossing_a_successful_terminal_is_not_source_eos() {
    let (mut peer, host) = connect(&pool(1), true, "forged-success");
    assert!(matches!(
        peer.next_event().unwrap(),
        PeerEvent::Ready { .. }
    ));
    let call = start(&host, CallKind::Tick, "finish", vec![]);
    host.cancel(call).unwrap();
    let (result, finished) = completed(&mut peer, call);
    assert_eq!(result.unwrap_err(), "source tick cancelled");
    assert!(!finished);
    host.close();
}

#[test]
#[ignore = "requires PREVIOUS_SOURCE_WORKER built before the source extension"]
fn previous_worker_accepts_legacy_handshake_and_rejects_source_extension() {
    let binary = std::env::var("PREVIOUS_SOURCE_WORKER").unwrap();
    for enabled in [false, true] {
        let process = WorkerProcess::spawn(&mut Command::new(&binary)).unwrap();
        // SAFETY: the saved fixture has the same immutable ownership contract.
        let (mut peer, host) = unsafe {
            PeerConnection::from_process_with_protocols(
                process,
                pool(1),
                Limits::default(),
                Protocols {
                    source_completion: enabled,
                    ..Protocols::default()
                },
            )
        }
        .unwrap();
        if enabled {
            assert!(peer.next_event().is_err());
        } else {
            assert!(matches!(
                peer.next_event().unwrap(),
                PeerEvent::Ready { .. }
            ));
        }
        host.close();
    }
}
