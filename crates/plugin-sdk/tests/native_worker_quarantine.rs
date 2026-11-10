#![cfg(all(feature = "test-native-worker", any(windows, target_os = "linux")))]
//! A worker whose exit cannot be confirmed must not have its leases reclaimed:
//! its reservations stay quarantined, even after the process really exits.
use remotemedia_plugin_sdk::types::RuntimeData;
use remotemedia_shared_memory::{
    channel::{test_faults, WorkerProcess},
    peer::{CallContext, CallKind, HostHandle, Limits, PeerConnection, PeerEvent},
    PoolConfig, SessionId, SharedPool, SizeClass,
};
use std::process::Command;

const SLOTS: usize = 4;

fn pool() -> SharedPool {
    SharedPool::new(
        SessionId([91; 16]),
        PoolConfig {
            classes: vec![SizeClass {
                bytes: 4096,
                slots: SLOTS,
            }],
            max_bytes: 4096 * SLOTS,
            max_leases: 32,
        },
    )
    .unwrap()
}

fn start(handle: &HostHandle, kind: CallKind, input: Option<RuntimeData>) -> u64 {
    let context = CallContext {
        session_id: Some("native-session".into()),
        node_id: "native-node".into(),
    };
    let inputs = input
        .map(|data| vec![("input".into(), data)])
        .unwrap_or_default();
    handle
        .start_call(kind, context, serde_json::Value::Null, inputs)
        .unwrap()
}

fn finish_call(peer: &mut PeerConnection, call: u64) {
    loop {
        match peer.next_event().unwrap() {
            PeerEvent::Output(output) => assert_eq!(output.call(), call),
            PeerEvent::Complete {
                call: done, result, ..
            } => {
                assert_eq!(done, call);
                result.unwrap();
                return;
            }
            _ => panic!("unexpected event"),
        }
    }
}

/// Kill a worker the broker could not confirm, by the test's own OS handle.
fn force_kill(pid: u32) {
    let status = if cfg!(windows) {
        Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .output()
    } else {
        Command::new("kill").args(["-9", &pid.to_string()]).output()
    };
    let _ = status;
}

/// Run the fixture's escaped-output violation, which fails the connection while
/// the worker still owns the output it produced. Returns (pool, worker pid).
fn escape(unconfirmed: bool) -> (SharedPool, u32) {
    let pool = pool();
    let mut command = Command::new(env!("CARGO_BIN_EXE_native-node-test-worker"));
    let process = WorkerProcess::spawn(&mut command).unwrap();
    let pid = process.id();
    if unconfirmed {
        test_faults::simulate_unconfirmed_termination(pid);
    }
    // SAFETY: this fixture uses native_worker::run and never delegates mappings.
    let (mut peer, handle) = unsafe {
        PeerConnection::from_process(
            process,
            pool.clone(),
            Limits {
                calls: 2,
                ..Limits::default()
            },
            0,
            false,
        )
    }
    .unwrap();
    assert!(matches!(
        peer.next_event().unwrap(),
        PeerEvent::Ready { .. }
    ));
    for kind in [CallKind::Describe, CallKind::Initialize] {
        let call = start(&handle, kind, None);
        finish_call(&mut peer, call);
    }
    let call = start(
        &handle,
        CallKind::Stream,
        Some(RuntimeData::Text("escape".into())),
    );
    let retained = match peer.next_event().unwrap() {
        PeerEvent::Output(output) => {
            assert_eq!(output.call(), call);
            output
        }
        _ => panic!("expected escaped output"),
    };
    start(
        &handle,
        CallKind::Control,
        Some(RuntimeData::Text("unblock".into())),
    );
    // The protocol violation fails the connection and closes the peer.
    loop {
        match peer.next_event() {
            Err(error) => {
                assert!(error
                    .to_string()
                    .contains("native execution could not complete"));
                break;
            }
            Ok(_) => continue,
        }
    }
    let RuntimeData::Binary(bytes) = retained.data() else {
        panic!("expected binary")
    };
    assert_eq!(bytes.as_ref(), &[5, 6, 7, 8]);
    drop(retained);
    drop(handle);
    drop(peer);
    (pool, pid)
}

fn available(pool: &SharedPool) -> usize {
    let loans: Vec<_> = std::iter::from_fn(|| pool.allocate::<u8>(4).ok())
        .take(SLOTS + 1)
        .collect();
    loans.len()
}

#[test]
fn confirmed_termination_reclaims_the_dead_workers_leases() {
    let (pool, _) = escape(false);
    assert_eq!(
        available(&pool),
        SLOTS,
        "confirmed death releases every slot"
    );
}

#[test]
fn unconfirmed_termination_quarantines_leases_even_after_real_exit() {
    let (pool, pid) = escape(true);
    // The broker could not confirm the exit, so the worker's ownership of its
    // produced output is retained: that slot is never handed out again.
    assert_eq!(
        available(&pool),
        SLOTS - 1,
        "unconfirmed exit must quarantine"
    );
    // Even once the process is really gone, nothing told the pool so; a later
    // allocation must still not reuse the quarantined storage.
    force_kill(pid);
    std::thread::sleep(std::time::Duration::from_millis(200));
    test_faults::clear(pid);
    assert_eq!(
        available(&pool),
        SLOTS - 1,
        "quarantine is never lifted implicitly"
    );
}
