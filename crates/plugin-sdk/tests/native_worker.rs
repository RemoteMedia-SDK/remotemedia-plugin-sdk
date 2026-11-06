#![cfg(all(feature = "test-native-worker", any(windows, target_os = "linux")))]
use remotemedia_plugin_sdk::{
    native_worker::{Description, Pacing, TickRequest},
    traits::rpc::{RpcEvent, RpcId, RpcReply, RpcRequest},
    types::{PayloadStorage, RuntimeData, TensorEncoding},
};
use remotemedia_shared_memory::{
    peer::{CallContext, CallKind, HostHandle, Limits, PeerConnection, PeerEvent},
    PoolConfig, SessionId, SharedPool, SizeClass,
};
use std::process::Command;

fn context() -> CallContext {
    CallContext {
        session_id: Some("native-session".into()),
        node_id: "native-node".into(),
    }
}
fn start(
    handle: &HostHandle,
    kind: CallKind,
    args: serde_json::Value,
    inputs: Vec<(String, RuntimeData)>,
) -> u64 {
    handle.start_call(kind, context(), args, inputs).unwrap()
}
fn output(peer: &mut PeerConnection, call: u64) -> RuntimeData {
    match peer.next_event().unwrap() {
        PeerEvent::Output(output) => {
            assert_eq!(output.call(), call);
            output.into_data()
        }
        _ => panic!("expected node output"),
    }
}
fn complete(peer: &mut PeerConnection, call: u64) -> Result<(), String> {
    match peer.next_event().unwrap() {
        PeerEvent::Complete {
            call: actual,
            result,
        } => {
            assert_eq!(call, actual);
            result
        }
        _ => panic!("expected node completion"),
    }
}
fn setup() -> (SharedPool, PeerConnection, HostHandle) {
    let pool = SharedPool::new(
        SessionId([73; 16]),
        PoolConfig {
            classes: vec![SizeClass {
                bytes: 4096,
                slots: 4,
            }],
            max_bytes: 16384,
            max_leases: 32,
        },
    )
    .unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_native-node-test-worker"));
    // SAFETY: this fixture uses native_worker::run and never delegates mappings.
    let (mut peer, handle) = unsafe {
        PeerConnection::spawn(
            &mut command,
            pool.clone(),
            Limits {
                calls: 2,
                ..Limits::default()
            },
        )
    }
    .unwrap();
    assert!(matches!(peer.next_event().unwrap(), PeerEvent::Ready { modes } if modes.len() == 8));
    let describe = start(&handle, CallKind::Describe, serde_json::Value::Null, vec![]);
    let RuntimeData::Json(value) = output(&mut peer, describe) else {
        panic!("missing description")
    };
    let descriptor: Description = serde_json::from_value(value).unwrap();
    assert_eq!(descriptor.node_type, "MappedNativeFixture");
    assert!(descriptor.multi_input);
    assert!(matches!(descriptor.pacing, Pacing::SourceWall(40)));
    assert_eq!(descriptor.fallback_rate_hz, 40);
    assert_eq!(descriptor.rpc_methods.len(), 3);
    complete(&mut peer, describe).unwrap();
    let initialize = start(
        &handle,
        CallKind::Initialize,
        serde_json::Value::Null,
        vec![],
    );
    assert!(
        matches!(output(&mut peer, initialize), RuntimeData::Json(v) if v["status"] == "ready" && v["message"] == "fixture initialized")
    );
    complete(&mut peer, initialize).unwrap();
    (pool, peer, handle)
}
fn bytes(pool: &SharedPool, values: &[u8]) -> RuntimeData {
    let mut loan = pool.allocate::<u8>(values.len()).unwrap();
    loan.copy_from_slice(values);
    RuntimeData::Binary(loan.freeze())
}
fn binary(data: &RuntimeData) -> &[u8] {
    let RuntimeData::Binary(data) = data else {
        panic!("expected binary")
    };
    data
}
fn rpc(handle: &HostHandle, method: &str, corr_id: RpcId) -> u64 {
    start(
        handle,
        CallKind::Rpc,
        serde_json::to_value(RpcRequest {
            method: method.into(),
            corr_id,
            args: vec![],
            kwargs: Default::default(),
        })
        .unwrap(),
        vec![],
    )
}
fn reply(peer: &mut PeerConnection, call: u64) -> RpcReply {
    let RuntimeData::Json(value) = output(peer, call) else {
        panic!("expected RPC JSON")
    };
    serde_json::from_value(value).unwrap()
}

#[test]
fn native_node_trait_transform_uses_host_loans_and_preserves_state_identity_and_rpc_modes() {
    let (pool, mut peer, handle) = setup();
    for audio in [true, false] {
        let input = if audio {
            let mut loan = pool.allocate::<f32>(4).unwrap();
            loan.copy_from_slice(&[1.0, 2.0, 3.0, 4.0]);
            RuntimeData::Audio {
                samples: loan.freeze().into(),
                sample_rate: 48000,
                channels: 2,
                stream_id: Some("voice".into()),
                timestamp_us: Some(90),
                arrival_ts_us: Some(93),
                metadata: Some(serde_json::json!({"mark":7})),
            }
        } else {
            bytes(&pool, &[1, 2, 3, 4])
        };
        let metadata = input
            .clone()
            .into_shared_parts(TensorEncoding::Runtime)
            .unwrap()
            .0;
        let call = start(
            &handle,
            CallKind::Process,
            serde_json::Value::Null,
            vec![("input".into(), input)],
        );
        let data = output(&mut peer, call);
        complete(&mut peer, call).unwrap();
        let (actual, storage) = data.into_shared_parts(TensorEncoding::Runtime).unwrap();
        assert_eq!(actual, metadata);
        match storage {
            PayloadStorage::Audio(data) => assert_eq!(&*data, &[3.0, 6.0, 9.0, 12.0]),
            PayloadStorage::Bytes(data) => assert_eq!(&*data, &[3, 4, 5, 6]),
            _ => unreachable!(),
        }
    }
    let call = rpc(&handle, "state", RpcId::String("state/1".into()));
    let response = reply(&mut peer, call);
    assert_eq!(response.corr_id, RpcId::String("state/1".into()));
    assert_eq!(
        response.event,
        RpcEvent::Value(
            serde_json::json!({"count":2,"node":"native-node","session":"native-session"})
        )
    );
    complete(&mut peer, call).unwrap();
    let call = rpc(&handle, "events", RpcId::Integer(-17));
    for event in [
        RpcEvent::Value(11.into()),
        RpcEvent::Value(22.into()),
        RpcEvent::End,
    ] {
        let response = reply(&mut peer, call);
        assert_eq!(response.corr_id, RpcId::Integer(-17));
        assert_eq!(response.event, event);
    }
    complete(&mut peer, call).unwrap();
    let reset = rpc(&handle, "reset", RpcId::Integer(41));
    complete(&mut peer, reset).unwrap();
    let state = rpc(&handle, "state", RpcId::Integer(42));
    assert!(matches!(reply(&mut peer, state).event, RpcEvent::Value(value) if value["count"] == 0));
    complete(&mut peer, state).unwrap();
    let private = start(
        &handle,
        CallKind::Process,
        serde_json::Value::Null,
        vec![("input".into(), RuntimeData::Text("private".into()))],
    );
    assert!(complete(&mut peer, private).is_err());
    let wrong = handle
        .start_call(
            CallKind::Process,
            CallContext {
                node_id: "other".into(),
                ..context()
            },
            serde_json::Value::Null,
            vec![("input".into(), RuntimeData::Text("anything".into()))],
        )
        .unwrap();
    assert!(complete(&mut peer, wrong)
        .unwrap_err()
        .contains("identity mismatch"));
    handle.close();
}

#[test]
fn native_node_stream_tick_and_named_inputs_retain_mapped_views_after_exit() {
    let (pool, mut peer, handle) = setup();
    let call = start(
        &handle,
        CallKind::Stream,
        serde_json::Value::Null,
        vec![("input".into(), bytes(&pool, &[0, 1, 2, 3, 4, 5]))],
    );
    let first = output(&mut peer, call);
    let second = output(&mut peer, call);
    complete(&mut peer, call).unwrap();
    assert_eq!(binary(&first), &[1, 2]);
    assert_eq!(binary(&second), &[3, 4]);
    let call = start(
        &handle,
        CallKind::MultiInput,
        serde_json::Value::Null,
        vec![
            ("left".into(), bytes(&pool, &[7, 8])),
            ("right".into(), RuntimeData::Text("side".into())),
        ],
    );
    let named = output(&mut peer, call);
    complete(&mut peer, call).unwrap();
    assert_eq!(binary(&named), &[7, 8]);
    let tick = TickRequest {
        clock_id: "wall:40".into(),
        pts_us: 123456,
        frame_idx: 9,
        deadline_us: 400,
    };
    let call = start(
        &handle,
        CallKind::Tick,
        serde_json::to_value(tick).unwrap(),
        vec![],
    );
    let generated = output(&mut peer, call);
    complete(&mut peer, call).unwrap();
    handle.close();
    drop(peer);
    assert_eq!(binary(&generated), &123456_u64.to_le_bytes());
    assert_eq!(binary(&first), &[1, 2]);
    drop(first);
    drop(second);
    drop(named);
    drop(generated);
    let loans: Vec<_> = (0..4).map(|_| pool.allocate::<u8>(16).unwrap()).collect();
    assert_eq!(loans.len(), 4);
}

#[test]
fn native_async_control_runs_during_media_and_end_of_input_reaches_the_node_hook() {
    // Both negotiated call slots are occupied: waiting media and its control.
    // The control hook also awaits node-owned blocking work. Dispatch capacity
    // must not be shared with that inner runtime's blocking pool.
    let (_, mut peer, handle) = setup();
    let waiting = start(
        &handle,
        CallKind::Stream,
        serde_json::Value::Null,
        vec![("input".into(), RuntimeData::Text("wait".into()))],
    );
    assert!(matches!(output(&mut peer, waiting), RuntimeData::Text(value) if value == "waiting"));
    handle.cancel(waiting).unwrap();
    let control = start(
        &handle,
        CallKind::Control,
        serde_json::Value::Null,
        vec![("input".into(), RuntimeData::Text("unblock".into()))],
    );
    let mut completed = Vec::new();
    let mut handled = false;
    while completed.len() < 2 {
        match peer.next_event().unwrap() {
            PeerEvent::Output(output) => {
                assert_eq!(output.call(), control);
                assert!(matches!(output.into_data(), RuntimeData::Json(v) if v == true));
                handled = true;
            }
            PeerEvent::Complete { call, result } => {
                result.unwrap();
                completed.push(call);
            }
            _ => panic!("unexpected event"),
        }
    }
    assert!(handled);
    completed.sort();
    assert_eq!(completed, vec![waiting, control]);
    let eos = start(
        &handle,
        CallKind::Control,
        serde_json::Value::Null,
        vec![(
            "input".into(),
            RuntimeData::Json(serde_json::json!({"event":"end_of_input"})),
        )],
    );
    assert!(matches!(output(&mut peer, eos), RuntimeData::Json(v) if v == true));
    complete(&mut peer, eos).unwrap();
    handle.close();
}

#[test]
fn escaped_pending_callback_fails_connection_without_invalidating_host_output() {
    let (pool, mut peer, handle) = setup();
    let call = start(
        &handle,
        CallKind::Stream,
        serde_json::Value::Null,
        vec![("input".into(), RuntimeData::Text("escape".into()))],
    );
    let retained = match peer.next_event().unwrap() {
        PeerEvent::Output(output) => {
            assert_eq!(output.call(), call);
            output
        }
        _ => panic!("expected escaped output"),
    };
    let control = start(
        &handle,
        CallKind::Control,
        serde_json::Value::Null,
        vec![("input".into(), RuntimeData::Text("unblock".into()))],
    );
    loop {
        match peer.next_event() {
            Err(error) => {
                assert!(error
                    .to_string()
                    .contains("native execution could not complete"));
                break;
            }
            Ok(PeerEvent::Output(output)) => {
                assert_eq!(output.call(), control);
                drop(output);
            }
            Ok(PeerEvent::Complete { call, result }) => {
                assert_eq!(call, control);
                result.unwrap();
            }
            _ => panic!("unexpected event before protocol failure"),
        }
    }
    assert_eq!(binary(retained.data()), &[5, 6, 7, 8]);
    let spare: Vec<_> = (0..3).map(|_| pool.allocate::<u8>(4).unwrap()).collect();
    assert!(pool.allocate::<u8>(4).is_err());
    drop(retained);
    assert!(pool.allocate::<u8>(4).is_ok());
    drop(spare);
}
