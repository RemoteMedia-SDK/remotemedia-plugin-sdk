#![cfg(all(feature = "test-native-worker", any(windows, target_os = "linux")))]
#[path = "support/native_namespace.rs"]
mod namespace;
#[cfg(feature = "mapped-python")]
#[path = "support/onnx_worker.rs"]
mod onnx_worker;
use remotemedia_plugin_sdk::{
    native_worker::{
        Limits, NativeWorkerFactory, NativeWorkerNode, PoolConfig, SessionId, SharedPool,
        SizeClass, WorkerInputCopies,
    },
    traits::{self, InitializeContextRead, StreamingNode, StreamingNodeFactory},
    types::{CopyPolicy, CopyReason, Error, RuntimeData},
};
use std::{
    collections::HashMap,
    process::Command,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use traits::rpc::{
    CallContext, RpcEvent, RpcId, RpcMethod, RpcMode, RpcReply, RpcReplySink, RpcRequest,
    RpcResponder,
};

#[derive(Default)]
struct Init(Mutex<Vec<(String, String)>>);
impl InitializeContextRead for Init {
    fn session_id(&self) -> &str {
        "native-session"
    }
    fn node_id(&self) -> &str {
        "native-node"
    }
    fn emit_progress(&self, status: &str, message: &str) {
        self.0.lock().unwrap().push((status.into(), message.into()));
    }
}
fn context() -> CallContext {
    CallContext {
        session_id: Some("native-session".into()),
        node_id: "native-node".into(),
    }
}
fn pool() -> SharedPool {
    SharedPool::new(
        SessionId([84; 16]),
        PoolConfig {
            classes: vec![SizeClass {
                bytes: 4096,
                slots: 8,
            }],
            max_bytes: 32768,
            max_leases: 64,
        },
    )
    .unwrap()
}
async fn node(pool: &SharedPool, calls: usize) -> Arc<dyn StreamingNode> {
    node_with_namespace(pool, calls, false).await
}

async fn node_with_namespace(
    pool: &SharedPool,
    calls: usize,
    namespaced: bool,
) -> Arc<dyn StreamingNode> {
    // SAFETY: this fixture uses the production runner and never delegates maps.
    let factory = unsafe {
        NativeWorkerFactory::new(
            env!("CARGO_BIN_EXE_native-node-test-worker").into(),
            "MappedNativeFixture".into(),
            pool.clone(),
        )
    }
    .with_limits(Limits {
        calls,
        ..Limits::default()
    });
    let factory = namespace::configure(factory, namespaced);
    // Creation inside the current-thread Tokio runtime must not call a Tokio
    // blocking_recv or wait for executor tasks on this same thread.
    let node: Arc<dyn StreamingNode> = Arc::from(
        factory
            .create(
                "native-node".into(),
                &serde_json::json!({"gain":5,"require_namespace":namespaced}),
                Some("native-session".into()),
            )
            .unwrap(),
    );
    let init = Init::default();
    node.initialize(&init).await.unwrap();
    assert_eq!(
        &*init.0.lock().unwrap(),
        &[("ready".into(), "fixture initialized".into())]
    );
    node
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
#[derive(Default)]
struct Replies(Mutex<Vec<RpcReply>>);
#[async_trait::async_trait]
impl RpcReplySink for Replies {
    async fn send(&self, reply: RpcReply) -> Result<(), Error> {
        self.0.lock().unwrap().push(reply);
        Ok(())
    }
}
async fn rpc(node: &dyn StreamingNode, method: RpcMethod, id: RpcId) -> Vec<RpcReply> {
    let sink = Arc::new(Replies::default());
    let mut replies = RpcResponder::new(sink.clone(), id.clone(), method.mode);
    let result = node
        .call_rpc(
            RpcRequest {
                method: method.name,
                corr_id: id,
                args: vec![],
                kwargs: Default::default(),
            },
            &context(),
            &mut replies,
        )
        .await;
    replies.finish(result).await.unwrap();
    let result = sink.0.lock().unwrap().clone();
    result
}

#[tokio::test(flavor = "current_thread")]
async fn factory_trait_calls_preserve_configuration_media_hooks_and_rpc_contract() {
    verify_factory_contract(false).await;
}

#[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
#[tokio::test(flavor = "current_thread")]
async fn namespaced_factory_preserves_configuration_media_hooks_and_rpc_contract() {
    verify_factory_contract(true).await;
}

async fn verify_factory_contract(namespaced: bool) {
    let pool = pool();
    let node = node_with_namespace(&pool, 4, namespaced).await;
    let ctx = context();
    assert!(node.is_multi_input());
    assert_eq!(node.pacing_nature(), traits::PacingNature::SourceWall(40));
    assert_eq!(node.rpc_methods().len(), 3);
    let mut loan = pool.allocate::<f32>(4).unwrap();
    loan.copy_from_slice(&[1.0, 2.0, 3.0, 4.0]);
    let audio = RuntimeData::Audio {
        samples: loan.freeze().into(),
        sample_rate: 48000,
        channels: 2,
        stream_id: Some("voice".into()),
        timestamp_us: Some(19),
        arrival_ts_us: None,
        metadata: None,
    };
    let output = node.process_async(audio, &ctx).await.unwrap();
    assert!(
        matches!(output, RuntimeData::Audio { ref samples, sample_rate:48000, channels:2, timestamp_us:Some(19), .. } if samples.as_ref() == [5.0,10.0,15.0,20.0])
    );
    let named = node
        .process_multi_async(
            HashMap::from([
                ("left".into(), bytes(&pool, &[6, 7])),
                ("right".into(), RuntimeData::Text("side".into())),
            ]),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(binary(&named), &[6, 7]);
    let streamed = Arc::new(Mutex::new(Vec::new()));
    let sink = streamed.clone();
    let count = node
        .process_streaming_async(
            bytes(&pool, &[0, 1, 2, 3, 4, 5]),
            &ctx,
            Box::new(move |value| {
                sink.lock().unwrap().push(value);
                Ok(())
            }),
        )
        .await
        .unwrap();
    assert_eq!(count, 2);
    assert_eq!(binary(&streamed.lock().unwrap()[1]), &[3, 4]);
    let ticks = Arc::new(Mutex::new(Vec::new()));
    let sink = ticks.clone();
    node.tick(
        traits::Tick {
            clock_id: "wall:40".into(),
            pts_us: 918,
            frame_idx: 9,
            deadline_us: 400,
        },
        &ctx,
        Box::new(move |value| {
            sink.lock().unwrap().push(value);
            Ok(())
        }),
    )
    .await
    .unwrap();
    assert_eq!(binary(&ticks.lock().unwrap()[0]), &918_u64.to_le_bytes());
    assert!(node
        .process_control_message(
            RuntimeData::Json(serde_json::json!({"event":"end_of_input"})),
            &ctx
        )
        .await
        .unwrap());
    let unary = rpc(
        node.as_ref(),
        RpcMethod::new("state", RpcMode::Reply),
        RpcId::String("state/abc".into()),
    )
    .await;
    assert_eq!(unary.len(), 1);
    assert_eq!(unary[0].corr_id, RpcId::String("state/abc".into()));
    assert!(matches!(&unary[0].event,RpcEvent::Value(value) if value["count"] == 1));
    let stream = rpc(
        node.as_ref(),
        RpcMethod::new("events", RpcMode::Stream),
        RpcId::Integer(-31),
    )
    .await;
    assert_eq!(
        stream.iter().map(|r| r.event.clone()).collect::<Vec<_>>(),
        vec![
            RpcEvent::Value(11.into()),
            RpcEvent::Value(22.into()),
            RpcEvent::End
        ]
    );
    assert!(stream.iter().all(|r| r.corr_id == RpcId::Integer(-31)));
    assert!(rpc(
        node.as_ref(),
        RpcMethod::new("reset", RpcMode::FireAndForget),
        RpcId::Integer(1)
    )
    .await
    .is_empty());
    drop(node);
    assert_eq!(binary(&named), &[6, 7]);
    drop(output);
    drop(named);
    drop(streamed);
    drop(ticks);
    let free: Vec<_> = (0..8).map(|_| pool.allocate::<u8>(1).unwrap()).collect();
    assert_eq!(free.len(), 8);
}

#[tokio::test(flavor = "current_thread")]
async fn abandoned_future_and_callback_error_cancel_only_their_own_call() {
    let pool = pool();
    let node = node(&pool, 4).await;
    let (started, ready) = tokio::sync::oneshot::channel();
    let mut started = Some(started);
    let worker = node.clone();
    let task = tokio::spawn(async move {
        worker
            .process_streaming_async(
                RuntimeData::Text("wait".into()),
                &context(),
                Box::new(move |_| {
                    if let Some(started) = started.take() {
                        let _ = started.send(());
                    }
                    Ok(())
                }),
            )
            .await
    });
    ready.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(node
        .process_control_message(RuntimeData::Text("unblock".into()), &context())
        .await
        .unwrap());
    let rejected = node
        .process_streaming_async(
            bytes(&pool, &[0, 1, 2, 3, 4, 5]),
            &context(),
            Box::new(|_| Err(Error::Execution("consumer stopped".into()))),
        )
        .await;
    assert!(rejected
        .unwrap_err()
        .to_string()
        .contains("consumer stopped"));
    let output = node
        .process_async(bytes(&pool, &[3, 4]), &context())
        .await
        .unwrap();
    assert_eq!(binary(&output), &[5, 6]);
    assert!(node
        .process_async(RuntimeData::Binary(vec![9].into()), &context())
        .await
        .is_err());
    let output = node
        .process_async(bytes(&pool, &[9]), &context())
        .await
        .unwrap();
    assert_eq!(binary(&output), &[11]);
}

#[tokio::test(flavor = "current_thread")]
async fn independent_native_workers_forward_one_pools_mapped_output() {
    let pool = pool();
    let first = node(&pool, 3).await;
    let second = node(&pool, 3).await;
    let first_output = first
        .process_async(bytes(&pool, &[1, 2, 3]), &context())
        .await
        .unwrap();
    let retained = first_output.clone();
    let second_output = second
        .process_async(first_output, &context())
        .await
        .unwrap();
    assert_eq!(binary(&second_output), &[5, 6, 7]);
    drop(first);
    assert_eq!(binary(&retained), &[3, 4, 5]);
    assert!(second
        .process_async(RuntimeData::Text("crash".into()), &context())
        .await
        .is_err());
    assert_eq!(binary(&second_output), &[5, 6, 7]);
    assert!(second
        .process_async(RuntimeData::Text("later".into()), &context())
        .await
        .is_err());
}

#[test]
fn startup_deadline_terminates_a_worker_that_never_handshakes() {
    let mut command = Command::new(env!("CARGO_BIN_EXE_native-node-test-worker"));
    command.arg("--silent");
    let before = Instant::now();
    // SAFETY: the silent fixture never imports/delegates a buffer or channel.
    let result = unsafe {
        NativeWorkerNode::spawn(
            &mut command,
            pool(),
            remotemedia_shared_memory::peer::CallContext {
                session_id: Some("native-session".into()),
                node_id: "native-node".into(),
            },
            Limits::default(),
            Duration::from_millis(100),
        )
    };
    assert!(result.err().unwrap().to_string().contains("startup failed"));
    assert!(before.elapsed() < Duration::from_secs(5));
}

#[tokio::test(flavor = "current_thread")]
async fn one_call_budget_is_released_before_completion_is_delivered() {
    let pool = pool();
    let node = node(&pool, 1).await;
    for value in 0..64_u8 {
        let output = node
            .process_async(bytes(&pool, &[value]), &context())
            .await
            .unwrap();
        assert_eq!(binary(&output), &[value + 2]);
    }
}

async fn python_node(pool: &SharedPool) -> Arc<dyn StreamingNode> {
    let python = std::env::var("MAPPED_WORKER_PYTHON")
        .expect("set MAPPED_WORKER_PYTHON to matching-wheel interpreter");
    let root = std::env::var("MAPPED_SDK_PYTHON").expect("set MAPPED_SDK_PYTHON to clients/python");
    let mut command = Command::new(python);
    command.args([
        "-m",
        "remotemedia_mapped_worker",
        "--node-type",
        "MappedFixtureNode",
        "--node-id",
        "native-node",
        "--session-id",
        "native-session",
        "--params",
        r#"{"gain":4}"#,
        "--module-root",
        &format!("{root}/tests/support"),
        "--register-module",
        "mapped_node_fixture",
    ]);
    // SAFETY: the checked-in Python fixture and mapped runner use owning views,
    // never fork/delegate mappings, and run under the trusted worker contract.
    let node = unsafe {
        NativeWorkerNode::spawn(
            &mut command,
            pool.clone(),
            remotemedia_shared_memory::peer::CallContext {
                session_id: Some("native-session".into()),
                node_id: "native-node".into(),
            },
            Limits::default(),
            Duration::from_secs(30),
        )
    }
    .unwrap();
    let node: Arc<dyn StreamingNode> = Arc::new(traits::AsyncNodeWrapper(Arc::new(node)));
    let init = Init::default();
    node.initialize(&init).await.unwrap();
    assert_eq!(
        *init.0.lock().unwrap(),
        vec![("ready".into(), "Python fixture initialized".into())]
    );
    node
}

#[cfg(feature = "mapped-python")]
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires MAPPED_CLIENT_WHEEL, MAPPED_BUFFER_WHEEL, MAPPED_WHEELHOUSE, MAPPED_ENV_CACHE and MAPPED_SDK_PYTHON"]
async fn managed_mapped_python_factory_installs_verifies_and_reuses_wheels() {
    use remotemedia_plugin_sdk::mapped_python::{
        MappedPythonConfig, MappedPythonFactory, MappedPythonWheels,
    };
    use remotemedia_py_env::{PythonEnvConfig, PythonEnvMode};
    let config = MappedPythonConfig {
        node_type: "MappedFixtureNode".into(),
        module_root: std::path::PathBuf::from(std::env::var("MAPPED_SDK_PYTHON").unwrap())
            .join("tests/support"),
        entry_module: "mapped_node_fixture".into(),
        requirements: vec![],
        wheels: MappedPythonWheels {
            client: std::env::var("MAPPED_CLIENT_WHEEL").unwrap().into(),
            buffers: std::env::var("MAPPED_BUFFER_WHEEL").unwrap().into(),
        },
        environment: PythonEnvConfig {
            mode: PythonEnvMode::System,
            // The manifest override must take precedence over this invalid default.
            python_version: "999.1".into(),
            cache_dir: Some(std::env::var("MAPPED_ENV_CACHE").unwrap().into()),
            no_index: true,
            ..Default::default()
        },
    };
    let pool = pool();
    // SAFETY: the selected local wheels and checked-in fixture obey the mapped
    // ownership contract and do not delegate mappings or launch descendants.
    let factory = unsafe { MappedPythonFactory::new(config, pool.clone()) };
    let marker =
        std::path::PathBuf::from(std::env::var("MAPPED_ENV_CACHE").unwrap()).join(format!(
            "cleanup-{}-{}.txt",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    let params = serde_json::json!({
        "__cleanup_marker": marker,
        "gain": 4,
        "__python_version__": std::env::var("MAPPED_PYTHON_VERSION").unwrap_or("3.12".into()),
        "__python_find_links__": [std::env::var("MAPPED_WHEELHOUSE").unwrap()],
        "__python_env_scope_context__": "managed-mapped-factory-test",
    });
    let strict = WorkerInputCopies::new(
        "native-session",
        ["native-node".into()],
        CopyPolicy::RequireZeroCopy,
    );
    let compatible = WorkerInputCopies::new(
        "native-session",
        ["native-node".into()],
        CopyPolicy::AllowCopies,
    );
    let mut retained = None;
    for iteration in 1..=2 {
        let node: Arc<dyn StreamingNode> = Arc::from(
            factory
                .clone()
                .with_input_copies(if iteration == 1 {
                    strict.clone()
                } else {
                    compatible.clone()
                })
                .create("native-node".into(), &params, Some("native-session".into()))
                .unwrap(),
        );
        node.initialize(&Init::default()).await.unwrap();
        let input = if iteration == 1 {
            let error = node
                .process_async(RuntimeData::Binary(vec![1, 2, 3].into()), &context())
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("rejects 3 payload bytes"),
                "{error}"
            );
            bytes(&pool, &[1, 2, 3])
        } else {
            RuntimeData::Binary(vec![1, 2, 3].into())
        };
        let output = node.process_async(input, &context()).await.unwrap();
        assert_eq!(binary(&output), &[5, 6, 7]);
        let RuntimeData::Binary(ref buffer) = output else {
            panic!("binary output")
        };
        // Exportability proves the result still belongs to the original pool.
        pool.export(buffer, remotemedia_shared_memory::PeerId(901))
            .unwrap();
        retained = Some(output);
        assert_managed_shutdown(node.clone(), &marker, iteration).await;
        drop(node);
    }
    assert_eq!(binary(retained.as_ref().unwrap()), &[5, 6, 7]);
    assert_eq!(
        strict.reports()["native-node"]
            .entries
            .iter()
            .map(|(_, c)| c.rejected_bytes)
            .sum::<u64>(),
        3
    );
    assert_eq!(
        compatible.reports()["native-node"]
            .entries
            .iter()
            .map(|(_, c)| c.copied_bytes)
            .sum::<u64>(),
        3
    );
    let mut failure = params.clone();
    failure["__cleanup_failure"] = serde_json::json!(true);
    let node = factory
        .create(
            "native-node".into(),
            &failure,
            Some("native-session".into()),
        )
        .unwrap();
    node.initialize(&Init::default()).await.unwrap();
    for _ in 0..2 {
        let error = node
            .process_control_message(shutdown_message(), &context())
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("fixture cleanup failed"),
            "{error}"
        );
        assert_eq!(
            std::fs::read_to_string(&marker).unwrap(),
            "cleanup\n".repeat(3)
        );
    }
    drop(node);
    std::fs::remove_file(&marker).unwrap();
    let mut mismatch = params;
    mismatch["__python_version__"] = serde_json::json!("999.1");
    let error = factory
        .create(
            "native-node".into(),
            &mismatch,
            Some("native-session".into()),
        )
        .err()
        .unwrap();
    assert!(error.to_string().contains("version mismatch"), "{error}");
}

#[cfg(feature = "mapped-python")]
fn shutdown_message() -> RuntimeData {
    RuntimeData::Json(serde_json::json!({"__aux_port__": "shutdown", "payload": {}}))
}

#[cfg(feature = "mapped-python")]
async fn assert_managed_shutdown(
    node: Arc<dyn StreamingNode>,
    marker: &std::path::Path,
    count: usize,
) {
    let worker = node.clone();
    let (started, ready) = tokio::sync::oneshot::channel();
    let mut started = Some(started);
    let running = tokio::spawn(async move {
        worker
            .process_streaming_async(
                RuntimeData::Text("wait".into()),
                &context(),
                Box::new(move |_| {
                    if let Some(started) = started.take() {
                        let _ = started.send(());
                    }
                    Ok(())
                }),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), ready)
        .await
        .unwrap()
        .unwrap();
    let first_context = context();
    let second_context = context();
    let (first, second) = tokio::join!(
        tokio::time::timeout(
            Duration::from_secs(5),
            node.process_control_message(shutdown_message(), &first_context)
        ),
        tokio::time::timeout(
            Duration::from_secs(5),
            node.process_control_message(shutdown_message(), &second_context)
        ),
    );
    assert!(first.unwrap().unwrap());
    assert!(second.unwrap().unwrap());
    assert_eq!(
        std::fs::read_to_string(marker).unwrap(),
        "cleanup\n".repeat(count)
    );
    let error = tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
    let error = node
        .process_async(RuntimeData::Text("late".into()), &context())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("shutting down"), "{error}");
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires MAPPED_WORKER_PYTHON, MAPPED_SDK_PYTHON and installed mapped wheel on PYTHONPATH"]
async fn mapped_python_node_hooks_and_rust_python_native_chain() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let pool = pool();
        let python = python_node(&pool).await;
        assert!(python.is_multi_input());
        assert_eq!(python.pacing_nature(), traits::PacingNature::SourceWall(40));
        let mut loan = pool.allocate::<f32>(4).unwrap();
        loan.copy_from_slice(&[1.0, 2.0, 3.0, 4.0]);
        let audio = RuntimeData::Audio { samples: loan.freeze().into(), sample_rate: 48000, channels: 2,
            stream_id: Some("voice".into()), timestamp_us: Some(u64::MAX), arrival_ts_us: Some(u64::MAX-1), metadata: None };
        let transformed = python.process_async(audio, &context()).await.unwrap();
        assert!(matches!(&transformed, RuntimeData::Audio { samples, timestamp_us: Some(u64::MAX), .. } if samples.as_ref() == [4.0,8.0,12.0,16.0]));
        let forwarded = python.process_multi_async(HashMap::from([("left".into(), bytes(&pool, &[9,10]))]), &context()).await.unwrap();
        assert_eq!(binary(&forwarded), &[9,10]);
        let ticks = Arc::new(Mutex::new(Vec::new()));
        let sink = ticks.clone();
        python.tick(traits::Tick { clock_id: "wall:40".into(), pts_us: 918, frame_idx: 9, deadline_us: 400 },
            &context(), Box::new(move |data| { sink.lock().unwrap().push(data); Ok(()) })).await.unwrap();
        assert_eq!(binary(&ticks.lock().unwrap()[0]), &918_u64.to_le_bytes());
        let replies = rpc(python.as_ref(), RpcMethod::new("status_info", RpcMode::Reply), RpcId::String("python-state".into())).await;
        assert!(matches!(&replies[0].event, RpcEvent::Value(v) if v["session"] == "native-session" && v["node"] == "native-node" && v["calls"] == 1));
        let replies = rpc(python.as_ref(), RpcMethod::new("events", RpcMode::Stream), RpcId::Integer(-31)).await;
        assert_eq!(replies.iter().map(|r| r.event.clone()).collect::<Vec<_>>(), vec![RpcEvent::Value(11.into()),RpcEvent::Value(22.into()),RpcEvent::End]);
        assert!(rpc(python.as_ref(), RpcMethod::new("reset", RpcMode::FireAndForget), RpcId::Integer(1)).await.is_empty());
        let chunks = Arc::new(Mutex::new(Vec::new()));
        let sink = chunks.clone();
        let count = python.process_streaming_async(RuntimeData::Text("chunks".into()), &context(),
            Box::new(move |data| { sink.lock().unwrap().push(data); Ok(()) })).await.unwrap();
        assert_eq!(count, 2);
        {
            let chunks = chunks.lock().unwrap();
            assert_eq!(binary(&chunks[0]), &[0,1]);
            assert_eq!(binary(&chunks[1]), &[3,4]);
            assert_eq!(binary(&chunks[1]).as_ptr() as usize - binary(&chunks[0]).as_ptr() as usize, 3);
        }
        let native = node(&pool, 4).await;
        let middle = python.process_async(bytes(&pool, &[1,2,3]), &context()).await.unwrap();
        let output = native.process_async(middle, &context()).await.unwrap();
        assert_eq!(binary(&output), &[7,8,9]);
        if let RuntimeData::Binary(data) = &output { drop(pool.export(data, remotemedia_shared_memory::PeerId(900)).unwrap()); }
        drop(native);
        drop(python);
        assert_eq!(binary(&output), &[7,8,9]);
        assert_eq!(binary(&forwarded), &[9,10]);
    }).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires MAPPED_WORKER_PYTHON, MAPPED_SDK_PYTHON and installed mapped wheel on PYTHONPATH"]
async fn mapped_python_waiting_call_cancellation_and_private_output_rejection() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let pool = pool();
        let python = python_node(&pool).await;
        let worker = python.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let mut started = Some(started);
        let task = tokio::spawn(async move {
            worker
                .process_streaming_async(
                    RuntimeData::Text("wait".into()),
                    &context(),
                    Box::new(move |_| {
                        if let Some(started) = started.take() {
                            let _ = started.send(());
                        }
                        Ok(())
                    }),
                )
                .await
        });
        ready.await.unwrap();
        let replies = rpc(
            python.as_ref(),
            RpcMethod::new("status_info", RpcMode::Reply),
            RpcId::Integer(17),
        )
        .await;
        assert!(matches!(&replies[0].event, RpcEvent::Value(value) if value["calls"] == 1));
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(python
            .process_control_message(RuntimeData::Text("unblock".into()), &context())
            .await
            .unwrap());
        let rejected = python
            .process_async(RuntimeData::Text("private".into()), &context())
            .await
            .unwrap_err();
        assert!(rejected
            .to_string()
            .contains("published RemoteMedia buffer"));
        let output = python
            .process_async(bytes(&pool, &[1, 2]), &context())
            .await
            .unwrap();
        assert_eq!(binary(&output), &[5, 6]);
    })
    .await
    .unwrap();
}

#[test]
fn input_scope_rejects_wrong_identity_before_native_startup() {
    let copies = WorkerInputCopies::new(
        "native-session",
        ["native-node".into()],
        CopyPolicy::RequireZeroCopy,
    );
    // SAFETY: identity validation must fail before this nonexistent binary starts.
    let factory = unsafe {
        NativeWorkerFactory::new(
            "nonexistent-input-policy-fixture".into(),
            "MappedNativeFixture".into(),
            pool(),
        )
    }
    .with_input_copies(copies.clone());
    for (node, session, expected) in [
        ("native-node", Some("other"), "different session"),
        ("native-node", None, "different session"),
        ("other", Some("native-session"), "has no node"),
    ] {
        let error = factory
            .create(
                node.into(),
                &serde_json::json!({}),
                session.map(str::to_owned),
            )
            .err()
            .unwrap();
        assert!(error.to_string().contains(expected), "{error}");
    }
    assert!(copies.reports()["native-node"]
        .entries
        .iter()
        .all(|(_, count)| count.completed == 0 && count.rejected == 0));
}

#[cfg(not(all(target_os = "linux", feature = "linux-worker-supervisor")))]
#[test]
fn linux_namespace_selection_requires_supported_platform_and_feature() {
    let factory = unsafe {
        NativeWorkerFactory::new(
            env!("CARGO_BIN_EXE_native-node-test-worker").into(),
            "MappedNativeFixture".into(),
            pool(),
        )
    };
    let error =
        unsafe { factory.with_linux_namespace("unused-helper".into(), "unused-unshare".into()) }
            .err()
            .expect("unsupported namespace configuration must fail");
    assert!(error.to_string().contains("require Linux"), "{error}");
}

#[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
#[test]
fn session_bindings_preserve_namespace_selection_and_never_fall_back() {
    use remotemedia_plugin_sdk::native_worker::SessionWorkerFactory;
    let factory = unsafe {
        NativeWorkerFactory::for_sessions(
            env!("CARGO_BIN_EXE_native-node-test-worker").into(),
            "MappedNativeFixture".into(),
        )
        .with_linux_namespace(namespace::supervisor(), "/bin/false".into())
    }
    .unwrap()
    .with_startup_timeout(Duration::from_millis(200));
    let copies = WorkerInputCopies::new(
        "native-session",
        ["native-node".into()],
        CopyPolicy::RequireZeroCopy,
    );
    for bound in [
        factory.bind_pool(pool()),
        factory.bind_session(pool(), copies.clone()),
    ] {
        // This node succeeds under a plain launch; losing containment during
        // session cloning would incorrectly turn this expected failure green.
        let error = bound
            .create(
                "native-node".into(),
                &serde_json::json!({}),
                Some("native-session".into()),
            )
            .err()
            .expect("invalid namespace launcher must not fall back");
        assert!(error.to_string().contains("namespace"), "{error}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_factory_enforces_and_retains_input_copy_scope() {
    verify_input_copy_scope(false).await;
}

#[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
#[tokio::test(flavor = "current_thread")]
async fn namespaced_factory_enforces_and_retains_input_copy_scope() {
    verify_input_copy_scope(true).await;
}

async fn verify_input_copy_scope(namespaced: bool) {
    let pool = pool();
    for policy in [CopyPolicy::RequireZeroCopy, CopyPolicy::AllowCopies] {
        let copies = WorkerInputCopies::new("native-session", ["native-node".into()], policy);
        // SAFETY: the fixture honors immutable publication and never delegates maps.
        let factory = unsafe {
            NativeWorkerFactory::new(
                env!("CARGO_BIN_EXE_native-node-test-worker").into(),
                "MappedNativeFixture".into(),
                pool.clone(),
            )
        }
        .with_input_copies(copies.clone());
        let factory = namespace::configure(factory, namespaced);
        let node = factory
            .create(
                "native-node".into(),
                &serde_json::json!({"gain":5,"require_namespace":namespaced}),
                Some("native-session".into()),
            )
            .unwrap();
        node.initialize(&Init::default()).await.unwrap();
        let result = node
            .process_async(RuntimeData::Binary(vec![1, 2, 3].into()), &context())
            .await;
        if policy == CopyPolicy::RequireZeroCopy {
            let error = result.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("session/native-session/node/native-node/mapped-input"),
                "{error}"
            );
        } else {
            assert_eq!(binary(&result.unwrap()), &[3, 4, 5]);
        }
        // An admission failure must leave both worker and call budget usable.
        let output = node
            .process_async(bytes(&pool, &[3, 4]), &context())
            .await
            .unwrap();
        assert_eq!(binary(&output), &[5, 6]);
        drop(node);
        assert_eq!(binary(&output), &[5, 6]);
        let report = &copies.reports()["native-node"];
        let counts = &report
            .entries
            .iter()
            .find(|(reason, _)| *reason == CopyReason::PrivateIpcIngress)
            .unwrap()
            .1;
        assert_eq!(
            counts.completed,
            u64::from(policy == CopyPolicy::AllowCopies)
        );
        assert_eq!(
            counts.copied_bytes,
            if policy == CopyPolicy::AllowCopies {
                3
            } else {
                0
            }
        );
        assert_eq!(
            counts.rejected,
            u64::from(policy == CopyPolicy::RequireZeroCopy)
        );
        assert_eq!(
            counts.rejected_bytes,
            if policy == CopyPolicy::RequireZeroCopy {
                3
            } else {
                0
            }
        );
        assert_eq!(counts.in_flight, 0);
    }
}
