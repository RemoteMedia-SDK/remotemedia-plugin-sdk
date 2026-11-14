//! Background calls on the mapped worker protocol (openspec:
//! add-source-node-lifecycle): a worker node's body runs in one long-lived
//! call whose outputs belong to no input.
#![cfg(all(feature = "test-native-worker", any(windows, target_os = "linux")))]
use remotemedia_plugin_sdk::{
    native_worker::{
        Limits, NativeWorkerFactory, NativeWorkerNode, PoolConfig, SessionId, SharedPool,
        SizeClass,
    },
    traits::{
        self,
        emit::{Emit, EmitOrigin, TryEmitError},
        InitializeContextRead, NodeRuntimeContextRead, StreamingNode, StreamingNodeFactory,
    },
    types::{Error, RuntimeData},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

struct Init;
impl InitializeContextRead for Init {
    fn session_id(&self) -> &str {
        "native-session"
    }
    fn node_id(&self) -> &str {
        "native-node"
    }
    fn emit_progress(&self, _: &str, _: &str) {}
}

/// Collects what the node emits, as a host's emitter would.
#[derive(Default)]
struct Collected(Mutex<Vec<String>>, tokio::sync::Notify);

#[async_trait::async_trait]
impl Emit for Collected {
    async fn emit_as(&self, data: RuntimeData, _origin: EmitOrigin) -> Result<(), Error> {
        let RuntimeData::Text(body) = data else {
            return Err(Error::Execution("expected text".into()));
        };
        self.0.lock().unwrap().push(body);
        self.1.notify_waiters();
        Ok(())
    }
    fn try_emit_as(&self, data: RuntimeData, _origin: EmitOrigin) -> Result<(), TryEmitError> {
        Err(TryEmitError::Full(data))
    }
    async fn started(&self) {}
    async fn closed(&self) {
        std::future::pending().await
    }
    fn is_closed(&self) -> bool {
        false
    }
    fn finish(&self) {}
}

impl Collected {
    fn frames(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
    async fn at_least(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let changed = self.1.notified();
                if self.0.lock().unwrap().len() >= count {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("frames in time")
    }
}

struct Ctx(Option<Arc<Collected>>);
impl NodeRuntimeContextRead for Ctx {
    fn emit_handle(&self) -> Option<Arc<dyn Emit>> {
        self.0.clone().map(|collected| collected as Arc<dyn Emit>)
    }
    fn session_id(&self) -> &str {
        "native-session"
    }
    fn node_id(&self) -> &str {
        "native-node"
    }
    fn capability(&self, _: &str) -> Option<traits::MediaCapabilities> {
        None
    }
    fn insert_capability(&self, _: &str, _: traits::MediaCapabilities) {}
    fn snapshot_any(&self, _: &str) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
    fn state_any(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
    fn try_state_any(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn pool() -> SharedPool {
    SharedPool::new(
        SessionId([85; 16]),
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

/// A worker node with these parameters; `background` selects whether the
/// host negotiates background calls.
async fn node(params: serde_json::Value, background: bool) -> Arc<dyn StreamingNode> {
    // SAFETY: this fixture uses the production runner and never delegates maps.
    let factory = unsafe {
        NativeWorkerFactory::new(
            env!("CARGO_BIN_EXE_native-node-test-worker").into(),
            "MappedNativeFixture".into(),
            pool(),
        )
    };
    let factory = if background {
        factory.with_background()
    } else {
        factory
    };
    let node: Arc<dyn StreamingNode> = Arc::from(
        factory
            .create("native-node".into(), &params, Some("native-session".into()))
            .unwrap(),
    );
    node.initialize(&Init).await.unwrap();
    node
}

async fn stream(node: &dyn StreamingNode, ctx: &Ctx, command: &str) -> Vec<String> {
    let replies = Arc::new(Mutex::new(Vec::new()));
    let sink = replies.clone();
    node.process_streaming_async(
        RuntimeData::Text(command.into()),
        ctx,
        Box::new(move |data| {
            if let RuntimeData::Text(body) = data {
                sink.lock().unwrap().push(body);
            }
            Ok(())
        }),
    )
    .await
    .unwrap();
    let replies = replies.lock().unwrap().clone();
    replies
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_source_runs_its_body_in_a_background_call() {
    let node = node(
        serde_json::json!({"body": "emit", "self_driven": true, "reactive": true}),
        true,
    )
    .await;
    // The worker declared what the host needs to treat it as a source.
    assert_eq!(node.self_driven(), Some(true));
    assert!(node.has_background());
    let collected = Arc::new(Collected::default());
    // The body emits three frames and returns: the call completes.
    tokio::time::timeout(
        Duration::from_secs(10),
        node.background(&Ctx(Some(collected.clone()))),
    )
    .await
    .expect("the body returned")
    .unwrap();
    assert_eq!(collected.frames(), ["body-0", "body-1", "body-2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_worker_body_is_an_error_of_the_background_call() {
    let node = node(serde_json::json!({"body": "fail", "reactive": true}), true).await;
    let collected = Arc::new(Collected::default());
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        node.background(&Ctx(Some(collected.clone()))),
    )
    .await
    .expect("the body ended")
    .expect_err("the body failed")
    .to_string();
    assert!(error.contains("worker body sentinel"), "{error}");
    assert_eq!(collected.frames(), ["body-0"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_emits_between_calls_only_while_its_background_call_is_open() {
    let node = node(serde_json::json!({"body": "run", "reactive": true}), true).await;
    let collected = Arc::new(Collected::default());
    let ctx = Ctx(Some(collected.clone()));

    // No background call yet: an emit from a call has nowhere to go.
    assert_eq!(stream(node.as_ref(), &ctx, "emit").await, ["unsupported"]);

    let running = {
        let (node, collected) = (node.clone(), collected.clone());
        tokio::spawn(async move { node.background(&Ctx(Some(collected))).await })
    };
    collected.at_least(1).await;
    // The call's own outputs are its reply only; the emitted frame arrives
    // on the background call, not among them.
    assert_eq!(stream(node.as_ref(), &ctx, "emit").await, ["emitted"]);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !collected.frames().iter().any(|frame| frame == "from a call") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the emitted frame arrived on the background call");

    // Stopping the instance cancels the call; the worker stays usable.
    running.abort();
    let _ = running.await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while stream(node.as_ref(), &ctx, "emit").await != ["unsupported"] {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the body stopped after its call was cancelled");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_the_background_protocol_a_worker_behaves_as_before() {
    // The host did not negotiate background calls: the worker announces no
    // such operation and no declarations, and its other calls work.
    let node = node(
        serde_json::json!({"body": "emit", "self_driven": true, "reactive": true}),
        false,
    )
    .await;
    assert!(!node.has_background());
    assert_eq!(node.self_driven(), None);
    let ctx = Ctx(None);
    assert_eq!(stream(node.as_ref(), &ctx, "emit").await, ["unsupported"]);
}

// ── The real mapped Python worker ───────────────────────────────────────
//
// `MAPPED_WORKER_PYTHON`: an interpreter that imports the matching
// `_remotemedia_buffers` extension and the Python client.
// `MAPPED_SDK_PYTHON`: the client's `clients/python` directory.

/// A Python node on the mapped runner, with background calls negotiated or
/// not.
async fn python_node(
    node_type: &str,
    params: &str,
    background: bool,
) -> Arc<dyn StreamingNode> {
    let python = std::env::var("MAPPED_WORKER_PYTHON")
        .expect("set MAPPED_WORKER_PYTHON to matching-wheel interpreter");
    let root = std::env::var("MAPPED_SDK_PYTHON").expect("set MAPPED_SDK_PYTHON to clients/python");
    let mut command = std::process::Command::new(python);
    command.args([
        "-m",
        "remotemedia_mapped_worker",
        "--node-type",
        node_type,
        "--node-id",
        "native-node",
        "--session-id",
        "native-session",
        "--params",
        params,
        "--module-root",
        &format!("{root}/tests/support"),
        "--register-module",
        "mapped_source_fixture",
    ]);
    let identity = remotemedia_shared_memory::peer::CallContext {
        session_id: Some("native-session".into()),
        node_id: "native-node".into(),
    };
    // SAFETY: the checked-in Python fixture and mapped runner use owning views,
    // never fork/delegate mappings, and run under the trusted worker contract.
    let node = unsafe {
        if background {
            NativeWorkerNode::spawn_with_background(
                &mut command,
                pool(),
                identity,
                Limits::default(),
                Duration::from_secs(60),
            )
        } else {
            NativeWorkerNode::spawn(
                &mut command,
                pool(),
                identity,
                Limits::default(),
                Duration::from_secs(60),
            )
        }
    }
    .unwrap();
    let node: Arc<dyn StreamingNode> = Arc::new(traits::AsyncNodeWrapper(Arc::new(node)));
    node.initialize(&Init).await.unwrap();
    node
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires MAPPED_WORKER_PYTHON and MAPPED_SDK_PYTHON"]
async fn a_mapped_python_source_node_runs_its_background_in_a_background_call() {
    let node = python_node("MappedSourceNode", r#"{"body":"emit"}"#, true).await;
    // A `SourceNode` class: self-driven, with a body.
    assert_eq!(node.self_driven(), Some(true));
    assert!(node.has_background());
    let collected = Arc::new(Collected::default());
    tokio::time::timeout(
        Duration::from_secs(30),
        node.background(&Ctx(Some(collected.clone()))),
    )
    .await
    .expect("background() returned")
    .unwrap();
    assert_eq!(collected.frames(), ["body-0", "body-1", "body-2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires MAPPED_WORKER_PYTHON and MAPPED_SDK_PYTHON"]
async fn a_failing_mapped_python_background_is_the_calls_error() {
    let node = python_node("MappedSourceNode", r#"{"body":"fail"}"#, true).await;
    let collected = Arc::new(Collected::default());
    let error = tokio::time::timeout(
        Duration::from_secs(30),
        node.background(&Ctx(Some(collected.clone()))),
    )
    .await
    .expect("background() ended")
    .expect_err("background() failed")
    .to_string();
    assert!(error.contains("python body sentinel"), "{error}");
    assert_eq!(collected.frames(), ["body-0"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires MAPPED_WORKER_PYTHON and MAPPED_SDK_PYTHON"]
async fn a_mapped_python_node_emits_between_calls_while_its_background_runs() {
    let node = python_node("MappedSourceNode", r#"{"body":"run"}"#, true).await;
    let collected = Arc::new(Collected::default());
    let ctx = Ctx(Some(collected.clone()));
    // Before the body runs, emit() has nowhere to go.
    assert_eq!(stream(node.as_ref(), &ctx, "emit").await, ["unsupported"]);

    let running = {
        let (node, collected) = (node.clone(), collected.clone());
        tokio::spawn(async move { node.background(&Ctx(Some(collected))).await })
    };
    collected.at_least(1).await;
    // The emitted frame is not one of the call's outputs.
    assert_eq!(stream(node.as_ref(), &ctx, "emit").await, ["emitted"]);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !collected.frames().iter().any(|frame| frame == "from a call") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the emitted frame arrived on the background call");

    // The instance stops: the call is cancelled and so is background().
    running.abort();
    let _ = running.await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while stream(node.as_ref(), &ctx, "emit").await != ["unsupported"] {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("background() stopped after its call was cancelled");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires MAPPED_WORKER_PYTHON and MAPPED_SDK_PYTHON"]
async fn a_mapped_python_node_without_a_body_gets_emit_unsupported() {
    let node = python_node("MappedPlainNode", "{}", true).await;
    assert!(!node.has_background());
    assert_eq!(node.self_driven(), None);
    assert_eq!(stream(node.as_ref(), &Ctx(None), "emit").await, ["unsupported"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires MAPPED_WORKER_PYTHON and MAPPED_SDK_PYTHON"]
async fn a_mapped_python_worker_on_a_host_without_the_protocol_behaves_as_before() {
    // The host did not ask for background calls: no declarations, no body.
    let node = python_node("MappedSourceNode", r#"{"body":"emit"}"#, false).await;
    assert!(!node.has_background());
    assert_eq!(node.self_driven(), None);
    assert_eq!(stream(node.as_ref(), &Ctx(None), "emit").await, ["unsupported"]);
}
