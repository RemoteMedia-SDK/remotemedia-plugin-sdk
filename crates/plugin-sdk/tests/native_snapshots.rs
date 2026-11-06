#![cfg(all(feature = "test-native-worker", any(windows, target_os = "linux")))]
#[path = "support/native_namespace.rs"]
mod namespace;
use remotemedia_plugin_sdk::{
    native_worker::{NativeWorkerFactory, PoolConfig, SessionId, SharedPool, SizeClass},
    traits::{
        self, InitializeContextRead, NodeRuntimeContextRead, StreamingNode, StreamingNodeFactory,
    },
    types::{Error, PayloadStorage, RuntimeData, TensorEncoding},
};
use remotemedia_shared_memory::{AllocationId, PeerId};
use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, Mutex},
};
use traits::ports::{OutputPort, SnapshotPort};
use traits::rpc::{RpcId, RpcMode, RpcReply, RpcReplySink, RpcRequest, RpcResponder};

struct Context {
    readers: HashMap<String, Arc<dyn SnapshotPort>>,
}
impl InitializeContextRead for Context {
    fn session_id(&self) -> &str {
        "native-session"
    }
    fn node_id(&self) -> &str {
        "native-node"
    }
    fn emit_progress(&self, _: &str, _: &str) {}
}
impl NodeRuntimeContextRead for Context {
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
    fn snapshot_inputs(&self) -> HashMap<String, Arc<dyn SnapshotPort>> {
        self.readers.clone()
    }
    fn snapshot_any(&self, port: &str) -> Option<Arc<dyn Any + Send + Sync>> {
        self.readers.get(port)?.snapshot_any()
    }
    fn state_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }
    fn try_state_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }
}
fn pool() -> SharedPool {
    SharedPool::new(
        SessionId([112; 16]),
        PoolConfig {
            classes: vec![SizeClass {
                bytes: 4096,
                slots: 4,
            }],
            max_bytes: 16384,
            max_leases: 32,
        },
    )
    .unwrap()
}
fn factory(pool: &SharedPool) -> NativeWorkerFactory {
    // SAFETY: fixture uses the production runner without mapping delegation.
    unsafe {
        NativeWorkerFactory::new(
            env!("CARGO_BIN_EXE_native-node-test-worker").into(),
            "MappedNativeFixture".into(),
            pool.clone(),
        )
    }
}
fn value(pool: &SharedPool, audio: bool, value: u8) -> RuntimeData {
    if audio {
        let mut loan = pool.allocate::<f32>(4).unwrap();
        loan.fill(value as f32);
        RuntimeData::Audio {
            samples: loan.freeze().into(),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        }
    } else {
        let mut loan = pool.allocate::<u8>(4).unwrap();
        loan.fill(value);
        RuntimeData::Binary(loan.freeze())
    }
}
fn allocation(pool: &SharedPool, value: &RuntimeData) -> AllocationId {
    let (_, storage) = value
        .clone()
        .into_shared_parts(TensorEncoding::Runtime)
        .unwrap();
    let grant = match storage {
        PayloadStorage::Audio(data) => pool.export(&data, PeerId(996)).unwrap(),
        PayloadStorage::Bytes(data) => pool.export(&data, PeerId(996)).unwrap(),
        _ => panic!("expected mapped storage"),
    };
    grant.descriptor().allocation
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

#[tokio::test(flavor = "current_thread")]
async fn native_context_hooks_sample_live_publications_and_preserve_mapped_owners() {
    verify_input_hooks(false).await;
}

#[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
#[tokio::test(flavor = "current_thread")]
async fn namespaced_context_hooks_sample_live_publications_and_preserve_mapped_owners() {
    verify_input_hooks(true).await;
}

async fn verify_input_hooks(namespaced: bool) {
    for audio in [false, true] {
        let pool = pool();
        let publication = OutputPort::empty();
        let ctx = Context {
            readers: HashMap::from([(
                "latest".into(),
                Arc::new(publication.input()) as Arc<dyn SnapshotPort>,
            )]),
        };
        let factory = namespace::configure(factory(&pool), namespaced)
            .with_snapshot_inputs(vec!["latest".into()])
            .unwrap();
        assert_eq!(
            factory.input_port_kinds()["latest"],
            traits::ports::PortKind::Snapshot
        );
        let node: Arc<dyn StreamingNode> = Arc::from(
            factory
                .create(
                    "native-node".into(),
                    &serde_json::json!({"require_namespace":namespaced}),
                    Some("native-session".into()),
                )
                .unwrap(),
        );
        node.initialize(&ctx).await.unwrap();
        node.validate_snapshot_input("latest", ctx.readers["latest"].as_ref())
            .unwrap();
        assert!(node
            .process_async(RuntimeData::Text("snapshot".into()), &ctx)
            .await
            .unwrap_err()
            .to_string()
            .contains("unpublished"));
        let first = value(&pool, audio, 7);
        let first_id = allocation(&pool, &first);
        publication.publish(first, 100);
        let unary = node
            .process_async(RuntimeData::Text("snapshot".into()), &ctx)
            .await
            .unwrap();
        assert_eq!(allocation(&pool, &unary), first_id);
        let named = node
            .process_multi_async(
                HashMap::from([("snapshot".into(), RuntimeData::Text("trigger".into()))]),
                &ctx,
            )
            .await
            .unwrap();
        assert_eq!(allocation(&pool, &named), first_id);
        let second = value(&pool, audio, 9);
        let second_id = allocation(&pool, &second);
        let replacement = publication.clone();
        let outputs = Arc::new(Mutex::new(Vec::new()));
        let sink = outputs.clone();
        let count = node
            .process_streaming_async(
                RuntimeData::Text("snapshots".into()),
                &ctx,
                Box::new(move |data| {
                    let mut values = sink.lock().unwrap();
                    if values.is_empty() {
                        replacement.publish(second.clone(), 200);
                    }
                    values.push(data);
                    Ok(())
                }),
            )
            .await
            .unwrap();
        assert_eq!(count, 3);
        assert_eq!(
            outputs
                .lock()
                .unwrap()
                .iter()
                .map(|v| allocation(&pool, v))
                .collect::<Vec<_>>(),
            vec![first_id, second_id, first_id]
        );
        let ticks = Arc::new(Mutex::new(Vec::new()));
        let sink = ticks.clone();
        node.tick(
            traits::Tick {
                clock_id: "snapshot".into(),
                pts_us: 200,
                frame_idx: 0,
                deadline_us: 0,
            },
            &ctx,
            Box::new(move |data| {
                sink.lock().unwrap().push(data);
                Ok(())
            }),
        )
        .await
        .unwrap();
        assert_eq!(allocation(&pool, &ticks.lock().unwrap()[0]), second_id);
        assert!(node
            .process_control_message(RuntimeData::Text("snapshot".into()), &ctx)
            .await
            .unwrap());
        let sink = Arc::new(Replies::default());
        let mut replies = RpcResponder::new(sink.clone(), RpcId::Integer(1), RpcMode::Reply);
        node.call_rpc(
            RpcRequest {
                method: "state".into(),
                corr_id: RpcId::Integer(1),
                args: vec![],
                kwargs: [("snapshot".into(), true.into())].into_iter().collect(),
            },
            &ctx,
            &mut replies,
        )
        .await
        .unwrap();
        assert!(
            matches!(&sink.0.lock().unwrap()[0].event, traits::rpc::RpcEvent::Value(v) if v["seq"]==1 && v["pts_us"]==200 && v["written_at_us"].as_u64().unwrap()>0)
        );
        // Strict failure is recoverable and never materializes the private bytes.
        publication.publish(RuntimeData::Binary(vec![1, 2, 3].into()), 300);
        assert!(node
            .process_async(RuntimeData::Text("snapshot".into()), &ctx)
            .await
            .is_err());
        publication.publish(RuntimeData::Text("metadata".into()), 400);
        assert!(
            matches!(node.process_async(RuntimeData::Text("snapshot".into()),&ctx).await.unwrap(),RuntimeData::Text(v) if v=="metadata")
        );
        drop(node);
        drop(ctx);
        drop(publication);
        assert_eq!(allocation(&pool, &unary), first_id);
        assert_eq!(allocation(&pool, &ticks.lock().unwrap()[0]), second_id);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_snapshot_registration_and_unsupported_bindings_fail_explicitly() {
    let pool = pool();
    assert!(factory(&pool)
        .with_snapshot_inputs(vec!["latest".into(), "latest".into()])
        .is_err());
    assert!(factory(&pool)
        .with_snapshot_inputs(vec![String::new()])
        .is_err());
    let typed = OutputPort::<u32>::empty();
    let portable = OutputPort::<RuntimeData>::empty();
    let plain = factory(&pool)
        .create(
            "native-node".into(),
            &serde_json::json!({}),
            Some("native-session".into()),
        )
        .unwrap();
    assert!(plain
        .validate_snapshot_input("latest", &portable.input())
        .is_err());
    let negotiated = factory(&pool)
        .with_snapshot_inputs(vec!["latest".into()])
        .unwrap()
        .create(
            "native-node".into(),
            &serde_json::json!({}),
            Some("native-session".into()),
        )
        .unwrap();
    assert!(negotiated
        .validate_snapshot_input("latest", &typed.input())
        .is_err());
    negotiated
        .validate_snapshot_input("latest", &portable.input())
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn native_output_readers_sample_latest_and_allow_same_peer_input_reads() {
    verify_output_readers(false).await;
}

#[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
#[tokio::test(flavor = "current_thread")]
async fn namespaced_output_readers_sample_latest_and_allow_same_peer_input_reads() {
    verify_output_readers(true).await;
}

async fn verify_output_readers(namespaced: bool) {
    for audio in [false, true] {
        let pool = pool();
        let factory = namespace::configure(factory(&pool), namespaced)
            .with_snapshot_outputs(vec!["latest".into()])
            .unwrap();
        assert_eq!(
            factory.output_port_kinds()["latest"],
            traits::ports::PortKind::Snapshot
        );
        let node: Arc<dyn StreamingNode> = Arc::from(
            factory
                .create(
                    "native-node".into(),
                    &serde_json::json!({"publish_snapshots":true,"require_namespace":namespaced}),
                    Some("native-session".into()),
                )
                .unwrap(),
        );
        assert!(node.snapshot_outputs().is_empty());
        let ctx = Context {
            readers: HashMap::new(),
        };
        node.initialize(&ctx).await.unwrap();
        let reader = node.snapshot_outputs().remove("latest").unwrap();
        assert!(reader.runtime_snapshot().unwrap().is_none());
        let first = node
            .process_async(value(&pool, audio, 3), &ctx)
            .await
            .unwrap();
        let first_id = allocation(&pool, &first);
        let old = reader.runtime_snapshot().unwrap().unwrap();
        assert_eq!((old.pts_us, old.seq), (987, 0));
        assert_eq!(allocation(&pool, &old.value), first_id);
        drop(first);
        // This read returns through the same connection handling its input read.
        // A receive-thread callback would deadlock; the bounded read task does not.
        let self_ctx = Context {
            readers: HashMap::from([("latest".into(), reader.clone())]),
        };
        let echoed = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            node.process_async(RuntimeData::Text("snapshot".into()), &self_ctx),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(allocation(&pool, &echoed), first_id);
        drop(echoed);
        let second = node
            .process_async(value(&pool, audio, 8), &ctx)
            .await
            .unwrap();
        let new = reader.runtime_snapshot().unwrap().unwrap();
        assert_eq!((new.pts_us, new.seq), (987, 1));
        assert_eq!(allocation(&pool, &new.value), allocation(&pool, &second));
        assert_ne!(allocation(&pool, &new.value), first_id);
        drop(second);
        drop(node);
        assert!(reader.runtime_snapshot().is_err());
        assert_eq!(allocation(&pool, &old.value), first_id);
        let spare1 = pool.allocate::<u8>(1).unwrap();
        let spare2 = pool.allocate::<u8>(1).unwrap();
        assert!(pool.allocate::<u8>(1).is_err());
        drop(old);
        let reclaimed = pool.allocate::<u8>(1).unwrap();
        assert!(pool.allocate::<u8>(1).is_err());
        drop(new);
        assert!(pool.allocate::<u8>(1).is_ok());
        drop((spare1, spare2, reclaimed));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_output_discovery_rejects_missing_and_nonportable_ports() {
    let pool = pool();
    let ctx = Context {
        readers: HashMap::new(),
    };
    for unsupported in [false, true] {
        let factory = factory(&pool)
            .with_snapshot_outputs(vec![if unsupported { "latest" } else { "missing" }.into()])
            .unwrap();
        let node = factory
            .create(
                "native-node".into(),
                &serde_json::json!({"publish_snapshots":true,"unsupported_snapshots":unsupported}),
                Some("native-session".into()),
            )
            .unwrap();
        let error = node.initialize(&ctx).await.unwrap_err().to_string();
        assert!(
            error.contains(if unsupported {
                "invalid portable worker snapshot outputs"
            } else {
                "differ from factory declarations"
            }),
            "{error}"
        );
        assert!(node.snapshot_outputs().is_empty());
    }
}
