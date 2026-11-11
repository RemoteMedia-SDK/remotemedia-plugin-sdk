use abi_stable::{
    sabi_trait::TD_Opaque,
    std_types::{ROk, RResult, RString, RVec},
};
use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use remotemedia_plugin_sdk::{
    adapter::StreamingNodeFfiAdapter,
    rpc::*,
    traits::{AsyncStreamingNode, NodeRuntimeContextRead},
    types::{Error, RuntimeData},
    *,
};
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};

struct Fixture {
    started: Arc<Notify>,
    stopped: Arc<Notify>,
}
struct Stopped(Arc<Notify>);
impl Drop for Stopped {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[async_trait]
impl AsyncStreamingNode for Fixture {
    fn node_type(&self) -> &str {
        "Fixture"
    }
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        Ok(data)
    }
    async fn process_streaming<F>(
        &self,
        _: RuntimeData,
        session: Option<String>,
        mut cb: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        tokio::task::yield_now().await;
        cb(RuntimeData::Json(serde_json::json!({"session":session})))?;
        Ok(1)
    }
    fn rpc_methods(&self) -> Vec<RpcMethod> {
        vec![
            RpcMethod::new("identity", RpcMode::Reply),
            RpcMethod::new("watch", RpcMode::Stream),
            RpcMethod::new("fail", RpcMode::Reply),
            RpcMethod::new("wait", RpcMode::Stream),
        ]
    }
    async fn call_rpc(
        &self,
        request: RpcRequest,
        ctx: &dyn NodeRuntimeContextRead,
        replies: &mut RpcResponder,
    ) -> Result<(), Error> {
        match request.method.as_str() {
            "identity" => {
                replies
                    .value(serde_json::json!({"session":ctx.session_id(),"node":ctx.node_id()}))
                    .await
            }
            "watch" => {
                for i in 0..4 {
                    replies.value(i).await?;
                }
                Ok(())
            }
            "wait" => {
                let _stopped = Stopped(self.stopped.clone());
                self.started.notify_one();
                std::future::pending().await
            }
            _ => Err(Error::Execution("fixture error".into())),
        }
    }
}

fn node() -> (FfiNodeBox, Arc<Notify>, Arc<Notify>) {
    let started = Arc::new(Notify::new());
    let stopped = Arc::new(Notify::new());
    (
        FfiNode_TO::from_value(
            StreamingNodeFfiAdapter::new(Fixture {
                started: started.clone(),
                stopped: stopped.clone(),
            }),
            TD_Opaque,
        ),
        started,
        stopped,
    )
}
fn context(session: &str) -> FfiCallContext {
    FfiCallContext {
        session_id: Some(RString::from(session)).into(),
        node_id: "manager".into(),
        ..Default::default()
    }
}
fn request(method: &str) -> RVec<u8> {
    rmp_serde::to_vec_named(&RpcRequest {
        method: method.into(),
        corr_id: RpcId::String("client:1".into()),
        args: vec![],
        kwargs: Default::default(),
    })
    .unwrap()
    .into()
}
struct Media(mpsc::UnboundedSender<RuntimeData>);
impl OutputSink for Media {
    fn push(&self, bytes: RVec<u8>) -> RResult<(), RString> {
        self.0.send(rmp_serde::from_slice(&bytes).unwrap()).unwrap();
        ROk(())
    }
}
struct Replies(mpsc::Sender<RpcReply>);
impl remotemedia_plugin_sdk::RpcReplySink for Replies {
    fn push(&self, bytes: RVec<u8>) -> FfiFuture<RResult<(), RString>> {
        let tx = self.0.clone();
        async move {
            let reply = rmp_serde::from_slice(&bytes).unwrap();
            match tx.send(reply).await {
                Ok(()) => ROk(()),
                Err(_) => RResult::RErr("consumer closed".into()),
            }
        }
        .into_ffi()
    }
}
fn replies() -> (RpcReplySinkBox, mpsc::Receiver<RpcReply>) {
    let (tx, rx) = mpsc::channel(1);
    (RpcReplySink_TO::from_value(Replies(tx), TD_Opaque), rx)
}

#[tokio::test]
async fn concurrent_media_preserves_each_session_and_legacy_none() {
    let (node, _, _) = node();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let bytes: RVec<u8> = rmp_serde::to_vec_named(&RuntimeData::Text("input".into()))
        .unwrap()
        .into();
    let a = node.process_streaming_with_context(
        bytes.clone(),
        context("a"),
        OutputSink_TO::from_value(Media(tx.clone()), TD_Opaque),
    );
    let b = node.process_streaming_with_context(
        bytes.clone(),
        context("b"),
        OutputSink_TO::from_value(Media(tx.clone()), TD_Opaque),
    );
    let (a, b) = tokio::join!(a, b);
    a.into_result().unwrap();
    b.into_result().unwrap();
    let mut sessions = vec![];
    for _ in 0..2 {
        if let RuntimeData::Json(v) = rx.recv().await.unwrap() {
            sessions.push(v["session"].as_str().unwrap().to_string());
        }
    }
    sessions.sort();
    assert_eq!(sessions, ["a", "b"]);
    node.process_streaming(bytes, OutputSink_TO::from_value(Media(tx), TD_Opaque))
        .await
        .into_result()
        .unwrap();
    assert_eq!(
        rx.recv().await.unwrap(),
        RuntimeData::Json(serde_json::json!({"session":null}))
    );
}

#[tokio::test]
async fn rpc_preserves_identity_and_correlation() {
    let (node, _, _) = node();
    let (sink, mut rx) = replies();
    node.call_rpc(request("identity"), context("host-session"), sink)
        .await
        .into_result()
        .unwrap();
    let reply = rx.recv().await.unwrap();
    assert_eq!(reply.corr_id, RpcId::String("client:1".into()));
    assert_eq!(
        reply.event,
        RpcEvent::Value(serde_json::json!({"session":"host-session", "node":"manager"}))
    );
    assert!(rx.recv().await.is_none());
    let descriptors: Vec<RpcMethod> = serde_json::from_str(node.rpc_methods().as_str()).unwrap();
    assert_eq!(descriptors[1].mode, RpcMode::Stream);
}

#[tokio::test]
async fn streaming_backpressure_preserves_values_and_one_terminal() {
    let (node, _, _) = node();
    let (sink, mut rx) = replies();
    let future = node.call_rpc(request("watch"), context("session"), sink);
    let consumer = async {
        let mut events = vec![];
        while let Some(reply) = rx.recv().await {
            events.push(reply.event);
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        events
    };
    let (result, events) = tokio::join!(future, consumer);
    result.into_result().unwrap();
    assert_eq!(
        events,
        vec![
            RpcEvent::Value(0.into()),
            RpcEvent::Value(1.into()),
            RpcEvent::Value(2.into()),
            RpcEvent::Value(3.into()),
            RpcEvent::End
        ]
    );
}

#[tokio::test]
async fn error_is_correlated_and_sink_failure_is_propagated() {
    let (node, _, _) = node();
    let (sink, mut rx) = replies();
    node.call_rpc(request("fail"), context("session"), sink)
        .await
        .into_result()
        .unwrap();
    assert!(
        matches!(rx.recv().await.unwrap().event, RpcEvent::Error(e) if e.contains("fixture error"))
    );
    assert!(rx.recv().await.is_none());
    let (sink, rx) = replies();
    drop(rx);
    let error = node
        .call_rpc(request("watch"), context("session"), sink)
        .await
        .into_result()
        .unwrap_err();
    assert!(error.contains("consumer closed"));
}

#[tokio::test]
async fn dropping_ffi_future_cancels_plugin_runtime_task() {
    for shared in [false, true] {
        let (node, started, stopped) = node();
        let (sink, _rx) = replies();
        let future = if shared {
            node.call_rpc_shared(
                request("wait"),
                buffer::FfiExecutionContext {
                    identity: context("session"),
                    allocator: None.into(),
                    snapshots: Default::default(),
                    materialization: Some(copy_ledger::runtime::export(types::CopyContext::new(
                        types::CopyPolicy::RequireZeroCopy,
                        "rpc-cancel",
                    )))
                    .into(),
                },
                sink,
            )
        } else {
            node.call_rpc(request("wait"), context("session"), sink)
        };
        let mut future = Box::pin(future);
        tokio::select! { _ = started.notified() => {}, _ = &mut future => panic!("wait returned") }
        drop(future);
        tokio::time::timeout(std::time::Duration::from_secs(2), stopped.notified())
            .await
            .unwrap();
    }
}

#[test]
fn typed_arguments_and_wire_reply_match_existing_clients() {
    let request: RpcRequest = serde_json::from_value(serde_json::json!({"method":"load","corr_id":42,"args":["model",3],"kwargs":{"enabled":true}})).unwrap();
    assert_eq!(
        request.positional::<(String, usize)>().unwrap(),
        ("model".into(), 3)
    );
    assert!(request.positional::<(usize,)>().is_err());
    #[derive(serde::Deserialize)]
    struct Params {
        enabled: bool,
    }
    assert!(request.named::<Params>().unwrap().enabled);
    assert_eq!(
        serde_json::to_value(RpcReply {
            corr_id: request.corr_id,
            event: RpcEvent::End
        })
        .unwrap(),
        serde_json::json!({"corr_id":42,"kind":"end"})
    );
}

struct LegacyFfi;
impl FfiNode for LegacyFfi {
    fn node_type(&self) -> RString {
        "Legacy".into()
    }
    fn process(&self, bytes: RVec<u8>) -> FfiFuture<RResult<RVec<u8>, RString>> {
        async move { ROk(bytes) }.into_ffi()
    }
}

#[tokio::test]
async fn rebuilt_legacy_ffi_implementation_uses_defaults() {
    let node = FfiNode_TO::from_value(LegacyFfi, TD_Opaque);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let input = RuntimeData::Text("legacy".into());
    node.process_streaming_with_context(
        rmp_serde::to_vec_named(&input).unwrap().into(),
        context("session"),
        OutputSink_TO::from_value(Media(tx), TD_Opaque),
    )
    .await
    .into_result()
    .unwrap();
    assert_eq!(rx.recv().await.unwrap(), input);
    assert_eq!(node.rpc_methods().as_str(), "[]");
    let (sink, _rx) = replies();
    assert!(node
        .call_rpc(request("identity"), context("session"), sink)
        .await
        .into_result()
        .is_err());
}
