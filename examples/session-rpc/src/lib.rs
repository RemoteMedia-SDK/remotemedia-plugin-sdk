//! Small native plugin demonstrating session identity and dedicated RPC.
use async_trait::async_trait;
use remotemedia_plugin_sdk::{
    abi_stable::{
        sabi_trait::TD_Opaque,
        std_types::{ROk, RResult, RString},
    },
    adapter::StreamingNodeFfiAdapter,
    rpc::*,
    traits::{AsyncStreamingNode, NodeRuntimeContextRead},
    types::{Error, RuntimeData},
    FfiNodeBox, FfiNodeFactory, FfiNode_TO,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Default)]
struct Example {
    active: Arc<AtomicUsize>,
    notifications: AtomicUsize,
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl AsyncStreamingNode for Example {
    fn node_type(&self) -> &str {
        "SessionRpcExample"
    }
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        Ok(data)
    }
    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        callback(RuntimeData::Json(
            serde_json::json!({"session":ctx.session_id(), "node":ctx.node_id()}),
        ))?;
        if matches!(data, RuntimeData::Text(ref s) if s == "hold_media") {
            std::future::pending::<()>().await;
        }
        Ok(1)
    }
    fn rpc_methods(&self) -> Vec<RpcMethod> {
        vec![
            RpcMethod::new("identity", RpcMode::Reply),
            RpcMethod::new("watch", RpcMode::Stream),
            RpcMethod::new("hold", RpcMode::Stream),
            RpcMethod::new("fail", RpcMode::Reply),
            RpcMethod::new("notify", RpcMode::FireAndForget),
        ]
    }
    async fn call_rpc(
        &self,
        request: RpcRequest,
        ctx: &dyn NodeRuntimeContextRead,
        replies: &mut RpcResponder,
    ) -> Result<(), Error> {
        match request.method.as_str() {
            "identity" => replies.value(serde_json::json!({"session":ctx.session_id(), "node":ctx.node_id(), "active":self.active.load(Ordering::SeqCst), "notifications":self.notifications.load(Ordering::SeqCst)})).await,
            "watch" => {
                let (count,): (usize,) = request.positional()?;
                for index in 0..count { replies.value(index).await?; }
                Ok(())
            },
            "hold" => {
                self.active.fetch_add(1, Ordering::SeqCst);
                let _active = Active(self.active.clone());
                replies.value("started").await?;
                std::future::pending().await
            },
            "notify" => { self.notifications.fetch_add(1, Ordering::SeqCst); Ok(()) },
            "fail" => Err(Error::Execution("example RPC failure".into())),
            _ => Err(Error::Execution("unknown method".into())),
        }
    }
}

#[derive(Default)]
pub struct Factory;
impl FfiNodeFactory for Factory {
    fn node_type(&self) -> RString {
        "SessionRpcExample".into()
    }
    fn create(&self, _: RString) -> RResult<FfiNodeBox, RString> {
        ROk(FfiNode_TO::from_value(
            StreamingNodeFfiAdapter::new(Example::default()),
            TD_Opaque,
        ))
    }
}
remotemedia_plugin_sdk::plugin_export!(Factory);
