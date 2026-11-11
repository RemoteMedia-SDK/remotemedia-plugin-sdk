//! Shared-media execution helpers; cancellation and runtime ownership match the
//! legacy adapter while bulk buffers remain separately owned throughout.
use super::{plugin_runtime, AbortOnDrop};
use abi_stable::std_types::{RErr, ROk, RResult, RString};
use async_ffi::{FfiFuture, FutureExt};
use loadable_node_abi::{
    buffer::{FfiExecutionContext, FfiMessage, SharedOutputSinkBox},
    runtime_buffer,
};
use remotemedia_traits::{MediaCapabilities, NodeRuntimeContextRead};
use remotemedia_types::{BufferAllocationError, BufferLoan, Error, OutputAllocator, RuntimeData};
use std::{any::Any, future::Future, sync::Arc};

pub(super) fn run<T: Send + 'static>(
    work: impl Future<Output = Result<T, Error>> + Send + 'static,
) -> FfiFuture<RResult<T, RString>> {
    async move {
        let join = plugin_runtime().spawn(work);
        let _abort = AbortOnDrop(join.abort_handle());
        match join.await {
            Ok(Ok(value)) => ROk(value),
            Ok(Err(error)) => RErr(error.to_string().into()),
            Err(error) => RErr(format!("plugin shared call failed: {error}").into()),
        }
    }
    .into_ffi()
}

pub(super) fn decode(message: FfiMessage) -> Result<RuntimeData, Error> {
    runtime_buffer::decode(message).map_err(|error| Error::Execution(error.into_string()))
}

pub(super) fn rpc<T: remotemedia_traits::AsyncStreamingNode + 'static>(
    inner: Arc<T>,
    request: abi_stable::std_types::RVec<u8>,
    context: Result<Box<dyn NodeRuntimeContextRead>, Error>,
    sink: loadable_node_abi::RpcReplySinkBox,
) -> FfiFuture<RResult<(), RString>> {
    use remotemedia_traits::rpc::{RpcMode, RpcRequest, RpcResponder};
    run(async move {
        let context = context?;
        let request: RpcRequest = rmp_serde::from_slice(request.as_slice())
            .map_err(|e| Error::Execution(format!("invalid RPC request: {e}")))?;
        let mode = inner
            .rpc_methods()
            .iter()
            .find(|m| m.name == request.method)
            .map(|m| m.mode);
        let mut replies = RpcResponder::new(
            Arc::new(super::FfiReplies(sink)),
            request.corr_id.clone(),
            mode.unwrap_or(RpcMode::Reply),
        );
        let result = if mode.is_some() {
            inner.call_rpc(request, &*context, &mut replies).await
        } else {
            Err(Error::Execution(format!(
                "unknown RPC method: {}",
                request.method
            )))
        };
        replies.finish(result).await
    })
}
pub(super) fn encode(data: RuntimeData) -> Result<FfiMessage, Error> {
    runtime_buffer::encode(data).map_err(|error| Error::Execution(error.into_string()))
}
pub(super) fn push(sink: &SharedOutputSinkBox, data: RuntimeData) -> Result<(), Error> {
    sink.push(encode(data)?)
        .into_result()
        .map_err(|error| Error::Execution(error.into_string()))
}

pub(super) struct Context {
    origin: Option<Arc<crate::Origin>>,
    emitter: Option<crate::Emitter>,
    identity: remotemedia_traits::rpc::CallContext,
    materialization: Option<remotemedia_types::CopyContext>,
    allocator: Option<Arc<dyn OutputAllocator>>,
    snapshots: std::collections::HashMap<String, Arc<dyn remotemedia_traits::ports::SnapshotPort>>,
}
impl TryFrom<FfiExecutionContext> for Context {
    type Error = Error;
    fn try_from(context: FfiExecutionContext) -> Result<Self, Error> {
        Ok(Self {
            origin: super::ffi_origin(&context.identity),
            emitter: None,
            materialization: context
                .materialization
                .into_option()
                .map(loadable_node_abi::copy_ledger::runtime::import),
            snapshots: loadable_node_abi::snapshot::runtime::import_ports(context.snapshots)?,
            identity: super::call_context(context.identity),
            allocator: context
                .allocator
                .into_option()
                .map(runtime_buffer::import_allocator),
        })
    }
}
impl Context {
    /// The instance's emitter, for `Emitter::for_call`.
    pub(super) fn with_emitter(mut self, emitter: Option<crate::Emitter>) -> Self {
        self.emitter = emitter;
        self
    }
}
impl NodeRuntimeContextRead for Context {
    fn origin(&self) -> Option<Arc<crate::Origin>> {
        self.origin.clone()
    }
    fn emitter_any(&self) -> Option<&(dyn Any + Send + Sync)> {
        self.emitter
            .as_ref()
            .map(|emitter| emitter as &(dyn Any + Send + Sync))
    }
    fn copy_context(&self) -> Option<remotemedia_types::CopyContext> {
        self.materialization.clone()
    }
    fn output_allocator(&self) -> Option<Arc<dyn OutputAllocator>> {
        self.allocator.clone()
    }
    fn allocate_output_bytes(&self, len: usize) -> Result<BufferLoan<u8>, BufferAllocationError> {
        self.allocator
            .as_ref()
            .ok_or(BufferAllocationError::Unavailable)?
            .allocate_bytes(len)
    }
    fn allocate_output_audio(
        &self,
        samples: usize,
    ) -> Result<BufferLoan<f32>, BufferAllocationError> {
        self.allocator
            .as_ref()
            .ok_or(BufferAllocationError::Unavailable)?
            .allocate_audio(samples)
    }
    fn session_id(&self) -> &str {
        self.identity.session_id()
    }
    fn node_id(&self) -> &str {
        self.identity.node_id()
    }
    fn capability(&self, _: &str) -> Option<MediaCapabilities> {
        None
    }
    fn insert_capability(&self, _: &str, _: MediaCapabilities) {}
    fn snapshot_any(&self, port: &str) -> Option<Arc<dyn Any + Send + Sync>> {
        self.snapshots.get(port)?.snapshot_any()
    }
    fn snapshot_inputs(
        &self,
    ) -> std::collections::HashMap<String, Arc<dyn remotemedia_traits::ports::SnapshotPort>> {
        self.snapshots.clone()
    }
    fn state_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }
    fn try_state_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
