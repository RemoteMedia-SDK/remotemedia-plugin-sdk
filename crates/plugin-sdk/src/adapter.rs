//! Generic StreamingNode → FfiNode adapter.
//!
//! Wraps any [`AsyncStreamingNode`] implementor in an `Arc<...>` so the
//! resulting [`FfiFuture`] is `'static`. Encodes/decodes [`RuntimeData`]
//! via `rmp-serde` for the wire boundary.

use std::any::Any;
use std::sync::{Arc, OnceLock};

use abi_stable::std_types::{RErr, ROk, RResult, RString, RVec};
use async_ffi::{FfiFuture, FutureExt};
use loadable_node_abi::buffer::{
    FfiExecutionContext, FfiMessage, FfiNamedInput, SharedOutputSinkBox, BUFFER_ABI_VERSION,
};
use loadable_node_abi::{FfiCallContext, FfiNode, FfiTick, OutputSinkBox, RpcReplySinkBox};
use remotemedia_traits::rpc::{CallContext, RpcReply, RpcReplySink};
use remotemedia_traits::runtime_context::InitializeContextRead;
use remotemedia_traits::streaming::{AsyncStreamingNode, PacingNature, Tick};
use remotemedia_types::RuntimeData;
use tokio::runtime::Runtime;
mod shared;

/// Tiny stand-in for the host's `InitializeContext`. Used to satisfy
/// `AsyncStreamingNode::initialize`'s `&dyn InitializeContextRead`
/// arg when invoked from the FFI side. Carries `session_id` and
/// `node_id` forwarded across the boundary; `emit_progress` is a
/// no-op (progress forwarding is not yet wired through FFI — see
/// `FfiNode::initialize` doc comment in loadable-node-abi).
struct PluginInitCtx {
    session_id: String,
    node_id: String,
    /// The emitter the host bound to this instance, if it provides one.
    emitter: Option<crate::Emitter>,
}

impl InitializeContextRead for PluginInitCtx {
    fn emitter_any(&self) -> Option<&(dyn Any + Send + Sync)> {
        self.emitter
            .as_ref()
            .map(|emitter| emitter as &(dyn Any + Send + Sync))
    }

    fn session_id(&self) -> &str {
        &self.session_id
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn emit_progress(&self, _status: &str, _message: &str) {
        // Not forwarded across FFI today; progress messages emitted
        // from inside loadable plugin `initialize()` are dropped.
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Returns the plugin-side Tokio runtime, lazily constructed on first
/// access. The runtime is owned by the dynamic library that this
/// adapter is compiled into — each `.so`/`.dylib`/`.dll` gets its own
/// `OnceLock<Runtime>` because dlopen'd libraries do not share statics
/// with the host process.
///
/// Why this exists:
///
/// The host polls our [`FfiFuture`] from whatever thread the host
/// scheduler picks. That thread does NOT have a Tokio runtime handle
/// installed in TLS, so any inner future that calls
/// `tokio::runtime::Handle::current()` (e.g. `reqwest`,
/// `tokio::sync::Mutex`, `tokio::time::sleep`, `tokio::spawn`) panics
/// with `there is no reactor running`.
///
/// We fix it at the FFI boundary by spawning the inner work onto a
/// runtime owned by the plugin and awaiting the resulting JoinHandle.
/// The host's outer poll then just drives the JoinHandle, while the
/// inner future runs in a thread that *does* have a real Tokio handle.
fn plugin_runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("rm-plugin-rt")
            .build()
            .expect("plugin-sdk: failed to build tokio runtime")
    })
}

/// Wraps an [`AsyncStreamingNode`] to expose the abi_stable [`FfiNode`]
/// interface.
///
/// The wrapped node lives behind an `Arc<N>` because [`FfiFuture`]
/// requires `'static`, and the adapter must outlive any in-flight call.
/// Cloning the `Arc` into the async block lets the future own a strong
/// reference to the node without borrowing `&self`.
///
/// [`AsyncStreamingNode::process`] takes `&self`, so concurrent calls
/// against the same adapter are permitted — implementors of
/// `AsyncStreamingNode` are expected to provide their own interior
/// mutability (e.g. `parking_lot::Mutex` around an ONNX session) when
/// they need it. This matches the contract the rest of the runtime
/// holds: nodes look like `&self` services, not exclusive resources.
pub struct StreamingNodeFfiAdapter<N> {
    inner: Arc<N>,
    node_type: String,
    /// Bound by the host before `initialize`; read through `Emitter::of`.
    emitter: std::sync::OnceLock<crate::Emitter>,
}

impl<N> StreamingNodeFfiAdapter<N>
where
    N: AsyncStreamingNode + Send + Sync + 'static,
{
    /// Wrap a streaming node, capturing its `node_type()` for the FFI
    /// boundary so subsequent FFI calls don't have to ping the inner
    /// node just to read a constant string.
    pub fn new(node: N) -> Self {
        let node_type = node.node_type().to_string();
        Self {
            inner: Arc::new(node),
            node_type,
            emitter: std::sync::OnceLock::new(),
        }
    }
}

impl<N> FfiNode for StreamingNodeFfiAdapter<N>
where
    N: AsyncStreamingNode + Send + Sync + 'static,
{
    fn snapshot_outputs(
        &self,
    ) -> RResult<RVec<loadable_node_abi::snapshot::FfiSnapshotPort>, RString> {
        match loadable_node_abi::snapshot::runtime::export_ports(self.inner.snapshot_outputs()) {
            Ok(ports) => ROk(ports),
            Err(error) => RErr(error.to_string().into()),
        }
    }
    fn shared_buffer_protocol(&self) -> u16 {
        BUFFER_ABI_VERSION
    }

    fn is_multi_input(&self) -> bool {
        self.inner.is_multi_input()
    }

    fn process_shared(
        &self,
        input: FfiMessage,
        context: FfiExecutionContext,
    ) -> FfiFuture<RResult<FfiMessage, RString>> {
        let inner = self.inner.clone();
        shared::run(async move {
            let input = shared::decode(input)?;
            let context = shared::Context::try_from(context)?;
            shared::encode(inner.process_with_ctx(input, &context).await?)
        })
    }

    fn process_inputs_shared(
        &self,
        inputs: RVec<FfiNamedInput>,
        context: FfiExecutionContext,
    ) -> FfiFuture<RResult<FfiMessage, RString>> {
        let inner = self.inner.clone();
        shared::run(async move {
            let mut decoded = std::collections::HashMap::new();
            for input in inputs {
                if decoded
                    .insert(input.name.into_string(), shared::decode(input.message)?)
                    .is_some()
                {
                    return Err(remotemedia_types::Error::Execution(
                        "duplicate named plugin input".into(),
                    ));
                }
            }
            let context = shared::Context::try_from(context)?;
            shared::encode(inner.process_multi_with_ctx(decoded, &context).await?)
        })
    }

    fn process_many_shared(
        &self,
        input: FfiMessage,
        context: FfiExecutionContext,
    ) -> FfiFuture<RResult<RVec<FfiMessage>, RString>> {
        let inner = self.inner.clone();
        shared::run(async move {
            let input = shared::decode(input)?;
            let context = shared::Context::try_from(context)?;
            let mut outputs = Vec::new();
            inner
                .process_streaming_with_ctx(input, &context, |data| {
                    outputs.push(shared::encode(data)?);
                    Ok(())
                })
                .await?;
            Ok(outputs.into())
        })
    }

    fn process_streaming_shared(
        &self,
        input: FfiMessage,
        context: FfiExecutionContext,
        sink: SharedOutputSinkBox,
    ) -> FfiFuture<RResult<usize, RString>> {
        let inner = self.inner.clone();
        shared::run(async move {
            let input = shared::decode(input)?;
            let context = shared::Context::try_from(context)?;
            inner
                .process_streaming_with_ctx(input, &context, move |data| shared::push(&sink, data))
                .await
        })
    }

    fn tick_shared(
        &self,
        tick: FfiTick,
        context: FfiExecutionContext,
        sink: SharedOutputSinkBox,
    ) -> FfiFuture<RResult<usize, RString>> {
        let inner = self.inner.clone();
        shared::run(async move {
            let context = shared::Context::try_from(context)?;
            let tick = Tick {
                clock_id: tick.clock_id.into_string(),
                pts_us: tick.pts_us,
                frame_idx: tick.frame_idx,
                deadline_us: tick.deadline_us,
            };
            let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let emitted = count.clone();
            inner
                .tick_with_ctx(
                    tick,
                    &context,
                    Box::new(move |data| {
                        shared::push(&sink, data)?;
                        emitted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Ok(())
                    }),
                )
                .await?;
            Ok(count.load(std::sync::atomic::Ordering::Relaxed))
        })
    }

    fn process_control_shared(
        &self,
        input: FfiMessage,
        context: FfiExecutionContext,
    ) -> FfiFuture<RResult<bool, RString>> {
        let inner = self.inner.clone();
        shared::run(async move {
            let input = shared::decode(input)?;
            let context = shared::Context::try_from(context)?;
            inner.process_control_with_ctx(input, &context).await
        })
    }

    fn node_type(&self) -> RString {
        RString::from(self.node_type.clone())
    }

    fn pacing(&self) -> RString {
        let (nature, rate) = match self.inner.pacing_nature() {
            PacingNature::Reactive => ("reactive", self.inner.fallback_rate_hz()),
            PacingNature::ClockedToOutboundMedia => ("outbound", self.inner.fallback_rate_hz()),
            PacingNature::SourceWall(hz) => ("wall", hz),
        };
        // `instance_scope` rides in this JSON so the ABI does not change.
        serde_json::json!({"nature":nature, "rate_hz":rate,
            "clocked_media_addr":self.inner.clocked_media_addr(),
            "instance_scope":self.inner.instance_scope().map(|scope| scope.as_str())})
        .to_string()
        .into()
    }

    fn tick(
        &self,
        tick: FfiTick,
        session_id: RString,
        sink: OutputSinkBox,
    ) -> FfiFuture<RResult<usize, RString>> {
        let inner = Arc::clone(&self.inner);
        async move {
            let join = plugin_runtime().spawn(async move {
                let tick = Tick {
                    clock_id: tick.clock_id.into_string(),
                    pts_us: tick.pts_us,
                    frame_idx: tick.frame_idx,
                    deadline_us: tick.deadline_us,
                };
                inner
                    .tick(
                        tick,
                        Some(session_id.into_string()),
                        Box::new(move |data| push_output(&sink, data)),
                    )
                    .await
            });
            match join.await {
                Ok(Ok(())) => ROk(0),
                Ok(Err(e)) => RErr(e.to_string().into()),
                Err(e) => RErr(format!("plugin tick panicked: {e}").into()),
            }
        }
        .into_ffi()
    }

    fn process_control_message(
        &self,
        input: RVec<u8>,
        session_id: RString,
    ) -> FfiFuture<RResult<bool, RString>> {
        let inner = Arc::clone(&self.inner);
        async move {
            let data = match rmp_serde::from_slice(input.as_slice()) {
                Ok(data) => data,
                Err(e) => return RErr(format!("rmp decode: {e}").into()),
            };
            match plugin_runtime()
                .spawn(async move {
                    inner
                        .process_control_message(data, Some(session_id.into_string()))
                        .await
                })
                .await
            {
                Ok(Ok(handled)) => ROk(handled),
                Ok(Err(e)) => RErr(e.to_string().into()),
                Err(e) => RErr(format!("plugin control panicked: {e}").into()),
            }
        }
        .into_ffi()
    }

    fn process(&self, input: RVec<u8>) -> FfiFuture<RResult<RVec<u8>, RString>> {
        let inner = Arc::clone(&self.inner);
        async move {
            let data: RuntimeData = match rmp_serde::from_slice(input.as_slice()) {
                Ok(d) => d,
                Err(e) => {
                    return RErr(RString::from(format!("rmp decode: {e}")));
                }
            };
            // Spawn onto the plugin's dedicated runtime so the inner
            // future runs in a Tokio context (TLS-set runtime handle).
            // Anything inside `inner.process(...)` that requires
            // `tokio::runtime::Handle::current()` (e.g. `reqwest`,
            // `tokio::sync::Mutex`, `tokio::time::sleep`,
            // `tokio::spawn`) sees a real runtime, instead of the
            // empty FFI polling-thread context.
            // Batched path: endpoint calls fail fast with `unsupported`
            // (outputs only leave after `process` returns).
            let join = plugin_runtime().spawn(crate::endpoint_call::on_batched_path(async move {
                inner.process(data).await
            }));
            let result = match join.await {
                Ok(r) => r,
                Err(e) => {
                    return RErr(RString::from(format!("plugin task panicked: {e}")));
                }
            };
            match result {
                Ok(out) => match rmp_serde::to_vec_named(&out) {
                    Ok(bytes) => ROk(RVec::from(bytes)),
                    Err(e) => RErr(RString::from(format!("rmp encode: {e}"))),
                },
                Err(e) => RErr(RString::from(format!("{e}"))),
            }
        }
        .into_ffi()
    }

    /// Forwards the host's `AsyncStreamingNode::initialize()` across
    /// the FFI boundary. Required for lazy-load plugins (e.g.
    /// llama-cpp spawning a worker thread that loads the GGUF) —
    /// without this, `process()` runs first against an uninitialized
    /// inner node and returns "worker not running".
    fn bind_emitter(&self, emitter: loadable_node_abi::buffer::NodeEmitterBox) {
        let _ = self.emitter.set(crate::Emitter::bound(emitter));
    }

    fn initialize(&self, session_id: RString, node_id: RString) -> FfiFuture<RResult<(), RString>> {
        let inner = Arc::clone(&self.inner);
        let emitter = self.emitter.get().cloned();
        async move {
            let ctx = PluginInitCtx {
                session_id: session_id.into_string(),
                node_id: node_id.into_string(),
                emitter,
            };
            let join = plugin_runtime().spawn(async move { inner.initialize(&ctx).await });
            match join.await {
                Ok(Ok(())) => ROk(()),
                Ok(Err(e)) => RErr(RString::from(format!("{e}"))),
                Err(e) => RErr(RString::from(format!("plugin task panicked: {e}"))),
            }
        }
        .into_ffi()
    }

    /// Multi-output FFI entry point.
    ///
    /// Drives the wrapped node's `AsyncStreamingNode::process_streaming`
    /// (which can fire its callback N times per input) and collects
    /// every emission into an `RVec<RVec<u8>>`. The host side
    /// (`LoadableNodeAdapter::process_streaming_with_ctx`) then
    /// dispatches each blob into the host's streaming callback chain
    /// — so multi-output nodes (SileroVAD's JSON event + audio
    /// passthrough, Whisper's per-segment text fragments, etc.) keep
    /// their full output fan-out across the dlopen boundary.
    ///
    /// Buffered (not streamed) on purpose: typical multi-output nodes
    /// emit ≤ a handful of `RuntimeData` per input chunk, so there's
    /// no back-pressure concern. True streaming with back-pressure
    /// would need `async_ffi::BorrowingFfiStream` or similar — that's
    /// a future enhancement if/when a node produces large bursts.
    fn process_multi(&self, input: RVec<u8>) -> FfiFuture<RResult<RVec<RVec<u8>>, RString>> {
        let inner = Arc::clone(&self.inner);
        async move {
            let data: RuntimeData = match rmp_serde::from_slice(input.as_slice()) {
                Ok(d) => d,
                Err(e) => return RErr(RString::from(format!("rmp decode: {e}"))),
            };

            // Collect emissions into a shared Vec via the
            // FnMut callback. The mutex is only held across
            // `lock().push()` (no awaits) so it never blocks the
            // executor — and the callback itself is `+ Send` so
            // tokio::spawn is safe.
            let outputs = Arc::new(std::sync::Mutex::new(Vec::<Vec<u8>>::new()));
            let outputs_for_cb = Arc::clone(&outputs);

            let join = plugin_runtime().spawn(crate::endpoint_call::on_batched_path(async move {
                let cb =
                    move |data: RuntimeData| -> std::result::Result<(), remotemedia_types::Error> {
                        let bytes = rmp_serde::to_vec_named(&data).map_err(|e| {
                            remotemedia_types::Error::Execution(format!("rmp encode: {e}"))
                        })?;
                        outputs_for_cb.lock().expect("outputs mutex").push(bytes);
                        Ok(())
                    };
                inner.process_streaming(data, None, cb).await
            }));

            match join.await {
                Ok(Ok(_count)) => {
                    let collected = outputs
                        .lock()
                        .expect("outputs mutex")
                        .drain(..)
                        .map(RVec::from)
                        .collect::<Vec<_>>();
                    ROk(RVec::from(collected))
                }
                Ok(Err(e)) => RErr(RString::from(format!("{e}"))),
                Err(e) => RErr(RString::from(format!("plugin task panicked: {e}"))),
            }
        }
        .into_ffi()
    }

    /// Streaming multi-output entry point.
    ///
    /// Forwards each `process_streaming` emission to `sink` *as it
    /// arrives* — no internal `Vec` collection. This is the real-time
    /// path for nodes that emit over wall time (TTS audio chunks, STT
    /// segments, …). Without this override the default impl in
    /// `loadable-node-abi` would fall through to `process_multi`,
    /// which batches the entire turn before returning.
    fn process_streaming(
        &self,
        input: RVec<u8>,
        sink: OutputSinkBox,
    ) -> FfiFuture<RResult<usize, RString>> {
        self.process_streaming_with_context(input, FfiCallContext::default(), sink)
    }

    fn process_streaming_with_context(
        &self,
        input: RVec<u8>,
        context: FfiCallContext,
        sink: OutputSinkBox,
    ) -> FfiFuture<RResult<usize, RString>> {
        let inner = Arc::clone(&self.inner);
        async move {
            let data = match rmp_serde::from_slice(input.as_slice()) {
                Ok(data) => data,
                Err(e) => return RErr(format!("rmp decode: {e}").into()),
            };
            let join = plugin_runtime().spawn(async move {
                let context = call_context(context);
                inner
                    .process_streaming_with_ctx(data, &context, move |data| {
                        push_output(&sink, data)
                    })
                    .await
            });
            let _abort = AbortOnDrop(join.abort_handle());
            match join.await {
                Ok(Ok(count)) => ROk(count),
                Ok(Err(e)) => RErr(e.to_string().into()),
                Err(e) => RErr(format!("plugin task panicked: {e}").into()),
            }
        }
        .into_ffi()
    }

    fn rpc_methods(&self) -> RString {
        serde_json::to_string(&self.inner.rpc_methods())
            .expect("RPC descriptors serialize")
            .into()
    }

    fn call_rpc(
        &self,
        request: RVec<u8>,
        context: FfiCallContext,
        sink: RpcReplySinkBox,
    ) -> FfiFuture<RResult<(), RString>> {
        shared::rpc(
            self.inner.clone(),
            request,
            Ok(Box::new(call_context(context))),
            sink,
        )
    }

    fn call_rpc_shared(
        &self,
        request: RVec<u8>,
        context: FfiExecutionContext,
        sink: RpcReplySinkBox,
    ) -> FfiFuture<RResult<(), RString>> {
        let context = shared::Context::try_from(context)
            .map(|ctx| Box::new(ctx) as Box<dyn remotemedia_traits::NodeRuntimeContextRead>);
        shared::rpc(self.inner.clone(), request, context, sink)
    }
}

fn push_output(sink: &OutputSinkBox, data: RuntimeData) -> Result<(), remotemedia_types::Error> {
    let bytes = rmp_serde::to_vec_named(&data)
        .map_err(|e| remotemedia_types::Error::Execution(format!("rmp encode: {e}")))?;
    match sink.push(bytes.into()) {
        ROk(()) => Ok(()),
        RErr(e) => Err(remotemedia_types::Error::Execution(e.into_string())),
    }
}

fn call_context(context: FfiCallContext) -> CallContext {
    CallContext {
        session_id: context.session_id.into_option().map(|id| id.into_string()),
        node_id: context.node_id.into_string(),
    }
}

struct AbortOnDrop(tokio::task::AbortHandle);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct FfiReplies(RpcReplySinkBox);
#[async_trait::async_trait]
impl RpcReplySink for FfiReplies {
    async fn send(&self, reply: RpcReply) -> Result<(), remotemedia_types::Error> {
        let bytes = rmp_serde::to_vec_named(&reply)
            .map_err(|e| remotemedia_types::Error::Execution(format!("RPC encode: {e}")))?;
        self.0
            .push(bytes.into())
            .await
            .into_result()
            .map_err(|e| remotemedia_types::Error::Execution(e.into_string()))
    }
}
