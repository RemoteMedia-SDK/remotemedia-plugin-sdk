//! Generic StreamingNode → FfiNode adapter.
//!
//! Wraps any [`AsyncStreamingNode`] implementor in an `Arc<...>` so the
//! resulting [`FfiFuture`] is `'static`. Encodes/decodes [`RuntimeData`]
//! via `rmp-serde` for the wire boundary.

use std::any::Any;
use std::sync::{Arc, OnceLock};

use abi_stable::std_types::{RErr, ROk, RResult, RString, RVec};
use async_ffi::{FfiFuture, FutureExt};
use loadable_node_abi::{FfiNode, OutputSinkBox};
use remotemedia_traits::runtime_context::InitializeContextRead;
use remotemedia_traits::streaming::AsyncStreamingNode;
use remotemedia_types::RuntimeData;
use tokio::runtime::Runtime;

/// Tiny stand-in for the host's `InitializeContext`. Used to satisfy
/// `AsyncStreamingNode::initialize`'s `&dyn InitializeContextRead`
/// arg when invoked from the FFI side. Carries `session_id` and
/// `node_id` forwarded across the boundary; `emit_progress` is a
/// no-op (progress forwarding is not yet wired through FFI — see
/// `FfiNode::initialize` doc comment in loadable-node-abi).
struct PluginInitCtx {
    session_id: String,
    node_id: String,
}

impl InitializeContextRead for PluginInitCtx {
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
        }
    }
}

impl<N> FfiNode for StreamingNodeFfiAdapter<N>
where
    N: AsyncStreamingNode + Send + Sync + 'static,
{
    fn node_type(&self) -> RString {
        RString::from(self.node_type.clone())
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
            let join = plugin_runtime().spawn(async move { inner.process(data).await });
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
    fn initialize(&self, session_id: RString, node_id: RString) -> FfiFuture<RResult<(), RString>> {
        let inner = Arc::clone(&self.inner);
        async move {
            let ctx = PluginInitCtx {
                session_id: session_id.into_string(),
                node_id: node_id.into_string(),
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

            let join = plugin_runtime().spawn(async move {
                let cb =
                    move |data: RuntimeData| -> std::result::Result<(), remotemedia_types::Error> {
                        let bytes = rmp_serde::to_vec_named(&data).map_err(|e| {
                            remotemedia_types::Error::Execution(format!("rmp encode: {e}"))
                        })?;
                        outputs_for_cb.lock().expect("outputs mutex").push(bytes);
                        Ok(())
                    };
                inner.process_streaming(data, None, cb).await
            });

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
        let inner = Arc::clone(&self.inner);
        async move {
            let data: RuntimeData = match rmp_serde::from_slice(input.as_slice()) {
                Ok(d) => d,
                Err(e) => return RErr(RString::from(format!("rmp decode: {e}"))),
            };

            // `OutputSinkBox`'s `push` takes `&self`, so we only need
            // an `Arc` (not `Arc<Mutex<…>>`). The inner sink — see
            // [`loadable_node_abi::OutputSink`] — is responsible for
            // its own interior mutability.
            let sink = Arc::new(sink);
            let sink_for_cb = Arc::clone(&sink);

            let join = plugin_runtime().spawn(async move {
                let cb =
                    move |data: RuntimeData| -> std::result::Result<(), remotemedia_types::Error> {
                        let bytes = rmp_serde::to_vec_named(&data).map_err(|e| {
                            remotemedia_types::Error::Execution(format!("rmp encode: {e}"))
                        })?;
                        match sink_for_cb.push(RVec::from(bytes)) {
                            ROk(()) => Ok(()),
                            RErr(e) => Err(remotemedia_types::Error::Execution(e.into_string())),
                        }
                    };
                inner.process_streaming(data, None, cb).await
            });

            match join.await {
                Ok(Ok(count)) => ROk(count),
                Ok(Err(e)) => RErr(RString::from(format!("{e}"))),
                Err(e) => RErr(RString::from(format!("plugin task panicked: {e}"))),
            }
        }
        .into_ffi()
    }
}
