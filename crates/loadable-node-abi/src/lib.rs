//! ABI-stable plugin interface.
//!
//! Both the host and the plugin depend on this crate. Everything that
//! crosses the `dlopen` boundary is defined here and uses `abi_stable`
//! types (`RVec`, `RString`, `RResult`, `RBox`, sabi-trait objects).
//!
//! `RuntimeData` itself never crosses the ABI as a native Rust enum. ABI 0.7
//! carries payload-free JSON metadata and retained immutable buffer owners in
//! `buffer::FfiMessage`. Its optional `runtime` bridge reconstructs local types
//! without copying bulk media. Legacy hooks retain MessagePack compatibility;
//! RPC uses its separate MessagePack reply contract.
//!
//! For the full contract — wire format, versioning policy, plugin
//! author rules, change history — see
//! [`docs/LOADABLE_NODE_ABI.md`](../../../docs/LOADABLE_NODE_ABI.md).

use abi_stable::{
    declare_root_module_statics,
    library::RootModule,
    package_version_strings, sabi_trait,
    sabi_types::VersionStrings,
    std_types::{RBox, RErr, ROk, ROption, RResult, RString, RVec},
    StableAbi,
};
use async_ffi::{FfiFuture, FutureExt};

pub mod buffer;
pub mod copy_ledger;
#[cfg(feature = "runtime")]
pub mod runtime_buffer;
pub mod snapshot;

/// Host clock signal, preserved across the plugin boundary.
#[repr(C)]
#[derive(Debug, Clone, StableAbi)]
pub struct FfiTick {
    pub clock_id: RString,
    pub pts_us: u64,
    pub frame_idx: u64,
    pub deadline_us: u64,
}

/// Owned identity for one invocation; never inferred from media or RPC payloads.
#[repr(C)]
#[derive(Debug, Clone, Default, StableAbi)]
pub struct FfiCallContext {
    pub session_id: ROption<RString>,
    pub node_id: RString,
}

/// FFI-safe node.
///
/// `process` returns an `FfiFuture` — an ABI-stable future the host
/// can `.await` directly. Plugin-side async runtimes (or none) work as
/// long as the future polls to completion without referencing
/// runtime-specific globals; in practice, plugins that call back into
/// host services do so by polling synchronous state from the async
/// block.
///
/// # Forward compatibility (multi-output extension)
///
/// `process` is marked `#[sabi(last_prefix_field)]` — that's the cut
/// between the original FFI surface (0.1) and any methods added in
/// minor versions. Methods added below it must carry a default impl
/// so source implementations may omit the new hooks. Compiled vtables
/// still undergo ABI layout and version checks; defaults alone are
/// not a binary compatibility guarantee. Rebuild plugins for ABI 0.7.
///
/// `process_multi` is the multi-output sibling of `process`: a node's
/// `process_streaming` callback can fire N times per input (think
/// SileroVAD emitting `Json(event)` plus the audio passthrough), and
/// the single-output `process` would silently drop everything but the
/// first emission. `process_multi` returns the full `RVec` so the
/// host can dispatch each blob into the streaming callback chain.
///
/// The default impl wraps `process` as a 1-element `RVec` so plugins
/// that only implement single-output stay correct (just lossy when
/// the underlying node was actually multi-output — same behaviour as
/// before this method existed).
#[sabi_trait]
pub trait FfiNode: Send + Sync + 'static {
    fn node_type(&self) -> RString;

    #[sabi(last_prefix_field)]
    fn process(&self, input: RVec<u8>) -> FfiFuture<RResult<RVec<u8>, RString>>;

    fn snapshot_outputs(&self) -> RResult<RVec<snapshot::FfiSnapshotPort>, RString> {
        ROk(RVec::new())
    }

    /// Multi-output process. Returns ALL emissions from one input as
    /// a flat `RVec<RVec<u8>>` (each inner vec is one rmp-serde
    /// `RuntimeData` blob, in emission order).
    ///
    /// Default impl: delegate to `process` and wrap its single output
    /// as a 1-element vec. Plugins that haven't been rebuilt against
    /// the multi-output ABI keep working — just without multi-output
    /// semantics. Plugins that override this method get full N-output
    /// fidelity through the `LoadableNodeAdapter` host wiring.
    fn process_multi(&self, input: RVec<u8>) -> FfiFuture<RResult<RVec<RVec<u8>>, RString>> {
        let fut = self.process(input);
        async move {
            match fut.await {
                ROk(out) => ROk(RVec::from(vec![out])),
                RErr(e) => RErr(e),
            }
        }
        .into_ffi()
    }

    /// Per-frame streaming process. Same wire format as `process_multi`
    /// (msgpack-encoded `RuntimeData` blobs in emission order) but each
    /// frame is delivered to `sink` *as it arrives* rather than
    /// accumulated and returned at the end. Returns the total emission
    /// count on completion.
    ///
    /// Why this exists: `process_multi` collects every emission into
    /// an `RVec<RVec<u8>>` before returning, so a streaming node that
    /// yields audio chunks over wall time (TTS, STT) emits nothing
    /// downstream until the *whole* generation finishes, then bursts
    /// every chunk at once. Real-time playback then perceives latency
    /// equal to total generation time. `process_streaming` fixes this
    /// by handing the plugin a [`OutputSinkBox`] that forwards each
    /// emission immediately.
    ///
    /// Default impl: delegates to `process_multi` and pushes each
    /// returned frame to the sink. Functionally correct but loses the
    /// real-time benefit — plugins must override this method (in
    /// practice via `LoadableNodeAdapter`, which forwards inside the
    /// inner node's own callback) to actually stream. Plugins on
    /// older ABI versions (vtable lacks this slot) fall back to the
    /// default, preserving load compatibility.
    fn process_streaming(
        &self,
        input: RVec<u8>,
        sink: OutputSinkBox,
    ) -> FfiFuture<RResult<usize, RString>> {
        let fut = self.process_multi(input);
        async move {
            match fut.await {
                ROk(outputs) => {
                    let mut count: usize = 0;
                    for out in outputs {
                        if let RErr(e) = sink.push(out) {
                            return RErr(e);
                        }
                        count += 1;
                    }
                    ROk(count)
                }
                RErr(e) => RErr(e),
            }
        }
        .into_ffi()
    }

    /// One-time, per-session initialization hook for lazy-load plugins.
    ///
    /// Forwarded from the host's `AsyncStreamingNode::initialize()`
    /// once per session, before the first `process` call. Plugins that
    /// do all their work eagerly inside `FfiNodeFactory::create()`
    /// (e.g. audio2face's `Audio2FaceLipSyncNode::load`, live2d-render's
    /// `WgpuBackend::new`) can leave this defaulted. Plugins with a
    /// non-trivial init (e.g. llama-cpp spawning a worker thread that
    /// loads a multi-GB GGUF) must override it — without forwarding,
    /// the worker is never spawned and `process` returns "worker not
    /// running".
    ///
    /// `session_id` and `node_id` are forwarded as RStrings so the
    /// plugin can log / tag work with them. `emit_progress` is NOT
    /// forwarded today — progress events emitted from inside a
    /// loadable plugin's `initialize()` are silently dropped. Plugin
    /// authors who need progress visibility should wrap heavy init in
    /// the host (e.g. via `WarmSessionPool::prewarm` which fires its
    /// own progress before delegating).
    ///
    /// Default impl: no-op. Older plugins not rebuilt against this
    /// method keep compiling, just without lazy-init semantics.
    fn initialize(
        &self,
        _session_id: RString,
        _node_id: RString,
    ) -> FfiFuture<RResult<(), RString>> {
        async { ROk(()) }.into_ffi()
    }

    /// JSON scheduling contract: nature (reactive, outbound, wall), rate_hz,
    /// and optional clocked_media_addr [medium, stream_id]. Older plugins
    /// remain reactive through this default prefix-extension method.
    fn pacing(&self) -> RString {
        RString::from("{\"nature\":\"reactive\",\"rate_hz\":30}")
    }

    /// One host-clock tick. Timestamps remain in the host session clock;
    /// outputs use the same MessagePack sink as reactive processing.
    fn tick(
        &self,
        _tick: FfiTick,
        _session_id: RString,
        _sink: OutputSinkBox,
    ) -> FfiFuture<RResult<usize, RString>> {
        async { ROk(0) }.into_ffi()
    }

    /// Forward auxiliary controls independently of data processing.
    /// Defaults to unhandled for plugins built against the earlier ABI.
    fn process_control_message(
        &self,
        _input: RVec<u8>,
        _session_id: RString,
    ) -> FfiFuture<RResult<bool, RString>> {
        async { ROk(false) }.into_ffi()
    }
    /// Context-aware media processing. The default preserves legacy implementations.
    fn process_streaming_with_context(
        &self,
        input: RVec<u8>,
        _context: FfiCallContext,
        sink: OutputSinkBox,
    ) -> FfiFuture<RResult<usize, RString>> {
        self.process_streaming(input, sink)
    }

    /// JSON array of RPC method descriptors. Empty means no dedicated RPC support.
    fn rpc_methods(&self) -> RString {
        "[]".into()
    }

    /// MessagePack RpcRequest in, MessagePack RpcReply frames on a dedicated sink.
    /// Dropping this future cancels asynchronous invocation work in SDK adapters.
    fn call_rpc(
        &self,
        _request: RVec<u8>,
        _context: FfiCallContext,
        _sink: RpcReplySinkBox,
    ) -> FfiFuture<RResult<(), RString>> {
        async { RErr("dedicated RPC is not supported".into()) }.into_ffi()
    }

    /// RPC payloads retain their dedicated codec; the execution context carries
    /// the owning allocator, snapshots and materialization ledger. No fallback
    /// to identity-only RPC is permitted when this hook is selected.
    fn call_rpc_shared(
        &self,
        _request: RVec<u8>,
        _context: buffer::FfiExecutionContext,
        _sink: RpcReplySinkBox,
    ) -> FfiFuture<RResult<(), RString>> {
        async { RErr("RPC execution context is not supported".into()) }.into_ffi()
    }

    /// 0 means explicit legacy-only media transport. Version 2 retains owners
    /// using buffer::FfiMessage; shared calls never fall back to MessagePack.
    fn shared_buffer_protocol(&self) -> u16 {
        0
    }

    fn is_multi_input(&self) -> bool {
        false
    }

    fn process_shared(
        &self,
        _input: buffer::FfiMessage,
        _context: buffer::FfiExecutionContext,
    ) -> FfiFuture<RResult<buffer::FfiMessage, RString>> {
        async { RErr("plugin does not support shared buffer processing".into()) }.into_ffi()
    }

    fn process_inputs_shared(
        &self,
        _inputs: RVec<buffer::FfiNamedInput>,
        _context: buffer::FfiExecutionContext,
    ) -> FfiFuture<RResult<buffer::FfiMessage, RString>> {
        async { RErr("plugin does not support shared multi-input processing".into()) }.into_ffi()
    }

    fn process_many_shared(
        &self,
        input: buffer::FfiMessage,
        context: buffer::FfiExecutionContext,
    ) -> FfiFuture<RResult<RVec<buffer::FfiMessage>, RString>> {
        let future = self.process_shared(input, context);
        async move { future.await.map(|message| vec![message].into()) }.into_ffi()
    }

    fn process_streaming_shared(
        &self,
        input: buffer::FfiMessage,
        context: buffer::FfiExecutionContext,
        sink: buffer::SharedOutputSinkBox,
    ) -> FfiFuture<RResult<usize, RString>> {
        let future = self.process_many_shared(input, context);
        async move {
            match future.await {
                ROk(messages) => {
                    let count = messages.len();
                    for message in messages {
                        if let RErr(error) = sink.push(message) {
                            return RErr(error);
                        }
                    }
                    ROk(count)
                }
                RErr(error) => RErr(error),
            }
        }
        .into_ffi()
    }

    fn tick_shared(
        &self,
        _tick: FfiTick,
        _context: buffer::FfiExecutionContext,
        _sink: buffer::SharedOutputSinkBox,
    ) -> FfiFuture<RResult<usize, RString>> {
        async { RErr("plugin does not support shared tick output".into()) }.into_ffi()
    }

    fn process_control_shared(
        &self,
        _input: buffer::FfiMessage,
        _context: buffer::FfiExecutionContext,
    ) -> FfiFuture<RResult<bool, RString>> {
        async { RErr("plugin does not support shared controls".into()) }.into_ffi()
    }

    /// The host hands the node instance its emitter, once, before
    /// `initialize` (openspec: add-node-initiated-output). A node that never
    /// produces output on its own keeps the default and drops it.
    fn bind_emitter(&self, _emitter: buffer::NodeEmitterBox) {}
}

/// Owned trait object for an FFI node.
///
/// `sabi_trait` drops the lifetime parameter when the trait has a
/// `'static` bound, so the alias does not name a lifetime.
pub type FfiNodeBox = FfiNode_TO<RBox<()>>;

/// Per-frame output sink for streaming plugins.
///
/// Crosses the dlopen boundary like `FfiNode`. The host hands an
/// [`OutputSinkBox`] to [`FfiNode::process_streaming`]; the plugin
/// invokes `push()` once per emission, and the host forwards each
/// blob into its own streaming callback chain (router → next node
/// → transport).
///
/// `push` takes `&self` because every realistic implementation
/// (tokio mpsc senders, lock-free queues) has interior mutability —
/// forcing `&mut self` would just push the synchronisation into the
/// plugin's callback closure.
///
/// `push` returns `RErr` if the host's receiver was dropped
/// mid-stream (session shutdown, barge-in). Plugins should treat
/// that as "consumer is gone, unwind ASAP" rather than retrying.
#[sabi_trait]
pub trait OutputSink: Send + Sync + 'static {
    fn push(&self, bytes: RVec<u8>) -> RResult<(), RString>;
}

/// Owned trait object for an output sink.
pub type OutputSinkBox = OutputSink_TO<RBox<()>>;

/// Asynchronous RPC-only sink. Hosts bind its destination and enforce capacity.
/// A failed push terminates delivery; plugins must not retry into a closed session.
#[sabi_trait]
pub trait RpcReplySink: Send + Sync + 'static {
    fn push(&self, bytes: RVec<u8>) -> FfiFuture<RResult<(), RString>>;
}

pub type RpcReplySinkBox = RpcReplySink_TO<RBox<()>>;

/// FFI-safe factory — produces FfiNode instances from a JSON params blob.
#[sabi_trait]
pub trait FfiNodeFactory: Send + Sync + 'static {
    fn node_type(&self) -> RString;
    fn create(&self, params: RString) -> RResult<FfiNodeBox, RString>;
    /// Pure capability declaration for these exact JSON parameters, before
    /// creating/initializing nodes. None is unknown. Some is BufferContract v1
    /// JSON, bounded to MAX_BUFFER_CONTRACT_BYTES by the host. Errors must not
    /// be converted to implicit support or cause a copying execution fallback.
    fn buffer_contract(
        &self,
        _params: RString,
    ) -> RResult<abi_stable::std_types::ROption<RString>, RString> {
        abi_stable::std_types::ROk(abi_stable::std_types::ROption::RNone)
    }
    fn input_ports(&self) -> RVec<snapshot::FfiPortDeclaration> {
        RVec::new()
    }
    fn output_ports(&self) -> RVec<snapshot::FfiPortDeclaration> {
        RVec::new()
    }
}

/// Owned trait object for an FFI factory.
pub type FfiNodeFactoryBox = FfiNodeFactory_TO<RBox<()>>;

pub const ABI_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const MAX_BUFFER_CONTRACT_BYTES: usize = 1024 * 1024;

// Android plugins sometimes need to force instantiation of certain libc++
// iostream template symbols, otherwise `dlopen` can fail at runtime with
// missing vtable symbols such as `std::__ndk1::basic_ifstream`.
//
// This helper is only built and called on Android targets. On non-Android
// platforms it is a no-op.
#[cfg(target_os = "android")]
mod android {
    extern "C" {
        fn loadable_node_android_force_libcxx_streams();
    }

    pub fn ensure_android_libcxx_streams() {
        unsafe { loadable_node_android_force_libcxx_streams() }
    }
}

#[cfg(not(target_os = "android"))]
mod android {
    pub fn ensure_android_libcxx_streams() {}
}

pub use android::ensure_android_libcxx_streams;

/// Root module exported by every plugin.
///
/// abi_stable validates layout, abi_stable version, and prefix-type
/// compatibility when the host calls `NodePluginRef::load_from_file`.
#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = NodePluginRef)))]
#[sabi(missing_field(panic))]
pub struct NodePlugin {
    /// Returns every factory this plugin provides.
    #[sabi(last_prefix_field)]
    pub list_factories: extern "C" fn() -> RVec<FfiNodeFactoryBox>,
}

impl RootModule for NodePluginRef {
    declare_root_module_statics! {NodePluginRef}
    const BASE_NAME: &'static str = "node_plugin";
    const NAME: &'static str = "node_plugin";
    const VERSION_STRINGS: VersionStrings = package_version_strings!();
}
