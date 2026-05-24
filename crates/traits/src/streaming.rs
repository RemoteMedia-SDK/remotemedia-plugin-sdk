//! Streaming-node trait surface.
//!
//! Hosts the trait declarations every node implements
//! (`StreamingNode`, `SyncStreamingNode`, `AsyncStreamingNode`,
//! `StreamingNodeFactory`) plus the wrapper types that bridge sync /
//! async into the unified trait (`SyncNodeWrapper`, `AsyncNodeWrapper`).
//!
//! The traits' per-call context parameters use the read-only trait
//! surface (`&dyn NodeRuntimeContextRead`, `&dyn InitializeContextRead`)
//! defined in `runtime_context`, so plugin code that links this crate
//! never has to depend on the host's heavy concrete context structs in
//! `remotemedia-core`. In-tree code that needs to access heavy fields
//! (e.g. the chat backend's barge-in cancel gate) downcasts via
//! `NodeRuntimeContextRead::as_any`.
//!
//! The session-level `StreamingNodeRegistry` and inventory plumbing
//! continue to live in `remotemedia-core`.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use remotemedia_types::{Error, RuntimeData};

use crate::capabilities::{CapabilityBehavior, MediaCapabilities};
use crate::ports::{PortKind, SnapshotPort};
use crate::runtime_context::{InitializeContextRead, NodeRuntimeContextRead};
use crate::schema::NodeSchema;

/// Function-parameter type delivered to `StreamingNode::tick` by a Pacer.
///
/// `Tick` is **not** a `RuntimeData` variant — earlier proposal drafts modeled
/// it that way and the design was rejected because a tick is a control signal,
/// not data. Carrying it as a structured method parameter makes the
/// concurrency contract honest: `tick` is called serially by the Pacer, while
/// `process_streaming` may run concurrently across input arrivals.
///
/// `clock_id` follows the canonical scheme `<medium>:<stream_id>` for
/// wire-bound clocks (e.g. `audio:default`, `video:default`) or `wall:<hz>`
/// for self-paced source nodes.
#[derive(Debug, Clone)]
pub struct Tick {
    /// Canonical clock address that fired this tick.
    pub clock_id: String,
    /// Presentation timestamp the listener is about to receive (in microseconds).
    pub pts_us: u64,
    /// Monotonic frame index of this tick within the clock's stream.
    pub frame_idx: u64,
    /// Wall-clock deadline (microseconds) the node SHOULD return by.
    pub deadline_us: u64,
}

/// Pacing nature declared by a node, used by the runtime to derive how the
/// node is invoked.
///
/// - [`PacingNature::Reactive`] (default) — the node is invoked via
///   `process_streaming` whenever an upstream input arrives. No clock binding.
/// - [`PacingNature::ClockedToOutboundMedia`] — the node terminates into an
///   outbound media stream and SHOULD have its `tick` method called per
///   `media_clock` event from that stream.
/// - [`PacingNature::SourceWall(hz)`] — the node has no upstream input and
///   no wire-bound clock; the runtime spawns a `wall:<hz>` Pacer that
///   invokes `tick` at that rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacingNature {
    Reactive,
    ClockedToOutboundMedia,
    SourceWall(u32),
}

impl Default for PacingNature {
    fn default() -> Self {
        PacingNature::Reactive
    }
}

/// Node execution status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeStatus {
    /// Node created but not initialized
    Idle = 0,
    /// Node is currently initializing (loading models, resources, etc.)
    Initializing = 1,
    /// Node is initialized and ready to process data
    Ready = 2,
    /// Node is currently processing data
    Processing = 3,
    /// Node encountered an error
    Error = 4,
    /// Node is being cleaned up
    Stopping = 5,
    /// Node has been stopped/destroyed
    Stopped = 6,
}

impl NodeStatus {
    /// Convert from u8 (for atomic storage)
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => NodeStatus::Idle,
            1 => NodeStatus::Initializing,
            2 => NodeStatus::Ready,
            3 => NodeStatus::Processing,
            4 => NodeStatus::Error,
            5 => NodeStatus::Stopping,
            6 => NodeStatus::Stopped,
            _ => NodeStatus::Idle,
        }
    }

    /// Convert to string for logging/display
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeStatus::Idle => "idle",
            NodeStatus::Initializing => "initializing",
            NodeStatus::Ready => "ready",
            NodeStatus::Processing => "processing",
            NodeStatus::Error => "error",
            NodeStatus::Stopping => "stopping",
            NodeStatus::Stopped => "stopped",
        }
    }
}

/// Per-(node, session) state slot. Implementors are downcast at call
/// sites via the host's `NodeRuntimeContext::state` accessor.
///
/// The blanket impl makes any `Send + Sync + 'static` type usable as a
/// session-state payload — concrete state types do **not** need to
/// implement this trait directly.
///
/// Carries one method, [`Self::into_any_arc`], so an
/// `Arc<dyn AnySessionState>` can be re-erased to
/// `Arc<dyn Any + Send + Sync>` for the read-trait surface in
/// `runtime_context`. Trait upcasting (`&dyn AnySessionState` →
/// `&dyn Any`) is also available for borrowed inspection.
pub trait AnySessionState: Any + Send + Sync + 'static {
    /// Upcast an `Arc<Self>` to `Arc<dyn Any + Send + Sync>` so the
    /// erased read-trait helpers can downcast back to the consumer's
    /// expected concrete type.
    ///
    /// Provided by the blanket impl below; concrete state types never
    /// implement this directly.
    fn into_any_arc(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn Any + Send + Sync>;
}

impl<T> AnySessionState for T
where
    T: Any + Send + Sync + 'static,
{
    fn into_any_arc(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn Any + Send + Sync> {
        // `Arc<T>` for concrete `T: Any + Send + Sync + 'static`
        // unsizes directly to `Arc<dyn Any + Send + Sync>` — the
        // compiler emits the vtable at this site.
        self
    }
}

// ─── Streaming-node trait declarations ──────────────────────────────

/// Synchronous streaming node trait.
///
/// Implement for Rust nodes that can process data without blocking the
/// async runtime. The unified [`StreamingNode`] trait is auto-implemented
/// for any `SyncStreamingNode` via [`SyncNodeWrapper`].
pub trait SyncStreamingNode: Send + Sync {
    /// Get the node type name.
    fn node_type(&self) -> &str;

    /// Process single-input data.
    fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error>;

    /// Process multi-input data (for nodes that require multiple named
    /// inputs).
    fn process_multi(&self, inputs: HashMap<String, RuntimeData>) -> Result<RuntimeData, Error> {
        if let Some((_name, data)) = inputs.into_iter().next() {
            self.process(data)
        } else {
            Err(Error::Execution("No input data provided".into()))
        }
    }

    /// Check if this node requires multiple inputs.
    fn is_multi_input(&self) -> bool {
        false
    }

    /// Process one input and emit zero or more outputs via `callback`.
    ///
    /// The callback is `&mut dyn FnMut` (not a generic) so the trait
    /// remains dyn-compatible — `Box<dyn SyncStreamingNode>` stays
    /// constructible.
    fn process_streaming(
        &self,
        data: RuntimeData,
        _session_id: Option<&str>,
        callback: &mut dyn FnMut(RuntimeData) -> Result<(), Error>,
    ) -> Result<usize, Error> {
        callback(self.process(data)?)?;
        Ok(1)
    }
}

/// Asynchronous streaming node trait.
///
/// Implement for nodes that require async processing (Python nodes,
/// I/O-bound operations). Bridged to the unified [`StreamingNode`] via
/// [`AsyncNodeWrapper`].
#[async_trait::async_trait]
pub trait AsyncStreamingNode: Send + Sync {
    /// Get the node type name.
    fn node_type(&self) -> &str;

    /// Initialize the node (load models, resources, etc.).
    ///
    /// Use `ctx.emit_progress()` to report loading progress.
    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        Ok(())
    }

    /// Process single-input data asynchronously.
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error>;

    /// Process multi-input data asynchronously.
    async fn process_multi(
        &self,
        inputs: HashMap<String, RuntimeData>,
    ) -> Result<RuntimeData, Error> {
        if let Some((_name, data)) = inputs.into_iter().next() {
            self.process(data).await
        } else {
            Err(Error::Execution("No input data provided".into()))
        }
    }

    /// Check if this node requires multiple inputs.
    fn is_multi_input(&self) -> bool {
        false
    }

    /// Process input and stream multiple outputs via callback.
    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        _session_id: Option<String>,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let output = self.process(data).await?;
        callback(output)?;
        Ok(1)
    }

    /// Ctx-aware streaming entry point.
    ///
    /// Default impl preserves the legacy contract: derives a
    /// `session_id` from `ctx` and delegates to `process_streaming`.
    /// Nodes that need access to the runtime context (cancel gate via
    /// `ctx.as_any().downcast_ref::<NodeRuntimeContext>()`, snapshot
    /// reads via `ctx.snapshot_any(...)`, capability cache via
    /// `ctx.capability(...)`) override this method.
    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let session_id = if ctx.session_id().is_empty() {
            None
        } else {
            Some(ctx.session_id().to_string())
        };
        self.process_streaming(data, session_id, callback).await
    }

    /// Process control message for pipeline flow control.
    async fn process_control_message(
        &self,
        _message: RuntimeData,
        _session_id: Option<String>,
    ) -> Result<bool, Error> {
        Ok(false)
    }

    // ─── Pacing-domains opt-in (mirrors `StreamingNode`) ─────────────

    /// Pacing nature for this node.
    fn pacing_nature(&self) -> PacingNature {
        PacingNature::Reactive
    }

    /// `(medium, stream_id)` pair for `ClockedToOutboundMedia`.
    fn clocked_media_addr(&self) -> Option<(String, String)> {
        None
    }

    /// Fallback wall-clock Hz when `pacing_nature() ==
    /// ClockedToOutboundMedia` and no wire clock is bound.
    fn fallback_rate_hz(&self) -> u32 {
        30
    }

    /// Tick-driven entry point. Pacer invokes once per tick. Default
    /// no-op so reactive nodes don't need to implement it.
    async fn tick(
        &self,
        _tick: Tick,
        _session_id: Option<String>,
        _callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

/// Unified streaming node trait used by the registry and pipeline
/// executor.
///
/// Auto-implemented for both `SyncStreamingNode` (via
/// [`SyncNodeWrapper`]) and `AsyncStreamingNode` (via
/// [`AsyncNodeWrapper`]).
#[async_trait::async_trait]
pub trait StreamingNode: Send + Sync {
    /// Get the node type name.
    fn node_type(&self) -> &str;

    /// Get the node ID. Default empty — overridden by nodes that
    /// have IDs.
    fn node_id(&self) -> &str {
        ""
    }

    /// Get the current execution status. Default `Idle`.
    fn get_status(&self) -> NodeStatus {
        NodeStatus::Idle
    }

    /// Initialize the node (load models, resources, etc.).
    ///
    /// Called during pre-initialization. Use `ctx.emit_progress()` to
    /// report loading state.
    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        Ok(())
    }

    /// Process single-input data asynchronously.
    async fn process_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error>;

    /// Process multi-input data asynchronously.
    async fn process_multi_async(
        &self,
        inputs: HashMap<String, RuntimeData>,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error>;

    /// Check if this node requires multiple inputs.
    fn is_multi_input(&self) -> bool;

    /// Process data with streaming callback (for multi-output nodes).
    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<usize, Error> {
        let mut callback = callback;
        let output = self.process_async(data, ctx).await?;
        callback(output)?;
        Ok(1)
    }

    /// Process control message for pipeline flow control.
    async fn process_control_message(
        &self,
        _message: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<bool, Error> {
        Ok(false)
    }

    /// Tick-driven entry point invoked by the runtime's Pacer once per
    /// clock tick. Default no-op — Reactive nodes never override this.
    async fn tick(
        &self,
        _tick: Tick,
        _ctx: &dyn NodeRuntimeContextRead,
        _cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Pacing nature for this node.
    fn pacing_nature(&self) -> PacingNature {
        PacingNature::Reactive
    }

    /// Capability subscription addresses the runtime should auto-bind
    /// at session start.
    fn capability_subscriptions(&self) -> Vec<String> {
        Vec::new()
    }

    /// `(medium, stream_id)` pair for `ClockedToOutboundMedia`.
    fn clocked_media_addr(&self) -> Option<(String, String)> {
        None
    }

    /// Fallback wall-clock rate (Hz) when `clocked_media_addr` is None.
    fn fallback_rate_hz(&self) -> u32 {
        30
    }

    // =========================================================================
    // Capability Resolution Methods (spec 023)
    // =========================================================================

    /// Return media capabilities for this node instance.
    fn media_capabilities(&self) -> Option<MediaCapabilities> {
        None
    }

    /// Return capability behavior for this node.
    fn capability_behavior(&self) -> CapabilityBehavior {
        CapabilityBehavior::Passthrough
    }

    /// Return potential capabilities for RuntimeDiscovered nodes (Phase 1).
    fn potential_capabilities(&self) -> Option<MediaCapabilities> {
        self.media_capabilities()
    }

    /// Return actual capabilities after device init (Phase 2).
    fn actual_capabilities(&self) -> Option<MediaCapabilities> {
        self.media_capabilities()
    }

    /// Configure this node based on upstream capabilities (spec 025).
    fn configure_from_upstream(&self, _upstream_caps: &MediaCapabilities) -> Result<(), Error> {
        Ok(())
    }

    // =========================================================================
    // Runtime Context Methods (node-runtime-context plan, PR 2)
    // =========================================================================

    /// Construct the per-(node, session) state slot. Default returns
    /// `Arc::new(())` — stateless nodes inherit unchanged.
    fn make_session_state(&self, _ctx: &dyn InitializeContextRead) -> Arc<dyn AnySessionState> {
        Arc::new(())
    }

    /// Hook fired when a media capability the node is subscribed to is
    /// published. Default impl writes the value to the per-(node,
    /// session) capability cache via `ctx.insert_capability(...)`.
    async fn on_capability_update(
        &self,
        addr: &str,
        caps: MediaCapabilities,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<(), Error> {
        ctx.insert_capability(addr, caps);
        Ok(())
    }

    /// Type-erased read handles for this node's snapshot output ports,
    /// keyed by output port name.
    fn snapshot_outputs(&self) -> HashMap<String, Arc<dyn SnapshotPort>> {
        HashMap::new()
    }
}

/// Wrapper that makes a `SyncStreamingNode` into a `StreamingNode`.
pub struct SyncNodeWrapper<T: SyncStreamingNode>(pub T);

#[async_trait::async_trait]
impl<T: SyncStreamingNode + 'static> StreamingNode for SyncNodeWrapper<T> {
    fn node_type(&self) -> &str {
        self.0.node_type()
    }

    async fn process_async(
        &self,
        data: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        self.0.process(data)
    }

    async fn process_multi_async(
        &self,
        inputs: HashMap<String, RuntimeData>,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        self.0.process_multi(inputs)
    }

    fn is_multi_input(&self) -> bool {
        self.0.is_multi_input()
    }

    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<usize, Error> {
        let session_id = if ctx.session_id().is_empty() {
            None
        } else {
            Some(ctx.session_id())
        };
        self.0.process_streaming(data, session_id, &mut *callback)
    }
}

/// Wrapper that makes an `AsyncStreamingNode` into a `StreamingNode`.
pub struct AsyncNodeWrapper<T: AsyncStreamingNode>(pub Arc<T>);

#[async_trait::async_trait]
impl<T: AsyncStreamingNode + 'static> StreamingNode for AsyncNodeWrapper<T> {
    fn node_type(&self) -> &str {
        self.0.node_type()
    }

    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        self.0.initialize(ctx).await
    }

    async fn process_async(
        &self,
        data: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        self.0.process(data).await
    }

    async fn process_multi_async(
        &self,
        inputs: HashMap<String, RuntimeData>,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        self.0.process_multi(inputs).await
    }

    fn is_multi_input(&self) -> bool {
        self.0.is_multi_input()
    }

    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<usize, Error> {
        // Route through ctx-aware entry point so nodes that need the
        // runtime context get it.
        self.0
            .process_streaming_with_ctx(data, ctx, move |output_data| callback(output_data))
            .await
    }

    /// Forward control messages. The session router dispatches
    /// `barge_in` aux-port envelopes to this method.
    async fn process_control_message(
        &self,
        message: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<bool, Error> {
        let session_id = if ctx.session_id().is_empty() {
            None
        } else {
            Some(ctx.session_id().to_string())
        };
        self.0.process_control_message(message, session_id).await
    }

    fn pacing_nature(&self) -> PacingNature {
        self.0.pacing_nature()
    }

    fn clocked_media_addr(&self) -> Option<(String, String)> {
        self.0.clocked_media_addr()
    }

    fn fallback_rate_hz(&self) -> u32 {
        self.0.fallback_rate_hz()
    }

    async fn tick(
        &self,
        tick: Tick,
        ctx: &dyn NodeRuntimeContextRead,
        callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        let session_id = if ctx.session_id().is_empty() {
            None
        } else {
            Some(ctx.session_id().to_string())
        };
        self.0.tick(tick, session_id, callback).await
    }
}

/// Factory trait for creating streaming node instances.
pub trait StreamingNodeFactory: Send + Sync {
    /// Create a new streaming node instance.
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error>;

    /// Get the node type this factory creates.
    fn node_type(&self) -> &str;

    /// Check if this factory creates Python-based nodes.
    fn is_python_node(&self) -> bool {
        false
    }

    /// Check if this factory creates multi-output streaming nodes.
    fn is_multi_output_streaming(&self) -> bool {
        false
    }

    /// Get the node schema for type generation.
    fn schema(&self) -> Option<NodeSchema> {
        None
    }

    // =========================================================================
    // Capability Resolution Methods (spec 023)
    // =========================================================================

    /// Return media capabilities for nodes created by this factory.
    fn media_capabilities(&self, _params: &Value) -> Option<MediaCapabilities> {
        None
    }

    /// Return capability behavior for nodes created by this factory.
    fn capability_behavior(&self) -> CapabilityBehavior {
        CapabilityBehavior::Passthrough
    }

    /// Return potential capabilities for RuntimeDiscovered nodes (Phase 1).
    fn potential_capabilities(&self, params: &Value) -> Option<MediaCapabilities> {
        self.media_capabilities(params)
    }

    // =========================================================================
    // Port-kind declarations (pacing-domains spec, Phase 4.3)
    // =========================================================================

    /// Declared kind of each named input port.
    fn input_port_kinds(&self) -> HashMap<String, PortKind> {
        HashMap::new()
    }

    /// Declared kind of each named output port.
    fn output_port_kinds(&self) -> HashMap<String, PortKind> {
        HashMap::new()
    }
}
