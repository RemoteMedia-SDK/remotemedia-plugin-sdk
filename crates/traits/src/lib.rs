//! Trait surface that loadable-node plugins implement against.
//!
//! Depends only on `remotemedia-types` (the wire-format crate) plus a
//! handful of leaf utility crates (serde, async-trait, thiserror,
//! arc-swap, schemars, inventory). Plugins that link this crate inherit
//! a small, stable dep tree.
//!
//! # What lives here (Task A4 phase 1)
//!
//! Plain leaf types lifted out of `remotemedia-core` so plugins can
//! construct/return them without depending on the heavy host crate:
//!
//! - **Schema**: `NodeSchema`, `NodeCapabilitiesSchema`,
//!   `RuntimeDataType`, `LatencyClass`, `ParameterType`, `NodeParameter`,
//!   `HasNodeSchema`, `NodeConfigSchema`, `RegisteredNodeConfig`,
//!   `NodeSchemaRegistry`, `collect_registered_configs`,
//!   `generate_typescript`.
//!
//! - **Capabilities**: `MediaCapabilities`, `MediaConstraints`,
//!   `AudioConstraints`, `VideoConstraints`, `TensorConstraints`,
//!   `TextConstraints`, `FileConstraints`, `JsonConstraints`,
//!   `ConstraintValue<T>`, `AudioSampleFormat`, `PixelFormat`
//!   (capability-side; aliased as `CapabilityPixelFormat` to avoid
//!   collision with the wire-format pixel format in
//!   `remotemedia_types`), `TensorDataType`, `CapabilityBehavior`.
//!
//! - **Snapshot ports**: `OutputPort<T>`, `InputPort<T>`, `SnapshotPort`,
//!   `TimestampedSnapshot<T>`, `PortKind`.
//!
//! - **Streaming primitives**: `Tick`, `PacingNature`, `NodeStatus`,
//!   `AnySessionState` (marker trait + blanket impl).
//!
//! - **Read-only context surface (R1 fallback)**:
//!   `NodeRuntimeContextRead`, `InitializeContextRead`, plus the typed
//!   free helpers `runtime_context::{snapshot, state, try_state}`.
//!
//! `remotemedia-core` re-exports each of the above from its
//! corresponding module path so existing
//! `use remotemedia_core::{nodes::*, capabilities::*}` call sites keep
//! working unchanged.
//!
//! # The R1 fallback path
//!
//! The concrete `NodeRuntimeContext` and `InitializeContext` STRUCTS
//! continue to live in `remotemedia-core` because they carry heavy
//! host machinery (`Arc<SessionControl>`, `Arc<DashMap<...>>`,
//! `Arc<CancelGate>`) that has no business in this crate. Plugin code
//! that wants to read `session_id`, `node_id`, capabilities, snapshot
//! ports, or per-session state goes through the
//! `NodeRuntimeContextRead` / `InitializeContextRead` traits + the
//! typed free helpers in `runtime_context` instead. The host's
//! concrete structs implement the read traits so a `&NodeRuntimeContext`
//! coerces transparently to `&dyn NodeRuntimeContextRead` at call
//! sites.
//!
//! Generic accessors that would break dyn-compat (`state::<S>`,
//! `snapshot::<T>`) are split into:
//!   - an erased `*_any` trait method returning
//!     `Option<Arc<dyn Any + Send + Sync>>`, and
//!   - a free helper at module scope that downcasts back to the
//!     caller's `T`/`S`.
//!
//! See the `runtime_context` module for the trait definitions, the
//! free helpers, and the rationale for each accessor that ended up on
//! the read surface.
//!
//! # What is NOT here (yet)
//!
//! The streaming-trait declarations themselves —
//! `StreamingNode` / `SyncStreamingNode` / `AsyncStreamingNode` /
//! `StreamingNodeFactory` / `SyncNodeWrapper` / `AsyncNodeWrapper` —
//! still live in `remotemedia-core`. Their method signatures refer to
//! `crate::Error` (= `remotemedia_core::Error`) which carries
//! host-only error variants (`IpcError`, `Validation(Vec<…>)`, …) that
//! cannot be lifted into `remotemedia-types` without a separate
//! cross-cutting refactor. Lifting the trait declarations themselves
//! is gated on resolving that error-type story (and on the
//! `core-derive` macro switching to `remotemedia_traits` paths in Task
//! A5). The R1 fallback shipped here is the load-bearing prerequisite
//! for that move — once the streaming traits do migrate, their method
//! signatures will switch to `&dyn NodeRuntimeContextRead` /
//! `&dyn InitializeContextRead` and pick up the helpers below.

#![warn(clippy::all)]

pub mod capabilities;
pub mod llm;
pub mod ports;
pub mod runtime_context;
pub mod schema;
pub mod streaming;

pub use capabilities::{
    AudioConstraints, AudioSampleFormat, CapabilityBehavior, CapabilityPixelFormat,
    ConstraintValue, FileConstraints, JsonConstraints, MediaCapabilities, MediaConstraints,
    PixelFormat, TensorConstraints, TensorDataType, TextConstraints, VideoConstraints,
};

pub use llm::{InterruptableBackend, StatefulConversationBackend, VoiceActivityDetectorBackend};

pub use ports::{InputPort, OutputPort, PortKind, SnapshotPort, TimestampedSnapshot};

pub use runtime_context::{InitializeContextRead, NodeRuntimeContextRead};

pub use schema::{
    collect_registered_configs, generate_typescript, HasNodeSchema, LatencyClass,
    NodeCapabilitiesSchema, NodeConfigSchema, NodeParameter, NodeSchema, NodeSchemaRegistry,
    ParameterType, RegisteredNodeConfig, RuntimeDataType,
};

pub use streaming::{
    AnySessionState, AsyncNodeWrapper, AsyncStreamingNode, NodeStatus, PacingNature, StreamingNode,
    StreamingNodeFactory, SyncNodeWrapper, SyncStreamingNode, Tick,
};
