//! Read-only trait surface over the host's per-call and initialization
//! contexts.
//!
//! The concrete `NodeRuntimeContext` and `InitializeContext` STRUCTS live in
//! `remotemedia-core` because they carry heavy host machinery
//! (`Arc<SessionControl>`, `Arc<DashMap<String, MediaCapabilities>>`,
//! `Arc<CancelGate>`, …). Plugin authors and the FFI adapter (Task A7)
//! cannot pull those deps in without violating the dep-isolation
//! guarantees of the plugin SDK.
//!
//! These traits expose just the **read accessors** plugin code (and the
//! adapter that wraps it) needs from the host context, in a `&dyn`
//! shape that's dyn-compatible. Generic accessors (`snapshot::<T>`,
//! `state::<S>`) are split into:
//!
//! - An erased trait method (`snapshot_any` / `state_any` /
//!   `try_state_any`) that returns `Arc<dyn Any + Send + Sync>` and
//!   can live on a `dyn`-compatible trait.
//! - A free helper at module scope (`snapshot::<T>`, `state::<S>`,
//!   `try_state::<S>`) that downcasts the erased value back to the
//!   caller's expected type.
//!
//! See the R1 fallback note in `crates/traits/src/lib.rs` for the
//! design rationale.

use std::any::Any;
use std::sync::Arc;

use crate::capabilities::MediaCapabilities;
use crate::ports::TimestampedSnapshot;

// ─── NodeRuntimeContextRead ───────────────────────────────────────────

/// Read-only view of the host's `NodeRuntimeContext`, exposed as a
/// `dyn`-compatible trait.
///
/// Implementations forward each method to the underlying host struct's
/// existing accessor. The erased `*_any` methods power the typed free
/// helpers in this module — plugin code is expected to call those rather
/// than handle `Arc<dyn Any>` directly.
pub trait NodeRuntimeContextRead: Send + Sync {
    /// Pipeline session this call belongs to.
    fn session_id(&self) -> &str;

    /// Node id (from manifest).
    fn node_id(&self) -> &str;

    /// Read the latest published capability for `addr`, or `None` if no
    /// publisher has emitted yet.
    fn capability(&self, addr: &str) -> Option<MediaCapabilities>;

    /// Insert / overwrite a capability in the per-(node, session) cache.
    ///
    /// Used by the default `on_capability_update` impl on `StreamingNode`
    /// so the cache stays in sync with the bus without having to reach
    /// for the host's concrete `Arc<DashMap<...>>` field. Plugin-side
    /// overrides may also call this to keep the cache fresh after doing
    /// their own eager work.
    fn insert_capability(&self, addr: &str, caps: MediaCapabilities);

    /// Erased read of the latest snapshot on the named input port.
    ///
    /// Returns `None` when the port name is unknown or the producer has
    /// not yet published. The contained `Arc` wraps the host's
    /// `TimestampedSnapshot<T>` for the producer's `T`; downcast via
    /// the free [`snapshot`] helper.
    fn snapshot_any(&self, port: &str) -> Option<Arc<dyn Any + Send + Sync>>;

    /// Erased read of the per-(node, session) state slot.
    ///
    /// Returns `None` only when the host failed to construct a state
    /// for this binding (e.g. test harness paths that bypass
    /// `make_session_state`). The downcast via [`state`] / [`try_state`]
    /// returns `None` if the requested type does not match.
    fn state_any(&self) -> Option<Arc<dyn Any + Send + Sync>>;

    /// Same as [`Self::state_any`] but returns `None` instead of
    /// panicking when the state slot is missing. The free [`try_state`]
    /// helper is the typed counterpart.
    fn try_state_any(&self) -> Option<Arc<dyn Any + Send + Sync>>;

    /// Type-erased self handle for in-tree code that needs to downcast
    /// to the host's concrete `NodeRuntimeContext` to access heavy
    /// fields not exposed on this read trait (`cancel_gate`, `control`).
    ///
    /// Plugin-side code should not rely on this — it's a controlled
    /// escape hatch for in-tree wrappers (e.g. the chat-backend's
    /// barge-in cancel gate forwarding) that historically held the
    /// concrete struct directly. The default impl is provided so
    /// existing `NodeRuntimeContextRead` implementors don't have to
    /// add it; concrete struct impls override to return `self`.
    fn as_any(&self) -> &dyn Any {
        // Default returns a sentinel that won't downcast to the host
        // struct. In-tree impls override.
        &()
    }
}

/// Read the latest snapshot on `port`, downcast to the consumer's
/// expected type.
///
/// Atomic load — never blocks. Returns `None` when:
///   - the port name isn't registered for this node, OR
///   - the producer hasn't published anything yet, OR
///   - the consumer's `T` doesn't match the producer's published type.
pub fn snapshot<T>(ctx: &dyn NodeRuntimeContextRead, port: &str) -> Option<TimestampedSnapshot<T>>
where
    T: Clone + Send + Sync + 'static,
{
    let any = ctx.snapshot_any(port)?;
    let arc: Arc<TimestampedSnapshot<T>> = any.downcast().ok()?;
    Some((*arc).clone())
}

/// Downcast the per-session state to the node's expected type.
///
/// # Panics
///
/// Panics when the stored state's runtime type does not match `S` or
/// when the host failed to populate a state slot. Mirrors the
/// host-side `NodeRuntimeContext::state::<S>` panic policy.
pub fn state<S>(ctx: &dyn NodeRuntimeContextRead) -> Arc<S>
where
    S: Send + Sync + 'static,
{
    let any = ctx
        .state_any()
        .expect("state called without a registered session state");
    any.downcast::<S>().unwrap_or_else(|_| {
        panic!(
            "remotemedia_traits::runtime_context::state::<{ty}>(): wrong type for node {node} session {session}",
            ty = std::any::type_name::<S>(),
            node = ctx.node_id(),
            session = ctx.session_id(),
        )
    })
}

/// Like [`state`] but returns `None` on type mismatch / missing slot
/// instead of panicking.
pub fn try_state<S>(ctx: &dyn NodeRuntimeContextRead) -> Option<Arc<S>>
where
    S: Send + Sync + 'static,
{
    ctx.try_state_any().and_then(|a| a.downcast::<S>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::{OutputPort, SnapshotPort};
    use crate::streaming::AnySessionState;
    use std::collections::HashMap;

    /// Stand-in `NodeRuntimeContextRead` impl that owns just enough
    /// state to drive the free helpers. Mirrors what the host's
    /// concrete `NodeRuntimeContext` implements but without the heavy
    /// Arc<DashMap<...>> / Arc<CancelGate> deps.
    struct StubCtx {
        session_id: String,
        node_id: String,
        snapshots: HashMap<String, Arc<dyn SnapshotPort>>,
        state: Arc<dyn AnySessionState>,
    }

    impl NodeRuntimeContextRead for StubCtx {
        fn session_id(&self) -> &str {
            &self.session_id
        }
        fn node_id(&self) -> &str {
            &self.node_id
        }
        fn capability(&self, _addr: &str) -> Option<MediaCapabilities> {
            None
        }
        fn insert_capability(&self, _addr: &str, _caps: MediaCapabilities) {
            // Stub — no cache to update.
        }
        fn snapshot_any(&self, port: &str) -> Option<Arc<dyn Any + Send + Sync>> {
            self.snapshots.get(port)?.snapshot_any()
        }
        fn state_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
            Some(AnySessionState::into_any_arc(Arc::clone(&self.state)))
        }
        fn try_state_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
            Some(AnySessionState::into_any_arc(Arc::clone(&self.state)))
        }
    }

    /// `state::<S>` round-trips through the erased trait method and
    /// returns the same allocation the stub stored.
    #[test]
    fn state_helper_round_trips_through_trait() {
        struct MyState {
            value: u32,
        }
        let original = Arc::new(MyState { value: 42 });
        let ctx = StubCtx {
            session_id: "sess-1".into(),
            node_id: "node-x".into(),
            snapshots: HashMap::new(),
            state: original.clone(),
        };
        let recovered: Arc<MyState> = state(&ctx);
        assert_eq!(recovered.value, 42);
        assert!(Arc::ptr_eq(&original, &recovered));
    }

    /// `try_state::<S>` returns `None` on type mismatch instead of
    /// panicking.
    #[test]
    fn try_state_returns_none_on_wrong_type() {
        struct A;
        struct B;
        let ctx = StubCtx {
            session_id: "sess-1".into(),
            node_id: "node-x".into(),
            snapshots: HashMap::new(),
            state: Arc::new(A),
        };
        assert!(try_state::<B>(&ctx).is_none());
    }

    /// `state::<S>` panics with a message naming the requested type
    /// when the stored state's runtime type does not match.
    #[test]
    #[should_panic(expected = "wrong type for node node-x session sess-1")]
    fn state_panics_on_wrong_type() {
        struct A;
        struct B;
        let ctx = StubCtx {
            session_id: "sess-1".into(),
            node_id: "node-x".into(),
            snapshots: HashMap::new(),
            state: Arc::new(A),
        };
        let _ = state::<B>(&ctx);
    }

    /// `snapshot::<T>` round-trips through the erased trait method and
    /// returns the latest published value.
    #[test]
    fn snapshot_helper_returns_latest_value() {
        let port: OutputPort<u32> = OutputPort::empty();
        port.publish(7, 1000);
        let mut snapshots: HashMap<String, Arc<dyn SnapshotPort>> = HashMap::new();
        snapshots.insert("weights".into(), Arc::new(port.input()));

        let ctx = StubCtx {
            session_id: "sess-1".into(),
            node_id: "node-x".into(),
            snapshots,
            state: Arc::new(()),
        };
        let snap = snapshot::<u32>(&ctx, "weights").expect("populated");
        assert_eq!(snap.value, 7);
        assert_eq!(snap.pts_us, 1000);
    }

    /// Wrong-type snapshot reads return `None` (silent — same policy
    /// as the host-side `ctx.snapshot::<T>` accessor).
    #[test]
    fn snapshot_helper_wrong_type_returns_none() {
        let port: OutputPort<u32> = OutputPort::empty();
        port.publish(7, 1000);
        let mut snapshots: HashMap<String, Arc<dyn SnapshotPort>> = HashMap::new();
        snapshots.insert("weights".into(), Arc::new(port.input()));

        let ctx = StubCtx {
            session_id: "sess-1".into(),
            node_id: "node-x".into(),
            snapshots,
            state: Arc::new(()),
        };
        assert!(snapshot::<String>(&ctx, "weights").is_none());
    }
}

// ─── InitializeContextRead ────────────────────────────────────────────

/// Read-only view of the host's `InitializeContext`, exposed as a
/// `dyn`-compatible trait.
///
/// Mirrors the read surface of the concrete struct in
/// `remotemedia-core`: `session_id`, `node_id`, and `emit_progress` so
/// plugin code can report loading progress through the host's control
/// bus without depending on `SessionControl` directly.
pub trait InitializeContextRead: Send + Sync {
    /// Session ID for this pipeline run.
    fn session_id(&self) -> &str;

    /// Node ID (from the manifest).
    fn node_id(&self) -> &str;

    /// Emit a progress event on the session control bus.
    ///
    /// No-op when the host context has no bus attached (unit tests,
    /// gRPC unary mode). Plugins call this to report loading progress
    /// while initializing models or other slow resources; the frontend
    /// surfaces the messages through the `__system__` event channel.
    fn emit_progress(&self, status: &str, message: &str);

    /// Type-erased self handle for in-tree code that needs to downcast
    /// to the host's concrete `InitializeContext` (e.g. to grab the
    /// optional `Arc<SessionControl>` for forwarding to spawned
    /// executors). Plugin-side code should not rely on this — use
    /// `emit_progress` for the supported control-bus surface.
    fn as_any(&self) -> &dyn Any {
        &()
    }
}
