//! Node-initiated output for loadable plugins (openspec:
//! add-node-initiated-output, add-source-node-lifecycle).
//!
//! The host hands every node instance an emitter before `initialize`: a
//! handle onto the instance's own output edge that needs no input in
//! progress. What a node pushes into a call's sink stays that input's output;
//! an emitted frame is the node speaking for itself.
//!
//! ```ignore
//! async fn background(&self, ctx: &dyn NodeRuntimeContextRead) -> Result<(), Error> {
//!     let emitter = Emitter::for_call(ctx);
//!     loop {
//!         emitter.emit(RuntimeData::Text("tick".into())).await?;
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//!     }
//! }
//! ```
//!
//! The operations, the attribution type and the non-waiting error are the
//! ones in-tree nodes use (`remotemedia_traits::emit`), so the same node
//! source builds in-tree and as a plugin.
//!
//! On a host that provides no emitter, [`Emitter::of`] returns a detached one
//! and every emit returns an error the plugin can handle.

use abi_stable::std_types::{RResult, RString};
use loadable_node_abi::buffer::{
    NodeEmitterBox, EMIT_AS_DEFAULT, EMIT_AS_ORIGIN, EMIT_AS_PARTICIPANT, EMIT_UNATTRIBUTED,
    TRY_EMIT_FULL, TRY_EMIT_NOT_STARTED, TRY_EMIT_QUEUED,
};
use loadable_node_abi::runtime_buffer;
use remotemedia_traits::runtime_context::{InitializeContextRead, NodeRuntimeContextRead};
use remotemedia_types::{Error, RuntimeData};
use std::sync::Arc;

pub use remotemedia_traits::emit::{Emit, EmitOrigin, Origin, TryEmitError};

fn to_ffi(origin: &EmitOrigin) -> (u8, RString) {
    match origin {
        EmitOrigin::Default => (EMIT_AS_DEFAULT, RString::new()),
        EmitOrigin::Unattributed => (EMIT_UNATTRIBUTED, RString::new()),
        EmitOrigin::Origin(origin) => (EMIT_AS_ORIGIN, RString::from(origin.to_json())),
        EmitOrigin::Participant(id) => (EMIT_AS_PARTICIPANT, RString::from(id.as_str())),
    }
}

/// A node instance's handle onto its own output edge. Cheap to clone; every
/// clone is the same emitter.
#[derive(Clone)]
pub struct Emitter {
    host: Option<Arc<NodeEmitterBox>>,
}

impl Emitter {
    /// The emitter the host bound to this node instance.
    pub(crate) fn bound(host: NodeEmitterBox) -> Self {
        Self {
            host: Some(Arc::new(host)),
        }
    }

    /// An emitter connected to nothing: every emit fails.
    pub fn detached() -> Self {
        Self { host: None }
    }

    /// The emitter of the instance being initialized, or a detached one when
    /// the host provides none.
    pub fn of(ctx: &dyn InitializeContextRead) -> Self {
        ctx.emitter_any()
            .and_then(|emitter| emitter.downcast_ref::<Emitter>())
            .cloned()
            .unwrap_or_else(Self::detached)
    }

    /// The emitter of the instance a per-call method or a background body
    /// runs on, or a detached one when the host provides none.
    pub fn for_call(ctx: &dyn NodeRuntimeContextRead) -> Self {
        ctx.emitter_any()
            .and_then(|emitter| emitter.downcast_ref::<Emitter>())
            .cloned()
            .unwrap_or_else(Self::detached)
    }

    fn missing() -> Error {
        Error::Execution(
            "this host does not provide an emitter; update the runtime, or produce the output from a call"
                .into(),
        )
    }

    /// Emit a frame with the instance's default attribution. Waits while the
    /// emit queue is full.
    pub async fn emit(&self, data: RuntimeData) -> Result<(), Error> {
        self.emit_as(data, EmitOrigin::Default).await
    }

    /// Emit a frame attributed to `origin`: one read from an input
    /// (`ctx.origin()`), or a current participant's id. Waits while the emit
    /// queue is full.
    pub async fn emit_as(&self, data: RuntimeData, origin: impl Into<EmitOrigin>) -> Result<(), Error> {
        let host = self.host.as_deref().ok_or_else(Self::missing)?;
        let (mode, participant) = to_ffi(&origin.into());
        let message =
            runtime_buffer::encode(data).map_err(|error| Error::Execution(error.into_string()))?;
        match host.emit(message, mode, participant).await {
            RResult::ROk(()) => Ok(()),
            RResult::RErr(error) => Err(Error::Execution(error.into_string())),
        }
    }

    /// [`Self::emit`] without waiting: a full queue hands the frame back.
    pub fn try_emit(&self, data: RuntimeData) -> Result<(), TryEmitError> {
        self.try_emit_as(data, EmitOrigin::Default)
    }

    /// [`Self::emit_as`] without waiting: a full queue hands the frame back.
    pub fn try_emit_as(
        &self,
        data: RuntimeData,
        origin: impl Into<EmitOrigin>,
    ) -> Result<(), TryEmitError> {
        let Some(host) = self.host.as_deref() else {
            return Err(TryEmitError::Rejected(data, Self::missing()));
        };
        let (mode, participant) = to_ffi(&origin.into());
        let message = match runtime_buffer::encode(data.clone()) {
            Ok(message) => message,
            Err(error) => {
                return Err(TryEmitError::Rejected(data, Error::Execution(error.into_string())))
            }
        };
        let outcome = host.try_emit(message, mode, participant);
        match outcome.status {
            TRY_EMIT_QUEUED => Ok(()),
            TRY_EMIT_FULL => Err(TryEmitError::Full(data)),
            TRY_EMIT_NOT_STARTED => Err(TryEmitError::NotStarted(data)),
            _ => Err(TryEmitError::Rejected(
                data,
                Error::Execution(outcome.error.into_string()),
            )),
        }
    }

    /// Whether emitting can no longer succeed.
    pub fn is_closed(&self) -> bool {
        self.host.as_deref().map_or(true, |host| host.is_closed())
    }

    /// Resolves when the instance has started (every node of the session is
    /// initialized and, for a session from the warm pool, a client acquired
    /// it), or the emitter is closed. Resolves at once on a detached emitter.
    pub async fn started(&self) {
        if let Some(host) = self.host.as_deref() {
            host.started().await;
        }
    }

    /// Resolves when the emitter is closed: the stop signal for a task that
    /// holds a clone. Resolves at once on a detached emitter.
    pub async fn closed(&self) {
        if let Some(host) = self.host.as_deref() {
            host.closed().await;
        }
    }

    /// Close the emitter for every clone: the node has finished producing.
    pub fn finish(&self) {
        if let Some(host) = self.host.as_deref() {
            host.finish();
        }
    }
}

#[async_trait::async_trait]
impl Emit for Emitter {
    async fn emit_as(&self, data: RuntimeData, origin: EmitOrigin) -> Result<(), Error> {
        Emitter::emit_as(self, data, origin).await
    }
    fn try_emit_as(&self, data: RuntimeData, origin: EmitOrigin) -> Result<(), TryEmitError> {
        Emitter::try_emit_as(self, data, origin)
    }
    async fn started(&self) {
        Emitter::started(self).await
    }
    async fn closed(&self) {
        Emitter::closed(self).await
    }
    fn is_closed(&self) -> bool {
        Emitter::is_closed(self)
    }
    fn finish(&self) {
        Emitter::finish(self)
    }
}
