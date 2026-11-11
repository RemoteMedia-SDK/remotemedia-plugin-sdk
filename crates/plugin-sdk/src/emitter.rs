//! Node-initiated output for loadable plugins (openspec:
//! add-node-initiated-output).
//!
//! The host hands every node instance an emitter before `initialize`: a
//! handle onto the instance's own output edge that needs no input in
//! progress. What a node pushes into a call's sink stays that input's output;
//! an emitted frame is the node speaking for itself.
//!
//! ```ignore
//! async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<(), Error> {
//!     let emitter = Emitter::of(ctx);
//!     tokio::spawn(async move {
//!         while emitter.emit(RuntimeData::Text("tick".into())).await.is_ok() {
//!             tokio::time::sleep(Duration::from_secs(1)).await;
//!         }
//!     });
//!     Ok(())
//! }
//! ```
//!
//! On a host that provides no emitter, [`Emitter::of`] returns a detached one
//! and every emit returns an error the plugin can handle.

use abi_stable::std_types::{RResult, RString};
use loadable_node_abi::buffer::{NodeEmitterBox, EMIT_AS_DEFAULT, EMIT_AS_PARTICIPANT, EMIT_UNATTRIBUTED};
use loadable_node_abi::runtime_buffer;
use remotemedia_traits::runtime_context::InitializeContextRead;
use remotemedia_types::{Error, RuntimeData};
use std::sync::Arc;

/// Who an emitted frame is attributed to.
#[derive(Debug, Clone, Default)]
pub enum EmitOrigin {
    /// The instance's default: its participant for a per-participant
    /// instance, nobody for a shared one.
    #[default]
    Default,
    /// Nobody, also on a per-participant instance.
    Unattributed,
    /// A participant currently in the session, by `participant.id`.
    Participant(String),
}

impl From<&str> for EmitOrigin {
    fn from(participant_id: &str) -> Self {
        Self::Participant(participant_id.to_string())
    }
}

impl From<String> for EmitOrigin {
    fn from(participant_id: String) -> Self {
        Self::Participant(participant_id)
    }
}

impl EmitOrigin {
    fn to_ffi(&self) -> (u8, RString) {
        match self {
            Self::Default => (EMIT_AS_DEFAULT, RString::new()),
            Self::Unattributed => (EMIT_UNATTRIBUTED, RString::new()),
            Self::Participant(id) => (EMIT_AS_PARTICIPANT, RString::from(id.as_str())),
        }
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

    fn host(&self) -> Result<&NodeEmitterBox, Error> {
        self.host.as_deref().ok_or_else(|| {
            Error::Execution(
                "this host does not provide an emitter; update the runtime, or produce the output from a call"
                    .into(),
            )
        })
    }

    /// Emit a frame with the instance's default attribution. Waits while the
    /// emit queue is full.
    pub async fn emit(&self, data: RuntimeData) -> Result<(), Error> {
        self.emit_as(data, EmitOrigin::Default).await
    }

    /// Emit a frame attributed to `origin`. Waits while the emit queue is full.
    pub async fn emit_as(&self, data: RuntimeData, origin: impl Into<EmitOrigin>) -> Result<(), Error> {
        let host = self.host()?;
        let (mode, participant) = origin.into().to_ffi();
        let message =
            runtime_buffer::encode(data).map_err(|error| Error::Execution(error.into_string()))?;
        match host.emit(message, mode, participant).await {
            RResult::ROk(()) => Ok(()),
            RResult::RErr(error) => Err(Error::Execution(error.into_string())),
        }
    }

    /// [`Self::emit`] without waiting. `Ok(false)` when the emit queue is
    /// full and the frame was not queued.
    pub fn try_emit(&self, data: RuntimeData) -> Result<bool, Error> {
        self.try_emit_as(data, EmitOrigin::Default)
    }

    /// [`Self::emit_as`] without waiting. `Ok(false)` when the emit queue is
    /// full and the frame was not queued.
    pub fn try_emit_as(&self, data: RuntimeData, origin: impl Into<EmitOrigin>) -> Result<bool, Error> {
        let host = self.host()?;
        let (mode, participant) = origin.into().to_ffi();
        let message =
            runtime_buffer::encode(data).map_err(|error| Error::Execution(error.into_string()))?;
        match host.try_emit(message, mode, participant) {
            RResult::ROk(queued) => Ok(queued),
            RResult::RErr(error) => Err(Error::Execution(error.into_string())),
        }
    }

    /// Whether emitting can no longer succeed.
    pub fn is_closed(&self) -> bool {
        self.host.as_deref().map_or(true, |host| host.is_closed())
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
