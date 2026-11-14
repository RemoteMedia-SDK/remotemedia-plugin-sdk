//! The emitter surface shared by in-tree nodes and loadable plugins
//! (openspec: add-node-initiated-output, add-source-node-lifecycle).
//!
//! A node instance's emitter puts data on the instance's own output edge
//! with no input in progress. The host's emitter and the plugin SDK's
//! emitter are different types behind the same operations ([`Emit`]), the
//! same attribution type ([`EmitOrigin`]) and the same non-waiting error
//! ([`TryEmitError`]), so node code compiles unchanged in either build.

use std::collections::HashMap;
use std::sync::Arc;

use remotemedia_types::{Error, RuntimeData};
use serde::{Deserialize, Serialize};

/// Metadata key of a participant's id.
pub const PARTICIPANT_ID: &str = "participant.id";
/// Metadata key marking a participant lifecycle packet; never part of an
/// [`Origin`].
pub const PARTICIPANT_LIFECYCLE: &str = "participant.lifecycle";

/// Who produced a frame: the sender's canonical `participant.*` fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Origin {
    id: String,
    fields: HashMap<String, String>,
}

impl Origin {
    /// The identity in `metadata`, or `None` without a `participant.id`.
    /// Only `participant.*` keys are kept.
    pub fn from_metadata(metadata: &HashMap<String, String>) -> Option<Self> {
        let id = metadata.get(PARTICIPANT_ID)?.clone();
        let fields = metadata
            .iter()
            .filter(|(key, _)| {
                key.starts_with("participant.") && key.as_str() != PARTICIPANT_LIFECYCLE
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Some(Self { id, fields })
    }

    /// `participant.id`.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// All carried `participant.*` fields.
    pub fn fields(&self) -> &HashMap<String, String> {
        &self.fields
    }

    /// The fields as a JSON object: how an origin crosses a process or
    /// plugin boundary.
    pub fn to_json(&self) -> String {
        serde_json::to_string(&self.fields).unwrap_or_else(|_| "{}".into())
    }

    /// The inverse of [`Self::to_json`]; `None` without a `participant.id`.
    pub fn from_json(json: &str) -> Option<Self> {
        let fields: HashMap<String, String> = serde_json::from_str(json).ok()?;
        Self::from_metadata(&fields)
    }
}

/// Who an emitted frame is attributed to.
#[derive(Debug, Clone, Default)]
pub enum EmitOrigin {
    /// The instance's default: its participant for a per-participant
    /// instance, nobody for a shared one.
    #[default]
    Default,
    /// Nobody, also on a per-participant instance.
    Unattributed,
    /// An origin the node read from an input (`ctx.origin()`).
    Origin(Arc<Origin>),
    /// A participant currently in the session, by `participant.id`.
    Participant(String),
}

impl From<Arc<Origin>> for EmitOrigin {
    fn from(origin: Arc<Origin>) -> Self {
        Self::Origin(origin)
    }
}

/// What `ctx.origin()` returns: `None` is an unattributed input.
impl From<Option<Arc<Origin>>> for EmitOrigin {
    fn from(origin: Option<Arc<Origin>>) -> Self {
        origin.map_or(Self::Unattributed, Self::Origin)
    }
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

/// Why a non-waiting emit did not queue its frame. The frame is handed back.
#[derive(Debug)]
pub enum TryEmitError {
    /// The emit queue is full; a waiting emit would wait.
    Full(RuntimeData),
    /// The instance has not started yet.
    NotStarted(RuntimeData),
    /// The frame cannot be emitted: the instance stopped, or the origin
    /// names an unknown participant.
    Rejected(RuntimeData, Error),
}

impl TryEmitError {
    /// The frame that was not queued.
    pub fn into_data(self) -> RuntimeData {
        match self {
            Self::Full(data) | Self::NotStarted(data) | Self::Rejected(data, _) => data,
        }
    }
}

impl std::fmt::Display for TryEmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full(_) => f.write_str("emit queue is full"),
            Self::NotStarted(_) => f.write_str("node instance has not started"),
            Self::Rejected(_, error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for TryEmitError {}

/// The operations of a node instance's emitter. Implemented by the host's
/// `Emitter` and by the plugin SDK's `Emitter`; both also offer them as
/// inherent methods, so most code needs no import.
#[async_trait::async_trait]
pub trait Emit: Send + Sync {
    /// Emit a frame attributed to `origin`. Waits while the emit queue is
    /// full and until the instance has started.
    async fn emit_as(&self, data: RuntimeData, origin: EmitOrigin) -> Result<(), Error>;

    /// [`Self::emit_as`] without waiting.
    fn try_emit_as(&self, data: RuntimeData, origin: EmitOrigin) -> Result<(), TryEmitError>;

    /// Resolves when the instance has started (or the emitter is closed).
    async fn started(&self);

    /// Resolves when the emitter is closed.
    async fn closed(&self);

    /// Whether emitting can no longer succeed.
    fn is_closed(&self) -> bool;

    /// Close the emitter: the node has finished producing.
    fn finish(&self);

    /// Emit a frame with the instance's default attribution.
    async fn emit(&self, data: RuntimeData) -> Result<(), Error> {
        self.emit_as(data, EmitOrigin::Default).await
    }

    /// [`Self::emit`] without waiting.
    fn try_emit(&self, data: RuntimeData) -> Result<(), TryEmitError> {
        self.try_emit_as(data, EmitOrigin::Default)
    }
}

/// What a plugin factory declares about a node before creating it
/// (`FfiNodeFactory::describe`), and what a created node declares about its
/// instance (in its `pacing()` JSON). Absent fields declare nothing.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeDescription {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub self_driven: Option<bool>,
    /// The node has a background body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background: Option<bool>,
    /// `"shared"` or `"per_participant"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_behavior: Option<crate::capabilities::CapabilityBehavior>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_capabilities: Option<crate::capabilities::MediaCapabilities>,
}

impl NodeDescription {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| "{}".into())
    }

    pub fn from_json(json: &str) -> Result<Self, Error> {
        serde_json::from_str(json)
            .map_err(|error| Error::Execution(format!("invalid node description: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_origin_survives_json() {
        let metadata = HashMap::from([
            (PARTICIPANT_ID.to_string(), "alice".to_string()),
            ("participant.role".to_string(), "user".to_string()),
            ("request_id".to_string(), "dropped".to_string()),
        ]);
        let origin = Origin::from_metadata(&metadata).unwrap();
        assert_eq!(origin.fields().len(), 2);
        assert_eq!(Origin::from_json(&origin.to_json()).unwrap(), origin);
        assert!(Origin::from_json("{}").is_none());
    }

    #[test]
    fn a_description_omits_what_it_does_not_declare() {
        assert_eq!(NodeDescription::default().to_json(), "{}");
        let description = NodeDescription {
            self_driven: Some(true),
            ..Default::default()
        };
        assert_eq!(NodeDescription::from_json(&description.to_json()).unwrap(), description);
        assert_eq!(NodeDescription::from_json("{}").unwrap(), NodeDescription::default());
    }
}
