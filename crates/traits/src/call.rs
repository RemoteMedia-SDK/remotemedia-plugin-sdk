//! Node-initiated calls to client-hosted endpoints (`add-agent-harness-integration`).
//!
//! A node calls a method on an endpoint hosted by an attached control client
//! (an agent harness bridge, Hermes, a service) with one convention that works
//! in every node runtime — in-tree Rust, multiprocess Python, loadable Rust and
//! Python plugins:
//!
//! * **Request** — the node emits an aux envelope on its normal output:
//!   `{"__aux_port__": "__call__", "payload": <CallRequestEnvelope>}`. The host
//!   router converts it into an endpoint call before fanout and never forwards
//!   it downstream.
//! * **Reply** — each reply chunk is delivered by invoking the reserved typed-RPC
//!   method [`CALL_REPLY_METHOD`] on the calling node, through its dedicated RPC
//!   lane (independent of media processing). Args are `[<CallReplyEnvelope>]`.
//! * **Opt-in** — a node opts in by listing [`CALL_REPLY_METHOD`] in its
//!   `rpc_methods()`; see [`call_reply_method`].
//!
//! Requests and replies reuse the typed-RPC shapes (`corr_id`, `args`,
//! `kwargs`; reply `kind` = `value` | `end` | `error`).

use crate::rpc::{RpcMethod, RpcMode};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Aux port carrying node-initiated call requests (and cancellations).
pub const CALL_AUX_PORT: &str = "__call__";

/// Reserved RPC method through which the host delivers reply chunks.
pub const CALL_REPLY_METHOD: &str = "__call_reply__";

/// The [`RpcMethod`] a node lists in `rpc_methods()` to opt into endpoint calls.
pub fn call_reply_method() -> RpcMethod {
    RpcMethod::new(CALL_REPLY_METHOD, RpcMode::FireAndForget)
}

/// Payload of a `__call__` aux envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallRequestEnvelope {
    /// Endpoint id, e.g. `"harness"`.
    pub endpoint: String,
    /// Method on the endpoint, e.g. `"decide"`.
    pub method: String,
    /// Caller-chosen correlation id, unique among the caller's in-flight calls.
    pub corr_id: String,
    #[serde(default = "default_mode")]
    pub mode: RpcMode,
    #[serde(default)]
    pub args: Vec<Value>,
    #[serde(default)]
    pub kwargs: Map<String, Value>,
    /// Deadline relative to emission, in milliseconds. Required for every call.
    #[serde(default = "default_deadline_ms")]
    pub deadline_ms: u64,
    /// Wait up to this long for a host to register when none is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_ms: Option<u64>,
    /// Advisory route (`fast` | `contextual` | `agentic`) for agent hosts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_hint: Option<String>,
    /// Advisory adapter name for agent hosts (honoured in debug mode only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter_hint: Option<String>,
    /// `true` cancels the in-flight call with the same `corr_id`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cancel: bool,
}

fn default_mode() -> RpcMode {
    RpcMode::Reply
}

/// Default deadline when a caller omits one (30 s, matching typed-RPC replies).
pub const DEFAULT_CALL_DEADLINE_MS: u64 = 30_000;

fn default_deadline_ms() -> u64 {
    DEFAULT_CALL_DEADLINE_MS
}

impl CallRequestEnvelope {
    pub fn new(
        endpoint: impl Into<String>,
        method: impl Into<String>,
        corr_id: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            method: method.into(),
            corr_id: corr_id.into(),
            mode: RpcMode::Reply,
            args: Vec::new(),
            kwargs: Map::new(),
            deadline_ms: DEFAULT_CALL_DEADLINE_MS,
            hold_ms: None,
            route_hint: None,
            adapter_hint: None,
            cancel: false,
        }
    }

    /// A cancellation for an in-flight call.
    pub fn cancellation(
        endpoint: impl Into<String>,
        method: impl Into<String>,
        corr_id: impl Into<String>,
    ) -> Self {
        Self {
            cancel: true,
            ..Self::new(endpoint, method, corr_id)
        }
    }

    /// Wrap as the `__call__` aux envelope JSON a node emits on its output.
    pub fn to_aux_json(&self) -> Value {
        serde_json::json!({
            "__aux_port__": CALL_AUX_PORT,
            "payload": self,
        })
    }

    /// Parse a `__call__` aux envelope (`{"__aux_port__":"__call__","payload":{..}}`).
    /// Returns `None` when `value` is not a call envelope.
    pub fn from_aux_json(value: &Value) -> Option<Result<Self, serde_json::Error>> {
        let obj = value.as_object()?;
        if obj.get("__aux_port__")?.as_str()? != CALL_AUX_PORT {
            return None;
        }
        let payload = obj.get("payload").cloned().unwrap_or(Value::Null);
        Some(serde_json::from_value(payload))
    }
}

/// Machine-readable failure kinds for endpoint calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallErrorKind {
    /// No host is registered for the endpoint.
    NoResponder,
    /// The host went away (unregistered, taken over, transport closed).
    ResponderGone,
    /// The call's deadline expired.
    Timeout,
    /// The endpoint's bounded queue is full.
    Overloaded,
    /// The call was cancelled (by the caller, barge-in, shutdown).
    Cancelled,
    /// The host is attached but cannot serve the call right now.
    Unavailable,
    /// The host or runtime does not support endpoint calls for this node.
    Unsupported,
    /// The endpoint or method does not exist.
    NotFound,
    /// The host answered with an application error.
    Error,
}

impl CallErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NoResponder => "no_responder",
            Self::ResponderGone => "responder_gone",
            Self::Timeout => "timeout",
            Self::Overloaded => "overloaded",
            Self::Cancelled => "cancelled",
            Self::Unavailable => "unavailable",
            Self::Unsupported => "unsupported",
            Self::NotFound => "not_found",
            Self::Error => "error",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "no_responder" => Self::NoResponder,
            "responder_gone" => Self::ResponderGone,
            "timeout" => Self::Timeout,
            "overloaded" => Self::Overloaded,
            "cancelled" => Self::Cancelled,
            "unavailable" => Self::Unavailable,
            "unsupported" => Self::Unsupported,
            "not_found" => Self::NotFound,
            "error" => Self::Error,
            _ => return None,
        })
    }
}

impl std::fmt::Display for CallErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One reply chunk delivered to the caller via [`CALL_REPLY_METHOD`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum CallReplyEvent {
    Value {
        data: Value,
    },
    End,
    Error {
        error: CallErrorKind,
        #[serde(default)]
        message: String,
    },
}

/// Argument of a `__call_reply__` invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallReplyEnvelope {
    pub corr_id: String,
    #[serde(flatten)]
    pub event: CallReplyEvent,
    /// Metadata from agent hosts (adapter, route used, escalated) when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
}

impl CallReplyEnvelope {
    pub fn value(corr_id: impl Into<String>, data: Value) -> Self {
        Self {
            corr_id: corr_id.into(),
            event: CallReplyEvent::Value { data },
            meta: None,
        }
    }
    pub fn end(corr_id: impl Into<String>) -> Self {
        Self {
            corr_id: corr_id.into(),
            event: CallReplyEvent::End,
            meta: None,
        }
    }
    pub fn error(
        corr_id: impl Into<String>,
        error: CallErrorKind,
        message: impl Into<String>,
    ) -> Self {
        Self {
            corr_id: corr_id.into(),
            event: CallReplyEvent::Error {
                error,
                message: message.into(),
            },
            meta: None,
        }
    }
    /// `true` for the last chunk of a call (`end` or `error`).
    pub fn is_terminal(&self) -> bool {
        !matches!(self.event, CallReplyEvent::Value { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trips_through_aux_envelope() {
        let mut req = CallRequestEnvelope::new("harness", "decide", "brain:1");
        req.kwargs.insert("utterance".into(), "book a table".into());
        req.route_hint = Some("fast".into());
        let aux = req.to_aux_json();
        assert_eq!(aux["__aux_port__"], CALL_AUX_PORT);
        let back = CallRequestEnvelope::from_aux_json(&aux).unwrap().unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn non_call_envelopes_are_ignored() {
        let other = serde_json::json!({"__aux_port__": "context", "payload": {"text": "x"}});
        assert!(CallRequestEnvelope::from_aux_json(&other).is_none());
        assert!(CallRequestEnvelope::from_aux_json(&serde_json::json!("text")).is_none());
    }

    #[test]
    fn defaults_apply_when_fields_omitted() {
        let aux = serde_json::json!({"__aux_port__": "__call__",
            "payload": {"endpoint": "harness", "method": "m", "corr_id": "c"}});
        let req = CallRequestEnvelope::from_aux_json(&aux).unwrap().unwrap();
        assert_eq!(req.mode, RpcMode::Reply);
        assert_eq!(req.deadline_ms, DEFAULT_CALL_DEADLINE_MS);
        assert!(!req.cancel);
    }

    #[test]
    fn reply_wire_shape_is_stable() {
        let v = serde_json::to_value(CallReplyEnvelope::value(
            "c",
            serde_json::json!({"tool": "x"}),
        ))
        .unwrap();
        assert_eq!(
            v,
            serde_json::json!({"corr_id": "c", "kind": "value", "data": {"tool": "x"}})
        );
        let e = serde_json::to_value(CallReplyEnvelope::error(
            "c",
            CallErrorKind::NoResponder,
            "none",
        ))
        .unwrap();
        assert_eq!(
            e,
            serde_json::json!({"corr_id": "c", "kind": "error", "error": "no_responder", "message": "none"})
        );
        assert!(CallReplyEnvelope::end("c").is_terminal());
        for k in [
            "no_responder",
            "responder_gone",
            "timeout",
            "overloaded",
            "cancelled",
            "unavailable",
            "unsupported",
            "not_found",
            "error",
        ] {
            assert_eq!(CallErrorKind::parse(k).unwrap().as_str(), k);
        }
    }
}
