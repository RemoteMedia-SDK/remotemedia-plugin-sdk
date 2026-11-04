//! Native control-bus RPC types. The wire protocol is shared with Python/JS.

use remotemedia_types::Error;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;

/// Authoritative identity supplied by the host for one invocation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CallContext {
    pub session_id: Option<String>,
    pub node_id: String,
}

impl crate::NodeRuntimeContextRead for CallContext {
    fn session_id(&self) -> &str {
        self.session_id.as_deref().unwrap_or("")
    }
    fn node_id(&self) -> &str {
        &self.node_id
    }
    fn capability(&self, _: &str) -> Option<crate::MediaCapabilities> {
        None
    }
    fn insert_capability(&self, _: &str, _: crate::MediaCapabilities) {}
    fn snapshot_any(&self, _: &str) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
    fn state_any(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
    fn try_state_any(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RpcMode {
    #[serde(rename = "ff")]
    FireAndForget,
    #[serde(rename = "reply")]
    Reply,
    #[serde(rename = "stream")]
    Stream,
}

/// Discoverable method. Schemas describe the kwargs object and result value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcMethod {
    pub name: String,
    pub mode: RpcMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_schema: Option<Value>,
}

impl RpcMethod {
    pub fn new(name: impl Into<String>, mode: RpcMode) -> Self {
        Self {
            name: name.into(),
            mode,
            params_schema: None,
            result_schema: None,
        }
    }
}

/// Correlation identifiers preserve the client's string or integer verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RpcId {
    String(String),
    Integer(i64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub method: String,
    pub corr_id: RpcId,
    #[serde(default)]
    pub args: Vec<Value>,
    #[serde(default)]
    pub kwargs: Map<String, Value>,
}

impl RpcRequest {
    /// Decode positional arguments as a tuple, e.g. `(String, usize)`.
    pub fn positional<T: DeserializeOwned>(&self) -> Result<T, Error> {
        serde_json::from_value(Value::Array(self.args.clone())).map_err(invalid_args)
    }
    /// Decode keyword arguments as a serde struct.
    pub fn named<T: DeserializeOwned>(&self) -> Result<T, Error> {
        serde_json::from_value(Value::Object(self.kwargs.clone())).map_err(invalid_args)
    }
}

fn invalid_args(error: serde_json::Error) -> Error {
    Error::Execution(format!("invalid RPC arguments: {error}"))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "lowercase")]
pub enum RpcEvent {
    Value(Value),
    End,
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RpcReply {
    pub corr_id: RpcId,
    #[serde(flatten)]
    pub event: RpcEvent,
}

/// A separate, bounded reply channel. Failure means the consumer is gone
/// or cannot accept the reply; implementations must not silently drop frames.
#[async_trait::async_trait]
pub trait RpcReplySink: Send + Sync {
    async fn send(&self, reply: RpcReply) -> Result<(), Error>;
}

/// Per-call typed emitter. The dispatcher owns completion; handlers emit values.
/// This is deliberately not Clone: one owner orders values and termination.
pub struct RpcResponder {
    sink: Arc<dyn RpcReplySink>,
    corr_id: RpcId,
    mode: RpcMode,
    terminal: bool,
}

impl RpcResponder {
    pub fn new(sink: Arc<dyn RpcReplySink>, corr_id: RpcId, mode: RpcMode) -> Self {
        Self {
            sink,
            corr_id,
            mode,
            terminal: false,
        }
    }

    pub async fn value<T: Serialize + Send>(&mut self, value: T) -> Result<(), Error> {
        if self.terminal {
            return Err(Error::Execution("RPC already completed".into()));
        }
        if self.mode == RpcMode::FireAndForget {
            return Err(Error::Execution(
                "fire-and-forget RPC cannot emit values".into(),
            ));
        }
        let value = serde_json::to_value(value).map_err(|error| {
            Error::Execution(format!("RPC result serialization failed: {error}"))
        })?;
        self.send(RpcEvent::Value(value), self.mode == RpcMode::Reply)
            .await
    }

    /// Complete once: unary values are terminal, streams end explicitly,
    /// errors terminate either mode, and fire-and-forget emits no reply.
    pub async fn finish(&mut self, result: Result<(), Error>) -> Result<(), Error> {
        if self.terminal {
            return result;
        }
        if self.mode == RpcMode::FireAndForget {
            self.terminal = true;
            return result;
        }
        let event = match result {
            Err(error) => RpcEvent::Error(error.to_string()),
            Ok(()) if self.mode == RpcMode::Reply => RpcEvent::Value(Value::Null),
            Ok(()) => RpcEvent::End,
        };
        self.send(event, true).await
    }

    async fn send(&mut self, event: RpcEvent, terminal: bool) -> Result<(), Error> {
        self.terminal = terminal;
        let result = self
            .sink
            .send(RpcReply {
                corr_id: self.corr_id.clone(),
                event,
            })
            .await;
        if result.is_err() {
            self.terminal = true;
        }
        result
    }
}
