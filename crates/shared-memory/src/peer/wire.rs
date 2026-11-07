//! Typed control messages. Bulk bytes are absent from every wire variant.
use crate::{AllocationId, ElementType, MessageDescriptor, PeerId, SessionId, WriteDescriptor};
use remotemedia_types::{RuntimeMetadata, TensorEncoding};
use serde::{Deserialize, Serialize};

pub const WIRE_VERSION: u16 = 1;
pub const SNAPSHOT_PROTOCOL: u16 = 1;
/// Adds node-output discovery/reads to the existing live-input protocol.
pub const SNAPSHOT_OUTPUT_PROTOCOL: u16 = 2;
/// Opt-in host-authorized materialization into a remote producer grant.
pub const OUTPUT_COPY_PROTOCOL: u16 = 1;
/// Explicit finite-source completion after a successful, drained tick.
pub const SOURCE_COMPLETION_PROTOCOL: u16 = 1;
fn false_flag(value: &bool) -> bool {
    !value
}
fn absent_protocol(version: &u16) -> bool {
    *version == 0
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotDescriptor {
    pub message: MessageDescriptor,
    pub pts_us: u64,
    pub written_at_us: u64,
    pub seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CallKind {
    Describe,
    Initialize,
    Process,
    Stream,
    MultiInput,
    Tick,
    Control,
    Rpc,
    Snapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallContext {
    pub session_id: Option<String>,
    pub node_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub calls: usize,
    pub inputs_per_call: usize,
    pub outputs_per_call: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            calls: 16,
            inputs_per_call: 32,
            outputs_per_call: 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum BufferReference {
    Input {
        lease: u64,
        offset: u64,
        byte_len: u64,
    },
    Producer {
        allocation: AllocationId,
        offset: u64,
        byte_len: u64,
    },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputMessage {
    pub metadata: RuntimeMetadata,
    pub buffer: Option<BufferReference>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AllocationFailure {
    InvalidLength,
    Exhausted,
    Unavailable,
    CopyDenied,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum HostMessage {
    Hello {
        version: u16,
        session: SessionId,
        peer: PeerId,
        encoding: TensorEncoding,
        limits: Limits,
        #[serde(default, skip_serializing_if = "absent_protocol")]
        snapshot_protocol: u16,
        #[serde(default, skip_serializing_if = "absent_protocol")]
        output_copy_protocol: u16,
        #[serde(default, skip_serializing_if = "absent_protocol")]
        source_completion_protocol: u16,
    },
    Begin {
        call: u64,
        kind: CallKind,
        context: CallContext,
        arguments: serde_json::Value,
        inputs: usize,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        snapshot_ports: Vec<String>,
    },
    Input {
        call: u64,
        name: String,
        message: MessageDescriptor,
    },
    Execute {
        call: u64,
    },
    Grant {
        request: u64,
        descriptor: WriteDescriptor,
    },
    AllocationFailed {
        request: u64,
        reason: AllocationFailure,
    },
    OutputAccepted {
        sequence: u64,
    },
    Cancel {
        call: u64,
    },
    Shutdown,
    Snapshot {
        request: u64,
        result: Result<Option<SnapshotDescriptor>, String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum WorkerMessage {
    Fatal {
        message: String,
    },
    Ready {
        version: u16,
        session: SessionId,
        peer: PeerId,
        modes: Vec<CallKind>,
        #[serde(default, skip_serializing_if = "absent_protocol")]
        snapshot_protocol: u16,
        #[serde(default, skip_serializing_if = "absent_protocol")]
        output_copy_protocol: u16,
        #[serde(default, skip_serializing_if = "absent_protocol")]
        source_completion_protocol: u16,
    },
    InputAccepted {
        lease: u64,
    },
    InputReleased {
        lease: u64,
    },
    Allocate {
        request: u64,
        call: u64,
        element: ElementType,
        elements: u64,
    },
    AllocateCopy {
        request: u64,
        call: u64,
        element: ElementType,
        elements: u64,
    },
    CopyFinished {
        call: u64,
        allocation: AllocationId,
        success: bool,
    },
    ReadSnapshot {
        request: u64,
        call: u64,
        port: String,
    },
    Output {
        call: u64,
        sequence: u64,
        message: OutputMessage,
    },
    ProducerReleased {
        allocation: AllocationId,
    },
    Complete {
        call: u64,
        result: std::result::Result<(), String>,
        #[serde(default, skip_serializing_if = "false_flag")]
        source_finished: bool,
    },
}
