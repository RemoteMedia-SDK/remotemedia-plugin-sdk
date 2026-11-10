//! Factory declarations for buffer preflight. These describe promises made by
//! an implementation, not evidence that arbitrary node code never copies.
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const BUFFER_CONTRACT_VERSION: u16 = 1;
/// Owning views and call-selected allocation for synchronous in-process Python.
pub const INPROCESS_PYTHON_BUFFER_VERSION: u16 = 1;

mod validation;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BufferDomain {
    Host,
    SessionMapped,
    Device(String),
}

/// Logical storage layouts. Media format constraints are negotiated separately.
/// Tensor conventions cannot be inferred from the same numeric dtype code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BufferLayout {
    AudioInterleavedF32,
    VideoBytes,
    ImageBytes,
    TensorRuntime,
    TensorPythonIpc,
    TensorProtobuf,
    NumpyCContiguous,
    NumpyFContiguous,
    Binary,
    /// A device-resident tensor (`RuntimeData::DeviceTensor`), shared by
    /// owner on the device named by the port's `Device` domain.
    TensorDevice,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BufferProtocol {
    RustOwners,
    InProcessPython { buffers: u16 },
    NativeAbi { abi: String, buffers: u16 },
    MappedNative { wire: u16, snapshots: u16 },
    MappedPython { wire: u16, snapshots: u16 },
    Serialized,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BufferProduction {
    /// Every outgoing owner is retained from an input; no payload materialization.
    ForwardInput,
    /// New payloads are filled directly through the supplied output allocator.
    OutputAllocator,
    /// Each output either retains an admitted input or comes from the bound
    /// allocator, e.g. a transform that can bypass work for matching formats.
    ForwardOrOutputAllocator,
    /// A producer returns private storage requiring adaptation at a mapped edge.
    PrivateStorage,
}

/// Metadata is permitted on every port. Empty layouts explicitly means
/// metadata-only. An absent port means unknown.
/// The empty string identifies the anonymous/default port, not a wildcard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferPortContract {
    pub layouts: Vec<BufferLayout>,
    pub domains: Vec<BufferDomain>,
    pub max_payload_bytes: u64,
}

/// Peak retained owners/bytes declared by the node, including its own outputs.
/// Router queues, snapshots and transport credits need additional reservations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferRetention {
    pub owners: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferContract {
    pub version: u16,
    pub protocol: BufferProtocol,
    pub inputs: BTreeMap<String, BufferPortContract>,
    pub outputs: BTreeMap<String, BufferPortContract>,
    pub production: BufferProduction,
    /// None is unbounded/undeclared, never zero retention.
    pub retention: Option<BufferRetention>,
}

impl BufferContract {
    /// Declare default input/output ports that carry metadata but no bulk payloads.
    /// The host rejects bulk inputs and outputs; this does not bound text/JSON
    /// storage or prove anything about allocations inside the node.
    pub fn metadata_only(protocol: BufferProtocol) -> Self {
        let port = BufferPortContract {
            layouts: Vec::new(),
            domains: Vec::new(),
            max_payload_bytes: 0,
        };
        Self {
            version: BUFFER_CONTRACT_VERSION,
            protocol,
            inputs: BTreeMap::from([(String::new(), port.clone())]),
            outputs: BTreeMap::from([(String::new(), port)]),
            production: BufferProduction::ForwardInput,
            retention: Some(BufferRetention {
                owners: 0,
                bytes: 0,
            }),
        }
    }
}
