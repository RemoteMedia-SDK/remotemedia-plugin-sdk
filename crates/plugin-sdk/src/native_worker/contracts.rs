//! Authored capability metadata checked against the concrete mapped adapter.
use remotemedia_shared_memory::peer::wire::{
    SNAPSHOT_OUTPUT_PROTOCOL, SNAPSHOT_PROTOCOL, WIRE_VERSION,
};
use remotemedia_traits::buffer_contract::*;
use remotemedia_types::Error;
use serde_json::Value;
use std::sync::Arc;

pub(crate) type Declaration =
    Arc<dyn Fn(&Value, &BufferProtocol) -> Result<Option<BufferContract>, Error> + Send + Sync>;

pub(crate) fn protocol(python: bool, inputs: &[String], outputs: &[String]) -> BufferProtocol {
    let snapshots = if !outputs.is_empty() {
        SNAPSHOT_OUTPUT_PROTOCOL
    } else if !inputs.is_empty() {
        SNAPSHOT_PROTOCOL
    } else {
        0
    };
    if python {
        BufferProtocol::MappedPython {
            wire: WIRE_VERSION,
            snapshots,
        }
    } else {
        BufferProtocol::MappedNative {
            wire: WIRE_VERSION,
            snapshots,
        }
    }
}

pub(crate) fn inspect(
    declaration: Option<&Declaration>,
    params: &Value,
    protocol: BufferProtocol,
) -> Result<Option<BufferContract>, Error> {
    let Some(declaration) = declaration else {
        return Ok(None);
    };
    let Some(contract) = declaration(params, &protocol)? else {
        return Ok(None);
    };
    if contract.version != BUFFER_CONTRACT_VERSION || contract.protocol != protocol {
        return Err(Error::ConfigError(
            "mapped buffer declaration disagrees with the adapter's contract or transport version"
                .into(),
        ));
    }
    if contract.production == BufferProduction::PrivateStorage {
        return Err(Error::ConfigError(
            "mapped producers cannot declare private output storage".into(),
        ));
    }
    for port in contract.inputs.values().chain(contract.outputs.values()) {
        port.validate_definition().map_err(Error::ConfigError)?;
        if !port.layouts.is_empty() && port.domains != [BufferDomain::SessionMapped] {
            return Err(Error::ConfigError(
                "mapped bulk ports must declare only SessionMapped storage".into(),
            ));
        }
        if port.layouts.iter().any(|layout| {
            matches!(
                layout,
                BufferLayout::TensorPythonIpc | BufferLayout::TensorProtobuf
            )
        }) {
            return Err(Error::ConfigError("mapped worker tensors require the runtime encoding; other conventions need an explicit adapter".into()));
        }
    }
    Ok(Some(contract))
}
