use super::*;
use abi_stable::std_types::ROption;
use remotemedia_plugin_sdk::traits::buffer_contract::*;
use std::collections::BTreeMap;

/// Audited stream/snapshot factory; the older fixture retains its unknown contract.
#[derive(Default)]
pub struct ContractFactory;
impl FfiNodeFactory for ContractFactory {
    fn input_ports(
        &self,
    ) -> remotemedia_plugin_sdk::abi_stable::std_types::RVec<snapshot_abi::FfiPortDeclaration> {
        Factory.input_ports()
    }
    fn output_ports(
        &self,
    ) -> remotemedia_plugin_sdk::abi_stable::std_types::RVec<snapshot_abi::FfiPortDeclaration> {
        Factory.output_ports()
    }
    fn node_type(&self) -> RString {
        "ContractBufferFixture".into()
    }
    fn buffer_contract(&self, params: RString) -> RResult<ROption<RString>, RString> {
        let params: serde_json::Value = match serde_json::from_str(&params) {
            Ok(value) => value,
            Err(error) => return RErr(error.to_string().into()),
        };
        let mode = params["mode"].as_str().unwrap_or("echo");
        match mode {
            "unknown_contract" => return ROk(ROption::RNone),
            "contract_error" => return RErr("fixture declaration failed".into()),
            "malformed_contract" => return ROk(ROption::RSome("{malformed".into())),
            "oversized_contract" => return ROk(ROption::RSome("x".repeat(1024 * 1024 + 1).into())),
            _ => {}
        }
        let production = match mode {
            "echo" | "chunks" | "retain" | "snapshot_late" | "snapshot_read" => {
                BufferProduction::ForwardInput
            }
            "gain" | "private" | "lie_protocol" | "wrong_abi" | "wrong_buffers"
            | "rust_protocol" => BufferProduction::OutputAllocator,
            _ => return ROk(ROption::RNone),
        };
        let domain = match params.get("domain") {
            None => BufferDomain::Host,
            Some(value) => match serde_json::from_value::<BufferDomain>(value.clone()) {
                Ok(domain @ (BufferDomain::Host | BufferDomain::SessionMapped)) => domain,
                _ => return RErr("fixture domain must be host or session_mapped".into()),
            },
        };
        let port = BufferPortContract {
            layouts: vec![BufferLayout::AudioInterleavedF32, BufferLayout::Binary],
            domains: vec![domain],
            max_payload_bytes: 4096,
        };
        let mut contract = BufferContract {
            version: BUFFER_CONTRACT_VERSION,
            protocol: remotemedia_plugin_sdk::buffer_contract::native_protocol(),
            inputs: BTreeMap::from([("".into(), port.clone()), ("latest".into(), port.clone())]),
            outputs: BTreeMap::from([("".into(), port.clone()), ("latest".into(), port)]),
            production,
            retention: Some(BufferRetention {
                owners: 4,
                bytes: 16384,
            }),
        };
        match mode {
            "wrong_abi" => {
                contract.protocol = BufferProtocol::NativeAbi {
                    abi: "0.7.0".into(),
                    buffers: 2,
                }
            }
            "wrong_buffers" => {
                if let BufferProtocol::NativeAbi { buffers, .. } = &mut contract.protocol {
                    *buffers = 1;
                }
            }
            "rust_protocol" => contract.protocol = BufferProtocol::RustOwners,
            _ => {}
        }
        remotemedia_plugin_sdk::buffer_contract::encode(Some(contract))
    }
    fn create(&self, params: RString) -> RResult<FfiNodeBox, RString> {
        let mut params: serde_json::Value = match serde_json::from_str(&params) {
            Ok(value) => value,
            Err(error) => return RErr(error.to_string().into()),
        };
        if params["mode"] == "lie_protocol" {
            params["mode"] = serde_json::json!("legacy");
        }
        Factory.create(serde_json::to_string(&params).unwrap().into())
    }
}
