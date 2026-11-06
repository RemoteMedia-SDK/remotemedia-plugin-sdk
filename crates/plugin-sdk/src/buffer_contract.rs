//! Factory capability metadata for the versioned native buffer path.
use abi_stable::std_types::{ROption, RResult, RString};
use remotemedia_traits::buffer_contract::{BufferContract, BufferProtocol};

pub fn native_protocol() -> BufferProtocol {
    BufferProtocol::NativeAbi {
        abi: loadable_node_abi::ABI_VERSION.into(),
        buffers: loadable_node_abi::buffer::BUFFER_ABI_VERSION,
    }
}

/// Encode an authored declaration, never infer it from media formats. None
/// remains unknown. The factory must be pure and cover its exact parameters.
pub fn encode(contract: Option<BufferContract>) -> RResult<ROption<RString>, RString> {
    let Some(contract) = contract else {
        return abi_stable::std_types::ROk(ROption::RNone);
    };
    let result = serde_json::to_string(&contract)
        .map_err(|error| error.to_string().into())
        .and_then(|json| {
            if json.len() > loadable_node_abi::MAX_BUFFER_CONTRACT_BYTES {
                Err(RString::from("native buffer contract exceeds 1 MiB"))
            } else {
                Ok(ROption::RSome(json.into()))
            }
        });
    result.into()
}
