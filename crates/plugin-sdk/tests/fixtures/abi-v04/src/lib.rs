use abi_stable::{
    prefix_type::PrefixTypeTrait,
    sabi_trait::TD_Opaque,
    std_types::{ROk, RResult, RString, RVec},
};
use async_ffi::{FfiFuture, FutureExt};
use loadable_node_abi::*;

struct Echo;
impl FfiNode for Echo {
    fn node_type(&self) -> RString {
        "LegacyEcho".into()
    }
    fn process(&self, input: RVec<u8>) -> FfiFuture<RResult<RVec<u8>, RString>> {
        async move { ROk(input) }.into_ffi()
    }
}
struct Factory;
impl FfiNodeFactory for Factory {
    fn node_type(&self) -> RString {
        "LegacyEcho".into()
    }
    fn create(&self, _: RString) -> RResult<FfiNodeBox, RString> {
        ROk(FfiNode_TO::from_value(Echo, TD_Opaque))
    }
}
#[abi_stable::sabi_extern_fn]
fn factories() -> RVec<FfiNodeFactoryBox> {
    vec![FfiNodeFactory_TO::from_value(Factory, TD_Opaque)].into()
}
#[abi_stable::export_root_module]
fn root() -> NodePluginRef {
    NodePlugin {
        list_factories: factories,
    }
    .leak_into_prefix()
}
