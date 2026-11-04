use remotemedia_plugin_sdk::{abi_stable::library::RootModule, NodePluginRef};

#[test]
#[ignore = "requires ABI_V04_DLL built from the frozen ABI fixture"]
fn current_host_rejects_old_binary_with_version_diagnostic() {
    let path = std::env::var("ABI_V04_DLL").expect("set ABI_V04_DLL");
    let error = NodePluginRef::load_from_file(std::path::Path::new(&path))
        .err()
        .expect("old ABI must require rebuild");
    let message = format!("{error}");
    assert!(
        message.contains("0.4") && message.contains("0.5"),
        "{message}"
    );
}
