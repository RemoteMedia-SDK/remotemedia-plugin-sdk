use remotemedia_plugin_sdk::{abi_stable::library::RootModule, NodePluginRef};

fn rejects_binary(variable: &str, previous: &str) {
    let path = std::env::var(variable).unwrap_or_else(|_| panic!("set {variable}"));
    let error = NodePluginRef::load_from_file(std::path::Path::new(&path))
        .err()
        .expect("older native ABI needs a rebuild");
    let message = format!("{error}");
    assert!(
        message.contains(previous) && message.contains("0.8"),
        "{message}"
    );
}

#[test]
#[ignore = "requires ABI_V07_DLL retained before factory contract discovery"]
fn current_host_rejects_v07_binary_with_version_diagnostic() {
    rejects_binary("ABI_V07_DLL", "0.7");
}

#[test]
#[ignore = "requires ABI_V06_DLL retained from the previous shared-buffer fixture"]
fn current_host_rejects_v06_binary_with_version_diagnostic() {
    rejects_binary("ABI_V06_DLL", "0.6");
}

#[test]
#[ignore = "requires ABI_V04_DLL built from the frozen ABI fixture"]
fn current_host_rejects_old_binary_with_version_diagnostic() {
    rejects_binary("ABI_V04_DLL", "0.4");
}

#[test]
#[ignore = "requires ABI_V05_DLL built from integration/abi-v05-fixture"]
fn current_host_rejects_v05_binary_with_version_diagnostic() {
    rejects_binary("ABI_V05_DLL", "0.5");
}
