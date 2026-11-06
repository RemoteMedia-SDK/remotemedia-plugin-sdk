use remotemedia_plugin_sdk::{abi_stable::library::RootModule, NodePluginRef};

#[test]
#[ignore = "requires ABI_V06_DLL retained from the previous shared-buffer fixture"]
fn current_host_rejects_v06_binary_with_version_diagnostic() {
    let path = std::env::var("ABI_V06_DLL").expect("set ABI_V06_DLL");
    let error = NodePluginRef::load_from_file(std::path::Path::new(&path))
        .err()
        .expect("ABI 0.6 needs rebuild for owning copy ledgers");
    let message = format!("{error}");
    assert!(
        message.contains("0.6") && message.contains("0.7"),
        "{message}"
    );
}

#[test]
#[ignore = "requires ABI_V04_DLL built from the frozen ABI fixture"]
fn current_host_rejects_old_binary_with_version_diagnostic() {
    let path = std::env::var("ABI_V04_DLL").expect("set ABI_V04_DLL");
    let error = NodePluginRef::load_from_file(std::path::Path::new(&path))
        .err()
        .expect("old ABI must require rebuild");
    let message = format!("{error}");
    assert!(
        message.contains("0.4") && message.contains("0.7"),
        "{message}"
    );
}

#[test]
#[ignore = "requires ABI_V05_DLL built from integration/abi-v05-fixture"]
fn current_host_rejects_v05_binary_with_version_diagnostic() {
    let path = std::env::var("ABI_V05_DLL").expect("set ABI_V05_DLL");
    let error = NodePluginRef::load_from_file(std::path::Path::new(&path))
        .err()
        .expect("ABI 0.5 needs rebuild");
    let message = format!("{error}");
    assert!(
        message.contains("0.5") && message.contains("0.7"),
        "{message}"
    );
}
