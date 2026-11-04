use abi_stable::library::RootModule;
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let result = loadable_node_abi::NodePluginRef::load_from_file(std::path::Path::new(&args[1]));
    match args[2].as_str() {
        "accept" => {
            result.expect("ABI 0.4 host must load ABI 0.4 plugin");
        }
        "reject" => {
            let error = result
                .err()
                .expect("ABI 0.4 host must reject ABI 0.5 plugin");
            assert!(format!("{error}").contains("incompatible package versions"));
            println!("ABI 0.4 host correctly rejected ABI 0.5 plugin");
        }
        _ => panic!("expected accept or reject"),
    }
}
