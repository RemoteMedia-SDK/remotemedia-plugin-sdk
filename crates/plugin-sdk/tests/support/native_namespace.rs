use remotemedia_plugin_sdk::native_worker::NativeWorkerFactory;

pub fn configure(factory: NativeWorkerFactory, enabled: bool) -> NativeWorkerFactory {
    if !enabled {
        return factory;
    }
    // SAFETY: the test explicitly supplies the matching helper built from this
    // workspace and the installed util-linux launcher, without map delegation.
    unsafe { factory.with_linux_namespace(supervisor(), "/usr/bin/unshare".into()) }.unwrap()
}

pub fn supervisor() -> std::path::PathBuf {
    std::env::var_os("NATIVE_WORKER_SUPERVISOR")
        .expect("namespace tests require NATIVE_WORKER_SUPERVISOR pointing to the matching built helper")
        .into()
}
