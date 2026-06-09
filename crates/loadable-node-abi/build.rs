fn main() {
    // Build the Android-specific libc++ stream symbol helper only for
    // Android targets. This avoids pulling C++ iostream code into non-Android
    // builds and keeps the host/plugin ABI surface unchanged on other platforms.
    println!("cargo:rerun-if-changed=src/android_libcxx_streams.cc");

    let target = std::env::var("TARGET").unwrap_or_default();
    if target.contains("android") {
        cc::Build::new()
            .cpp(true)
            .file("src/android_libcxx_streams.cc")
            .compile("loadable_node_android_libcxx_streams");
    }
}
