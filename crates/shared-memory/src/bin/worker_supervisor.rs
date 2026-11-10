fn main() {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: dedicated single-threaded executable; owns inherited stdio.
        match unsafe { remotemedia_shared_memory::channel::run_namespace_supervisor() } {
            Ok(code) => std::process::exit(code),
            Err(error) => {
                eprintln!("worker namespace supervisor: {error}");
                std::process::exit(125);
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("worker namespace supervisor requires Linux");
        std::process::exit(125);
    }
}
