//! Explicit guard loading only in the trusted native test executable.
use std::{ffi::c_void, os::windows::ffi::OsStrExt};

#[link(name = "kernel32")]
extern "system" {
    fn LoadLibraryW(path: *const u16) -> *mut c_void;
    fn GetProcAddress(module: *mut c_void, name: *const u8) -> *mut c_void;
}

pub fn install() {
    // The parent suite carries the DLL path as a capability; only explicitly
    // instrumented workers receive a log destination and selected file identity.
    if std::env::var_os("RM_PROBE_LOG").is_none() {
        return;
    }
    let Some(path) = std::env::var_os("PAYLOAD_WINDOWS_GUARD") else {
        return;
    };
    assert!(std::path::Path::new(&path).is_absolute());
    let path: Vec<u16> = path.encode_wide().chain(Some(0)).collect();
    // SAFETY: caller explicitly selects the trusted test DLL. Keep its module
    // reference until process exit because patched imports retain its functions.
    unsafe {
        let module = LoadLibraryW(path.as_ptr());
        assert!(!module.is_null(), "load Windows payload guard");
        let entry = GetProcAddress(module, b"rm_probe_install\0".as_ptr());
        assert!(!entry.is_null(), "guard install export");
        let install: unsafe extern "system" fn(*mut c_void) -> i32 = std::mem::transmute(entry);
        assert_eq!(
            install(std::ptr::null_mut()),
            1,
            "install Windows payload guard"
        );
    }
}
