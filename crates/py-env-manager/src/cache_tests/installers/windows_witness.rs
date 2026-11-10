use std::{
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    path::Path,
};
use windows_sys::Win32::{
    Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT},
    System::Threading::{OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE},
};

pub(super) struct Processes(Vec<OwnedHandle>);
impl Processes {
    pub(super) fn capture(project: &Path) -> Self {
        Self(
            (0..=2)
                .map(|depth| {
                    let pid = std::fs::read_to_string(project.join(format!("ready-{depth}")))
                        .unwrap()
                        .parse()
                        .unwrap();
                    let raw = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
                    assert!(!raw.is_null(), "{}", std::io::Error::last_os_error());
                    let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
                    assert_eq!(
                        unsafe { WaitForSingleObject(handle.as_raw_handle(), 0) },
                        WAIT_TIMEOUT,
                        "fixture exited before cancellation"
                    );
                    handle
                })
                .collect(),
        )
    }

    pub(super) fn assert_exited(&self) {
        for handle in &self.0 {
            assert_eq!(
                unsafe { WaitForSingleObject(handle.as_raw_handle(), 0) },
                WAIT_OBJECT_0,
                "installer fixture was still alive when cancellation returned"
            );
        }
        println!("original_installer_process_handles_exited=true");
    }
}
