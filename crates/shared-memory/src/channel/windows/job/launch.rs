//! Non-protocol programs must enter their job before their first instruction.
use super::ProcessJob;
use crate::channel::ProcessResources;
use std::{
    io, mem,
    os::windows::{
        io::{AsRawHandle, FromRawHandle, OwnedHandle},
        process::CommandExt,
    },
    process::{Child, Command},
};
use windows_sys::Win32::{
    Foundation::{ERROR_NO_MORE_FILES, INVALID_HANDLE_VALUE},
    System::{
        Diagnostics::ToolHelp::{
            CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
        },
        Threading::{
            GetProcessIdOfThread, OpenThread, ResumeThread, CREATE_NO_WINDOW, CREATE_SUSPENDED,
            THREAD_QUERY_LIMITED_INFORMATION, THREAD_SUSPEND_RESUME,
        },
    },
};

impl ProcessJob {
    pub(crate) fn spawn_captured(
        &self,
        command: &mut Command,
        resources: &mut ProcessResources,
    ) -> io::Result<Child> {
        // Captured commands are noninteractive. Keep std's argv/environment/pipe
        // handling, but do not execute dependency code before job assignment.
        let mut child = command
            .creation_flags(CREATE_NO_WINDOW | CREATE_SUSPENDED)
            .spawn()?;
        resources.live = true;
        let started = self.assign(&child).and_then(|()| resume_primary(&child));
        if let Err(error) = started {
            // Assignment itself may have failed, so also target the original
            // handle. Release guards only after child and job death are confirmed.
            let _ = child.kill();
            if self.terminate(&mut child).is_ok() {
                resources.release();
            }
            return Err(error);
        }
        Ok(child)
    }
}

fn resume_primary(child: &Child) -> io::Result<()> {
    // std::process retains the process handle, but closes CreateProcess's primary
    // thread handle. A freshly suspended, not-yet-executed program has one thread.
    // Refuse ambiguous snapshots rather than resuming an unidentified thread.
    let thread_id = primary_thread_id(child.id())?;
    let raw = unsafe {
        OpenThread(
            THREAD_SUSPEND_RESUME | THREAD_QUERY_LIMITED_INFORMATION,
            0,
            thread_id,
        )
    };
    if raw.is_null() {
        return Err(io::Error::last_os_error());
    }
    let thread = unsafe { OwnedHandle::from_raw_handle(raw) };
    // A thread ID alone can be reused between the snapshot and OpenThread. Bind
    // the opened handle to the still-owned process before allowing execution.
    if unsafe { GetProcessIdOfThread(thread.as_raw_handle()) } != child.id() {
        return Err(io::Error::other(
            "primary thread no longer belongs to the captured process",
        ));
    }
    match unsafe { ResumeThread(thread.as_raw_handle()) } {
        1 => Ok(()),
        u32::MAX => Err(io::Error::last_os_error()),
        _ => Err(io::Error::other("unexpected primary-thread suspend count")),
    }
}

fn primary_thread_id(process_id: u32) -> io::Result<u32> {
    let raw = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if raw == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    let snapshot = unsafe { OwnedHandle::from_raw_handle(raw) };
    let mut entry: THREADENTRY32 = unsafe { mem::zeroed() };
    entry.dwSize = mem::size_of_val(&entry) as u32;
    if unsafe { Thread32First(snapshot.as_raw_handle(), &mut entry) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut selected = None;
    loop {
        if entry.dwSize < 16 {
            return Err(io::Error::other("incomplete thread snapshot"));
        }
        if entry.th32OwnerProcessID == process_id {
            if selected.replace(entry.th32ThreadID).is_some() {
                return Err(io::Error::other("ambiguous suspended process threads"));
            }
        }
        entry.dwSize = mem::size_of_val(&entry) as u32;
        if unsafe { Thread32Next(snapshot.as_raw_handle(), &mut entry) } == 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_NO_MORE_FILES as i32) {
                return Err(error);
            }
            break;
        }
    }
    selected.ok_or_else(|| io::Error::other("suspended primary thread missing"))
}
