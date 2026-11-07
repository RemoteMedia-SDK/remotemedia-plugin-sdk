//! Observation collects identities; the sealed job's lifetime count proves
//! completeness. Polling frequency is never a reclamation authority.
use std::{
    io, mem,
    os::windows::io::{AsHandle, AsRawHandle, FromRawHandle, OwnedHandle},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use windows_sys::Win32::{
    Foundation::{CompareObjectHandles, ERROR_MORE_DATA, WAIT_OBJECT_0, WAIT_TIMEOUT},
    System::{
        JobObjects::{
            IsProcessInJob, JobObjectBasicAccountingInformation, JobObjectBasicProcessIdList,
            QueryInformationJobObject, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
            JOBOBJECT_BASIC_PROCESS_ID_LIST,
        },
        Threading::{
            OpenProcess, WaitForSingleObject, PROCESS_QUERY_LIMITED_INFORMATION,
            PROCESS_SYNCHRONIZE,
        },
    },
};

const MAX_IDENTITIES: usize = 4096;

pub(super) fn accounting(job: &OwnedHandle) -> io::Result<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION> {
    let mut info = unsafe { mem::zeroed::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() };
    if unsafe {
        QueryInformationJobObject(
            job.as_raw_handle(),
            JobObjectBasicAccountingInformation,
            (&mut info as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
            mem::size_of_val(&info) as u32,
            std::ptr::null_mut(),
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(info)
}

fn members(job: &OwnedHandle) -> io::Result<Vec<u32>> {
    let mut capacity = 64;
    for _ in 0..8 {
        // usize storage supplies pointer alignment; the first two DWORDs precede
        // the variable pointer-sized array on both Windows pointer widths.
        let header_bytes =
            mem::size_of::<JOBOBJECT_BASIC_PROCESS_ID_LIST>() - mem::size_of::<usize>();
        let mut storage = vec![0usize; header_bytes / mem::size_of::<usize>() + capacity];
        let info = storage
            .as_mut_ptr()
            .cast::<JOBOBJECT_BASIC_PROCESS_ID_LIST>();
        let ok = unsafe {
            QueryInformationJobObject(
                job.as_raw_handle(),
                JobObjectBasicProcessIdList,
                info.cast(),
                (storage.len() * mem::size_of::<usize>()) as u32,
                std::ptr::null_mut(),
            )
        };
        let error = io::Error::last_os_error();
        if ok == 0 && error.raw_os_error() != Some(ERROR_MORE_DATA as i32) {
            return Err(error);
        }
        let assigned = unsafe { (*info).NumberOfAssignedProcesses as usize };
        let count = unsafe { (*info).NumberOfProcessIdsInList as usize };
        if assigned > MAX_IDENTITIES {
            return Err(io::Error::other("job member identity budget exceeded"));
        }
        if ok == 0 || count < assigned {
            capacity = assigned.max(capacity * 2).min(MAX_IDENTITIES);
            continue;
        }
        if count > capacity {
            return Err(io::Error::other("invalid job membership count"));
        }
        let ids = unsafe { std::slice::from_raw_parts((*info).ProcessIdList.as_ptr(), count) };
        return ids
            .iter()
            .map(|id| u32::try_from(*id).map_err(|_| io::Error::other("invalid job process ID")))
            .collect();
    }
    Err(io::Error::other(
        "job membership snapshot remained incomplete",
    ))
}

#[derive(Default)]
struct Ledger {
    handles: Vec<OwnedHandle>,
    exhausted: bool,
}
impl Ledger {
    fn track(&mut self, handle: OwnedHandle) {
        if self.handles.iter().any(|prior| unsafe { CompareObjectHandles(prior.as_raw_handle(), handle.as_raw_handle()) } != 0) {
            return;
        }
        if self.handles.len() == MAX_IDENTITIES {
            self.exhausted = true;
            return;
        }
        self.handles.push(handle);
    }

    fn observe(&mut self, job: &OwnedHandle) -> io::Result<()> {
        for id in members(job)? {
            let raw = unsafe {
                OpenProcess(
                    PROCESS_SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION,
                    0,
                    id,
                )
            };
            if raw.is_null() {
                continue;
            }
            let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
            let mut belongs = 0;
            // A numeric PID can race exit/reuse. Only an opened handle verified
            // against this original job may enter the identity ledger.
            if unsafe { IsProcessInJob(handle.as_raw_handle(), job.as_raw_handle(), &mut belongs) }
                != 0
                && belongs != 0
            {
                self.track(handle);
            }
        }
        Ok(())
    }
}

pub(super) struct Monitor {
    ledger: Arc<Mutex<Ledger>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}
impl Monitor {
    pub(super) fn new(job: Arc<OwnedHandle>) -> io::Result<Self> {
        let ledger = Arc::new(Mutex::new(Ledger::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let observed = ledger.clone();
        let stopped = stop.clone();
        let thread = thread::Builder::new()
            .name("rm-job-identities".into())
            .spawn(move || {
                while !stopped.load(Ordering::Acquire) {
                    // Missed/transient observations cannot authorize release: the
                    // sealed lifetime-count comparison below must still succeed.
                    let Ok(mut ledger) = observed.lock() else {
                        break;
                    };
                    let _ = ledger.observe(&job);
                    drop(ledger);
                    thread::sleep(Duration::from_millis(5));
                }
            })?;
        Ok(Self {
            ledger,
            stop,
            thread: Some(thread),
        })
    }

    pub(super) fn track(&self, handle: OwnedHandle) -> io::Result<()> {
        self.ledger
            .lock()
            .map_err(|_| io::Error::other("job identity ledger poisoned"))?
            .track(handle);
        Ok(())
    }

    /// Caller has sealed this job against further assignments before snapshot.
    pub(super) fn snapshot(&self, job: &OwnedHandle) -> io::Result<ExitProof> {
        let mut ledger = self
            .ledger
            .lock()
            .map_err(|_| io::Error::other("job identity ledger poisoned"))?;
        ledger.observe(job)?;
        let total = accounting(job)?.TotalProcesses;
        if ledger.exhausted || total as usize != ledger.handles.len() {
            return Err(io::Error::other(format!(
                "job exit proof incomplete: lifetime members={total}, owned identities={}, exhausted={}",
                ledger.handles.len(), ledger.exhausted)));
        }
        let handles = ledger
            .handles
            .iter()
            .map(|h| h.as_handle().try_clone_to_owned())
            .collect::<io::Result<Vec<_>>>()?;
        Ok(ExitProof { handles, total })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::{ProcessResources, WorkerProcess};
    use std::process::{Command, Stdio};

    #[test]
    fn missing_lifetime_identity_quarantines_resources_even_with_zero_active_members() {
        let owner = Arc::new(());
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "channel::platform::job::tests::seal_child"])
            .stdin(Stdio::null());
        let mut process =
            WorkerProcess::spawn_captured(&mut command, ProcessResources::new(owner.clone()))
                .unwrap();
        process.wait().unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while accounting(&process.job.0).unwrap().ActiveProcesses != 0 {
            assert!(
                Instant::now() < deadline,
                "exited fixture still counted active"
            );
            thread::sleep(Duration::from_millis(1));
        }
        let total = accounting(&process.job.0).unwrap().TotalProcesses;
        assert!(total > 0);
        // Inject a lost historical observation after the process has exited and
        // disappeared from the active list. A fresh PID scan cannot repair it.
        {
            let mut ledger = process.job.1.ledger.lock().unwrap();
            assert!(!ledger.handles.is_empty());
            println!(
                "injected_lost_history lifetime_members={total} recorded={}",
                ledger.handles.len()
            );
            ledger.handles.clear();
        }
        let error = process.terminate().unwrap_err();
        assert!(
            error.to_string().contains("exit proof incomplete"),
            "{error}"
        );
        assert_eq!(Arc::strong_count(&owner), 2);
        drop(process);
        // Intentional tiny quarantine allocation; process exit is its lifetime.
        assert_eq!(Arc::strong_count(&owner), 2);
    }
}
impl Drop for Monitor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(super) struct ExitProof {
    handles: Vec<OwnedHandle>,
    total: u32,
}
impl ExitProof {
    pub(super) fn wait(self, job: &OwnedHandle, deadline: Instant) -> io::Result<()> {
        for handle in &self.handles {
            let timeout = deadline
                .saturating_duration_since(Instant::now())
                .as_millis()
                .min(u32::MAX as u128 - 1) as u32;
            match unsafe { WaitForSingleObject(handle.as_raw_handle(), timeout) } {
                WAIT_OBJECT_0 => (),
                WAIT_TIMEOUT => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "job member handle did not signal exit",
                    ))
                }
                _ => return Err(io::Error::last_os_error()),
            }
        }
        loop {
            let final_count = accounting(job)?;
            if final_count.TotalProcesses != self.total {
                return Err(io::Error::other(
                    "sealed job membership changed during exit proof",
                ));
            }
            if final_count.ActiveProcesses == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "exited job members remain in active accounting",
                ));
            }
            thread::sleep(Duration::from_millis(1));
        }
    }
}
