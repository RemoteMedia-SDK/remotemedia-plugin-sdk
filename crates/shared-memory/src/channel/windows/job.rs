//! Broker-owned, noninheritable job for workers and ordinary descendants.
#[path = "job/launch.rs"]
mod launch;
#[path = "job/lifetime.rs"]
mod lifetime;
use std::{
    io, mem,
    os::windows::io::{AsHandle, AsRawHandle, FromRawHandle, OwnedHandle},
    process::{Child, ExitStatus},
    sync::Arc,
    time::{Duration, Instant},
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
    SetInformationJobObject, TerminateJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    JOB_OBJECT_LIMIT_ACTIVE_PROCESS, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
};

pub(crate) struct ProcessJob(Arc<OwnedHandle>, lifetime::Monitor);

impl ProcessJob {
    fn seal(&self) -> io::Result<()> {
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { mem::zeroed() };
        limits.BasicLimitInformation.LimitFlags =
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE | JOB_OBJECT_LIMIT_ACTIVE_PROCESS;
        limits.BasicLimitInformation.ActiveProcessLimit = 0;
        if unsafe {
            SetInformationJobObject(
                self.0.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                mem::size_of_val(&limits) as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    pub(crate) fn new() -> io::Result<Self> {
        // Null security attributes create a noninheritable handle. An unnamed
        // job cannot accidentally attach to another broker's existing object.
        let raw = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful creation transfers a new owning handle to us.
        let handle = Arc::new(unsafe { OwnedHandle::from_raw_handle(raw) });
        let job = Self(handle.clone(), lifetime::Monitor::new(handle)?);
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { mem::zeroed() };
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // Neither breakaway flag is set. Ordinary descendants inherit this job.
        let ok = unsafe {
            SetInformationJobObject(
                job.0.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                mem::size_of_val(&limits) as u32,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(job)
    }

    pub(crate) fn assign(&self, child: &Child) -> io::Result<()> {
        // SAFETY: both handles are live broker-owned objects. Never fall back to
        // an uncontained worker when nesting or access restrictions reject this.
        if unsafe { AssignProcessToJobObject(self.0.as_raw_handle(), child.as_raw_handle()) } == 0 {
            return Err(io::Error::last_os_error());
        }
        self.1.track(child.as_handle().try_clone_to_owned()?)?;
        Ok(())
    }

    pub(crate) fn terminate(&self, child: &mut Child) -> io::Result<ExitStatus> {
        // Collect every original identity before termination removes members
        // from the job's active list. A sealed lifetime count closes the set.
        let proof = self.seal().and_then(|()| self.1.snapshot(&self.0));
        // Even an already-reaped parent can have live descendants. Always kill
        // the job before consulting the original child's cached exit status.
        if unsafe { TerminateJobObject(self.0.as_raw_handle(), 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let status = child.wait()?;
        proof?.wait(&self.0, Instant::now() + Duration::from_secs(10))?;
        Ok(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::ProcessResources;
    use std::process::{Command, Stdio};

    #[test]
    fn seal_child() {
        if std::env::var_os("RM_JOB_SEAL_CHILD").is_some() {
            println!("seal fixture ready");
            std::thread::sleep(Duration::from_secs(10));
        }
    }

    fn command() -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "channel::platform::job::tests::seal_child",
                "--nocapture",
            ])
            .env("RM_JOB_SEAL_CHILD", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        command
    }

    #[test]
    fn zero_active_limit_rejects_assignment_without_killing_existing_member() {
        let job = ProcessJob::new().unwrap();
        let mut first = job
            .spawn_captured(&mut command(), &mut ProcessResources::default())
            .unwrap();
        use std::io::BufRead;
        let mut output = std::io::BufReader::new(first.stdout.take().unwrap());
        let mut line = String::new();
        loop {
            line.clear();
            assert_ne!(
                output.read_line(&mut line).unwrap(),
                0,
                "fixture did not start"
            );
            if line.contains("seal fixture ready") {
                break;
            }
        }
        job.seal().unwrap();
        let existing_alive = first.try_wait().unwrap().is_none();
        let error = job
            .spawn_captured(&mut command(), &mut ProcessResources::default())
            .unwrap_err();
        // The rejected spawn's cleanup terminates this private job too.
        job.terminate(&mut first).unwrap();
        println!("existing_alive_after_seal={existing_alive} new_assignment_error={error}");
        assert!(existing_alive);
        assert_eq!(error.raw_os_error(), Some(1816));
    }
}
