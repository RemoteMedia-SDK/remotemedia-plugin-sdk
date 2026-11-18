//! Process lifetime ownership for trusted macOS commands and their children.
use crate::process_capture::{self, CapturedProcess};
pub use crate::process_capture::{CapturedOutput, ProcessResources};
use std::{
    io,
    os::unix::process::CommandExt,
    process::{Child, ChildStderr, ChildStdout, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Use only for a trusted command and descendants that stay in its process
/// group. A process group provides lifetime ownership, not installer isolation.
pub fn capture_trusted(
    command: &mut Command,
    mut resources: ProcessResources,
) -> io::Result<CapturedOutput> {
    let child = command
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    resources.live = true;
    process_capture::begin(ProcessGroup {
        child,
        resources,
        signalled: false,
        terminated: None,
    })
}

struct ProcessGroup {
    child: Child,
    resources: ProcessResources,
    signalled: bool,
    terminated: Option<ExitStatus>,
}

impl ProcessGroup {
    fn leader_exited(&self) -> io::Result<bool> {
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        loop {
            // Observe exit without reaping the child. Until reaping, its PID
            // still identifies the group that was created by CommandExt.
            let result = unsafe {
                libc::waitid(
                    libc::P_PID,
                    self.child.id() as libc::id_t,
                    &mut info,
                    libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
                )
            };
            if result == 0 {
                return Ok(info.si_pid != 0);
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    fn signal_group(&self, signal: libc::c_int) -> io::Result<bool> {
        let result = unsafe { libc::killpg(self.child.id() as libc::pid_t, signal) };
        if result == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(false)
        } else {
            Err(error)
        }
    }
}

impl CapturedProcess for ProcessGroup {
    fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout.take()
    }

    fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.stderr.take()
    }

    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        if self.leader_exited()? {
            self.terminate().map(Some)
        } else {
            Ok(None)
        }
    }

    fn terminate(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.terminated {
            return Ok(status);
        }
        if !self.signalled {
            let exited = self.leader_exited()?;
            if let Err(error) = self.signal_group(libc::SIGKILL) {
                // Darwin can return EPERM when the group only contains the
                // exited leader. Reap it and still require the group to vanish.
                if error.raw_os_error() != Some(libc::EPERM) || !(exited || self.leader_exited()?) {
                    return Err(error);
                }
            }
            self.signalled = true;
        }
        let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait()? {
                // Once reaped, only test for group presence. A recycled group
                // ID cannot receive a real signal from this owner.
                if !self.signal_group(0)? {
                    self.terminated = Some(status);
                    self.resources.release();
                    return Ok(status);
                }
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "macOS command process group did not exit",
                ));
            }
            thread::sleep(POLL_INTERVAL);
        }
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    struct Lease(Arc<AtomicBool>);

    impl Drop for Lease {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn captures_a_short_command_without_descendants() {
        let mut command = Command::new("/bin/echo");
        command.arg("done");
        let output = futures::executor::block_on(
            capture_trusted(&mut command, ProcessResources::default()).unwrap(),
        )
        .unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"done\n");
    }

    #[test]
    fn captures_failed_command_and_closes_descendant_pipes() {
        let released = Arc::new(AtomicBool::new(false));
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            "sleep 300 & printf 'output'; printf 'error' >&2; exit 23",
        ]);
        let output = futures::executor::block_on(
            capture_trusted(&mut command, ProcessResources::new(Lease(released.clone()))).unwrap(),
        )
        .unwrap();
        assert_eq!(output.status.code(), Some(23));
        assert_eq!(output.stdout, b"output");
        assert_eq!(output.stderr, b"error");
        assert!(released.load(Ordering::Acquire));
    }

    #[test]
    fn dropping_capture_joins_the_group_and_releases_its_lease() {
        let released = Arc::new(AtomicBool::new(false));
        let directory = tempfile::tempdir().unwrap();
        let ready = directory.path().join("ready");
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "sleep 300 & echo $! > \"$1\"; wait", "fixture"]);
        command.arg(&ready);
        let capture =
            capture_trusted(&mut command, ProcessResources::new(Lease(released.clone()))).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let descendant = loop {
            if let Some(pid) = std::fs::read_to_string(&ready)
                .ok()
                .and_then(|value| value.trim().parse::<libc::pid_t>().ok())
            {
                break pid;
            }
            assert!(Instant::now() < deadline, "test child did not start");
            thread::sleep(POLL_INTERVAL);
        };
        assert!(!released.load(Ordering::Acquire));
        drop(capture);
        assert!(released.load(Ordering::Acquire));
        assert_ne!(unsafe { libc::kill(descendant, 0) }, 0);
    }
}
