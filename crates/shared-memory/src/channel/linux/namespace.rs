//! Explicit Linux namespace launch. No inference from inherited environment.
use super::super::WorkerProcess;
use std::{
    ffi::OsStr,
    fs::File,
    io,
    os::unix::net::UnixStream,
    path::Path,
    process::Command,
    time::{Duration, Instant},
};
#[path = "namespace/supervisor.rs"]
mod supervisor;
#[path = "namespace/sys.rs"]
mod sys;
pub use supervisor::run_namespace_supervisor;

const LIFELINE: &str = "REMOTEMEDIA_NAMESPACE_LIFELINE_V1";
const INIT_SOCKET: &str = "REMOTEMEDIA_NAMESPACE_INIT_V1";
const START_TIMEOUT: &str = "REMOTEMEDIA_NAMESPACE_TIMEOUT_MS_V1";
const START: u8 = 1;
const INIT: u8 = 2;
const RUN: u8 = 3;
const READY: u8 = 4;

/// Build the supervisor command from the outset, preserving its exact environment,
/// working directory and stdout/stderr configuration through worker execution.
/// stdin is reserved for the private worker protocol, as in WorkerProcess::spawn.
/// The supervisor/unshare must be trusted, matching executables. This does not
/// make arbitrary worker code safe to share immutable mappings with.
pub struct NamespaceCommand {
    command: Command,
    timeout: Duration,
}
impl NamespaceCommand {
    pub fn new(supervisor: &Path, unshare: &Path, worker: impl AsRef<OsStr>) -> io::Result<Self> {
        // Absolute paths survive a requested worker current_dir/env_clear.
        let mut command = Command::new(supervisor.canonicalize()?);
        command
            .arg("supervise-v1")
            .arg(unshare.canonicalize()?)
            .arg(worker);
        Ok(Self {
            command,
            timeout: Duration::from_secs(10),
        })
    }

    /// Additional args are worker arguments. Environment and cwd apply to both
    /// supervisor and worker; internal bootstrap variables are stripped at exec.
    /// UID/GID transitions and custom pre_exec hooks are not supported by this
    /// launch contract. In particular, do not replace the helper's argv[0].
    pub fn command_mut(&mut self) -> &mut Command {
        &mut self.command
    }

    pub fn set_startup_timeout(&mut self, timeout: Duration) -> io::Result<()> {
        if timeout < Duration::from_millis(1) || timeout > Duration::from_secs(300) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "namespace startup timeout must be 1ms..300s",
            ));
        }
        self.timeout = timeout;
        Ok(())
    }
}

pub(crate) struct NamespaceLease {
    init: File,
    lifeline: UnixStream,
}
impl NamespaceLease {
    pub(crate) fn terminate(&self) -> io::Result<()> {
        sys::kill_pidfd(&self.init)?;
        // pidfd readiness for single-threaded namespace init follows the kernel's
        // namespace teardown. Timeout/error never authorizes buffer reclamation.
        sys::wait_fd(&self.init, None, Instant::now() + Duration::from_secs(10))?;
        let _ = self.lifeline.shutdown(std::net::Shutdown::Both);
        Ok(())
    }
}
impl Drop for NamespaceLease {
    fn drop(&mut self) {
        let _ = sys::kill_pidfd(&self.init);
    }
}

pub(crate) fn spawn(
    command: &mut NamespaceCommand,
    resources: super::super::ProcessResources,
) -> io::Result<WorkerProcess> {
    spawn_inner(command, resources, false)
}

pub(crate) fn spawn_captured(
    command: &mut NamespaceCommand,
    resources: super::super::ProcessResources,
) -> io::Result<WorkerProcess> {
    spawn_inner(command, resources, true)
}

fn spawn_inner(
    command: &mut NamespaceCommand,
    resources: super::super::ProcessResources,
    captured: bool,
) -> io::Result<WorkerProcess> {
    let listener = sys::PrivateListener::new()?;
    let deadline = Instant::now() + command.timeout;
    command
        .command
        .env(LIFELINE, listener.address())
        .env(START_TIMEOUT, command.timeout.as_millis().to_string());
    let result = if captured {
        WorkerProcess::spawn_capture_bootstrap(&mut command.command, resources)
    } else {
        WorkerProcess::spawn_with_resources(&mut command.command, resources)
    };
    command
        .command
        .env_remove(LIFELINE)
        .env_remove(START_TIMEOUT);
    let mut process = result?;
    let lifeline = listener.accept(None, deadline)?;
    if sys::peer_pid(&lifeline)? != process.child.id() as i32 {
        return Err(sys::invalid("namespace guardian identity mismatch"));
    }
    sys::send(&lifeline, START, None)?;
    let init = sys::receive(&lifeline, INIT, true, None, deadline)?.unwrap();
    // A real pidfd is required; arbitrary files cannot stand in for exit proof.
    sys::validate_pidfd(&init)?;
    process.namespace = Some(NamespaceLease { init, lifeline });
    let lease = process.namespace.as_ref().unwrap();
    sys::send(&lease.lifeline, RUN, None)?;
    sys::receive(&lease.lifeline, READY, false, None, deadline)?;
    Ok(process)
}
