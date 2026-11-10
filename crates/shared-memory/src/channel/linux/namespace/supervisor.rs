use super::{sys, INIT, INIT_SOCKET, LIFELINE, READY, RUN, START, START_TIMEOUT};
use std::{
    ffi::OsString,
    fs::File,
    io,
    os::{
        fd::AsFd,
        unix::process::{CommandExt, ExitStatusExt},
    },
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Launcher(Child);
impl Drop for Launcher {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
struct InitOwner(File);
impl Drop for InitOwner {
    fn drop(&mut self) {
        let _ = sys::kill_pidfd(&self.0);
    }
}

/// Entrypoint for the matching standalone helper binary.
/// # Safety
/// Call only in a dedicated single-threaded executable which owns stdin. Its
/// main thread must remain alive until this function returns. Not a host-library
/// thread entrypoint; stdin is closed after it is forwarded to the child.
pub unsafe fn run_namespace_supervisor() -> io::Result<i32> {
    let mut args = std::env::args_os().skip(1);
    let mode = args
        .next()
        .ok_or_else(|| sys::invalid("missing supervisor mode"))?;
    let timeout: u64 = std::env::var(START_TIMEOUT)
        .map_err(|_| sys::invalid("missing startup deadline"))?
        .parse()
        .map_err(|_| sys::invalid("invalid startup deadline"))?;
    if !(1..=300_000).contains(&timeout) {
        return Err(sys::invalid("invalid startup deadline"));
    }
    let deadline = Instant::now() + Duration::from_millis(timeout);
    match mode.to_str() {
        Some("supervise-v2") => unsafe { supervise(args.collect(), deadline) },
        Some("init-v2") => unsafe { init(args.collect(), deadline) },
        _ => Err(sys::invalid("incompatible supervisor mode")),
    }
}

unsafe fn launch_namespace(
    args: &[OsString],
    listener: &sys::PrivateListener,
) -> io::Result<Launcher> {
    let mut command = Command::new(&args[0]);
    command
        .args([
            "--user",
            "--map-current-user",
            "--pid",
            "--fork",
            "--kill-child=SIGKILL",
            "--",
        ])
        .arg(std::env::current_exe()?)
        .arg("init-v2")
        .args(&args[1..])
        .env(INIT_SOCKET, listener.address())
        .stdin(Stdio::from(io::stdin().as_fd().try_clone_to_owned()?));
    let guardian = std::process::id() as libc::pid_t;
    // SAFETY: only async-signal-safe syscalls after fork. Unlike the broker's
    // caller thread, this guardian's main thread persists throughout execution.
    unsafe {
        command.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) < 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::getppid() != guardian {
                return Err(io::Error::from_raw_os_error(libc::ECHILD));
            }
            Ok(())
        });
    }
    let launcher = Launcher(command.spawn()?);
    drop(command); // Close its extra child-side protocol endpoint too.
    unsafe {
        sys::close_protocol_stdin();
    }
    Ok(launcher)
}

unsafe fn supervise(args: Vec<OsString>, deadline: Instant) -> io::Result<i32> {
    if args.len() < 2 {
        return Err(sys::invalid("missing unshare or worker executable"));
    }
    let lifeline = sys::connect(LIFELINE)?;
    sys::receive(&lifeline, START, false, None, deadline)?;
    let listener = sys::PrivateListener::new()?;
    let mut launcher = unsafe { launch_namespace(&args, &listener)? };
    let init_channel = listener.accept(Some(&lifeline), deadline)?;
    let root = sys::receive(&init_channel, INIT, true, Some(&lifeline), deadline)?.unwrap();
    sys::verify_init(&init_channel, &root, launcher.0.id())?;
    let root = InitOwner(root);
    sys::send(&lifeline, INIT, Some(&root.0))?;
    sys::receive(&lifeline, RUN, false, None, deadline)?;
    sys::send(&init_channel, RUN, None)?;
    sys::receive(&init_channel, READY, false, Some(&lifeline), deadline)?;
    let worker_pid = sys::receive_pid(&init_channel, Some(&lifeline), deadline)?;
    sys::send(&lifeline, READY, None)?;
    sys::send_pid(&lifeline, worker_pid)?;
    // A bounded poll interval is not a lifetime timeout. Both init death and
    // broker EOF are observed continuously, without reading the worker channel.
    loop {
        match sys::wait_fd(
            &root.0,
            Some(&lifeline),
            Instant::now() + Duration::from_secs(60),
        ) {
            Ok(()) => break,
            Err(error) if error.kind() == io::ErrorKind::TimedOut => continue,
            Err(error) if error.kind() == io::ErrorKind::ConnectionAborted => {
                sys::kill_pidfd(&root.0)?;
                sys::wait_fd(&root.0, None, Instant::now() + Duration::from_secs(10))?;
                break;
            }
            Err(error) => return Err(error),
        }
    }
    let status = launcher.0.wait()?;
    Ok(status
        .code()
        .unwrap_or_else(|| 128 + status.signal().unwrap_or(1)))
}

unsafe fn init(args: Vec<OsString>, deadline: Instant) -> io::Result<i32> {
    if std::process::id() != 1 || args.is_empty() {
        return Err(sys::invalid("namespace init identity or worker missing"));
    }
    let channel = sys::connect(INIT_SOCKET)?;
    let root = sys::self_pidfd()?;
    sys::send(&channel, INIT, Some(&root))?;
    sys::receive(&channel, RUN, false, None, deadline)?;
    let mut command = Command::new(&args[0]);
    command
        .args(&args[1..])
        .env_remove(LIFELINE)
        .env_remove(INIT_SOCKET)
        .env_remove(START_TIMEOUT)
        .stdin(Stdio::from(io::stdin().as_fd().try_clone_to_owned()?));
    let child = command.spawn()?;
    let worker = child.id() as libc::pid_t;
    drop(command);
    unsafe {
        sys::close_protocol_stdin();
    }
    sys::send(&channel, READY, None)?;
    sys::send_pid(&channel, child.id())?;
    drop(channel);
    // Namespace PID 1 reaps orphaned descendants as well as the main worker.
    // Once the main worker exits, returning exits init and the kernel kills any
    // remaining descendants, including detached process groups/nested namespaces.
    loop {
        let mut status = 0;
        let pid = unsafe { libc::waitpid(-1, &mut status, 0) };
        if pid < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if pid == worker {
            let status = std::process::ExitStatus::from_raw(status);
            return Ok(status
                .code()
                .unwrap_or_else(|| 128 + status.signal().unwrap_or(1)));
        }
    }
}
