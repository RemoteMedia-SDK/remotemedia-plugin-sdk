use super::super::{receive_first_byte, send_first_byte};
use std::{
    fs::File,
    io::{self, Read},
    mem,
    os::{
        fd::{AsRawFd, FromRawFd},
        linux::net::SocketAddrExt,
        unix::net::{SocketAddr, UnixListener, UnixStream},
    },
    time::Instant,
};

pub(super) fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(super) struct PrivateListener {
    listener: UnixListener,
    name: String,
}
impl PrivateListener {
    pub(super) fn new() -> io::Result<Self> {
        // Abstract sockets disappear with their last owning fd, even on SIGKILL.
        // Random addresses avoid collisions; credentials authenticate peers.
        // Abstract addresses are discoverable and are not an access-control ACL.
        let mut random = [0; 32];
        File::open("/dev/urandom")?.read_exact(&mut random)?;
        let mut name = String::from("remotemedia.ns.");
        for byte in random {
            use std::fmt::Write;
            write!(&mut name, "{byte:02x}").unwrap();
        }
        let listener = UnixListener::bind_addr(&SocketAddr::from_abstract_name(name.as_bytes())?)?;
        listener.set_nonblocking(true)?;
        Ok(Self { listener, name })
    }
    pub(super) fn address(&self) -> &str {
        &self.name
    }
    pub(super) fn accept(
        &self,
        cancel: Option<&UnixStream>,
        deadline: Instant,
    ) -> io::Result<UnixStream> {
        wait_fd(&self.listener, cancel, deadline)?;
        self.listener.accept().map(|(stream, _)| stream)
    }
}

pub(super) fn connect(variable: &str) -> io::Result<UnixStream> {
    let name =
        std::env::var(variable).map_err(|_| invalid("missing namespace bootstrap endpoint"))?;
    UnixStream::connect_addr(&SocketAddr::from_abstract_name(name.as_bytes())?)
}

/// Wait for one bounded protocol byte or process death, interruptible by a
/// separate lifeline. No line parser or unbounded allocation exists at startup.
pub(super) fn wait_fd(
    target: &impl AsRawFd,
    cancel: Option<&UnixStream>,
    deadline: Instant,
) -> io::Result<()> {
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "namespace lifecycle wait timed out",
                )
            })?;
        let mut ready = [
            libc::pollfd {
                fd: target.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: cancel.map_or(-1, AsRawFd::as_raw_fd),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let timeout = remaining.as_millis().clamp(1, i32::MAX as u128) as i32;
        let result = unsafe { libc::poll(ready.as_mut_ptr(), ready.len() as _, timeout) };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if ready[1].revents != 0 {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "namespace broker lifeline closed or violated startup order",
            ));
        }
        if ready[0].revents & (libc::POLLERR | libc::POLLNVAL) != 0 {
            return Err(invalid("invalid namespace lifecycle descriptor"));
        }
        if ready[0].revents != 0 {
            return Ok(());
        }
    }
}

pub(super) fn send(stream: &UnixStream, token: u8, file: Option<&File>) -> io::Result<()> {
    send_first_byte(stream, token, file)
}
pub(super) fn receive(
    stream: &UnixStream,
    expected: u8,
    file: bool,
    cancel: Option<&UnixStream>,
    deadline: Instant,
) -> io::Result<Option<File>> {
    wait_fd(stream, cancel, deadline)?;
    let mut token = 0;
    let mut files = receive_first_byte(stream, &mut token)?;
    if token != expected || files.len() != usize::from(file) {
        return Err(invalid("incompatible namespace supervisor handshake"));
    }
    Ok(files.pop())
}

/// A fixed-size namespace-local child identity from the trusted init. Each byte
/// uses the existing descriptor-aware framing; unexpected handles are rejected.
pub(super) fn send_pid(stream: &UnixStream, pid: u32) -> io::Result<()> {
    for byte in pid.to_le_bytes() {
        send(stream, byte, None)?;
    }
    Ok(())
}

pub(super) fn receive_pid(
    stream: &UnixStream,
    cancel: Option<&UnixStream>,
    deadline: Instant,
) -> io::Result<u32> {
    let mut bytes = [0; 4];
    for byte in &mut bytes {
        wait_fd(stream, cancel, deadline)?;
        if !receive_first_byte(stream, byte)?.is_empty() {
            return Err(invalid("unexpected handle in namespace worker identity"));
        }
    }
    let pid = u32::from_le_bytes(bytes);
    if pid <= 1 || pid > i32::MAX as u32 {
        return Err(invalid("invalid namespace worker identity"));
    }
    Ok(pid)
}

#[cfg(test)]
mod identity_tests {
    use super::*;
    use std::time::Duration;

    fn read(stream: &UnixStream) -> io::Result<u32> {
        receive_pid(stream, None, Instant::now() + Duration::from_millis(50))
    }

    #[test]
    fn worker_identity_round_trips_exact_namespace_pid() {
        for pid in [2, 12345, i32::MAX as u32] {
            let (sender, receiver) = UnixStream::pair().unwrap();
            send_pid(&sender, pid).unwrap();
            assert_eq!(read(&receiver).unwrap(), pid);
        }
    }

    #[test]
    fn reserved_and_overflow_worker_identities_fail() {
        for pid in [0, 1, i32::MAX as u32 + 1, u32::MAX] {
            let (sender, receiver) = UnixStream::pair().unwrap();
            send_pid(&sender, pid).unwrap();
            assert_eq!(
                read(&receiver).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[test]
    fn unexpected_identity_handle_fails() {
        let (sender, receiver) = UnixStream::pair().unwrap();
        send(&sender, 2, Some(&File::open("/dev/null").unwrap())).unwrap();
        assert_eq!(
            read(&receiver).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn truncated_identity_fails() {
        let (sender, receiver) = UnixStream::pair().unwrap();
        send(&sender, 2, None).unwrap();
        drop(sender);
        assert!(read(&receiver).is_err());
    }

    #[test]
    fn stalled_identity_obeys_startup_deadline() {
        let (sender, receiver) = UnixStream::pair().unwrap();
        send(&sender, 2, None).unwrap();
        assert_eq!(read(&receiver).unwrap_err().kind(), io::ErrorKind::TimedOut);
    }
}

pub(super) fn peer_pid(stream: &UnixStream) -> io::Result<i32> {
    let mut credentials: libc::ucred = unsafe { mem::zeroed() };
    let mut length = mem::size_of_val(&credentials) as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut credentials as *mut libc::ucred).cast(),
            &mut length,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if length as usize != mem::size_of_val(&credentials) || credentials.pid <= 0 {
        return Err(invalid("invalid guardian credentials"));
    }
    Ok(credentials.pid)
}

pub(super) fn verify_init(stream: &UnixStream, root: &File, launcher: u32) -> io::Result<()> {
    let pid = peer_pid(stream)?;
    let info = std::fs::read_to_string(format!("/proc/self/fdinfo/{}", root.as_raw_fd()))?;
    let fd_pid = info
        .lines()
        .find_map(|line| line.strip_prefix("Pid:"))
        .and_then(|value| value.trim().parse::<i32>().ok());
    if fd_pid != Some(pid) {
        return Err(invalid("namespace init pidfd does not identify its sender"));
    }
    // The trusted launcher has exactly one direct child, blocked before worker
    // spawn. Authenticate it before adopting any authority to signal its pidfd.
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
    let parent = status
        .lines()
        .find_map(|line| line.strip_prefix("PPid:"))
        .and_then(|value| value.trim().parse::<u32>().ok());
    let namespace_pid = status
        .lines()
        .find_map(|line| line.strip_prefix("NSpid:"))
        .and_then(|value| value.split_whitespace().last());
    if parent != Some(launcher) || namespace_pid != Some("1") {
        return Err(invalid(
            "namespace init is not the launcher's namespace root",
        ));
    }
    validate_pidfd(root)
}

pub(super) fn self_pidfd() -> io::Result<File> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, libc::getpid(), 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd as i32) })
}
fn signal_pidfd(fd: &File, signal: i32) -> io::Result<()> {
    if unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            fd.as_raw_fd(),
            signal,
            std::ptr::null::<libc::siginfo_t>(),
            0,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
pub(super) fn validate_pidfd(fd: &File) -> io::Result<()> {
    signal_pidfd(fd, 0)
}
pub(super) fn kill_pidfd(fd: &File) -> io::Result<()> {
    match signal_pidfd(fd, libc::SIGKILL) {
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(()),
        result => result,
    }
}

/// Only called in the dedicated helper, after forwarding stdin into a child.
pub(super) unsafe fn close_protocol_stdin() {
    unsafe {
        libc::close(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn lifecycle_frames_reject_missing_handles_wrong_tokens_and_closed_lifelines() {
        let deadline = Instant::now() + Duration::from_secs(1);
        let (left, right) = UnixStream::pair().unwrap();
        send(&left, 2, None).unwrap();
        assert!(receive(&right, 2, true, None, deadline).is_err());
        send(&left, 3, None).unwrap();
        assert!(receive(&right, 2, false, None, deadline).is_err());
        let (life, other) = UnixStream::pair().unwrap();
        drop(other);
        assert_eq!(
            wait_fd(&right, Some(&life), deadline).unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(
            wait_fd(&right, None, Instant::now() + Duration::from_millis(1))
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
    }

    #[test]
    fn regular_files_and_non_init_peers_cannot_supply_process_death_authority() {
        let ordinary = tempfile::tempfile().unwrap();
        assert!(validate_pidfd(&ordinary).is_err());
        let (left, _right) = UnixStream::pair().unwrap();
        let own_pidfd = self_pidfd().unwrap();
        // Same-process socket and valid pidfd still cannot impersonate the
        // launcher's namespace init. Rejection must not signal the candidate.
        assert!(verify_init(&left, &own_pidfd, std::process::id()).is_err());
        validate_pidfd(&own_pidfd).unwrap();
    }
}
