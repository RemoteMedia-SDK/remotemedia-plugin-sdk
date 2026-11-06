//! Private broker-to-worker handle transport. Payload storage is never placed in
//! these bounded control frames. Session/lease validation belongs to the typed
//! worker protocol above this layer; endpoint possession identifies the child.
#![deny(unsafe_op_in_unsafe_fn)]
use std::{
    fs::File,
    io,
    process::{Child, Command, ExitStatus},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[cfg(target_os = "linux")]
#[path = "channel/linux.rs"]
mod platform;
#[cfg(windows)]
#[path = "channel/windows.rs"]
mod platform;

#[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
pub use platform::namespace::{run_namespace_supervisor, NamespaceCommand};

const HEADER_BYTES: usize = 28;
pub const MAX_CONTROL_BYTES: usize = 1024 * 1024;
pub const CHANNEL_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleAccess {
    Read,
    ReadWrite,
}

pub struct Frame {
    pub metadata: Vec<u8>,
    pub file: Option<File>,
    pub access: Option<HandleAccess>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Broker,
    Worker,
}

/// A single reader/writer pair. Methods require exclusive access so frames and
/// attached handles cannot interleave. Higher layers may put this on an I/O task.
pub struct Channel {
    endpoint: platform::Endpoint,
    side: Side,
    sent: u64,
    received: u64,
    failed: Arc<AtomicBool>,
}

impl Channel {
    fn new(endpoint: platform::Endpoint, side: Side) -> Self {
        Self {
            endpoint,
            side,
            sent: 1,
            received: 1,
            failed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Claim the channel installed by `WorkerProcess::spawn`. On Windows stdin
    /// and stdout are protocol pipes; logs must go to stderr. Linux uses stdin
    /// as a private Unix socket, leaving stdout available for logs.
    ///
    /// # Safety
    /// Call once, before other threads or Python code use these standard handles.
    /// They must be the private endpoints of the trusted spawning broker. Do not
    /// fork/delegate the channel or granted mappings. This does not authenticate
    /// arbitrary standard input supplied by an unknown process.
    pub unsafe fn from_inherited_stdio() -> io::Result<Self> {
        Ok(Self::new(platform::Endpoint::from_stdio()?, Side::Worker))
    }

    /// Separate control input from output/release callbacks. A blocking receive
    /// never holds the sender lock needed by a final buffer-owner acknowledgement.
    /// On Windows, supervisors terminate the worker on a channel fault to wake
    /// any synchronous pipe read already blocked in another thread.
    pub fn split(self) -> io::Result<(Sender, Receiver)> {
        self.check_open()?;
        let (writer, reader) = self.endpoint.split()?;
        Ok((
            Sender(Channel {
                endpoint: writer,
                side: self.side,
                sent: self.sent,
                received: self.received,
                failed: self.failed.clone(),
            }),
            Receiver(Channel {
                endpoint: reader,
                side: self.side,
                sent: self.sent,
                received: self.received,
                failed: self.failed,
            }),
        ))
    }

    pub fn send(&mut self, metadata: &[u8], file: Option<(&File, HandleAccess)>) -> io::Result<()> {
        self.check_open()?;
        if metadata.len() > MAX_CONTROL_BYTES || (file.is_some() && self.side != Side::Broker) {
            return Err(invalid(
                "oversized control frame or worker attempted a file grant",
            ));
        }
        let next = self
            .sent
            .checked_add(1)
            .ok_or_else(|| invalid("channel sequence exhausted"))?;
        let mut header = [0u8; HEADER_BYTES];
        header[..4].copy_from_slice(b"RMZC");
        header[4..6].copy_from_slice(&CHANNEL_VERSION.to_le_bytes());
        let flag: u16 = match file.map(|(_, access)| access) {
            None => 0,
            Some(HandleAccess::Read) => 1,
            Some(HandleAccess::ReadWrite) => 2,
        };
        header[6..8].copy_from_slice(&flag.to_le_bytes());
        header[8..12].copy_from_slice(&(metadata.len() as u32).to_le_bytes());
        header[12..20].copy_from_slice(&self.sent.to_le_bytes());
        let result = self.endpoint.send(header, metadata, file);
        if result.is_err() {
            self.fail();
        } else {
            self.sent = next;
        }
        result
    }

    pub fn receive(&mut self) -> io::Result<Frame> {
        self.check_open()?;
        let result = self.receive_inner();
        if result.is_err() {
            self.fail();
        }
        result
    }

    fn receive_inner(&mut self) -> io::Result<Frame> {
        let next = self
            .received
            .checked_add(1)
            .ok_or_else(|| invalid("channel sequence exhausted"))?;
        let (header, files) = self.endpoint.receive_header()?;
        let decoded = decode_header(&header, self.received)?;
        // Adopt only broker-granted handles. A worker cannot cause the broker to
        // close or import an arbitrary numeric Windows handle in its own process.
        let file = self.endpoint.receive_file(
            &header,
            files,
            decoded.access,
            self.side == Side::Worker,
        )?;
        let mut metadata = vec![0; decoded.len];
        self.endpoint.read_metadata(&mut metadata)?;
        self.received = next;
        Ok(Frame {
            metadata,
            file,
            access: decoded.access,
        })
    }

    fn check_open(&self) -> io::Result<()> {
        if self.failed.load(Ordering::Acquire) {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "worker channel failed",
            ))
        } else {
            Ok(())
        }
    }
    fn fail(&mut self) {
        self.failed.store(true, Ordering::Release);
        self.endpoint.shutdown();
    }
}

pub struct Sender(Channel);
impl Sender {
    pub fn send(&mut self, metadata: &[u8], file: Option<(&File, HandleAccess)>) -> io::Result<()> {
        self.0.send(metadata, file)
    }
}
impl Drop for Sender {
    fn drop(&mut self) {
        self.0.endpoint.shutdown_write();
    }
}
pub struct Receiver(Channel);
impl Receiver {
    pub fn receive(&mut self) -> io::Result<Frame> {
        self.0.receive()
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        self.0.fail();
    }
}

struct Header {
    len: usize,
    access: Option<HandleAccess>,
}
fn decode_header(header: &[u8; HEADER_BYTES], sequence: u64) -> io::Result<Header> {
    if &header[..4] != b"RMZC"
        || u16::from_le_bytes(header[4..6].try_into().unwrap()) != CHANNEL_VERSION
    {
        return Err(invalid("incompatible worker channel header"));
    }
    if u64::from_le_bytes(header[12..20].try_into().unwrap()) != sequence {
        return Err(invalid("unexpected worker frame sequence"));
    }
    let access = match u16::from_le_bytes(header[6..8].try_into().unwrap()) {
        0 => None,
        1 => Some(HandleAccess::Read),
        2 => Some(HandleAccess::ReadWrite),
        _ => return Err(invalid("unknown worker handle access")),
    };
    let len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
    if len > MAX_CONTROL_BYTES {
        return Err(invalid("worker control frame exceeds limit"));
    }
    Ok(Header { len, access })
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn finish_spawn(
    mut child: Child,
    endpoint: io::Result<platform::Endpoint>,
) -> io::Result<(Child, platform::Endpoint)> {
    match endpoint {
        Ok(endpoint) => Ok((child, endpoint)),
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(error)
        }
    }
}

/// Owns the original OS child handle. Windows also assigns a private kill-on-close
/// job before returning the channel; trusted workers must wait for their first
/// host message before spawning descendants. This is not atomic spawn containment.
/// Plain Linux launches own only the original child; explicit namespaced launches
/// also own a pidfd for their dedicated namespace init. Drop terminates/reaps owned
/// processes but does not reclaim leases; that requires the typed connection's
/// separate process-incarnation/non-delegation contract.
pub struct WorkerProcess {
    child: Child,
    channel: Option<Channel>,
    #[cfg(windows)]
    job: platform::ProcessJob,
    #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
    namespace: Option<platform::namespace::NamespaceLease>,
}
impl WorkerProcess {
    pub fn spawn(command: &mut Command) -> io::Result<Self> {
        #[cfg(windows)]
        let job = platform::ProcessJob::new()?;
        #[cfg(windows)]
        let (child, endpoint) = platform::Endpoint::spawn(command, &job)?;
        #[cfg(not(windows))]
        let (child, endpoint) = platform::Endpoint::spawn(command)?;
        Ok(Self {
            child,
            channel: Some(Channel::new(endpoint, Side::Broker)),
            #[cfg(windows)]
            job,
            #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
            namespace: None,
        })
    }
    /// Launch through the explicitly selected Linux namespace supervisor.
    /// The broker receives an owning init pidfd before the target can execute.
    /// Unsupported namespace/pidfd hosts fail; no uncontained fallback is used.
    #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
    pub fn spawn_namespaced(command: &mut NamespaceCommand) -> io::Result<Self> {
        platform::namespace::spawn(command)
    }
    pub fn channel(&mut self) -> &mut Channel {
        self.channel.as_mut().expect("channel not taken")
    }
    /// Original child PID; for namespaced launches this identifies the guardian,
    /// not the worker inside it. It is not proof of either process's death.
    pub fn id(&self) -> u32 {
        self.child.id()
    }
    /// Move I/O to a dedicated task without transferring process authority.
    pub fn take_channel(&mut self) -> Option<Channel> {
        self.channel.take()
    }
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }
    /// Wait only for the original child (guardian for namespaced launches). Use
    /// terminate to confirm job/namespace death before reclaiming remote leases.
    pub fn wait(&mut self) -> io::Result<ExitStatus> {
        self.child.wait()
    }
    /// Windows success additionally confirms zero active job members; explicit
    /// Linux namespace launches confirm init pidfd readiness. A timeout or query
    /// failure is not evidence that remote buffer owners are gone.
    pub fn terminate(&mut self) -> io::Result<ExitStatus> {
        #[cfg(windows)]
        {
            self.job.terminate(&mut self.child)
        }
        #[cfg(not(windows))]
        {
            #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
            if let Some(namespace) = &self.namespace {
                // Namespace init death is confirmed before guardian status can
                // authorize the typed peer to reclaim its buffer reservations.
                namespace.terminate()?;
            }
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            self.child.kill()?;
            self.child.wait()
        }
    }
}
impl Drop for WorkerProcess {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn framing_rejects_version_sequence_size_and_access_before_allocation() {
        let mut header = [0; HEADER_BYTES];
        header[..4].copy_from_slice(b"RMZC");
        header[4..6].copy_from_slice(&CHANNEL_VERSION.to_le_bytes());
        header[12..20].copy_from_slice(&1u64.to_le_bytes());
        assert!(decode_header(&header, 1).is_ok());
        assert!(decode_header(&header, 2).is_err());
        for (offset, bytes) in [
            (4, vec![2, 0]),
            (6, vec![3, 0]),
            (8, ((MAX_CONTROL_BYTES + 1) as u32).to_le_bytes().to_vec()),
        ] {
            let mut bad = header;
            bad[offset..offset + bytes.len()].copy_from_slice(&bytes);
            assert!(decode_header(&bad, 1).is_err());
        }
    }
}
