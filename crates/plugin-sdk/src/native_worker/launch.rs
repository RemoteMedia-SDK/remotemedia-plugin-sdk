//! Preserve command configuration while selecting process ownership explicitly.
use remotemedia_shared_memory::{
    peer::{HostHandle, Limits, PeerConnection},
    SharedPool,
};
use std::{io, process::Command};

pub(crate) enum WorkerCommand {
    Direct(Command),
    #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
    Namespace(remotemedia_shared_memory::channel::NamespaceCommand),
}
impl WorkerCommand {
    pub(crate) fn command_mut(&mut self) -> &mut Command {
        match self {
            Self::Direct(command) => command,
            #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
            Self::Namespace(command) => command.command_mut(),
        }
    }

    /// # Safety
    /// The trusted command and its dependencies must honor PeerConnection's
    /// immutable publication/non-delegation contract for all parameterized code.
    pub(crate) unsafe fn spawn_peer(
        &mut self,
        pool: SharedPool,
        limits: Limits,
        snapshot_protocol: u16,
    ) -> io::Result<(PeerConnection, HostHandle)> {
        // SAFETY: caller accepted the complete executable ownership contract.
        unsafe {
            match self {
                Self::Direct(command) => match snapshot_protocol {
                    0 => PeerConnection::spawn(command, pool, limits),
                    1 => PeerConnection::spawn_with_snapshots(command, pool, limits),
                    2 => PeerConnection::spawn_with_snapshot_outputs(command, pool, limits),
                    _ => Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "unsupported snapshot protocol",
                    )),
                },
                #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
                Self::Namespace(command) => {
                    PeerConnection::spawn_namespaced(command, pool, limits, snapshot_protocol)
                }
            }
        }
    }
}

#[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
#[derive(Clone)]
pub(crate) struct NamespaceLaunch {
    pub(crate) supervisor: std::path::PathBuf,
    pub(crate) unshare: std::path::PathBuf,
}
#[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
impl NamespaceLaunch {
    pub(crate) fn new(
        supervisor: std::path::PathBuf,
        unshare: std::path::PathBuf,
    ) -> io::Result<Self> {
        Ok(Self {
            supervisor: supervisor.canonicalize()?,
            unshare: unshare.canonicalize()?,
        })
    }
}
