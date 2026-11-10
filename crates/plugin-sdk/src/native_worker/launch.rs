//! Preserve command configuration while selecting process ownership explicitly.
use remotemedia_shared_memory::{
    channel::{ProcessResources, WorkerProcess},
    peer::{HostHandle, Limits, PeerConnection, Protocols},
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
        protocols: Protocols,
    ) -> io::Result<(PeerConnection, HostHandle)> {
        // SAFETY: caller accepted the complete executable ownership contract.
        unsafe {
            self.spawn_peer_with_resources(pool, limits, protocols, ProcessResources::default())
        }
    }

    /// # Safety
    /// The complete executable ownership contract of `spawn_peer` applies.
    pub(crate) unsafe fn spawn_peer_with_resources(
        &mut self,
        pool: SharedPool,
        limits: Limits,
        protocols: Protocols,
        resources: ProcessResources,
    ) -> io::Result<(PeerConnection, HostHandle)> {
        let process = match self {
            Self::Direct(command) => WorkerProcess::spawn_with_resources(command, resources)?,
            #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
            Self::Namespace(command) => {
                WorkerProcess::spawn_namespaced_with_resources(command, resources)?
            }
        };
        // SAFETY: caller accepted the complete executable ownership contract.
        unsafe { PeerConnection::from_process_with_protocols(process, pool, limits, protocols) }
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
