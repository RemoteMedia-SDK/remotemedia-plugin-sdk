//! Provider contract for workload commands planned by the trusted host.

use std::time::Duration;

#[cfg(target_os = "linux")]
pub mod runsc;

pub const PROVIDER_CONTRACT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkloadTarget {
    LinuxCpu,
    LinuxCuda,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkloadPhase {
    Build,
    Install,
    Serve { loopback_port: u16 },
}

/// Per-workload limits. All values must be nonzero.
#[derive(Clone, Copy, Debug)]
pub struct WorkloadLimits {
    pub cpu_millis: u32,
    pub memory_bytes: u64,
    pub scratch_bytes: u64,
    pub processes: u32,
    pub lifetime: Duration,
}

/// The trusted host plans the command after approving the grant and staging
/// the artifact in a provider reservation. The program runs inside the guest.
#[derive(Clone, Debug)]
pub struct WorkloadRequest {
    pub target: WorkloadTarget,
    pub phase: WorkloadPhase,
    pub limits: WorkloadLimits,
    pub argv: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkloadExit {
    Exited(i32),
    Stopped,
    Deadline,
}

#[derive(Debug, thiserror::Error)]
pub enum SandboxError {
    #[error("unsupported workload target {0:?}")]
    UnsupportedTarget(WorkloadTarget),
    #[error("exclusive device capacity is reserved")]
    DeviceBusy,
    #[error("exclusive device requires reconciliation by its bound provider")]
    DeviceRecoveryRequired,
    #[error("invalid workload configuration: {0}")]
    Invalid(&'static str),
    #[error("sandbox runtime failed during {0}")]
    Runtime(&'static str),
    #[error("sandbox runtime timed out during {0}")]
    Timeout(&'static str),
    #[cfg(target_os = "linux")]
    #[error("sandbox controller {controller} requires {expected}, observed {observed}")]
    ResourceLimit {
        controller: &'static str,
        expected: String,
        observed: String,
    },
    #[error("sandbox cleanup is unconfirmed for {0}")]
    CleanupPending(String),
    #[error("sandbox I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[cfg(target_os = "linux")]
    #[error("sandbox state is invalid: {0}")]
    State(#[from] serde_json::Error),
}
