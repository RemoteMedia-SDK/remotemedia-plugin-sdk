//! Explicit managed Python placement on the shared mapped-worker protocol.
//! The caller supplies matching wheels and trusted node modules. This does not
//! discover trusted code automatically or make arbitrary Python a safe mapping
//! peer. Factory templates can be bound by the host to separate session pools.
mod launch;
mod provisioning;

use crate::native_worker::{
    input_copy_context, Limits, NativeWorkerNode, SessionWorkerFactory, SharedPool, SnapshotMode,
    WorkerCommand, WorkerInputCopies,
};
use remotemedia_py_env::PythonEnvConfig;
use remotemedia_shared_memory::peer::CallContext;
use remotemedia_traits::{AsyncNodeWrapper, StreamingNode, StreamingNodeFactory};
use remotemedia_types::{Error, Result};
use std::{path::PathBuf, sync::Arc, time::Duration};

/// Exact wheel artifacts installed together in every mapped worker environment.
/// Their contents are hashed for both installation verification and cache identity.
#[derive(Clone, Debug)]
pub struct MappedPythonWheels {
    pub client: PathBuf,
    pub buffers: PathBuf,
}

/// Trusted module and environment inputs for a mapped Python registry entry.
#[derive(Clone, Debug)]
pub struct MappedPythonConfig {
    pub node_type: String,
    pub module_root: PathBuf,
    pub entry_module: String,
    pub requirements: Vec<String>,
    pub wheels: MappedPythonWheels,
    pub environment: PythonEnvConfig,
}

#[derive(Clone)]
pub struct MappedPythonFactory {
    config: MappedPythonConfig,
    pool: Option<SharedPool>,
    input_copies: Option<WorkerInputCopies>,
    limits: Limits,
    startup_timeout: Duration,
    snapshot_inputs: Vec<String>,
    snapshot_outputs: Vec<String>,
    #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
    namespace: Option<crate::native_worker::NamespaceLaunch>,
}

impl MappedPythonFactory {
    /// # Safety
    /// The selected Python runtime, SDK/buffer wheels, node modules, dependencies
    /// and parameter-selected code must all uphold `NativeWorkerNode::spawn`'s
    /// immutable publication, final-unmap and no-delegation contract. Descendant
    /// process containment follows the underlying host's documented platform
    /// limits. Windows assignment precedes the first protocol message.
    pub unsafe fn new(config: MappedPythonConfig, pool: SharedPool) -> Self {
        Self {
            config,
            pool: Some(pool),
            input_copies: None,
            limits: Limits::default(),
            startup_timeout: Duration::from_secs(30),
            snapshot_inputs: vec![],
            snapshot_outputs: vec![],
            #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
            namespace: None,
        }
    }

    /// Register trusted Python code for binding to a separate pool per session.
    /// Calling `create` while unbound fails before environment provisioning.
    ///
    /// # Safety
    /// The runtime, wheels, modules, dependencies and parameter-selected code
    /// must uphold the complete mapping contract documented by [`Self::new`].
    pub unsafe fn for_sessions(config: MappedPythonConfig) -> Self {
        Self {
            config,
            pool: None,
            input_copies: None,
            limits: Limits::default(),
            startup_timeout: Duration::from_secs(30),
            snapshot_inputs: vec![],
            snapshot_outputs: vec![],
            #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
            namespace: None,
        }
    }

    /// Bind a fixed session/node policy scope. Identity is checked before startup.
    pub fn with_input_copies(mut self, copies: WorkerInputCopies) -> Self {
        self.input_copies = Some(copies);
        self
    }

    /// Declare portable snapshot inputs for every instance of this registration.
    /// Matching client and buffer wheels must expose snapshot protocol 1.
    pub fn with_snapshot_inputs(mut self, ports: Vec<String>) -> Result<Self> {
        let unique: std::collections::HashSet<_> = ports.iter().collect();
        if unique.len() != ports.len() || ports.iter().any(|port| port.is_empty()) {
            return Err(Error::ConfigError(
                "snapshot input names must be nonempty and unique".into(),
            ));
        }
        self.snapshot_inputs = ports;
        Ok(self)
    }
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Declare the complete initialized output set; requires snapshot protocol 2.
    pub fn with_snapshot_outputs(mut self, ports: Vec<String>) -> Result<Self> {
        let unique: std::collections::HashSet<_> = ports.iter().collect();
        if unique.len() != ports.len() || ports.iter().any(|port| port.is_empty()) {
            return Err(Error::ConfigError(
                "snapshot output names must be nonempty and unique".into(),
            ));
        }
        self.snapshot_outputs = ports;
        Ok(self)
    }

    pub fn with_startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    /// Select explicit namespace ownership without changing interpreter/wheel
    /// provisioning. Unsupported builds/platforms fail before provisioning; no
    /// plain-child fallback is permitted. Session bindings retain this choice.
    /// # Safety
    /// In addition to new's Python contract, these paths must identify trusted
    /// matching supervisor/unshare executables throughout all factory use.
    pub unsafe fn with_linux_namespace(
        self,
        supervisor: PathBuf,
        unshare: PathBuf,
    ) -> Result<Self> {
        #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
        {
            let mut configured = self;
            configured.namespace = Some(
                crate::native_worker::NamespaceLaunch::new(supervisor, unshare).map_err(|e| {
                    Error::ConfigError(format!("mapped Python namespace configuration: {e}"))
                })?,
            );
            Ok(configured)
        }
        #[cfg(not(all(target_os = "linux", feature = "linux-worker-supervisor")))]
        {
            let _ = (self, supervisor, unshare);
            Err(Error::ConfigError(
                "Linux namespace workers require Linux and the linux-worker-supervisor feature"
                    .into(),
            ))
        }
    }

    fn command(&self, python: &launch::PreparedPython) -> Result<WorkerCommand> {
        #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
        if let Some(namespace) = &self.namespace {
            return python.namespaced_command(namespace, self.startup_timeout);
        }
        Ok(WorkerCommand::Direct(python.command()))
    }
}

impl StreamingNodeFactory for MappedPythonFactory {
    fn output_port_kinds(
        &self,
    ) -> std::collections::HashMap<String, remotemedia_traits::ports::PortKind> {
        self.snapshot_outputs
            .iter()
            .map(|name| (name.clone(), remotemedia_traits::ports::PortKind::Snapshot))
            .collect()
    }
    fn input_port_kinds(
        &self,
    ) -> std::collections::HashMap<String, remotemedia_traits::ports::PortKind> {
        self.snapshot_inputs
            .iter()
            .map(|name| (name.clone(), remotemedia_traits::ports::PortKind::Snapshot))
            .collect()
    }
    fn node_type(&self) -> &str {
        &self.config.node_type
    }
    fn is_multi_output_streaming(&self) -> bool {
        true
    }

    fn create(
        &self,
        node_id: String,
        params: &serde_json::Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>> {
        let pool = self.pool.clone().ok_or_else(|| {
            Error::ConfigError("mapped Python factory requires a session pool".into())
        })?;
        if !params.is_object() {
            return Err(Error::ConfigError(
                "mapped Python node parameters must be an object".into(),
            ));
        }
        let config = self.config.clone();
        let provision_params = params.clone();
        let identity = CallContext {
            node_id,
            session_id,
        };
        let input_copies = input_copy_context(self.input_copies.as_ref(), &identity)?;
        let provision_identity = identity.clone();
        let mode = if !self.snapshot_outputs.is_empty() {
            SnapshotMode::Outputs(self.snapshot_outputs.clone())
        } else if !self.snapshot_inputs.is_empty() {
            SnapshotMode::Inputs
        } else {
            SnapshotMode::Disabled
        };
        let snapshots = mode.protocol();
        // Factories are synchronous and may run inside a host Tokio runtime.
        // Provision on an owned thread rather than nesting/blocking that runtime.
        let python = std::thread::Builder::new()
            .name("rm-mapped-provision".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| Error::Execution(e.to_string()))?;
                runtime.block_on(provisioning::provision(
                    &config,
                    &provision_params,
                    &provision_identity,
                    snapshots,
                ))
            })
            .map_err(|e| Error::Execution(e.to_string()))?
            .join()
            .map_err(|_| Error::Execution("mapped Python provisioning thread panicked".into()))??;

        let module_root = self
            .config
            .module_root
            .canonicalize()
            .map_err(|e| Error::ConfigError(format!("mapped Python module root: {e}")))?;
        let mut worker_command = self.command(&python)?;
        let command = worker_command.command_mut();
        command
            .args([
                "-I",
                "-m",
                "remotemedia_mapped_worker",
                "--node-type",
                &self.config.node_type,
                "--node-id",
                &identity.node_id,
                "--params",
                &params.to_string(),
                "--module-root",
            ])
            .arg(module_root)
            .args(["--register-module", &self.config.entry_module]);
        if let Some(session) = &identity.session_id {
            command.args(["--session-id", session]);
        }
        // SAFETY: the constructor accepts the complete selected Python code's
        // mapping contract. Provisioning verifies the exact installed wheels.
        let peer = unsafe { worker_command.spawn_peer(pool, self.limits, snapshots) }
            .map_err(|e| Error::Execution(format!("mapped Python worker launch: {e}")))?;
        let node = NativeWorkerNode::from_peer(
            peer,
            identity,
            self.limits,
            self.startup_timeout,
            input_copies,
            mode,
        )?;
        if node.description().node_type != self.config.node_type {
            return Err(Error::Execution(
                "mapped Python worker reported an unexpected node type".into(),
            ));
        }
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }
}

impl SessionWorkerFactory for MappedPythonFactory {
    fn bind_pool(&self, pool: SharedPool) -> Arc<dyn StreamingNodeFactory> {
        let mut bound = self.clone();
        bound.pool = Some(pool);
        bound.input_copies = None;
        Arc::new(bound)
    }
    fn bind_session(
        &self,
        pool: SharedPool,
        copies: WorkerInputCopies,
    ) -> Arc<dyn StreamingNodeFactory> {
        let mut bound = self.clone();
        bound.pool = Some(pool);
        bound.input_copies = Some(copies);
        Arc::new(bound)
    }
}
