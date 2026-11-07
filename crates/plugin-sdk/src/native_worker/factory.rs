use super::*;
use remotemedia_traits::{AsyncNodeWrapper, StreamingNodeFactory};
use std::{path::PathBuf, process::Command, time::Duration};

/// Registry factory for mapped native workers. Input is strict by default;
/// explicit compatibility policy may stage private or foreign input with counters.
/// Worker outputs always require mapped publication.
#[derive(Clone)]
pub struct NativeWorkerFactory {
    buffer_declaration: Option<crate::native_worker::contracts::Declaration>,
    binary_path: PathBuf,
    node_type: String,
    pool: Option<SharedPool>,
    input_copies: Option<WorkerInputCopies>,
    output_copies: bool,
    source_completion: bool,
    limits: Limits,
    startup_timeout: Duration,
    snapshot_inputs: Vec<String>,
    snapshot_outputs: Vec<String>,
    #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
    namespace: Option<NamespaceLaunch>,
}
impl NativeWorkerFactory {
    /// # Safety
    /// Every process created from this executable (including code selected by
    /// its manifest parameters) must uphold `NativeWorkerNode::spawn`'s mapping
    /// ownership, startup and non-delegation contract, including its platform limits.
    pub unsafe fn new(binary_path: PathBuf, node_type: String, pool: SharedPool) -> Self {
        Self {
            binary_path,
            node_type,
            pool: Some(pool),
            buffer_declaration: None,
            input_copies: None,
            output_copies: false,
            source_completion: false,
            limits: Limits::default(),
            startup_timeout: Duration::from_secs(10),
            snapshot_inputs: vec![],
            snapshot_outputs: vec![],
            #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
            namespace: None,
        }
    }
    /// Register a trusted executable whose pool is supplied separately for each
    /// session. Calling `create` before binding a pool fails without spawning.
    ///
    /// # Safety
    /// The same complete executable/parameter contract as [`Self::new`] applies.
    pub unsafe fn for_sessions(binary_path: PathBuf, node_type: String) -> Self {
        Self {
            binary_path,
            node_type,
            pool: None,
            buffer_declaration: None,
            input_copies: None,
            output_copies: false,
            source_completion: false,
            limits: Limits::default(),
            startup_timeout: Duration::from_secs(10),
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

    /// Require output-copy protocol 1. Permission still comes from each call's
    /// host context; enabling negotiation never grants permission to copy.
    pub fn with_output_copies(mut self) -> Self {
        self.output_copies = true;
        self
    }

    /// Require the finite-source extension. Old workers reject the handshake;
    /// source completion is applied only through an active host tick context.
    pub fn with_source_completion(mut self) -> Self {
        self.source_completion = true;
        self
    }

    /// Attach a pure declaration covering these exact parameters and every
    /// execution lane. The callback receives the adapter's actual protocol.
    /// None remains unknown. Runtime layout, domain and provenance checks still
    /// apply; declarations do not certify arbitrary node-internal copies.
    pub fn with_buffer_contract(
        mut self,
        declaration: impl Fn(
                &serde_json::Value,
                &remotemedia_traits::buffer_contract::BufferProtocol,
            )
                -> Result<Option<remotemedia_traits::buffer_contract::BufferContract>, Error>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.buffer_declaration = Some(Arc::new(declaration));
        self
    }

    /// Declare portable snapshot inputs for every instance of this registration.
    /// The declaration is static, independent of individual manifest parameters.
    pub fn with_snapshot_inputs(mut self, ports: Vec<String>) -> Result<Self, Error> {
        let unique: std::collections::HashSet<_> = ports.iter().collect();
        if unique.len() != ports.len() || ports.iter().any(|port| port.is_empty()) {
            return Err(Error::ConfigError(
                "snapshot input names must be nonempty and unique".into(),
            ));
        }
        self.snapshot_inputs = ports;
        Ok(self)
    }
    /// Declare the complete portable output snapshot set exposed after initialize.
    pub fn with_snapshot_outputs(mut self, ports: Vec<String>) -> Result<Self, Error> {
        let unique: std::collections::HashSet<_> = ports.iter().collect();
        if unique.len() != ports.len() || ports.iter().any(|port| port.is_empty()) {
            return Err(Error::ConfigError(
                "snapshot output names must be nonempty and unique".into(),
            ));
        }
        self.snapshot_outputs = ports;
        Ok(self)
    }
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }
    pub fn with_startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    /// Select Linux namespace ownership for all instances, including session
    /// bindings. Unsupported platform/build combinations fail here; startup never
    /// falls back to an uncontained child. Matching helper deployment is explicit.
    ///
    /// # Safety
    /// The supervisor and unshare paths must designate trusted matching binaries
    /// throughout use, in addition to the worker contract accepted by new.
    pub unsafe fn with_linux_namespace(
        self,
        supervisor: PathBuf,
        unshare: PathBuf,
    ) -> Result<Self, Error> {
        #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
        {
            let mut configured = self;
            configured.namespace =
                Some(NamespaceLaunch::new(supervisor, unshare).map_err(transport_error)?);
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

    fn command(&self) -> Result<WorkerCommand, Error> {
        #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
        if let Some(namespace) = &self.namespace {
            let mut command = remotemedia_shared_memory::channel::NamespaceCommand::new(
                &namespace.supervisor,
                &namespace.unshare,
                &self.binary_path,
            )
            .map_err(transport_error)?;
            command
                .set_startup_timeout(self.startup_timeout)
                .map_err(transport_error)?;
            return Ok(WorkerCommand::Namespace(command));
        }
        Ok(WorkerCommand::Direct(Command::new(&self.binary_path)))
    }
}
impl StreamingNodeFactory for NativeWorkerFactory {
    fn try_buffer_contract(
        &self,
        params: &serde_json::Value,
    ) -> Result<Option<remotemedia_traits::buffer_contract::BufferContract>, Error> {
        crate::native_worker::contracts::inspect(
            self.buffer_declaration.as_ref(),
            params,
            crate::native_worker::contracts::protocol(
                false,
                &self.snapshot_inputs,
                &self.snapshot_outputs,
            ),
        )
    }

    fn supports_buffer_protocol(
        &self,
        protocol: &remotemedia_traits::buffer_contract::BufferProtocol,
    ) -> bool {
        *protocol
            == crate::native_worker::contracts::protocol(
                false,
                &self.snapshot_inputs,
                &self.snapshot_outputs,
            )
    }
    fn is_multi_output_streaming(&self) -> bool {
        true
    }
    fn input_port_kinds(&self) -> HashMap<String, remotemedia_traits::ports::PortKind> {
        self.snapshot_inputs
            .iter()
            .map(|name| (name.clone(), remotemedia_traits::ports::PortKind::Snapshot))
            .collect()
    }
    fn output_port_kinds(&self) -> HashMap<String, remotemedia_traits::ports::PortKind> {
        self.snapshot_outputs
            .iter()
            .map(|name| (name.clone(), remotemedia_traits::ports::PortKind::Snapshot))
            .collect()
    }
    fn node_type(&self) -> &str {
        &self.node_type
    }
    fn create(
        &self,
        node_id: String,
        params: &serde_json::Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        self.try_buffer_contract(params)?;
        let pool = self.pool.clone().ok_or_else(|| {
            Error::ConfigError("native worker factory requires a session pool".into())
        })?;
        let identity = CallContext {
            session_id,
            node_id,
        };
        let input_copies = input_copy_context(self.input_copies.as_ref(), &identity)?;
        let mut command = self.command()?;
        command
            .command_mut()
            .env("RM_NODE_ID", &identity.node_id)
            .env(
                "RM_SESSION_ID",
                identity.session_id.as_deref().unwrap_or(""),
            )
            .env(
                "RM_NODE_PARAMS",
                serde_json::to_string(params).map_err(transport_error)?,
            );
        // SAFETY: construction fixed the trusted binary and accepted its entire
        // parameterized execution contract. Callers cannot replace the binary.
        let mode = if !self.snapshot_outputs.is_empty() {
            SnapshotMode::Outputs(self.snapshot_outputs.clone())
        } else if self.snapshot_inputs.is_empty() {
            SnapshotMode::Disabled
        } else {
            SnapshotMode::Inputs
        };
        let peer = unsafe {
            command.spawn_peer(
                pool,
                self.limits,
                remotemedia_shared_memory::peer::Protocols {
                    snapshots: mode.protocol(),
                    output_copies: self.output_copies,
                    source_completion: self.source_completion,
                },
            )
        }
        .map_err(transport_error)?;
        let node = NativeWorkerNode::from_peer(
            peer,
            identity,
            self.limits,
            self.startup_timeout,
            input_copies,
            mode,
        )?;
        if node.description().node_type != self.node_type {
            return Err(Error::Execution(
                "native worker reported an unexpected node type".into(),
            ));
        }
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }
}

/// A trusted worker registration that can be bound to a session's allocator.
/// Binding never starts a worker or changes the registered executable/module.
pub trait SessionWorkerFactory: StreamingNodeFactory {
    /// Bind storage with default strict input handling and a fresh per-worker
    /// ledger. Any previously bound session accounting scope is discarded.
    fn bind_pool(&self, pool: SharedPool) -> Arc<dyn StreamingNodeFactory>;
    /// Bind both storage and session-scoped input policy. Implementors must
    /// enforce the supplied scope; there is no policy-ignoring default.
    fn bind_session(
        &self,
        pool: SharedPool,
        copies: WorkerInputCopies,
    ) -> Arc<dyn StreamingNodeFactory>;
}

impl SessionWorkerFactory for NativeWorkerFactory {
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
