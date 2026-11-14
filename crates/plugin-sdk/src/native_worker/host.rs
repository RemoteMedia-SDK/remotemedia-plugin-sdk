use super::client::{Call, Client};
use super::*;
use remotemedia_shared_memory::{
    peer::{CallResources, Limits, PeerConnection, PeerEvent, SnapshotInputs},
    SharedPool,
};
use remotemedia_traits::rpc::{RpcEvent, RpcMode};
use remotemedia_traits::{
    AsyncStreamingNode, InitializeContextRead, NodeRuntimeContextRead, PacingNature,
};
use std::{process::Command, time::Duration};

struct Drained {
    outputs: usize,
    source_finished: bool,
}

/// Host node adapter for a native mapped worker. All blocking channel work runs
/// on owned OS threads; async calls only wait for their own bounded output stream.
pub struct NativeWorkerNode {
    client: Arc<Client>,
    identity: CallContext,
    description: Description,
    snapshots: bool,
    snapshot_outputs: Vec<String>,
    output_readers: std::sync::RwLock<SnapshotInputs>,
}

impl NativeWorkerNode {
    /// Spawn, negotiate and describe the child with a bounded startup wait.
    /// This constructor blocks and is suitable for the synchronous node factory.
    ///
    /// # Safety
    /// The selected executable and dependencies must honor `PeerConnection::spawn`'s
    /// immutable-buffer/final-unmap/no-delegation contract. This adapter does not
    /// make an arbitrary executable a safe peer. Windows jobs contain ordinary
    /// descendants created after the first host message. This direct-command
    /// constructor uses original-child ownership on Linux; factories can select
    /// the explicit namespace backend. Atomic Windows assignment remains pending.
    pub unsafe fn spawn(
        command: &mut Command,
        pool: SharedPool,
        identity: CallContext,
        limits: Limits,
        startup_timeout: Duration,
    ) -> Result<Self, Error> {
        let copies = input_copy_context(None, &identity)?;
        // SAFETY: the caller supplies the same worker ownership contract.
        unsafe {
            Self::spawn_with_input_copy_context(
                command,
                pool,
                identity,
                limits,
                startup_timeout,
                copies,
            )
        }
    }

    /// Spawn with an explicit host-input policy and retained accounting ledger.
    /// Worker output publication remains strict. This does not instrument node code.
    ///
    /// # Safety
    /// The complete executable ownership contract of [`Self::spawn`] applies.
    pub unsafe fn spawn_with_input_copy_context(
        command: &mut Command,
        pool: SharedPool,
        identity: CallContext,
        limits: Limits,
        startup_timeout: Duration,
        input_copies: remotemedia_types::CopyContext,
    ) -> Result<Self, Error> {
        // SAFETY: the same executable ownership contract applies.
        unsafe {
            Self::spawn_protocol(
                command,
                pool,
                identity,
                limits,
                startup_timeout,
                input_copies,
                SnapshotMode::Disabled,
            )
        }
    }

    /// Spawn with live portable snapshot reads and explicit input copy policy.
    /// An older worker must reject negotiation rather than omit the bindings.
    ///
    /// # Safety
    /// The complete executable ownership contract of [`Self::spawn`] applies.
    pub unsafe fn spawn_with_snapshots(
        command: &mut Command,
        pool: SharedPool,
        identity: CallContext,
        limits: Limits,
        startup_timeout: Duration,
        input_copies: remotemedia_types::CopyContext,
    ) -> Result<Self, Error> {
        // SAFETY: the same executable ownership contract applies.
        unsafe {
            Self::spawn_protocol(
                command,
                pool,
                identity,
                limits,
                startup_timeout,
                input_copies,
                SnapshotMode::Inputs,
            )
        }
    }

    /// Spawn with live output readers and input snapshot support. Output names
    /// must match the initialized worker's complete portable port set.
    ///
    /// # Safety
    /// The complete executable ownership contract of [`Self::spawn`] applies.
    pub unsafe fn spawn_with_snapshot_outputs(
        command: &mut Command,
        pool: SharedPool,
        identity: CallContext,
        limits: Limits,
        startup_timeout: Duration,
        input_copies: remotemedia_types::CopyContext,
        output_ports: Vec<String>,
    ) -> Result<Self, Error> {
        // SAFETY: the same executable ownership contract applies.
        unsafe {
            Self::spawn_protocol(
                command,
                pool,
                identity,
                limits,
                startup_timeout,
                input_copies,
                SnapshotMode::Outputs(output_ports),
            )
        }
    }

    /// Spawn a worker and negotiate background calls (openspec:
    /// add-source-node-lifecycle): a worker node with a body runs it in one
    /// long-lived call, and reports whether it is self-driven. A worker that
    /// predates the protocol rejects the handshake.
    ///
    /// # Safety
    /// The complete executable ownership contract of [`Self::spawn`] applies.
    pub unsafe fn spawn_with_background(
        command: &mut Command,
        pool: SharedPool,
        identity: CallContext,
        limits: Limits,
        startup_timeout: Duration,
    ) -> Result<Self, Error> {
        let copies = input_copy_context(None, &identity)?;
        let process = remotemedia_shared_memory::channel::WorkerProcess::spawn_with_resources(
            command,
            Default::default(),
        )
        .map_err(transport_error)?;
        // SAFETY: the caller supplies the worker executable's ownership contract.
        let peer = unsafe {
            PeerConnection::from_process_with_protocols(
                process,
                pool,
                limits,
                remotemedia_shared_memory::peer::Protocols {
                    background: true,
                    ..Default::default()
                },
            )
        }
        .map_err(transport_error)?;
        Self::from_peer(
            peer,
            identity,
            limits,
            startup_timeout,
            copies,
            SnapshotMode::Disabled,
        )
    }

    unsafe fn spawn_protocol(
        command: &mut Command,
        pool: SharedPool,
        identity: CallContext,
        limits: Limits,
        startup_timeout: Duration,
        input_copies: remotemedia_types::CopyContext,
        mode: SnapshotMode,
    ) -> Result<Self, Error> {
        // SAFETY: the caller supplies the worker executable's ownership contract.
        let peer = unsafe {
            match &mode {
                SnapshotMode::Outputs(_) => {
                    PeerConnection::spawn_with_snapshot_outputs(command, pool, limits)
                }
                SnapshotMode::Inputs => PeerConnection::spawn_with_snapshots(command, pool, limits),
                SnapshotMode::Disabled => PeerConnection::spawn(command, pool, limits),
            }
        }
        .map_err(transport_error)?;
        Self::from_peer(peer, identity, limits, startup_timeout, input_copies, mode)
    }

    pub(crate) fn from_peer(
        (mut peer, handle): (PeerConnection, remotemedia_shared_memory::peer::HostHandle),
        identity: CallContext,
        limits: Limits,
        startup_timeout: Duration,
        input_copies: remotemedia_types::CopyContext,
        mode: SnapshotMode,
    ) -> Result<Self, Error> {
        let shutdown = handle.clone();
        let startup_identity = identity.clone();
        let snapshots = !matches!(mode, SnapshotMode::Disabled);
        let output_protocol = matches!(mode, SnapshotMode::Outputs(_));
        let snapshot_outputs = match mode {
            SnapshotMode::Outputs(names) => names,
            _ => vec![],
        };
        if output_protocol
            && (snapshot_outputs.is_empty()
                || snapshot_outputs.len() > limits.inputs_per_call
                || snapshot_outputs.iter().any(|name| name.is_empty())
                || snapshot_outputs
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != snapshot_outputs.len())
        {
            return Err(Error::ConfigError(
                "invalid snapshot output declarations".into(),
            ));
        }
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("rm-native-host-startup".into())
            .spawn(move || {
                let result =
                    negotiate_description(&mut peer, &handle, startup_identity, output_protocol)
                        .and_then(|description| {
                            Client::new(peer, handle, limits, input_copies)
                                .map(|client| (client, description))
                        });
                let _ = send.send(result);
            })
            .map_err(transport_error)?;
        let result = receive.recv_timeout(startup_timeout);
        match result {
            Ok(Ok((client, description))) => Ok(Self {
                client: Arc::new(client),
                identity,
                description,
                snapshots,
                snapshot_outputs,
                output_readers: std::sync::RwLock::new(HashMap::new()),
            }),
            Ok(Err(error)) => {
                shutdown.close();
                Err(error)
            }
            Err(error) => {
                shutdown.close();
                Err(transport_error(format!("startup failed: {error}")))
            }
        }
    }
    pub fn description(&self) -> &Description {
        &self.description
    }
    fn validate_identity(&self, session_id: &str, node_id: &str) -> Result<(), Error> {
        if self.identity.session_id.as_deref().unwrap_or("") == session_id
            && self.identity.node_id == node_id
        {
            Ok(())
        } else {
            Err(Error::Execution(
                "native worker session/node identity mismatch".into(),
            ))
        }
    }
    fn validate_context(&self, ctx: &dyn NodeRuntimeContextRead) -> Result<(), Error> {
        self.validate_identity(ctx.session_id(), ctx.node_id())
    }
    fn validate_session(&self, session: Option<&str>) -> Result<(), Error> {
        self.validate_identity(
            session.unwrap_or(self.identity.session_id.as_deref().unwrap_or("")),
            &self.identity.node_id,
        )
    }
    async fn begin(
        &self,
        kind: CallKind,
        arguments: serde_json::Value,
        inputs: Vec<(String, RuntimeData)>,
        resources: CallResources,
    ) -> Result<Call, Error> {
        self.client
            .call(kind, self.identity.clone(), arguments, inputs, resources)
            .await
    }
    async fn drain<F>(
        &self,
        kind: CallKind,
        arguments: serde_json::Value,
        inputs: Vec<(String, RuntimeData)>,
        resources: CallResources,
        mut callback: F,
    ) -> Result<Drained, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let mut call = self.begin(kind, arguments, inputs, resources).await?;
        let mut outputs = 0;
        while let Some(output) = call.next().await? {
            callback(output.into_data())?;
            outputs += 1;
        }
        Ok(Drained {
            outputs,
            source_finished: call.source_finished,
        })
    }
    async fn one(
        &self,
        kind: CallKind,
        inputs: Vec<(String, RuntimeData)>,
        resources: CallResources,
    ) -> Result<RuntimeData, Error> {
        let mut result = None;
        self.drain(kind, serde_json::Value::Null, inputs, resources, |data| {
            if result.is_some() {
                return Err(Error::Execution(
                    "native unary call emitted multiple outputs".into(),
                ));
            }
            result = Some(data);
            Ok(())
        })
        .await?;
        result.ok_or_else(|| Error::Execution("native unary call emitted no output".into()))
    }
}

#[async_trait::async_trait]
impl AsyncStreamingNode for NativeWorkerNode {
    fn validate_snapshot_input(
        &self,
        port: &str,
        reader: &dyn remotemedia_traits::ports::SnapshotPort,
    ) -> Result<(), Error> {
        if !self.snapshots || !reader.supports_runtime_data() {
            return Err(Error::Execution(format!("native worker snapshot input {port:?} requires negotiated portable RuntimeData snapshots")));
        }
        Ok(())
    }
    fn snapshot_outputs(&self) -> SnapshotInputs {
        self.output_readers.read().unwrap().clone()
    }
    fn node_type(&self) -> &str {
        &self.description.node_type
    }
    fn is_multi_input(&self) -> bool {
        self.description.multi_input
    }
    fn pacing_nature(&self) -> PacingNature {
        match self.description.pacing {
            Pacing::Reactive => PacingNature::Reactive,
            Pacing::ClockedToOutboundMedia => PacingNature::ClockedToOutboundMedia,
            Pacing::SourceWall(hz) => PacingNature::SourceWall(hz),
        }
    }
    fn clocked_media_addr(&self) -> Option<(String, String)> {
        self.description.clocked_media_addr.clone()
    }
    fn fallback_rate_hz(&self) -> u32 {
        self.description.fallback_rate_hz
    }
    fn rpc_methods(&self) -> Vec<RpcMethod> {
        self.description.rpc_methods.clone()
    }
    fn self_driven(&self) -> Option<bool> {
        self.description.self_driven
    }
    fn has_background(&self) -> bool {
        self.description.background
    }
    /// The worker node's body: one long-lived call. Its outputs belong to no
    /// input and go to the instance's emitter; it completes when the body
    /// returns, and an error completion is a failure of the node. Dropping
    /// this future cancels the call.
    async fn background(&self, ctx: &dyn NodeRuntimeContextRead) -> Result<(), Error> {
        self.validate_identity(ctx.session_id(), ctx.node_id())?;
        let emitter = ctx.emit_handle().ok_or_else(|| {
            Error::Execution("this host provides no emitter for a worker node's body".into())
        })?;
        let mut call = self
            .begin(
                CallKind::Background,
                serde_json::Value::Null,
                vec![],
                call_resources(ctx),
            )
            .await?;
        while let Some(output) = call.next().await? {
            emitter.emit(output.into_data()).await?;
        }
        Ok(())
    }
    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        self.validate_identity(ctx.session_id(), ctx.node_id())?;
        self.drain(
            CallKind::Initialize,
            serde_json::Value::Null,
            vec![],
            CallResources::default(),
            |data| {
                let RuntimeData::Json(value) = data else {
                    return Err(Error::Execution("invalid initialization progress".into()));
                };
                let status = value["status"]
                    .as_str()
                    .ok_or_else(|| Error::Execution("missing progress status".into()))?;
                let message = value["message"]
                    .as_str()
                    .ok_or_else(|| Error::Execution("missing progress message".into()))?;
                ctx.emit_progress(status, message);
                Ok(())
            },
        )
        .await?;
        if !self.snapshot_outputs.is_empty() {
            let readers =
                super::snapshots::discover(&self.client, &self.identity, &self.snapshot_outputs)
                    .await?;
            *self.output_readers.write().unwrap() = readers;
        }
        Ok(())
    }
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        self.one(
            CallKind::Process,
            vec![("input".into(), data)],
            CallResources::default(),
        )
        .await
    }
    async fn process_with_ctx(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        self.validate_context(ctx)?;
        self.one(
            CallKind::Process,
            vec![("input".into(), data)],
            call_resources(ctx),
        )
        .await
    }
    async fn process_multi(
        &self,
        inputs: HashMap<String, RuntimeData>,
    ) -> Result<RuntimeData, Error> {
        self.one(
            CallKind::MultiInput,
            inputs.into_iter().collect(),
            CallResources::default(),
        )
        .await
    }
    async fn process_multi_with_ctx(
        &self,
        inputs: HashMap<String, RuntimeData>,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        self.validate_context(ctx)?;
        self.one(
            CallKind::MultiInput,
            inputs.into_iter().collect(),
            call_resources(ctx),
        )
        .await
    }
    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        session: Option<String>,
        callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        self.validate_session(session.as_deref())?;
        self.drain(
            CallKind::Stream,
            serde_json::Value::Null,
            vec![("input".into(), data)],
            CallResources::default(),
            callback,
        )
        .await
        .map(|drained| drained.outputs)
    }
    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        self.validate_context(ctx)?;
        self.drain(
            CallKind::Stream,
            serde_json::Value::Null,
            vec![("input".into(), data)],
            call_resources(ctx),
            callback,
        )
        .await
        .map(|drained| drained.outputs)
    }
    async fn tick(
        &self,
        tick: Tick,
        session: Option<String>,
        callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        self.validate_session(session.as_deref())?;
        let finished = self
            .drain(
                CallKind::Tick,
                serde_json::to_value(TickRequest::from(tick)).map_err(transport_error)?,
                vec![],
                CallResources::default(),
                callback,
            )
            .await?;
        if finished.source_finished {
            return Err(Error::Execution(
                "source completion requires an active host tick context".into(),
            ));
        }
        Ok(())
    }
    async fn tick_with_ctx(
        &self,
        tick: Tick,
        ctx: &dyn NodeRuntimeContextRead,
        callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        self.validate_context(ctx)?;
        let finished = self
            .drain(
                CallKind::Tick,
                serde_json::to_value(TickRequest::from(tick)).map_err(transport_error)?,
                vec![],
                call_resources(ctx),
                callback,
            )
            .await?;
        if finished.source_finished {
            ctx.finish_source()?;
        }
        Ok(())
    }
    async fn process_control_message(
        &self,
        data: RuntimeData,
        session: Option<String>,
    ) -> Result<bool, Error> {
        self.validate_session(session.as_deref())?;
        match self
            .one(
                CallKind::Control,
                vec![("input".into(), data)],
                CallResources::default(),
            )
            .await?
        {
            RuntimeData::Json(serde_json::Value::Bool(handled)) => Ok(handled),
            _ => Err(Error::Execution("invalid native control result".into())),
        }
    }
    async fn process_control_with_ctx(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<bool, Error> {
        self.validate_context(ctx)?;
        match self
            .one(
                CallKind::Control,
                vec![("input".into(), data)],
                call_resources(ctx),
            )
            .await?
        {
            RuntimeData::Json(serde_json::Value::Bool(handled)) => Ok(handled),
            _ => Err(Error::Execution("invalid native control result".into())),
        }
    }
    async fn call_rpc(
        &self,
        request: RpcRequest,
        ctx: &dyn NodeRuntimeContextRead,
        replies: &mut RpcResponder,
    ) -> Result<(), Error> {
        self.validate_context(ctx)?;
        let mode = self
            .description
            .rpc_methods
            .iter()
            .find(|method| method.name == request.method)
            .ok_or_else(|| Error::Execution(format!("unknown RPC method: {}", request.method)))?
            .mode;
        let mut call = self
            .begin(
                CallKind::Rpc,
                serde_json::to_value(&request).map_err(transport_error)?,
                vec![],
                call_resources(ctx),
            )
            .await?;
        let mut terminal = false;
        while let Some(output) = call.next().await? {
            let RuntimeData::Json(value) = output.into_data() else {
                return Err(Error::Execution("invalid native RPC reply payload".into()));
            };
            let reply: RpcReply = serde_json::from_value(value).map_err(transport_error)?;
            if reply.corr_id != request.corr_id || terminal {
                return Err(Error::Execution(
                    "invalid native RPC correlation or terminal sequence".into(),
                ));
            }
            match reply.event {
                RpcEvent::Value(value) if mode != RpcMode::FireAndForget => {
                    replies.value(value).await?;
                    terminal = mode == RpcMode::Reply;
                }
                RpcEvent::End if mode == RpcMode::Stream => terminal = true,
                RpcEvent::Error(error) if mode != RpcMode::FireAndForget => {
                    return Err(Error::Execution(error))
                }
                _ => return Err(Error::Execution("unexpected native RPC event".into())),
            }
        }
        if mode != RpcMode::FireAndForget && !terminal {
            return Err(Error::Execution("native RPC omitted terminal reply".into()));
        }
        Ok(())
    }
}

/// Complete capability negotiation before accepting any execution calls.
fn negotiate_description(
    peer: &mut PeerConnection,
    handle: &remotemedia_shared_memory::peer::HostHandle,
    identity: CallContext,
    output_protocol: bool,
) -> Result<Description, Error> {
    let PeerEvent::Ready { modes } = peer.next_event().map_err(transport_error)? else {
        return Err(Error::Execution("native worker did not handshake".into()));
    };
    if output_protocol && !modes.contains(&CallKind::Snapshot) {
        return Err(Error::Execution(
            "worker lacks snapshot output operations".into(),
        ));
    }
    for kind in [
        CallKind::Describe,
        CallKind::Initialize,
        CallKind::Process,
        CallKind::MultiInput,
        CallKind::Stream,
        CallKind::Tick,
        CallKind::Control,
        CallKind::Rpc,
    ] {
        if !modes.contains(&kind) {
            return Err(Error::Execution(format!("native worker lacks {kind:?}")));
        }
    }
    let describe = handle
        .start_call(
            CallKind::Describe,
            identity,
            serde_json::Value::Null,
            vec![],
        )
        .map_err(transport_error)?;
    let mut description = None;
    loop {
        match peer.next_event().map_err(transport_error)? {
            PeerEvent::Output(output) if output.call() == describe && description.is_none() => {
                let RuntimeData::Json(value) = output.into_data() else {
                    return Err(Error::Execution("invalid node description payload".into()));
                };
                description =
                    Some(serde_json::from_value::<Description>(value).map_err(transport_error)?);
            }
            PeerEvent::Complete { call, result, .. } if call == describe => {
                result.map_err(Error::Execution)?;
                break;
            }
            _ => return Err(Error::Execution("invalid node description sequence".into())),
        }
    }
    let mut description =
        description.ok_or_else(|| Error::Execution("missing node description".into()))?;
    // No background call on a worker that did not announce the operation.
    if !modes.contains(&CallKind::Background) {
        description.background = false;
    }
    validate_description(&description)?;
    Ok(description)
}

fn validate_description(description: &Description) -> Result<(), Error> {
    let names: std::collections::HashSet<_> = description
        .rpc_methods
        .iter()
        .map(|method| &method.name)
        .collect();
    if description.node_type.is_empty()
        || description.fallback_rate_hz == 0
        || matches!(description.pacing, Pacing::SourceWall(0))
        || names.len() != description.rpc_methods.len()
        || names.iter().any(|name| name.is_empty())
    {
        return Err(Error::Execution("invalid native node description".into()));
    }
    Ok(())
}

pub(crate) enum SnapshotMode {
    Disabled,
    Inputs,
    Outputs(Vec<String>),
}
impl SnapshotMode {
    pub(crate) fn protocol(&self) -> u16 {
        match self {
            Self::Disabled => 0,
            Self::Inputs => 1,
            Self::Outputs(_) => 2,
        }
    }
}
impl Drop for NativeWorkerNode {
    fn drop(&mut self) {
        // An in-flight output reader may hold a temporary strong client handle.
        // Node teardown must still close its process and unblock that read.
        self.client.close();
    }
}

fn call_resources(ctx: &dyn NodeRuntimeContextRead) -> CallResources {
    CallResources {
        snapshots: ctx.snapshot_inputs(),
        output_allocator: ctx.output_allocator(),
        output_copies: ctx.copy_context(),
        ..Default::default()
    }
}
