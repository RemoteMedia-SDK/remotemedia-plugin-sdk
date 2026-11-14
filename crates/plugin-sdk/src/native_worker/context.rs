use super::*;
use remotemedia_traits::{InitializeContextRead, MediaCapabilities, NodeRuntimeContextRead};
use remotemedia_types::{BufferAllocationError, BufferLoan, OutputAllocator};
use std::{any::Any, sync::Mutex};

/// The node's emitter in a worker process: frames go out on the output lane
/// of the open background call. Attribution is the instance's default; the
/// mapped wire carries no other.
struct BackgroundEmitter(Arc<WorkerRequest>);

impl loadable_node_abi::buffer::NodeEmitter for BackgroundEmitter {
    fn emit(
        &self,
        message: loadable_node_abi::buffer::FfiMessage,
        origin: u8,
        _participant: abi_stable::std_types::RString,
    ) -> async_ffi::FfiFuture<abi_stable::std_types::RResult<(), abi_stable::std_types::RString>>
    {
        use abi_stable::std_types::{RErr, ROk};
        use async_ffi::FutureExt;
        let request = self.0.clone();
        async move {
            if origin != loadable_node_abi::buffer::EMIT_AS_DEFAULT {
                return RErr("a worker-process node emits with the default attribution only".into());
            }
            let data = match loadable_node_abi::runtime_buffer::decode(message) {
                Ok(data) => data,
                Err(error) => return RErr(error),
            };
            // Emitting waits for output credit: off the async executor.
            match tokio::task::spawn_blocking(move || request.emit(data)).await {
                Ok(Ok(())) => ROk(()),
                Ok(Err(error)) => RErr(error.to_string().into()),
                Err(error) => RErr(error.to_string().into()),
            }
        }
        .into_ffi()
    }

    fn try_emit(
        &self,
        _message: loadable_node_abi::buffer::FfiMessage,
        _origin: u8,
        _participant: abi_stable::std_types::RString,
    ) -> loadable_node_abi::buffer::FfiTryEmit {
        loadable_node_abi::buffer::FfiTryEmit {
            status: loadable_node_abi::buffer::TRY_EMIT_REJECTED,
            message: abi_stable::std_types::RNone,
            error: "a worker-process node has no non-waiting emit".into(),
        }
    }

    fn started(&self) -> async_ffi::FfiFuture<()> {
        use async_ffi::FutureExt;
        // The background call is opened when the instance starts.
        async {}.into_ffi()
    }

    fn is_closed(&self) -> bool {
        self.0.is_cancelled()
    }

    fn closed(&self) -> async_ffi::FfiFuture<()> {
        use async_ffi::FutureExt;
        let request = self.0.clone();
        async move {
            while !request.is_cancelled() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
        .into_ffi()
    }

    fn finish(&self) {}
}

/// Clears the session's background emitter when the background call ends.
pub(super) struct OpenBackground<'a>(&'a NodeSession);

impl Drop for OpenBackground<'_> {
    fn drop(&mut self) {
        *self.0.background.lock().unwrap() = None;
    }
}

pub(super) struct NodeSession {
    /// The emitter of the open background call, if one runs.
    background: Mutex<Option<crate::Emitter>>,
    identity: CallContext,
    pub(super) snapshots: remotemedia_shared_memory::peer::SnapshotInputs,
    state: Arc<dyn Any + Send + Sync>,
    capabilities: Arc<Mutex<HashMap<String, MediaCapabilities>>>,
}
impl NodeSession {
    pub(super) fn new(
        identity: CallContext,
        state: Arc<dyn Any + Send + Sync>,
        snapshots: remotemedia_shared_memory::peer::SnapshotInputs,
    ) -> Self {
        Self {
            background: Mutex::new(None),
            identity,
            snapshots,
            state,
            capabilities: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    /// A background call started: its lane carries the node's emitted output
    /// until the returned guard is dropped.
    pub(super) fn open_background(
        &self,
        request: &Arc<WorkerRequest>,
    ) -> Result<OpenBackground<'_>, Error> {
        let mut open = self.background.lock().unwrap();
        if open.is_some() {
            return Err(Error::Execution(
                "the node's background call is already open".into(),
            ));
        }
        *open = Some(crate::Emitter::bound(
            loadable_node_abi::buffer::NodeEmitter_TO::from_value(
                BackgroundEmitter(request.clone()),
                abi_stable::sabi_trait::TD_Opaque,
            ),
        ));
        Ok(OpenBackground(self))
    }
    pub(super) fn validate(&self, identity: &CallContext) -> Result<(), Error> {
        if &self.identity == identity {
            Ok(())
        } else {
            Err(Error::Execution(
                "native worker session/node identity mismatch".into(),
            ))
        }
    }
}

/// Worker-side context exposes direct host allocation and cooperative cancellation.
/// Portable snapshot reads sample the live host publication for this call. Local
/// node state and capability insertions persist for the worker's bound identity;
/// capability-bus forwarding is not yet transported.
pub struct WorkerContext {
    /// The node's emitter while its background call is open.
    emitter: Option<crate::Emitter>,
    request: Arc<WorkerRequest>,
    allocator: Arc<dyn OutputAllocator>,
    state: Arc<dyn Any + Send + Sync>,
    capabilities: Arc<Mutex<HashMap<String, MediaCapabilities>>>,
}
impl WorkerContext {
    pub(super) fn new(request: Arc<WorkerRequest>, session: &NodeSession) -> Self {
        Self {
            emitter: session.background.lock().unwrap().clone(),
            allocator: request.allocator(),
            request,
            state: session.state.clone(),
            capabilities: session.capabilities.clone(),
        }
    }
    pub fn is_cancelled(&self) -> bool {
        self.request.is_cancelled()
    }
}
impl NodeRuntimeContextRead for WorkerContext {
    fn emitter_any(&self) -> Option<&(dyn Any + Send + Sync)> {
        self.emitter
            .as_ref()
            .map(|emitter| emitter as &(dyn Any + Send + Sync))
    }
    fn finish_source(&self) -> Result<(), Error> {
        self.request.finish_source().map_err(transport_error)
    }
    fn session_id(&self) -> &str {
        self.request.context.session_id.as_deref().unwrap_or("")
    }
    fn node_id(&self) -> &str {
        &self.request.context.node_id
    }
    fn output_allocator(&self) -> Option<Arc<dyn OutputAllocator>> {
        Some(self.allocator.clone())
    }
    fn allocate_output_audio(&self, len: usize) -> Result<BufferLoan<f32>, BufferAllocationError> {
        self.allocator.allocate_audio(len)
    }
    fn allocate_output_bytes(&self, len: usize) -> Result<BufferLoan<u8>, BufferAllocationError> {
        self.allocator.allocate_bytes(len)
    }
    fn capability(&self, addr: &str) -> Option<MediaCapabilities> {
        self.capabilities.lock().unwrap().get(addr).cloned()
    }
    fn insert_capability(&self, addr: &str, caps: MediaCapabilities) {
        self.capabilities.lock().unwrap().insert(addr.into(), caps);
    }
    fn snapshot_any(&self, port: &str) -> Option<Arc<dyn Any + Send + Sync>> {
        self.runtime_snapshot(port)
            .ok()
            .flatten()
            .map(|value| value as Arc<dyn Any + Send + Sync>)
    }
    fn runtime_snapshot(
        &self,
        port: &str,
    ) -> Result<Option<Arc<remotemedia_traits::ports::TimestampedSnapshot<RuntimeData>>>, Error>
    {
        self.request.read_snapshot(port).map_err(transport_error)
    }
    fn snapshot_inputs(&self) -> HashMap<String, Arc<dyn remotemedia_traits::ports::SnapshotPort>> {
        self.request
            .snapshot_port_names()
            .into_iter()
            .map(|port| {
                let reader: Arc<dyn remotemedia_traits::ports::SnapshotPort> =
                    Arc::new(RemoteSnapshot {
                        request: self.request.clone(),
                        port: port.clone(),
                    });
                (port, reader)
            })
            .collect()
    }
    fn state_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        Some(self.state.clone())
    }
    fn try_state_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.state_any()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub(super) struct Initialization {
    request: Arc<WorkerRequest>,
    progress_error: Mutex<Option<String>>,
}
impl Initialization {
    pub(super) fn new(request: Arc<WorkerRequest>) -> Self {
        Self {
            request,
            progress_error: Mutex::new(None),
        }
    }
    pub(super) fn check_progress(&self) -> Result<(), Error> {
        match self.progress_error.lock().unwrap().take() {
            None => Ok(()),
            Some(error) => Err(Error::Execution(error)),
        }
    }
}
impl InitializeContextRead for Initialization {
    fn session_id(&self) -> &str {
        self.request.context.session_id.as_deref().unwrap_or("")
    }
    fn node_id(&self) -> &str {
        &self.request.context.node_id
    }
    fn emit_progress(&self, status: &str, message: &str) {
        if let Err(error) = emit_json(
            &self.request,
            serde_json::json!({ "status": status, "message": message }),
        ) {
            *self.progress_error.lock().unwrap() = Some(error.to_string());
        }
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A call-scoped live reader. Already-returned publications outlive the call;
/// further reads fail after completion instead of returning cached stale data.
struct RemoteSnapshot {
    request: Arc<WorkerRequest>,
    port: String,
}
impl remotemedia_traits::ports::SnapshotPort for RemoteSnapshot {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn snapshot_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.runtime_snapshot()
            .ok()
            .flatten()
            .map(|value| value as Arc<dyn Any + Send + Sync>)
    }
    fn supports_runtime_data(&self) -> bool {
        true
    }
    fn runtime_snapshot(
        &self,
    ) -> Result<Option<Arc<remotemedia_traits::ports::TimestampedSnapshot<RuntimeData>>>, Error>
    {
        self.request
            .read_snapshot(&self.port)
            .map_err(transport_error)
    }
}
