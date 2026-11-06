//! Native node execution on the typed, mapped worker transport.
//!
//! A binary constructs its existing `StreamingNode` and calls [`run`]. Bulk
//! inputs retain read leases; context allocation requests initialized storage
//! from the host. Private output is an error, never an implicit copying fallback.
use remotemedia_shared_memory::peer::{CallContext, CallKind, WorkerEndpoint, WorkerRequest};
use remotemedia_traits::rpc::{RpcMethod, RpcReply, RpcReplySink, RpcRequest, RpcResponder};
use remotemedia_traits::{StreamingNode, Tick};
use remotemedia_types::{Error, RuntimeData};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

mod client;
mod context;
mod host;
mod input_copies;
mod launch;
mod snapshots;
pub use host::NativeWorkerNode;
pub(crate) use host::SnapshotMode;
pub(crate) use input_copies::input_copy_context;
pub use input_copies::WorkerInputCopies;
#[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
pub(crate) use launch::NamespaceLaunch;
pub(crate) use launch::WorkerCommand;
mod factory;
pub use context::WorkerContext;
use context::{Initialization, NodeSession};
pub use factory::{NativeWorkerFactory, SessionWorkerFactory};
pub use remotemedia_shared_memory::peer::wire::WIRE_VERSION;
pub use remotemedia_shared_memory::peer::Limits;
pub use remotemedia_shared_memory::{PoolConfig, SessionId, SharedPool, SizeClass};

/// Node properties used by a host to preserve scheduling and RPC discovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Description {
    pub node_type: String,
    pub multi_input: bool,
    pub pacing: Pacing,
    pub clocked_media_addr: Option<(String, String)>,
    pub fallback_rate_hz: u32,
    pub rpc_methods: Vec<RpcMethod>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Pacing {
    Reactive,
    ClockedToOutboundMedia,
    SourceWall(u32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TickRequest {
    pub clock_id: String,
    pub pts_us: u64,
    pub frame_idx: u64,
    pub deadline_us: u64,
}
impl From<TickRequest> for Tick {
    fn from(tick: TickRequest) -> Self {
        Self {
            clock_id: tick.clock_id,
            pts_us: tick.pts_us,
            frame_idx: tick.frame_idx,
            deadline_us: tick.deadline_us,
        }
    }
}
impl From<Tick> for TickRequest {
    fn from(tick: Tick) -> Self {
        Self {
            clock_id: tick.clock_id,
            pts_us: tick.pts_us,
            frame_idx: tick.frame_idx,
            deadline_us: tick.deadline_us,
        }
    }
}

/// Run one native node for this worker process's lifetime. Initialization binds
/// its session/node identity; a different identity is rejected. Calls run on
/// separate OS threads so synchronous output credits and remote allocation do not
/// occupy the Tokio executor needed by concurrent node/control futures.
///
/// # Safety
/// Call once at process startup with the private endpoints of the typed broker.
/// Neither the node nor any dependency may fork/delegate live channel or mappings,
/// write immutable storage, or retain raw writable aliases after freeze. The host
/// supervisor owns process termination; this is not an embedded reusable session.
pub unsafe fn run(node: Arc<dyn StreamingNode>) -> Result<(), Error> {
    // SAFETY: run's caller supplies the same startup and ownership contract.
    let endpoint = unsafe {
        WorkerEndpoint::from_inherited_stdio(vec![
            CallKind::Describe,
            CallKind::Initialize,
            CallKind::Process,
            CallKind::MultiInput,
            CallKind::Stream,
            CallKind::Tick,
            CallKind::Control,
            CallKind::Rpc,
            CallKind::Snapshot,
        ])
    }
    .map_err(transport_error)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("rm-native-worker-runtime")
        .build()
        .map_err(transport_error)?;
    // Reuse OS call threads without sharing capacity with node-owned blocking
    // work on `runtime`. The negotiated budget bounds queued + executing calls;
    // one possible thread per admitted call preserves control/RPC progress while
    // other calls wait for output credit or cancellation. No async tasks run on
    // this dispatcher. It is dropped before `runtime`, whose drivers calls use.
    let dispatcher = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(endpoint.limits().calls)
        .thread_name("rm-native-worker-call")
        .build()
        .map_err(transport_error)?;
    let session = Arc::new(tokio::sync::OnceCell::new());
    loop {
        let mut request = endpoint.next_request().map_err(transport_error)?;
        let inputs = std::mem::take(&mut request.inputs);
        let request = Arc::new(request);
        let execution = request.clone();
        let node = node.clone();
        let session = session.clone();
        let runtime = runtime.handle().clone();
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            dispatcher.spawn_blocking(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    runtime.block_on(execute(node.as_ref(), &session, execution.clone(), inputs))
                }))
                .unwrap_or_else(|_| Err(Error::Execution("native worker node panicked".into())));
                if let Err(error) = execution.complete(result.map_err(|error| error.to_string())) {
                    execution
                        .fail_connection(&format!("native execution could not complete: {error}"));
                }
            })
        }))
        .is_err()
        {
            request
                .complete(Err("cannot dispatch native worker call".into()))
                .map_err(transport_error)?;
        }
    }
}

async fn execute(
    node: &dyn StreamingNode,
    session: &tokio::sync::OnceCell<NodeSession>,
    request: Arc<WorkerRequest>,
    mut inputs: HashMap<String, RuntimeData>,
) -> Result<(), Error> {
    if request.kind == CallKind::Describe {
        require_empty(&inputs)?;
        let pacing = match node.pacing_nature() {
            remotemedia_traits::PacingNature::Reactive => Pacing::Reactive,
            remotemedia_traits::PacingNature::ClockedToOutboundMedia => {
                Pacing::ClockedToOutboundMedia
            }
            remotemedia_traits::PacingNature::SourceWall(hz) => Pacing::SourceWall(hz),
        };
        return emit_json(
            &request,
            Description {
                node_type: node.node_type().into(),
                multi_input: node.is_multi_input(),
                pacing,
                clocked_media_addr: node.clocked_media_addr(),
                fallback_rate_hz: node.fallback_rate_hz(),
                rpc_methods: node.rpc_methods(),
            },
        );
    }
    if request.kind == CallKind::Initialize {
        require_empty(&inputs)?;
        let initialized = session
            .get_or_try_init(|| async {
                let context = Initialization::new(request.clone());
                node.initialize(&context).await?;
                context.check_progress()?;
                let snapshots = if request.supports_snapshot_outputs() {
                    let snapshots = node.snapshot_outputs();
                    if snapshots.len() > request.limits().inputs_per_call
                        || snapshots.iter().any(|(name, reader)| {
                            name.is_empty() || !reader.supports_runtime_data()
                        })
                    {
                        return Err(Error::Execution(
                            "invalid portable worker snapshot outputs".into(),
                        ));
                    }
                    snapshots
                } else {
                    HashMap::new()
                };
                Ok::<_, Error>(NodeSession::new(
                    request.context.clone(),
                    node.make_session_state(&context).into_any_arc(),
                    snapshots,
                ))
            })
            .await?;
        return initialized.validate(&request.context);
    }
    let session = session
        .get()
        .ok_or_else(|| Error::Execution("native worker node is not initialized".into()))?;
    session.validate(&request.context)?;
    if request.is_cancelled() {
        return Err(Error::Execution("native worker call cancelled".into()));
    }
    let ctx = WorkerContext::new(request.clone(), session);
    match request.kind {
        CallKind::Snapshot => {
            require_empty(&inputs)?;
            snapshots::execute(&request, &session.snapshots)
        }
        CallKind::Process => {
            let output = node.process_async(single(&mut inputs)?, &ctx).await?;
            request.emit(output).map_err(transport_error)
        }
        CallKind::MultiInput => {
            let output = node.process_multi_async(inputs, &ctx).await?;
            request.emit(output).map_err(transport_error)
        }
        CallKind::Stream => {
            let sink = request.clone();
            node.process_streaming_async(
                single(&mut inputs)?,
                &ctx,
                Box::new(move |data| sink.emit(data).map_err(transport_error)),
            )
            .await?;
            Ok(())
        }
        CallKind::Tick => {
            require_empty(&inputs)?;
            let tick: TickRequest =
                serde_json::from_value(request.arguments.clone()).map_err(transport_error)?;
            let sink = request.clone();
            node.tick(
                tick.into(),
                &ctx,
                Box::new(move |data| sink.emit(data).map_err(transport_error)),
            )
            .await
        }
        CallKind::Control => {
            let handled = node
                .process_control_message(single(&mut inputs)?, &ctx)
                .await?;
            emit_json(&request, handled)
        }
        CallKind::Rpc => {
            require_empty(&inputs)?;
            let rpc: RpcRequest =
                serde_json::from_value(request.arguments.clone()).map_err(transport_error)?;
            let mode = node
                .rpc_methods()
                .into_iter()
                .find(|method| method.name == rpc.method)
                .ok_or_else(|| Error::Execution(format!("unknown RPC method: {}", rpc.method)))?
                .mode;
            let mut replies = RpcResponder::new(
                Arc::new(ReplySink(request.clone())),
                rpc.corr_id.clone(),
                mode,
            );
            let result = node.call_rpc(rpc, &ctx, &mut replies).await;
            replies.finish(result).await
        }
        CallKind::Describe | CallKind::Initialize => unreachable!(),
    }
}

struct ReplySink(Arc<WorkerRequest>);
#[async_trait::async_trait]
impl RpcReplySink for ReplySink {
    async fn send(&self, reply: RpcReply) -> Result<(), Error> {
        emit_json(&self.0, reply)
    }
}
fn emit_json(request: &WorkerRequest, value: impl Serialize) -> Result<(), Error> {
    request
        .emit(RuntimeData::Json(
            serde_json::to_value(value).map_err(transport_error)?,
        ))
        .map_err(transport_error)
}
fn require_empty(inputs: &HashMap<String, RuntimeData>) -> Result<(), Error> {
    if inputs.is_empty() {
        Ok(())
    } else {
        Err(Error::Execution(
            "operation does not accept media inputs".into(),
        ))
    }
}
fn single(inputs: &mut HashMap<String, RuntimeData>) -> Result<RuntimeData, Error> {
    if inputs.len() != 1 || !inputs.contains_key("input") {
        return Err(Error::Execution(
            "operation requires exactly one named input".into(),
        ));
    }
    Ok(inputs.remove("input").unwrap())
}
fn transport_error(error: impl std::fmt::Display) -> Error {
    Error::Execution(format!("native worker: {error}"))
}
