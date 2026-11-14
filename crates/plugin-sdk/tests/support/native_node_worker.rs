use remotemedia_plugin_sdk::{
    native_worker::{self, WorkerContext},
    traits::{self, InitializeContextRead, NodeRuntimeContextRead, StreamingNode, Tick},
    types::{Error, PayloadStorage, RuntimeData, TensorEncoding},
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use traits::rpc::{RpcMethod, RpcMode, RpcRequest, RpcResponder};
mod native_worker_profile;
#[cfg(windows)]
mod windows_payload_guard;

fn forward_probe(data: RuntimeData, copy: bool) -> Result<RuntimeData, Error> {
    if copy {
        // Preserve the original owner on return: identity and framework copy
        // ledgers alone cannot detect this deliberate materialization.
        let (_, storage) = data
            .clone()
            .into_shared_parts(TensorEncoding::Runtime)
            .map_err(|e| Error::Execution(e.to_string()))?;
        match storage {
            PayloadStorage::Audio(data) => {
                std::hint::black_box(data.into_vec());
            }
            PayloadStorage::Bytes(data) => {
                std::hint::black_box(data.into_vec());
            }
            PayloadStorage::None => {}
        }
    }
    Ok(data)
}

struct Fixture {
    /// The node's background body: `emit` (three frames, then return),
    /// `fail` (one frame, then an error), `run` (frames until cancelled).
    body: Option<String>,
    self_driven: bool,
    profile: Option<native_worker_profile::Profile>,
    unblock: tokio::sync::Notify,
    gain: f32,
    source: bool,
    finite_source: bool,
    reactive: bool,
    multi_input: bool,
    transform_stream: bool,
    forward: bool,
    probe_copy: bool,
    publish_snapshots: bool,
    unsupported_snapshots: bool,
    snapshot_out: traits::ports::OutputPort<RuntimeData>,
}
#[async_trait::async_trait]
impl StreamingNode for Fixture {
    fn snapshot_outputs(&self) -> HashMap<String, Arc<dyn traits::ports::SnapshotPort>> {
        if self.unsupported_snapshots {
            return HashMap::from([(
                "latest".into(),
                Arc::new(traits::ports::OutputPort::<u32>::empty().input())
                    as Arc<dyn traits::ports::SnapshotPort>,
            )]);
        }
        if self.publish_snapshots {
            HashMap::from([(
                "latest".into(),
                Arc::new(self.snapshot_out.input()) as Arc<dyn traits::ports::SnapshotPort>,
            )])
        } else {
            HashMap::new()
        }
    }
    fn node_type(&self) -> &str {
        "MappedNativeFixture"
    }
    fn self_driven(&self) -> Option<bool> {
        self.self_driven.then_some(true)
    }
    fn has_background(&self) -> bool {
        self.body.is_some()
    }
    async fn background(&self, ctx: &dyn NodeRuntimeContextRead) -> Result<(), Error> {
        let emitter = remotemedia_plugin_sdk::Emitter::for_call(ctx);
        let body = self.body.as_deref().unwrap_or_default();
        let mut n = 0;
        loop {
            emitter.emit(RuntimeData::Text(format!("body-{n}"))).await?;
            n += 1;
            match body {
                "fail" => return Err(Error::Execution("worker body sentinel".into())),
                "emit" if n == 3 => return Ok(()),
                _ => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
            }
        }
    }
    fn is_multi_input(&self) -> bool {
        self.multi_input
    }
    fn pacing_nature(&self) -> traits::PacingNature {
        if self.reactive {
            traits::PacingNature::Reactive
        } else {
            traits::PacingNature::SourceWall(40)
        }
    }
    fn fallback_rate_hz(&self) -> u32 {
        40
    }
    fn make_session_state(
        &self,
        _: &dyn InitializeContextRead,
    ) -> Arc<dyn traits::AnySessionState> {
        Arc::new(AtomicUsize::new(0))
    }
    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        assert_eq!(
            ctx.session_id(),
            std::env::var("RM_SESSION_ID").unwrap_or_else(|_| "native-session".into())
        );
        assert_eq!(
            ctx.node_id(),
            std::env::var("RM_NODE_ID").unwrap_or_else(|_| "native-node".into())
        );
        tokio::spawn(async { tokio::task::yield_now().await })
            .await
            .unwrap();
        ctx.emit_progress("ready", "fixture initialized");
        Ok(())
    }
    async fn process_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        if self.forward {
            return forward_probe(data, self.probe_copy);
        }
        if matches!(&data, RuntimeData::Text(value) if value == "snapshot") {
            return Ok(snapshot(ctx)?.value.clone());
        }
        if matches!(&data, RuntimeData::Text(value) if value == "crash") {
            std::process::exit(29);
        }
        if matches!(&data, RuntimeData::Text(value) if value == "private") {
            return Ok(RuntimeData::Binary(vec![1, 2, 3].into()));
        }
        let started = self.profile.as_ref().map(|_| std::time::Instant::now());
        let mut allocation_ns = 0;
        let mut allocated = None;
        traits::runtime_context::state::<AtomicUsize>(ctx).fetch_add(1, Ordering::SeqCst);
        let (metadata, storage) = data
            .into_shared_parts(TensorEncoding::Runtime)
            .map_err(|e| Error::Execution(e.to_string()))?;
        let output = match storage {
            PayloadStorage::Audio(input) => {
                let allocating = started.map(|_| std::time::Instant::now());
                let mut output = ctx
                    .allocate_output_audio(input.len())
                    .map_err(|e| Error::Execution(e.to_string()))?;
                allocated = allocating.map(|start| {
                    let end = std::time::Instant::now();
                    allocation_ns = end.duration_since(start).as_nanos() as u64;
                    end
                });
                let address = output.as_ptr();
                for (dst, src) in output.iter_mut().zip(input.iter()) {
                    *dst = src * self.gain;
                }
                let output = output.freeze();
                assert_eq!(address, output.as_ptr());
                PayloadStorage::Audio(output)
            }
            PayloadStorage::Bytes(input) => {
                let allocating = started.map(|_| std::time::Instant::now());
                let mut output = ctx
                    .allocate_output_bytes(input.len())
                    .map_err(|e| Error::Execution(e.to_string()))?;
                allocated = allocating.map(|start| {
                    let end = std::time::Instant::now();
                    allocation_ns = end.duration_since(start).as_nanos() as u64;
                    end
                });
                for (dst, src) in output.iter_mut().zip(input.iter()) {
                    *dst = src.wrapping_add(2);
                }
                PayloadStorage::Bytes(output.freeze())
            }
            PayloadStorage::None => PayloadStorage::None,
        };
        let output = metadata
            .attach(output, TensorEncoding::Runtime)
            .map_err(|e| Error::Execution(e.to_string()))?;
        if self.publish_snapshots {
            self.snapshot_out.publish(output.clone(), 987);
        }
        if let (Some(profile), Some(started), Some(allocated)) = (&self.profile, started, allocated)
        {
            let end = std::time::Instant::now();
            profile.record([
                allocation_ns,
                end.duration_since(allocated).as_nanos() as u64,
                end.duration_since(started).as_nanos() as u64,
            ])?;
        }
        Ok(output)
    }
    async fn process_multi_async(
        &self,
        mut inputs: HashMap<String, RuntimeData>,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        if inputs.contains_key("snapshot") {
            return Ok(snapshot(ctx)?.value.clone());
        }
        assert_eq!(inputs.len(), 2);
        assert!(inputs.contains_key("right"));
        let left = inputs.remove("left").unwrap();
        if self.forward {
            // Fan-in forwarding through the multi-input hook; `probe_copy`
            // deliberately materializes the payload first.
            return forward_probe(left, self.probe_copy);
        }
        Ok(left)
    }
    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<usize, Error> {
        if matches!(&data, RuntimeData::Text(value) if value == "emit") {
            // Emit from a call: it goes out on the background call's lane
            // when one is open, and fails otherwise.
            let emitted = remotemedia_plugin_sdk::Emitter::for_call(ctx)
                .emit(RuntimeData::Text("from a call".into()))
                .await;
            callback(RuntimeData::Text(
                if emitted.is_ok() { "emitted" } else { "unsupported" }.into(),
            ))?;
            return Ok(1);
        }
        if matches!(&data, RuntimeData::Text(value) if value == "snapshots") {
            let reader = ctx.snapshot_inputs().remove("latest").unwrap();
            let first = reader.runtime_snapshot()?.unwrap();
            callback(first.value.clone())?;
            // Output credit is returned only after the host callback publishes
            // the replacement. The second read must sample that live publication.
            let second = reader.runtime_snapshot()?.unwrap();
            assert!(second.seq > first.seq);
            assert!(second.pts_us > first.pts_us);
            callback(second.value.clone())?;
            callback(first.value.clone())?;
            return Ok(3);
        }
        if self.transform_stream {
            callback(self.process_async(data, ctx).await?)?;
            return Ok(1);
        }
        if matches!(&data, RuntimeData::Text(value) if value == "escape") {
            let mut loan = ctx.allocate_output_bytes(4).unwrap();
            loan.copy_from_slice(&[5, 6, 7, 8]);
            std::thread::spawn(move || {
                let _ = callback(RuntimeData::Binary(loan.freeze()));
            });
            self.unblock.notified().await;
            // Deliberately violate the execution contract: return while an
            // escaped callback still awaits its host acknowledgement.
            return Ok(1);
        }
        if matches!(&data, RuntimeData::Text(value) if value == "wait") {
            callback(RuntimeData::Text("waiting".into()))?;
            self.unblock.notified().await;
            assert!(ctx
                .as_any()
                .downcast_ref::<WorkerContext>()
                .unwrap()
                .is_cancelled());
            return Ok(1);
        }
        // Multi-output streaming hook; `probe_copy` deliberately materializes
        // the payload before slicing it.
        let data = forward_probe(data, self.probe_copy)?;
        let RuntimeData::Binary(data) = data else {
            return Err(Error::Execution("binary stream input required".into()));
        };
        callback(RuntimeData::Binary(data.slice(1..3).unwrap()))?;
        callback(RuntimeData::Binary(data.slice(3..5).unwrap()))?;
        Ok(2)
    }
    async fn tick(
        &self,
        tick: Tick,
        ctx: &dyn NodeRuntimeContextRead,
        mut cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        if tick.clock_id == "snapshot" {
            return cb(snapshot(ctx)?.value.clone());
        }
        assert_eq!(tick.clock_id, "wall:40");
        if self.finite_source {
            if tick.frame_idx == 0 {
                return Ok(());
            }
            if tick.frame_idx == 3 {
                ctx.finish_source()?;
            }
        }
        if !self.source {
            assert_eq!(tick.frame_idx, 9);
            assert_eq!(tick.deadline_us, 400);
        }
        let mut loan = ctx
            .allocate_output_bytes(8)
            .map_err(|e| Error::Execution(e.to_string()))?;
        loan.copy_from_slice(&tick.pts_us.to_le_bytes());
        let data = RuntimeData::Binary(loan.freeze());
        if self.publish_snapshots {
            self.snapshot_out.publish(data.clone(), tick.pts_us);
        }
        cb(data)
    }
    async fn process_control_message(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<bool, Error> {
        match data {
            RuntimeData::Text(value) if value == "snapshot" => Ok(snapshot(ctx)?.seq > 0),
            RuntimeData::Text(value) if value == "unblock" => {
                let thread = tokio::task::spawn_blocking(|| {
                    std::thread::current().name().unwrap().to_owned()
                })
                .await
                .unwrap();
                assert_eq!(thread, "rm-native-worker-runtime");
                self.unblock.notify_one();
                Ok(true)
            }
            RuntimeData::Json(value) => Ok(value["event"] == "end_of_input"),
            _ => Ok(false),
        }
    }
    fn rpc_methods(&self) -> Vec<RpcMethod> {
        vec![
            RpcMethod::new("state", RpcMode::Reply),
            RpcMethod::new("events", RpcMode::Stream),
            RpcMethod::new("reset", RpcMode::FireAndForget),
        ]
    }
    async fn call_rpc(
        &self,
        req: RpcRequest,
        ctx: &dyn NodeRuntimeContextRead,
        replies: &mut RpcResponder,
    ) -> Result<(), Error> {
        if req.kwargs.contains_key("snapshot") {
            let value = snapshot(ctx)?;
            return replies.value(serde_json::json!({"seq":value.seq,"pts_us":value.pts_us,"written_at_us":value.written_at_us})).await;
        }
        let state = traits::runtime_context::state::<AtomicUsize>(ctx);
        match req.method.as_str() {
            "state" => replies.value(serde_json::json!({"count": state.load(Ordering::SeqCst), "node": ctx.node_id(), "session": ctx.session_id()})).await,
            "events" => { replies.value(11).await?; replies.value(22).await },
            "reset" => { state.store(0, Ordering::SeqCst); Ok(()) },
            _ => unreachable!(),
        }
    }
}

fn snapshot(
    ctx: &dyn NodeRuntimeContextRead,
) -> Result<Arc<traits::ports::TimestampedSnapshot<RuntimeData>>, Error> {
    ctx.runtime_snapshot("latest")?
        .ok_or_else(|| Error::Execution("snapshot unpublished".into()))
}

fn main() -> Result<(), Error> {
    #[cfg(windows)]
    windows_payload_guard::install();
    if std::env::args().any(|arg| arg == "--silent") {
        loop {
            std::thread::park();
        }
    }
    let params: serde_json::Value = std::env::var("RM_NODE_PARAMS")
        .ok()
        .map(|value| serde_json::from_str(&value).unwrap())
        .unwrap_or_default();
    if params["require_namespace"].as_bool() == Some(true) {
        #[cfg(target_os = "linux")]
        {
            let status = std::fs::read_to_string("/proc/self/status")
                .map_err(|e| Error::Execution(e.to_string()))?;
            let pids: Vec<_> = status
                .lines()
                .find_map(|line| line.strip_prefix("NSpid:"))
                .expect("NSpid")
                .split_whitespace()
                .collect();
            assert!(pids.len() > 1 && std::process::id() > 1);
            let parent = status
                .lines()
                .find_map(|line| line.strip_prefix("PPid:"))
                .expect("PPid")
                .trim();
            let parent = std::fs::read_to_string(format!("/proc/{parent}/status")).unwrap();
            assert_eq!(
                parent
                    .lines()
                    .find_map(|line| line.strip_prefix("NSpid:"))
                    .unwrap()
                    .split_whitespace()
                    .last(),
                Some("1")
            );
        }
        #[cfg(not(target_os = "linux"))]
        return Err(Error::Execution("namespace fixture requires Linux".into()));
    }
    // SAFETY: this standalone fixture never forks/delegates or keeps writable
    // aliases. Only the production native runner owns its private stdio channel.
    unsafe {
        native_worker::run(Arc::new(Fixture {
            body: params["body"].as_str().map(str::to_string),
            self_driven: params["self_driven"].as_bool().unwrap_or(false),
            profile: native_worker_profile::Profile::from_env()?,
            unblock: tokio::sync::Notify::new(),
            gain: params["gain"].as_f64().unwrap_or(3.0) as f32,
            source: params["source"].as_bool().unwrap_or(false),
            finite_source: params["finite_source"].as_bool().unwrap_or(false),
            reactive: params["reactive"].as_bool().unwrap_or(false),
            multi_input: params["multi_input"].as_bool().unwrap_or(true),
            transform_stream: params["transform_stream"].as_bool().unwrap_or(false),
            forward: params["forward"].as_bool().unwrap_or(false),
            probe_copy: params["probe_copy"].as_bool().unwrap_or(false),
            publish_snapshots: params["publish_snapshots"].as_bool().unwrap_or(false),
            unsupported_snapshots: params["unsupported_snapshots"].as_bool().unwrap_or(false),
            snapshot_out: traits::ports::OutputPort::empty(),
        }))
    }
}
