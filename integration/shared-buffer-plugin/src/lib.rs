//! Real dynamic-library acceptance fixture. Uses the public author-facing SDK;
//! it does not share Rust static state or cast host types across the DLL boundary.
#[cfg(feature = "rt-allocation-probe")]
mod rt_probe;
use async_trait::async_trait;
use remotemedia_plugin_sdk::snapshot as snapshot_abi;
use remotemedia_plugin_sdk::{
    abi_stable::{
        sabi_trait::TD_Opaque,
        std_types::{RErr, ROk, RResult, RString},
    },
    adapter::StreamingNodeFfiAdapter,
    rpc::{RpcMethod, RpcMode, RpcRequest, RpcResponder},
    traits::{AsyncStreamingNode, NodeRuntimeContextRead, Tick},
    types::{Error, RuntimeData},
    FfiNodeBox, FfiNodeFactory, FfiNode_TO,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

#[derive(Default)]
pub struct Factory;
impl FfiNodeFactory for Factory {
    fn input_ports(
        &self,
    ) -> remotemedia_plugin_sdk::abi_stable::std_types::RVec<snapshot_abi::FfiPortDeclaration> {
        vec![snapshot_abi::FfiPortDeclaration {
            name: "latest".into(),
            kind: snapshot_abi::FfiPortKind::Snapshot,
        }]
        .into()
    }
    fn output_ports(
        &self,
    ) -> remotemedia_plugin_sdk::abi_stable::std_types::RVec<snapshot_abi::FfiPortDeclaration> {
        self.input_ports()
    }
    fn node_type(&self) -> RString {
        "SharedBufferFixture".into()
    }
    fn create(&self, params: RString) -> RResult<FfiNodeBox, RString> {
        let params: serde_json::Value = match serde_json::from_str(&params) {
            Ok(value) => value,
            Err(error) => return RErr(error.to_string().into()),
        };
        let protocol = match params["mode"].as_str() {
            Some("legacy") => Some(0),
            Some("missing_shared") => Some(remotemedia_plugin_sdk::buffer::BUFFER_ABI_VERSION),
            Some("unknown_protocol") => Some(u16::MAX),
            _ => None,
        };
        if let Some(protocol) = protocol {
            return ROk(FfiNode_TO::from_value(
                LegacyEcho(protocol, Mutex::new(None)),
                TD_Opaque,
            ));
        }
        let node = Fixture {
            initialized: AtomicBool::new(false),
            snapshot: remotemedia_plugin_sdk::traits::ports::OutputPort::empty(),
            mode: params["mode"].as_str().unwrap_or("echo").into(),
            held: Mutex::new(Vec::new()),
            cancelled: Arc::new(AtomicUsize::new(0)),
            last_address: AtomicUsize::new(0),
            control: Mutex::new(None),
            continue_stream: tokio::sync::Notify::new(),
        };
        ROk(FfiNode_TO::from_value(
            StreamingNodeFfiAdapter::new(node),
            TD_Opaque,
        ))
    }
}

struct Fixture {
    initialized: AtomicBool,
    snapshot: remotemedia_plugin_sdk::traits::ports::OutputPort<RuntimeData>,
    mode: String,
    held: Mutex<Vec<RuntimeData>>,
    cancelled: Arc<AtomicUsize>,
    last_address: AtomicUsize,
    control: Mutex<Option<(String, String)>>,
    continue_stream: tokio::sync::Notify,
}
// Retain encoded controls without decoding/copying; a tick returns the same
// bytes, letting host tests isolate encode/decode attribution across calls.
struct LegacyEcho(u16, Mutex<Option<abi_stable::std_types::RVec<u8>>>);
impl remotemedia_plugin_sdk::FfiNode for LegacyEcho {
    fn shared_buffer_protocol(&self) -> u16 {
        self.0
    }
    fn node_type(&self) -> RString {
        "SharedBufferFixture".into()
    }
    fn process(
        &self,
        input: abi_stable::std_types::RVec<u8>,
    ) -> remotemedia_plugin_sdk::async_ffi::FfiFuture<
        RResult<abi_stable::std_types::RVec<u8>, RString>,
    > {
        use remotemedia_plugin_sdk::async_ffi::FutureExt;
        async move { ROk(input) }.into_ffi()
    }

    fn process_control_message(
        &self,
        input: abi_stable::std_types::RVec<u8>,
        _session: RString,
    ) -> remotemedia_plugin_sdk::async_ffi::FfiFuture<RResult<bool, RString>> {
        use remotemedia_plugin_sdk::async_ffi::FutureExt;
        *self.1.lock().unwrap() = Some(input);
        async { ROk(true) }.into_ffi()
    }

    fn tick(
        &self,
        _tick: remotemedia_plugin_sdk::FfiTick,
        _session: RString,
        sink: remotemedia_plugin_sdk::OutputSinkBox,
    ) -> remotemedia_plugin_sdk::async_ffi::FfiFuture<RResult<usize, RString>> {
        use remotemedia_plugin_sdk::async_ffi::FutureExt;
        let output = self.1.lock().unwrap().take();
        async move {
            match output {
                Some(bytes) => match sink.push(bytes) {
                    ROk(()) => ROk(1),
                    RErr(error) => RErr(error),
                },
                None => ROk(0),
            }
        }
        .into_ffi()
    }
}
struct Cancelled(Arc<AtomicUsize>);
impl Drop for Cancelled {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
static PRIVATE_RELEASES: AtomicUsize = AtomicUsize::new(0);
struct PrivateOwner(Vec<u8>);
struct AccountedOwner {
    bytes: Vec<u8>,
    copies: remotemedia_plugin_sdk::types::CopyContext,
}
impl AsRef<[u8]> for AccountedOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
impl Drop for AccountedOwner {
    fn drop(&mut self) {
        let _ = self.copies.materialize(
            remotemedia_plugin_sdk::types::CopyReason::VecMaterialization,
            self.bytes.len() as u64,
            || self.bytes.clone(),
        );
    }
}
fn copy_probe(ctx: &dyn NodeRuntimeContextRead, lane: &str) -> Result<RuntimeData, Error> {
    let copies = ctx
        .copy_context()
        .ok_or_else(|| Error::Execution("missing copy context".into()))?;
    let source: remotemedia_plugin_sdk::types::SharedBuffer<u8> = vec![1, 2, 3, 4].into();
    let copied = source.clone().try_into_vec(&copies).is_ok();
    // A stack output slice models a borrowed device destination without opening
    // hardware. This verifies the distinct transfer reason across the real ABI.
    let mut device_output = [0u8; 4];
    let device_copied = copies
        .materialize(
            remotemedia_plugin_sdk::types::CopyReason::DeviceTransfer,
            source.len() as u64,
            || device_output.copy_from_slice(&source),
        )
        .is_ok();
    Ok(RuntimeData::Json(
        serde_json::json!({"copied":copied,"device_copied":device_copied,"device_output":device_output,"lane":lane,"boundary":copies.report().boundary,"session":ctx.session_id(),"node":ctx.node_id()}),
    ))
}
impl AsRef<[u8]> for PrivateOwner {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
impl Drop for PrivateOwner {
    fn drop(&mut self) {
        PRIVATE_RELEASES.fetch_add(1, Ordering::SeqCst);
    }
}

fn metadata(ctx: &dyn NodeRuntimeContextRead, address: usize) -> serde_json::Value {
    serde_json::json!({"session":ctx.session_id(), "node":ctx.node_id(), "producer_address":address})
}

impl Fixture {
    fn gain(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        match data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                stream_id,
                timestamp_us,
                arrival_ts_us,
                ..
            } => {
                let mut loan = ctx
                    .allocate_output_audio(samples.len())
                    .map_err(|e| Error::Execution(e.to_string()))?;
                let address = loan.as_ptr() as usize;
                for (output, input) in loan.iter_mut().zip(samples.iter()) {
                    *output = *input * 0.5;
                }
                self.last_address.store(address, Ordering::SeqCst);
                Ok(RuntimeData::Audio {
                    samples: loan.freeze().into(),
                    sample_rate,
                    channels,
                    stream_id,
                    timestamp_us,
                    arrival_ts_us,
                    metadata: Some(metadata(ctx, address)),
                })
            }
            RuntimeData::Binary(data) => {
                let mut loan = ctx
                    .allocate_output_bytes(data.len())
                    .map_err(|e| Error::Execution(e.to_string()))?;
                self.last_address
                    .store(loan.as_ptr() as usize, Ordering::SeqCst);
                for (out, input) in loan.iter_mut().zip(data.iter()) {
                    *out = input.wrapping_add(1);
                }
                Ok(RuntimeData::Binary(loan.freeze()))
            }
            // Metadata-only frames follow the same stream without sample work.
            other => Ok(other),
        }
    }
}

#[async_trait]
impl AsyncStreamingNode for Fixture {
    async fn initialize(
        &self,
        _: &dyn remotemedia_plugin_sdk::traits::InitializeContextRead,
    ) -> Result<(), Error> {
        self.initialized.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn snapshot_outputs(
        &self,
    ) -> HashMap<String, Arc<dyn remotemedia_plugin_sdk::traits::SnapshotPort>> {
        if self.mode == "snapshot_late" && !self.initialized.load(Ordering::SeqCst) {
            return HashMap::new();
        }
        match self.mode.as_str() {
            "snapshot_publish" | "snapshot_late" => HashMap::from([(
                "latest".into(),
                Arc::new(self.snapshot.input())
                    as Arc<dyn remotemedia_plugin_sdk::traits::SnapshotPort>,
            )]),
            "snapshot_unsupported" => HashMap::from([(
                "latest".into(),
                Arc::new(remotemedia_plugin_sdk::traits::OutputPort::<u32>::empty().input())
                    as Arc<dyn remotemedia_plugin_sdk::traits::SnapshotPort>,
            )]),
            _ => HashMap::new(),
        }
    }
    fn node_type(&self) -> &str {
        "SharedBufferFixture"
    }
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        Ok(data)
    }
    async fn process_with_ctx(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        match self.mode.as_str() {
            "copy_policy" => copy_probe(ctx, "single"),
            "copy_on_drop" => Ok(RuntimeData::Binary(
                remotemedia_plugin_sdk::types::SharedBuffer::from_owner(AccountedOwner {
                    bytes: vec![1, 2, 3, 4],
                    copies: ctx
                        .copy_context()
                        .ok_or_else(|| Error::Execution("missing copy context".into()))?,
                }),
            )),
            "snapshot_publish" | "snapshot_late" => {
                self.snapshot.publish(data.clone(), 987);
                Ok(data)
            }
            "snapshot_read" => ctx
                .runtime_snapshot("latest")?
                .map(|s| s.value.clone())
                .ok_or_else(|| Error::Execution("latest snapshot not published".into())),
            "gain" => self.gain(data, ctx),
            "private" => {
                let buffer =
                    remotemedia_plugin_sdk::types::SharedBuffer::from_owner(PrivateOwner(vec![
                        42;
                        32
                    ]));
                self.last_address
                    .store(buffer.as_ptr() as usize, Ordering::SeqCst);
                Ok(RuntimeData::Binary(buffer))
            }
            "retain" => {
                self.held.lock().unwrap().push(data.clone());
                Ok(data)
            }
            _ => Ok(data),
        }
    }
    fn is_multi_input(&self) -> bool {
        self.mode == "named"
    }
    async fn process_multi_with_ctx(
        &self,
        mut inputs: HashMap<String, RuntimeData>,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        if self.mode == "copy_policy" {
            return copy_probe(ctx, "multi");
        }
        let left = inputs
            .remove("left")
            .ok_or_else(|| Error::Execution("missing left".into()))?;
        let right = inputs
            .remove("right")
            .ok_or_else(|| Error::Execution("missing right".into()))?;
        match (left, right) {
            (
                RuntimeData::Audio {
                    samples: a,
                    sample_rate,
                    channels,
                    ..
                },
                RuntimeData::Audio { samples: b, .. },
            ) if a.len() == b.len() => {
                let mut loan = ctx
                    .allocate_output_audio(a.len())
                    .map_err(|e| Error::Execution(e.to_string()))?;
                let address = loan.as_ptr() as usize;
                for ((output, a), b) in loan.iter_mut().zip(a.iter()).zip(b.iter()) {
                    *output = a + b;
                }
                Ok(RuntimeData::Audio {
                    samples: loan.freeze().into(),
                    sample_rate,
                    channels,
                    stream_id: None,
                    timestamp_us: None,
                    arrival_ts_us: None,
                    metadata: Some(metadata(ctx, address)),
                })
            }
            _ => Err(Error::Execution(
                "named fixture needs equal audio frames".into(),
            )),
        }
    }
    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        if self.mode == "wait" {
            let _cancelled = Cancelled(self.cancelled.clone());
            callback(RuntimeData::Json(
                serde_json::json!({"started":true,"session":ctx.session_id()}),
            ))?;
            return std::future::pending().await;
        }
        if self.mode != "chunks" {
            callback(self.process_with_ctx(data, ctx).await?)?;
            return Ok(1);
        }
        let RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            ..
        } = data
        else {
            return Err(Error::Execution("chunks needs audio".into()));
        };
        let samples = samples.into_shared();
        for index in 0..2 {
            if index == 1 {
                self.continue_stream.notified().await;
            }
            let start = index * samples.len() / 2;
            callback(RuntimeData::Audio {
                samples: samples
                    .slice(start..start + samples.len() / 2)
                    .unwrap()
                    .into(),
                sample_rate,
                channels,
                stream_id: None,
                timestamp_us: Some(index as u64),
                arrival_ts_us: None,
                metadata: Some(metadata(ctx, samples.as_ptr() as usize)),
            })?;
        }
        Ok(2)
    }
    async fn tick_with_ctx(
        &self,
        tick: Tick,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        if self.mode == "copy_policy" {
            return callback(copy_probe(ctx, "tick")?);
        }
        let mut loan = ctx
            .allocate_output_audio(8)
            .map_err(|e| Error::Execution(e.to_string()))?;
        let address = loan.as_ptr() as usize;
        loan.fill(tick.frame_idx as f32);
        callback(RuntimeData::Audio {
            samples: loan.freeze().into(),
            sample_rate: 48000,
            channels: 2,
            stream_id: Some(tick.clock_id),
            timestamp_us: Some(tick.pts_us),
            arrival_ts_us: None,
            metadata: Some(metadata(ctx, address)),
        })
    }
    async fn process_control_with_ctx(
        &self,
        message: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<bool, Error> {
        if self.mode == "copy_policy" {
            copy_probe(ctx, "control")?;
            return Ok(true);
        }
        *self.control.lock().unwrap() = Some((ctx.session_id().into(), ctx.node_id().into()));
        if matches!(message, RuntimeData::Text(ref text) if text == "release") {
            self.held.lock().unwrap().clear();
        }
        if matches!(message, RuntimeData::Text(ref text) if text == "continue_stream") {
            self.continue_stream.notify_one();
        }
        Ok(true)
    }
    fn rpc_methods(&self) -> Vec<RpcMethod> {
        vec![RpcMethod::new("identity", RpcMode::Reply)]
    }
    async fn call_rpc(
        &self,
        _: RpcRequest,
        ctx: &dyn NodeRuntimeContextRead,
        replies: &mut RpcResponder,
    ) -> Result<(), Error> {
        if self.mode == "copy_policy" {
            let RuntimeData::Json(value) = copy_probe(ctx, "rpc")? else {
                unreachable!()
            };
            return replies.value(value).await;
        }
        let value = serde_json::json!({"session":ctx.session_id(), "node":ctx.node_id(), "address":self.last_address.load(Ordering::SeqCst),
            "cancelled":self.cancelled.load(Ordering::SeqCst), "held":self.held.lock().unwrap().len(), "control":self.control.lock().unwrap().clone(),
            "private_releases":PRIVATE_RELEASES.load(Ordering::SeqCst)});
        replies.value(value).await
    }
}

mod contract;
remotemedia_plugin_sdk::plugin_export!(Factory, contract::ContractFactory);
