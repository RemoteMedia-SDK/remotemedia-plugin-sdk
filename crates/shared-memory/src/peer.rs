//! Broker-side typed worker dispatcher. Callers can send work/cancellation while
//! a dedicated receive loop services allocations and releases. Output guards
//! supply bounded credits; slow consumers never hold the receive-side lock.
//!
//! Input records retain the exact exported host view until worker release or
//! connection teardown. Forwarded outputs slice that view after range/layout
//! checks, preserving host provenance without a second mapping or recipient
//! lease. Compatibility staging retains its new mapped owner and cannot inherit
//! the copied source's proof. Remote producer publications remain a separate
//! ownership path; this alone does not certify a worker's buffer contract.
use crate::{
    channel::{HandleAccess, Receiver, Sender, WorkerProcess, MAX_CONTROL_BYTES},
    AllocationId, BufferDescriptor, ElementType, PeerId, SharedPool, WriteDescriptor,
};
use remotemedia_types::{CopyContext, PayloadStorage, RuntimeData, TensorEncoding};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io,
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
};

mod allocation;
mod dispatch;
mod snapshots;
pub type SnapshotInputs = HashMap<String, Arc<dyn remotemedia_traits::ports::SnapshotPort>>;

/// Host-owned capabilities retained for one worker call. An explicit output
/// provider is authoritative: incompatible loans fail without a pool fallback.
#[derive(Default)]
pub struct CallResources {
    pub copies: Option<CopyContext>,
    /// Separate output materialization authority; None never permits a copy.
    pub output_copies: Option<CopyContext>,
    pub snapshots: SnapshotInputs,
    pub output_allocator: Option<Arc<dyn remotemedia_types::OutputAllocator>>,
}
mod output;
pub mod wire;
mod worker;
pub use output::{prepare_output, PreparedOutput};
pub use wire::{CallContext, CallKind, Limits};
use wire::{HostMessage, WorkerMessage};
pub use worker::{WorkerEndpoint, WorkerRequest};

static NEXT_PEER: AtomicU64 = AtomicU64::new(1 << 63);

struct Input {
    descriptor: BufferDescriptor,
    accepted: bool,
    storage: PayloadStorage,
}
struct Producer {
    descriptor: WriteDescriptor,
    published: Option<PayloadStorage>,
    pending: Option<crate::remote::PendingPublication>,
    copy: Option<(u64, remotemedia_types::CopyOperation)>,
    copy_failed: bool,
}
struct Call {
    snapshots: SnapshotInputs,
    copies: Option<CopyContext>,
    output_copies: Option<CopyContext>,
    pending_copies: usize,
    output_allocator: Option<Arc<dyn remotemedia_types::OutputAllocator>>,
    cancelled: bool,
    outstanding: usize,
    snapshot_reads: usize,
}
struct Ledger {
    modes: Option<HashSet<CallKind>>,
    next_call: u64,
    next_allocation: u64,
    next_snapshot: u64,
    next_output: u64,
    calls: HashMap<u64, Call>,
    inputs: HashMap<u64, Input>,
    producers: HashMap<AllocationId, Producer>,
    outputs: HashMap<u64, u64>,
}
struct Shared {
    snapshot_protocol: u16,
    output_copy_protocol: u16,
    pool: SharedPool,
    peer: PeerId,
    limits: Limits,
    sender: Mutex<Sender>,
    start: Mutex<()>,
    process: Mutex<WorkerProcess>,
    ledger: Mutex<Ledger>,
    closed: AtomicBool,
}

pub struct PeerConnection {
    shared: Arc<Shared>,
    receiver: Receiver,
}
#[derive(Clone)]
pub struct HostHandle {
    shared: Arc<Shared>,
}

pub enum PeerEvent {
    Ready {
        modes: Vec<CallKind>,
    },
    Output(ReceivedOutput),
    Complete {
        call: u64,
        result: std::result::Result<(), String>,
    },
}

pub struct ReceivedOutput {
    call: u64,
    sequence: u64,
    data: Option<RuntimeData>,
    connection: Weak<Shared>,
}
impl ReceivedOutput {
    pub fn call(&self) -> u64 {
        self.call
    }
    pub fn data(&self) -> &RuntimeData {
        self.data.as_ref().unwrap()
    }
    /// Return the owned media and release this queue credit. Media ownership is
    /// independent of the worker's reference and may outlive the whole connection.
    pub fn into_data(mut self) -> RuntimeData {
        self.data.take().unwrap()
    }
}
impl Drop for ReceivedOutput {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.upgrade() {
            connection.acknowledge(self.sequence);
        }
    }
}

impl PeerConnection {
    /// Connect a process already launched with the private worker channel.
    /// Resources owned by the process survive handshake failure and concurrent
    /// close until termination is confirmed.
    ///
    /// # Safety
    /// The complete executable ownership/non-delegation contract of [`Self::spawn`]
    /// applies, along with copy reporting when `output_copies` is enabled.
    pub unsafe fn from_process(
        process: WorkerProcess,
        pool: SharedPool,
        limits: Limits,
        snapshot_protocol: u16,
        output_copies: bool,
    ) -> io::Result<(Self, HostHandle)> {
        // SAFETY: caller supplies the same complete worker ownership contract.
        unsafe {
            Self::spawn_protocol(
                || Ok(process),
                pool,
                limits,
                snapshot_protocol,
                if output_copies {
                    wire::OUTPUT_COPY_PROTOCOL
                } else {
                    0
                },
            )
        }
    }

    /// Spawn a runtime worker on a private channel and issue a fresh incarnation.
    /// Call `next_event` to complete the version/capability handshake before work.
    ///
    /// # Safety
    /// The executable must implement this protocol's ownership contract: imported
    /// readers are immutable, outputs freeze before emission, releases follow final
    /// unmap, and buffers/channels are never delegated or inherited by descendants.
    /// Windows job assignment happens before the first host message; a worker
    /// must not spawn descendants before that message. Plain Linux spawn owns only
    /// the original child; spawn_namespaced adds explicit namespace containment.
    /// Neither platform permits delegating mappings or protocol
    /// handles, even to a contained descendant. Arbitrary executables are not safe
    /// peers merely because their standard pipes are private.
    pub unsafe fn spawn(
        command: &mut Command,
        pool: SharedPool,
        limits: Limits,
    ) -> io::Result<(Self, HostHandle)> {
        // SAFETY: forwarded from the caller's process ownership contract.
        unsafe { Self::spawn_protocol(|| WorkerProcess::spawn(command), pool, limits, 0, 0) }
    }

    /// Require live shared snapshot reads in the worker handshake.
    /// # Safety
    /// The complete immutable ownership/non-delegation contract of spawn applies.
    pub unsafe fn spawn_with_snapshots(
        command: &mut Command,
        pool: SharedPool,
        limits: Limits,
    ) -> io::Result<(Self, HostHandle)> {
        // SAFETY: forwarded from the caller's process ownership contract.
        unsafe {
            Self::spawn_protocol(
                || WorkerProcess::spawn(command),
                pool,
                limits,
                wire::SNAPSHOT_PROTOCOL,
                0,
            )
        }
    }

    /// Negotiate both live input reads and node-output snapshot operations.
    ///
    /// # Safety
    /// The complete executable ownership contract of [`Self::spawn`] applies.
    pub unsafe fn spawn_with_snapshot_outputs(
        command: &mut Command,
        pool: SharedPool,
        limits: Limits,
    ) -> io::Result<(Self, HostHandle)> {
        // SAFETY: forwarded from the caller's process ownership contract.
        unsafe {
            Self::spawn_protocol(
                || WorkerProcess::spawn(command),
                pool,
                limits,
                wire::SNAPSHOT_OUTPUT_PROTOCOL,
                0,
            )
        }
    }

    /// Explicit Linux namespace ownership, with snapshot protocol 0 (none),
    /// 1 (inputs), or 2 (inputs and outputs). No uncontained fallback.
    /// # Safety
    /// The complete immutable ownership/non-delegation contract of spawn applies
    /// to the worker and the selected matching supervisor/unshare executables.
    #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
    pub unsafe fn spawn_namespaced(
        command: &mut crate::channel::NamespaceCommand,
        pool: SharedPool,
        limits: Limits,
        snapshot_protocol: u16,
    ) -> io::Result<(Self, HostHandle)> {
        // SAFETY: caller supplies the executable and ownership contracts.
        unsafe {
            Self::spawn_protocol(
                || WorkerProcess::spawn_namespaced(command),
                pool,
                limits,
                snapshot_protocol,
                0,
            )
        }
    }

    /// Negotiate output-copy accounting independently of snapshots. Older peers
    /// reject the extension rather than silently dropping copy policy.
    /// # Safety
    /// The complete trust, immutable-publication and non-delegation contract of
    /// spawn applies. Workers must report each authorized copy exactly once.
    pub unsafe fn spawn_with_output_copies(
        command: &mut Command,
        pool: SharedPool,
        limits: Limits,
        snapshot_protocol: u16,
    ) -> io::Result<(Self, HostHandle)> {
        // SAFETY: caller supplies the same worker ownership contract as spawn.
        unsafe {
            Self::spawn_protocol(
                || WorkerProcess::spawn(command),
                pool,
                limits,
                snapshot_protocol,
                wire::OUTPUT_COPY_PROTOCOL,
            )
        }
    }

    /// Namespace-contained counterpart of spawn_with_output_copies.
    /// # Safety
    /// The ownership, non-delegation and copy-reporting contracts of both
    /// spawn_namespaced and spawn_with_output_copies apply.
    #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
    pub unsafe fn spawn_namespaced_with_output_copies(
        command: &mut crate::channel::NamespaceCommand,
        pool: SharedPool,
        limits: Limits,
        snapshot_protocol: u16,
    ) -> io::Result<(Self, HostHandle)> {
        // SAFETY: forwarded from the caller's complete worker contract.
        unsafe {
            Self::spawn_protocol(
                || WorkerProcess::spawn_namespaced(command),
                pool,
                limits,
                snapshot_protocol,
                wire::OUTPUT_COPY_PROTOCOL,
            )
        }
    }

    unsafe fn spawn_protocol(
        launch: impl FnOnce() -> io::Result<WorkerProcess>,
        pool: SharedPool,
        limits: Limits,
        snapshot_protocol: u16,
        output_copy_protocol: u16,
    ) -> io::Result<(Self, HostHandle)> {
        if snapshot_protocol > wire::SNAPSHOT_OUTPUT_PROTOCOL
            || !(1..=64).contains(&limits.calls)
            || !(1..=64).contains(&limits.inputs_per_call)
            || !(1..=16).contains(&limits.outputs_per_call)
        {
            return Err(invalid("invalid worker protocol budgets"));
        }
        let peer = PeerId(
            NEXT_PEER
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                .map_err(|_| invalid("worker incarnation space exhausted"))?,
        );
        let mut process = launch()?;
        let (sender, receiver) = process.take_channel().unwrap().split()?;
        let shared = Arc::new(Shared {
            snapshot_protocol,
            output_copy_protocol,
            pool,
            peer,
            limits,
            sender: Mutex::new(sender),
            start: Mutex::new(()),
            process: Mutex::new(process),
            closed: AtomicBool::new(false),
            ledger: Mutex::new(Ledger {
                modes: None,
                next_call: 1,
                next_allocation: 1,
                next_snapshot: 1,
                next_output: 1,
                calls: HashMap::new(),
                inputs: HashMap::new(),
                producers: HashMap::new(),
                outputs: HashMap::new(),
            }),
        });
        shared.send(
            &HostMessage::Hello {
                version: wire::WIRE_VERSION,
                snapshot_protocol,
                output_copy_protocol,
                session: shared.pool.session_id(),
                peer,
                encoding: TensorEncoding::Runtime,
                limits,
            },
            None,
        )?;
        Ok((
            Self {
                shared: shared.clone(),
                receiver,
            },
            HostHandle { shared },
        ))
    }

    pub fn next_event(&mut self) -> io::Result<PeerEvent> {
        let result = self.next_inner();
        if result.is_err() {
            self.shared.close();
        }
        result
    }
    fn next_inner(&mut self) -> io::Result<PeerEvent> {
        loop {
            self.shared.check_open()?;
            let frame = self.receiver.receive()?;
            if frame.file.is_some() {
                return Err(invalid("worker sent an unauthorized handle"));
            }
            let message: WorkerMessage = serde_json::from_slice(&frame.metadata).map_err(other)?;
            if let Some(event) = self.shared.dispatch(message)? {
                return Ok(event);
            }
        }
    }
}
impl Drop for PeerConnection {
    fn drop(&mut self) {
        self.shared.close();
    }
}

impl HostHandle {
    pub fn peer_id(&self) -> PeerId {
        self.shared.peer
    }
    pub fn start_call(
        &self,
        kind: CallKind,
        context: CallContext,
        arguments: serde_json::Value,
        inputs: Vec<(String, RuntimeData)>,
    ) -> io::Result<u64> {
        self.start_call_with_resources(kind, context, arguments, inputs, CallResources::default())
    }

    /// Start a call with an explicit host-input copy policy and ledger. Worker
    /// outputs remain strict mapped publications; this does not certify the
    /// whole pipeline or change the worker's allocation contract.
    pub fn start_call_with_copy_context(
        &self,
        kind: CallKind,
        context: CallContext,
        arguments: serde_json::Value,
        inputs: Vec<(String, RuntimeData)>,
        copies: &CopyContext,
    ) -> io::Result<u64> {
        self.start_call_with_snapshots(
            kind,
            context,
            arguments,
            inputs,
            Some(copies),
            SnapshotInputs::new(),
        )
    }

    /// Bind live portable readers to this call. Readers are sampled on each
    /// worker request, not captured as media at call startup.
    pub fn start_call_with_snapshots(
        &self,
        kind: CallKind,
        context: CallContext,
        arguments: serde_json::Value,
        inputs: Vec<(String, RuntimeData)>,
        copies: Option<&CopyContext>,
        snapshots: SnapshotInputs,
    ) -> io::Result<u64> {
        self.start_call_with_resources(
            kind,
            context,
            arguments,
            inputs,
            CallResources {
                copies: copies.cloned(),
                snapshots,
                ..Default::default()
            },
        )
    }

    /// Start a call with explicitly selected host capabilities. Newly produced
    /// outputs preserve the provider's owning loan through remote publication.
    pub fn start_call_with_resources(
        &self,
        kind: CallKind,
        context: CallContext,
        arguments: serde_json::Value,
        inputs: Vec<(String, RuntimeData)>,
        resources: CallResources,
    ) -> io::Result<u64> {
        let CallResources {
            copies,
            output_copies,
            snapshots,
            output_allocator,
        } = resources;
        // Assign IDs in the same order as their complete Begin/Input/Execute
        // batches, even when callers use independent HostHandle clones.
        let _start = self.shared.start.lock().unwrap();
        self.shared.check_open()?;
        if inputs.len() > self.shared.limits.inputs_per_call
            || inputs
                .iter()
                .map(|(name, _)| name)
                .collect::<HashSet<_>>()
                .len()
                != inputs.len()
        {
            return Err(invalid("duplicate or excessive named worker inputs"));
        }
        if !snapshots.is_empty() && self.shared.snapshot_protocol == 0 {
            return Err(invalid("snapshot reads were not negotiated"));
        }
        if snapshots.len() > self.shared.limits.inputs_per_call
            || snapshots.values().any(|port| !port.supports_runtime_data())
        {
            return Err(invalid("invalid portable snapshot bindings"));
        }
        let mut snapshot_ports: Vec<_> = snapshots.keys().cloned().collect();
        snapshot_ports.sort();
        let call = {
            let mut ledger = self.shared.ledger.lock().unwrap();
            self.shared.check_open()?;
            if !ledger
                .modes
                .as_ref()
                .is_some_and(|modes| modes.contains(&kind))
            {
                return Err(invalid("worker operation not negotiated"));
            }
            if ledger.calls.len() >= self.shared.limits.calls {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "worker call budget exhausted",
                ));
            }
            let id = ledger.next_call;
            ledger.next_call = id
                .checked_add(1)
                .ok_or_else(|| invalid("worker call ids exhausted"))?;
            ledger.calls.insert(
                id,
                Call {
                    snapshots,
                    copies: copies.clone(),
                    output_copies,
                    pending_copies: 0,
                    output_allocator,
                    cancelled: false,
                    outstanding: 0,
                    snapshot_reads: 0,
                },
            );
            id
        };
        let result = self.send_call(
            call,
            kind,
            context,
            arguments,
            inputs,
            copies.as_ref(),
            snapshot_ports,
        );
        if result.is_err() {
            let removed = self.shared.ledger.lock().unwrap().calls.remove(&call);
            drop(removed);
        }
        result.map(|()| call)
    }
    fn send_call(
        &self,
        call: u64,
        kind: CallKind,
        context: CallContext,
        arguments: serde_json::Value,
        inputs: Vec<(String, RuntimeData)>,
        copies: Option<&CopyContext>,
        snapshot_ports: Vec<String>,
    ) -> io::Result<()> {
        // Preflight all metadata and lease grants before sending Begin. Any error
        // here drops undispatched deliveries and leaves the connection usable.
        let begin = encode(&HostMessage::Begin {
            call,
            kind,
            context,
            arguments,
            inputs: inputs.len(),
            snapshot_ports,
        })?;
        let mut prepared = Vec::with_capacity(inputs.len());
        for (name, data) in inputs {
            let delivery = match copies {
                Some(copies) => self.shared.pool.prepare_message_with_copy_context(
                    data,
                    self.shared.peer,
                    TensorEncoding::Runtime,
                    copies,
                ),
                None => self.shared.pool.prepare_message(
                    data,
                    self.shared.peer,
                    TensorEncoding::Runtime,
                ),
            }
            .map_err(other)?;
            let bytes = encode(&HostMessage::Input {
                call,
                name,
                message: delivery.descriptor().clone(),
            })?;
            prepared.push((delivery, bytes));
        }
        let execute = encode(&HostMessage::Execute { call })?;
        let result = (|| {
            // Serialize the outbound call batch; the receive task stays independent.
            let mut sender = self.shared.sender.lock().unwrap();
            self.shared.check_open()?;
            sender.send(&begin, None)?;
            for (delivery, bytes) in prepared {
                if let Some(descriptor) = &delivery.descriptor().buffer {
                    let mut ledger = self.shared.ledger.lock().unwrap();
                    self.shared.check_open()?;
                    ledger.inputs.insert(
                        descriptor.lease,
                        Input {
                            descriptor: descriptor.clone(),
                            accepted: false,
                            storage: delivery.storage(),
                        },
                    );
                }
                // SAFETY: this connection retains the exact child incarnation,
                // records its descriptor, and binds the file on its private channel.
                let (_, file) = unsafe { delivery.into_transport() };
                sender.send(&bytes, file.as_ref().map(|file| (file, HandleAccess::Read)))?;
            }
            sender.send(&execute, None)
        })();
        if result.is_err() {
            self.shared.close();
        }
        result
    }
    pub fn cancel(&self, call: u64) -> io::Result<()> {
        self.shared.check_open()?;
        let outputs = {
            let mut ledger = self.shared.ledger.lock().unwrap();
            let Some(active) = ledger.calls.get_mut(&call) else {
                return Ok(());
            };
            active.cancelled = true;
            ledger
                .outputs
                .iter()
                .filter_map(|(&sequence, &owner)| (owner == call).then_some(sequence))
                .collect::<Vec<_>>()
        };
        self.shared.send(&HostMessage::Cancel { call }, None)?;
        // Already delivered outputs own their local storage independently. Free
        // their queue credits so cancellation is not held up by slow consumers.
        // Cancellation precedes these acknowledgements on the worker channel.
        for sequence in outputs {
            self.shared.acknowledge(sequence);
        }
        Ok(())
    }
    pub fn close(&self) {
        self.shared.close();
    }
}

impl Shared {
    fn check_open(&self) -> io::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "worker connection closed",
            ))
        } else {
            Ok(())
        }
    }
    fn send(&self, message: &HostMessage, file: Option<(&File, HandleAccess)>) -> io::Result<()> {
        let bytes = encode(message)?;
        self.check_open()?;
        let result = self.sender.lock().unwrap().send(&bytes, file);
        if result.is_err() {
            self.close();
        }
        result
    }
    fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let terminated = self.process.lock().unwrap().terminate().is_ok();
        let mut ledger = self.ledger.lock().unwrap();
        if terminated {
            // SAFETY: spawn's contract forbids delegation; the original child
            // handle confirmed termination; Windows jobs have no active members,
            // and explicit Linux namespace launches also confirmed init death.
            // Never reclaim on an I/O timeout alone.
            unsafe { self.pool.release_dead_peer(self.peer) };
        }
        let calls = std::mem::take(&mut ledger.calls);
        let inputs = std::mem::take(&mut ledger.inputs);
        let producers = std::mem::take(&mut ledger.producers);
        ledger.outputs.clear();
        drop(ledger);
        drop(calls);
        // Retained host owners may run arbitrary final-release callbacks.
        drop(inputs);
        drop(producers);
        // Failed termination intentionally leaves broker reservations quarantined.
    }
    fn acknowledge(&self, sequence: u64) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let present = {
            let mut ledger = self.ledger.lock().unwrap();
            if let Some(call) = ledger.outputs.remove(&sequence) {
                ledger
                    .calls
                    .get_mut(&call)
                    .expect("output retains active call")
                    .outstanding -= 1;
                true
            } else {
                false
            }
        };
        if present {
            let _ = self.send(&HostMessage::OutputAccepted { sequence }, None);
        }
    }
}

fn encode(value: &HostMessage) -> io::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(other)?;
    if bytes.len() > MAX_CONTROL_BYTES {
        return Err(invalid("worker metadata exceeds channel budget"));
    }
    Ok(bytes)
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}
