use super::{
    invalid, other, prepare_output, wire::*, CallContext, CallKind, Limits, PreparedOutput,
};
use crate::{
    channel::{Channel, Frame, HandleAccess, Receiver},
    import_message, import_writer, AllocationId, ElementType, PeerId, SessionId,
};
use remotemedia_types::{
    BufferAllocationError, BufferLoan, OutputAllocator, RuntimeData, TensorEncoding,
};
use std::{
    collections::{HashMap, HashSet},
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex, Weak,
    },
};

mod copies;
mod receive;
mod snapshots;
type RuntimeSnapshot = Arc<remotemedia_traits::ports::TimestampedSnapshot<RuntimeData>>;
struct PendingSnapshot {
    reply: mpsc::SyncSender<io::Result<Option<RuntimeSnapshot>>>,
}

type AllocationResult = std::result::Result<GrantedLoan, AllocationFailure>;
struct GrantedLoan {
    allocation: AllocationId,
    loan: Loan,
}
enum Loan {
    Audio(BufferLoan<f32>),
    Bytes(BufferLoan<u8>),
}
struct PendingAllocation {
    element: ElementType,
    elements: usize,
    reply: mpsc::SyncSender<AllocationResult>,
}
struct PendingOutput {
    prepared: PreparedOutput,
    reply: mpsc::SyncSender<io::Result<()>>,
}
struct Active {
    pending_copies: usize,
    snapshot_reads: usize,
    cancelled: Arc<AtomicBool>,
    allocations: usize,
    outputs: usize,
}
struct Staged {
    snapshot_ports: HashSet<String>,
    kind: CallKind,
    context: CallContext,
    arguments: serde_json::Value,
    expected: usize,
    inputs: HashMap<String, RuntimeData>,
}
struct State {
    snapshots: HashMap<u64, PendingSnapshot>,
    last_call: u64,
    calls: HashMap<u64, Active>,
    staged: HashMap<u64, Staged>,
    allocations: HashMap<u64, PendingAllocation>,
    outputs: HashMap<u64, PendingOutput>,
    readers: HashSet<u64>,
    producers: HashSet<AllocationId>,
}
struct Outbound {
    next_snapshot: u64,
    sender: Option<mpsc::Sender<Vec<u8>>>,
    next_allocation: u64,
    next_output: u64,
}
impl Outbound {
    fn send(&self, bytes: Vec<u8>) -> io::Result<()> {
        self.sender
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "worker writer closed"))?
            .send(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "worker writer stopped"))
    }
}
struct SharedWorker {
    snapshot_protocol: u16,
    output_copy_protocol: u16,
    session: SessionId,
    peer: PeerId,
    encoding: TensorEncoding,
    limits: Limits,
    modes: HashSet<CallKind>,
    outbound: Mutex<Outbound>,
    state: Mutex<State>,
    closed: AtomicBool,
}

/// Process-lifetime worker endpoint. A Rust I/O thread receives allocator replies
/// and cancellation while application code executes or produces streaming output.
pub struct WorkerEndpoint {
    shared: Arc<SharedWorker>,
    requests: mpsc::Receiver<WorkerRequest>,
}
pub struct WorkerRequest {
    snapshot_ports: HashSet<String>,
    pub call: u64,
    pub kind: CallKind,
    pub context: CallContext,
    pub arguments: serde_json::Value,
    pub inputs: HashMap<String, RuntimeData>,
    shared: Arc<SharedWorker>,
    cancelled: Arc<AtomicBool>,
    completed: AtomicBool,
}

impl WorkerEndpoint {
    /// # Safety
    /// Claim only the private stdio endpoints of the spawning broker, once and
    /// before other threads use them. Follow the non-delegation/final-unmap contract
    /// of PeerConnection::spawn, including no fork with live channel or buffers.
    pub unsafe fn from_inherited_stdio(modes: Vec<CallKind>) -> io::Result<Self> {
        // SAFETY: forwarded directly from this function's startup contract.
        let channel = unsafe { Channel::from_inherited_stdio() }?;
        // SAFETY: this is the private channel claimed above; all remaining
        // lifetime/non-delegation requirements are the caller's contract.
        unsafe { Self::from_channel(channel, modes) }
    }

    /// Start the dispatcher on an already claimed private worker channel. This
    /// permits an interpreter to redirect application stdio after claiming the
    /// endpoint, before any channel I/O threads or application imports start.
    ///
    /// # Safety
    /// `channel` must be the unique worker side of the spawning broker's private
    /// channel. Follow the same trust, final-unmap and non-delegation requirements
    /// as `from_inherited_stdio`; never supply a broker-side or unrelated channel.
    pub unsafe fn from_channel(channel: Channel, mut modes: Vec<CallKind>) -> io::Result<Self> {
        let unique: HashSet<_> = modes.iter().copied().collect();
        if modes.is_empty() || unique.len() != modes.len() {
            return Err(invalid("invalid worker operation capabilities"));
        }
        let (mut sender, mut receiver) = channel.split()?;
        let frame = receiver.receive()?;
        let HostMessage::Hello {
            version,
            session,
            peer,
            encoding,
            limits,
            snapshot_protocol,
            output_copy_protocol,
        } = serde_json::from_slice(&frame.metadata).map_err(other)?
        else {
            return Err(invalid("worker expected initial hello"));
        };
        if frame.file.is_some()
            || frame.access.is_some()
            || version != WIRE_VERSION
            || ![0, SNAPSHOT_PROTOCOL, SNAPSHOT_OUTPUT_PROTOCOL].contains(&snapshot_protocol)
            || ![0, OUTPUT_COPY_PROTOCOL].contains(&output_copy_protocol)
            || !(1..=64).contains(&limits.calls)
            || !(1..=64).contains(&limits.inputs_per_call)
            || !(1..=16).contains(&limits.outputs_per_call)
        {
            return Err(invalid("invalid broker handshake"));
        }
        // Preserve the old Ready shape unless output snapshots were requested.
        if snapshot_protocol != SNAPSHOT_OUTPUT_PROTOCOL {
            modes.retain(|kind| *kind != CallKind::Snapshot);
        }
        let unique: HashSet<_> = modes.iter().copied().collect();
        if modes.is_empty() {
            return Err(invalid("no operations for negotiated protocol"));
        }
        let (wire_send, wire_receive) = mpsc::channel();
        let shared = Arc::new(SharedWorker {
            snapshot_protocol,
            output_copy_protocol,
            session,
            peer,
            encoding,
            limits,
            modes: unique,
            outbound: Mutex::new(Outbound {
                next_snapshot: 1,
                sender: Some(wire_send),
                next_allocation: 1,
                next_output: 1,
            }),
            state: Mutex::new(State {
                snapshots: HashMap::new(),
                last_call: 0,
                calls: HashMap::new(),
                staged: HashMap::new(),
                allocations: HashMap::new(),
                outputs: HashMap::new(),
                readers: HashSet::new(),
                producers: HashSet::new(),
            }),
            closed: AtomicBool::new(false),
        });
        // Never write the OS pipe from the receive task: a large output may be
        // waiting for the broker while its call batch still needs to be read.
        // Queue growth is bounded by negotiated calls/credits plus the broker's
        // finite live mapping leases. Completion cannot overtake prior releases.
        let writer_shared = Arc::downgrade(&shared);
        std::thread::Builder::new()
            .name("mapped-worker-writer".into())
            .spawn(move || {
                for bytes in wire_receive {
                    if let Err(error) = sender.send(&bytes, None) {
                        if let Some(shared) = writer_shared.upgrade() {
                            shared.fail(&error.to_string());
                        }
                        break;
                    }
                }
            })?;
        shared.send(&WorkerMessage::Ready {
            snapshot_protocol,
            output_copy_protocol,
            version: WIRE_VERSION,
            session,
            peer,
            modes,
        })?;
        let (send, requests) = mpsc::channel();
        let reader_shared = shared.clone();
        std::thread::Builder::new()
            .name("mapped-worker-control".into())
            .spawn(move || {
                let result = reader_shared.receive_loop(&mut receiver, send);
                if let Err(error) = result {
                    reader_shared.fail(&error.to_string());
                } else {
                    reader_shared.close_pending();
                }
            })?;
        Ok(Self { shared, requests })
    }
    /// Negotiated bounds for sizing the application's call executor and queues.
    pub fn limits(&self) -> super::Limits {
        self.shared.limits
    }

    /// Queued plus executing requests are bounded by the negotiated call budget.
    pub fn next_request(&self) -> io::Result<WorkerRequest> {
        self.requests
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "worker request channel closed"))
    }
}
impl Drop for WorkerEndpoint {
    fn drop(&mut self) {
        self.shared.fail("worker endpoint dropped");
    }
}

impl WorkerRequest {
    pub fn limits(&self) -> Limits {
        self.shared.limits
    }
    pub fn supports_snapshot_outputs(&self) -> bool {
        self.shared.snapshot_protocol == SNAPSHOT_OUTPUT_PROTOCOL
    }
    /// Fail this connection when the execution adapter cannot honor the call
    /// protocol (for example, it returned while output operations were pending).
    /// The broker receives a fatal event and owns termination/reclamation.
    pub fn fail_connection(&self, reason: &str) {
        self.shared.fail(reason);
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire) || self.shared.closed.load(Ordering::Acquire)
    }
    pub fn allocator(&self) -> Arc<dyn OutputAllocator> {
        Arc::new(RemoteAllocator {
            shared: self.shared.clone(),
            call: self.call,
        })
    }
    /// Retains the immutable owner until the host consumes/acknowledges this output.
    /// The receiver task remains free to service cancellation and allocation replies.
    pub fn emit(&self, data: RuntimeData) -> io::Result<()> {
        if self.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "worker call cancelled",
            ));
        }
        let prepared = prepare_output(
            data,
            self.shared.session,
            self.shared.peer,
            self.shared.encoding,
        )
        .map_err(other)?;
        let (reply, wait) = mpsc::sync_channel(1);
        let mut registered = false;
        let send = (|| {
            let mut outbound = self.shared.outbound.lock().unwrap();
            let mut state = self.shared.state.lock().unwrap();
            self.shared.check_open()?;
            let active = state
                .calls
                .get_mut(&self.call)
                .ok_or_else(|| invalid("output after call completion"))?;
            if active.cancelled.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "worker call cancelled",
                ));
            }
            if active.outputs >= self.shared.limits.outputs_per_call {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "worker output credit budget exhausted",
                ));
            }
            let sequence = outbound.next_output;
            let next = sequence
                .checked_add(1)
                .ok_or_else(|| invalid("worker output ids exhausted"))?;
            let bytes = encode(&WorkerMessage::Output {
                call: self.call,
                sequence,
                message: prepared.message().clone(),
            })?;
            active.outputs += 1;
            state
                .outputs
                .insert(sequence, PendingOutput { prepared, reply });
            registered = true;
            outbound.next_output = next;
            drop(state);
            outbound.send(bytes)
        })();
        if let Err(error) = send {
            if registered {
                self.shared.fail(&error.to_string());
                if let Some(active) = self.shared.state.lock().unwrap().calls.get_mut(&self.call) {
                    active.outputs -= 1;
                }
            }
            return Err(error);
        }
        let result = wait.recv().unwrap_or_else(|_| {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "worker output acknowledgement lost",
            ))
        });
        if let Some(active) = self.shared.state.lock().unwrap().calls.get_mut(&self.call) {
            active.outputs -= 1;
        }
        result
    }
    pub fn complete(&self, result: std::result::Result<(), String>) -> io::Result<()> {
        if self.completed.load(Ordering::Acquire) {
            return Err(invalid("duplicate worker call completion"));
        }
        {
            let mut state = self.shared.state.lock().unwrap();
            let active = state
                .calls
                .get(&self.call)
                .ok_or_else(|| invalid("unknown worker call completion"))?;
            if active.outputs != 0
                || active.allocations != 0
                || active.snapshot_reads != 0
                || active.pending_copies != 0
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "worker call still has pending operations",
                ));
            }
            state.calls.remove(&self.call);
        }
        self.completed.store(true, Ordering::Release);
        self.shared.send(&WorkerMessage::Complete {
            call: self.call,
            result,
        })
    }
}
impl Drop for WorkerRequest {
    fn drop(&mut self) {
        if !self.completed.load(Ordering::Acquire)
            && self
                .complete(Err("worker request dropped without completion".into()))
                .is_err()
        {
            self.shared
                .fail("worker request dropped with pending operations");
        }
    }
}

struct RemoteAllocator {
    shared: Arc<SharedWorker>,
    call: u64,
}
impl RemoteAllocator {
    fn allocate(&self, element: ElementType, elements: usize, copying: bool) -> AllocationResult {
        let (reply, wait) = mpsc::sync_channel(1);
        let send = (|| {
            let mut outbound = self.shared.outbound.lock().unwrap();
            let mut state = self.shared.state.lock().unwrap();
            if self.shared.closed.load(Ordering::Acquire) {
                return Err(AllocationFailure::Unavailable);
            }
            let active = state
                .calls
                .get_mut(&self.call)
                .ok_or(AllocationFailure::Unavailable)?;
            if active.cancelled.load(Ordering::Acquire) {
                return Err(AllocationFailure::Unavailable);
            }
            if active.allocations >= self.shared.limits.outputs_per_call {
                return Err(AllocationFailure::Exhausted);
            }
            let request = outbound.next_allocation;
            let next = request
                .checked_add(1)
                .ok_or(AllocationFailure::Unavailable)?;
            let message = if copying {
                WorkerMessage::AllocateCopy {
                    request,
                    call: self.call,
                    element,
                    elements: elements as u64,
                }
            } else {
                WorkerMessage::Allocate {
                    request,
                    call: self.call,
                    element,
                    elements: elements as u64,
                }
            };
            let bytes = encode(&message).map_err(|_| AllocationFailure::Unavailable)?;
            active.allocations += 1;
            state.allocations.insert(
                request,
                PendingAllocation {
                    element,
                    elements,
                    reply,
                },
            );
            outbound.next_allocation = next;
            drop(state);
            if let Err(error) = outbound.send(bytes) {
                drop(outbound);
                self.shared.fail(&error.to_string());
            }
            Ok(())
        })();
        send?;
        let result = wait.recv().unwrap_or(Err(AllocationFailure::Unavailable));
        if let Some(active) = self.shared.state.lock().unwrap().calls.get_mut(&self.call) {
            active.allocations -= 1;
        }
        result
    }
}
impl OutputAllocator for RemoteAllocator {
    fn allocate_audio(
        &self,
        samples: usize,
    ) -> std::result::Result<BufferLoan<f32>, BufferAllocationError> {
        match self
            .allocate(ElementType::F32Le, samples, false)
            .map_err(allocation_error)?
            .loan
        {
            Loan::Audio(loan) => Ok(loan),
            _ => Err(BufferAllocationError::Unavailable),
        }
    }
    fn allocate_bytes(
        &self,
        len: usize,
    ) -> std::result::Result<BufferLoan<u8>, BufferAllocationError> {
        match self
            .allocate(ElementType::Bytes, len, false)
            .map_err(allocation_error)?
            .loan
        {
            Loan::Bytes(loan) => Ok(loan),
            _ => Err(BufferAllocationError::Unavailable),
        }
    }
}

impl SharedWorker {
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
    fn send(&self, message: &WorkerMessage) -> io::Result<()> {
        self.check_open()?;
        let bytes = encode(message)?;
        let result = self.outbound.lock().unwrap().send(bytes);
        if result.is_err() {
            self.close_pending();
        }
        result
    }
    fn fail(&self, message: &str) {
        if !self.closed.load(Ordering::Acquire) {
            let _ = self.send(&WorkerMessage::Fatal {
                message: message.into(),
            });
        }
        self.close_pending();
    }
    fn close_pending(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.outbound.lock().unwrap().sender.take();
        let (allocations, outputs, staged, snapshots) = {
            let mut state = self.state.lock().unwrap();
            for active in state.calls.values() {
                active.cancelled.store(true, Ordering::Release);
            }
            (
                std::mem::take(&mut state.allocations),
                std::mem::take(&mut state.outputs),
                std::mem::take(&mut state.staged),
                std::mem::take(&mut state.snapshots),
            )
        };
        // Never drop buffer owners under the state lock: release callbacks use it.
        drop(staged);
        for (_, pending) in snapshots {
            let _ = pending.reply.send(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "snapshot connection closed",
            )));
        }
        for (_, pending) in allocations {
            let _ = pending.reply.send(Err(AllocationFailure::Unavailable));
        }
        for (_, pending) in outputs {
            drop(pending.prepared);
            let _ = pending.reply.send(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "worker connection closed",
            )));
        }
    }
    fn release_input(this: &Weak<Self>, lease: u64) {
        if let Some(shared) = this.upgrade() {
            let known = shared.state.lock().unwrap().readers.remove(&lease);
            if known {
                let _ = shared.send(&WorkerMessage::InputReleased { lease });
            }
        }
    }
    fn release_producer(this: &Weak<Self>, allocation: AllocationId) {
        if let Some(shared) = this.upgrade() {
            let known = shared.state.lock().unwrap().producers.remove(&allocation);
            if known {
                let _ = shared.send(&WorkerMessage::ProducerReleased { allocation });
            }
        }
    }
}
fn allocation_error(error: AllocationFailure) -> BufferAllocationError {
    match error {
        AllocationFailure::InvalidLength => BufferAllocationError::InvalidLength,
        AllocationFailure::Exhausted => BufferAllocationError::Exhausted,
        AllocationFailure::Unavailable | AllocationFailure::CopyDenied => {
            BufferAllocationError::Unavailable
        }
    }
}
fn encode(value: &WorkerMessage) -> io::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(other)?;
    if bytes.len() > crate::channel::MAX_CONTROL_BYTES {
        return Err(invalid("worker metadata exceeds channel budget"));
    }
    Ok(bytes)
}
