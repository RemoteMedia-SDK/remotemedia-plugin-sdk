//! Same-process buffer ownership. These handles must never be sent to another
//! process: IPC uses a separately authenticated mapping descriptor protocol.
// abi_stable 0.11's consuming-self thunk emits a same-type pointer cast. This
// module contains ABI declarations only; handwritten casts are in runtime_buffer.
#![allow(clippy::unnecessary_cast)]
use abi_stable::{
    sabi_trait,
    std_types::{RArc, RBox, ROption, RResult, RSlice, RSliceMut, RString, RVec},
    StableAbi,
};

pub const BUFFER_ABI_VERSION: u16 = 2;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, StableAbi)]
pub enum BufferElement {
    Bytes,
    AudioF32,
}

/// Immutable storage owned in its creating module. All clones retain the same
/// initialized allocation; view must remain unchanged until its final owner drops.
/// The loader must keep the creating library loaded while any handle exists.
///
/// # Safety
/// view must expose the same initialized immutable allocation for every call;
/// cloning retains it, and release cannot run until every cloned owner drops.
#[sabi_trait]
#[sabi(no_trait_impl)]
pub unsafe trait BufferOwner: Clone + Send + Sync + 'static {
    fn element(&self) -> BufferElement;
    fn view(&self) -> RSlice<'_, u8>;
}
pub type BufferOwnerArc = BufferOwner_TO<RArc<()>>;

#[repr(C)]
#[derive(Clone, StableAbi)]
pub struct FfiBuffer {
    pub owner: BufferOwnerArc,
    pub offset: u64,
    pub byte_len: u64,
}

/// Metadata JSON uses RuntimeMetadata schema v1. Bulk storage is NEVER embedded
/// in metadata. Numeric tensor codes use TensorEncoding::Runtime for this ABI.
#[repr(C)]
#[derive(Clone, StableAbi)]
pub struct FfiMessage {
    pub version: u16,
    pub metadata: RVec<u8>,
    pub buffer: ROption<FfiBuffer>,
}

/// An exclusive initialized producer loan. Rust borrowing prevents simultaneous
/// writable aliases. freeze consumes this handle and never changes its address.
///
/// # Safety
/// The loan is the sole writer to its initialized stable allocation. freeze must
/// return an owner of that exact allocation and revoke all writable access.
#[sabi_trait]
#[sabi(no_trait_impl)]
pub unsafe trait BufferLoan: Send + 'static {
    fn element(&self) -> BufferElement;
    fn view(&self) -> RSlice<'_, u8>;
    fn view_mut(&mut self) -> RSliceMut<'_, u8>;
    fn freeze(self) -> BufferOwnerArc;
}
pub type BufferLoanBox = BufferLoan_TO<RBox<()>>;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, StableAbi)]
pub enum AllocationError {
    InvalidLength,
    Exhausted,
    Unavailable,
}

#[sabi_trait]
pub trait OutputAllocator: Clone + Send + Sync + 'static {
    fn allocate(
        &self,
        element: BufferElement,
        elements: u64,
    ) -> RResult<BufferLoanBox, AllocationError>;
}
pub type OutputAllocatorArc = OutputAllocator_TO<RArc<()>>;

#[repr(C)]
#[derive(Clone, StableAbi)]
pub struct FfiExecutionContext {
    pub identity: crate::FfiCallContext,
    pub materialization: ROption<crate::copy_ledger::FfiCopyContext>,
    pub allocator: ROption<OutputAllocatorArc>,
    pub snapshots: RVec<crate::snapshot::FfiSnapshotPort>,
}

#[repr(C)]
#[derive(Clone, StableAbi)]
pub struct FfiNamedInput {
    pub name: RString,
    pub message: FfiMessage,
}

#[sabi_trait]
pub trait SharedOutputSink: Send + Sync + 'static {
    fn push(&self, message: FfiMessage) -> RResult<(), RString>;
}
pub type SharedOutputSinkBox = SharedOutputSink_TO<RBox<()>>;

/// `origin` of [`NodeEmitter::emit`]: the instance's default attribution (its
/// participant for a per-participant instance, nobody otherwise).
pub const EMIT_AS_DEFAULT: u8 = 0;
/// `origin` of [`NodeEmitter::emit`]: nobody.
pub const EMIT_UNATTRIBUTED: u8 = 1;
/// `origin` of [`NodeEmitter::emit`]: the participant whose id is passed.
pub const EMIT_AS_PARTICIPANT: u8 = 2;
/// `origin` of [`NodeEmitter::emit`]: an origin the node read from an input
/// (`FfiCallContext::origin`), passed back as the same JSON.
pub const EMIT_AS_ORIGIN: u8 = 3;

/// [`FfiTryEmit::status`]: the frame was queued.
pub const TRY_EMIT_QUEUED: u8 = 0;
/// [`FfiTryEmit::status`]: the emit queue is full.
pub const TRY_EMIT_FULL: u8 = 1;
/// [`FfiTryEmit::status`]: the instance has not started.
pub const TRY_EMIT_NOT_STARTED: u8 = 2;
/// [`FfiTryEmit::status`]: the frame cannot be emitted; see `error`.
pub const TRY_EMIT_REJECTED: u8 = 3;

/// The outcome of [`NodeEmitter::try_emit`]. A host may hand a frame it did
/// not queue back in `message`; a plugin must not rely on it.
#[repr(C)]
#[derive(StableAbi)]
pub struct FfiTryEmit {
    pub status: u8,
    pub message: abi_stable::std_types::ROption<FfiMessage>,
    pub error: RString,
}

/// A node instance's handle onto its own output edge (openspec:
/// add-node-initiated-output). Unlike a call's sink it outlives every call:
/// the host binds it before `initialize` and closes it when the instance
/// stops. Frames pushed into a call's sink stay that input's output; frames
/// emitted here belong to no input.
#[sabi_trait]
pub trait NodeEmitter: Send + Sync + 'static {
    /// Emit one frame, waiting while the emit queue is full. `origin` is one
    /// of the `EMIT_*` constants; `participant` is the participant's id for
    /// [`EMIT_AS_PARTICIPANT`] and the origin's JSON for [`EMIT_AS_ORIGIN`]. Fails once the instance has stopped, and
    /// for a participant who is not in the session.
    fn emit(
        &self,
        message: FfiMessage,
        origin: u8,
        participant: RString,
    ) -> async_ffi::FfiFuture<RResult<(), RString>>;
    /// [`Self::emit`] without waiting.
    fn try_emit(&self, message: FfiMessage, origin: u8, participant: RString) -> FfiTryEmit;
    /// Resolves when the instance has started, or the emitter is closed.
    fn started(&self) -> async_ffi::FfiFuture<()>;
    /// Whether emitting can no longer succeed.
    fn is_closed(&self) -> bool;
    /// Resolves when the emitter is closed.
    fn closed(&self) -> async_ffi::FfiFuture<()>;
    /// Close the emitter: the node has finished producing.
    fn finish(&self);
}
pub type NodeEmitterBox = NodeEmitter_TO<RBox<()>>;
