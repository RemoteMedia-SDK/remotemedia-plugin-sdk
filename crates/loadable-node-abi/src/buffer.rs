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
