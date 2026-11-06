//! Same-host buffer storage. Payloads are separate from descriptor/control data.
//!
//! The broker owns allocation generations and recipient leases; losing a sender
//! or timing out a delivery does not prove a recipient has stopped reading.

mod broker;
mod descriptor;
mod message;
mod pool;
mod remote;

#[cfg(all(feature = "worker-channel", any(windows, target_os = "linux")))]
pub mod channel;
#[cfg(all(feature = "worker-protocol", any(windows, target_os = "linux")))]
pub mod peer;

pub use descriptor::WriteDescriptor;
pub use descriptor::{
    AllocationId, BufferDescriptor, ElementType, PeerId, SessionId, PROTOCOL_VERSION,
};
pub use message::{import_message, MessageDelivery, MessageDescriptor};
pub use pool::{import, Delivery, MappedElement, PoolConfig, SharedPool, SizeClass};
pub use remote::{import_writer, writer_descriptor, WriteGrant};

#[derive(Debug, thiserror::Error)]
pub enum SharedMemoryError {
    #[error("invalid shared-memory configuration or layout")]
    InvalidLayout,
    #[error("payload storage is private and cannot be exported as a shared mapping")]
    PrivateStorage,
    #[error(transparent)]
    CopyRejected(#[from] remotemedia_types::CopyRejected),
    #[error("shared-memory pool or lease budget exhausted")]
    Exhausted,
    #[error("shared-memory session is closed")]
    Closed,
    #[error("unknown allocation or stale generation")]
    StaleAllocation,
    #[error("unknown, released, or mismatched delivery lease")]
    InvalidLease,
    #[error("shared-memory allocation is not published")]
    NotPublished,
    #[error("descriptor belongs to another session or protocol")]
    WrongSession,
    #[error(transparent)]
    PayloadLayout(#[from] remotemedia_types::PayloadLayoutError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, SharedMemoryError>;
