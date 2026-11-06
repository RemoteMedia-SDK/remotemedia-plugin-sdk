use crate::{Result, SharedMemoryError};
use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u16 = 1;

/// Runtime-generated unique session/pool identity, never an address or file path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub [u8; 16]);

/// Broker-issued process incarnation. This is not an OS PID (which can be reused).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AllocationId {
    pub slot: u32,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ElementType {
    Bytes,
    F32Le,
}

impl ElementType {
    pub fn size(self) -> u64 {
        match self {
            Self::Bytes => 1,
            Self::F32Le => 4,
        }
    }
}

/// A contiguous primitive storage view. Tensor/plane metadata is carried above
/// this layer and must be validated against this byte range by the adapter.
/// OS handles are delivered separately over the authenticated local channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferDescriptor {
    pub version: u16,
    pub session: SessionId,
    pub allocation: AllocationId,
    pub lease: u64,
    pub recipient: PeerId,
    pub offset: u64,
    pub byte_len: u64,
    pub element: ElementType,
}

/// One exclusive producer reservation. Unlike a read lease, this grants write
/// access to the complete initialized allocation until immutable publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteDescriptor {
    pub version: u16,
    pub session: SessionId,
    pub allocation: AllocationId,
    pub producer: PeerId,
    pub byte_len: u64,
    pub element: ElementType,
}

impl WriteDescriptor {
    pub fn validate(&self, session: SessionId, producer: PeerId, capacity: u64) -> Result<()> {
        if self.producer != producer {
            return Err(SharedMemoryError::InvalidLease);
        }
        BufferDescriptor {
            version: self.version,
            session: self.session,
            allocation: self.allocation,
            lease: 0,
            recipient: producer,
            offset: 0,
            byte_len: self.byte_len,
            element: self.element,
        }
        .validate(session, capacity)
    }
}

impl BufferDescriptor {
    pub fn validate(&self, session: SessionId, capacity: u64) -> Result<()> {
        if self.version != PROTOCOL_VERSION || self.session != session {
            return Err(SharedMemoryError::WrongSession);
        }
        let size = self.element.size();
        if !self.offset.is_multiple_of(size)
            || !self.byte_len.is_multiple_of(size)
            || self
                .offset
                .checked_add(self.byte_len)
                .is_none_or(|end| end > capacity)
            || capacity > isize::MAX as u64
            || (self.element == ElementType::F32Le && !cfg!(target_endian = "little"))
        {
            return Err(SharedMemoryError::InvalidLayout);
        }
        Ok(())
    }
}
