//! Ownership state only; OS mappings and authenticated process handles live in
//! the provider. No expiry clock can release an active or pending lease.

use crate::descriptor::{
    AllocationId, BufferDescriptor, ElementType, PeerId, SessionId, WriteDescriptor,
    PROTOCOL_VERSION,
};
use crate::{Result, SharedMemoryError};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Free,
    Writing,
    Published,
    Retired,
}

#[derive(Debug)]
struct Slot {
    capacity: u64,
    generation: u64,
    phase: Phase,
    local_owner: bool,
    producer: Option<WriteDescriptor>,
    leases: usize,
    byte_len: u64,
    element: ElementType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryState {
    Pending,
    Accepted,
}

#[derive(Debug)]
struct Delivery {
    descriptor: BufferDescriptor,
    state: DeliveryState,
}

pub(crate) struct Broker {
    session: SessionId,
    slots: Vec<Slot>,
    deliveries: HashMap<u64, Delivery>,
    next_lease: u64,
    max_leases: usize,
    closed: bool,
}

impl Broker {
    pub(crate) fn new(session: SessionId, capacities: &[u64], max_leases: usize) -> Result<Self> {
        if capacities.is_empty()
            || capacities.len() > u32::MAX as usize
            || max_leases == 0
            || capacities.iter().any(|&n| n == 0 || n > isize::MAX as u64)
        {
            return Err(SharedMemoryError::InvalidLayout);
        }
        Ok(Self {
            session,
            slots: capacities
                .iter()
                .map(|&capacity| Slot {
                    capacity,
                    generation: 1,
                    phase: Phase::Free,
                    local_owner: false,
                    producer: None,
                    leases: 0,
                    byte_len: 0,
                    element: ElementType::Bytes,
                })
                .collect(),
            deliveries: HashMap::new(),
            next_lease: 1,
            max_leases,
            closed: false,
        })
    }

    fn slot(&self, id: AllocationId) -> Result<&Slot> {
        self.slots
            .get(id.slot as usize)
            .filter(|s| {
                s.generation == id.generation
                    && matches!(s.phase, Phase::Writing | Phase::Published)
            })
            .ok_or(SharedMemoryError::StaleAllocation)
    }

    pub(crate) fn allocate(&mut self, byte_len: u64, element: ElementType) -> Result<AllocationId> {
        if self.closed {
            return Err(SharedMemoryError::Closed);
        }
        if !byte_len.is_multiple_of(element.size())
            || (element == ElementType::F32Le && !cfg!(target_endian = "little"))
        {
            return Err(SharedMemoryError::InvalidLayout);
        }
        let (index, slot) = self
            .slots
            .iter_mut()
            .enumerate()
            .filter(|(_, s)| s.phase == Phase::Free && s.capacity >= byte_len)
            .min_by_key(|(_, s)| s.capacity)
            .ok_or(SharedMemoryError::Exhausted)?;
        slot.phase = Phase::Writing;
        slot.local_owner = true;
        slot.byte_len = byte_len;
        slot.element = element;
        Ok(AllocationId {
            slot: index as u32,
            generation: slot.generation,
        })
    }

    /// Called only through an immutable SharedBuffer, never through a writer.
    pub(crate) fn publish(&mut self, id: AllocationId) -> Result<()> {
        self.slot(id)?;
        self.slots[id.slot as usize].phase = Phase::Published;
        Ok(())
    }

    /// Move an initialized local reservation to exactly one process incarnation.
    pub(crate) fn handoff_writer(
        &mut self,
        id: AllocationId,
        peer: PeerId,
    ) -> Result<WriteDescriptor> {
        if self.closed {
            return Err(SharedMemoryError::Closed);
        }
        let slot = self.slot(id)?;
        if slot.phase != Phase::Writing || !slot.local_owner || slot.producer.is_some() {
            return Err(SharedMemoryError::InvalidLease);
        }
        let descriptor = WriteDescriptor {
            version: PROTOCOL_VERSION,
            session: self.session,
            allocation: id,
            producer: peer,
            byte_len: slot.byte_len,
            element: slot.element,
        };
        let slot = &mut self.slots[id.slot as usize];
        slot.local_owner = false;
        slot.producer = Some(descriptor.clone());
        Ok(descriptor)
    }

    fn producer(&self, descriptor: &WriteDescriptor) -> Result<&Slot> {
        let slot = self.slot(descriptor.allocation)?;
        descriptor.validate(self.session, descriptor.producer, slot.byte_len)?;
        if slot.producer.as_ref() != Some(descriptor) {
            return Err(SharedMemoryError::InvalidLease);
        }
        Ok(slot)
    }

    pub(crate) fn validate_writer(&self, descriptor: &WriteDescriptor) -> Result<()> {
        self.producer(descriptor).map(|_| ())
    }

    /// Caller established immutable publication, not just receipt of a message.
    pub(crate) fn publish_remote(&mut self, descriptor: &WriteDescriptor) -> Result<()> {
        if self.closed {
            return Err(SharedMemoryError::Closed);
        }
        if self.producer(descriptor)?.phase != Phase::Writing {
            return Err(SharedMemoryError::InvalidLease);
        }
        let slot = &mut self.slots[descriptor.allocation.slot as usize];
        slot.phase = Phase::Published;
        slot.local_owner = true;
        // Retain the producer independently: its frozen views may still exist.
        Ok(())
    }

    pub(crate) fn release_producer(&mut self, descriptor: &WriteDescriptor) -> Result<()> {
        self.producer(descriptor)?;
        self.slots[descriptor.allocation.slot as usize].producer = None;
        self.recycle(descriptor.allocation.slot);
        Ok(())
    }

    pub(crate) fn grant(
        &mut self,
        id: AllocationId,
        recipient: PeerId,
        offset: u64,
        byte_len: u64,
    ) -> Result<BufferDescriptor> {
        if self.closed {
            return Err(SharedMemoryError::Closed);
        }
        let slot = self.slot(id)?;
        if slot.phase != Phase::Published {
            return Err(SharedMemoryError::NotPublished);
        }
        let descriptor = BufferDescriptor {
            version: PROTOCOL_VERSION,
            session: self.session,
            allocation: id,
            lease: self.next_lease,
            recipient,
            offset,
            byte_len,
            element: slot.element,
        };
        descriptor.validate(self.session, slot.byte_len)?;
        if self.deliveries.len() >= self.max_leases {
            return Err(SharedMemoryError::Exhausted);
        }
        let next = self
            .next_lease
            .checked_add(1)
            .ok_or(SharedMemoryError::Exhausted)?;
        self.deliveries.insert(
            descriptor.lease,
            Delivery {
                descriptor: descriptor.clone(),
                state: DeliveryState::Pending,
            },
        );
        self.next_lease = next;
        self.slots[id.slot as usize].leases += 1;
        Ok(descriptor)
    }

    fn delivery(&self, descriptor: &BufferDescriptor) -> Result<&Delivery> {
        let slot = self.slot(descriptor.allocation)?;
        descriptor.validate(self.session, slot.byte_len)?;
        self.deliveries
            .get(&descriptor.lease)
            .filter(|d| d.descriptor == *descriptor)
            .ok_or(SharedMemoryError::InvalidLease)
    }

    pub(crate) fn accept(&mut self, descriptor: &BufferDescriptor) -> Result<()> {
        self.delivery(descriptor)?;
        self.deliveries.get_mut(&descriptor.lease).unwrap().state = DeliveryState::Accepted;
        Ok(())
    }

    /// Forward only a range owned by an accepted lease, never widen its access.
    pub(crate) fn forward(
        &mut self,
        source: &BufferDescriptor,
        recipient: PeerId,
        offset: u64,
        byte_len: u64,
    ) -> Result<BufferDescriptor> {
        if self.delivery(source)?.state != DeliveryState::Accepted {
            return Err(SharedMemoryError::InvalidLease);
        }
        let end = offset
            .checked_add(byte_len)
            .ok_or(SharedMemoryError::InvalidLayout)?;
        if offset < source.offset || end > source.offset + source.byte_len {
            return Err(SharedMemoryError::InvalidLayout);
        }
        self.grant(source.allocation, recipient, offset, byte_len)
    }

    /// Provider calls this after final local owner drop, including abandoned loans.
    pub(crate) fn release_local(&mut self, id: AllocationId) -> Result<()> {
        if !self.slot(id)?.local_owner {
            return Err(SharedMemoryError::InvalidLease);
        }
        self.slots[id.slot as usize].local_owner = false;
        self.recycle(id.slot);
        Ok(())
    }

    /// Provider must establish final remote owner release or confirmed process
    /// termination. A timeout, sender drop, or failed acknowledgement is insufficient.
    pub(crate) fn release_remote(&mut self, descriptor: &BufferDescriptor) -> Result<()> {
        self.delivery(descriptor)?;
        self.deliveries.remove(&descriptor.lease);
        self.slots[descriptor.allocation.slot as usize].leases -= 1;
        self.recycle(descriptor.allocation.slot);
        Ok(())
    }

    /// Invoked only with verified termination of this exact process incarnation.
    pub(crate) fn release_dead_peer(&mut self, peer: PeerId) {
        let descriptors: Vec<_> = self
            .deliveries
            .values()
            .filter(|d| d.descriptor.recipient == peer)
            .map(|d| d.descriptor.clone())
            .collect();
        for descriptor in descriptors {
            let _ = self.release_remote(&descriptor);
        }
        let writers: Vec<_> = self
            .slots
            .iter()
            .filter_map(|slot| {
                slot.producer
                    .as_ref()
                    .filter(|d| d.producer == peer)
                    .cloned()
            })
            .collect();
        for descriptor in writers {
            let _ = self.release_producer(&descriptor);
        }
    }

    fn recycle(&mut self, index: u32) {
        let slot = &mut self.slots[index as usize];
        if !slot.local_owner && slot.leases == 0 && slot.producer.is_none() {
            if let Some(next) = slot.generation.checked_add(1) {
                slot.generation = next;
                slot.phase = Phase::Free;
            } else {
                slot.phase = Phase::Retired;
            }
        }
    }

    pub(crate) fn close(&mut self) {
        self.closed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_exhaustion_retires_instead_of_wrapping() {
        let mut broker = Broker::new(SessionId([0; 16]), &[16], 1).unwrap();
        broker.slots[0].generation = u64::MAX;
        let id = broker.allocate(4, ElementType::Bytes).unwrap();
        broker.release_local(id).unwrap();
        assert_eq!(broker.slots[0].phase, Phase::Retired);
        assert!(matches!(
            broker.allocate(4, ElementType::Bytes),
            Err(SharedMemoryError::Exhausted)
        ));
    }

    #[test]
    fn lease_identifier_exhaustion_does_not_register_a_partial_grant() {
        let mut broker = Broker::new(SessionId([0; 16]), &[16], 1).unwrap();
        let id = broker.allocate(4, ElementType::Bytes).unwrap();
        broker.publish(id).unwrap();
        broker.next_lease = u64::MAX;
        assert!(matches!(
            broker.grant(id, PeerId(1), 0, 4),
            Err(SharedMemoryError::Exhausted)
        ));
        assert!(broker.deliveries.is_empty());
        broker.release_local(id).unwrap();
        assert!(broker.allocate(4, ElementType::Bytes).is_ok());
    }
}
