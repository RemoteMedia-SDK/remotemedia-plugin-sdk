//! Remote producer grants use the exact selected host allocator when supplied.
use super::*;
use wire::AllocationFailure;

impl Shared {
    pub(super) fn allocate(
        &self,
        request: u64,
        call: u64,
        element: ElementType,
        elements: u64,
    ) -> io::Result<()> {
        let (cancelled, allocator) = {
            let mut ledger = self.ledger.lock().unwrap();
            self.check_open()?;
            if ledger.modes.is_none() || request != ledger.next_allocation {
                return Err(invalid("invalid worker allocation sequence"));
            }
            ledger.next_allocation = request
                .checked_add(1)
                .ok_or_else(|| invalid("allocation ids exhausted"))?;
            let active = ledger
                .calls
                .get(&call)
                .ok_or_else(|| invalid("allocation outside a live call"))?;
            (active.cancelled, active.output_allocator.clone())
        };
        // Custom providers run without the connection lock. Cancellation may
        // arrive while they allocate; validate the call again before dispatch.
        let mut grant = if cancelled {
            Err(AllocationFailure::Unavailable)
        } else {
            self.reserve_output(allocator.as_deref(), element, elements)
        };
        if let Ok(granted) = &mut grant {
            let mut ledger = self.ledger.lock().unwrap();
            self.check_open()?;
            let active = ledger
                .calls
                .get(&call)
                .ok_or_else(|| invalid("allocation call disappeared"))?;
            if active.cancelled {
                drop(ledger);
                grant = Err(AllocationFailure::Unavailable);
            } else {
                let descriptor = granted.descriptor().clone();
                ledger.producers.insert(
                    descriptor.allocation,
                    Producer {
                        descriptor,
                        published: None,
                        pending: granted.publication.take(),
                    },
                );
            }
        }
        match grant {
            Ok(grant) => {
                let descriptor = grant.descriptor().clone();
                // SAFETY: recorded reservation and private child handle are bound
                // here; any partial send faults/terminates the connection.
                let (file, _) = unsafe { grant.into_transport() };
                self.send(
                    &HostMessage::Grant {
                        request,
                        descriptor,
                    },
                    Some((&file, HandleAccess::ReadWrite)),
                )
            }
            Err(reason) => self.send(&HostMessage::AllocationFailed { request, reason }, None),
        }
    }

    fn reserve_output(
        &self,
        allocator: Option<&dyn remotemedia_types::OutputAllocator>,
        element: ElementType,
        elements: u64,
    ) -> Result<crate::WriteGrant, AllocationFailure> {
        let elements = usize::try_from(elements).map_err(|_| AllocationFailure::InvalidLength)?;
        let result = if let Some(allocator) = allocator {
            self.pool
                .allocate_remote_with(allocator, element, elements, self.peer)
        } else {
            match element {
                ElementType::Bytes => self.pool.allocate_remote::<u8>(elements, self.peer),
                ElementType::F32Le => self.pool.allocate_remote::<f32>(elements, self.peer),
            }
        };
        result.map_err(|error| match error {
            crate::SharedMemoryError::InvalidLayout => AllocationFailure::InvalidLength,
            crate::SharedMemoryError::Exhausted => AllocationFailure::Exhausted,
            _ => AllocationFailure::Unavailable,
        })
    }
}
