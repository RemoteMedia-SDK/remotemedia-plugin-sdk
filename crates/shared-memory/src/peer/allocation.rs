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
        copying: bool,
    ) -> io::Result<()> {
        if copying && self.output_copy_protocol != wire::OUTPUT_COPY_PROTOCOL {
            return Err(invalid("output copies were not negotiated"));
        }
        let (cancelled, allocator, copies, pending_copies) = {
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
            (
                active.cancelled,
                active.output_allocator.clone(),
                active.output_copies.clone(),
                active.pending_copies,
            )
        };
        if cancelled {
            return self.send(
                &HostMessage::AllocationFailed {
                    request,
                    reason: AllocationFailure::Unavailable,
                },
                None,
            );
        }
        let mut copy = if copying {
            match authorize(
                copies.as_ref(),
                element,
                elements,
                pending_copies,
                self.limits.outputs_per_call,
            ) {
                Ok(operation) => Some(operation),
                Err(reason) => {
                    return self.send(&HostMessage::AllocationFailed { request, reason }, None)
                }
            }
        } else {
            None
        };
        // Custom providers run without the connection lock. Cancellation may
        // arrive while they allocate; validate the call again before dispatch.
        let mut grant = self.reserve_output(allocator.as_deref(), element, elements);
        if let Ok(granted) = &mut grant {
            if !self.register_output(call, granted, &mut copy)? {
                grant = Err(AllocationFailure::Unavailable);
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

    /// Commit a reservation only while its call remains live. On cancellation,
    /// the caller drops both the loan and external copy ledger outside this lock.
    fn register_output(
        &self,
        call: u64,
        grant: &mut crate::WriteGrant,
        copy: &mut Option<remotemedia_types::CopyOperation>,
    ) -> io::Result<bool> {
        let mut ledger = self.ledger.lock().unwrap();
        self.check_open()?;
        let active = ledger
            .calls
            .get_mut(&call)
            .ok_or_else(|| invalid("allocation call disappeared"))?;
        if active.cancelled {
            return Ok(false);
        }
        if copy.is_some() {
            active.pending_copies += 1;
        }
        let descriptor = grant.descriptor().clone();
        ledger.producers.insert(
            descriptor.allocation,
            Producer {
                descriptor,
                published: None,
                pending: grant.publication.take(),
                copy: copy.take().map(|operation| (call, operation)),
                copy_failed: false,
            },
        );
        Ok(true)
    }

    pub(super) fn finish_copy(
        &self,
        call: u64,
        allocation: AllocationId,
        success: bool,
    ) -> io::Result<()> {
        let operation = {
            let mut ledger = self.ledger.lock().unwrap();
            self.check_open()?;
            let producer = ledger
                .producers
                .get(&allocation)
                .ok_or_else(|| invalid("copy completion for unknown allocation"))?;
            if !producer
                .copy
                .as_ref()
                .is_some_and(|(owner, _)| *owner == call)
            {
                return Err(invalid("copy completion has wrong call or is duplicated"));
            }
            let active = ledger
                .calls
                .get_mut(&call)
                .ok_or_else(|| invalid("copy completion outside a live call"))?;
            active.pending_copies = active
                .pending_copies
                .checked_sub(1)
                .ok_or_else(|| invalid("unbalanced output copy completion"))?;
            let producer = ledger.producers.get_mut(&allocation).unwrap();
            let (_, operation) = producer.copy.take().unwrap();
            producer.copy_failed = !success;
            operation
        };
        if success {
            operation.complete();
        }
        Ok(())
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

fn authorize(
    copies: Option<&CopyContext>,
    element: ElementType,
    elements: u64,
    pending: usize,
    limit: usize,
) -> Result<remotemedia_types::CopyOperation, AllocationFailure> {
    let bytes = elements
        .checked_mul(element.size())
        .filter(|bytes| *bytes <= isize::MAX as u64)
        .ok_or(AllocationFailure::InvalidLength)?;
    if pending >= limit {
        return Err(AllocationFailure::Exhausted);
    }
    // BorrowedInput means materializing a borrowed source into an owning output,
    // attributed to the host's output context, not its IPC input-staging ledger.
    copies
        .ok_or(AllocationFailure::CopyDenied)?
        .begin(remotemedia_types::CopyReason::BorrowedInput, bytes)
        .map_err(|_| AllocationFailure::CopyDenied)
}
