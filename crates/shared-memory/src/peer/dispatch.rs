use super::*;
use remotemedia_types::PayloadKind;
use wire::{BufferReference, OutputMessage};

impl Shared {
    pub(super) fn dispatch(
        self: &Arc<Self>,
        message: WorkerMessage,
    ) -> io::Result<Option<PeerEvent>> {
        match message {
            WorkerMessage::Fatal { message } => {
                Err(other(format!("worker protocol failure: {message}")))
            }
            WorkerMessage::Ready {
                version,
                session,
                peer,
                modes,
                snapshot_protocol,
                output_copy_protocol,
                source_completion_protocol,
                background_protocol,
            } => {
                let mut ledger = self.ledger.lock().unwrap();
                self.check_open()?;
                let unique: HashSet<_> = modes.iter().copied().collect();
                if ledger.modes.is_some()
                    || version != wire::WIRE_VERSION
                    || snapshot_protocol != self.snapshot_protocol
                    || output_copy_protocol != self.output_copy_protocol
                    || source_completion_protocol != self.source_completion_protocol
                    // A worker may decline background calls (its node has
                    // none to run); it may not invent them.
                    || (background_protocol != 0
                        && background_protocol != self.background_protocol)
                    || (modes.contains(&CallKind::Background)
                        != (background_protocol == wire::BACKGROUND_PROTOCOL))
                    || session != self.pool.session_id()
                    || peer != self.peer
                    || modes.is_empty()
                    || unique.len() != modes.len()
                {
                    return Err(invalid("invalid worker capability handshake"));
                }
                ledger.modes = Some(unique);
                Ok(Some(PeerEvent::Ready { modes }))
            }
            WorkerMessage::InputAccepted { lease } => {
                let mut ledger = self.ledger.lock().unwrap();
                self.check_open()?;
                let input = ledger
                    .inputs
                    .get_mut(&lease)
                    .ok_or_else(|| invalid("unknown input acceptance"))?;
                if input.accepted {
                    return Err(invalid("duplicate input acceptance"));
                }
                self.pool.accept(&input.descriptor).map_err(other)?;
                input.accepted = true;
                Ok(None)
            }
            WorkerMessage::InputReleased { lease } => {
                let mut ledger = self.ledger.lock().unwrap();
                self.check_open()?;
                let input = ledger
                    .inputs
                    .get(&lease)
                    .ok_or_else(|| invalid("unknown or duplicate input release"))?;
                // SAFETY: authenticated peer's callback acknowledges final unmap;
                // only the connection-owned descriptor can reach the broker.
                unsafe { self.pool.release_remote(&input.descriptor) }.map_err(other)?;
                let removed = ledger.inputs.remove(&lease);
                drop(ledger);
                drop(removed);
                Ok(None)
            }
            WorkerMessage::ProducerReleased { allocation } => {
                let mut ledger = self.ledger.lock().unwrap();
                self.check_open()?;
                let producer = ledger
                    .producers
                    .get(&allocation)
                    .ok_or_else(|| invalid("unknown or duplicate producer release"))?;
                if producer.copy.is_some() {
                    return Err(invalid("producer released before copy completion"));
                }
                // SAFETY: the connection validates this live producer record;
                // publication is not a release, only final unmap sends this event.
                unsafe { self.pool.release_producer(&producer.descriptor) }.map_err(other)?;
                let removed = ledger.producers.remove(&allocation);
                drop(ledger);
                drop(removed);
                Ok(None)
            }
            WorkerMessage::ReadSnapshot {
                request,
                call,
                port,
            } => {
                self.read_snapshot(request, call, &port)?;
                Ok(None)
            }
            WorkerMessage::Allocate {
                request,
                call,
                element,
                elements,
            } => {
                self.allocate(request, call, element, elements, false)?;
                Ok(None)
            }
            WorkerMessage::AllocateCopy {
                request,
                call,
                element,
                elements,
            } => {
                self.allocate(request, call, element, elements, true)?;
                Ok(None)
            }
            WorkerMessage::CopyFinished {
                call,
                allocation,
                success,
            } => {
                self.finish_copy(call, allocation, success)?;
                Ok(None)
            }
            WorkerMessage::Output {
                call,
                sequence,
                message,
            } => self.output(call, sequence, message),
            WorkerMessage::Complete {
                call,
                result,
                source_finished,
            } => self.complete_call(call, result, source_finished).map(Some),
        }
    }

    fn complete_call(
        &self,
        call: u64,
        mut result: std::result::Result<(), String>,
        mut source_finished: bool,
    ) -> io::Result<PeerEvent> {
        let mut ledger = self.ledger.lock().unwrap();
        self.check_open()?;
        let active = ledger
            .calls
            .get(&call)
            .ok_or_else(|| invalid("unknown or duplicate call completion"))?;
        if source_finished {
            if self.source_completion_protocol != wire::SOURCE_COMPLETION_PROTOCOL
                || active.kind != CallKind::Tick
                || result.is_err()
            {
                return Err(invalid("invalid source completion"));
            }
            // Cancellation and completion can cross on opposite pipes.
            // Preserve this call's cancellation without killing peers.
            if active.cancelled {
                source_finished = false;
                result = Err("source tick cancelled".into());
            }
        }
        if active.outstanding != 0 || active.snapshot_reads != 0 || active.pending_copies != 0 {
            return Err(invalid(
                "worker completed before pending operation acknowledgement",
            ));
        }
        let completed = ledger.calls.remove(&call);
        drop(ledger);
        drop(completed);
        Ok(PeerEvent::Complete {
            call,
            result,
            source_finished,
        })
    }

    fn output(
        self: &Arc<Self>,
        call: u64,
        sequence: u64,
        message: OutputMessage,
    ) -> io::Result<Option<PeerEvent>> {
        let (data, cancelled) = {
            let mut ledger = self.ledger.lock().unwrap();
            self.check_open()?;
            if sequence != ledger.next_output {
                return Err(invalid("invalid worker output sequence"));
            }
            let active = ledger
                .calls
                .get(&call)
                .ok_or_else(|| invalid("output outside a live call"))?;
            if active.outstanding >= self.limits.outputs_per_call {
                return Err(invalid("worker exceeded output credits"));
            }
            let cancelled = active.cancelled;
            let data = self.decode_output(&mut ledger, message)?;
            ledger.next_output = sequence
                .checked_add(1)
                .ok_or_else(|| invalid("output ids exhausted"))?;
            ledger.outputs.insert(sequence, call);
            ledger.calls.get_mut(&call).unwrap().outstanding += 1;
            (data, cancelled)
        };
        let output = ReceivedOutput {
            call,
            sequence,
            data: Some(data),
            connection: Arc::downgrade(self),
        };
        if cancelled {
            drop(output);
            Ok(None)
        } else {
            Ok(Some(PeerEvent::Output(output)))
        }
    }

    fn decode_output(
        &self,
        ledger: &mut Ledger,
        message: OutputMessage,
    ) -> io::Result<RuntimeData> {
        let storage = match message.buffer {
            None => PayloadStorage::None,
            Some(BufferReference::Input {
                lease,
                offset,
                byte_len,
            }) => {
                let input = ledger
                    .inputs
                    .get(&lease)
                    .filter(|input| input.accepted)
                    .ok_or_else(|| invalid("output references an unaccepted input"))?;
                validate_layout(&message.metadata, input.descriptor.element, byte_len)?;
                // The authenticated lease identifies the exact exported host
                // view. Returning a checked subview preserves its provenance and
                // avoids a second mapping/lease. The worker release is independent
                // of this retained host owner.
                slice_storage(&input.storage, offset, byte_len)?
            }
            Some(BufferReference::Producer {
                allocation,
                offset,
                byte_len,
            }) => {
                let producer = ledger
                    .producers
                    .get_mut(&allocation)
                    .ok_or_else(|| invalid("output references an unknown producer"))?;
                if producer.copy.is_some() || producer.copy_failed {
                    return Err(invalid(
                        "output published before successful copy completion",
                    ));
                }
                validate_layout(&message.metadata, producer.descriptor.element, byte_len)?;
                if offset
                    .checked_add(byte_len)
                    .is_none_or(|end| end > producer.descriptor.byte_len)
                    || !offset.is_multiple_of(producer.descriptor.element.size())
                {
                    return Err(invalid("output widens producer range"));
                }
                if producer.published.is_none() {
                    // SAFETY: trusted worker output preparation accepts only
                    // frozen owners; this exact producer grant belongs to it.
                    producer.published = Some(if let Some(pending) = producer.pending.take() {
                        // SAFETY: the same authenticated freeze contract, with
                        // the actual host-selected loan retained since its grant.
                        unsafe { pending.publish(&self.pool, &producer.descriptor) }
                            .map_err(other)?
                    } else {
                        match producer.descriptor.element {
                            ElementType::Bytes => PayloadStorage::Bytes(
                                unsafe { self.pool.publish_remote::<u8>(&producer.descriptor) }
                                    .map_err(other)?,
                            ),
                            ElementType::F32Le => PayloadStorage::Audio(
                                unsafe { self.pool.publish_remote::<f32>(&producer.descriptor) }
                                    .map_err(other)?,
                            ),
                        }
                    });
                }
                slice_storage(producer.published.as_ref().unwrap(), offset, byte_len)?
            }
        };
        message
            .metadata
            .attach(storage, TensorEncoding::Runtime)
            .map_err(other)
    }
}

/// Offsets are relative to the retained exported view, never its wider backing
/// allocation. Validate before conversion, including overflow and f32 alignment.
fn slice_storage(storage: &PayloadStorage, offset: u64, bytes: u64) -> io::Result<PayloadStorage> {
    let end = offset
        .checked_add(bytes)
        .ok_or_else(|| invalid("output range overflows"))?;
    let start =
        usize::try_from(offset).map_err(|_| invalid("output offset exceeds address space"))?;
    let end = usize::try_from(end).map_err(|_| invalid("output end exceeds address space"))?;
    match storage {
        PayloadStorage::Bytes(data) => data
            .slice(start..end)
            .map(PayloadStorage::Bytes)
            .map_err(other),
        PayloadStorage::Audio(data) if start % 4 == 0 && end % 4 == 0 => data
            .slice(start / 4..end / 4)
            .map(PayloadStorage::Audio)
            .map_err(other),
        _ => Err(invalid("output storage or alignment mismatch")),
    }
}

fn validate_layout(
    metadata: &remotemedia_types::RuntimeMetadata,
    element: ElementType,
    byte_len: u64,
) -> io::Result<()> {
    let kind = match element {
        ElementType::Bytes => PayloadKind::Bytes,
        ElementType::F32Le => PayloadKind::AudioF32,
    };
    metadata
        .validate_payload(Some(kind), byte_len, TensorEncoding::Runtime)
        .map_err(other)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returned_ranges_cannot_widen_retained_views_overflow_or_misalign() {
        let backing: remotemedia_types::SharedBuffer<u8> = vec![7u8; 32].into();
        let bytes = PayloadStorage::Bytes(backing.slice(8..24).unwrap());
        let PayloadStorage::Bytes(view) = slice_storage(&bytes, 2, 12).unwrap() else {
            panic!()
        };
        assert_eq!(view.as_ptr(), backing.as_ptr().wrapping_add(10));
        assert!(slice_storage(&bytes, 0, 17).is_err());
        assert!(slice_storage(&bytes, 17, 0).is_err());
        assert!(slice_storage(&bytes, u64::MAX, 2).is_err());
        assert!(slice_storage(&bytes, 16, 0).is_ok());

        let backing: remotemedia_types::SharedBuffer<f32> = vec![0.75f32; 32].into();
        let audio = PayloadStorage::Audio(backing.slice(8..24).unwrap());
        let PayloadStorage::Audio(view) = slice_storage(&audio, 8, 48).unwrap() else {
            panic!()
        };
        assert_eq!(view.as_ptr(), backing.as_ptr().wrapping_add(10));
        assert!(slice_storage(&audio, 1, 4).is_err());
        assert!(slice_storage(&audio, 4, 1).is_err());
        assert!(slice_storage(&audio, 0, 68).is_err());
        assert!(slice_storage(&audio, 64, 0).is_ok());
        assert!(slice_storage(&PayloadStorage::None, 0, 0).is_err());
    }
}
