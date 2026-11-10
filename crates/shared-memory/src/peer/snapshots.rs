use super::*;
use wire::SnapshotDescriptor;

impl Shared {
    pub(super) fn read_snapshot(
        self: &Arc<Self>,
        request: u64,
        call: u64,
        port: &str,
    ) -> io::Result<()> {
        let (reader, copies, cancelled) = {
            let mut ledger = self.ledger.lock().unwrap();
            self.check_open()?;
            if self.snapshot_protocol == 0 || request != ledger.next_snapshot {
                return Err(invalid("invalid snapshot request sequence or protocol"));
            }
            ledger.next_snapshot = request
                .checked_add(1)
                .ok_or_else(|| invalid("snapshot request ids exhausted"))?;
            let active = ledger
                .calls
                .get_mut(&call)
                .ok_or_else(|| invalid("snapshot outside a live call"))?;
            if active.snapshot_reads >= self.limits.outputs_per_call {
                return Err(invalid("snapshot request budget exhausted"));
            }
            active.snapshot_reads += 1;
            (
                active.snapshots.get(port).cloned(),
                active.copies.clone(),
                active.cancelled,
            )
        };
        let shared = self.clone();
        std::thread::Builder::new()
            .name("mapped-snapshot-read".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    shared.reply_snapshot(request, call, reader, copies, cancelled)
                }));
                if !matches!(result, Ok(Ok(()))) {
                    shared.close();
                }
            })?;
        Ok(())
    }

    fn reply_snapshot(
        &self,
        request: u64,
        call: u64,
        reader: Option<Arc<dyn remotemedia_traits::ports::SnapshotPort>>,
        copies: Option<CopyContext>,
        cancelled: bool,
    ) -> io::Result<()> {
        // A user/foreign reader and any final-owner destructor run without the
        // peer ledger lock. Snapshot publication itself is an atomic latest read.
        let prepared = (|| {
            if cancelled {
                return Err(other("snapshot call cancelled"));
            }
            let reader = reader.ok_or_else(|| other("snapshot port is not bound to this call"))?;
            let Some(snapshot) = reader.runtime_snapshot().map_err(other)? else {
                return Ok(None);
            };
            if matches!(
                &snapshot.value,
                RuntimeData::Audio {
                    samples: remotemedia_types::AudioSamples::Vec(_),
                    ..
                }
            ) {
                return Err(other("snapshot contains unshared legacy audio"));
            }
            let value = snapshot.value.clone();
            let delivery = match &copies {
                Some(copies) => self.pool.prepare_message_with_copy_context(
                    value,
                    self.peer,
                    TensorEncoding::Runtime,
                    copies,
                ),
                None => self
                    .pool
                    .prepare_message(value, self.peer, TensorEncoding::Runtime),
            }
            .map_err(other)?;
            let descriptor = SnapshotDescriptor {
                message: delivery.descriptor().clone(),
                pts_us: snapshot.pts_us,
                written_at_us: snapshot.written_at_us,
                seq: snapshot.seq,
            };
            Ok(Some((delivery, descriptor)))
        })();
        let (delivery, descriptor) = match prepared {
            Ok(Some(value)) => value,
            Ok(None) => {
                return self.finish_snapshot(
                    call,
                    &HostMessage::Snapshot {
                        request,
                        result: Ok(None),
                    },
                    None,
                )
            }
            Err(error) => {
                return self.finish_snapshot(
                    call,
                    &HostMessage::Snapshot {
                        request,
                        result: Err(error.to_string()),
                    },
                    None,
                )
            }
        };
        let cancelled = {
            let mut ledger = self.ledger.lock().unwrap();
            self.check_open()?;
            let active = ledger
                .calls
                .get(&call)
                .ok_or_else(|| invalid("snapshot call disappeared"))?;
            if active.cancelled {
                true
            } else {
                if let Some(buffer) = &descriptor.message.buffer {
                    ledger.inputs.insert(
                        buffer.lease,
                        Input {
                            descriptor: buffer.clone(),
                            accepted: false,
                            storage: delivery.storage(),
                        },
                    );
                }
                false
            }
        };
        if cancelled {
            return self.finish_snapshot(
                call,
                &HostMessage::Snapshot {
                    request,
                    result: Err("snapshot call cancelled".into()),
                },
                None,
            );
        }
        // SAFETY: this live call's connection owns the exact recipient lease and
        // original child. Errors fault the connection and retain until confirmed death.
        let (_, file) = unsafe { delivery.into_transport() };
        self.finish_snapshot(
            call,
            &HostMessage::Snapshot {
                request,
                result: Ok(Some(descriptor)),
            },
            file.as_ref().map(|file| (file, HandleAccess::Read)),
        )
    }
    fn finish_snapshot(
        &self,
        call: u64,
        message: &HostMessage,
        file: Option<(&File, HandleAccess)>,
    ) -> io::Result<()> {
        {
            let mut ledger = self.ledger.lock().unwrap();
            self.check_open()?;
            let active = ledger
                .calls
                .get_mut(&call)
                .ok_or_else(|| invalid("snapshot call disappeared"))?;
            active.snapshot_reads = active
                .snapshot_reads
                .checked_sub(1)
                .ok_or_else(|| invalid("snapshot completion without a read"))?;
        }
        self.send(message, file)
    }
}
