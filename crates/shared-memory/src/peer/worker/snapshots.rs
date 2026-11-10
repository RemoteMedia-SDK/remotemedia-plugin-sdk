use super::*;

impl WorkerRequest {
    pub fn snapshot_port_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.snapshot_ports.iter().cloned().collect();
        names.sort();
        names
    }

    /// Synchronously sample a live host publication. Returned immutable views
    /// retain their read leases independently of the request and later reads.
    /// Call from application threads, never the endpoint receive thread.
    pub fn read_snapshot(&self, port: &str) -> io::Result<Option<RuntimeSnapshot>> {
        if self.shared.snapshot_protocol == 0 || !self.snapshot_ports.contains(port) {
            return Err(invalid("snapshot port not negotiated for this call"));
        }
        let (reply, wait) = mpsc::sync_channel(1);
        {
            let mut outbound = self.shared.outbound.lock().unwrap();
            let mut state = self.shared.state.lock().unwrap();
            self.shared.check_open()?;
            let active = state
                .calls
                .get_mut(&self.call)
                .ok_or_else(|| invalid("snapshot after call completion"))?;
            if active.cancelled.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "snapshot call cancelled",
                ));
            }
            if active.snapshot_reads >= self.shared.limits.outputs_per_call {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "snapshot request budget exhausted",
                ));
            }
            let request = outbound.next_snapshot;
            let next = request
                .checked_add(1)
                .ok_or_else(|| invalid("snapshot request ids exhausted"))?;
            let bytes = encode(&WorkerMessage::ReadSnapshot {
                request,
                call: self.call,
                port: port.into(),
            })?;
            active.snapshot_reads += 1;
            state.snapshots.insert(request, PendingSnapshot { reply });
            outbound.next_snapshot = next;
            drop(state);
            if let Err(error) = outbound.send(bytes) {
                drop(outbound);
                self.shared.fail(&error.to_string());
            }
        }
        let result = wait.recv().unwrap_or_else(|_| {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "snapshot response lost",
            ))
        });
        if let Some(active) = self.shared.state.lock().unwrap().calls.get_mut(&self.call) {
            active.snapshot_reads -= 1;
        }
        result
    }
}

impl SharedWorker {
    pub(super) fn receive_snapshot(
        self: &Arc<Self>,
        request: u64,
        result: Result<Option<SnapshotDescriptor>, String>,
        frame: Frame,
    ) -> io::Result<()> {
        let pending = self
            .state
            .lock()
            .unwrap()
            .snapshots
            .remove(&request)
            .ok_or_else(|| invalid("unknown snapshot response"))?;
        let reply = match result {
            Ok(Some(snapshot)) => {
                let data = self.import_readonly(snapshot.message, frame)?;
                Ok(Some(Arc::new(
                    remotemedia_traits::ports::TimestampedSnapshot {
                        value: data,
                        pts_us: snapshot.pts_us,
                        written_at_us: snapshot.written_at_us,
                        seq: snapshot.seq,
                    },
                )))
            }
            empty => {
                if frame.file.is_some() || frame.access.is_some() {
                    return Err(invalid("unexpected empty snapshot handle"));
                }
                empty.map(|_| None).map_err(other)
            }
        };
        // If the application receiver disappeared, dropping the reply releases
        // its owning view outside the state lock.
        let _ = pending.reply.send(reply);
        Ok(())
    }
}
