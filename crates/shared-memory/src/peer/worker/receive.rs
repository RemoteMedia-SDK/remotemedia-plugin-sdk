use super::*;

impl SharedWorker {
    pub(super) fn receive_loop(
        self: &Arc<Self>,
        receiver: &mut Receiver,
        requests: mpsc::Sender<WorkerRequest>,
    ) -> io::Result<()> {
        loop {
            let frame = receiver.receive()?;
            let message: HostMessage = serde_json::from_slice(&frame.metadata).map_err(other)?;
            if !matches!(
                message,
                HostMessage::Input { .. }
                    | HostMessage::Grant { .. }
                    | HostMessage::Snapshot { .. }
            ) && (frame.file.is_some() || frame.access.is_some())
            {
                return Err(invalid("unexpected broker file grant"));
            }
            match message {
                HostMessage::Hello { .. } => return Err(invalid("duplicate broker handshake")),
                HostMessage::Begin {
                    call,
                    kind,
                    context,
                    arguments,
                    inputs,
                    snapshot_ports,
                } => {
                    let mut state = self.state.lock().unwrap();
                    self.check_open()?;
                    if call <= state.last_call
                        || !self.modes.contains(&kind)
                        || state.calls.len() >= self.limits.calls
                        || inputs > self.limits.inputs_per_call
                    {
                        return Err(invalid("invalid worker call declaration"));
                    }
                    let snapshot_set: HashSet<_> = snapshot_ports.iter().cloned().collect();
                    if snapshot_set.len() != snapshot_ports.len()
                        || snapshot_set.len() > self.limits.inputs_per_call
                        || (!snapshot_set.is_empty() && self.snapshot_protocol == 0)
                    {
                        return Err(invalid("invalid snapshot call bindings"));
                    }
                    state.last_call = call;
                    state.calls.insert(
                        call,
                        Active {
                            snapshot_reads: 0,
                            cancelled: Arc::new(AtomicBool::new(false)),
                            allocations: 0,
                            outputs: 0,
                        },
                    );
                    state.staged.insert(
                        call,
                        Staged {
                            snapshot_ports: snapshot_set,
                            kind,
                            context,
                            arguments,
                            expected: inputs,
                            inputs: HashMap::new(),
                        },
                    );
                }
                HostMessage::Input {
                    call,
                    name,
                    message,
                } => self.receive_input(call, name, message, frame)?,
                HostMessage::Execute { call } => {
                    let request = {
                        let mut state = self.state.lock().unwrap();
                        let staged = state
                            .staged
                            .get(&call)
                            .ok_or_else(|| invalid("execute without staged call"))?;
                        if staged.inputs.len() != staged.expected {
                            return Err(invalid("execute before all inputs arrived"));
                        }
                        let staged = state.staged.remove(&call).unwrap();
                        let cancelled = state.calls.get(&call).unwrap().cancelled.clone();
                        WorkerRequest {
                            snapshot_ports: staged.snapshot_ports,
                            call,
                            kind: staged.kind,
                            context: staged.context,
                            arguments: staged.arguments,
                            inputs: staged.inputs,
                            shared: self.clone(),
                            cancelled,
                            completed: AtomicBool::new(false),
                        }
                    };
                    requests
                        .send(request)
                        .map_err(|_| invalid("worker application receiver closed"))?;
                }
                HostMessage::Snapshot { request, result } => {
                    self.receive_snapshot(request, result, frame)?
                }
                HostMessage::Grant {
                    request,
                    descriptor,
                } => self.receive_grant(request, descriptor, frame)?,
                HostMessage::AllocationFailed { request, reason } => {
                    let pending = self
                        .state
                        .lock()
                        .unwrap()
                        .allocations
                        .remove(&request)
                        .ok_or_else(|| invalid("unknown allocation rejection"))?;
                    let error = match reason {
                        AllocationFailure::InvalidLength => BufferAllocationError::InvalidLength,
                        AllocationFailure::Exhausted => BufferAllocationError::Exhausted,
                        AllocationFailure::Unavailable => BufferAllocationError::Unavailable,
                    };
                    let _ = pending.reply.send(Err(error));
                }
                HostMessage::OutputAccepted { sequence } => {
                    let pending = self
                        .state
                        .lock()
                        .unwrap()
                        .outputs
                        .remove(&sequence)
                        .ok_or_else(|| invalid("unknown output acknowledgement"))?;
                    // Final-unmap callbacks can take the state lock and send releases.
                    drop(pending.prepared);
                    let _ = pending.reply.send(Ok(()));
                }
                HostMessage::Cancel { call } => {
                    let state = self.state.lock().unwrap();
                    if call == 0 || call > state.last_call {
                        return Err(invalid("cancel for undeclared call"));
                    }
                    // Completion and cancellation can cross on opposite directions.
                    if let Some(active) = state.calls.get(&call) {
                        active.cancelled.store(true, Ordering::Release);
                    }
                }
                HostMessage::Shutdown => return Ok(()),
            }
        }
    }

    fn receive_input(
        self: &Arc<Self>,
        call: u64,
        name: String,
        message: crate::MessageDescriptor,
        frame: Frame,
    ) -> io::Result<()> {
        {
            let state = self.state.lock().unwrap();
            self.check_open()?;
            let staged = state
                .staged
                .get(&call)
                .ok_or_else(|| invalid("input without staged call"))?;
            if staged.inputs.len() >= staged.expected || staged.inputs.contains_key(&name) {
                return Err(invalid("unexpected or duplicate named input"));
            }
        }
        let data = self.import_readonly(message, frame)?;
        // Move the owning input only after acknowledgement. No owner is dropped
        // while holding the state lock, including connection-close races.
        let mut state = self.state.lock().unwrap();
        self.check_open()?;
        state
            .staged
            .get_mut(&call)
            .ok_or_else(|| invalid("staged call closed"))?
            .inputs
            .insert(name, data);
        Ok(())
    }

    pub(super) fn import_readonly(
        self: &Arc<Self>,
        message: crate::MessageDescriptor,
        frame: Frame,
    ) -> io::Result<RuntimeData> {
        let expected_access = message.buffer.as_ref().map(|_| HandleAccess::Read);
        if frame.access != expected_access {
            return Err(invalid("invalid input mapping access"));
        }
        let capacity = frame
            .file
            .as_ref()
            .map(|file| file.metadata().map(|m| m.len()))
            .transpose()?;
        message
            .validate(self.session, self.peer, capacity, self.encoding)
            .map_err(other)?;
        let lease = message.buffer.as_ref().map(|buffer| buffer.lease);
        {
            let mut state = self.state.lock().unwrap();
            self.check_open()?;
            if let Some(lease) = lease {
                if !state.readers.insert(lease) {
                    return Err(invalid("duplicate input read lease"));
                }
            }
        }
        let weak = Arc::downgrade(self);
        // SAFETY: the private parent channel binds this validated descriptor and
        // read-only handle. Its broker retains the lease through this final unmap.
        let data = unsafe {
            import_message(
                message,
                frame.file,
                self.session,
                self.peer,
                self.encoding,
                move || {
                    if let Some(lease) = lease {
                        Self::release_input(&weak, lease);
                    }
                },
            )
        }
        .map_err(other)?;
        if let Some(lease) = lease {
            self.send(&WorkerMessage::InputAccepted { lease })?;
        }
        Ok(data)
    }

    fn receive_grant(
        self: &Arc<Self>,
        request: u64,
        descriptor: crate::WriteDescriptor,
        frame: Frame,
    ) -> io::Result<()> {
        let pending = self
            .state
            .lock()
            .unwrap()
            .allocations
            .remove(&request)
            .ok_or_else(|| invalid("unknown producer allocation grant"))?;
        if frame.access != Some(HandleAccess::ReadWrite) {
            return Err(invalid("producer grant is not writable"));
        }
        let file = frame
            .file
            .ok_or_else(|| invalid("missing producer mapping"))?;
        descriptor
            .validate(self.session, self.peer, file.metadata()?.len())
            .map_err(other)?;
        let bytes = (pending.elements as u64).checked_mul(pending.element.size());
        if descriptor.element != pending.element || Some(descriptor.byte_len) != bytes {
            return Err(invalid("producer grant differs from requested layout"));
        }
        {
            let mut state = self.state.lock().unwrap();
            self.check_open()?;
            if !state.producers.insert(descriptor.allocation) {
                return Err(invalid("duplicate producer grant"));
            }
        }
        let weak = Arc::downgrade(self);
        let allocation = descriptor.allocation;
        let release = move || Self::release_producer(&weak, allocation);
        // SAFETY: the parent granted this exact initialized, exclusive allocation
        // once on its private channel and retains it through the final-unmap ack.
        let loan = unsafe {
            match pending.element {
                ElementType::F32Le => {
                    import_writer(file, descriptor, self.session, self.peer, release)
                        .map(Loan::Audio)
                }
                ElementType::Bytes => {
                    import_writer(file, descriptor, self.session, self.peer, release)
                        .map(Loan::Bytes)
                }
            }
        };
        let loan = loan.map_err(other)?;
        let _ = pending.reply.send(Ok(loan));
        Ok(())
    }
}
