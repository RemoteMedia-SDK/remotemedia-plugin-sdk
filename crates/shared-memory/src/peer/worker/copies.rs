//! Copy authority is requested from the host; workers never synthesize policy.
use super::*;
use remotemedia_types::SharedBuffer;

impl WorkerRequest {
    /// Whether this immutable payload already has a descriptor for this worker.
    /// This checks ownership, not metadata layout or the host's node contract.
    /// Callers must still emit through the normal validated output boundary.
    pub fn can_forward_payload(
        &self,
        payload: &remotemedia_types::PayloadStorage,
    ) -> io::Result<bool> {
        let state = self.shared.state.lock().unwrap();
        self.shared.check_open()?;
        let active = state
            .calls
            .get(&self.call)
            .ok_or_else(|| invalid("output adaptation after call completion"))?;
        if active.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "output adaptation cancelled",
            ));
        }
        drop(state);
        use remotemedia_types::PayloadStorage;
        Ok(match payload {
            PayloadStorage::None => true,
            PayloadStorage::Audio(data) => {
                super::super::output::reference(data, self.shared.session, self.shared.peer).is_ok()
            }
            PayloadStorage::Bytes(data) => {
                super::super::output::reference(data, self.shared.session, self.shared.peer).is_ok()
            }
        })
    }

    pub fn supports_output_copies(&self) -> bool {
        self.shared.output_copy_protocol == OUTPUT_COPY_PROTOCOL
    }

    /// Materialize exactly `samples` F32 elements into a host-selected output.
    /// The host authorizes and accounts logical payload bytes before invoking
    /// `copy`. Callback errors/unwinding and peer death record failed attempts.
    /// The outer error is protocol/allocation/policy failure; the inner error
    /// preserves the callback's original failure. Arbitrary internal copies are
    /// not instrumented. This call and all callback accesses must finish before
    /// completing the worker request.
    pub fn materialize_audio<E>(
        &self,
        samples: usize,
        copy: impl FnOnce(&mut [f32]) -> Result<(), E>,
    ) -> io::Result<Result<SharedBuffer<f32>, E>> {
        let mut operation = OutputCopy::new(self, ElementType::F32Le, samples)?;
        let Some(Loan::Audio(loan)) = operation.loan.as_mut() else {
            return Err(invalid("audio copy received wrong loan type"));
        };
        if let Err(error) = copy(loan) {
            return Ok(Err(error));
        }
        match operation.finish()? {
            Loan::Audio(loan) => Ok(Ok(loan.freeze())),
            _ => Err(invalid("audio copy finished with wrong loan type")),
        }
    }

    /// Byte counterpart of materialize_audio; identical authority and lifecycle.
    pub fn materialize_bytes<E>(
        &self,
        len: usize,
        copy: impl FnOnce(&mut [u8]) -> Result<(), E>,
    ) -> io::Result<Result<SharedBuffer<u8>, E>> {
        let mut operation = OutputCopy::new(self, ElementType::Bytes, len)?;
        let Some(Loan::Bytes(loan)) = operation.loan.as_mut() else {
            return Err(invalid("byte copy received wrong loan type"));
        };
        if let Err(error) = copy(loan) {
            return Ok(Err(error));
        }
        match operation.finish()? {
            Loan::Bytes(loan) => Ok(Ok(loan.freeze())),
            _ => Err(invalid("byte copy finished with wrong loan type")),
        }
    }
}

struct OutputCopy {
    shared: Arc<SharedWorker>,
    call: u64,
    allocation: Option<AllocationId>,
    loan: Option<Loan>,
    reported: bool,
}
impl OutputCopy {
    fn new(request: &WorkerRequest, element: ElementType, elements: usize) -> io::Result<Self> {
        if !request.supports_output_copies() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "output copies were not negotiated",
            ));
        }
        {
            let mut state = request.shared.state.lock().unwrap();
            request.shared.check_open()?;
            let active = state
                .calls
                .get_mut(&request.call)
                .ok_or_else(|| invalid("output copy outside a live call"))?;
            if active.cancelled.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "output copy cancelled",
                ));
            }
            if active.pending_copies >= request.shared.limits.outputs_per_call {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "output copy budget exhausted",
                ));
            }
            active.pending_copies += 1;
        }
        let mut operation = Self {
            shared: request.shared.clone(),
            call: request.call,
            allocation: None,
            loan: None,
            reported: false,
        };
        let granted = RemoteAllocator {
            shared: request.shared.clone(),
            call: request.call,
        }
        .allocate(element, elements, true)
        .map_err(|error| {
            if error == AllocationFailure::CopyDenied {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "host output copy policy denied materialization",
                )
            } else {
                other(allocation_error(error))
            }
        })?;
        operation.allocation = Some(granted.allocation);
        operation.loan = Some(granted.loan);
        if request.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "output copy cancelled before callback",
            ));
        }
        Ok(operation)
    }

    fn finish(mut self) -> io::Result<Loan> {
        self.shared.send(&WorkerMessage::CopyFinished {
            call: self.call,
            allocation: self.allocation.unwrap(),
            success: true,
        })?;
        self.reported = true;
        Ok(self.loan.take().unwrap())
    }
}
impl Drop for OutputCopy {
    fn drop(&mut self) {
        // Completion precedes producer release on the ordered control stream.
        // This Drop runs before the loan field's final-unmap callback.
        if !self.reported {
            if let Some(allocation) = self.allocation {
                let _ = self.shared.send(&WorkerMessage::CopyFinished {
                    call: self.call,
                    allocation,
                    success: false,
                });
            }
        }
        if let Some(active) = self.shared.state.lock().unwrap().calls.get_mut(&self.call) {
            active.pending_copies -= 1;
        }
    }
}
