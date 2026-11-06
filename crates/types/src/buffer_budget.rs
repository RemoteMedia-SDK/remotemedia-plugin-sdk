//! Live capacity for allocations that remain owned by loans or published views.
use crate::{AudioSamples, BufferAllocationError, BufferLoan, RuntimeData, SharedBuffer};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

/// Outstanding allocation groups and their backing bytes, not visible slice sizes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferBudgetUsage {
    pub owners: u64,
    pub bytes: u64,
}

struct State {
    limits: BufferBudgetUsage,
    usage: Mutex<BufferBudgetUsage>,
    admitted: Mutex<HashMap<(u8, usize), Weak<Reservation>>>,
}

/// A shared budget that fails rather than waiting for capacity.
/// A reservation survives allocator/session destruction until its last view drops.
/// Clones and slices share one reservation; independently allocated outputs do not.
/// This covers provider backing spans, not hidden provider metadata or device memory.
#[derive(Clone)]
pub struct BufferBudget(Arc<State>);

impl BufferBudget {
    pub fn new(owners: u64, bytes: u64) -> Self {
        Self(Arc::new(State {
            limits: BufferBudgetUsage { owners, bytes },
            usage: Mutex::new(BufferBudgetUsage::default()),
            admitted: Mutex::new(HashMap::new()),
        }))
    }

    pub fn usage(&self) -> BufferBudgetUsage {
        *self.0.usage.lock().unwrap()
    }

    pub fn limits(&self) -> BufferBudgetUsage {
        self.0.limits
    }

    /// Charge a retained input without copying its payload. Repeated admission
    /// of the same owning Arc shares one live reservation, including subranges.
    /// Independent wrappers are conservatively distinct. Metadata is uncharged.
    /// The weak lookup never keeps payloads or reservations alive.
    pub fn admit(&self, data: RuntimeData) -> Result<RuntimeData, BufferAllocationError> {
        let mut data = crate::provenance::normalize_owner(data);
        match &mut data {
            RuntimeData::Audio {
                samples: AudioSamples::Shared(buffer),
                ..
            } => self.retain(buffer)?,
            RuntimeData::Binary(buffer)
            | RuntimeData::Video {
                pixel_data: buffer, ..
            }
            | RuntimeData::Image { data: buffer, .. }
            | RuntimeData::Tensor { data: buffer, .. }
            | RuntimeData::Numpy { data: buffer, .. } => self.retain(buffer)?,
            _ => {}
        }
        Ok(data)
    }

    fn retain<T>(&self, buffer: &mut SharedBuffer<T>) -> Result<(), BufferAllocationError> {
        let mut link = buffer.provenance.budget.as_deref();
        while let Some(owner) = link {
            if Arc::ptr_eq(&owner._reservation.state, &self.0) {
                return Ok(());
            }
            link = owner.prior.as_deref();
        }
        let key = buffer.storage_identity();
        let reservation = {
            let mut admitted = self.0.admitted.lock().unwrap();
            match admitted.get(&key).and_then(Weak::upgrade) {
                Some(reservation) => reservation,
                None => {
                    admitted.retain(|_, lease| lease.strong_count() != 0);
                    let reservation = Arc::new(self.reserve(buffer.backing_bytes() as u64)?);
                    admitted.insert(key, Arc::downgrade(&reservation));
                    reservation
                }
            }
        };
        buffer.provenance.budget = Some(Arc::new(BudgetOwner {
            _reservation: reservation,
            prior: buffer.provenance.budget.take(),
        }));
        Ok(())
    }

    fn reserve(&self, bytes: u64) -> Result<Reservation, BufferAllocationError> {
        let mut usage = self.0.usage.lock().unwrap();
        let owners = usage
            .owners
            .checked_add(1)
            .ok_or(BufferAllocationError::Exhausted)?;
        let total = usage
            .bytes
            .checked_add(bytes)
            .ok_or(BufferAllocationError::Exhausted)?;
        if owners > self.0.limits.owners || total > self.0.limits.bytes {
            return Err(BufferAllocationError::Exhausted);
        }
        *usage = BufferBudgetUsage {
            owners,
            bytes: total,
        };
        Ok(Reservation {
            state: self.0.clone(),
            bytes,
        })
    }

    pub(crate) fn allocate<T>(
        &self,
        len: usize,
        allocate: impl FnOnce() -> Result<BufferLoan<T>, BufferAllocationError>,
    ) -> Result<BufferLoan<T>, BufferAllocationError> {
        let bytes = len
            .checked_mul(std::mem::size_of::<T>())
            .filter(|&n| n <= isize::MAX as usize)
            .ok_or(BufferAllocationError::InvalidLength)?;
        // Reserve before invoking provider code. No callbacks execute under the lock.
        let mut reservation = self.reserve(bytes as u64)?;
        let mut loan = allocate()?;
        // Providers may round up or retain extra Vec capacity. Charge the whole
        // exposed backing owner before publishing the loan; roll back on failure.
        reservation.resize(loan.buffer.backing_bytes() as u64)?;
        loan.buffer.provenance.budget = Some(Arc::new(BudgetOwner {
            _reservation: Arc::new(reservation),
            prior: loan.buffer.provenance.budget.take(),
        }));
        Ok(loan)
    }
}

struct Reservation {
    state: Arc<State>,
    bytes: u64,
}
impl Reservation {
    fn resize(&mut self, bytes: u64) -> Result<(), BufferAllocationError> {
        let mut usage = self.state.usage.lock().unwrap();
        let total = (usage.bytes - self.bytes)
            .checked_add(bytes)
            .filter(|&n| n <= self.state.limits.bytes)
            .ok_or(BufferAllocationError::Exhausted)?;
        usage.bytes = total;
        self.bytes = bytes;
        Ok(())
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        let mut usage = self.state.usage.lock().unwrap();
        usage.owners -= 1;
        usage.bytes -= self.bytes;
    }
}

/// Metadata only: does not wrap or hide the provider's recognizable storage owner.
pub(crate) struct BudgetOwner {
    _reservation: Arc<Reservation>,
    prior: Option<Arc<BudgetOwner>>,
}
impl Drop for BudgetOwner {
    fn drop(&mut self) {
        // Nested providers retain all reservations without recursive destruction.
        let mut prior = self.prior.take();
        while let Some(link) = prior {
            match Arc::try_unwrap(link) {
                Ok(mut unique) => prior = unique.prior.take(),
                Err(_) => break,
            }
        }
    }
}
