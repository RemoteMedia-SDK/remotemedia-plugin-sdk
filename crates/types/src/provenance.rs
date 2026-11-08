//! In-process, opaque evidence of accepted input and bound producer allocation.
//! Tokens never cross portable serialization or independently wrapped owners.
//! This proves the framework path of a view, not how a node computed its bytes.
use crate::{AudioSamples, BufferAllocationError, BufferLoan, OutputAllocator, RuntimeData};
use std::sync::Arc;

#[derive(Clone, Default)]
pub(crate) struct Provenance {
    pub(crate) budget: Option<Arc<crate::buffer_budget::BudgetOwner>>,
    allocation: Option<Arc<()>>,
    forwarding: Option<Arc<AcceptedBy>>,
}

struct AcceptedBy {
    scope: Arc<()>,
    prior: Option<Arc<AcceptedBy>>,
}

// A long nested path must not recursively destroy its entire admission chain.
impl Drop for AcceptedBy {
    fn drop(&mut self) {
        let mut prior = self.prior.take();
        while let Some(link) = prior {
            match Arc::try_unwrap(link) {
                Ok(mut unique) => prior = unique.prior.take(),
                Err(_) => break,
            }
        }
    }
}

impl Provenance {
    pub(crate) fn same_as(&self, other: &Self) -> bool {
        self.budget.as_ref().map(Arc::as_ptr) == other.budget.as_ref().map(Arc::as_ptr)
            && self.allocation.as_ref().map(Arc::as_ptr)
                == other.allocation.as_ref().map(Arc::as_ptr)
            && self.forwarding.as_ref().map(Arc::as_ptr)
                == other.forwarding.as_ref().map(Arc::as_ptr)
    }

    fn accepted_by(&self, scope: &Arc<()>) -> bool {
        let mut link = self.forwarding.as_deref();
        while let Some(accepted) = link {
            if Arc::ptr_eq(&accepted.scope, scope) {
                return true;
            }
            link = accepted.prior.as_deref();
        }
        false
    }

    fn accept(&mut self, scope: &Arc<()>) {
        // Repeated admission through the same node does not grow the chain.
        if !self.accepted_by(scope) {
            self.forwarding = Some(Arc::new(AcceptedBy {
                scope: scope.clone(),
                prior: self.forwarding.take(),
            }));
        }
    }
}

/// Identifies one selected output provider for one node/session binding.
/// Retaining this allocator permits later asynchronous allocations. The private
/// token is attached only to loans returned by the wrapped provider; callers
/// cannot copy it onto independently allocated storage. Clones/slices and unique
/// loan reuse preserve it. Vec/Arc extraction and rewrapping deliberately lose it.
pub struct BoundOutputAllocator {
    provider: Arc<dyn OutputAllocator>,
    token: Arc<()>,
    budget: Option<crate::BufferBudget>,
}
impl BoundOutputAllocator {
    pub fn new(provider: Arc<dyn OutputAllocator>) -> Self {
        Self {
            provider,
            token: Arc::new(()),
            budget: None,
        }
    }

    /// Limit live output loans and all their published descendants. Zero-size
    /// outputs still consume one owner. The provider is not called when full.
    pub fn with_budget(mut self, budget: crate::BufferBudget) -> Self {
        self.budget = Some(budget);
        self
    }

    /// The live reservation shared with this node's input admission boundary.
    pub fn budget(&self) -> Option<&crate::BufferBudget> {
        self.budget.as_ref()
    }

    /// Metadata has no allocation requirement. Even empty bulk buffers require
    /// evidence from this exact binding. Provider internals remain trusted.
    pub fn owns_payload(&self, data: &RuntimeData) -> bool {
        match provenance(data) {
            PayloadProvenance::Metadata => true,
            PayloadProvenance::Shared(proof) => proof
                .allocation
                .as_ref()
                .is_some_and(|token| Arc::ptr_eq(token, &self.token)),
            PayloadProvenance::Untracked => false,
        }
    }
}
impl OutputAllocator for BoundOutputAllocator {
    fn allocate_bytes(&self, len: usize) -> Result<BufferLoan<u8>, BufferAllocationError> {
        let allocate = || self.provider.allocate_bytes(len);
        let mut loan = match &self.budget {
            Some(budget) => budget.allocate(len, allocate)?,
            None => allocate()?,
        };
        loan.buffer.provenance.allocation = Some(self.token.clone());
        Ok(loan)
    }
    fn allocate_audio(&self, samples: usize) -> Result<BufferLoan<f32>, BufferAllocationError> {
        let allocate = || self.provider.allocate_audio(samples);
        let mut loan = match &self.budget {
            Some(budget) => budget.allocate(samples, allocate)?,
            None => allocate()?,
        };
        loan.buffer.provenance.allocation = Some(self.token.clone());
        Ok(loan)
    }
}

/// Opaque admission scope for one node instance. It does not retain payloads.
/// Admitted views can be forwarded later, sliced, or pass through nested scopes.
/// Admission allocates small metadata once per distinct scope in a view's path;
/// it does not copy elements and is not a real-time-safe API. Retention budgets
/// and payload validation are separate responsibilities of the caller.
#[derive(Default)]
pub struct ForwardingScope {
    token: Arc<()>,
}
impl ForwardingScope {
    /// Call only after accepting/validating input. Legacy audio is promoted by
    /// moving its owner. Existing allocation and earlier forwarding proofs survive.
    pub fn accept(&self, data: RuntimeData) -> RuntimeData {
        let mut data = normalize_owner(data);
        if let Some(proof) = provenance_mut(&mut data) {
            proof.accept(&self.token);
        }
        data
    }
    pub fn accepts_output(&self, data: &RuntimeData) -> bool {
        match provenance(data) {
            PayloadProvenance::Metadata => true,
            PayloadProvenance::Shared(proof) => proof.accepted_by(&self.token),
            PayloadProvenance::Untracked => false,
        }
    }
}

pub(crate) fn normalize_owner(data: RuntimeData) -> RuntimeData {
    let mut data = data.into_shared();
    if let RuntimeData::Audio { samples, .. } = &mut data {
        if matches!(samples, AudioSamples::Arc(_)) {
            *samples = AudioSamples::Shared(std::mem::take(samples).into_shared());
        }
    }
    data
}

enum PayloadProvenance<'a> {
    Metadata,
    Shared(&'a Provenance),
    Untracked,
}
fn provenance(data: &RuntimeData) -> PayloadProvenance<'_> {
    match data {
        RuntimeData::Audio {
            samples: AudioSamples::Shared(buffer),
            ..
        } => PayloadProvenance::Shared(&buffer.provenance),
        RuntimeData::Audio { .. } => PayloadProvenance::Untracked,
        RuntimeData::Binary(buffer)
        | RuntimeData::Video {
            pixel_data: buffer, ..
        }
        | RuntimeData::Image { data: buffer, .. }
        | RuntimeData::Tensor { data: buffer, .. }
        | RuntimeData::Numpy { data: buffer, .. } => PayloadProvenance::Shared(&buffer.provenance),
        _ => PayloadProvenance::Metadata,
    }
}
fn provenance_mut(data: &mut RuntimeData) -> Option<&mut Provenance> {
    match data {
        RuntimeData::Audio {
            samples: AudioSamples::Shared(buffer),
            ..
        } => Some(&mut buffer.provenance),
        RuntimeData::Binary(buffer)
        | RuntimeData::Video {
            pixel_data: buffer, ..
        }
        | RuntimeData::Image { data: buffer, .. }
        | RuntimeData::Tensor { data: buffer, .. }
        | RuntimeData::Numpy { data: buffer, .. } => Some(&mut buffer.provenance),
        _ => None,
    }
}
