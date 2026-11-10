//! Explicit payload materialization policy and boundary-local accounting.
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, OnceLock};

mod decode;
pub(crate) use decode::deserialize_vec;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyPolicy {
    #[default]
    AllowCopies,
    RequireZeroCopy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(usize)]
pub enum CopyReason {
    VecMaterialization,
    ArcMaterialization,
    LegacyAudioClone,
    BorrowedInput,
    PrivateIpcIngress,
    ForeignPoolIngress,
    /// Logical bulk payload bytes encoded into a portable representation.
    /// Excludes metadata, wire framing and codec-internal reallocations.
    PortableSerialization,
    /// Logical element storage reconstructed by a portable decoder. Excludes
    /// metadata, codec scratch storage and Vec growth. Strict rejection reports
    /// the first attempted element, not an inferred unread sequence length.
    PortableDeserialization,
    /// Logical payload bytes copied across a device boundary that cannot share
    /// the source allocation (for example, filling a borrowed CPAL output slice).
    /// Excludes silence generation and unobserved driver/kernel transfers.
    DeviceTransfer,
}
const REASONS: [CopyReason; 9] = [
    CopyReason::VecMaterialization,
    CopyReason::ArcMaterialization,
    CopyReason::LegacyAudioClone,
    CopyReason::BorrowedInput,
    CopyReason::PrivateIpcIngress,
    CopyReason::ForeignPoolIngress,
    CopyReason::PortableSerialization,
    CopyReason::PortableDeserialization,
    CopyReason::DeviceTransfer,
];

/// Fixed order used by owning accounting adapters. Changing it requires an ABI
/// revision in adapters that expose these counters across a library boundary.
pub const COPY_REASONS: [CopyReason; 9] = REASONS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyEvent {
    Started,
    Completed(u64),
    Failed(u64),
    Rejected(u64),
}

/// Owning accounting backend for explicitly instrumented materializations.
/// Implementations must be thread-safe, non-panicking, retain their provider,
/// and preserve event order per operation. Each Started is followed by exactly
/// one Completed or Failed; Rejected has no Started. Counts must be coherent.
/// This is a Rust integration interface, never a layout to pass through FFI.
pub trait CopyLedger: Send + Sync + 'static {
    fn counts(&self) -> [CopyCounts; COPY_REASONS.len()];
    fn record(&self, reason: CopyReason, event: CopyEvent);
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct CopyCounts {
    pub completed: u64,
    pub copied_bytes: u64,
    pub rejected: u64,
    pub rejected_bytes: u64,
    /// A failed operation may have copied some bytes before error or unwinding. Such an operation
    /// is not reported as completed or as proof of zero copied bytes.
    pub failed: u64,
    pub failed_requested_bytes: u64,
    pub in_flight: u64,
    pub saturated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct CopyReport {
    pub boundary: String,
    pub policy: CopyPolicy,
    pub entries: Vec<(CopyReason, CopyCounts)>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("zero-copy boundary '{boundary}' rejects {bytes} payload bytes for {reason:?}")]
pub struct CopyRejected {
    pub boundary: String,
    pub reason: CopyReason,
    pub bytes: u64,
}

/// A caller reported more copied bytes than the operation authorized.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "copy completion reports {actual_bytes} bytes, exceeding {authorized_bytes} authorized bytes"
)]
pub struct CopyCompletionError {
    pub authorized_bytes: u64,
    pub actual_bytes: u64,
}

struct Inner {
    boundary: String,
    policy: CopyPolicy,
    ledger: Arc<dyn CopyLedger>,
}

struct LocalLedger(Mutex<[CopyCounts; REASONS.len()]>);
impl CopyLedger for LocalLedger {
    fn counts(&self) -> [CopyCounts; REASONS.len()] {
        *self.0.lock().unwrap()
    }
    fn record(&self, reason: CopyReason, event: CopyEvent) {
        let mut ledger = self.0.lock().unwrap();
        let counts = &mut ledger[reason as usize];
        match event {
            CopyEvent::Started => add(&mut counts.in_flight, 1, &mut counts.saturated),
            CopyEvent::Rejected(bytes) => {
                add(&mut counts.rejected, 1, &mut counts.saturated);
                add(&mut counts.rejected_bytes, bytes, &mut counts.saturated);
            }
            CopyEvent::Completed(bytes) | CopyEvent::Failed(bytes) => {
                counts.in_flight = counts.in_flight.saturating_sub(1);
                if matches!(event, CopyEvent::Completed(_)) {
                    add(&mut counts.completed, 1, &mut counts.saturated);
                    add(&mut counts.copied_bytes, bytes, &mut counts.saturated);
                } else {
                    add(&mut counts.failed, 1, &mut counts.saturated);
                    add(
                        &mut counts.failed_requested_bytes,
                        bytes,
                        &mut counts.saturated,
                    );
                }
            }
        }
    }
}

/// A fixed-size ledger shared by clones at one named framework boundary.
/// Counts cover explicit instrumented operations, not arbitrary library copies.
/// Inspect only after operations finish when using a report as acceptance evidence.
#[derive(Clone)]
pub struct CopyContext(Arc<Inner>);

impl std::fmt::Debug for CopyContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyContext")
            .field("policy", &self.policy())
            .field("boundary", &self.boundary())
            .finish_non_exhaustive()
    }
}

impl CopyContext {
    pub fn new(policy: CopyPolicy, boundary: impl Into<String>) -> Self {
        Self::from_ledger(
            policy,
            boundary,
            Arc::new(LocalLedger(Mutex::new(
                [CopyCounts::default(); REASONS.len()],
            ))),
        )
    }

    /// Bind immutable policy/identity to an owning ledger adapter. Adapters must
    /// preserve the originating boundary and policy, never synthesize permission.
    pub fn from_ledger(
        policy: CopyPolicy,
        boundary: impl Into<String>,
        ledger: Arc<dyn CopyLedger>,
    ) -> Self {
        Self(Arc::new(Inner {
            boundary: boundary.into(),
            policy,
            ledger,
        }))
    }

    /// Compatibility APIs without a supplied context report to this process-local
    /// ledger. It is explicitly unattributed to any pipeline/session.
    pub fn legacy() -> &'static Self {
        static LEGACY: OnceLock<CopyContext> = OnceLock::new();
        LEGACY.get_or_init(|| Self::new(CopyPolicy::AllowCopies, "legacy_buffer_api"))
    }

    pub fn report(&self) -> CopyReport {
        let counts = self.counts();
        CopyReport {
            boundary: self.0.boundary.clone(),
            policy: self.0.policy,
            entries: REASONS.into_iter().zip(counts).collect(),
        }
    }

    pub fn policy(&self) -> CopyPolicy {
        self.0.policy
    }

    pub fn boundary(&self) -> &str {
        &self.0.boundary
    }

    /// Whether both handles retain the exact same immutable authority. Matching
    /// boundary labels or independently wrapped ledgers do not establish identity.
    pub fn same_authority(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Check before allocating destination storage. Zero-byte operations do not
    /// materialize a payload and are allowed under either policy.
    pub fn check(&self, reason: CopyReason, bytes: u64) -> Result<(), CopyRejected> {
        if bytes == 0 || self.0.policy == CopyPolicy::AllowCopies {
            return Ok(());
        }
        self.record(reason, CopyEvent::Rejected(bytes));
        Err(CopyRejected {
            boundary: self.0.boundary.clone(),
            reason,
            bytes,
        })
    }

    /// Execute one infallible copy primitive after checking policy. The caller
    /// supplies the visible payload byte count, excluding allocation headers.
    /// Do not wrap a fallible operation returning Result: allocate/validate first.
    /// For generic Clone elements this counts their inline storage, not copies
    /// recursively made by user Clone implementations; media u8/f32 are exact.
    pub fn materialize<T>(
        &self,
        reason: CopyReason,
        bytes: u64,
        operation: impl FnOnce() -> T,
    ) -> Result<T, CopyRejected> {
        self.materialize_result(reason, bytes, || {
            Ok::<T, std::convert::Infallible>(operation())
        })
        .map(|result| match result {
            Ok(value) => value,
            Err(never) => match never {},
        })
    }

    /// Run a fallible materialization after checking policy. The outer error is
    /// policy rejection (the operation was not called); the inner error is the
    /// original operation failure. Failed operations may have copied bytes, so
    /// their requested size is recorded separately, never as a zero-copy success.
    /// This counts logical payload bytes, not codec-internal copies or framing.
    pub fn materialize_result<T, E>(
        &self,
        reason: CopyReason,
        bytes: u64,
        operation: impl FnOnce() -> Result<T, E>,
    ) -> Result<Result<T, E>, CopyRejected> {
        let attempt = self.begin(reason, bytes)?;
        let value = operation();
        if value.is_ok() {
            attempt.complete();
        }
        Ok(value)
    }

    /// Authorize an asynchronous or remote copy before it starts. The owning
    /// operation records failure if dropped, including during peer teardown.
    /// Call complete after the exact authorized copy succeeded, or complete_bytes
    /// for a shorter copy within the authorized bound. Zero-byte authorizations
    /// produce no events. This does not instrument arbitrary code.
    pub fn begin(&self, reason: CopyReason, bytes: u64) -> Result<CopyOperation, CopyRejected> {
        self.check(reason, bytes)?;
        if bytes != 0 {
            self.record(reason, CopyEvent::Started);
        }
        Ok(CopyOperation {
            context: self.clone(),
            reason,
            bytes,
            completed: None,
        })
    }
}

impl CopyLedger for CopyContext {
    fn counts(&self) -> [CopyCounts; REASONS.len()] {
        self.0.ledger.counts()
    }
    fn record(&self, reason: CopyReason, event: CopyEvent) {
        self.0.ledger.record(reason, event);
    }
}

/// One authorized copy retaining its boundary ledger until completion or failure.
#[must_use = "dropping an unfinished copy records failure"]
pub struct CopyOperation {
    context: CopyContext,
    reason: CopyReason,
    bytes: u64,
    completed: Option<u64>,
}
impl CopyOperation {
    /// Record the full authorized byte count as successfully copied.
    pub fn complete(mut self) {
        self.completed = Some(self.bytes);
    }

    /// Record the actual byte count of a successful copy within the authorized
    /// bound, for example a partial final capture chunk. Report only after all
    /// copying has finished. An overrun consumes the operation as a failure;
    /// this accounting check cannot prevent a copy already performed by a caller.
    /// A positive authorization completed with zero bytes still closes its
    /// Started event. Completion and Drop may call a locking or foreign ledger:
    /// transfer the operation to a worker before completing it in an RT path.
    pub fn complete_bytes(mut self, actual_bytes: u64) -> Result<(), CopyCompletionError> {
        if actual_bytes > self.bytes {
            return Err(CopyCompletionError {
                authorized_bytes: self.bytes,
                actual_bytes,
            });
        }
        self.completed = Some(actual_bytes);
        Ok(())
    }
}
impl Drop for CopyOperation {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        self.context.record(
            self.reason,
            if let Some(bytes) = self.completed {
                CopyEvent::Completed(bytes)
            } else {
                CopyEvent::Failed(self.bytes)
            },
        );
    }
}

fn add(value: &mut u64, amount: u64, saturated: &mut bool) {
    match value.checked_add(amount) {
        Some(sum) => *value = sum,
        None => {
            *value = u64::MAX;
            *saturated = true;
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn overflow_is_saturated_and_explicitly_reported() {
        let mut value = u64::MAX - 1;
        let mut saturated = false;
        super::add(&mut value, 1, &mut saturated);
        assert_eq!(value, u64::MAX);
        assert!(!saturated);
        super::add(&mut value, 1, &mut saturated);
        assert_eq!(value, u64::MAX);
        assert!(saturated);
    }
}
