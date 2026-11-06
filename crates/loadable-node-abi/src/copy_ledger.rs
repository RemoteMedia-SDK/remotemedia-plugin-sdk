//! Same-process, owning copy-accounting handle. No Rust trait object layout crosses FFI.
use abi_stable::{
    sabi_trait,
    std_types::{RArc, RString},
    StableAbi,
};

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, StableAbi)]
pub enum CopyPolicy {
    AllowCopies,
    RequireZeroCopy,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, StableAbi)]
pub enum CopyReason {
    VecMaterialization,
    ArcMaterialization,
    LegacyAudioClone,
    BorrowedInput,
    PrivateIpcIngress,
    ForeignPoolIngress,
    PortableSerialization,
    PortableDeserialization,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, StableAbi)]
pub enum CopyEvent {
    Started,
    Completed(u64),
    Failed(u64),
    Rejected(u64),
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, StableAbi)]
pub struct CopyCounts {
    pub completed: u64,
    pub copied_bytes: u64,
    pub rejected: u64,
    pub rejected_bytes: u64,
    pub failed: u64,
    pub failed_requested_bytes: u64,
    pub in_flight: u64,
    pub saturated: bool,
}

/// Counts follow CopyReason declaration order. Implementations retain their
/// originating ledger and must not panic. Started/Completed/Failed event ordering
/// follows the Rust CopyLedger contract. The loader keeps provider code mapped.
#[sabi_trait]
pub trait CopyLedger: Clone + Send + Sync + 'static {
    fn counts(&self) -> [CopyCounts; 8];
    fn record(&self, reason: CopyReason, event: CopyEvent);
}
pub type CopyLedgerArc = CopyLedger_TO<RArc<()>>;

#[repr(C)]
#[derive(Clone, StableAbi)]
pub struct FfiCopyContext {
    pub policy: CopyPolicy,
    pub boundary: RString,
    pub ledger: CopyLedgerArc,
}

#[cfg(feature = "runtime")]
pub mod runtime {
    use super::*;
    use abi_stable::sabi_trait::TD_Opaque;
    use remotemedia_types::{self as rust, CopyLedger as _};
    use std::sync::Arc;

    #[derive(Clone)]
    struct Exported(rust::CopyContext);
    impl CopyLedger for Exported {
        fn counts(&self) -> [CopyCounts; 8] {
            self.0.counts().map(Into::into)
        }
        fn record(&self, reason: CopyReason, event: CopyEvent) {
            self.0.record(reason.into(), event.into());
        }
    }
    struct Imported(CopyLedgerArc);
    impl rust::CopyLedger for Imported {
        fn counts(&self) -> [rust::CopyCounts; 8] {
            self.0.counts().map(Into::into)
        }
        fn record(&self, reason: rust::CopyReason, event: rust::CopyEvent) {
            self.0.record(reason.into(), event.into());
        }
    }

    /// Export immutable policy/identity and retain the exact originating ledger.
    pub fn export(context: rust::CopyContext) -> FfiCopyContext {
        FfiCopyContext {
            policy: match context.policy() {
                rust::CopyPolicy::AllowCopies => CopyPolicy::AllowCopies,
                rust::CopyPolicy::RequireZeroCopy => CopyPolicy::RequireZeroCopy,
            },
            boundary: context.boundary().into(),
            ledger: CopyLedger_TO::from_ptr(RArc::new(Exported(context)), TD_Opaque),
        }
    }

    /// Imported clones continue to report directly to the provider after the
    /// execution context, node and session have been dropped.
    pub fn import(context: FfiCopyContext) -> rust::CopyContext {
        let policy = match context.policy {
            CopyPolicy::AllowCopies => rust::CopyPolicy::AllowCopies,
            CopyPolicy::RequireZeroCopy => rust::CopyPolicy::RequireZeroCopy,
        };
        rust::CopyContext::from_ledger(
            policy,
            context.boundary.into_string(),
            Arc::new(Imported(context.ledger)),
        )
    }

    impl From<rust::CopyCounts> for CopyCounts {
        fn from(value: rust::CopyCounts) -> Self {
            Self {
                completed: value.completed,
                copied_bytes: value.copied_bytes,
                rejected: value.rejected,
                rejected_bytes: value.rejected_bytes,
                failed: value.failed,
                failed_requested_bytes: value.failed_requested_bytes,
                in_flight: value.in_flight,
                saturated: value.saturated,
            }
        }
    }
    impl From<CopyCounts> for rust::CopyCounts {
        fn from(value: CopyCounts) -> Self {
            Self {
                completed: value.completed,
                copied_bytes: value.copied_bytes,
                rejected: value.rejected,
                rejected_bytes: value.rejected_bytes,
                failed: value.failed,
                failed_requested_bytes: value.failed_requested_bytes,
                in_flight: value.in_flight,
                saturated: value.saturated,
            }
        }
    }

    impl From<rust::CopyReason> for CopyReason {
        fn from(value: rust::CopyReason) -> Self {
            match value {
                rust::CopyReason::VecMaterialization => Self::VecMaterialization,
                rust::CopyReason::ArcMaterialization => Self::ArcMaterialization,
                rust::CopyReason::LegacyAudioClone => Self::LegacyAudioClone,
                rust::CopyReason::BorrowedInput => Self::BorrowedInput,
                rust::CopyReason::PrivateIpcIngress => Self::PrivateIpcIngress,
                rust::CopyReason::ForeignPoolIngress => Self::ForeignPoolIngress,
                rust::CopyReason::PortableSerialization => Self::PortableSerialization,
                rust::CopyReason::PortableDeserialization => Self::PortableDeserialization,
            }
        }
    }
    impl From<CopyReason> for rust::CopyReason {
        fn from(value: CopyReason) -> Self {
            match value {
                CopyReason::VecMaterialization => Self::VecMaterialization,
                CopyReason::ArcMaterialization => Self::ArcMaterialization,
                CopyReason::LegacyAudioClone => Self::LegacyAudioClone,
                CopyReason::BorrowedInput => Self::BorrowedInput,
                CopyReason::PrivateIpcIngress => Self::PrivateIpcIngress,
                CopyReason::ForeignPoolIngress => Self::ForeignPoolIngress,
                CopyReason::PortableSerialization => Self::PortableSerialization,
                CopyReason::PortableDeserialization => Self::PortableDeserialization,
            }
        }
    }

    impl From<rust::CopyEvent> for CopyEvent {
        fn from(value: rust::CopyEvent) -> Self {
            match value {
                rust::CopyEvent::Started => Self::Started,
                rust::CopyEvent::Completed(bytes) => Self::Completed(bytes),
                rust::CopyEvent::Failed(bytes) => Self::Failed(bytes),
                rust::CopyEvent::Rejected(bytes) => Self::Rejected(bytes),
            }
        }
    }
    impl From<CopyEvent> for rust::CopyEvent {
        fn from(value: CopyEvent) -> Self {
            match value {
                CopyEvent::Started => Self::Started,
                CopyEvent::Completed(bytes) => Self::Completed(bytes),
                CopyEvent::Failed(bytes) => Self::Failed(bytes),
                CopyEvent::Rejected(bytes) => Self::Rejected(bytes),
            }
        }
    }
}
