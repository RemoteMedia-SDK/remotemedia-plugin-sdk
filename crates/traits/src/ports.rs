//! Snapshot ports for the Reactive→Clocked seam.
//!
//! A snapshot port is an atomic publish/load slot used between a producer
//! (Reactive node) and a consumer (Clocked node) when the consumer wants
//! "the latest published value" rather than "every value the producer
//! ever emitted." Local slot reads and writes never block; the consumer
//! always observes the most recent fully-written value.
//!
//! ## Producer side
//! ```ignore
//! let port: OutputPort<Weights> = OutputPort::empty();
//! port.publish(Weights { ... }, /* pts_us = */ 1234567);
//! ```
//!
//! ## Consumer side (via NodeRuntimeContext)
//! ```ignore
//! let snap: Option<Arc<TimestampedSnapshot<Weights>>> =
//!     ctx.snapshot::<Weights>("weights");
//! match snap {
//!     Some(snap) if (tick.pts_us - snap.pts_us) < FRESH_US => render(&snap.value),
//!     _ => render_idle(),
//! }
//! ```

use remotemedia_types::{Error, RuntimeData};
use std::any::{Any, TypeId};
use std::sync::Arc;

use arc_swap::ArcSwapOption;

/// Declared kind of a named port on a node factory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PortKind {
    /// Streaming port: every emitted value flows through the runtime's
    /// per-node fan-out into successors' input channels.
    Stream,
    /// Snapshot port: atomic latest-wins slot.
    Snapshot,
}

/// One published value on a snapshot port.
#[derive(Debug, Clone)]
pub struct TimestampedSnapshot<T> {
    pub value: T,
    pub pts_us: u64,
    pub written_at_us: u64,
    pub seq: u64,
}

/// Producer side. Holds the atomic slot.
pub struct OutputPort<T: Send + Sync + 'static> {
    /// Latest value, or `None` if nothing has been published.
    slot: Arc<ArcSwapOption<TimestampedSnapshot<T>>>,
    /// Monotonic sequence stamped on every published snapshot.
    seq: Arc<std::sync::atomic::AtomicU64>,
}

impl<T: Send + Sync + 'static> OutputPort<T> {
    /// Build a fresh empty port. `snapshot()` returns `None` until the
    /// first `publish`.
    pub fn empty() -> Self {
        Self {
            slot: Arc::new(ArcSwapOption::const_empty()),
            seq: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Atomically publish a new snapshot.
    pub fn publish(&self, mut value: T, pts_us: u64) {
        // RuntimeData snapshots may cross language/library boundaries. Promote
        // legacy owned audio before publication so all later reads retain it.
        if let Some(data) = (&mut value as &mut dyn Any).downcast_mut::<RuntimeData>() {
            let owned = std::mem::replace(data, RuntimeData::Text(String::new()));
            *data = owned.into_shared();
        }
        let written_at_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        let seq = self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.slot.store(Some(Arc::new(TimestampedSnapshot {
            value,
            pts_us,
            written_at_us,
            seq,
        })));
    }

    /// Retain the latest snapshot without cloning its payload. A retained read
    /// remains valid after replacement or after the port is dropped.
    pub fn snapshot(&self) -> Option<Arc<TimestampedSnapshot<T>>> {
        self.slot.load_full()
    }

    /// Build a read handle pointing at this port's slot.
    pub fn input(&self) -> InputPort<T> {
        InputPort {
            slot: Arc::clone(&self.slot),
        }
    }
}

impl<T: Send + Sync + 'static> Default for OutputPort<T> {
    fn default() -> Self {
        Self::empty()
    }
}

impl<T: Send + Sync + 'static> Clone for OutputPort<T> {
    fn clone(&self) -> Self {
        Self {
            slot: Arc::clone(&self.slot),
            seq: Arc::clone(&self.seq),
        }
    }
}

/// Consumer side. Atomically loads the latest snapshot from the bound
/// `OutputPort`. Cheap to clone.
pub struct InputPort<T: Send + Sync + 'static> {
    slot: Arc<ArcSwapOption<TimestampedSnapshot<T>>>,
}

impl<T: Send + Sync + 'static> InputPort<T> {
    /// Retain the latest snapshot. The payload is not cloned.
    pub fn snapshot(&self) -> Option<Arc<TimestampedSnapshot<T>>> {
        self.slot.load_full()
    }
}

impl<T: Send + Sync + 'static> Clone for InputPort<T> {
    fn clone(&self) -> Self {
        Self {
            slot: Arc::clone(&self.slot),
        }
    }
}

/// Type-erased read handle. Remote implementations may wait for a transport
/// reply; use the fallible portable accessor to preserve transport errors.
pub trait SnapshotPort: Send + Sync {
    /// Whether this port has the portable RuntimeData payload contract. Generic
    /// application Rust types remain local and must not cross a dynamic ABI.
    fn supports_runtime_data(&self) -> bool {
        false
    }

    /// Error-preserving portable read. Unknown application payload types fail
    /// explicitly rather than masquerading as an unpublished snapshot.
    fn runtime_snapshot(&self) -> Result<Option<Arc<TimestampedSnapshot<RuntimeData>>>, Error> {
        Err(Error::Execution(
            "snapshot payload is not portable RuntimeData".into(),
        ))
    }

    /// Type-erased downcast hook. Implementations return `&self`; the
    /// caller is expected to `downcast_ref::<InputPort<T>>()`.
    fn as_any(&self) -> &dyn Any;

    /// Type-erased load of the latest published snapshot.
    ///
    /// Returns `None` when no producer has published yet. Otherwise
    /// returns `Arc<TimestampedSnapshot<T>>` for the producer's `T`,
    /// erased to `Arc<dyn Any + Send + Sync>`. The
    /// `runtime_context::snapshot::<T>` free helper downcasts back to
    /// the consumer's expected type.
    fn snapshot_any(&self) -> Option<Arc<dyn Any + Send + Sync>>;
}

impl<T: Send + Sync + 'static> SnapshotPort for InputPort<T> {
    fn supports_runtime_data(&self) -> bool {
        TypeId::of::<T>() == TypeId::of::<RuntimeData>()
    }

    fn runtime_snapshot(&self) -> Result<Option<Arc<TimestampedSnapshot<RuntimeData>>>, Error> {
        if !self.supports_runtime_data() {
            return Err(Error::Execution(
                "snapshot payload is not portable RuntimeData".into(),
            ));
        }
        self.snapshot_any()
            .map(|value| {
                value.downcast().map_err(|_| {
                    Error::Execution("snapshot RuntimeData type contract mismatch".into())
                })
            })
            .transpose()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn snapshot_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        let arc = self.slot.load_full()?;
        // `Arc<TimestampedSnapshot<T>>` upcasts to
        // `Arc<dyn Any + Send + Sync>` for any `T: 'static + Send + Sync`.
        Some(arc as Arc<dyn Any + Send + Sync>)
    }
}
