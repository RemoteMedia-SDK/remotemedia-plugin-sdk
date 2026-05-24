//! Snapshot ports for the Reactive→Clocked seam.
//!
//! A snapshot port is an atomic publish/load slot used between a producer
//! (Reactive node) and a consumer (Clocked node) when the consumer wants
//! "the latest published value" rather than "every value the producer
//! ever emitted." Reads never block; writes never block; the consumer
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
//! let snap: Option<TimestampedSnapshot<Weights>> =
//!     ctx.snapshot::<Weights>("weights");
//! match snap {
//!     Some(snap) if (tick.pts_us - snap.pts_us) < FRESH_US => render(&snap.value),
//!     _ => render_idle(),
//! }
//! ```

use std::any::Any;
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
    pub fn publish(&self, value: T, pts_us: u64) {
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

    /// Read the latest snapshot.
    pub fn snapshot(&self) -> Option<TimestampedSnapshot<T>>
    where
        T: Clone,
    {
        self.slot.load_full().map(|arc| (*arc).clone())
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

impl<T: Send + Sync + Clone + 'static> InputPort<T> {
    /// Read the latest snapshot. Atomic load — never blocks.
    pub fn snapshot(&self) -> Option<TimestampedSnapshot<T>> {
        self.slot.load_full().map(|arc| (*arc).clone())
    }
}

impl<T: Send + Sync + 'static> Clone for InputPort<T> {
    fn clone(&self) -> Self {
        Self {
            slot: Arc::clone(&self.slot),
        }
    }
}

/// Type-erased read handle.
pub trait SnapshotPort: Send + Sync {
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

impl<T: Send + Sync + Clone + 'static> SnapshotPort for InputPort<T> {
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
