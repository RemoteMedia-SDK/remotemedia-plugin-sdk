//! Immutable storage with ownership independent of any particular allocator.
//!
//! Moving a Vec into an Arc<Vec<T>> moves only its header. Unlike converting a
//! Vec into Arc<[T]>, it preserves the payload allocation, including capacity.
//! External owners keep pool-return or mapping-release behavior alive until the
//! final buffer or subrange is dropped. No raw pointer constructor is exposed.

use crate::{CopyContext, CopyReason, CopyRejected};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::any::Any;
use std::ops::{Deref, DerefMut, Range};
use std::sync::Arc;

trait BufferOwner<T>: AsRef<[T]> + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}
impl<T, O: AsRef<[T]> + Send + Sync + 'static> BufferOwner<T> for O {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
trait MutableOwner<T>: BufferOwner<T> + AsMut<[T]> {}
impl<T, O: AsRef<[T]> + AsMut<[T]> + Send + Sync + 'static> MutableOwner<T> for O {}

enum Storage<T> {
    Vec(Arc<Vec<T>>),
    Slice(Arc<[T]>),
    Owner(Arc<dyn BufferOwner<T>>),
    MutableOwner(Arc<dyn MutableOwner<T>>),
}

impl<T> Storage<T> {
    fn as_slice(&self) -> &[T] {
        match self {
            Self::Vec(owner) => owner.as_slice(),
            Self::Slice(owner) => owner,
            Self::Owner(owner) => owner.as_ref().as_ref(),
            Self::MutableOwner(owner) => owner.as_ref().as_ref(),
        }
    }

    fn exclusive_slice(&mut self) -> Option<&mut [T]> {
        match self {
            Self::Vec(owner) => Arc::get_mut(owner).map(Vec::as_mut_slice),
            Self::Slice(owner) => Arc::get_mut(owner),
            Self::MutableOwner(owner) => Arc::get_mut(owner).map(AsMut::as_mut),
            Self::Owner(_) => None,
        }
    }
}

impl<T> Clone for Storage<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Vec(owner) => Self::Vec(Arc::clone(owner)),
            Self::Slice(owner) => Self::Slice(Arc::clone(owner)),
            Self::Owner(owner) => Self::Owner(Arc::clone(owner)),
            Self::MutableOwner(owner) => Self::MutableOwner(Arc::clone(owner)),
        }
    }
}

/// Immutable payload storage. Clones and subranges retain the same owner.
///
/// A buffer never exposes mutable access to its published elements. Producers
/// can fill an owned Vec or pool loan and then move it into a buffer. Transport
/// providers can use [`Self::from_owner`] with their validated, immutable mapped
/// owner; the provider remains responsible for cross-process publication safety.
pub struct SharedBuffer<T> {
    storage: Storage<T>,
    range: Range<usize>,
    pub(crate) provenance: crate::provenance::Provenance,
}

/// A requested subrange lies outside the current buffer view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("buffer range {start}..{end} is invalid for length {len}")]
pub struct BufferRangeError {
    pub start: usize,
    pub end: usize,
    pub len: usize,
}

impl<T> SharedBuffer<T> {
    /// Inspect a provider's owner without materializing payload bytes. This is
    /// for transport providers recognizing their own immutable storage handles.
    pub fn owner<O: Any>(&self) -> Option<&O> {
        match &self.storage {
            Storage::Owner(owner) => owner.as_ref().as_any().downcast_ref(),
            Storage::MutableOwner(owner) => owner.as_ref().as_any().downcast_ref(),
            _ => None,
        }
    }

    /// Element range relative to the original owner, including nested slices.
    pub fn storage_range(&self) -> Range<usize> {
        self.range.clone()
    }

    /// Move a stable, immutable owner into this buffer without moving its data.
    ///
    /// The owner's AsRef implementation must expose the same initialized slice
    /// throughout its lifetime. No mutable aliases may write to published data.
    /// A pool owner is returned to its pool only after the final view is dropped.
    pub fn from_owner(owner: impl AsRef<[T]> + Send + Sync + 'static) -> Self {
        let len = owner.as_ref().len();
        Self {
            storage: Storage::Owner(Arc::new(owner)),
            range: 0..len,
            provenance: Default::default(),
        }
    }

    /// Borrow the current range without copying its elements.
    pub fn as_slice(&self) -> &[T] {
        &self.storage.as_slice()[self.range.clone()]
    }

    pub fn len(&self) -> usize {
        self.range.end - self.range.start
    }

    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// Create a view relative to this range, retaining the entire backing owner.
    /// Empty ranges at the end are valid. Reversed or overflowing ranges fail.
    pub fn slice(&self, range: Range<usize>) -> Result<Self, BufferRangeError> {
        if range.start > range.end || range.end > self.len() {
            return Err(BufferRangeError {
                start: range.start,
                end: range.end,
                len: self.len(),
            });
        }
        // Both sums are bounded by the existing range.end after the checks above.
        Ok(Self {
            storage: self.storage.clone(),
            range: self.range.start + range.start..self.range.start + range.end,
            provenance: self.provenance.clone(),
        })
    }

    /// Whether two views retain the same owner, independent of their ranges.
    /// Independently wrapped aliases can return false even if their bytes overlap.
    pub fn shares_storage(&self, other: &Self) -> bool {
        match (&self.storage, &other.storage) {
            (Storage::Vec(a), Storage::Vec(b)) => Arc::ptr_eq(a, b),
            (Storage::Slice(a), Storage::Slice(b)) => Arc::ptr_eq(a, b),
            (Storage::Owner(a), Storage::Owner(b)) => Arc::ptr_eq(a, b),
            (Storage::MutableOwner(a), Storage::MutableOwner(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }

    /// Recover exclusive write access without allocation or copying, if the
    /// backing Vec or Arc slice has no other views. External owners cannot be
    /// reopened: they may have remote readers not represented by local Arc counts.
    /// Failure returns this buffer unchanged; it never performs copy-on-write.
    pub fn try_into_loan(mut self) -> Result<BufferLoan<T>, Self> {
        if matches!(self.storage, Storage::Vec(_) | Storage::Slice(_))
            && self.storage.exclusive_slice().is_some()
        {
            Ok(BufferLoan { buffer: self })
        } else {
            Err(self)
        }
    }
}

/// Exclusive, initialized output storage. There is no Clone implementation.
///
/// Allocate or wrap a loan outside a real-time callback: creation allocates
/// ownership metadata. Filling and [`Self::freeze`] do not allocate or copy.
/// Rust borrows prevent publication while a writable slice remains in use.
/// Foreign buffer exporters must separately enforce their writable-export count.
///
/// ```compile_fail
/// use remotemedia_types::BufferLoan;
/// let mut loan = BufferLoan::from(vec![0u8; 4]);
/// let writable = loan.as_mut_slice();
/// let published = loan.freeze();
/// writable[0] = 1;
/// ```
#[derive(Debug)]
pub struct BufferLoan<T> {
    pub(crate) buffer: SharedBuffer<T>,
}

impl<T> BufferLoan<T> {
    /// Retain an exclusive pool/mapping owner, allocating only its Arc header.
    /// AsRef and AsMut must expose the same stable initialized slice. Providers
    /// must ensure no external aliases can access it during exclusive use.
    pub fn from_owner(owner: impl AsRef<[T]> + AsMut<[T]> + Send + Sync + 'static) -> Self {
        let len = owner.as_ref().len();
        Self {
            buffer: SharedBuffer {
                storage: Storage::MutableOwner(Arc::new(owner)),
                range: 0..len,
                provenance: Default::default(),
            },
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        let range = self.buffer.range.clone();
        &mut self
            .buffer
            .storage
            .exclusive_slice()
            .expect("loan owns its storage exclusively")[range]
    }

    /// Publish the initialized range by moving its already allocated owner.
    pub fn freeze(self) -> SharedBuffer<T> {
        self.buffer
    }
}

impl<T> From<Vec<T>> for BufferLoan<T> {
    fn from(value: Vec<T>) -> Self {
        Self {
            buffer: value.into(),
        }
    }
}

impl<T> Deref for BufferLoan<T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.buffer.as_slice()
    }
}

impl<T> DerefMut for BufferLoan<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<T: Clone> SharedBuffer<T> {
    /// Recover a uniquely owned, full Vec without copying. Other storage and
    /// subranges require explicit materialization into a new allocation.
    pub fn into_vec(self) -> Vec<T> {
        self.into_vec_with_context(None)
            .expect("legacy copy policy permits materialization")
    }

    /// Recover unique Vec storage, or explicitly authorize/account for copying
    /// the visible range. Strict rejection happens before cloning any element.
    pub fn try_into_vec(self, context: &CopyContext) -> Result<Vec<T>, CopyRejected> {
        self.into_vec_with_context(Some(context))
    }

    fn into_vec_with_context(self, context: Option<&CopyContext>) -> Result<Vec<T>, CopyRejected> {
        if self.range.start == 0 && self.range.end == self.storage.as_slice().len() {
            if let Storage::Vec(owner) = self.storage {
                return match Arc::try_unwrap(owner) {
                    Ok(value) => Ok(value),
                    Err(owner) => context
                        .unwrap_or_else(|| CopyContext::legacy())
                        .materialize(
                            CopyReason::VecMaterialization,
                            std::mem::size_of_val(owner.as_slice()) as u64,
                            || owner.as_ref().clone(),
                        ),
                };
            }
        }
        context
            .unwrap_or_else(|| CopyContext::legacy())
            .materialize(
                CopyReason::VecMaterialization,
                std::mem::size_of_val(self.as_slice()) as u64,
                || self.as_slice().to_vec(),
            )
    }

    /// Recover a full Arc slice if it is already backed by one. Converting any
    /// other owner or a subrange allocates and copies the visible elements.
    pub fn into_arc(self) -> Arc<[T]> {
        self.into_arc_with_context(None)
            .expect("legacy copy policy permits materialization")
    }

    /// Reuse an existing full Arc slice, or authorize/account for materialization.
    pub fn try_into_arc(self, context: &CopyContext) -> Result<Arc<[T]>, CopyRejected> {
        self.into_arc_with_context(Some(context))
    }

    fn into_arc_with_context(
        self,
        context: Option<&CopyContext>,
    ) -> Result<Arc<[T]>, CopyRejected> {
        if self.range.start == 0 && self.range.end == self.storage.as_slice().len() {
            if let Storage::Slice(owner) = self.storage {
                return Ok(owner);
            }
        }
        context
            .unwrap_or_else(|| CopyContext::legacy())
            .materialize(
                CopyReason::ArcMaterialization,
                std::mem::size_of_val(self.as_slice()) as u64,
                || Arc::from(self.as_slice()),
            )
    }
}

impl<T> From<Vec<T>> for SharedBuffer<T> {
    fn from(value: Vec<T>) -> Self {
        let len = value.len();
        Self {
            storage: Storage::Vec(Arc::new(value)),
            range: 0..len,
            provenance: Default::default(),
        }
    }
}

impl<T> From<Arc<[T]>> for SharedBuffer<T> {
    fn from(value: Arc<[T]>) -> Self {
        let len = value.len();
        Self {
            storage: Storage::Slice(value),
            range: 0..len,
            provenance: Default::default(),
        }
    }
}

impl<T> From<Box<[T]>> for SharedBuffer<T> {
    fn from(value: Box<[T]>) -> Self {
        // Box<[T]> -> Vec<T> preserves the elements' existing allocation.
        value.into_vec().into()
    }
}

impl<T> Clone for SharedBuffer<T> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            range: self.range.clone(),
            provenance: self.provenance.clone(),
        }
    }
}

impl<T> Default for SharedBuffer<T> {
    fn default() -> Self {
        Vec::new().into()
    }
}

impl<T> AsRef<[T]> for SharedBuffer<T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> Deref for SharedBuffer<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T> std::fmt::Debug for SharedBuffer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedBuffer")
            .field("range", &self.range)
            .finish_non_exhaustive()
    }
}

impl<T: PartialEq> PartialEq for SharedBuffer<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: Eq> Eq for SharedBuffer<T> {}

// Portable compatibility codecs serialize the visible elements, never an owner
// or address. Shared-memory transports must transfer descriptors instead.
impl<T: Serialize> Serialize for SharedBuffer<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_slice().serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for SharedBuffer<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        crate::copy_policy::deserialize_vec(deserializer).map(Self::from)
    }
}
