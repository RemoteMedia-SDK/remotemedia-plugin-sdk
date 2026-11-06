use super::*;
use abi_stable::{
    sabi_trait::TD_Opaque,
    std_types::{RErr, ROk, RResult, RSliceMut},
};
use remotemedia_types::{BufferAllocationError, BufferLoan as Loan, OutputAllocator as Allocator};
use std::sync::{Arc, Mutex};

enum LoanStorage {
    Audio(Loan<f32>),
    Bytes(Loan<u8>),
}
struct SourceLoan {
    storage: LoanStorage,
    // Allocate the publication header with the loan, not during freeze.
    owner: RArc<SourceOwner>,
}

// SAFETY: Rust BufferLoan enforces initialized exclusive storage. freeze moves
// its existing owner into the preallocated immutable publication header.
unsafe impl BufferLoan for SourceLoan {
    fn element(&self) -> BufferElement {
        match self.storage {
            LoanStorage::Audio(_) => BufferElement::AudioF32,
            LoanStorage::Bytes(_) => BufferElement::Bytes,
        }
    }
    fn view(&self) -> RSlice<'_, u8> {
        match &self.storage {
            LoanStorage::Audio(data) => audio_bytes(data).into(),
            LoanStorage::Bytes(data) => data.as_ref().into(),
        }
    }
    fn view_mut(&mut self) -> RSliceMut<'_, u8> {
        match &mut self.storage {
            LoanStorage::Bytes(data) => data.as_mut_slice().into(),
            LoanStorage::Audio(data) => {
                let bytes = data.len() * std::mem::size_of::<f32>();
                // SAFETY: f32 is initialized, has no padding, and every byte
                // pattern is valid. &mut self provides the sole writable view.
                unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr().cast(), bytes) }.into()
            }
        }
    }
    fn freeze(self) -> BufferOwnerArc {
        let storage = match self.storage {
            LoanStorage::Audio(data) => PayloadStorage::Audio(data.freeze()),
            LoanStorage::Bytes(data) => PayloadStorage::Bytes(data.freeze()),
        };
        assert!(
            self.owner.storage.set(storage).is_ok(),
            "loan published exactly once"
        );
        BufferOwner_TO::from_ptr(self.owner, TD_CanDowncast)
    }
}

#[derive(Clone)]
struct SourceAllocator(Arc<dyn Allocator>);
impl OutputAllocator for SourceAllocator {
    fn allocate(
        &self,
        element: BufferElement,
        elements: u64,
    ) -> RResult<BufferLoanBox, AllocationError> {
        let Ok(elements) = usize::try_from(elements) else {
            return RErr(AllocationError::InvalidLength);
        };
        let storage = match element {
            BufferElement::Bytes => self.0.allocate_bytes(elements).map(LoanStorage::Bytes),
            BufferElement::AudioF32 => self.0.allocate_audio(elements).map(LoanStorage::Audio),
        };
        match storage {
            Ok(storage) => ROk(BufferLoan_TO::from_value(
                SourceLoan {
                    storage,
                    owner: RArc::new(SourceOwner {
                        storage: OnceLock::new(),
                    }),
                },
                TD_Opaque,
            )),
            Err(error) => RErr(match error {
                BufferAllocationError::InvalidLength => AllocationError::InvalidLength,
                BufferAllocationError::Exhausted => AllocationError::Exhausted,
                BufferAllocationError::Unavailable => AllocationError::Unavailable,
            }),
        }
    }
}

/// Retain the chosen provider; no heap fallback or allocator layout crosses FFI.
pub fn export_allocator(allocator: Arc<dyn Allocator>) -> OutputAllocatorArc {
    OutputAllocator_TO::from_ptr(RArc::new(SourceAllocator(allocator)), TD_Opaque)
}

/// A provider imported from another module. Its loans retain their own ownership
/// independently of the caller, provider wrapper, node or session lifetime.
pub fn import_allocator(allocator: OutputAllocatorArc) -> Arc<dyn Allocator> {
    Arc::new(ImportedAllocator(allocator))
}

struct ImportedAllocator(OutputAllocatorArc);
impl ImportedAllocator {
    fn loan<T: Element>(
        &self,
        elements: usize,
    ) -> std::result::Result<Loan<T>, BufferAllocationError> {
        let bytes = elements
            .checked_mul(std::mem::size_of::<T>())
            .filter(|&bytes| bytes <= isize::MAX as usize)
            .ok_or(BufferAllocationError::InvalidLength)?;
        let mut loan = self
            .0
            .allocate(T::KIND, elements as u64)
            .into_result()
            .map_err(|error| match error {
                AllocationError::InvalidLength => BufferAllocationError::InvalidLength,
                AllocationError::Exhausted => BufferAllocationError::Exhausted,
                AllocationError::Unavailable => BufferAllocationError::Unavailable,
            })?;
        if loan.element() != T::KIND {
            return Err(BufferAllocationError::Unavailable);
        }
        let address = {
            let mut view = loan.view_mut();
            if view.len() != bytes {
                return Err(BufferAllocationError::Unavailable);
            }
            view.as_mut_ptr() as usize
        };
        if !address.is_multiple_of(std::mem::align_of::<T>()) {
            return Err(BufferAllocationError::Unavailable);
        }
        Ok(Loan::from_owner(ImportedLoan::<T> {
            loan: Mutex::new(Some(loan)),
            frozen: OnceLock::new(),
            address,
            elements,
            marker: PhantomData,
        }))
    }
}
impl Allocator for ImportedAllocator {
    fn allocate_bytes(&self, len: usize) -> std::result::Result<Loan<u8>, BufferAllocationError> {
        self.loan(len)
    }
    fn allocate_audio(
        &self,
        samples: usize,
    ) -> std::result::Result<Loan<f32>, BufferAllocationError> {
        self.loan(samples)
    }
}

pub(super) struct ImportedLoan<T> {
    loan: Mutex<Option<BufferLoanBox>>,
    frozen: OnceLock<BufferOwnerArc>,
    address: usize,
    elements: usize,
    marker: PhantomData<T>,
}

impl<T: Element> ImportedLoan<T> {
    pub(super) fn publish(&self) -> &BufferOwnerArc {
        self.frozen.get_or_init(|| {
            self.loan
                .lock()
                .expect("native loan poisoned")
                .take()
                .expect("exclusive native loan")
                .freeze()
        })
    }
}
impl<T: Element> AsRef<[T]> for ImportedLoan<T> {
    fn as_ref(&self) -> &[T] {
        // SAFETY: validated initialized typed allocation. The exclusive loan
        // or its published owner remains alive, with the same stable address.
        unsafe { std::slice::from_raw_parts(self.address as *const T, self.elements) }
    }
}
impl<T: Element> AsMut<[T]> for ImportedLoan<T> {
    fn as_mut(&mut self) -> &mut [T] {
        assert!(self.frozen.get().is_none(), "published loan is immutable");
        // SAFETY: &mut self is obtained only through the exclusive Rust loan.
        // Publication is reachable only after its consuming freeze operation.
        unsafe { std::slice::from_raw_parts_mut(self.address as *mut T, self.elements) }
    }
}
