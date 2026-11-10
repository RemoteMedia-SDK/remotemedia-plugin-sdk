use pyo3::{
    exceptions::{PyBufferError, PyOverflowError},
    ffi,
    prelude::*,
};
use remotemedia_types::{BufferLoan, SharedBuffer};
use std::ffi::{c_int, c_void, CStr};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Clone)]
pub enum BufferData {
    Bytes(SharedBuffer<u8>),
    Audio(SharedBuffer<f32>),
}

struct Layout {
    pointer: usize,
    byte_len: isize,
    elements: isize,
    itemsize: isize,
    format: &'static CStr,
}

impl Layout {
    fn new(
        pointer: usize,
        elements: usize,
        itemsize: usize,
        format: &'static CStr,
    ) -> PyResult<Self> {
        let byte_len = elements
            .checked_mul(itemsize)
            .and_then(|n| isize::try_from(n).ok())
            .ok_or_else(|| {
                PyOverflowError::new_err("buffer length exceeds Python addressable memory")
            })?;
        Ok(Self {
            pointer,
            byte_len,
            elements: elements as isize,
            itemsize: itemsize as isize,
            format,
        })
    }

    /// The caller keeps this layout and its backing allocation alive through
    /// exporter (view.obj). Writable access is governed by OutputLoan's counter.
    unsafe fn fill(
        &self,
        exporter: &Bound<'_, PyAny>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
        readonly: bool,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyBufferError::new_err("null buffer view"));
        }
        // SAFETY: CPython supplied the writable Py_buffer. FillInfo owns a new
        // exporter reference and applies the requested writable/readonly check.
        let result = unsafe {
            ffi::PyBuffer_FillInfo(
                view,
                exporter.as_ptr(),
                self.pointer as *mut c_void,
                self.byte_len,
                i32::from(readonly),
                flags,
            )
        };
        if result != 0 {
            return Err(PyErr::fetch(exporter.py()));
        }
        // SAFETY: shape/stride point into a frozen PyClass retained by view.obj.
        // Format is a static NUL-terminated PEP 3118 primitive descriptor.
        unsafe {
            (*view).itemsize = self.itemsize;
            (*view).format = if flags & ffi::PyBUF_FORMAT != 0 {
                self.format.as_ptr().cast_mut()
            } else {
                std::ptr::null_mut()
            };
            (*view).shape = if flags & ffi::PyBUF_ND != 0 {
                (&self.elements as *const isize).cast_mut()
            } else {
                std::ptr::null_mut()
            };
            (*view).strides = if flags & ffi::PyBUF_STRIDES == ffi::PyBUF_STRIDES {
                (&self.itemsize as *const isize).cast_mut()
            } else {
                std::ptr::null_mut()
            };
        }
        Ok(())
    }
}

/// Read-only exporter. Its Py_buffer.obj reference owns the Rust storage owner.
#[pyclass(frozen, module = "_remotemedia_buffers")]
pub struct ReadBuffer {
    pub(crate) data: BufferData,
    layout: Layout,
}

impl ReadBuffer {
    pub fn new(data: BufferData) -> PyResult<Self> {
        let layout = match &data {
            BufferData::Bytes(bytes) => Layout::new(bytes.as_ptr() as usize, bytes.len(), 1, c"B")?,
            BufferData::Audio(audio) => Layout::new(audio.as_ptr() as usize, audio.len(), 4, c"f")?,
        };
        Ok(Self { data, layout })
    }
}

#[pymethods]
impl ReadBuffer {
    #[getter]
    fn nbytes(&self) -> isize {
        self.layout.byte_len
    }
    fn __len__(&self) -> usize {
        self.layout.elements as usize
    }

    /// A contiguous range retaining the original owner; no negative/wrapping indexes.
    fn slice(&self, start: usize, end: usize) -> PyResult<Self> {
        let data = match &self.data {
            BufferData::Bytes(bytes) => BufferData::Bytes(
                bytes
                    .slice(start..end)
                    .map_err(|e| PyBufferError::new_err(e.to_string()))?,
            ),
            BufferData::Audio(audio) => BufferData::Audio(
                audio
                    .slice(start..end)
                    .map_err(|e| PyBufferError::new_err(e.to_string()))?,
            ),
        };
        Self::new(data)
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        // SAFETY: immutable data and layout remain owned by slf/view.obj.
        unsafe { slf.get().layout.fill(slf.as_any(), view, flags, true) }
    }
}

enum LoanData {
    Bytes(BufferLoan<u8>),
    Audio(BufferLoan<f32>),
}
struct LoanState {
    loan: Option<LoanData>,
    exports: usize,
}

/// All raw writable views must release before publish. A readonly ndarray flag
/// does not release its buffer lease and cannot bypass this guard.
#[pyclass(frozen, module = "_remotemedia_buffers")]
pub struct OutputLoan {
    state: Mutex<LoanState>,
    layout: Layout,
}

impl OutputLoan {
    pub fn bytes(mut loan: BufferLoan<u8>) -> PyResult<Self> {
        let elements = loan.len();
        let layout = Layout::new(loan.as_mut_ptr() as usize, elements, 1, c"B")?;
        Ok(Self {
            state: Mutex::new(LoanState {
                loan: Some(LoanData::Bytes(loan)),
                exports: 0,
            }),
            layout,
        })
    }

    pub fn audio(mut loan: BufferLoan<f32>) -> PyResult<Self> {
        let elements = loan.len();
        let layout = Layout::new(loan.as_mut_ptr() as usize, elements, 4, c"f")?;
        Ok(Self {
            state: Mutex::new(LoanState {
                loan: Some(LoanData::Audio(loan)),
                exports: 0,
            }),
            layout,
        })
    }

    fn state(&self) -> PyResult<MutexGuard<'_, LoanState>> {
        self.state
            .lock()
            .map_err(|_| PyBufferError::new_err("output loan state is poisoned"))
    }
}

#[pymethods]
impl OutputLoan {
    #[getter]
    fn nbytes(&self) -> isize {
        self.layout.byte_len
    }
    #[getter]
    fn writable_exports(&self) -> PyResult<usize> {
        Ok(self.state()?.exports)
    }
    fn __len__(&self) -> usize {
        self.layout.elements as usize
    }

    fn publish(&self) -> PyResult<ReadBuffer> {
        let mut state = self.state()?;
        if state.exports != 0 {
            return Err(PyBufferError::new_err(format!(
                "cannot publish: {} writable buffer exports remain",
                state.exports
            )));
        }
        let data = match state
            .loan
            .take()
            .ok_or_else(|| PyBufferError::new_err("loan is already published"))?
        {
            LoanData::Bytes(loan) => BufferData::Bytes(loan.freeze()),
            LoanData::Audio(loan) => BufferData::Audio(loan.freeze()),
        };
        ReadBuffer::new(data)
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let this = slf.get();
        // CPython requires a null owner on every failure path, including an
        // already-published loan or a poisoned state lock before FillInfo.
        if !view.is_null() {
            unsafe {
                (*view).obj = std::ptr::null_mut();
            }
        }
        let mut state = this.state()?;
        if state.loan.is_none() {
            return Err(PyBufferError::new_err("loan is already published"));
        }
        let next = state
            .exports
            .checked_add(1)
            .ok_or_else(|| PyBufferError::new_err("too many writable exports"))?;
        // SAFETY: no Rust references to elements are created while exports live.
        // All successful exports retain slf and block publication under this mutex.
        unsafe {
            this.layout.fill(slf.as_any(), view, flags, false)?;
        }
        state.exports = next;
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) -> PyResult<()> {
        let mut state = self.state()?;
        state.exports = state
            .exports
            .checked_sub(1)
            .ok_or_else(|| PyBufferError::new_err("unbalanced buffer release"))?;
        Ok(())
    }
}

/// Context-injected provider. Published buffers and loans retain their own storage.
#[pyclass(frozen, module = "_remotemedia_buffers")]
pub struct OutputAllocator {
    allocator: Arc<dyn remotemedia_types::OutputAllocator>,
    #[pyo3(get)]
    session_id: Option<String>,
    #[pyo3(get)]
    node_id: Option<String>,
}
impl OutputAllocator {
    pub fn new(allocator: Arc<dyn remotemedia_types::OutputAllocator>) -> Self {
        Self {
            allocator,
            session_id: None,
            node_id: None,
        }
    }

    /// Attach host-owned call identity without changing the selected provider.
    pub fn with_identity(mut self, session_id: &str, node_id: &str) -> Self {
        self.session_id = Some(session_id.to_owned());
        self.node_id = Some(node_id.to_owned());
        self
    }
}
#[pymethods]
impl OutputAllocator {
    fn allocate_bytes(&self, py: Python<'_>, len: usize) -> PyResult<OutputLoan> {
        OutputLoan::bytes(
            py.detach(|| self.allocator.allocate_bytes(len))
                .map_err(|e| PyBufferError::new_err(e.to_string()))?,
        )
    }
    fn allocate_audio(&self, py: Python<'_>, samples: usize) -> PyResult<OutputLoan> {
        OutputLoan::audio(
            py.detach(|| self.allocator.allocate_audio(samples))
                .map_err(|e| PyBufferError::new_err(e.to_string()))?,
        )
    }
}
