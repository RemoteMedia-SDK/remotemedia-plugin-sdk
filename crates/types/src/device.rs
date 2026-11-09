//! Device-resident tensor owners (task 6.4).
//!
//! A device allocation is never exposed as a host slice. In-process consumers on
//! the same device share the allocation through [`DeviceBuffer`] clones; any
//! boundary that needs host bytes must call [`RuntimeData::device_to_host`],
//! which is a counted `CopyReason::DeviceTransfer` (rejected under strict
//! policy). Serde deliberately refuses device owners, so a codec that has not
//! been taught about them fails loudly instead of copying silently.
use crate::{CopyContext, CopyReason, CopyRejected, RuntimeData, SharedBytes};
use serde::{de, ser, Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt, sync::Arc};

const NO_SERDE: &str = "device tensors cannot be serialized; the boundary must perform an explicit, counted DeviceTransfer (RuntimeData::device_to_host)";

/// Device family. Values follow DLPack `DLDeviceType` where one exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DeviceKind {
    /// CUDA device memory (DLPack `kDLCUDA` = 2).
    Cuda,
}

/// A specific device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Device {
    pub kind: DeviceKind,
    pub ordinal: i32,
}

impl Device {
    pub fn cuda(ordinal: i32) -> Self {
        Self {
            kind: DeviceKind::Cuda,
            ordinal,
        }
    }
    /// The buffer-contract domain name, e.g. `cuda:1`.
    pub fn domain(&self) -> String {
        match self.kind {
            DeviceKind::Cuda => format!("cuda:{}", self.ordinal),
        }
    }
    /// DLPack `DLDeviceType` code.
    pub fn dlpack_type(&self) -> i32 {
        match self.kind {
            DeviceKind::Cuda => 2,
        }
    }
}

/// One device allocation, owned by the backend that created it. Dropping the
/// last [`DeviceBuffer`] clone drops the allocation, which releases it through
/// that backend.
pub trait DeviceAllocation: Send + Sync + 'static {
    fn device(&self) -> Device;
    /// Base device pointer of the allocation.
    fn ptr(&self) -> u64;
    /// Allocation length in bytes.
    fn len(&self) -> usize;
    /// Copy `destination.len()` bytes starting at `offset` into host memory.
    /// Callers wait for the producer's completion first.
    fn copy_to_host(&self, offset: usize, destination: &mut [u8]) -> Result<(), String>;
}

/// Readiness of a device tensor's contents, recorded on its producer's stream.
pub trait DeviceCompletion: Send + Sync + 'static {
    /// Make work later submitted to `stream` (a backend stream handle; 0 is
    /// the legacy default stream) wait until the contents are ready.
    fn wait_on_stream(&self, stream: u64) -> Result<(), String>;
    /// Block the calling thread until the contents are ready.
    fn synchronize(&self) -> Result<(), String>;
}

/// Shared owner of a device allocation. Clones share it; equality is identity.
#[derive(Clone)]
pub struct DeviceBuffer(Arc<dyn DeviceAllocation>);

impl DeviceBuffer {
    pub fn new(allocation: impl DeviceAllocation) -> Self {
        Self(Arc::new(allocation))
    }
    pub fn from_arc(allocation: Arc<dyn DeviceAllocation>) -> Self {
        Self(allocation)
    }
    pub fn device(&self) -> Device {
        self.0.device()
    }
    pub fn ptr(&self) -> u64 {
        self.0.ptr()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
    }
    /// Number of owners currently retaining the allocation.
    pub fn owners(&self) -> usize {
        Arc::strong_count(&self.0)
    }
    pub fn same_allocation(&self, other: &Self) -> bool {
        std::ptr::addr_eq(Arc::as_ptr(&self.0), Arc::as_ptr(&other.0))
    }
    pub fn allocation(&self) -> &Arc<dyn DeviceAllocation> {
        &self.0
    }
    pub fn copy_to_host(&self, offset: usize, destination: &mut [u8]) -> Result<(), String> {
        match offset.checked_add(destination.len()) {
            Some(end) if end <= self.len() => self.0.copy_to_host(offset, destination),
            _ => Err("device copy range exceeds the allocation".into()),
        }
    }
}

impl fmt::Debug for DeviceBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceBuffer")
            .field("device", &self.device())
            .field("ptr", &format_args!("{:#x}", self.ptr()))
            .field("len", &self.len())
            .finish()
    }
}
impl PartialEq for DeviceBuffer {
    fn eq(&self, other: &Self) -> bool {
        self.same_allocation(other)
    }
}
impl Serialize for DeviceBuffer {
    fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(ser::Error::custom(NO_SERDE))
    }
}
impl<'de> Deserialize<'de> for DeviceBuffer {
    fn deserialize<D: Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(de::Error::custom(NO_SERDE))
    }
}

/// Shared readiness event. Owners keep it until the last consumer drops them.
#[derive(Clone)]
pub struct DeviceReady(Arc<dyn DeviceCompletion>);

impl DeviceReady {
    pub fn new(completion: impl DeviceCompletion) -> Self {
        Self(Arc::new(completion))
    }
    pub fn from_arc(completion: Arc<dyn DeviceCompletion>) -> Self {
        Self(completion)
    }
    pub fn wait_on_stream(&self, stream: u64) -> Result<(), String> {
        self.0.wait_on_stream(stream)
    }
    pub fn synchronize(&self) -> Result<(), String> {
        self.0.synchronize()
    }
}
impl fmt::Debug for DeviceReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DeviceReady")
    }
}
impl PartialEq for DeviceReady {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::addr_eq(Arc::as_ptr(&self.0), Arc::as_ptr(&other.0))
    }
}
impl Serialize for DeviceReady {
    fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(ser::Error::custom(NO_SERDE))
    }
}
impl<'de> Deserialize<'de> for DeviceReady {
    fn deserialize<D: Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(de::Error::custom(NO_SERDE))
    }
}

/// DLPack `DLDataType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DlDtype {
    /// `kDLInt` 0, `kDLUInt` 1, `kDLFloat` 2, `kDLBfloat` 4, `kDLComplex` 5, `kDLBool` 6.
    pub code: u8,
    pub bits: u8,
    pub lanes: u16,
}

impl DlDtype {
    pub const F16: Self = Self::new(2, 16);
    pub const F32: Self = Self::new(2, 32);
    pub const F64: Self = Self::new(2, 64);
    pub const I8: Self = Self::new(0, 8);
    pub const I16: Self = Self::new(0, 16);
    pub const I32: Self = Self::new(0, 32);
    pub const I64: Self = Self::new(0, 64);
    pub const U8: Self = Self::new(1, 8);
    pub const U16: Self = Self::new(1, 16);
    pub const U32: Self = Self::new(1, 32);
    pub const U64: Self = Self::new(1, 64);
    pub const BOOL: Self = Self::new(6, 8);

    pub const fn new(code: u8, bits: u8) -> Self {
        Self {
            code,
            bits,
            lanes: 1,
        }
    }
    /// Bytes per element, for single-lane byte-aligned dtypes.
    pub fn item_bytes(&self) -> Option<usize> {
        (self.lanes == 1 && self.bits % 8 == 0 && self.bits > 0).then_some(self.bits as usize / 8)
    }
    /// The NumPy dtype a host copy of this tensor uses.
    pub fn numpy_name(&self) -> Option<&'static str> {
        Some(match (*self, self.lanes) {
            (Self::F16, _) => "float16",
            (Self::F32, _) => "float32",
            (Self::F64, _) => "float64",
            (Self::I8, _) => "int8",
            (Self::I16, _) => "int16",
            (Self::I32, _) => "int32",
            (Self::I64, _) => "int64",
            (Self::U8, _) => "uint8",
            (Self::U16, _) => "uint16",
            (Self::U32, _) => "uint32",
            (Self::U64, _) => "uint64",
            (Self::BOOL, _) => "bool",
            _ => return None,
        })
    }
}

/// Why a boundary could not produce host bytes for a value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransferError {
    /// The boundary's copy policy rejected the copy (strict sessions).
    #[error(transparent)]
    Rejected(#[from] CopyRejected),
    /// The device transfer itself is impossible or failed.
    #[error("device transfer failed: {0}")]
    Device(String),
}

impl From<String> for TransferError {
    fn from(message: String) -> Self {
        Self::Device(message)
    }
}
impl From<&str> for TransferError {
    fn from(message: &str) -> Self {
        Self::Device(message.into())
    }
}

/// C-contiguous element strides for `shape`.
pub fn contiguous_strides(shape: &[i64]) -> Vec<i64> {
    let mut strides = vec![0; shape.len()];
    let mut step = 1i64;
    for (stride, &dim) in strides.iter_mut().zip(shape).rev() {
        *stride = step;
        step = step.saturating_mul(dim.max(1));
    }
    strides
}

impl RuntimeData {
    /// True for device-resident tensors.
    pub fn is_device(&self) -> bool {
        matches!(self, RuntimeData::DeviceTensor { .. })
    }

    /// The device of a device-resident tensor.
    pub fn device(&self) -> Option<Device> {
        match self {
            RuntimeData::DeviceTensor { buffer, .. } => Some(buffer.device()),
            _ => None,
        }
    }

    /// Convert a device tensor to a host NumPy array for a boundary that needs
    /// host bytes; other values pass through unchanged. This is a counted
    /// `DeviceTransfer`: strict policy rejects it before any work, and
    /// compatibility waits for the producer's completion, then copies through
    /// the allocation's own primitive. Only C-contiguous tensors with a NumPy
    /// dtype are supported; others reject (a layout conversion would be needed).
    pub fn device_to_host(self, copies: &CopyContext) -> Result<RuntimeData, TransferError> {
        let RuntimeData::DeviceTensor {
            buffer,
            dtype,
            shape,
            strides,
            byte_offset,
            ready,
            ..
        } = self
        else {
            return Ok(self);
        };
        let item = dtype
            .item_bytes()
            .ok_or("device tensor dtype has no host representation")?;
        let numpy = dtype
            .numpy_name()
            .ok_or("device tensor dtype has no NumPy equivalent")?;
        if shape.iter().any(|&d| d < 0) {
            return Err("device tensor has a negative dimension".into());
        }
        let elements = shape
            .iter()
            .try_fold(1u64, |n, &d| n.checked_mul(d as u64))
            .ok_or("device tensor size overflow")?;
        let c_strides = contiguous_strides(&shape);
        if elements > 1 && strides.as_ref().is_some_and(|s| *s != c_strides) {
            return Err(
                "non-contiguous device tensor needs a layout conversion before a host transfer"
                    .into(),
            );
        }
        let bytes = elements
            .checked_mul(item as u64)
            .ok_or("device tensor size overflow")?;
        let offset = usize::try_from(byte_offset).map_err(|_| "device offset overflow")?;
        let offset: usize = offset;
        if offset as u64 + bytes > buffer.len() as u64 {
            return Err("device tensor view exceeds its allocation".into());
        }
        copies.check(CopyReason::DeviceTransfer, bytes)?;
        if let Some(ready) = &ready {
            ready.synchronize()?;
        }
        let host = copies.materialize_result(CopyReason::DeviceTransfer, bytes, || {
            let mut host = vec![0u8; bytes as usize];
            buffer.copy_to_host(offset, &mut host).map(|()| host)
        })??;
        let byte_strides: Vec<isize> = c_strides
            .iter()
            .map(|&s| s as isize * item as isize)
            .collect();
        let non_unit = shape.iter().filter(|&&d| d != 1).count();
        Ok(RuntimeData::Numpy {
            data: SharedBytes::from(host),
            shape: shape.iter().map(|&d| d as usize).collect(),
            dtype: numpy.into(),
            strides: byte_strides,
            c_contiguous: true,
            f_contiguous: non_unit <= 1,
        })
    }
}
