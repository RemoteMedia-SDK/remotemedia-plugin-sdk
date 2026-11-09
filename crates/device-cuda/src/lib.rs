//! CUDA device owners for [`remotemedia_types::DeviceBuffer`] (task 6.4).
//!
//! The CUDA driver API is loaded at run time (`nvcuda.dll` / `libcuda.so.1`),
//! so building needs no CUDA toolkit and a host without a GPU only loses this
//! backend. The surface is deliberately small: device memory, streams, events,
//! asynchronous fills and host copies. That is enough for Rust producers and
//! consumers to share one allocation in process, with readiness carried as a
//! [`CudaEvent`] instead of a host synchronization.
//!
//! Every call binds the device's primary context on the calling thread for its
//! duration, so owners may be used and dropped from any thread.

use libloading::Library;
use remotemedia_types::{Device, DeviceAllocation, DeviceBuffer, DeviceCompletion, DeviceReady};
use std::ffi::{c_char, c_void, CStr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

type CuResult = i32;
type CuDevice = i32;
type CuContext = *mut c_void;
type CuStream = *mut c_void;
type CuEvent = *mut c_void;
type CuDevicePtr = u64;

const CUDA_SUCCESS: CuResult = 0;
const CUDA_ERROR_NOT_READY: CuResult = 600;
const CU_STREAM_NON_BLOCKING: u32 = 1;
const CU_EVENT_DISABLE_TIMING: u32 = 2;

macro_rules! driver_api {
    ($($field:ident = $symbol:literal : fn($($arg:ty),*);)*) => {
        struct Api {
            $($field: unsafe extern "C" fn($($arg),*) -> CuResult,)*
        }
        impl Api {
            unsafe fn load(library: &Library) -> Result<Self, String> {
                Ok(Self {
                    $($field: *library
                        .get::<unsafe extern "C" fn($($arg),*) -> CuResult>($symbol)
                        .map_err(|e| format!("CUDA driver lacks {}: {e}", String::from_utf8_lossy(&$symbol[..$symbol.len() - 1])))?,)*
                })
            }
        }
    };
}

driver_api! {
    init = b"cuInit\0": fn(u32);
    device_get = b"cuDeviceGet\0": fn(*mut CuDevice, i32);
    device_count = b"cuDeviceGetCount\0": fn(*mut i32);
    primary_retain = b"cuDevicePrimaryCtxRetain\0": fn(*mut CuContext, CuDevice);
    primary_release = b"cuDevicePrimaryCtxRelease_v2\0": fn(CuDevice);
    ctx_push = b"cuCtxPushCurrent_v2\0": fn(CuContext);
    ctx_pop = b"cuCtxPopCurrent_v2\0": fn(*mut CuContext);
    mem_alloc = b"cuMemAlloc_v2\0": fn(*mut CuDevicePtr, usize);
    mem_free = b"cuMemFree_v2\0": fn(CuDevicePtr);
    memcpy_dtoh = b"cuMemcpyDtoH_v2\0": fn(*mut c_void, CuDevicePtr, usize);
    memcpy_htod = b"cuMemcpyHtoD_v2\0": fn(CuDevicePtr, *const c_void, usize);
    memset_d32_async = b"cuMemsetD32Async\0": fn(CuDevicePtr, u32, usize, CuStream);
    stream_create = b"cuStreamCreate\0": fn(*mut CuStream, u32);
    stream_destroy = b"cuStreamDestroy_v2\0": fn(CuStream);
    stream_synchronize = b"cuStreamSynchronize\0": fn(CuStream);
    stream_wait_event = b"cuStreamWaitEvent\0": fn(CuStream, CuEvent, u32);
    event_create = b"cuEventCreate\0": fn(*mut CuEvent, u32);
    event_record = b"cuEventRecord\0": fn(CuEvent, CuStream);
    event_synchronize = b"cuEventSynchronize\0": fn(CuEvent);
    event_query = b"cuEventQuery\0": fn(CuEvent);
    event_destroy = b"cuEventDestroy_v2\0": fn(CuEvent);
    error_name = b"cuGetErrorName\0": fn(CuResult, *mut *const c_char);
}

struct Driver {
    api: Api,
    _library: Library,
}

// SAFETY: the driver API is thread-safe; the table holds only function pointers.
unsafe impl Send for Driver {}
unsafe impl Sync for Driver {}

static DRIVER: OnceLock<Result<Driver, String>> = OnceLock::new();

fn driver() -> Result<&'static Driver, String> {
    DRIVER
        .get_or_init(|| unsafe {
            let name = if cfg!(windows) {
                "nvcuda.dll"
            } else {
                "libcuda.so.1"
            };
            let library =
                Library::new(name).map_err(|e| format!("CUDA driver {name} unavailable: {e}"))?;
            let api = Api::load(&library)?;
            let driver = Driver {
                api,
                _library: library,
            };
            driver.check((driver.api.init)(0), "cuInit")?;
            Ok(driver)
        })
        .as_ref()
        .map_err(Clone::clone)
}

impl Driver {
    fn check(&self, code: CuResult, call: &str) -> Result<(), String> {
        if code == CUDA_SUCCESS {
            return Ok(());
        }
        let mut name: *const c_char = std::ptr::null();
        // SAFETY: cuGetErrorName writes a static string pointer or fails.
        let known =
            unsafe { (self.api.error_name)(code, &mut name) } == CUDA_SUCCESS && !name.is_null();
        let name = if known {
            // SAFETY: the driver returned a NUL-terminated static string.
            unsafe { CStr::from_ptr(name) }
                .to_string_lossy()
                .into_owned()
        } else {
            format!("error {code}")
        };
        Err(format!("{call} failed: {name}"))
    }
}

/// Number of CUDA devices visible to this process (after `CUDA_VISIBLE_DEVICES`),
/// or an error when no driver is installed.
pub fn device_count() -> Result<usize, String> {
    let driver = driver()?;
    let mut count = 0;
    driver.check(
        unsafe { (driver.api.device_count)(&mut count) },
        "cuDeviceGetCount",
    )?;
    Ok(count.max(0) as usize)
}

/// A CUDA device with its primary context retained. Owners created from it
/// keep it alive.
pub struct CudaDevice {
    ordinal: i32,
    raw: CuDevice,
    context: usize,
    live_allocations: AtomicUsize,
}

// SAFETY: the primary context handle is process-wide; each use binds it on the
// calling thread through `bind`.
unsafe impl Send for CudaDevice {}
unsafe impl Sync for CudaDevice {}

struct Bound<'a>(&'a Driver);

impl Drop for Bound<'_> {
    fn drop(&mut self) {
        let mut previous: CuContext = std::ptr::null_mut();
        // SAFETY: pops the context `bind` pushed on this thread.
        unsafe { (self.0.api.ctx_pop)(&mut previous) };
    }
}

impl CudaDevice {
    /// Open device `ordinal` (an index among visible devices).
    pub fn open(ordinal: i32) -> Result<Arc<Self>, String> {
        let driver = driver()?;
        let mut raw = 0;
        driver.check(
            unsafe { (driver.api.device_get)(&mut raw, ordinal) },
            "cuDeviceGet",
        )?;
        let mut context: CuContext = std::ptr::null_mut();
        driver.check(
            unsafe { (driver.api.primary_retain)(&mut context, raw) },
            "cuDevicePrimaryCtxRetain",
        )?;
        Ok(Arc::new(Self {
            ordinal,
            raw,
            context: context as usize,
            live_allocations: AtomicUsize::new(0),
        }))
    }

    pub fn device(&self) -> Device {
        Device::cuda(self.ordinal)
    }

    /// Allocations from this device not yet released.
    pub fn live_allocations(&self) -> usize {
        self.live_allocations.load(Ordering::Acquire)
    }

    fn bind(&self) -> Result<Bound<'static>, String> {
        let driver = driver()?;
        driver.check(
            unsafe { (driver.api.ctx_push)(self.context as CuContext) },
            "cuCtxPushCurrent",
        )?;
        Ok(Bound(driver))
    }

    /// Allocate `bytes` of device memory (uninitialized).
    pub fn alloc(self: &Arc<Self>, bytes: usize) -> Result<CudaAllocation, String> {
        let bound = self.bind()?;
        let mut ptr = 0;
        bound.0.check(
            unsafe { (bound.0.api.mem_alloc)(&mut ptr, bytes.max(1)) },
            "cuMemAlloc",
        )?;
        self.live_allocations.fetch_add(1, Ordering::AcqRel);
        Ok(CudaAllocation {
            device: Arc::clone(self),
            ptr,
            len: bytes,
        })
    }

    /// Create a non-blocking stream: it never synchronizes implicitly with the
    /// legacy default stream, so readiness must travel as events.
    pub fn stream(self: &Arc<Self>) -> Result<CudaStream, String> {
        let bound = self.bind()?;
        let mut raw: CuStream = std::ptr::null_mut();
        bound.0.check(
            unsafe { (bound.0.api.stream_create)(&mut raw, CU_STREAM_NON_BLOCKING) },
            "cuStreamCreate",
        )?;
        Ok(CudaStream {
            device: Arc::clone(self),
            raw: raw as usize,
        })
    }
}

impl Drop for CudaDevice {
    fn drop(&mut self) {
        if let Ok(driver) = driver() {
            // SAFETY: balances the retain in `open`.
            unsafe { (driver.api.primary_release)(self.raw) };
        }
    }
}

/// Device memory owned by its [`CudaDevice`]. Freed on drop.
pub struct CudaAllocation {
    device: Arc<CudaDevice>,
    ptr: CuDevicePtr,
    len: usize,
}

impl CudaAllocation {
    /// Synchronous host-to-device upload (test and staging helper).
    pub fn copy_from_host(&self, offset: usize, source: &[u8]) -> Result<(), String> {
        if offset
            .checked_add(source.len())
            .is_none_or(|end| end > self.len)
        {
            return Err("host upload exceeds the allocation".into());
        }
        let bound = self.device.bind()?;
        bound.0.check(
            unsafe {
                (bound.0.api.memcpy_htod)(
                    self.ptr + offset as u64,
                    source.as_ptr().cast(),
                    source.len(),
                )
            },
            "cuMemcpyHtoD",
        )
    }

    /// Wrap as a shared [`DeviceBuffer`] owner.
    pub fn into_buffer(self) -> DeviceBuffer {
        DeviceBuffer::new(self)
    }
}

impl DeviceAllocation for CudaAllocation {
    fn device(&self) -> Device {
        self.device.device()
    }
    fn ptr(&self) -> u64 {
        self.ptr
    }
    fn len(&self) -> usize {
        self.len
    }
    fn copy_to_host(&self, offset: usize, destination: &mut [u8]) -> Result<(), String> {
        if offset
            .checked_add(destination.len())
            .is_none_or(|end| end > self.len)
        {
            return Err("device copy exceeds the allocation".into());
        }
        let bound = self.device.bind()?;
        bound.0.check(
            unsafe {
                (bound.0.api.memcpy_dtoh)(
                    destination.as_mut_ptr().cast(),
                    self.ptr + offset as u64,
                    destination.len(),
                )
            },
            "cuMemcpyDtoH",
        )
    }
}

impl Drop for CudaAllocation {
    fn drop(&mut self) {
        if let Ok(bound) = self.device.bind() {
            // SAFETY: frees the pointer this owner allocated, once.
            unsafe { (bound.0.api.mem_free)(self.ptr) };
        }
        self.device.live_allocations.fetch_sub(1, Ordering::AcqRel);
    }
}

/// A non-blocking CUDA stream.
pub struct CudaStream {
    device: Arc<CudaDevice>,
    raw: usize,
}

// SAFETY: stream handles are usable from any thread with the context bound.
unsafe impl Send for CudaStream {}
unsafe impl Sync for CudaStream {}

impl CudaStream {
    /// The raw stream handle, as passed to [`DeviceCompletion::wait_on_stream`].
    pub fn raw(&self) -> u64 {
        self.raw as u64
    }

    /// Enqueue a fill of `count` 32-bit words at byte `offset` with `value`.
    pub fn fill_u32(
        &self,
        allocation: &CudaAllocation,
        offset: usize,
        value: u32,
        count: usize,
    ) -> Result<(), String> {
        if offset % 4 != 0
            || count
                .checked_mul(4)
                .and_then(|n| n.checked_add(offset))
                .is_none_or(|end| end > allocation.len)
        {
            return Err("fill exceeds the allocation or is misaligned".into());
        }
        let bound = self.device.bind()?;
        bound.0.check(
            unsafe {
                (bound.0.api.memset_d32_async)(
                    allocation.ptr + offset as u64,
                    value,
                    count,
                    self.raw as CuStream,
                )
            },
            "cuMemsetD32Async",
        )
    }

    /// Record an event capturing all work enqueued so far.
    pub fn record(&self) -> Result<CudaEvent, String> {
        let bound = self.device.bind()?;
        let mut raw: CuEvent = std::ptr::null_mut();
        bound.0.check(
            unsafe { (bound.0.api.event_create)(&mut raw, CU_EVENT_DISABLE_TIMING) },
            "cuEventCreate",
        )?;
        let event = CudaEvent {
            device: Arc::clone(&self.device),
            raw: raw as usize,
        };
        bound.0.check(
            unsafe { (bound.0.api.event_record)(raw, self.raw as CuStream) },
            "cuEventRecord",
        )?;
        Ok(event)
    }

    pub fn synchronize(&self) -> Result<(), String> {
        let bound = self.device.bind()?;
        bound.0.check(
            unsafe { (bound.0.api.stream_synchronize)(self.raw as CuStream) },
            "cuStreamSynchronize",
        )
    }
}

impl Drop for CudaStream {
    fn drop(&mut self) {
        if let Ok(bound) = self.device.bind() {
            // SAFETY: destroys the stream this owner created; pending work completes first.
            unsafe { (bound.0.api.stream_destroy)(self.raw as CuStream) };
        }
    }
}

/// Readiness event recorded on a producer's stream.
pub struct CudaEvent {
    device: Arc<CudaDevice>,
    raw: usize,
}

// SAFETY: event handles are usable from any thread with the context bound.
unsafe impl Send for CudaEvent {}
unsafe impl Sync for CudaEvent {}

impl CudaEvent {
    /// Whether the recorded work has completed (non-blocking).
    pub fn is_complete(&self) -> Result<bool, String> {
        let bound = self.device.bind()?;
        match unsafe { (bound.0.api.event_query)(self.raw as CuEvent) } {
            CUDA_SUCCESS => Ok(true),
            CUDA_ERROR_NOT_READY => Ok(false),
            code => bound.0.check(code, "cuEventQuery").map(|()| false),
        }
    }

    /// Wrap as a shared [`DeviceReady`] owner.
    pub fn into_ready(self) -> DeviceReady {
        DeviceReady::new(self)
    }
}

impl DeviceCompletion for CudaEvent {
    fn wait_on_stream(&self, stream: u64) -> Result<(), String> {
        let bound = self.device.bind()?;
        bound.0.check(
            unsafe {
                (bound.0.api.stream_wait_event)(stream as usize as CuStream, self.raw as CuEvent, 0)
            },
            "cuStreamWaitEvent",
        )
    }
    fn synchronize(&self) -> Result<(), String> {
        let bound = self.device.bind()?;
        bound.0.check(
            unsafe { (bound.0.api.event_synchronize)(self.raw as CuEvent) },
            "cuEventSynchronize",
        )
    }
}

impl Drop for CudaEvent {
    fn drop(&mut self) {
        if let Ok(bound) = self.device.bind() {
            // SAFETY: destroys the event this owner created.
            unsafe { (bound.0.api.event_destroy)(self.raw as CuEvent) };
        }
    }
}
