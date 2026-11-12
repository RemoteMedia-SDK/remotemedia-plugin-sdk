//! Private Python runner adapter for the typed mapped transport. No path, raw
//! handle, or unvalidated descriptor can be imported through this API.
//!
//! Like the native runner, this is a trusted process entry point, not a sandbox:
//! the spawning host must trust Python and every loaded module to obey the
//! immutable-publication, final-unmap and non-delegation contracts. In particular
//! the worker must not fork with live channels or mappings, or pass them onward.
use crate::{recover_audio, recover_bytes, BufferData, OutputAllocator, ReadBuffer};
use pyo3::{
    exceptions::{PyBufferError, PyKeyError, PyRuntimeError, PyValueError},
    prelude::*,
};
use remotemedia_shared_memory::{
    channel::Channel,
    peer::{CallKind, WorkerEndpoint, WorkerRequest},
};
use remotemedia_types::{PayloadStorage, RuntimeMetadata, TensorEncoding};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

static CLAIMED: AtomicBool = AtomicBool::new(false);
mod copies;
mod snapshots;

/// Metadata, owning payload exporter, media timestamp, write timestamp, sequence.
type ExportedSnapshot = (String, Option<ReadBuffer>, u64, u64, u64);

#[pyclass(frozen, module = "_remotemedia_buffers", name = "_WorkerEndpoint")]
struct Endpoint {
    endpoint: Mutex<WorkerEndpoint>,
    #[pyo3(get)]
    max_calls: usize,
    /// The host negotiated background calls.
    #[pyo3(get)]
    supports_background: bool,
}

#[pymethods]
impl Endpoint {
    /// Blocks without holding the GIL. Dispatch each returned request separately
    /// so its output backpressure cannot stall incoming control calls.
    fn next_request(&self, py: Python<'_>) -> PyResult<Request> {
        let request = py.detach(|| {
            self.endpoint
                .lock()
                .map_err(|_| PyRuntimeError::new_err("worker endpoint lock poisoned"))?
                .next_request()
                .map_err(PyErr::from)
        })?;
        Ok(Request {
            request: Arc::new(request),
        })
    }
}

#[pyclass(frozen, module = "_remotemedia_buffers", name = "_WorkerRequest")]
struct Request {
    request: Arc<WorkerRequest>,
}

#[pymethods]
impl Request {
    #[getter]
    fn call(&self) -> u64 {
        self.request.call
    }
    #[getter]
    fn kind(&self) -> String {
        format!("{:?}", self.request.kind)
    }
    #[getter]
    fn context_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.request.context)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
    #[getter]
    fn arguments_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.request.arguments)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
    #[getter]
    fn input_names(&self) -> Vec<String> {
        self.request.inputs.keys().cloned().collect()
    }
    /// Metadata JSON contains no bulk payload. The separate exporter owns its
    /// mapping independently of this request and any derived Python views.
    fn input(&self, name: &str) -> PyResult<(String, Option<ReadBuffer>)> {
        let data = self
            .request
            .inputs
            .get(name)
            .ok_or_else(|| PyKeyError::new_err(name.to_owned()))?;
        export_data(data)
    }
    #[getter]
    fn snapshot_port_names(&self) -> Vec<String> {
        self.request.snapshot_port_names()
    }
    #[getter]
    fn supports_snapshot_outputs(&self) -> bool {
        self.request.supports_snapshot_outputs()
    }
    #[getter]
    fn supports_background(&self) -> bool {
        self.request.supports_background()
    }
    #[getter]
    fn max_snapshot_ports(&self) -> usize {
        self.request.limits().inputs_per_call
    }
    fn emit_snapshot(&self, py: Python<'_>, port: &snapshots::SnapshotOutput) -> PyResult<()> {
        snapshots::emit(py, &self.request, port)
    }
    /// Sample the live publication without holding the GIL. The exporter owns
    /// the read lease, including when only a derived NumPy slice remains alive.
    fn read_snapshot(&self, py: Python<'_>, port: &str) -> PyResult<Option<ExportedSnapshot>> {
        let snapshot = py.detach(|| self.request.read_snapshot(port))?;
        snapshot
            .map(|snapshot| {
                let (metadata, buffer) = export_data(&snapshot.value)?;
                Ok((
                    metadata,
                    buffer,
                    snapshot.pts_us,
                    snapshot.written_at_us,
                    snapshot.seq,
                ))
            })
            .transpose()
    }
    #[getter]
    fn allocator(&self) -> OutputAllocator {
        OutputAllocator::new(self.request.allocator())
    }
    fn is_cancelled(&self) -> bool {
        self.request.is_cancelled()
    }
    /// Request EOS for an active negotiated tick; complete() commits on success.
    fn finish_source(&self) -> PyResult<()> {
        self.request.finish_source().map_err(PyErr::from)
    }
    #[getter]
    fn supports_output_copies(&self) -> bool {
        self.request.supports_output_copies()
    }
    /// Retain a shareable native-F32 view, or request an explicitly authorized
    /// copy. No dtype conversion or private fallback allocation is performed.
    fn adapt_audio(&self, py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<ReadBuffer> {
        copies::adapt_audio(py, &self.request, payload)
    }
    /// Recover only published owning views. Metadata shape and length are
    /// validated before dispatch; private arrays never trigger a hidden copy.
    #[pyo3(signature = (metadata_json, payload=None))]
    fn emit(
        &self,
        py: Python<'_>,
        metadata_json: &str,
        payload: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let data = import_data(metadata_json, payload)?;
        py.detach(|| self.request.emit(data)).map_err(PyErr::from)
    }
    #[pyo3(signature = (error=None))]
    fn complete(&self, py: Python<'_>, error: Option<String>) -> PyResult<()> {
        py.detach(|| {
            let result = self.request.complete(error.map_or(Ok(()), Err));
            if let Err(error) = &result {
                // A returned call with live operations violates the adapter's
                // lifecycle. Do not leave callers waiting on an orphaned call.
                self.request.fail_connection(&error.to_string());
            }
            result
        })
        .map_err(PyErr::from)
    }
}

fn import_data(
    metadata_json: &str,
    payload: Option<&Bound<'_, PyAny>>,
) -> PyResult<remotemedia_types::RuntimeData> {
    let metadata: RuntimeMetadata =
        serde_json::from_str(metadata_json).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let payload = match payload {
        None => PayloadStorage::None,
        Some(value) if matches!(metadata, RuntimeMetadata::Audio { .. }) => {
            PayloadStorage::Audio(recover_audio(value)?)
        }
        Some(value) => PayloadStorage::Bytes(recover_bytes(value)?),
    };
    metadata
        .attach(payload, TensorEncoding::Runtime)
        .map_err(|e| PyBufferError::new_err(e.to_string()))
}

/// Share the same validated metadata/exporter conversion for inputs and snapshots.
fn export_data(data: &remotemedia_types::RuntimeData) -> PyResult<(String, Option<ReadBuffer>)> {
    let (metadata, payload) = data
        .clone()
        .into_shared_parts(TensorEncoding::Runtime)
        .map_err(|e| PyBufferError::new_err(e.to_string()))?;
    let buffer = match payload {
        PayloadStorage::None => None,
        PayloadStorage::Audio(data) => Some(ReadBuffer::new(BufferData::Audio(data))?),
        PayloadStorage::Bytes(data) => Some(ReadBuffer::new(BufferData::Bytes(data))?),
    };
    let metadata =
        serde_json::to_string(&metadata).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok((metadata, buffer))
}

/// Called once by the private runner at startup, before application code or
/// threads can use stdio. Never call from an embedded or interactive interpreter.
#[pyfunction]
fn _connect_worker(py: Python<'_>, modes: Vec<String>) -> PyResult<Endpoint> {
    let modes: Vec<CallKind> = modes
        .into_iter()
        .map(|mode| serde_json::from_value(serde_json::Value::String(mode)))
        .collect::<Result<_, _>>()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    if CLAIMED.swap(true, Ordering::AcqRel) {
        return Err(PyRuntimeError::new_err("worker endpoints already claimed"));
    }
    // SAFETY: private trusted-runner entry point under the host's unsafe spawn
    // contract, documented above. CLAIMED prevents a second ownership claim,
    // including retry after partial startup. No raw import API is exposed.
    let channel = unsafe { Channel::from_inherited_stdio() }?;
    redirect_application_stdio(py)?;
    // SAFETY: the unique worker channel above owns independent handles. Python's
    // standard descriptors now address ordinary application streams instead.
    let endpoint = py.detach(|| unsafe { WorkerEndpoint::from_channel(channel, modes) })?;
    Ok(Endpoint {
        max_calls: endpoint.limits().calls,
        supports_background: endpoint.supports_background(),
        endpoint: Mutex::new(endpoint),
    })
}

fn redirect_application_stdio(py: Python<'_>) -> PyResult<()> {
    // Redirect CRT descriptors as well as Python objects: native extensions can
    // inspect/read fd 0 or write fd 1 without consulting sys.stdin/sys.stdout.
    // In particular, Windows DLL initialization can inspect synchronous stdio
    // handles while the channel reader is blocked. Do this before that reader
    // starts; duplicating a handle alone still shares its underlying file object.
    let os = py.import("os")?;
    let null = py
        .import("builtins")?
        .getattr("open")?
        .call1((os.getattr("devnull")?, "rb"))?;
    let result = (|| {
        os.call_method1("dup2", (null.call_method0("fileno")?, 0, false))?;
        os.call_method1("dup2", (2, 1, false))?;
        PyResult::Ok(())
    })();
    null.call_method0("close")?;
    result
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Endpoint>()?;
    module.add_class::<Request>()?;
    module.add_class::<snapshots::SnapshotOutput>()?;
    module.add_function(wrap_pyfunction!(_connect_worker, module)?)?;
    module.add("WORKER_PROTOCOL_VERSION", 1)?;
    module.add("SNAPSHOT_PROTOCOL_VERSION", 1)?;
    module.add("SNAPSHOT_OUTPUT_PROTOCOL_VERSION", 2)?;
    module.add("OUTPUT_COPY_PROTOCOL_VERSION", 1)?;
    module.add("SOURCE_COMPLETION_PROTOCOL_VERSION", 1)?;
    module.add("BACKGROUND_PROTOCOL_VERSION", 1)?;
    Ok(())
}
