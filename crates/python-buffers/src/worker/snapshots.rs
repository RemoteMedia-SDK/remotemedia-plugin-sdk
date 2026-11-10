//! Capture immutable portable publications without retaining mutable Python metadata.
use super::*;
use remotemedia_traits::ports::OutputPort;
use remotemedia_types::RuntimeData;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BufferData, ReadBuffer};
    use remotemedia_shared_memory::{PoolConfig, SessionId, SharedPool, SizeClass};

    #[test]
    fn publication_captures_metadata_and_retains_exact_storage_after_replacement() {
        let pool = SharedPool::new(
            SessionId([91; 16]),
            PoolConfig {
                classes: vec![SizeClass {
                    bytes: 4096,
                    slots: 1,
                }],
                max_bytes: 4096,
                max_leases: 4,
            },
        )
        .unwrap();
        let mut loan = pool.allocate::<u8>(4).unwrap();
        loan.copy_from_slice(&[11, 22, 33, 44]);
        let data = loan.freeze();
        let pointer = data.as_ptr();
        let retained = Python::attach(|py| {
            let port = SnapshotOutput::new();
            assert!(port.port.snapshot().is_none());
            let owner = Py::new(py, ReadBuffer::new(BufferData::Bytes(data)).unwrap()).unwrap();
            port.publish("\"Binary\"", (1 << 53) + 9, Some(owner.bind(py).as_any()))
                .unwrap();
            let snapshot = port.port.snapshot().unwrap();
            let RuntimeData::Binary(bytes) = &snapshot.value else {
                panic!("binary expected")
            };
            assert_eq!(bytes.as_ptr(), pointer);
            assert_eq!(snapshot.pts_us, (1 << 53) + 9);
            assert_eq!(snapshot.seq, 0);
            assert!(snapshot.written_at_us > 0);
            port.publish("{\"Json\":{\"version\":2}}", 1, None).unwrap();
            assert_eq!(port.port.snapshot().unwrap().seq, 1);
            snapshot
        });
        assert!(pool.allocate::<u8>(1).is_err());
        assert!(
            matches!(&retained.value, RuntimeData::Binary(bytes) if bytes.as_slice()==[11,22,33,44])
        );
        drop(retained);
        assert!(pool.allocate::<u8>(1).is_ok());
    }
}

#[pyclass(frozen, module = "_remotemedia_buffers", name = "_SnapshotOutput")]
pub(super) struct SnapshotOutput {
    port: OutputPort<RuntimeData>,
}

#[pymethods]
impl SnapshotOutput {
    #[new]
    fn new() -> Self {
        Self {
            port: OutputPort::empty(),
        }
    }

    #[pyo3(signature = (metadata_json, pts_us, payload=None))]
    fn publish(
        &self,
        metadata_json: &str,
        pts_us: u64,
        payload: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let data = import_data(metadata_json, payload)?;
        self.port.publish(data, pts_us);
        Ok(())
    }
}

pub(super) fn emit(py: Python<'_>, request: &WorkerRequest, port: &SnapshotOutput) -> PyResult<()> {
    if request.kind != CallKind::Snapshot || !request.supports_snapshot_outputs() {
        return Err(PyRuntimeError::new_err(
            "snapshot outputs were not negotiated for this call",
        ));
    }
    // Retain exactly one publication across both acknowledged output messages.
    let snapshot = port.port.snapshot();
    py.detach(|| {
        let Some(snapshot) = snapshot else {
            return request.emit(RuntimeData::Json(serde_json::Value::Null));
        };
        request.emit(RuntimeData::Json(serde_json::json!({
            "pts_us":snapshot.pts_us,"written_at_us":snapshot.written_at_us,"seq":snapshot.seq
        })))?;
        request.emit(snapshot.value.clone())
    })
    .map_err(PyErr::from)
}
