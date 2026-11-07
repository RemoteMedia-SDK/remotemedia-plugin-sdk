//! Direct Python audio helper with a real one-owner mapped allocator.
use _remotemedia_buffers::{recover_audio, OutputAllocator};
use pyo3::{prelude::*, types::PyDict};
use remotemedia_shared_memory::{PeerId, PoolConfig, SessionId, SharedPool, SizeClass};
use remotemedia_types::{BoundOutputAllocator, BufferBudget, RuntimeData};
use std::sync::Arc;

#[test]
#[ignore = "requires MAPPED_SDK_PYTHON and NumPy"]
fn direct_audio_failures_reclaim_budget_and_retained_slices_keep_exact_storage() {
    let pool = Arc::new(
        SharedPool::new(
            SessionId([94; 16]),
            PoolConfig {
                classes: vec![SizeClass {
                    bytes: 4096,
                    slots: 1,
                }],
                max_bytes: 4096,
                max_leases: 4,
            },
        )
        .unwrap(),
    );
    let budget = BufferBudget::new(1, 4096);
    let allocator = Arc::new(BoundOutputAllocator::new(pool.clone()).with_budget(budget));
    Python::attach(|py| {
        let locals = PyDict::new(py);
        locals
            .set_item("sdk_path", std::env::var("MAPPED_SDK_PYTHON").unwrap())
            .unwrap();
        locals
            .set_item(
                "allocator",
                Py::new(py, OutputAllocator::new(allocator.clone())).unwrap(),
            )
            .unwrap();
        py.run(
            c"
import sys, gc
sys.path.insert(0, sdk_path)
from remotemedia.core.audio_output import render_audio
failures, pointers = [], []
def broken(output):
    try:
        raise ValueError('failed render')
    except ValueError as cause:
        raise ExceptionGroup('nested', [cause]) from cause
def fill(output):
    pointers.append(output.ctypes.data)
    output[:] = 0.25
gc.disable()
try:
    for _ in range(8):
        try:
            render_audio(allocator, 8, 48000, broken)
        except ExceptionGroup as error:
            failures.append(error)
        audio = render_audio(allocator, 8, 48000, fill)
        del audio
    audio = render_audio(allocator, 8, 48000, fill)
    retained = audio.payload[2:6]
    del audio
    try:
        render_audio(allocator, 8, 48000, fill)
    except BufferError as error:
        failures.append(error)
    else:
        raise AssertionError('retained slice did not hold one-owner budget')
finally:
    gc.enable()
",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
        let retained = recover_audio(&locals.get_item("retained").unwrap().unwrap()).unwrap();
        let pointer: usize = locals
            .get_item("pointers")
            .unwrap()
            .unwrap()
            .get_item(-1)
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(retained.as_ptr() as usize, pointer + 8);
        assert_eq!(retained.as_slice(), &[0.25; 4]);
        assert!(allocator.owns_payload(&RuntimeData::Audio {
            samples: retained.clone().into(),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        }));
        assert!(pool.export(&retained, PeerId(94)).is_ok());
        drop(retained);
        py.run(
            c"del retained; audio = render_audio(allocator, 8, 48000, fill); del audio",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
    });
}
