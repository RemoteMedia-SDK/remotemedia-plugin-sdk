//! Optional real-model integration: requires MAPPED_SDK_PYTHON and CPU ONNX Runtime.
use _remotemedia_buffers::{recover_audio, recover_bytes, BufferData, OutputAllocator, ReadBuffer};
use pyo3::{prelude::*, types::PyDict};
use remotemedia_shared_memory::{PeerId, PoolConfig, SessionId, SharedPool, SizeClass};
use remotemedia_types::{BoundOutputAllocator, BufferBudget, OutputAllocator as _, RuntimeData};
use std::{path::PathBuf, sync::Arc};

#[test]
#[ignore = "requires MAPPED_SDK_PYTHON, local Silero ONNX model, NumPy and CPU ONNX Runtime"]
fn real_model_outputs_retain_mapped_owner_kind_budget_and_allocation() {
    let sdk = PathBuf::from(std::env::var("MAPPED_SDK_PYTHON").unwrap());
    let model = sdk
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("crates/core/silero_vad.onnx");
    assert!(model.is_file());
    let pool = Arc::new(
        SharedPool::new(
            SessionId([93; 16]),
            PoolConfig {
                classes: vec![SizeClass {
                    bytes: 4096,
                    slots: 5,
                }],
                max_bytes: 20480,
                max_leases: 16,
            },
        )
        .unwrap(),
    );
    let input = pool.allocate_audio(576).unwrap().freeze();
    let input_pointer = input.as_ptr() as usize;
    let state = pool.allocate_audio(256).unwrap().freeze();
    let mut rate = pool.allocate_bytes(8).unwrap();
    rate.copy_from_slice(&16000i64.to_le_bytes());
    let budget = BufferBudget::new(2, 1028);
    let allocator = Arc::new(BoundOutputAllocator::new(pool.clone()).with_budget(budget.clone()));
    Python::attach(|py| {
        let locals = PyDict::new(py);
        locals.set_item("sdk_path", sdk.to_str().unwrap()).unwrap();
        locals
            .set_item("model_path", model.to_str().unwrap())
            .unwrap();
        locals
            .set_item(
                "allocator",
                Py::new(py, OutputAllocator::new(allocator.clone())).unwrap(),
            )
            .unwrap();
        for (name, data) in [
            ("audio_owner", BufferData::Audio(input)),
            ("state_owner", BufferData::Audio(state)),
            ("rate_owner", BufferData::Bytes(rate.freeze())),
        ] {
            locals
                .set_item(name, Py::new(py, ReadBuffer::new(data).unwrap()).unwrap())
                .unwrap();
        }
        locals.set_item("input_pointer", input_pointer).unwrap();
        py.run(
            c"
import sys, gc
sys.path.insert(0, sdk_path)
import numpy as np
import onnxruntime as ort
from remotemedia.core.onnx_io import run_onnx_into
options = ort.SessionOptions()
options.intra_op_num_threads = 1
options.inter_op_num_threads = 1
model = ort.InferenceSession(model_path, options, providers=['CPUExecutionProvider'])
feeds = {'input': np.asarray(audio_owner).reshape(1,576),
         'state': np.asarray(state_owner).reshape(2,1,128),
         'sr': np.asarray(rate_owner).view(np.int64).reshape(())}
assert feeds['input'].ctypes.data == input_pointer
assert all(not x.flags.writeable for x in feeds.values())
expected = model.run(None, feeds)
outputs = run_onnx_into(model, feeds, allocator, max_output_bytes=1028,
                        output_shapes={'output':(1,1),'stateN':(2,1,128)}, audio_outputs={'output'})
for value, baseline in zip(outputs.values(), expected):
    np.testing.assert_array_equal(value, baseline)
del value, baseline
probability = outputs['output']
retained = outputs['stateN'].reshape(-1)[7:17]
state_pointer = outputs['stateN'].ctypes.data
probability_pointer = probability.ctypes.data
del outputs, expected
failure = None
try:
    run_onnx_into(model, feeds, allocator, max_output_bytes=1028,
                   output_shapes={'output':(1,1),'stateN':(2,1,128)})
except Exception as error:
    failure = error
assert failure is not None, 'retained outputs must exhaust the two-owner budget'
del failure, feeds, audio_owner, state_owner, rate_owner, model, allocator
gc.collect()
",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
        let probability = recover_audio(&locals.get_item("probability").unwrap().unwrap()).unwrap();
        let retained = recover_bytes(&locals.get_item("retained").unwrap().unwrap()).unwrap();
        let pointer: usize = locals
            .get_item("state_pointer")
            .unwrap()
            .unwrap()
            .extract()
            .unwrap();
        let probability_pointer: usize = locals
            .get_item("probability_pointer")
            .unwrap()
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(probability.as_ptr() as usize, probability_pointer);
        assert_eq!(retained.as_ptr() as usize, pointer + 7 * 4);
        assert_eq!(retained.len(), 40);
        assert!(allocator.owns_payload(&RuntimeData::Binary(retained.clone())));
        let delivery = pool.export(&retained, PeerId(991)).unwrap();
        assert_eq!(delivery.descriptor().offset, 28);
        drop(delivery);
        assert_eq!((budget.usage().owners, budget.usage().bytes), (2, 1028));
        py.run(
            c"del probability, retained; gc.collect()",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
        // Only Rust's recovered views now retain model outputs and budget credit.
        pool.close();
        assert!(probability[0].is_finite());
        assert!(retained.iter().any(|byte| *byte != 0));
        drop(probability);
        assert_eq!((budget.usage().owners, budget.usage().bytes), (1, 1024));
        drop(retained);
        assert_eq!((budget.usage().owners, budget.usage().bytes), (0, 0));
    });
}
