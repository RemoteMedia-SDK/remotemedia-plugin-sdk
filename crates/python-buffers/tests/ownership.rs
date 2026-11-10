use _remotemedia_buffers::{
    recover_audio, recover_bytes, BufferData, OutputAllocator, OutputLoan, ReadBuffer,
};
use pyo3::{prelude::*, types::PyDict};
use remotemedia_shared_memory::{
    import_writer, writer_descriptor, PeerId, PoolConfig, SessionId, SharedMemoryError, SharedPool,
    SizeClass,
};
use remotemedia_types::SharedBuffer;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct TrackedOwner {
    data: Vec<f32>,
    releases: Arc<AtomicUsize>,
}
impl AsRef<[f32]> for TrackedOwner {
    fn as_ref(&self) -> &[f32] {
        &self.data
    }
}
impl Drop for TrackedOwner {
    fn drop(&mut self) {
        self.releases.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn numpy_input_and_retained_slice_preserve_pointer_and_release_once() {
    let releases = Arc::new(AtomicUsize::new(0));
    let data = SharedBuffer::from_owner(TrackedOwner {
        data: vec![1.0, 2.0, 3.0, 4.0],
        releases: releases.clone(),
    });
    let pointer = data.as_ptr() as usize;
    Python::attach(|py| {
        let locals = PyDict::new(py);
        locals
            .set_item(
                "input",
                Py::new(py, ReadBuffer::new(BufferData::Audio(data)).unwrap()).unwrap(),
            )
            .unwrap();
        locals.set_item("pointer", pointer).unwrap();
        py.run(
            c"
import numpy as np
import gc
array = np.asarray(input)
assert array.dtype == np.float32
assert array.__array_interface__['data'][0] == pointer
assert not array.flags.writeable
try:
    array.setflags(write=True)
except ValueError:
    pass
else:
    raise AssertionError('readonly exporter became writable')
retained = array[1:3]
del array, input
gc.collect()
assert retained.tolist() == [2.0, 3.0]
",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        let recovered = recover_audio(&locals.get_item("retained").unwrap().unwrap()).unwrap();
        let swapped = locals
            .get_item("retained")
            .unwrap()
            .unwrap()
            .call_method1(
                "view",
                (if cfg!(target_endian = "little") {
                    ">f4"
                } else {
                    "<f4"
                },),
            )
            .unwrap();
        assert!(recover_audio(&swapped).is_err());
        drop(swapped);
        assert_eq!(recovered.as_ptr() as usize, pointer + 4);
        py.run(c"del retained; gc.collect()", Some(&locals), Some(&locals))
            .unwrap();
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        assert_eq!(recovered.as_slice(), &[2.0, 3.0]);
        drop(recovered);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn python_view_handoff_recovers_an_owner_that_releases_without_the_gil() {
    let releases = Arc::new(AtomicUsize::new(0));
    let data = SharedBuffer::from_owner(TrackedOwner {
        data: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
        releases: releases.clone(),
    });
    let pointer = data.as_ptr() as usize;
    let retained = Python::attach(|py| {
        let locals = PyDict::new(py);
        locals
            .set_item(
                "input",
                Py::new(py, ReadBuffer::new(BufferData::Audio(data)).unwrap()).unwrap(),
            )
            .unwrap();
        py.run(
            c"import numpy as np; retained = np.asarray(input)[1:7]",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
        locals.get_item("retained").unwrap().unwrap().unbind()
    });
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    let recovered = std::thread::spawn(move || {
        let recovered = Python::attach(|py| {
            let data = recover_audio(retained.bind(py)).unwrap();
            assert_eq!(data.as_ptr() as usize, pointer + 4);
            data.slice(1..3).unwrap()
        });
        // Dropping a Py handle while detached may defer decref. Reattach to
        // drain it before verifying that only the recovered Rust owner remains.
        drop(retained);
        Python::attach(|_| {});
        recovered
    })
    .join()
    .unwrap();
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    std::thread::spawn(move || {
        assert_eq!(recovered.as_ptr() as usize, pointer + 8);
        assert_eq!(recovered.as_slice(), &[3.0, 4.0]);
        drop(recovered);
    })
    .join()
    .unwrap();
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

fn mapped_pool() -> Arc<SharedPool> {
    Arc::new(
        SharedPool::new(
            SessionId([75; 16]),
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
    )
}

#[test]
fn numpy_remote_grant_publishes_same_storage_and_retains_producer_reservation() {
    let pool = mapped_pool();
    let grant = pool.allocate_remote::<f32>(8, PeerId(41)).unwrap();
    // SAFETY: this harness exclusively transfers this initialized grant to the
    // exporter and acknowledges only its final mapped-owner destruction.
    let (file, descriptor) = unsafe { grant.into_transport() };
    let release_pool = pool.clone();
    let release_descriptor = descriptor.clone();
    let releases = Arc::new(AtomicUsize::new(0));
    let counter = releases.clone();
    let loan = unsafe {
        import_writer::<f32>(
            file,
            descriptor.clone(),
            pool.session_id(),
            PeerId(41),
            move || {
                release_pool.release_producer(&release_descriptor).unwrap();
                counter.fetch_add(1, Ordering::SeqCst);
            },
        )
    }
    .unwrap();
    let address = loan.as_ptr() as usize;
    Python::attach(|py| {
        let locals = PyDict::new(py);
        locals
            .set_item(
                "loan",
                Py::new(py, OutputLoan::audio(loan).unwrap()).unwrap(),
            )
            .unwrap();
        locals.set_item("address", address).unwrap();
        py.run(
            c"
import gc
import numpy as np
array = np.asarray(loan)
assert array.__array_interface__['data'][0] == address
np.add(np.arange(8, dtype=np.float32), np.float32(3), out=array)
alias = memoryview(array)
del array
try:
    loan.publish()
except BufferError:
    pass
else:
    raise AssertionError('remote grant published while writable alias existed')
alias.release()
del alias
gc.collect()
published = loan.publish()
array = np.asarray(published)
retained = array[2:6]
assert not retained.flags.writeable
del array, published, loan
gc.collect()
",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
        let worker = recover_audio(&locals.get_item("retained").unwrap().unwrap()).unwrap();
        assert_eq!(worker.as_ptr() as usize, address + 8);
        assert_eq!(writer_descriptor(&worker), Some(&descriptor));
        // SAFETY: Python publication rejected live exports, then consumed its
        // loan; the returned ndarray and recovered owner are immutable.
        let host = unsafe { pool.publish_remote::<f32>(&descriptor) }.unwrap();
        assert_eq!(host.as_ref(), &[3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]);
        let delivery = pool.export(&host.slice(2..6).unwrap(), PeerId(42)).unwrap();
        assert_eq!(delivery.descriptor().allocation, descriptor.allocation);
        drop(delivery);
        drop(host);
        assert!(pool.allocate::<u8>(1).is_err());
        pool.close();
        py.run(c"del retained; gc.collect()", Some(&locals), Some(&locals))
            .unwrap();
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        assert_eq!(worker.as_ref(), &[5.0, 6.0, 7.0, 8.0]);
        drop(worker);
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn numpy_out_uses_mapped_loan_and_escaped_alias_blocks_publication() {
    let pool = mapped_pool();
    Python::attach(|py| {
        let locals = PyDict::new(py);
        locals
            .set_item(
                "ctx",
                Py::new(py, OutputAllocator::new(pool.clone())).unwrap(),
            )
            .unwrap();
        py.run(
            c"
import numpy as np
import gc
loan = ctx.allocate_audio(8)
array = np.asarray(loan)
pointer = array.__array_interface__['data'][0]
np.multiply(np.arange(8, dtype=np.float32), np.float32(0.5), out=array)
escaped = array[2:6]
array.setflags(write=False)
del array
gc.collect()
assert loan.writable_exports > 0
try:
    loan.publish()
except BufferError:
    pass
else:
    raise AssertionError('escaped slice allowed publication')
escaped[:] = 3.0
del escaped
gc.collect()
assert loan.writable_exports == 0
published = loan.publish()
result = np.asarray(published)
assert result.__array_interface__['data'][0] == pointer
assert not result.flags.writeable
assert result.tolist() == [0.0, 0.5, 3.0, 3.0, 3.0, 3.0, 3.0, 3.5]
for action in (lambda: loan.publish(), lambda: memoryview(loan)):
    try:
        action()
    except BufferError:
        pass
    else:
        raise AssertionError('published loan remained writable')
del ctx, loan, published
gc.collect()
",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
        let data = recover_audio(&locals.get_item("result").unwrap().unwrap()).unwrap();
        let pointer: usize = locals
            .get_item("pointer")
            .unwrap()
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(data.as_ptr() as usize, pointer);
        assert!(matches!(
            pool.allocate::<f32>(8),
            Err(SharedMemoryError::Exhausted)
        ));
        let descriptor = pool.export(&data, PeerId(1)).unwrap();
        assert_eq!(descriptor.descriptor().byte_len, 32);
        drop(descriptor);
        pool.close();
        py.run(
            c"assert result[2] == 3.0; del result; gc.collect()",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
        assert_eq!(data[2], 3.0);
    });
}

#[test]
fn independent_exports_and_readonly_memoryview_still_block_publication() {
    Python::attach(|py| {
        let locals = PyDict::new(py);
        locals
            .set_item(
                "ctx",
                Py::new(
                    py,
                    OutputAllocator::new(Arc::new(remotemedia_types::HeapOutputAllocator)),
                )
                .unwrap(),
            )
            .unwrap();
        py.run(
            c"
import gc
loan = ctx.allocate_bytes(16)
first = memoryview(loan)
second = memoryview(loan)
readonly_alias = first.toreadonly()[2:8]
assert loan.writable_exports == 2
first.release()
second.release()
del first, second
gc.collect()
assert loan.writable_exports == 1
try:
    loan.publish()
except BufferError:
    pass
else:
    raise AssertionError('readonly alias released its original writable export')
readonly_alias.release()
assert loan.writable_exports == 0
published = loan.publish()
assert memoryview(published).readonly
",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
    });
}

#[test]
fn private_arrays_writable_loans_dtype_changes_and_strides_are_rejected() {
    Python::attach(|py| {
        let locals = PyDict::new(py);
        locals
            .set_item(
                "input",
                Py::new(
                    py,
                    ReadBuffer::new(BufferData::Audio(vec![1.0f32; 16].into())).unwrap(),
                )
                .unwrap(),
            )
            .unwrap();
        locals
            .set_item(
                "ctx",
                Py::new(
                    py,
                    OutputAllocator::new(Arc::new(remotemedia_types::HeapOutputAllocator)),
                )
                .unwrap(),
            )
            .unwrap();
        py.run(
            c"
import numpy as np
private = np.ones(8, dtype=np.float32)
private.setflags(write=False)
array = np.asarray(input)
strided = array[::2]
reversed_view = array[::-1]
converted = array.astype(np.float64)
loan = ctx.allocate_audio(8)
unpublished = np.asarray(loan)
unpublished.setflags(write=False)
",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
        for name in [
            "private",
            "strided",
            "reversed_view",
            "converted",
            "unpublished",
        ] {
            assert!(
                recover_audio(&locals.get_item(name).unwrap().unwrap()).is_err(),
                "accepted {name}"
            );
        }
    });
}

#[test]
fn binary_views_recover_subranges_without_payload_copies() {
    let data: SharedBuffer<u8> = (0..32).collect::<Vec<u8>>().into();
    let pointer = data.as_ptr() as usize;
    Python::attach(|py| {
        let locals = PyDict::new(py);
        locals
            .set_item(
                "input",
                Py::new(py, ReadBuffer::new(BufferData::Bytes(data)).unwrap()).unwrap(),
            )
            .unwrap();
        py.run(
            c"
import numpy as np
array = np.frombuffer(input, dtype=np.uint8).reshape(4, 8)
view = array[1:3].reshape(16)
typed = np.frombuffer(input, dtype=np.uint32)[2:6]
fortran = np.ndarray((4, 8), dtype=np.uint8, buffer=input, order='F')
",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
        let bytes = recover_bytes(&locals.get_item("view").unwrap().unwrap()).unwrap();
        assert_eq!(bytes.as_ptr() as usize, pointer + 8);
        assert_eq!(bytes.as_slice(), &(8..24).collect::<Vec<u8>>());
        let typed = recover_bytes(&locals.get_item("typed").unwrap().unwrap()).unwrap();
        assert_eq!(typed.as_ptr() as usize, pointer + 8);
        assert!(typed.shares_storage(&bytes));
        let fortran = recover_bytes(&locals.get_item("fortran").unwrap().unwrap()).unwrap();
        assert_eq!(fortran.as_ptr() as usize, pointer);
        assert_eq!(fortran.len(), 32);
        assert!(fortran.shares_storage(&bytes));
    });
}

#[test]
fn empty_and_direct_memoryview_outputs_are_supported() {
    Python::attach(|py| {
        let locals = PyDict::new(py);
        locals
            .set_item(
                "ctx",
                Py::new(
                    py,
                    OutputAllocator::new(Arc::new(remotemedia_types::HeapOutputAllocator)),
                )
                .unwrap(),
            )
            .unwrap();
        py.run(
            c"
import numpy as np
audio = ctx.allocate_audio(0).publish()
empty = np.asarray(audio)
bytes_output = ctx.allocate_bytes(8).publish()
view = memoryview(bytes_output)[2:6]
",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
        assert!(recover_audio(&locals.get_item("audio").unwrap().unwrap())
            .unwrap()
            .is_empty());
        assert!(recover_audio(&locals.get_item("empty").unwrap().unwrap())
            .unwrap()
            .is_empty());
        assert_eq!(
            recover_bytes(&locals.get_item("view").unwrap().unwrap())
                .unwrap()
                .as_slice(),
            &[0; 4]
        );
    });
}

#[test]
fn dropping_unpublished_python_loan_returns_pool_slot_after_final_view() {
    let pool = mapped_pool();
    Python::attach(|py| {
        let locals = PyDict::new(py);
        locals
            .set_item(
                "ctx",
                Py::new(py, OutputAllocator::new(pool.clone())).unwrap(),
            )
            .unwrap();
        py.run(
            c"
import gc
loan = ctx.allocate_bytes(32)
retained = memoryview(loan)[4:8]
del loan, ctx
gc.collect()
retained[:] = b'test'
",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
        assert!(matches!(
            pool.allocate::<u8>(1),
            Err(SharedMemoryError::Exhausted)
        ));
        py.run(
            c"retained.release(); del retained; gc.collect()",
            Some(&locals),
            Some(&locals),
        )
        .unwrap();
        assert!(pool.allocate::<u8>(32).is_ok());
    });
}
