//! Real-GPU tests. They run only with REMOTEMEDIA_CUDA_TESTS=1, and then only
//! on the device selected by CUDA_VISIBLE_DEVICES (this workstation reserves
//! GPU0 for interactive use, so evidence runs set CUDA_VISIBLE_DEVICES=1).
use remotemedia_device_cuda::{CudaAllocation, CudaDevice, CudaStream};
use remotemedia_types::{CopyContext, CopyPolicy, CopyReason, DlDtype, RuntimeData, TransferError};
use std::sync::Arc;

fn gpu() -> Option<Arc<CudaDevice>> {
    if std::env::var("REMOTEMEDIA_CUDA_TESTS").as_deref() != Ok("1") {
        eprintln!("skipped: set REMOTEMEDIA_CUDA_TESTS=1 (and CUDA_VISIBLE_DEVICES) to run");
        return None;
    }
    assert!(
        std::env::var("CUDA_VISIBLE_DEVICES").is_ok(),
        "CUDA tests must pin a device with CUDA_VISIBLE_DEVICES"
    );
    Some(CudaDevice::open(0).expect("CUDA device 0"))
}

/// Enqueue enough work that the final fill is still pending when the host
/// looks, so readiness genuinely depends on the event.
const DELAY_WORDS: usize = 64 << 20; // 256 MiB per pass
const DELAY_PASSES: usize = 64;

/// The producer's delay work. Kept alive until the consumer has read: freeing
/// device memory can synchronize the device, which would hide a missing wait.
struct Pending(
    #[allow(dead_code)] CudaAllocation,
    #[allow(dead_code)] CudaStream,
);

fn delayed_tensor(gpu: &Arc<CudaDevice>, words: usize, value: u32) -> (RuntimeData, bool, Pending) {
    let scratch = gpu.alloc(DELAY_WORDS * 4).unwrap();
    let output = gpu.alloc(words * 4).unwrap();
    let stream = gpu.stream().unwrap();
    for pass in 0..DELAY_PASSES {
        stream
            .fill_u32(&scratch, 0, pass as u32, DELAY_WORDS)
            .unwrap();
    }
    stream.fill_u32(&output, 0, value, words).unwrap();
    let event = stream.record().unwrap();
    let pending = !event.is_complete().unwrap();
    let tensor = RuntimeData::DeviceTensor {
        buffer: output.into_buffer(),
        dtype: DlDtype::U32,
        shape: vec![words as i64],
        strides: None,
        byte_offset: 0,
        ready: Some(event.into_ready()),
        metadata: None,
    };
    (tensor, pending, Pending(scratch, stream))
}

#[test]
fn compatibility_transfer_waits_for_the_producer_and_counts_bytes() {
    let Some(gpu) = gpu() else { return };
    let words = 1 << 20;
    let (tensor, pending, _work) = delayed_tensor(&gpu, words, 0xC0FFEE);
    assert!(
        pending,
        "the producer finished before the consumer looked; delay too short"
    );
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "cuda-test");
    let host = tensor.device_to_host(&copies).unwrap();
    let RuntimeData::Numpy {
        data, dtype, shape, ..
    } = host
    else {
        panic!("expected a host NumPy tensor")
    };
    assert_eq!((dtype.as_str(), shape), ("uint32", vec![words]));
    assert!(data
        .chunks_exact(4)
        .all(|w| u32::from_le_bytes(w.try_into().unwrap()) == 0xC0FFEE));
    let counts = copies.report().entries;
    let transfer = counts
        .iter()
        .find(|(reason, _)| *reason == CopyReason::DeviceTransfer)
        .unwrap()
        .1;
    assert_eq!(
        (transfer.completed, transfer.copied_bytes),
        (1, words as u64 * 4)
    );
}

#[test]
fn strict_transfer_rejects_before_touching_the_device() {
    let Some(gpu) = gpu() else { return };
    let (tensor, _, _work) = delayed_tensor(&gpu, 1024, 7);
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "cuda-strict");
    match tensor.device_to_host(&strict) {
        Err(TransferError::Rejected(rejected)) => {
            assert_eq!(rejected.reason, CopyReason::DeviceTransfer);
            assert_eq!(rejected.bytes, 4096);
            assert!(
                rejected.to_string().contains("DeviceTransfer"),
                "{rejected}"
            );
        }
        other => panic!("strict transfer must reject, got {other:?}"),
    }
}

#[test]
fn clones_share_one_allocation_released_by_the_last_owner() {
    let Some(gpu) = gpu() else { return };
    let before = gpu.live_allocations();
    let (tensor, _, work) = delayed_tensor(&gpu, 256, 1);
    drop(work);
    let RuntimeData::DeviceTensor { buffer, .. } = &tensor else {
        unreachable!()
    };
    let first = buffer.clone();
    let second = buffer.clone();
    assert!(first.same_allocation(&second) && first.ptr() == second.ptr());
    assert_eq!(gpu.live_allocations(), before + 1);
    drop(tensor);
    drop(first);
    assert_eq!(gpu.live_allocations(), before + 1, "a clone still owns it");
    drop(second);
    assert_eq!(gpu.live_allocations(), before);
}
