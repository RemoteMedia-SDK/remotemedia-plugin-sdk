#![cfg(feature = "runtime")]
use abi_stable::{
    sabi_trait::TD_Opaque,
    std_types::{RArc, ROption, RSlice},
};
use loadable_node_abi::{
    buffer::{BufferElement, BufferOwner, BufferOwner_TO},
    runtime_buffer::{decode, encode, export_allocator, import_allocator, MAX_METADATA_BYTES},
};
use remotemedia_shared_memory::{PeerId, PoolConfig, SessionId, SharedPool, SizeClass};
use remotemedia_types::{BufferAllocationError, RuntimeData, SharedBuffer};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn pool() -> SharedPool {
    SharedPool::new(
        SessionId([3; 16]),
        PoolConfig {
            classes: vec![SizeClass {
                bytes: 1024,
                slots: 1,
            }],
            max_bytes: 1024,
            max_leases: 4,
        },
    )
    .unwrap()
}
fn audio(samples: SharedBuffer<f32>) -> RuntimeData {
    RuntimeData::Audio {
        samples: samples.into(),
        sample_rate: 48000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    }
}

#[test]
fn allocator_bridge_preserves_address_provenance_and_exhaustion() {
    let pool = pool();
    let allocator = import_allocator(export_allocator(Arc::new(pool.clone())));
    let mut loan = allocator.allocate_audio(8).unwrap();
    let pointer = loan.as_ptr();
    loan.fill(0.5);
    assert!(matches!(
        allocator.allocate_bytes(1),
        Err(BufferAllocationError::Exhausted)
    ));
    let data = decode(encode(audio(loan.freeze())).unwrap()).unwrap();
    let RuntimeData::Audio { samples, .. } = data else {
        panic!()
    };
    assert_eq!(samples.as_ptr(), pointer);
    assert_eq!(samples.as_slice(), &[0.5; 8]);
    assert!(pool.export(&samples.into_shared(), PeerId(1)).is_ok());
    assert!(allocator.allocate_bytes(1).is_ok());
    pool.close();
    assert!(matches!(
        allocator.allocate_audio(1),
        Err(BufferAllocationError::Unavailable)
    ));
}

struct Tracked {
    data: Vec<u8>,
    released: Arc<AtomicUsize>,
}
impl AsRef<[u8]> for Tracked {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
impl Drop for Tracked {
    fn drop(&mut self) {
        self.released.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn cloned_messages_and_nested_slices_release_exactly_once() {
    let released = Arc::new(AtomicUsize::new(0));
    let data = SharedBuffer::from_owner(Tracked {
        data: vec![9; 32],
        released: released.clone(),
    });
    let pointer = data.as_ptr();
    let message = encode(RuntimeData::Binary(data)).unwrap();
    let another = message.clone();
    let RuntimeData::Binary(buffer) = decode(message).unwrap() else {
        panic!()
    };
    let slice = buffer.slice(2..8).unwrap();
    assert_eq!(slice.as_ptr(), pointer.wrapping_add(2));
    drop(buffer);
    drop(another);
    assert_eq!(released.load(Ordering::SeqCst), 0);
    std::thread::spawn(move || drop(slice)).join().unwrap();
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[test]
fn malformed_native_ranges_versions_and_metadata_are_rejected() {
    let message = encode(audio(vec![1.0; 4].into())).unwrap();
    for mutation in 0..7 {
        let mut bad = message.clone();
        match mutation {
            0 => bad.version += 1,
            1 => bad.buffer.as_mut().unwrap().offset = u64::MAX,
            2 => bad.buffer.as_mut().unwrap().byte_len = 17,
            3 => bad.buffer.as_mut().unwrap().offset = 2,
            4 => bad.buffer = ROption::RNone,
            5 => bad.metadata = b"{}".to_vec().into(),
            _ => bad.metadata = vec![b' '; MAX_METADATA_BYTES + 1].into(),
        }
        assert!(decode(bad).is_err());
    }
    assert!(encode(RuntimeData::Text("x".repeat(MAX_METADATA_BYTES))).is_err());
}

#[derive(Clone)]
struct Unaligned(Arc<Vec<u8>>);
// SAFETY: every clone retains the same immutable initialized allocation. The
// intentionally unaligned view tests the importer before any f32 slice exists.
unsafe impl BufferOwner for Unaligned {
    fn element(&self) -> BufferElement {
        BufferElement::AudioF32
    }
    fn view(&self) -> RSlice<'_, u8> {
        (&self.0[1..9]).into()
    }
}

#[test]
fn foreign_typed_buffer_alignment_is_checked_before_access() {
    let mut message = encode(audio(vec![0.0; 2].into())).unwrap();
    message.buffer.as_mut().unwrap().owner =
        BufferOwner_TO::from_ptr(RArc::new(Unaligned(Arc::new(vec![0; 9]))), TD_Opaque);
    assert!(decode(message).is_err());
}

#[test]
fn abandoned_and_empty_native_loans_have_bounded_lifetimes() {
    let pool = pool();
    let allocator = import_allocator(export_allocator(Arc::new(pool.clone())));
    drop(allocator.allocate_audio(4).unwrap());
    let data =
        decode(encode(audio(allocator.allocate_audio(0).unwrap().freeze())).unwrap()).unwrap();
    assert!(matches!(
        allocator.allocate_bytes(1),
        Err(BufferAllocationError::Exhausted)
    ));
    drop(data);
    assert!(allocator.allocate_bytes(1).is_ok());
}
