#![cfg(feature = "runtime")]
use abi_stable::{
    sabi_trait::TD_Opaque,
    std_types::{RArc, RSlice},
};
use loadable_node_abi::{
    buffer::{BufferElement, BufferOwner, BufferOwner_TO},
    runtime_buffer::{decode, encode},
};
use remotemedia_types::{PayloadStorage, RuntimeData, SharedBuffer, TensorEncoding};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Barrier,
};

struct Allocation {
    storage: PayloadStorage,
    releases: Arc<AtomicUsize>,
}
impl Drop for Allocation {
    fn drop(&mut self) {
        self.releases.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct ForeignOwner(Arc<Allocation>, BufferElement);
// SAFETY: every ABI clone retains the same immutable, initialized storage.
// The test deliberately disables downcasting to exercise the foreign importer.
unsafe impl BufferOwner for ForeignOwner {
    fn element(&self) -> BufferElement {
        self.1
    }
    fn view(&self) -> RSlice<'_, u8> {
        match &self.0.storage {
            PayloadStorage::Audio(data) => {
                // SAFETY: f32 has no padding; the owner retains these initialized
                // bytes throughout the returned immutable borrow.
                unsafe {
                    std::slice::from_raw_parts(
                        data.as_ptr().cast(),
                        std::mem::size_of_val(data.as_slice()),
                    )
                }
                .into()
            }
            PayloadStorage::Bytes(data) => data.as_ref().into(),
            PayloadStorage::None => unreachable!(),
        }
    }
}

fn read_after_parent_drop<T>(
    data: SharedBuffer<T>,
    value: T,
    pointer: usize,
    releases: &AtomicUsize,
) where
    T: Copy + Send + Sync + std::fmt::Debug + PartialEq + 'static,
{
    assert_eq!(data.as_ptr() as usize, pointer);
    let barrier = Arc::new(Barrier::new(9));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let retained = data.slice(2..10).unwrap().slice(1..5).unwrap();
            assert!(data.shares_storage(&retained));
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                assert_eq!(retained.storage_range(), 3..7);
                assert_eq!(
                    retained.as_ptr() as usize,
                    pointer + 3 * std::mem::size_of::<T>()
                );
                assert_eq!(retained.as_slice(), &[value; 4]);
            })
        })
        .collect();
    drop(data);
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    barrier.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

#[test]
fn opaque_foreign_audio_and_bytes_retain_views_until_the_last_reader_thread() {
    for data in [
        RuntimeData::Binary(vec![7u8; 12].into()),
        RuntimeData::Audio {
            samples: vec![0.75; 12].into(),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        },
    ] {
        let mut message = encode(data.clone()).unwrap();
        let element = message.buffer.as_ref().unwrap().owner.element();
        let (_, storage) = data.into_shared_parts(TensorEncoding::Runtime).unwrap();
        let releases = Arc::new(AtomicUsize::new(0));
        message.buffer.as_mut().unwrap().owner = BufferOwner_TO::from_ptr(
            RArc::new(ForeignOwner(
                Arc::new(Allocation {
                    storage,
                    releases: releases.clone(),
                }),
                element,
            )),
            TD_Opaque,
        );
        let pointer = message.buffer.as_ref().unwrap().owner.view().as_ptr() as usize;
        let (_, storage) = decode(message)
            .unwrap()
            .into_shared_parts(TensorEncoding::Runtime)
            .unwrap();
        match storage {
            PayloadStorage::Audio(data) => read_after_parent_drop(data, 0.75, pointer, &releases),
            PayloadStorage::Bytes(data) => read_after_parent_drop(data, 7, pointer, &releases),
            PayloadStorage::None => panic!("missing foreign storage"),
        }
    }
}
