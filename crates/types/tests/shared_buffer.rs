use remotemedia_types::{AudioSamples, ImageFormat, PixelFormat, RuntimeData, SharedBuffer};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[test]
fn audio_length_and_byte_count_do_not_access_external_payloads() {
    struct MeasuredOwner(Arc<AtomicUsize>);
    impl AsRef<[f32]> for MeasuredOwner {
        fn as_ref(&self) -> &[f32] {
            self.0.fetch_add(1, Ordering::SeqCst);
            &[0.25, -0.5]
        }
    }
    let reads = Arc::new(AtomicUsize::new(0));
    let samples = SharedBuffer::from_owner(MeasuredOwner(reads.clone()));
    let initial = reads.load(Ordering::SeqCst);
    let samples: AudioSamples = samples.into();
    assert_eq!(samples.len(), 2);
    assert!(!samples.is_empty());
    let data = RuntimeData::Audio {
        samples,
        sample_rate: 48000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    };
    assert_eq!(data.bulk_payload_bytes(), 8);
    assert_eq!(reads.load(Ordering::SeqCst), initial);
    let RuntimeData::Audio { samples, .. } = data else {
        unreachable!()
    };
    assert_eq!(samples.as_slice(), &[0.25, -0.5]);
    assert!(reads.load(Ordering::SeqCst) > initial);
}

struct TrackedOwner {
    values: Vec<f32>,
    releases: Arc<AtomicUsize>,
}

#[test]
fn runtime_audio_promotion_preserves_allocation_and_metadata() {
    let mut samples = Vec::with_capacity(1024);
    samples.extend_from_slice(&[1.0, 2.0]);
    let pointer = samples.as_ptr();
    let data = RuntimeData::Audio {
        samples: AudioSamples::Vec(samples),
        sample_rate: 48000,
        channels: 1,
        stream_id: Some("voice".into()),
        timestamp_us: Some(71),
        arrival_ts_us: Some(80),
        metadata: Some(serde_json::json!({"participant_id": "alice"})),
    }
    .into_shared()
    .into_shared();
    let retained = data.clone();
    drop(data);
    let RuntimeData::Audio {
        samples,
        sample_rate,
        channels,
        stream_id,
        timestamp_us,
        arrival_ts_us,
        metadata,
    } = retained
    else {
        panic!("audio required")
    };
    assert_eq!(samples.as_ptr(), pointer);
    assert_eq!(samples.variant_name(), "Shared");
    assert_eq!(
        (sample_rate, channels, timestamp_us, arrival_ts_us),
        (48000, 1, Some(71), Some(80))
    );
    assert_eq!(stream_id.as_deref(), Some("voice"));
    assert_eq!(metadata.unwrap()["participant_id"], "alice");
    assert_eq!(samples.into_vec().capacity(), 1024);
}

impl AsRef<[f32]> for TrackedOwner {
    fn as_ref(&self) -> &[f32] {
        &self.values
    }
}

impl Drop for TrackedOwner {
    fn drop(&mut self) {
        self.releases.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn moving_a_vec_preserves_pointer_and_spare_capacity() {
    let mut input = Vec::with_capacity(4096);
    input.extend_from_slice(&[1.0, 2.0, 3.0, 4.0]);
    let pointer = input.as_ptr();
    let capacity = input.capacity();
    let buffer = SharedBuffer::from(input);
    assert_eq!(buffer.as_ptr(), pointer);
    let clone = buffer.clone();
    assert!(buffer.shares_storage(&clone));
    assert_eq!(clone.as_ptr(), pointer);
    drop(clone);
    let output = buffer.into_vec();
    assert_eq!(output.as_ptr(), pointer);
    assert_eq!(output.capacity(), capacity);
}

#[test]
fn nested_slice_retains_owner_across_thread_and_parent_drop() {
    let releases = Arc::new(AtomicUsize::new(0));
    let values = vec![1.0, 2.0, 3.0, 4.0];
    let slice_pointer = values[2..].as_ptr() as usize;
    let buffer = SharedBuffer::from_owner(TrackedOwner {
        values,
        releases: Arc::clone(&releases),
    });
    let slice = buffer.slice(1..4).unwrap().slice(1..2).unwrap();
    assert!(buffer.shares_storage(&slice));
    drop(buffer);
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    std::thread::spawn(move || {
        assert_eq!(slice.as_ptr() as usize, slice_pointer);
        assert_eq!(slice.as_slice(), &[3.0]);
    })
    .join()
    .unwrap();
    assert_eq!(releases.load(Ordering::SeqCst), 1);
}

#[test]
fn empty_and_invalid_ranges_do_not_escape_the_view() {
    let buffer = SharedBuffer::from(vec![1u8, 2, 3]);
    let middle = buffer.slice(1..2).unwrap();
    assert!(middle.slice(1..1).unwrap().is_empty());
    assert!(middle.slice(0..2).is_err());
    // Build a malformed descriptor, not a reverse iterator.
    let reversed = std::ops::Range { start: 2, end: 1 };
    assert!(middle.slice(reversed).is_err());
    assert!(middle.slice(usize::MAX..usize::MAX).is_err());
    assert!(buffer.slice(0..0).unwrap().shares_storage(&buffer));
}

#[test]
fn materializing_one_alias_does_not_mutate_another() {
    let buffer = SharedBuffer::from(vec![10, 20, 30]);
    let pointer = buffer.as_ptr();
    let mut owned = buffer.clone().into_vec();
    assert_ne!(owned.as_ptr(), pointer);
    owned[0] = 99;
    assert_eq!(buffer.as_slice(), &[10, 20, 30]);
    assert_eq!(buffer.slice(1..3).unwrap().into_vec(), vec![20, 30]);
}

#[test]
fn boxed_and_arc_slices_keep_their_allocation() {
    let input = vec![10u8, 20].into_boxed_slice();
    let pointer = input.as_ptr();
    let buffer = SharedBuffer::from(input);
    assert_eq!(buffer.as_ptr(), pointer);
    let arc: Arc<[u8]> = Arc::from([30, 40]);
    let buffer = SharedBuffer::from(Arc::clone(&arc));
    assert_eq!(buffer.as_ptr(), arc.as_ptr());
    assert!(Arc::ptr_eq(&buffer.into_arc(), &arc));
}

#[test]
fn portable_encoding_matches_a_vec_and_only_encodes_visible_range() {
    let buffer = SharedBuffer::from(vec![11u8, 22, 33, 44]);
    let slice = buffer.slice(1..3).unwrap();
    let encoded = rmp_serde::to_vec_named(&slice).unwrap();
    assert_eq!(encoded, rmp_serde::to_vec_named(&vec![22u8, 33]).unwrap());
    let decoded: SharedBuffer<u8> = rmp_serde::from_slice(&encoded).unwrap();
    assert_eq!(decoded, slice);
    assert!(!decoded.shares_storage(&slice));
}

#[test]
fn runtime_bulk_variants_clone_without_moving_payloads() {
    let storage = SharedBuffer::from(vec![0u8, 0, 128, 63]);
    let messages = [
        RuntimeData::Binary(storage.clone()),
        RuntimeData::Tensor {
            data: storage.clone(),
            shape: vec![1],
            dtype: 0,
            metadata: None,
        },
        RuntimeData::Numpy {
            data: storage.clone(),
            shape: vec![1],
            dtype: "float32".into(),
            strides: vec![4],
            c_contiguous: true,
            f_contiguous: true,
        },
        RuntimeData::Image {
            data: storage.clone(),
            format: ImageFormat::Raw {
                pixel_format: PixelFormat::Rgba32,
            },
            width: 1,
            height: 1,
            timestamp_us: None,
            stream_id: None,
            metadata: None,
        },
        RuntimeData::Video {
            pixel_data: storage.clone(),
            width: 1,
            height: 1,
            format: PixelFormat::Rgba32,
            codec: None,
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: true,
            stream_id: None,
            arrival_ts_us: None,
        },
    ];
    for message in messages {
        let clone = message.clone();
        drop(message);
        let cloned_storage = match &clone {
            RuntimeData::Binary(data)
            | RuntimeData::Tensor { data, .. }
            | RuntimeData::Numpy { data, .. }
            | RuntimeData::Image { data, .. }
            | RuntimeData::Video {
                pixel_data: data, ..
            } => data,
            _ => unreachable!(),
        };
        assert!(storage.shares_storage(cloned_storage));
        assert_eq!(storage.as_ptr(), cloned_storage.as_ptr());
    }
}

#[test]
fn normal_audio_conversion_and_legacy_promotion_preserve_allocations() {
    let input = vec![1.0, 2.0, 3.0];
    let pointer = input.as_ptr();
    let samples = AudioSamples::from(input);
    assert_eq!(samples.clone().as_ptr(), pointer);
    let promoted = samples.into_shared();
    assert_eq!(promoted.as_ptr(), pointer);

    let legacy = vec![4.0, 5.0, 6.0];
    let pointer = legacy.as_ptr();
    let shared = AudioSamples::Vec(legacy).into_shared();
    assert_eq!(shared.clone().as_ptr(), pointer);
    let recovered = shared.into_vec();
    assert_eq!(recovered.as_ptr(), pointer);
}
