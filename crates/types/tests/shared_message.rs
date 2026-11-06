use remotemedia_types::{
    AudioSamples, ControlMessageType, ImageFormat, PayloadKind, PayloadLayoutError, PayloadStorage,
    PixelFormat, RuntimeData, RuntimeMetadata, TensorEncoding, VideoCodec,
};

const ENCODING: TensorEncoding = TensorEncoding::Runtime;

fn byte_data() -> remotemedia_types::SharedBytes {
    vec![7; 24].into()
}

#[test]
fn split_and_attach_preserve_every_variant_and_bulk_address() {
    let cases = vec![
        RuntimeData::Audio {
            samples: AudioSamples::Vec(vec![0.5; 12]),
            sample_rate: 48000,
            channels: 2,
            stream_id: Some("speech".into()),
            timestamp_us: Some(3),
            arrival_ts_us: Some(4),
            metadata: Some(serde_json::json!({"speaker":1})),
        },
        RuntimeData::Video {
            pixel_data: byte_data(),
            width: 4,
            height: 2,
            format: PixelFormat::Rgb24,
            codec: None,
            frame_number: 5,
            timestamp_us: 6,
            is_keyframe: true,
            stream_id: Some("camera".into()),
            arrival_ts_us: Some(7),
        },
        RuntimeData::Image {
            data: byte_data(),
            width: 3,
            height: 2,
            format: ImageFormat::Raw {
                pixel_format: PixelFormat::Rgba32,
            },
            timestamp_us: Some(8),
            stream_id: Some("photo".into()),
            metadata: Some(serde_json::json!({"tag":"test"})),
        },
        RuntimeData::Tensor {
            data: byte_data(),
            shape: vec![2, 3],
            dtype: 0,
            metadata: Some(serde_json::json!({"layer":2})),
        },
        RuntimeData::Numpy {
            data: byte_data(),
            shape: vec![2, 3],
            dtype: "<f4".into(),
            strides: vec![12, 4],
            c_contiguous: true,
            f_contiguous: false,
        },
        RuntimeData::Binary(byte_data()),
        RuntimeData::Json(serde_json::json!({"data_type":"audio","value":[1,2,3]})),
        RuntimeData::Text("text".into()),
        RuntimeData::ControlMessage {
            message_type: ControlMessageType::CancelSpeculation {
                from_timestamp: 10,
                to_timestamp: 20,
            },
            segment_id: Some("seg".into()),
            timestamp_ms: 9,
            metadata: serde_json::json!({"reason":"cancel"}),
        },
        RuntimeData::File {
            path: "source.bin".into(),
            filename: Some("source.bin".into()),
            mime_type: Some("application/octet-stream".into()),
            size: Some(30),
            offset: Some(2),
            length: Some(8),
            stream_id: Some("file".into()),
        },
    ];
    for original in cases {
        original.validate_shared_layout(ENCODING).unwrap();
        let pointer = address(&original);
        let forwarding = remotemedia_types::ForwardingScope::default();
        let original = forwarding.accept(original);
        let expected = original.clone();
        let (metadata, storage) = original.into_shared_parts(ENCODING).unwrap();
        let encoded = serde_json::to_vec(&metadata).unwrap();
        let decoded: RuntimeMetadata = serde_json::from_slice(&encoded).unwrap();
        let output = decoded.attach(storage, ENCODING).unwrap();
        assert_eq!(output, expected);
        assert_eq!(address(&output), pointer);
        assert!(forwarding.accepts_output(&output));
        assert_bound_allocation(output);
    }
}

fn assert_bound_allocation(data: RuntimeData) {
    use remotemedia_types::{BoundOutputAllocator, HeapOutputAllocator, OutputAllocator};
    let allocator = BoundOutputAllocator::new(std::sync::Arc::new(HeapOutputAllocator));
    let (metadata, original) = data.into_shared_parts(ENCODING).unwrap();
    let storage = match original {
        PayloadStorage::None => PayloadStorage::None,
        PayloadStorage::Audio(samples) => {
            PayloadStorage::Audio(allocator.allocate_audio(samples.len()).unwrap().freeze())
        }
        PayloadStorage::Bytes(bytes) => {
            PayloadStorage::Bytes(allocator.allocate_bytes(bytes.len()).unwrap().freeze())
        }
    };
    let output = metadata.attach(storage, ENCODING).unwrap();
    assert!(allocator.owns_payload(&output));
    let output = remotemedia_types::ForwardingScope::default().accept(output);
    assert!(allocator.owns_payload(&output));
}

fn address(data: &RuntimeData) -> Option<usize> {
    match data {
        RuntimeData::Audio { samples, .. } => Some(samples.as_ptr() as usize),
        RuntimeData::Video { pixel_data, .. } => Some(pixel_data.as_ptr() as usize),
        RuntimeData::Image { data, .. }
        | RuntimeData::Tensor { data, .. }
        | RuntimeData::Numpy { data, .. }
        | RuntimeData::Binary(data) => Some(data.as_ptr() as usize),
        _ => None,
    }
}

#[test]
fn metadata_encoding_never_contains_bulk_bytes() {
    for len in [1, 1024, 1024 * 1024] {
        let (metadata, storage) = RuntimeData::Binary(vec![0xab; len].into())
            .into_shared_parts(ENCODING)
            .unwrap();
        assert_eq!(serde_json::to_string(&metadata).unwrap(), "\"Binary\"");
        assert_eq!(storage.layout(), (Some(PayloadKind::Bytes), len as u64));
    }
}

#[test]
fn tensor_conventions_are_explicit_and_cannot_cross_unchecked() {
    for (encoding, dtype, bytes) in [
        (ENCODING, 0, 4),
        (ENCODING, 1, 4),
        (TensorEncoding::PythonIpc, 2, 8),
        (TensorEncoding::PythonIpc, 4, 8),
        (TensorEncoding::PythonIpc, 6, 2),
        (TensorEncoding::Protobuf, 2, 2),
        (TensorEncoding::Protobuf, 4, 1),
    ] {
        let data = RuntimeData::Tensor {
            data: vec![0; bytes].into(),
            shape: vec![],
            dtype,
            metadata: None,
        };
        let (metadata, storage) = data.into_shared_parts(encoding).unwrap();
        let other = if encoding == ENCODING {
            TensorEncoding::PythonIpc
        } else {
            ENCODING
        };
        assert_eq!(
            metadata.clone().attach(storage.clone(), other).unwrap_err(),
            PayloadLayoutError::TensorEncodingMismatch
        );
        assert!(metadata.attach(storage, encoding).is_ok());
    }
    for (dtype, shape, bytes) in [
        (99, vec![1], 4),
        (0, vec![-1], 4),
        (0, vec![2, 3], 23),
        (0, vec![i32::MAX; 5], 4),
    ] {
        let input = RuntimeData::Tensor {
            data: vec![0; bytes].into(),
            shape,
            dtype,
            metadata: None,
        };
        assert!(input.into_shared_parts(ENCODING).is_err());
    }
}

fn array(
    shape: Vec<usize>,
    strides: Vec<isize>,
    dtype: &str,
    c: bool,
    f: bool,
    bytes: usize,
) -> RuntimeData {
    RuntimeData::Numpy {
        data: vec![0; bytes].into(),
        shape,
        strides,
        dtype: dtype.into(),
        c_contiguous: c,
        f_contiguous: f,
    }
}

#[test]
fn numpy_layouts_check_real_strides_flags_and_supported_dtypes() {
    for input in [
        array(vec![2, 3], vec![12, 4], "float32", true, false, 24),
        array(vec![2, 3], vec![4, 8], "<f4", false, true, 24),
        array(vec![], vec![], "uint8", true, true, 1),
        array(vec![2, 0, 3], vec![0, 0, 0], "f8", true, true, 0),
        array(vec![1, 4], vec![99, 4], "i4", true, true, 16),
    ] {
        assert!(input.into_shared_parts(ENCODING).is_ok());
    }
    for input in [
        array(vec![2, 3], vec![24, 4], "f4", false, false, 24),
        array(vec![2, 3], vec![12, -4], "f4", true, false, 24),
        array(vec![2, 3], vec![12, 4], "f4", false, true, 24),
        array(vec![2, 3], vec![12], "f4", true, false, 24),
        array(vec![2, 3], vec![0, 4], "f4", true, false, 24),
        array(vec![2, 3], vec![12, 4], ">f4", true, false, 24),
        array(vec![2, 3], vec![24, 8], "object", true, false, 48),
        array(vec![usize::MAX, 2], vec![8, 4], "f4", true, false, 8),
        array(vec![2, 3], vec![12, 4], "f4", true, false, 25),
    ] {
        assert!(input.into_shared_parts(ENCODING).is_err());
    }
}

#[test]
fn pixel_planes_require_exact_checked_dimensions_and_codec_agreement() {
    for format in [
        PixelFormat::Yuv420p,
        PixelFormat::I420,
        PixelFormat::NV12,
        PixelFormat::Rgb24,
        PixelFormat::Rgba32,
    ] {
        let bytes = match format {
            PixelFormat::Rgb24 => 48,
            PixelFormat::Rgba32 => 64,
            _ => 24,
        };
        let metadata = RuntimeMetadata::Video {
            width: 4,
            height: 4,
            format,
            codec: None,
            frame_number: 0,
            timestamp_us: 1,
            is_keyframe: true,
            stream_id: None,
            arrival_ts_us: None,
        };
        assert!(metadata
            .validate_payload(Some(PayloadKind::Bytes), bytes, ENCODING)
            .is_ok());
        assert!(metadata
            .validate_payload(Some(PayloadKind::Bytes), bytes - 1, ENCODING)
            .is_err());
    }
    for (width, height, format, codec) in [
        (3, 4, PixelFormat::I420, None),
        (4, 3, PixelFormat::NV12, None),
        (0, 4, PixelFormat::Rgb24, None),
        (u32::MAX, u32::MAX, PixelFormat::Rgba32, None),
        (4, 4, PixelFormat::Rgb24, Some(VideoCodec::H264)),
        (4, 4, PixelFormat::Encoded, None),
    ] {
        let metadata = RuntimeMetadata::Video {
            width,
            height,
            format,
            codec,
            frame_number: 0,
            timestamp_us: 1,
            is_keyframe: true,
            stream_id: None,
            arrival_ts_us: None,
        };
        assert!(metadata
            .validate_payload(Some(PayloadKind::Bytes), 24, ENCODING)
            .is_err());
    }
}

#[test]
fn audio_and_inline_metadata_cannot_claim_wrong_storage_or_lengths() {
    let metadata = RuntimeMetadata::Audio {
        sample_rate: 48000,
        channels: 2,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    };
    assert!(metadata
        .validate_payload(Some(PayloadKind::AudioF32), 16, ENCODING)
        .is_ok());
    assert!(metadata
        .validate_payload(Some(PayloadKind::AudioF32), 12, ENCODING)
        .is_err());
    assert_eq!(
        metadata
            .attach(PayloadStorage::Bytes(vec![0; 16].into()), ENCODING)
            .unwrap_err(),
        PayloadLayoutError::WrongPayload
    );
    assert!(RuntimeMetadata::Text("x".into())
        .validate_payload(None, 1, ENCODING)
        .is_err());
    assert!(RuntimeMetadata::Binary
        .attach(PayloadStorage::None, ENCODING)
        .is_err());
    let unknown = r#"{"Tensor":{"shape":[1],"dtype":0,"encoding":"Runtime","metadata":null,"payload":[1,2,3,4]}}"#;
    assert!(serde_json::from_str::<RuntimeMetadata>(unknown).is_err());
}
