#![cfg(feature = "inprocess-python")]
use pyo3::{prelude::*, types::PyModule};
use remotemedia_plugin_sdk::PythonNodeHandle;
use remotemedia_shared_memory::{PeerId, PoolConfig, SessionId, SharedPool, SizeClass};
use remotemedia_types::{AudioSamples, ControlMessageType, ImageFormat, PixelFormat, RuntimeData};
use std::sync::Arc;

fn handle(class: &str) -> PythonNodeHandle {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    // Wait without holding the GIL: NumPy import can release it while the module
    // is partially initialized, so a sys.modules presence check is insufficient.
    INSTALL.call_once(|| {
        Python::attach(|py| {
            let modules = py.import("sys").unwrap().getattr("modules").unwrap();
            if !modules.contains("shared_buffer_nodes").unwrap() {
                let source =
                    std::ffi::CString::new(include_str!("fixtures/shared_buffer_nodes.py"))
                        .unwrap();
                let module = PyModule::from_code(
                    py,
                    &source,
                    c"shared_buffer_nodes.py",
                    c"shared_buffer_nodes",
                )
                .unwrap();
                modules.set_item("shared_buffer_nodes", module).unwrap();
            }
        })
    });
    PythonNodeHandle::load("shared_buffer_nodes", class).unwrap()
}

fn allocator() -> Arc<SharedPool> {
    Arc::new(
        SharedPool::new(
            SessionId([86; 16]),
            PoolConfig {
                classes: vec![SizeClass {
                    bytes: 4096,
                    slots: 4,
                }],
                max_bytes: 16384,
                max_leases: 8,
            },
        )
        .unwrap(),
    )
}

fn audio(samples: AudioSamples) -> RuntimeData {
    RuntimeData::Audio {
        samples,
        sample_rate: 48_000,
        channels: 1,
        stream_id: Some("voice".into()),
        timestamp_us: Some(42),
        arrival_ts_us: Some(45),
        metadata: Some(serde_json::json!({"request": "shared"})),
    }
}

fn pointer(data: &RuntimeData) -> usize {
    match data {
        RuntimeData::Audio { samples, .. } => samples.as_ptr() as usize,
        RuntimeData::Video { pixel_data, .. } => pixel_data.as_ptr() as usize,
        RuntimeData::Image { data, .. }
        | RuntimeData::Tensor { data, .. }
        | RuntimeData::Numpy { data, .. }
        | RuntimeData::Binary(data) => data.as_ptr() as usize,
        _ => panic!("expected bulk data"),
    }
}

#[test]
fn audio_pass_through_promotes_legacy_vec_without_copying() {
    let input = audio(AudioSamples::Vec(vec![0.25; 12]));
    let address = pointer(&input);
    let expected = input.clone();
    let output = handle("PassThrough")
        .process_shared(input, allocator())
        .unwrap();
    assert_eq!(pointer(&output), address);
    assert_eq!(output, expected);
}

#[test]
fn numpy_gain_writes_into_context_mapping_and_output_outlives_node() {
    let pool = allocator();
    let node = handle("Gain");
    let output = node
        .process_shared(audio(vec![1.0; 12].into()), pool.clone())
        .unwrap();
    drop(node);
    let RuntimeData::Audio {
        samples, metadata, ..
    } = output
    else {
        panic!("expected audio")
    };
    assert_eq!(samples.as_slice(), &[0.5; 12]);
    assert_eq!(
        samples.as_ptr() as u64,
        metadata.unwrap()["producer_address"].as_u64().unwrap()
    );
    let data = samples.into_shared();
    let descriptor = pool.export(&data, PeerId(1)).unwrap();
    assert_eq!(descriptor.descriptor().byte_len, 48);
    drop(descriptor);
    pool.close();
    assert_eq!(data[0], 0.5);
}

#[test]
fn streaming_numpy_chunks_keep_one_input_allocation() {
    let input = audio((0..12).map(|n| n as f32).collect::<Vec<_>>().into());
    let address = pointer(&input);
    let outputs = handle("Chunks")
        .process_streaming_shared(input, allocator())
        .unwrap();
    assert_eq!(outputs.len(), 3);
    for (i, output) in outputs.iter().enumerate() {
        assert_eq!(pointer(output), address + i * 4 * 4);
    }
}

#[test]
fn non_audio_bulk_variants_preserve_allocation_and_metadata() {
    let cases = vec![
        RuntimeData::Binary(vec![5u8; 16].into()),
        RuntimeData::Video {
            pixel_data: vec![7u8; 6].into(),
            width: 2,
            height: 1,
            format: PixelFormat::Rgb24,
            codec: None,
            frame_number: 3,
            is_keyframe: true,
            timestamp_us: 100,
            stream_id: Some("camera".into()),
            arrival_ts_us: Some(120),
        },
        RuntimeData::Image {
            data: vec![9u8; 6].into(),
            format: ImageFormat::Raw {
                pixel_format: PixelFormat::Rgb24,
            },
            width: 2,
            height: 1,
            timestamp_us: Some(200),
            stream_id: Some("photo".into()),
            metadata: None,
        },
        RuntimeData::Tensor {
            data: vec![0u8; 16].into(),
            shape: vec![2, 2],
            dtype: 1,
            metadata: None,
        },
        RuntimeData::Numpy {
            data: vec![0u8; 16].into(),
            shape: vec![2, 2],
            dtype: "float32".into(),
            strides: vec![8, 4],
            c_contiguous: true,
            f_contiguous: false,
        },
    ];
    let node = handle("PassThrough");
    for input in cases {
        let address = pointer(&input);
        let expected = input.clone();
        let output = node.process_shared(input, allocator()).unwrap();
        assert_eq!(output, expected);
        assert_eq!(pointer(&output), address);
    }
}

#[test]
fn control_json_and_file_metadata_survive_shared_contract() {
    let mut cases = vec![
        RuntimeData::Json(serde_json::json!({"data_type": "audio", "value": 123})),
        RuntimeData::Text("hello".into()),
        RuntimeData::File {
            path: "data.bin".into(),
            filename: Some("data.bin".into()),
            mime_type: None,
            size: Some(8),
            offset: Some(2),
            length: Some(4),
            stream_id: Some("file".into()),
        },
    ];
    for message_type in [
        ControlMessageType::CancelSpeculation {
            from_timestamp: 7,
            to_timestamp: 9,
        },
        ControlMessageType::BatchHint {
            suggested_batch_size: 4,
        },
        ControlMessageType::DeadlineWarning { deadline_us: 999 },
    ] {
        cases.push(RuntimeData::ControlMessage {
            message_type,
            segment_id: Some("turn".into()),
            timestamp_ms: 50,
            metadata: serde_json::json!({"control": true}),
        });
    }
    let node = handle("PassThrough");
    for input in cases {
        let expected = input.clone();
        assert_eq!(node.process_shared(input, allocator()).unwrap(), expected);
    }
}

#[test]
fn private_and_unpublished_outputs_fail_without_compatibility_fallback() {
    for class in ["PrivateOutput", "Unpublished", "BadMetadata", "BadLayout"] {
        let result = handle(class).process_shared(audio(vec![1.0; 4].into()), allocator());
        assert!(result.is_err(), "accepted {class}");
    }
}

#[test]
fn shared_contract_rejects_malformed_input_and_generator_output_layouts() {
    let input = RuntimeData::Numpy {
        data: vec![0; 16].into(),
        shape: vec![2, 2],
        dtype: "float32".into(),
        strides: vec![16, 4],
        c_contiguous: true,
        f_contiguous: false,
    };
    assert!(handle("PassThrough")
        .process_shared(input, allocator())
        .is_err());
    assert!(handle("BadLayout")
        .process_streaming_shared(audio(vec![1.0; 4].into()), allocator())
        .is_err());
}

#[test]
fn compatibility_dictionary_audio_roundtrip_keeps_legacy_path_working() {
    let input = audio(vec![0.25; 4].into());
    assert_eq!(handle("PassThrough").process(&input).unwrap(), input);
}
