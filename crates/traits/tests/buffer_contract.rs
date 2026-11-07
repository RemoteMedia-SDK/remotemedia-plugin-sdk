use remotemedia_traits::buffer_contract::{BufferDomain, BufferLayout, BufferPortContract};
use remotemedia_types::{AudioSamples, ImageFormat, PixelFormat, RuntimeData};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

#[test]
fn metadata_only_factory_contract_has_explicit_zero_bulk_bounds() {
    use remotemedia_traits::buffer_contract::{
        BufferContract, BufferProduction, BufferProtocol, BufferRetention,
    };
    let contract = BufferContract::metadata_only(BufferProtocol::RustOwners);
    assert_eq!(contract.production, BufferProduction::ForwardInput);
    assert_eq!(
        contract.retention,
        Some(BufferRetention {
            owners: 0,
            bytes: 0
        })
    );
    assert_eq!(contract.inputs.len(), 1);
    assert_eq!(contract.outputs.len(), 1);
    for port in contract.inputs.values().chain(contract.outputs.values()) {
        port.validate_definition().unwrap();
        port.validate_payload(&RuntimeData::Text("hello".into()))
            .unwrap();
        assert!(port
            .validate_payload(&RuntimeData::Binary(Vec::<u8>::new().into()))
            .is_err());
    }
    let decoded: BufferContract =
        serde_json::from_value(serde_json::to_value(&contract).unwrap()).unwrap();
    assert_eq!(decoded, contract);
}

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}
struct CountAlloc;
fn count() {
    if COUNTING.try_with(Cell::get).unwrap_or(false) {
        let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
    }
}
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        System.alloc(layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count();
        System.alloc_zeroed(layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count();
        System.realloc(ptr, layout, size)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}
#[global_allocator]
static ALLOCATOR: CountAlloc = CountAlloc;

fn port(layout: BufferLayout) -> BufferPortContract {
    BufferPortContract {
        layouts: vec![layout],
        domains: vec![BufferDomain::Host],
        max_payload_bytes: 64,
    }
}
fn audio(channels: u32) -> RuntimeData {
    RuntimeData::Audio {
        samples: AudioSamples::Vec(vec![0.25; 12]),
        sample_rate: 48000,
        channels,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    }
}
fn numpy(strides: Vec<isize>, c: bool, f: bool) -> RuntimeData {
    RuntimeData::Numpy {
        data: vec![0u8; 24].into(),
        shape: vec![2, 3],
        dtype: "f4".into(),
        strides,
        c_contiguous: c,
        f_contiguous: f,
    }
}
fn tensor(dtype: i32, bytes: usize) -> RuntimeData {
    RuntimeData::Tensor {
        data: vec![0u8; bytes].into(),
        shape: vec![2, 3],
        dtype,
        metadata: None,
    }
}

#[test]
fn all_valid_layouts_validate_without_allocating_or_cloning_legacy_audio() {
    let cases = vec![
        (BufferLayout::AudioInterleavedF32, audio(2)),
        (
            BufferLayout::VideoBytes,
            RuntimeData::Video {
                pixel_data: vec![0u8; 24].into(),
                width: 4,
                height: 2,
                format: PixelFormat::Rgb24,
                codec: None,
                frame_number: 0,
                timestamp_us: 0,
                is_keyframe: false,
                stream_id: None,
                arrival_ts_us: None,
            },
        ),
        (
            BufferLayout::ImageBytes,
            RuntimeData::Image {
                data: vec![0u8; 24].into(),
                width: 3,
                height: 2,
                format: ImageFormat::Raw {
                    pixel_format: PixelFormat::Rgba32,
                },
                timestamp_us: None,
                stream_id: None,
                metadata: None,
            },
        ),
        (BufferLayout::TensorRuntime, tensor(0, 24)),
        (BufferLayout::TensorPythonIpc, tensor(2, 48)),
        (BufferLayout::TensorProtobuf, tensor(2, 12)),
        (
            BufferLayout::NumpyCContiguous,
            numpy(vec![12, 4], true, false),
        ),
        (
            BufferLayout::NumpyFContiguous,
            numpy(vec![4, 8], false, true),
        ),
        (
            BufferLayout::Binary,
            RuntimeData::Binary(vec![0u8; 24].into()),
        ),
    ];
    for (layout, data) in cases {
        let contract = port(layout);
        ALLOCS.with(|v| v.set(0));
        COUNTING.with(|v| v.set(true));
        let result = contract.validate_payload(&data);
        COUNTING.with(|v| v.set(false));
        assert_eq!(ALLOCS.with(Cell::get), 0, "{:?}", contract.layouts);
        result.unwrap();
        if let RuntimeData::Audio { samples, .. } = data {
            assert!(matches!(samples, AudioSamples::Vec(_)));
        }
    }
}

#[test]
fn malformed_sizes_shapes_and_layouts_fail_without_implicit_packing() {
    assert!(port(BufferLayout::AudioInterleavedF32)
        .validate_payload(&audio(5))
        .is_err());
    assert!(port(BufferLayout::TensorRuntime)
        .validate_payload(&tensor(0, 20))
        .is_err());
    assert!(port(BufferLayout::NumpyCContiguous)
        .validate_payload(&numpy(vec![12, -4], true, false))
        .is_err());
    assert!(port(BufferLayout::NumpyCContiguous)
        .validate_payload(&numpy(vec![4, 8], false, true))
        .is_err());
    let mut limited = port(BufferLayout::Binary);
    limited.max_payload_bytes = 2;
    assert!(limited
        .validate_payload(&RuntimeData::Binary(vec![0u8; 3].into()))
        .unwrap_err()
        .contains("declared maximum"));
    let metadata = BufferPortContract {
        layouts: vec![],
        domains: vec![],
        max_payload_bytes: 0,
    };
    metadata
        .validate_payload(&RuntimeData::Text("metadata".into()))
        .unwrap();
    assert!(metadata
        .validate_payload(&RuntimeData::Binary(vec![].into()))
        .is_err());
}

#[test]
fn tensor_conventions_and_memory_domains_cannot_be_guessed() {
    let mut ambiguous = port(BufferLayout::TensorRuntime);
    ambiguous.layouts.push(BufferLayout::TensorPythonIpc);
    assert!(ambiguous
        .validate_definition()
        .unwrap_err()
        .contains("ambiguous"));
    assert!(ambiguous
        .validate_payload(&tensor(1, 24))
        .unwrap_err()
        .contains("ambiguous"));
    assert!(port(BufferLayout::TensorRuntime)
        .validate_payload(&tensor(2, 48))
        .is_err());
    let mut mapped = port(BufferLayout::Binary);
    mapped.domains = vec![BufferDomain::SessionMapped];
    assert!(mapped
        .validate_payload(&RuntimeData::Binary(vec![1u8].into()))
        .unwrap_err()
        .contains("provider binding"));
    // Only the host can establish provider identity; after it does so, the
    // declared layout and size rules still apply to that verified domain.
    mapped
        .validate_payload_in_domain(
            &RuntimeData::Binary(vec![1u8].into()),
            &BufferDomain::SessionMapped,
        )
        .unwrap();
    assert!(mapped
        .validate_payload_in_domain(
            &RuntimeData::Binary(vec![0u8; 65].into()),
            &BufferDomain::SessionMapped
        )
        .is_err());
    assert!(mapped
        .validate_payload_in_domain(&audio(2), &BufferDomain::SessionMapped)
        .is_err());
}
