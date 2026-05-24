//! Round-trip wire-format tests for `RuntimeData` and friends.
//!
//! Every variant of `RuntimeData` must serialize via `rmp-serde`
//! and deserialize back to a byte-for-byte equal value. This is the
//! safety net that lets Task A2 move the type out of `core` without
//! changing the wire format.

use remotemedia_types::{
    AudioSamples, ControlMessageType, ImageFormat, PixelFormat, RuntimeData, VideoCodec,
};

/// Round-trip via `rmp-serde` in **named** mode.
///
/// We use `to_vec_named` (the field-name-preserving encoding)
/// rather than the default compact positional encoding because the
/// wire-format types include `#[serde(default,
/// skip_serializing_if = "Option::is_none")]` on optional fields.
/// In compact mode, skipping a positional field shifts every later
/// field over by one — i.e. compact + `skip_serializing_if` are
/// fundamentally incompatible. The named encoder writes a map keyed
/// by field name, so omitted fields are simply absent and the
/// `#[serde(default)]` annotation fills them on decode.
///
/// This matches how the typed wire format is actually consumed
/// downstream (e.g. via JSON in the introspection API). Transports
/// that prefer compact rmp-serde are responsible for guaranteeing
/// they never need to skip fields — that is a transport-layer
/// concern, not a wire-shape concern owned by this crate.
fn roundtrip(original: RuntimeData) {
    let bytes = rmp_serde::to_vec_named(&original).expect("serialize");
    let decoded: RuntimeData = rmp_serde::from_slice(&bytes).expect("deserialize");
    assert_eq!(original, decoded);
}

#[test]
fn audio_roundtrips() {
    let original = RuntimeData::Audio {
        samples: AudioSamples::Vec(vec![0.1, 0.2, 0.3]),
        sample_rate: 16_000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    };
    roundtrip(original);
}

#[test]
fn audio_with_metadata_roundtrips() {
    let original = RuntimeData::Audio {
        samples: AudioSamples::Vec(vec![0.0, 1.0, -1.0, 0.5]),
        sample_rate: 48_000,
        channels: 2,
        stream_id: Some("main".into()),
        timestamp_us: Some(123_456),
        arrival_ts_us: Some(123_457),
        metadata: Some(serde_json::json!({"speaker": "alice", "confidence": 0.95})),
    };
    roundtrip(original);
}

#[test]
fn video_roundtrips() {
    let original = RuntimeData::Video {
        pixel_data: vec![1, 2, 3, 4, 5],
        width: 1280,
        height: 720,
        format: PixelFormat::Yuv420p,
        codec: None,
        frame_number: 42,
        timestamp_us: 1_000_000,
        is_keyframe: true,
        stream_id: None,
        arrival_ts_us: None,
    };
    roundtrip(original);
}

#[test]
fn video_encoded_roundtrips() {
    let original = RuntimeData::Video {
        pixel_data: vec![0xff, 0xfe, 0xfd],
        width: 1920,
        height: 1080,
        format: PixelFormat::Encoded,
        codec: Some(VideoCodec::Vp8),
        frame_number: 100,
        timestamp_us: 2_000_000,
        is_keyframe: false,
        stream_id: Some("video_track".into()),
        arrival_ts_us: Some(2_000_500),
    };
    roundtrip(original);
}

#[test]
fn image_jpeg_roundtrips() {
    let original = RuntimeData::Image {
        data: vec![0xff, 0xd8, 0xff, 0xe0],
        format: ImageFormat::Jpeg,
        width: 640,
        height: 480,
        timestamp_us: Some(500_000),
        stream_id: None,
        metadata: None,
    };
    roundtrip(original);
}

#[test]
fn image_raw_roundtrips() {
    let original = RuntimeData::Image {
        data: vec![0; 64],
        format: ImageFormat::Raw {
            pixel_format: PixelFormat::Rgba32,
        },
        width: 4,
        height: 4,
        timestamp_us: None,
        stream_id: Some("preview".into()),
        metadata: Some(serde_json::json!({"source": "camera"})),
    };
    roundtrip(original);
}

#[test]
fn text_roundtrips() {
    roundtrip(RuntimeData::Text("hello world".into()));
}

#[test]
fn binary_roundtrips() {
    roundtrip(RuntimeData::Binary(vec![0, 1, 2, 3, 255]));
}

#[test]
fn json_roundtrips() {
    roundtrip(RuntimeData::Json(serde_json::json!({
        "key": "value",
        "n": 42,
        "arr": [1, 2, 3],
    })));
}

#[test]
fn tensor_roundtrips() {
    let original = RuntimeData::Tensor {
        data: vec![0u8; 16],
        shape: vec![2, 2, 2, 2],
        dtype: 0,
        metadata: None,
    };
    roundtrip(original);
}

#[test]
fn tensor_with_metadata_roundtrips() {
    let original = RuntimeData::Tensor {
        data: vec![1u8; 8],
        shape: vec![2, 4],
        dtype: 1,
        metadata: Some(serde_json::json!({"layer": "embedding"})),
    };
    roundtrip(original);
}

#[test]
fn numpy_roundtrips() {
    let original = RuntimeData::Numpy {
        data: vec![0u8; 32],
        shape: vec![4, 2],
        dtype: "float32".into(),
        strides: vec![8, 4],
        c_contiguous: true,
        f_contiguous: false,
    };
    roundtrip(original);
}

#[test]
fn control_message_roundtrips() {
    let original = RuntimeData::ControlMessage {
        message_type: ControlMessageType::CancelSpeculation {
            from_timestamp: 1000,
            to_timestamp: 2000,
        },
        segment_id: Some("seg-1".into()),
        timestamp_ms: 12345,
        metadata: serde_json::json!({"reason": "user_cancel"}),
    };
    roundtrip(original);
}

#[test]
fn control_message_batch_hint_roundtrips() {
    let original = RuntimeData::ControlMessage {
        message_type: ControlMessageType::BatchHint {
            suggested_batch_size: 8,
        },
        segment_id: None,
        timestamp_ms: 999,
        metadata: serde_json::Value::Null,
    };
    roundtrip(original);
}

#[test]
fn control_message_deadline_warning_roundtrips() {
    let original = RuntimeData::ControlMessage {
        message_type: ControlMessageType::DeadlineWarning {
            deadline_us: 50_000,
        },
        segment_id: None,
        timestamp_ms: 1234,
        metadata: serde_json::Value::Null,
    };
    roundtrip(original);
}

#[test]
fn file_minimal_roundtrips() {
    let original = RuntimeData::File {
        path: "/tmp/output.bin".into(),
        filename: None,
        mime_type: None,
        size: None,
        offset: None,
        length: None,
        stream_id: None,
    };
    roundtrip(original);
}

#[test]
fn file_full_roundtrips() {
    let original = RuntimeData::File {
        path: "/data/large.bin".into(),
        filename: Some("large.bin".into()),
        mime_type: Some("application/octet-stream".into()),
        size: Some(1_073_741_824),
        offset: Some(10 * 1024 * 1024),
        length: Some(64 * 1024),
        stream_id: Some("track1".into()),
    };
    roundtrip(original);
}

// ===== M3: partial-optional coverage for the named encoder =====
//
// These tests lock in the named encoder's correct-behaviour-on-partial-
// optionals — exactly the variant shapes that corrupt under the compact
// encoder used by the production loadable wire (see `mod
// compact_encoder` below and the FIXME at
// `crates/core/src/loadable/factory.rs`).

#[test]
fn audio_partial_optional_middle_some_roundtrips_named() {
    // `timestamp_us = Some(_)` while sibling optionals stay `None`.
    let original = RuntimeData::Audio {
        samples: AudioSamples::Vec(vec![0.1, 0.2]),
        sample_rate: 16_000,
        channels: 1,
        stream_id: None,
        timestamp_us: Some(42),
        arrival_ts_us: None,
        metadata: None,
    };
    roundtrip(original);
}

#[test]
fn tensor_partial_optional_metadata_some_roundtrips_named() {
    let original = RuntimeData::Tensor {
        data: vec![0u8; 4],
        shape: vec![1, 4],
        dtype: 0,
        metadata: Some(serde_json::json!({"layer": "head"})),
    };
    roundtrip(original);
}

#[test]
fn image_partial_optional_stream_id_some_roundtrips_named() {
    // `stream_id = Some(_)` while `timestamp_us` and `metadata` stay None.
    let original = RuntimeData::Image {
        data: vec![1, 2, 3],
        format: ImageFormat::Png,
        width: 8,
        height: 8,
        timestamp_us: None,
        stream_id: Some("preview".into()),
        metadata: None,
    };
    roundtrip(original);
}

// ===== rmp-serde compact-encoder behaviour documentation =====
//
// These tests document properties of `rmp_serde`'s *compact* (positional)
// encoder (`rmp_serde::to_vec`) for `RuntimeData` variants. They are
// NOT a regression suite for remotemedia code — production wire format
// uses named (map) encoding via `rmp_serde::to_vec_named` (see
// `crates/core/src/loadable/factory.rs` and
// `crates/plugin-sdk/src/adapter.rs`).
//
// Kept here as reference for anyone considering compact encoding for
// performance later: the compact encoder works for variants whose
// `Option<...>` fields are uniformly `None` (or uniformly `Some`), but
// silently corrupts the round-trip when `skip_serializing_if = "Option::is_none"`
// fields have a *mix* of `Some` / `None` — the decoder reads positionally
// and the skipped fields shift everything downstream.
mod compact_encoder {
    use super::*;

    /// Documents that rmp-serde's compact (positional) encoder
    /// survives this variant. Compact encoding is reliable for
    /// variants whose optionals are uniformly `None` (or absent).
    #[test]
    fn text_roundtrips_compact() {
        let original = RuntimeData::Text("compact-ok".into());
        let bytes = rmp_serde::to_vec(&original).expect("serialize");
        let decoded: RuntimeData = rmp_serde::from_slice(&bytes).expect("deserialize");
        assert_eq!(original, decoded);
    }

    /// Documents that rmp-serde's compact (positional) encoder
    /// survives this variant. Compact encoding is reliable for
    /// variants whose optionals are uniformly `None` (or absent).
    #[test]
    fn binary_roundtrips_compact() {
        let original = RuntimeData::Binary(vec![1, 2, 3, 4]);
        let bytes = rmp_serde::to_vec(&original).expect("serialize");
        let decoded: RuntimeData = rmp_serde::from_slice(&bytes).expect("deserialize");
        assert_eq!(original, decoded);
    }

    /// Documents that rmp-serde's compact (positional) encoder
    /// survives this variant. Compact encoding is reliable for
    /// variants whose optionals are uniformly `None` (or absent).
    #[test]
    fn json_roundtrips_compact() {
        let original = RuntimeData::Json(serde_json::json!({"k": "v", "n": 1}));
        let bytes = rmp_serde::to_vec(&original).expect("serialize");
        let decoded: RuntimeData = rmp_serde::from_slice(&bytes).expect("deserialize");
        assert_eq!(original, decoded);
    }

    /// Documents that rmp-serde's compact (positional) encoder
    /// survives this variant when its `skip_serializing_if` optional
    /// (`metadata`) is `None`. Setting it to `Some(_)` would corrupt
    /// the round-trip; see `rmp_compact_encoder_loses_skip_serializing_optionals`.
    #[test]
    fn tensor_all_optionals_none_roundtrips_compact() {
        let original = RuntimeData::Tensor {
            data: vec![0u8; 8],
            shape: vec![2, 4],
            dtype: 0,
            metadata: None,
        };
        let bytes = rmp_serde::to_vec(&original).expect("serialize");
        let decoded: RuntimeData = rmp_serde::from_slice(&bytes).expect("deserialize");
        assert_eq!(original, decoded);
    }

    /// Documents that rmp-serde's compact (positional) encoder
    /// survives this variant when its `skip_serializing_if` optional
    /// (`segment_id`) is `None`. Setting it to `Some(_)` would corrupt
    /// the round-trip; see `rmp_compact_encoder_loses_skip_serializing_optionals`.
    #[test]
    fn control_message_all_optionals_none_roundtrips_compact() {
        let original = RuntimeData::ControlMessage {
            message_type: ControlMessageType::BatchHint {
                suggested_batch_size: 8,
            },
            segment_id: None,
            timestamp_ms: 999,
            metadata: serde_json::Value::Null,
        };
        let bytes = rmp_serde::to_vec(&original).expect("serialize");
        let decoded: RuntimeData = rmp_serde::from_slice(&bytes).expect("deserialize");
        assert_eq!(original, decoded);
    }

    /// Documents that rmp-serde's compact (positional) encoder is
    /// incompatible with `#[serde(skip_serializing_if = "Option::is_none")]`
    /// on partial-optional structs: when only some optional fields are
    /// `Some`, the skipped `None` fields shift the positional layout
    /// and the decoder reads the wrong field at the wrong slot.
    ///
    /// Remotemedia's loadable wire uses named (map) encoding via
    /// `rmp_serde::to_vec_named` to avoid this — see
    /// `crates/core/src/loadable/factory.rs` and
    /// `crates/plugin-sdk/src/adapter.rs`.
    #[test]
    fn rmp_compact_encoder_loses_skip_serializing_optionals() {
        let original = RuntimeData::Audio {
            samples: AudioSamples::Vec(vec![0.1, 0.2]),
            sample_rate: 16_000,
            channels: 1,
            stream_id: None,
            timestamp_us: Some(42),
            arrival_ts_us: None,
            metadata: None,
        };
        let bytes = rmp_serde::to_vec(&original).expect("serialize");
        assert!(
            rmp_serde::from_slice::<RuntimeData>(&bytes).is_err(),
            "rmp-serde compact encoder unexpectedly survived a partial-optional round-trip; \
             if this fires, rmp-serde's behaviour around skip_serializing_if has changed"
        );
        // Sanity: the named encoder does survive the same input, which
        // is why production wire uses `to_vec_named`.
        let named_bytes = rmp_serde::to_vec_named(&original).expect("named serialize");
        let decoded: RuntimeData = rmp_serde::from_slice(&named_bytes).expect("named deserialize");
        assert_eq!(original, decoded);
    }
}
