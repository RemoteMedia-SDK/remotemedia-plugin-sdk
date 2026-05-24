//! # remotemedia-types
//!
//! Wire-format / data-shape types shared between `remotemedia-core`,
//! the loadable-node ABI, host runtimes, and out-of-tree plugins.
//!
//! This crate is intentionally **skinny**: it links only `serde`,
//! `serde_json`, and `thiserror` (plus an optional `schemars`
//! feature for derive-based JSON-schema introspection). It must
//! NOT pull in `tokio`, `async-trait`, `ndarray`, `ort`, `iceoryx2`,
//! `pyo3`, `prost`, or any other heavyweight runtime dependency.
//! Plugins compiled against `remotemedia-types` (typically via
//! `remotemedia-traits` / `remotemedia-plugin-sdk`) can therefore
//! pick their own versions of those crates without conflicting
//! with the host.
//!
//! ## Scope
//!
//! Types in this crate describe **data on the pipeline wire**:
//! [`RuntimeData`] and its constituents (audio samples, pixel /
//! codec enums, image format, text-channel header helpers, control-
//! message type tag), plus the trait-surface error variants plugins
//! need to match on ([`Error`]).
//!
//! Execution machinery (executors, schedulers, pools, registries,
//! transports, capability resolution) stays in `remotemedia-core`.
//!
//! ## A2 design decisions
//!
//! - **`AudioSamples`** is defined here with only `Vec` and `Arc`
//!   variants. The `Pooled` variant + `AudioBufferPool` /
//!   `PooledAudioBuf` machinery stays in core; core bridges pool
//!   buffers into `AudioSamples::Arc` via `From<PooledAudioBuf>`
//!   when they cross the wire-typed boundary. That bridge is a
//!   single shrink-to-fit memcpy when `len < capacity` (the
//!   common case after partial fill — e.g. a 480-sample chunk in
//!   a 1600-cap buffer); `len == capacity` is copy-free. The
//!   trade-off is one bounded heap copy at the wire boundary in
//!   exchange for letting `RuntimeData::Audio` carry a stable
//!   `Arc<[f32]>` shape that survives FFI. The wire format is
//!   unchanged because `Pooled` already serialized as `Vec<f32>`.
//! - **Control messages**: only [`ControlMessageType`] (the type
//!   tag enum) is needed by `RuntimeData::ControlMessage` field
//!   types, so only the enum is moved here. The richer
//!   `ControlMessage` *struct* (with `uuid::Uuid`, validation,
//!   IPC byte codec) stays in core. This keeps `uuid` out of the
//!   types-crate dep set.
//! - **`PixelFormat` / `VideoCodec`**: defined here without
//!   `schemars::JsonSchema` by default. An optional `schemars`
//!   feature on this crate's `Cargo.toml` adds the derive when
//!   the host opts in (core does, plugins don't have to).
//! - **`Error`**: a slim trait-surface error with the variants
//!   plugins need (`Execution`, `InvalidData`, `InvalidInput`,
//!   `Serialization`, `Io`, `Other`). Core keeps its richer
//!   `Error` (IPC, ingestion, validation, …) and adds a
//!   `From<remotemedia_types::Error>` impl so trait-surface
//!   errors lift into core's domain.
//!
//!   Note: lifting trait-surface errors into core's richer error
//!   currently loses node_id/context. Task A7 owns the boundary
//!   fix; see crates/core/src/error.rs comment.

#![warn(clippy::all)]

pub mod validation_error;
pub use validation_error::{ValidationConstraint, ValidationError, ValidationWarning, WarningType};

use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::sync::Arc;

// =============================================================================
// AudioSamples
// =============================================================================

/// Backing storage for `RuntimeData::Audio` samples.
///
/// Only the wire-relevant variants live in `remotemedia-types`:
/// `Vec` (owned) and `Arc` (zero-copy shared). The `Pooled` variant
/// (returns to an `AudioBufferPool` on drop) is core-only — core
/// owns the pool machinery and bridges pool buffers into
/// `AudioSamples::Arc` when handing them to the pipeline.
///
/// All variants `Deref` to `&[f32]`, so read-only consumers
/// (`.len()`, `.iter()`, indexing, slicing, `.to_vec()`) work
/// unchanged regardless of storage. Only code that previously
/// consumed a `Vec<f32>` by value needs to call
/// [`AudioSamples::into_vec`] explicitly.
///
/// # Clone cost
///
/// | variant | clone |
/// |---------|-------|
/// | `Vec`    | full `Vec<f32>` copy |
/// | `Arc`    | ref-count bump (zero copy) |
pub enum AudioSamples {
    /// Owned heap vector. Historical form; `Clone` copies.
    Vec(Vec<f32>),
    /// Shared immutable slice. `Clone` bumps ref count — zero copy.
    Arc(Arc<[f32]>),
}

impl AudioSamples {
    /// Borrow the samples as a slice. Zero-copy for every variant.
    #[inline]
    pub fn as_slice(&self) -> &[f32] {
        self
    }

    /// Number of samples.
    #[inline]
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    /// Whether the sample buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    /// Consume and return a `Vec<f32>`.
    ///
    /// * `Vec` — O(1), returns the existing vector.
    /// * `Arc` — O(n) copy.
    pub fn into_vec(self) -> Vec<f32> {
        match self {
            AudioSamples::Vec(v) => v,
            AudioSamples::Arc(a) => a.to_vec(),
        }
    }

    /// Consume and return an `Arc<[f32]>`.
    ///
    /// * `Arc` — O(1), returns the existing `Arc`.
    /// * `Vec` — O(1) on modern stdlib (boxes the vec then wraps).
    pub fn into_arc(self) -> Arc<[f32]> {
        match self {
            AudioSamples::Vec(v) => Arc::from(v.into_boxed_slice()),
            AudioSamples::Arc(a) => a,
        }
    }

    /// Which variant am I? For diagnostics / tests.
    pub fn variant_name(&self) -> &'static str {
        match self {
            AudioSamples::Vec(_) => "Vec",
            AudioSamples::Arc(_) => "Arc",
        }
    }
}

impl Deref for AudioSamples {
    type Target = [f32];

    #[inline]
    fn deref(&self) -> &[f32] {
        match self {
            AudioSamples::Vec(v) => v.as_slice(),
            AudioSamples::Arc(a) => &a[..],
        }
    }
}

impl AsRef<[f32]> for AudioSamples {
    #[inline]
    fn as_ref(&self) -> &[f32] {
        self
    }
}

impl std::fmt::Debug for AudioSamples {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AudioSamples")
            .field("variant", &self.variant_name())
            .field("len", &self.len())
            .finish()
    }
}

impl Clone for AudioSamples {
    /// Clone policy:
    ///
    /// - `Vec` → full copy
    /// - `Arc` → ref-count bump (zero copy)
    fn clone(&self) -> Self {
        match self {
            AudioSamples::Vec(v) => AudioSamples::Vec(v.clone()),
            AudioSamples::Arc(a) => AudioSamples::Arc(Arc::clone(a)),
        }
    }
}

impl PartialEq for AudioSamples {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Default for AudioSamples {
    fn default() -> Self {
        AudioSamples::Vec(Vec::new())
    }
}

impl From<Vec<f32>> for AudioSamples {
    #[inline]
    fn from(v: Vec<f32>) -> Self {
        AudioSamples::Vec(v)
    }
}

impl From<Arc<[f32]>> for AudioSamples {
    #[inline]
    fn from(a: Arc<[f32]>) -> Self {
        AudioSamples::Arc(a)
    }
}

impl From<Box<[f32]>> for AudioSamples {
    #[inline]
    fn from(b: Box<[f32]>) -> Self {
        AudioSamples::Arc(Arc::from(b))
    }
}

impl From<&[f32]> for AudioSamples {
    #[inline]
    fn from(s: &[f32]) -> Self {
        AudioSamples::Vec(s.to_vec())
    }
}

// Serialize as a flat `[f32]` (bincode/JSON/msgpack-compatible with
// the old `Vec<f32>` representation). Deserialize always lands in
// the `Vec` variant — there is no wire format for `Arc`.
impl Serialize for AudioSamples {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        self.as_slice().serialize(s)
    }
}

impl<'de> Deserialize<'de> for AudioSamples {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Vec::<f32>::deserialize(d).map(AudioSamples::Vec)
    }
}

// =============================================================================
// Video pixel format / codec
// =============================================================================

/// Pixel format for video frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[repr(u8)]
pub enum PixelFormat {
    /// Unknown/unspecified format
    Unspecified = 0,

    /// YUV 4:2:0 planar (standard codec format)
    /// Layout: Y plane (width*height), U plane (width/2 * height/2), V plane (width/2 * height/2)
    /// Memory: width * height * 3/2 bytes
    /// Use case: Codec input/output, efficient storage
    Yuv420p = 1,

    /// I420 (identical to YUV420P, alternate name for WebRTC compat)
    I420 = 2,

    /// NV12 (semi-planar, Y plane + interleaved UV)
    /// Layout: Y plane (width*height), UV plane (width * height/2)
    /// Memory: width * height * 3/2 bytes
    /// Use case: Hardware acceleration (common GPU format)
    NV12 = 3,

    /// RGB24 (packed 24-bit RGB)
    /// Layout: Packed RGBRGBRGB... (no padding)
    /// Memory: width * height * 3 bytes
    /// Use case: Image processing, display, Python interop (PIL/OpenCV)
    Rgb24 = 4,

    /// RGBA32 (packed 32-bit RGBA with alpha)
    /// Layout: Packed RGBARGBARGBA... (4-byte aligned)
    /// Memory: width * height * 4 bytes
    /// Use case: Compositing, transparency, browser rendering
    Rgba32 = 5,

    /// Encoded bitstream (not raw pixels)
    /// Used when pixel_data contains codec-specific compressed data
    Encoded = 255,
}

impl PixelFormat {
    /// Calculate expected buffer size in bytes
    pub fn buffer_size(&self, width: u32, height: u32) -> usize {
        match self {
            PixelFormat::Yuv420p | PixelFormat::I420 | PixelFormat::NV12 => {
                (width * height * 3 / 2) as usize
            }
            PixelFormat::Rgb24 => (width * height * 3) as usize,
            PixelFormat::Rgba32 => (width * height * 4) as usize,
            PixelFormat::Encoded | PixelFormat::Unspecified => 0, // Variable or unknown
        }
    }

    /// Check if format requires even dimensions (YUV formats)
    pub fn requires_even_dimensions(&self) -> bool {
        matches!(
            self,
            PixelFormat::Yuv420p | PixelFormat::I420 | PixelFormat::NV12
        )
    }
}

/// Video codec type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[repr(u8)]
pub enum VideoCodec {
    /// VP8 (WebM, WebRTC standard)
    /// RFC 6386, royalty-free
    Vp8 = 1,

    /// H.264/AVC (Baseline/Main/High profile)
    /// ITU-T H.264, widely supported
    H264 = 2,

    /// AV1 (next-gen royalty-free codec)
    /// Higher compression than VP8, lower than HEVC
    Av1 = 3,
}

impl VideoCodec {
    /// MIME type for WebRTC/gRPC
    pub fn mime_type(&self) -> &'static str {
        match self {
            VideoCodec::Vp8 => "video/VP8",
            VideoCodec::H264 => "video/H264",
            VideoCodec::Av1 => "video/AV1",
        }
    }

    /// RTP payload type (WebRTC)
    pub fn rtp_payload_type(&self) -> u8 {
        match self {
            VideoCodec::Vp8 => 96, // Dynamic payload type
            VideoCodec::H264 => 102,
            VideoCodec::Av1 => 104,
        }
    }
}

// =============================================================================
// Audio / image / data type tag enums
// =============================================================================

/// Audio format enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AudioFormat {
    /// Unknown format
    Unspecified = 0,
    /// 32-bit float (little-endian)
    F32 = 1,
    /// 16-bit signed integer (little-endian)
    S16 = 2,
}

/// Still-image format identifier.
///
/// `Jpeg`/`Png`/`WebP` indicate container-encoded bytes that can be
/// embedded directly in a `data:image/...;base64,...` URL.
/// `Raw` carries uncompressed pixels — the caller must transcode
/// before sending to a vision LLM.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ImageFormat {
    Jpeg,
    Png,
    WebP,
    Raw { pixel_format: PixelFormat },
}

impl ImageFormat {
    /// IANA mime type for the encoded variants. Returns `None` for
    /// `Raw` because raw pixels have no canonical mime.
    pub fn mime_type(&self) -> Option<&'static str> {
        match self {
            ImageFormat::Jpeg => Some("image/jpeg"),
            ImageFormat::Png => Some("image/png"),
            ImageFormat::WebP => Some("image/webp"),
            ImageFormat::Raw { .. } => None,
        }
    }
}

/// Data type hint for routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataTypeHint {
    /// Unspecified
    Unspecified = 0,
    /// Audio
    Audio = 1,
    /// Video
    Video = 2,
    /// Tensor
    Tensor = 3,
    /// JSON
    Json = 4,
    /// Text
    Text = 5,
    /// Binary
    Binary = 6,
    /// Any type
    Any = 7,
    /// File reference (spec 001)
    File = 8,
}

// =============================================================================
// Control message type tag
// =============================================================================
//
// Only the *type tag* enum lives in `types`. The richer
// `ControlMessage` struct (uuid, IPC byte codec, validation) stays
// in core because it pulls `uuid` and is not part of the wire
// `RuntimeData::ControlMessage` field shape.

/// Type of control message carried in `RuntimeData::ControlMessage`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum ControlMessageType {
    /// Cancel a speculative segment
    CancelSpeculation {
        from_timestamp: u64,
        to_timestamp: u64,
    },

    /// Hint to batch more aggressively
    BatchHint { suggested_batch_size: usize },

    /// Soft deadline approaching
    DeadlineWarning {
        deadline_us: u64, // Microseconds from now
    },
}

// =============================================================================
// Text-channel routing helpers
// =============================================================================

/// Default channel name applied when a text payload carries no
/// channel header.
pub const TEXT_CHANNEL_DEFAULT: &str = "tts";

/// Parse a possibly-tagged text string into `(channel, content)`.
///
/// Layout:
/// - `[0x00][channel_len:u8][channel:utf8][text:utf8]` → `(channel, text)`
/// - `<anything else>` → `("tts", input)` (legacy untagged path)
pub fn split_text_str(s: &str) -> (&str, &str) {
    let bytes = s.as_bytes();
    if bytes.len() >= 2 && bytes[0] == 0x00 {
        let channel_len = bytes[1] as usize;
        if 2 + channel_len <= bytes.len() {
            if let Ok(channel) = std::str::from_utf8(&bytes[2..2 + channel_len]) {
                if !channel.is_empty() {
                    // The remaining bytes are the tail of the original
                    // UTF-8 string, so they're still valid UTF-8.
                    if let Ok(content) = std::str::from_utf8(&bytes[2 + channel_len..]) {
                        return (channel, content);
                    }
                }
            }
        }
    }
    (TEXT_CHANNEL_DEFAULT, s)
}

/// Build a tagged text string from a `(channel, text)` pair.
/// `"tts"` / empty channel returns the text unchanged (legacy
/// format).
pub fn tag_text_str(text: &str, channel: &str) -> String {
    if channel.is_empty() || channel == TEXT_CHANNEL_DEFAULT {
        return text.to_string();
    }
    let mut channel_bytes = channel.as_bytes();
    if channel_bytes.len() > u8::MAX as usize {
        channel_bytes = &channel_bytes[..u8::MAX as usize];
    }
    let mut out = Vec::with_capacity(2 + channel_bytes.len() + text.len());
    out.push(0x00);
    out.push(channel_bytes.len() as u8);
    out.extend_from_slice(channel_bytes);
    out.extend_from_slice(text.as_bytes());
    // All parts are valid UTF-8 (U+0000 + ASCII channel + UTF-8 text).
    String::from_utf8(out).expect("tagged text payload is valid UTF-8")
}

// =============================================================================
// RuntimeData
// =============================================================================

/// Runtime data representation matching the DataBuffer wire schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RuntimeData {
    /// Audio samples (f32 PCM)
    Audio {
        /// Audio samples.
        ///
        /// Backed by [`AudioSamples`], which can hold a `Vec<f32>`
        /// or an `Arc<[f32]>` (zero-copy shared). Both variants
        /// `Deref` to `&[f32]` so read sites work unchanged.
        samples: AudioSamples,
        /// Sample rate in Hz
        sample_rate: u32,
        /// Number of channels (1=mono, 2=stereo)
        channels: u32,
        /// Optional stream identifier for multi-track routing (spec 013)
        /// When None, uses default track for backward compatibility
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stream_id: Option<String>,
        /// Media timestamp in microseconds (spec 026)
        /// Represents the presentation timestamp of this audio chunk
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timestamp_us: Option<u64>,
        /// Arrival timestamp in microseconds (spec 026)
        /// Set by transport ingest layer for drift monitoring
        #[serde(default, skip_serializing_if = "Option::is_none")]
        arrival_ts_us: Option<u64>,
        /// Optional extensible metadata (e.g., speaker diarization, confidence scores)
        /// Flows through pipeline with the audio data. Nodes that don't use it ignore it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<serde_json::Value>,
    },
    /// Video frame
    Video {
        /// Pixel data (raw or encoded)
        /// - Raw: Depends on PixelFormat (e.g., YUV420P planar, RGB24 packed)
        /// - Encoded: Codec bitstream (VP8/AV1/H.264)
        pixel_data: Vec<u8>,
        /// Frame width in pixels
        width: u32,
        /// Frame height in pixels
        height: u32,
        /// Pixel format (Yuv420p, RGB24, etc., or Encoded for compressed)
        format: PixelFormat,
        /// Codec used (None for raw frames, Some(codec) for encoded)
        codec: Option<VideoCodec>,
        /// Sequential frame number (monotonic counter)
        frame_number: u64,
        /// Presentation timestamp in microseconds
        timestamp_us: u64,
        /// Keyframe indicator (true for I-frames, false for P/B-frames)
        is_keyframe: bool,
        /// Optional stream identifier for multi-track routing (spec 013)
        /// When None, uses default track for backward compatibility
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stream_id: Option<String>,
        /// Arrival timestamp in microseconds (spec 026)
        /// Set by transport ingest layer for drift monitoring
        #[serde(default, skip_serializing_if = "Option::is_none")]
        arrival_ts_us: Option<u64>,
    },
    /// Still-frame image (encoded JPEG/PNG/WebP, or raw RGBA).
    ///
    /// Distinct from [`RuntimeData::Video`] because image-shaped
    /// inputs to vision LLMs (gpt-4o, llama.cpp + llava, Claude
    /// Vision) are *encoded* container formats — JPEG/PNG/WebP —
    /// and not codec bitstreams (VP8/AV1/H.264). Carrying them
    /// inside `Video` would force every consumer to inspect
    /// pixel data to figure out what shape it's in. A separate
    /// variant gives clean type-level routing for vision
    /// pipelines.
    Image {
        /// Encoded image bytes (JPEG/PNG/WebP) or raw pixels.
        data: Vec<u8>,
        /// Image format. `Raw { pixel_format }` carries
        /// uncompressed pixels; the other variants carry
        /// container-encoded bytes ready for direct embedding
        /// (e.g. base64 in a `data:image/png;base64,…` URL).
        format: ImageFormat,
        /// Frame width in pixels.
        width: u32,
        /// Frame height in pixels.
        height: u32,
        /// Optional presentation timestamp in microseconds.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timestamp_us: Option<u64>,
        /// Optional stream identifier for multi-track routing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stream_id: Option<String>,
        /// Optional extensible metadata.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<serde_json::Value>,
    },
    /// Tensor data
    Tensor {
        /// Flattened tensor data
        data: Vec<u8>,
        /// Tensor shape
        shape: Vec<i32>,
        /// Data type (0=float32, 1=int32, etc.)
        dtype: i32,
        /// Optional extensible metadata (e.g., emotion labels, layer info, model ID).
        /// Flows through pipeline with the tensor data. Nodes that don't use it ignore it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        metadata: Option<serde_json::Value>,
    },
    /// Numpy array data (zero-copy passthrough until IPC boundary)
    /// This allows numpy arrays to flow through the pipeline without conversion,
    /// only serializing once at the IPC boundary for iceoryx2 transport.
    Numpy {
        /// Raw array data
        data: Vec<u8>,
        /// Array shape (dimensions)
        shape: Vec<usize>,
        /// Data type string (e.g., "float32", "int16", "uint8")
        dtype: String,
        /// Array strides (bytes to step in each dimension)
        strides: Vec<isize>,
        /// Whether array is C-contiguous
        c_contiguous: bool,
        /// Whether array is Fortran-contiguous
        f_contiguous: bool,
    },
    /// JSON data
    Json(serde_json::Value),
    /// Text data
    Text(String),
    /// Binary data
    Binary(Vec<u8>),
    /// Control message for pipeline flow control (spec 007)
    ControlMessage {
        /// Type of control message
        message_type: ControlMessageType,
        /// Optional target segment ID for cancellation
        segment_id: Option<String>,
        /// Timestamp when message was created (milliseconds)
        timestamp_ms: u64,
        /// Extensible metadata
        metadata: serde_json::Value,
    },
    /// File reference with metadata and byte range support (spec 001)
    ///
    /// Represents a reference to a file on the local filesystem.
    /// Does NOT contain file contents - only metadata for referencing.
    File {
        /// File path (absolute or relative, UTF-8)
        path: String,

        /// Original filename (optional, preserved separately from path)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,

        /// MIME type hint (optional)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,

        /// File size in bytes (optional, None = unknown)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        size: Option<u64>,

        /// Byte offset for range read/write (optional)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        offset: Option<u64>,

        /// Number of bytes for range request (optional, None = to EOF)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        length: Option<u64>,

        /// Stream identifier for multi-track routing
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stream_id: Option<String>,
    },
}

impl RuntimeData {
    /// Get the type of this data as string.
    pub fn data_type(&self) -> &str {
        match self {
            RuntimeData::Audio { .. } => "audio",
            RuntimeData::Video { .. } => "video",
            RuntimeData::Image { .. } => "image",
            RuntimeData::Tensor { .. } => "tensor",
            RuntimeData::Numpy { .. } => "numpy",
            RuntimeData::Json(_) => "json",
            RuntimeData::Text(_) => "text",
            RuntimeData::Binary(_) => "binary",
            RuntimeData::ControlMessage { .. } => "control_message",
            RuntimeData::File { .. } => "file",
        }
    }

    /// Get timing information for drift monitoring (spec 026).
    ///
    /// Returns `(media_timestamp_us, arrival_timestamp_us)` for
    /// Audio and Video variants.
    pub fn timing(&self) -> (Option<u64>, Option<u64>) {
        match self {
            RuntimeData::Audio {
                timestamp_us,
                arrival_ts_us,
                ..
            } => (*timestamp_us, *arrival_ts_us),
            RuntimeData::Video {
                timestamp_us,
                arrival_ts_us,
                ..
            } => (Some(*timestamp_us), *arrival_ts_us),
            _ => (None, None),
        }
    }

    /// Get stream identifier if present (spec 026).
    pub fn stream_id(&self) -> Option<&str> {
        match self {
            RuntimeData::Audio { stream_id, .. } => stream_id.as_deref(),
            RuntimeData::Video { stream_id, .. } => stream_id.as_deref(),
            RuntimeData::Image { stream_id, .. } => stream_id.as_deref(),
            RuntimeData::File { stream_id, .. } => stream_id.as_deref(),
            _ => None,
        }
    }

    /// Check if this is audio data (spec 026).
    pub fn is_audio(&self) -> bool {
        matches!(self, RuntimeData::Audio { .. })
    }

    /// Check if this is video data (spec 026).
    pub fn is_video(&self) -> bool {
        matches!(self, RuntimeData::Video { .. })
    }

    /// Check if this is still-image data.
    pub fn is_image(&self) -> bool {
        matches!(self, RuntimeData::Image { .. })
    }

    /// Check if this is a timed media type (audio or video).
    pub fn is_timed_media(&self) -> bool {
        self.is_audio() || self.is_video()
    }

    /// Set arrival timestamp for drift monitoring (spec 026).
    ///
    /// Should be called by transport ingest layer when data
    /// arrives. Only affects Audio and Video variants.
    ///
    /// # Returns
    /// `true` if timestamp was set, `false` if not applicable to
    /// this variant.
    pub fn set_arrival_timestamp(&mut self, arrival_us: u64) -> bool {
        match self {
            RuntimeData::Audio { arrival_ts_us, .. } => {
                *arrival_ts_us = Some(arrival_us);
                true
            }
            RuntimeData::Video { arrival_ts_us, .. } => {
                *arrival_ts_us = Some(arrival_us);
                true
            }
            _ => false,
        }
    }

    /// Set media timestamp for audio (spec 026).
    pub fn set_audio_timestamp(&mut self, media_ts_us: u64) -> bool {
        match self {
            RuntimeData::Audio { timestamp_us, .. } => {
                *timestamp_us = Some(media_ts_us);
                true
            }
            _ => false,
        }
    }

    /// Get item count.
    pub fn item_count(&self) -> usize {
        match self {
            RuntimeData::Audio { samples, .. } => samples.len(),
            RuntimeData::Video { .. } => 1,
            RuntimeData::Image { .. } => 1,
            RuntimeData::Tensor { data, .. } => data.len(),
            RuntimeData::Numpy { shape, .. } => shape.iter().product(),
            RuntimeData::Json(value) => match value {
                serde_json::Value::Array(arr) => arr.len(),
                serde_json::Value::Object(obj) => obj.len(),
                _ => 1,
            },
            RuntimeData::Text(s) => s.len(),
            RuntimeData::Binary(b) => b.len(),
            RuntimeData::ControlMessage { .. } => 1,
            RuntimeData::File { .. } => 1, // One file reference
        }
    }

    /// Get memory size in bytes.
    pub fn size_bytes(&self) -> usize {
        match self {
            RuntimeData::Audio { samples, .. } => samples.len() * 4,
            RuntimeData::Video { pixel_data, .. } => pixel_data.len(),
            RuntimeData::Image { data, .. } => data.len(),
            RuntimeData::Tensor { data, .. } => data.len(),
            RuntimeData::Numpy {
                data,
                shape,
                dtype,
                strides,
                ..
            } => {
                // Size includes data + metadata overhead
                let data_size = data.len();
                let metadata_size = shape.len() * 8 + strides.len() * 8 + dtype.len() + 10;
                data_size + metadata_size
            }
            RuntimeData::Json(value) => serde_json::to_string(value).map(|s| s.len()).unwrap_or(0),
            RuntimeData::Text(s) => s.len(),
            RuntimeData::Binary(b) => b.len(),
            RuntimeData::ControlMessage {
                segment_id,
                metadata,
                ..
            } => {
                // Approximate size: type + timestamp + segment_id + metadata
                let segment_id_size = segment_id.as_ref().map(|s| s.len()).unwrap_or(0);
                let metadata_size = serde_json::to_string(metadata)
                    .map(|s| s.len())
                    .unwrap_or(0);
                std::mem::size_of::<ControlMessageType>() + 8 + segment_id_size + metadata_size
            }
            RuntimeData::File {
                path,
                filename,
                mime_type,
                stream_id,
                ..
            } => {
                // Approximate memory footprint of the reference (not file contents)
                path.len()
                    + filename.as_ref().map(|s| s.len()).unwrap_or(0)
                    + mime_type.as_ref().map(|s| s.len()).unwrap_or(0)
                    + stream_id.as_ref().map(|s| s.len()).unwrap_or(0)
                    + 24 // 3 u64 fields (size, offset, length)
            }
        }
    }

    /// Validate video frame structure (spec 012).
    ///
    /// Checks that video frame dimensions and buffer sizes are
    /// consistent.
    pub fn validate_video_frame(&self) -> std::result::Result<(), String> {
        match self {
            RuntimeData::Video {
                width,
                height,
                format,
                pixel_data,
                ..
            } => {
                // Check dimensions
                if *width == 0 || *height == 0 {
                    return Err("Invalid dimensions: width and height must be > 0".to_string());
                }

                // Check even dimensions for YUV formats
                if format.requires_even_dimensions() && (*width % 2 != 0 || *height % 2 != 0) {
                    return Err("YUV formats require even dimensions".to_string());
                }

                // Check buffer size (skip for encoded frames with variable size)
                let expected_size = format.buffer_size(*width, *height);
                if expected_size > 0 && pixel_data.len() != expected_size {
                    return Err(format!(
                        "Buffer size mismatch: expected {}, got {}",
                        expected_size,
                        pixel_data.len()
                    ));
                }

                Ok(())
            }
            _ => Err("Not a video frame".to_string()),
        }
    }
}

// =============================================================================
// Standalone shape structs (used by node code that wants a specific
// frame type rather than the wire enum)
// =============================================================================

/// Audio buffer (standalone struct for nodes).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AudioBuffer {
    /// Raw audio samples (bytes for f32)
    pub samples: Vec<u8>,
    /// Sample rate in Hz
    pub sample_rate: u32,
    /// Number of channels
    pub channels: u32,
    /// Audio format
    pub format: i32,
    /// Number of samples
    pub num_samples: u64,
}

/// Video frame (standalone struct for nodes).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VideoFrame {
    /// Pixel data (raw or encoded)
    pub pixel_data: Vec<u8>,
    /// Frame width in pixels
    pub width: u32,
    /// Frame height in pixels
    pub height: u32,
    /// Pixel format
    pub format: PixelFormat,
    /// Codec used (None for raw frames)
    pub codec: Option<VideoCodec>,
    /// Sequential frame number
    pub frame_number: u64,
    /// Presentation timestamp in microseconds
    pub timestamp_us: u64,
    /// Keyframe indicator
    pub is_keyframe: bool,
}

/// Tensor buffer (standalone struct for nodes).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TensorBuffer {
    /// Tensor data (bytes)
    pub data: Vec<u8>,
    /// Tensor shape
    pub shape: Vec<i32>,
    /// Data type
    pub dtype: i32,
}

// =============================================================================
// Trait-surface error (full domain)
// =============================================================================
//
// `remotemedia_types::Error` carries every variant the host's pipeline
// machinery raises — execution, IPC, ingestion, manifest, transport,
// remote-execution, capability validation. Plugin code that only needs
// to construct/match on the lighter variants (`Execution`,
// `InvalidData`, `InvalidInput`, `Serialization`, `Io`, `Other`) just
// uses those; the full enum simply ensures plugins and the host speak
// the same error language without core having to re-define a parallel
// type.
//
// Heavy upstream From impls (`From<anyhow::Error>`,
// `From<ort::Error>`) stay in `remotemedia-core` because the source
// crates aren't deps of this crate.

/// Result alias using the trait-surface [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// Error types raised by the runtime + nodes. Shared by host and
/// plugin code so both sides match on the same enum.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Manifest parsing or validation error
    #[error("Manifest error: {0}")]
    Manifest(String),

    /// Manifest parsing or validation error (alias)
    #[error("Invalid manifest: {0}")]
    InvalidManifest(String),

    /// Pipeline execution error
    #[error("Execution error: {0}")]
    Execution(String),

    /// Invalid input data (type mismatch, validation failure)
    #[error("Invalid data: {0}")]
    InvalidData(String),

    /// IPC communication error
    #[error("IPC error: {0}")]
    IpcError(String),

    /// Transport error (for compatibility)
    #[error("Transport error: {0}")]
    Transport(String),

    /// WASM error (for compatibility, not used in core)
    #[error("WASM error: {0}")]
    Wasm(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Invalid input (node-specific)
    #[error("Invalid input: {message}")]
    InvalidInput {
        /// Error message
        message: String,
        /// Node that rejected the input
        node_id: String,
        /// Additional context
        context: String,
    },

    /// Remote pipeline execution error
    #[error("Remote execution failed: {0}")]
    RemoteExecutionFailed(String),

    /// Remote execution timeout
    #[error("Remote execution timeout after {timeout_ms}ms: {context}")]
    RemoteTimeout {
        /// Timeout duration in milliseconds
        timeout_ms: u64,
        /// Additional context
        context: String,
    },

    /// Circuit breaker is open (too many failures)
    #[error("Circuit breaker open for endpoint {endpoint}: {reason}")]
    CircuitBreakerOpen {
        /// Endpoint URL
        endpoint: String,
        /// Reason for circuit breaker activation
        reason: String,
    },

    /// All configured endpoints failed
    #[error("All {count} endpoints failed: {details}")]
    AllEndpointsFailed {
        /// Number of endpoints that failed
        count: usize,
        /// Failure details
        details: String,
    },

    /// Failed to fetch remote manifest
    #[error("Manifest fetch failed from {url}: {reason}")]
    ManifestFetchFailed {
        /// Manifest URL
        url: String,
        /// Failure reason
        reason: String,
    },

    /// Circular dependency detected in remote pipeline references
    #[error("Circular dependency detected: {reason}\nDependency chain: {}", chain.join(" -> "))]
    CircularDependency {
        /// Chain of manifest names/identifiers showing the cycle
        chain: Vec<String>,
        /// Description of the circular dependency
        reason: String,
    },

    /// Generic error
    #[error("{0}")]
    Other(String),

    /// Node parameter validation failed
    #[error("Parameter validation failed: {} error(s)", .0.len())]
    Validation(Vec<ValidationError>),

    // =========================================================================
    // Ingestion errors (spec 028)
    // =========================================================================
    /// File not found for ingestion
    #[error("Ingest file not found: {path}")]
    IngestFileNotFound {
        /// Path that was not found
        path: String,
    },

    /// Invalid URI scheme for ingestion
    #[error("Invalid ingest scheme: {scheme}. Expected one of: {expected:?}")]
    IngestInvalidScheme {
        /// The invalid scheme
        scheme: String,
        /// Expected schemes
        expected: Vec<String>,
    },

    /// Unsupported URI scheme for ingestion (no plugin registered)
    #[error("Unsupported ingest scheme: {scheme}. Available: {available:?}")]
    IngestUnsupportedScheme {
        /// The unsupported scheme
        scheme: String,
        /// Available schemes
        available: Vec<String>,
    },

    /// Media decode error during ingestion
    #[error("Ingest decode error: {message}")]
    IngestDecodeError {
        /// Error message
        message: String,
        /// Optional codec name
        codec: Option<String>,
    },

    /// Connection error during ingestion
    #[error("Ingest connection error: {message}")]
    IngestConnectionError {
        /// Error message
        message: String,
        /// URL that failed to connect
        url: String,
    },

    /// Plugin already registered in ingest registry
    #[error("Ingest plugin already registered: {name}")]
    IngestPluginAlreadyRegistered {
        /// Plugin name
        name: String,
    },

    /// Plugin not found in ingest registry
    #[error("Ingest plugin not found: {name}")]
    IngestPluginNotFound {
        /// Plugin name
        name: String,
    },

    /// Lock error in ingest registry
    #[error("Ingest registry lock error: {message}")]
    IngestLockError {
        /// Error message
        message: String,
    },
}

impl Error {
    /// Wrap a plugin-emitted error string with the host-side context
    /// (`node_id` + `node_type`) so structured logging can attribute the
    /// failure. Used by `crates/core/src/loadable/factory.rs` when
    /// converting `RErr(RString)` from a `dlopen`'d plugin into the
    /// host's structured `Error`.
    pub fn from_plugin(plugin_err: impl Into<String>, node_id: &str, node_type: &str) -> Self {
        Error::Execution(format!(
            "plugin '{node_type}' (id={node_id}): {}",
            plugin_err.into()
        ))
    }
}

#[cfg(test)]
mod error_tests {
    use super::*;

    #[test]
    fn from_plugin_includes_node_id_and_type() {
        let err = Error::from_plugin("inference failed", "vad-1", "SileroVAD");
        let msg = err.to_string();
        assert!(msg.contains("vad-1"), "expected node_id 'vad-1' in {msg:?}");
        assert!(
            msg.contains("SileroVAD"),
            "expected node_type 'SileroVAD' in {msg:?}"
        );
        assert!(
            msg.contains("inference failed"),
            "expected plugin error in {msg:?}"
        );
    }
}
