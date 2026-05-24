//! Plugin-author capability surface.
//!
//! These types form the public surface of `MediaCapabilities` and the
//! associated constraint primitives — what nodes/factories declare via
//! the `media_capabilities()` / `capability_behavior()` trait methods.
//!
//! Resolver / negotiation / validation logic lives in
//! `remotemedia_core::capabilities` (host-side machinery).
//!
//! # PixelFormat naming
//!
//! The `PixelFormat` enum here is the *capability-side* pixel format
//! (variant set: RGB24/RGBA/BGR24/BGRA/YUV420/YUV422/NV12/NV21). It is
//! distinct from `remotemedia_types::PixelFormat`, which is the
//! wire-format pixel format for `RuntimeData::Video` payloads. To avoid
//! a name clash when a consumer imports both, this enum is also
//! re-exported as `CapabilityPixelFormat`.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// =============================================================================
// Core Constraint Value Type
// =============================================================================

/// Generic constraint expression supporting exact values, ranges, sets, or "any".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConstraintValue<T> {
    /// Single exact value required
    Exact(T),
    /// Inclusive range of acceptable values
    Range {
        /// Minimum value (inclusive)
        min: T,
        /// Maximum value (inclusive)
        max: T,
    },
    /// List of discrete acceptable values
    Set(Vec<T>),
}

impl<T: PartialOrd + PartialEq> ConstraintValue<T> {
    /// Check if a value satisfies this constraint.
    pub fn satisfies(&self, value: &T) -> bool {
        match self {
            ConstraintValue::Exact(exact) => value == exact,
            ConstraintValue::Range { min, max } => value >= min && value <= max,
            ConstraintValue::Set(set) => set.iter().any(|v| v == value),
        }
    }

    /// Check if this constraint is flexible (Range or Set).
    pub fn is_flexible(&self) -> bool {
        matches!(
            self,
            ConstraintValue::Range { .. } | ConstraintValue::Set(_)
        )
    }

    /// Check if two constraints are compatible (for Ord types).
    pub fn compatible_with(&self, other: &ConstraintValue<T>) -> bool
    where
        T: Clone + Ord,
    {
        self.compatible_with_partial(other)
    }

    /// Check if two constraints are compatible (for PartialOrd types like f32).
    pub fn compatible_with_partial(&self, other: &ConstraintValue<T>) -> bool
    where
        T: Clone,
    {
        match (self, other) {
            (ConstraintValue::Exact(a), ConstraintValue::Exact(b)) => a == b,
            (ConstraintValue::Exact(a), ConstraintValue::Range { min, max })
            | (ConstraintValue::Range { min, max }, ConstraintValue::Exact(a)) => {
                a >= min && a <= max
            }
            (ConstraintValue::Exact(a), ConstraintValue::Set(set))
            | (ConstraintValue::Set(set), ConstraintValue::Exact(a)) => set.contains(a),
            (
                ConstraintValue::Range {
                    min: min1,
                    max: max1,
                },
                ConstraintValue::Range {
                    min: min2,
                    max: max2,
                },
            ) => min1 <= max2 && min2 <= max1,
            (ConstraintValue::Range { min, max }, ConstraintValue::Set(set))
            | (ConstraintValue::Set(set), ConstraintValue::Range { min, max }) => {
                set.iter().any(|v| v >= min && v <= max)
            }
            (ConstraintValue::Set(set1), ConstraintValue::Set(set2)) => {
                set1.iter().any(|v| set2.contains(v))
            }
        }
    }
}

// =============================================================================
// Format Enums
// =============================================================================

/// Audio sample format enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum AudioSampleFormat {
    /// 32-bit floating point [-1.0, 1.0]
    F32,
    /// 16-bit signed integer [-32768, 32767]
    I16,
    /// 32-bit signed integer
    I32,
    /// 8-bit unsigned integer [0, 255]
    U8,
}

/// Video pixel format enumeration (capability-side).
///
/// Distinct from `remotemedia_types::PixelFormat` (wire-format). When
/// importing both into the same scope, prefer the `CapabilityPixelFormat`
/// re-export to avoid name collision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "UPPERCASE")]
pub enum PixelFormat {
    /// 24-bit RGB (8 bits per channel, packed)
    RGB24,
    /// 32-bit RGBA (8 bits per channel, packed)
    RGBA,
    /// 24-bit BGR (8 bits per channel, packed)
    BGR24,
    /// 32-bit BGRA (8 bits per channel, packed)
    BGRA,
    /// YUV 4:2:0 planar
    YUV420,
    /// YUV 4:2:2 planar
    YUV422,
    /// NV12 (Y plane + interleaved UV)
    NV12,
    /// NV21 (Y plane + interleaved VU)
    NV21,
}

/// Disambiguating alias for [`PixelFormat`] — use this when importing
/// both the capability and wire pixel format types in one scope.
pub type CapabilityPixelFormat = PixelFormat;

/// Tensor element data type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum TensorDataType {
    /// 32-bit floating point
    Float32,
    /// 64-bit floating point
    Float64,
    /// 32-bit signed integer
    Int32,
    /// 64-bit signed integer
    Int64,
    /// 8-bit unsigned integer
    Uint8,
    /// Boolean
    Bool,
}

// =============================================================================
// Media Type Constraints
// =============================================================================

/// Audio format constraints.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AudioConstraints {
    /// Sample rate constraint in Hz. `None` = any sample rate accepted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<ConstraintValue<u32>>,

    /// Channel count constraint. `None` = any channel count accepted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channels: Option<ConstraintValue<u32>>,

    /// Sample format constraint. `None` = any format accepted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<ConstraintValue<AudioSampleFormat>>,
}

/// Video format constraints.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct VideoConstraints {
    /// Frame width constraint in pixels. `None` = any width accepted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<ConstraintValue<u32>>,

    /// Frame height constraint in pixels. `None` = any height accepted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<ConstraintValue<u32>>,

    /// Framerate constraint in frames per second. `None` = any framerate accepted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub framerate: Option<ConstraintValue<f32>>,

    /// Pixel format constraint. `None` = any format accepted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pixel_format: Option<ConstraintValue<PixelFormat>>,
}

/// Tensor/Numpy data constraints.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TensorConstraints {
    /// Shape constraint. Inner `None` values indicate dynamic dimensions.
    /// Outer `None` = any shape accepted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shape: Option<ConstraintValue<Vec<Option<usize>>>>,

    /// Data type constraint. `None` = any dtype accepted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dtype: Option<ConstraintValue<TensorDataType>>,
}

/// Text data constraints.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TextConstraints {
    /// Character encoding constraint (e.g., "UTF-8", "ASCII").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<ConstraintValue<String>>,

    /// Text format constraint (e.g., "plain", "markdown", "json").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<ConstraintValue<String>>,
}

/// File data constraints.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FileConstraints {
    /// Accepted file extensions (without leading dot).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Option<ConstraintValue<Vec<String>>>,

    /// Accepted MIME types (e.g., "video/mp4", "audio/*").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_types: Option<ConstraintValue<Vec<String>>>,
}

/// JSON data constraints.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct JsonConstraints {
    /// JSON Schema for structure validation. `None` = any JSON accepted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
}

// =============================================================================
// Media Constraints Union
// =============================================================================

/// Union type for constraints on different media types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum MediaConstraints {
    /// Audio data constraints
    Audio(AudioConstraints),
    /// Video data constraints
    Video(VideoConstraints),
    /// Tensor/Numpy data constraints
    Tensor(TensorConstraints),
    /// Text data constraints
    Text(TextConstraints),
    /// File reference constraints
    File(FileConstraints),
    /// JSON data constraints
    Json(JsonConstraints),
    /// Binary data (no constraints applicable)
    Binary,
}

impl MediaConstraints {
    /// Get the media type name as a string.
    pub fn media_type(&self) -> &'static str {
        match self {
            MediaConstraints::Audio(_) => "audio",
            MediaConstraints::Video(_) => "video",
            MediaConstraints::Tensor(_) => "tensor",
            MediaConstraints::Text(_) => "text",
            MediaConstraints::File(_) => "file",
            MediaConstraints::Json(_) => "json",
            MediaConstraints::Binary => "binary",
        }
    }

    /// Check if this constraint is flexible (has Range or Set constraints).
    pub fn is_flexible(&self) -> bool {
        match self {
            MediaConstraints::Audio(c) => {
                c.sample_rate
                    .as_ref()
                    .map(|v| v.is_flexible())
                    .unwrap_or(false)
                    || c.channels
                        .as_ref()
                        .map(|v| v.is_flexible())
                        .unwrap_or(false)
                    || c.format.as_ref().map(|v| v.is_flexible()).unwrap_or(false)
            }
            MediaConstraints::Video(c) => {
                c.width.as_ref().map(|v| v.is_flexible()).unwrap_or(false)
                    || c.height.as_ref().map(|v| v.is_flexible()).unwrap_or(false)
                    || c.framerate
                        .as_ref()
                        .map(|v| v.is_flexible())
                        .unwrap_or(false)
                    || c.pixel_format
                        .as_ref()
                        .map(|v| v.is_flexible())
                        .unwrap_or(false)
            }
            MediaConstraints::Tensor(c) => {
                c.shape.as_ref().map(|v| v.is_flexible()).unwrap_or(false)
                    || c.dtype.as_ref().map(|v| v.is_flexible()).unwrap_or(false)
            }
            MediaConstraints::Text(c) => {
                c.encoding
                    .as_ref()
                    .map(|v| v.is_flexible())
                    .unwrap_or(false)
                    || c.format.as_ref().map(|v| v.is_flexible()).unwrap_or(false)
            }
            MediaConstraints::File(c) => {
                c.extensions
                    .as_ref()
                    .map(|v| v.is_flexible())
                    .unwrap_or(false)
                    || c.mime_types
                        .as_ref()
                        .map(|v| v.is_flexible())
                        .unwrap_or(false)
            }
            MediaConstraints::Json(_) => false,
            MediaConstraints::Binary => false,
        }
    }
}

// =============================================================================
// Node Media Capabilities
// =============================================================================

/// Complete capability declaration for a node.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MediaCapabilities {
    /// Input port requirements. Key = port name, Value = constraints.
    /// Empty map means "accept any input".
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub inputs: HashMap<String, MediaConstraints>,

    /// Output port capabilities. Key = port name, Value = constraints.
    /// Empty map means "output format is unspecified/passthrough".
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub outputs: HashMap<String, MediaConstraints>,
}

impl MediaCapabilities {
    /// Create capabilities with a single default input.
    pub fn with_input(constraints: MediaConstraints) -> Self {
        let mut inputs = HashMap::new();
        inputs.insert("default".to_string(), constraints);
        Self {
            inputs,
            outputs: HashMap::new(),
        }
    }

    /// Create capabilities with a single default output.
    pub fn with_output(constraints: MediaConstraints) -> Self {
        let mut outputs = HashMap::new();
        outputs.insert("default".to_string(), constraints);
        Self {
            inputs: HashMap::new(),
            outputs,
        }
    }

    /// Create capabilities with both default input and output.
    pub fn with_input_output(input: MediaConstraints, output: MediaConstraints) -> Self {
        let mut inputs = HashMap::new();
        let mut outputs = HashMap::new();
        inputs.insert("default".to_string(), input);
        outputs.insert("default".to_string(), output);
        Self { inputs, outputs }
    }

    /// Check if this node accepts any input.
    pub fn accepts_any(&self) -> bool {
        self.inputs.is_empty()
    }

    /// Check if this node's output is unspecified.
    pub fn output_unspecified(&self) -> bool {
        self.outputs.is_empty()
    }

    /// Get the default input constraints, if any.
    pub fn default_input(&self) -> Option<&MediaConstraints> {
        self.inputs.get("default")
    }

    /// Get the default output constraints, if any.
    pub fn default_output(&self) -> Option<&MediaConstraints> {
        self.outputs.get("default")
    }
}

// =============================================================================
// Capability Behavior
// =============================================================================

/// How a node's capabilities are determined during pipeline resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapabilityBehavior {
    /// Fixed at compile time, never changes (e.g., Whisper: 16kHz mono f32)
    Static,
    /// Resolved from node params during manifest parsing
    Configured,
    /// Output inherits from upstream node's output
    Passthrough,
    /// Output adapts to downstream node's requirements
    Adaptive,
    /// Capabilities discovered at device init time (two-phase resolution)
    RuntimeDiscovered,
}

impl Default for CapabilityBehavior {
    fn default() -> Self {
        CapabilityBehavior::Passthrough
    }
}
