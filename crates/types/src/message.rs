//! Metadata and bulk owners have separate types: serializing metadata can never
//! accidentally serialize its shared byte/sample allocation.
use crate::payload_layout::{self as layout, PayloadLayoutError};
use crate::{
    ControlMessageType, ImageFormat, PixelFormat, RuntimeData, SharedBuffer, SharedBytes,
    VideoCodec,
};
use serde::{Deserialize, Serialize};
type Result<T> = std::result::Result<T, PayloadLayoutError>;

/// Must be negotiated, never inferred from a numeric dtype code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TensorEncoding {
    /// RuntimeData documentation: 0=f32, 1=i32. Other codes are not specified.
    Runtime,
    /// Python multiprocessing TensorType: 1=f32, 2=f64, 3=i32, 4=i64, 5=u8, 6=u16.
    PythonIpc,
    /// common.proto TensorDtype: 1=f32, 2=f16, 3=i32, 4=i8, 5=u8.
    Protobuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    AudioF32,
    Bytes,
}

/// Intentionally has no serde implementation. Clone retains the existing owner.
#[derive(Debug, Clone)]
pub enum PayloadStorage {
    None,
    Audio(SharedBuffer<f32>),
    Bytes(SharedBytes),
}
impl PayloadStorage {
    pub fn layout(&self) -> (Option<PayloadKind>, u64) {
        match self {
            Self::None => (None, 0),
            Self::Audio(data) => (Some(PayloadKind::AudioF32), data.len() as u64 * 4),
            Self::Bytes(data) => (Some(PayloadKind::Bytes), data.len() as u64),
        }
    }
}

/// Small per-message fields only. This schema is versioned by its IPC/ABI envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RuntimeMetadata {
    Audio {
        sample_rate: u32,
        channels: u32,
        stream_id: Option<String>,
        timestamp_us: Option<u64>,
        arrival_ts_us: Option<u64>,
        metadata: Option<serde_json::Value>,
    },
    Video {
        width: u32,
        height: u32,
        format: PixelFormat,
        codec: Option<VideoCodec>,
        frame_number: u64,
        timestamp_us: u64,
        is_keyframe: bool,
        stream_id: Option<String>,
        arrival_ts_us: Option<u64>,
    },
    Image {
        format: ImageFormat,
        width: u32,
        height: u32,
        timestamp_us: Option<u64>,
        stream_id: Option<String>,
        metadata: Option<serde_json::Value>,
    },
    Tensor {
        shape: Vec<i32>,
        dtype: i32,
        metadata: Option<serde_json::Value>,
        encoding: TensorEncoding,
    },
    Numpy {
        shape: Vec<usize>,
        dtype: String,
        strides: Vec<isize>,
        c_contiguous: bool,
        f_contiguous: bool,
    },
    ControlMessage {
        message_type: ControlMessageType,
        segment_id: Option<String>,
        timestamp_ms: u64,
        metadata: serde_json::Value,
    },
    File {
        path: String,
        filename: Option<String>,
        mime_type: Option<String>,
        size: Option<u64>,
        offset: Option<u64>,
        length: Option<u64>,
        stream_id: Option<String>,
    },
    Binary,
    Json(serde_json::Value),
    Text(String),
}

impl RuntimeData {
    /// Validate a borrowed payload with the same rules as a shared descriptor.
    /// Does not clone owners, metadata, or payload elements and never packs a view.
    /// Tensor codes are interpreted only in the supplied negotiated convention.
    pub fn validate_shared_layout(&self, encoding: TensorEncoding) -> Result<()> {
        let bytes = self.bulk_payload_bytes() as u64;
        if bytes > isize::MAX as u64 {
            return Err(PayloadLayoutError::Invalid("payload too large"));
        }
        match self {
            Self::Audio {
                sample_rate,
                channels,
                ..
            } => layout::audio(*sample_rate, *channels, bytes),
            Self::Video {
                format,
                codec,
                width,
                height,
                ..
            } => layout::video(*format, codec.is_some(), *width, *height, bytes),
            Self::Image {
                format,
                width,
                height,
                ..
            } => layout::image(format, *width, *height, bytes),
            Self::Tensor { shape, dtype, .. } => layout::tensor(shape, *dtype, encoding, bytes),
            Self::Numpy {
                shape,
                dtype,
                strides,
                c_contiguous,
                f_contiguous,
                ..
            } => layout::numpy(shape, strides, dtype, *c_contiguous, *f_contiguous, bytes),
            _ => Ok(()),
        }
    }

    /// Move payload owners out without reading or copying their elements.
    /// Legacy Vec audio is promoted by moving its Vec into a shared owner.
    pub fn into_shared_parts(
        self,
        tensor_encoding: TensorEncoding,
    ) -> Result<(RuntimeMetadata, PayloadStorage)> {
        let (metadata, payload) = match self {
            Self::Audio {
                sample_rate,
                channels,
                stream_id,
                timestamp_us,
                arrival_ts_us,
                metadata,
                samples,
            } => (
                RuntimeMetadata::Audio {
                    sample_rate,
                    channels,
                    stream_id,
                    timestamp_us,
                    arrival_ts_us,
                    metadata,
                },
                PayloadStorage::Audio(samples.into_shared()),
            ),
            Self::Video {
                width,
                height,
                format,
                codec,
                frame_number,
                timestamp_us,
                is_keyframe,
                stream_id,
                arrival_ts_us,
                pixel_data,
            } => (
                RuntimeMetadata::Video {
                    width,
                    height,
                    format,
                    codec,
                    frame_number,
                    timestamp_us,
                    is_keyframe,
                    stream_id,
                    arrival_ts_us,
                },
                PayloadStorage::Bytes(pixel_data),
            ),
            Self::Image {
                format,
                width,
                height,
                timestamp_us,
                stream_id,
                metadata,
                data,
            } => (
                RuntimeMetadata::Image {
                    format,
                    width,
                    height,
                    timestamp_us,
                    stream_id,
                    metadata,
                },
                PayloadStorage::Bytes(data),
            ),
            Self::Tensor {
                shape,
                dtype,
                metadata,
                data,
            } => (
                RuntimeMetadata::Tensor {
                    shape,
                    dtype,
                    metadata,
                    encoding: tensor_encoding,
                },
                PayloadStorage::Bytes(data),
            ),
            Self::Numpy {
                shape,
                dtype,
                strides,
                c_contiguous,
                f_contiguous,
                data,
            } => (
                RuntimeMetadata::Numpy {
                    shape,
                    dtype,
                    strides,
                    c_contiguous,
                    f_contiguous,
                },
                PayloadStorage::Bytes(data),
            ),
            Self::ControlMessage {
                message_type,
                segment_id,
                timestamp_ms,
                metadata,
            } => (
                RuntimeMetadata::ControlMessage {
                    message_type,
                    segment_id,
                    timestamp_ms,
                    metadata,
                },
                PayloadStorage::None,
            ),
            Self::File {
                path,
                filename,
                mime_type,
                size,
                offset,
                length,
                stream_id,
            } => (
                RuntimeMetadata::File {
                    path,
                    filename,
                    mime_type,
                    size,
                    offset,
                    length,
                    stream_id,
                },
                PayloadStorage::None,
            ),
            Self::Binary(data) => (RuntimeMetadata::Binary, PayloadStorage::Bytes(data)),
            Self::Json(data) => (RuntimeMetadata::Json(data), PayloadStorage::None),
            Self::Text(data) => (RuntimeMetadata::Text(data), PayloadStorage::None),
        };
        let (kind, byte_len) = payload.layout();
        metadata.validate_payload(kind, byte_len, tensor_encoding)?;
        Ok((metadata, payload))
    }
}

impl RuntimeMetadata {
    pub fn payload_kind(&self) -> Option<PayloadKind> {
        match self {
            Self::Audio { .. } => Some(PayloadKind::AudioF32),
            Self::Video { .. }
            | Self::Image { .. }
            | Self::Tensor { .. }
            | Self::Numpy { .. }
            | Self::Binary => Some(PayloadKind::Bytes),
            _ => None,
        }
    }

    /// Validate against the descriptor before mapping or making any typed view.
    /// Native-endian numeric views and contiguous layouts are supported; opaque
    /// encoded images/video are checked for nonempty storage, not decoded here.
    pub fn validate_payload(
        &self,
        kind: Option<PayloadKind>,
        bytes: u64,
        tensor_encoding: TensorEncoding,
    ) -> Result<()> {
        if self.payload_kind() != kind {
            return Err(PayloadLayoutError::WrongPayload);
        }
        if bytes > isize::MAX as u64 {
            return Err(PayloadLayoutError::Invalid("payload too large"));
        }
        match self {
            Self::Audio {
                sample_rate,
                channels,
                ..
            } => layout::audio(*sample_rate, *channels, bytes),
            Self::Video {
                width,
                height,
                format,
                codec,
                ..
            } => layout::video(*format, codec.is_some(), *width, *height, bytes),
            Self::Image {
                width,
                height,
                format,
                ..
            } => layout::image(format, *width, *height, bytes),
            Self::Tensor {
                shape,
                dtype,
                encoding,
                ..
            } => {
                if *encoding != tensor_encoding {
                    return Err(PayloadLayoutError::TensorEncodingMismatch);
                }
                layout::tensor(shape, *dtype, *encoding, bytes)
            }
            Self::Numpy {
                shape,
                dtype,
                strides,
                c_contiguous,
                f_contiguous,
            } => layout::numpy(shape, strides, dtype, *c_contiguous, *f_contiguous, bytes),
            Self::Binary => Ok(()),
            _ => layout::exact(bytes, 0),
        }
    }

    /// Reattach a validated owner, preserving metadata and allocation identity.
    pub fn attach(
        self,
        payload: PayloadStorage,
        tensor_encoding: TensorEncoding,
    ) -> Result<RuntimeData> {
        let (kind, bytes) = payload.layout();
        self.validate_payload(kind, bytes, tensor_encoding)?;
        Ok(match (self, payload) {
            (
                Self::Audio {
                    sample_rate,
                    channels,
                    stream_id,
                    timestamp_us,
                    arrival_ts_us,
                    metadata,
                },
                PayloadStorage::Audio(samples),
            ) => RuntimeData::Audio {
                sample_rate,
                channels,
                stream_id,
                timestamp_us,
                arrival_ts_us,
                metadata,
                samples: samples.into(),
            },
            (
                Self::Video {
                    width,
                    height,
                    format,
                    codec,
                    frame_number,
                    timestamp_us,
                    is_keyframe,
                    stream_id,
                    arrival_ts_us,
                },
                PayloadStorage::Bytes(pixel_data),
            ) => RuntimeData::Video {
                width,
                height,
                format,
                codec,
                frame_number,
                timestamp_us,
                is_keyframe,
                stream_id,
                arrival_ts_us,
                pixel_data,
            },
            (
                Self::Image {
                    format,
                    width,
                    height,
                    timestamp_us,
                    stream_id,
                    metadata,
                },
                PayloadStorage::Bytes(data),
            ) => RuntimeData::Image {
                format,
                width,
                height,
                timestamp_us,
                stream_id,
                metadata,
                data,
            },
            (
                Self::Tensor {
                    shape,
                    dtype,
                    metadata,
                    encoding: _,
                },
                PayloadStorage::Bytes(data),
            ) => RuntimeData::Tensor {
                shape,
                dtype,
                metadata,
                data,
            },
            (
                Self::Numpy {
                    shape,
                    dtype,
                    strides,
                    c_contiguous,
                    f_contiguous,
                },
                PayloadStorage::Bytes(data),
            ) => RuntimeData::Numpy {
                shape,
                dtype,
                strides,
                c_contiguous,
                f_contiguous,
                data,
            },
            (
                Self::ControlMessage {
                    message_type,
                    segment_id,
                    timestamp_ms,
                    metadata,
                },
                PayloadStorage::None,
            ) => RuntimeData::ControlMessage {
                message_type,
                segment_id,
                timestamp_ms,
                metadata,
            },
            (
                Self::File {
                    path,
                    filename,
                    mime_type,
                    size,
                    offset,
                    length,
                    stream_id,
                },
                PayloadStorage::None,
            ) => RuntimeData::File {
                path,
                filename,
                mime_type,
                size,
                offset,
                length,
                stream_id,
            },
            (Self::Binary, PayloadStorage::Bytes(data)) => RuntimeData::Binary(data),
            (Self::Json(data), PayloadStorage::None) => RuntimeData::Json(data),
            (Self::Text(data), PayloadStorage::None) => RuntimeData::Text(data),
            _ => return Err(PayloadLayoutError::WrongPayload),
        })
    }
}
