use crate::{ImageFormat, PixelFormat, TensorEncoding};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PayloadLayoutError {
    #[error("payload storage kind does not match its metadata")]
    WrongPayload,
    #[error("invalid shared payload layout: {0}")]
    Invalid(&'static str),
    #[error("unsupported scalar dtype or byte order")]
    UnsupportedDtype,
    #[error("tensor dtype convention differs from the negotiated convention")]
    TensorEncodingMismatch,
}

type Result<T> = std::result::Result<T, PayloadLayoutError>;

pub(crate) fn audio(sample_rate: u32, channels: u32, bytes: u64) -> Result<()> {
    if sample_rate == 0 || channels == 0 || !bytes.is_multiple_of(4 * u64::from(channels)) {
        return Err(PayloadLayoutError::Invalid(
            "audio rate/channels/frame length",
        ));
    }
    Ok(())
}

pub(crate) fn video(
    format: PixelFormat,
    has_codec: bool,
    width: u32,
    height: u32,
    bytes: u64,
) -> Result<()> {
    if (format == PixelFormat::Encoded) != has_codec {
        return Err(PayloadLayoutError::Invalid("video codec/pixel format"));
    }
    pixels(format, width, height, bytes)
}

pub(crate) fn tensor(
    shape: &[i32],
    dtype: i32,
    encoding: TensorEncoding,
    bytes: u64,
) -> Result<()> {
    if shape.len() > 64 || shape.iter().any(|&d| d < 0) {
        return Err(PayloadLayoutError::Invalid("tensor dimensions"));
    }
    let size = tensor_size(dtype, encoding)?;
    let expected = if shape.contains(&0) {
        0
    } else {
        product(shape.iter().map(|&d| d as u64), size)?
    };
    exact(bytes, expected)
}

pub(crate) fn exact(actual: u64, expected: u64) -> Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(PayloadLayoutError::Invalid("payload byte length"))
    }
}

pub(crate) fn product(mut shape: impl Iterator<Item = u64>, item_size: u64) -> Result<u64> {
    shape.try_fold(item_size, |size, dimension| {
        size.checked_mul(dimension)
            .filter(|&n| n <= isize::MAX as u64)
            .ok_or(PayloadLayoutError::Invalid("shape byte size overflow"))
    })
}

/// RuntimeData's documented codes differ from both older Python IPC and protobuf.
/// No convention is inferred from an integer or from the payload's byte count.
pub(crate) fn tensor_size(dtype: i32, encoding: TensorEncoding) -> Result<u64> {
    match (encoding, dtype) {
        (TensorEncoding::Runtime, 0 | 1) => Ok(4),
        (TensorEncoding::PythonIpc, 1 | 3) => Ok(4),
        (TensorEncoding::PythonIpc, 2 | 4) => Ok(8),
        (TensorEncoding::PythonIpc, 5) => Ok(1),
        (TensorEncoding::PythonIpc, 6) => Ok(2),
        (TensorEncoding::Protobuf, 1 | 3) => Ok(4),
        (TensorEncoding::Protobuf, 2) => Ok(2),
        (TensorEncoding::Protobuf, 4 | 5) => Ok(1),
        _ => Err(PayloadLayoutError::UnsupportedDtype),
    }
}

/// Portable numeric NumPy dtypes only: no object pointers, structs or strings.
/// Explicit non-native endian is preserved as metadata and rejected here; this
/// host-view protocol does not silently swap or reinterpret bytes.
pub(crate) fn numpy_size(dtype: &str) -> Result<u64> {
    let (endian, code) = match dtype.as_bytes().first() {
        Some(b'<' | b'>' | b'=' | b'|') => (&dtype[..1], &dtype[1..]),
        _ => ("=", dtype),
    };
    let size = match code {
        "bool" | "bool_" | "?" | "b1" | "int8" | "uint8" | "i1" | "u1" => 1,
        "float16" | "int16" | "uint16" | "f2" | "i2" | "u2" => 2,
        "float32" | "int32" | "uint32" | "f4" | "i4" | "u4" => 4,
        "float64" | "int64" | "uint64" | "complex64" | "f8" | "i8" | "u8" | "c8" => 8,
        "complex128" | "c16" => 16,
        _ => return Err(PayloadLayoutError::UnsupportedDtype),
    };
    if size > 1
        && (endian == "|"
            || (endian == ">" && cfg!(target_endian = "little"))
            || (endian == "<" && cfg!(target_endian = "big")))
    {
        return Err(PayloadLayoutError::UnsupportedDtype);
    }
    Ok(size)
}

pub(crate) fn pixels(format: PixelFormat, width: u32, height: u32, bytes: u64) -> Result<()> {
    if width == 0 || height == 0 {
        return Err(PayloadLayoutError::Invalid("zero image dimension"));
    }
    let pixels = product([width as u64, height as u64].into_iter(), 1)?;
    let expected = match format {
        PixelFormat::Rgb24 => product([pixels].into_iter(), 3)?,
        PixelFormat::Rgba32 => product([pixels].into_iter(), 4)?,
        PixelFormat::Yuv420p | PixelFormat::I420 | PixelFormat::NV12 => {
            if !width.is_multiple_of(2) || !height.is_multiple_of(2) {
                return Err(PayloadLayoutError::Invalid(
                    "4:2:0 planes need even dimensions",
                ));
            }
            product([pixels / 2].into_iter(), 3)?
        }
        PixelFormat::Encoded => {
            return if bytes > 0 {
                Ok(())
            } else {
                Err(PayloadLayoutError::Invalid("empty encoded frame"))
            };
        }
        PixelFormat::Unspecified => {
            return Err(PayloadLayoutError::Invalid("unspecified pixel layout"))
        }
    };
    exact(bytes, expected)
}

pub(crate) fn image(format: &ImageFormat, width: u32, height: u32, bytes: u64) -> Result<()> {
    match format {
        ImageFormat::Raw {
            pixel_format: PixelFormat::Encoded | PixelFormat::Unspecified,
        } => Err(PayloadLayoutError::Invalid(
            "raw image needs a raw pixel layout",
        )),
        ImageFormat::Raw { pixel_format } => pixels(*pixel_format, width, height, bytes),
        _ if width > 0 && height > 0 && bytes > 0 => Ok(()),
        _ => Err(PayloadLayoutError::Invalid("empty encoded image")),
    }
}

fn contiguous<'a>(
    dimensions: impl Iterator<Item = (&'a usize, &'a isize)>,
    item_size: u64,
) -> bool {
    let mut expected = item_size;
    for (&dimension, &stride) in dimensions {
        if dimension > 1 && u64::try_from(stride).ok() != Some(expected) {
            return false;
        }
        let Some(next) = expected.checked_mul(dimension as u64) else {
            return false;
        };
        expected = next;
    }
    true
}

pub(crate) fn numpy(
    shape: &[usize],
    strides: &[isize],
    dtype: &str,
    c_contiguous: bool,
    f_contiguous: bool,
    bytes: u64,
) -> Result<()> {
    if shape.iter().any(|&d| d > isize::MAX as usize) {
        return Err(PayloadLayoutError::Invalid("array dimension"));
    }
    if shape.len() != strides.len() || shape.len() > 64 {
        return Err(PayloadLayoutError::Invalid("array rank/stride count"));
    }
    let item_size = numpy_size(dtype)?;
    // Negative/broadcast/noncontiguous views need a richer origin/span contract;
    // rejecting them is deliberate, never a request to pack a private copy.
    let empty = shape.contains(&0);
    let expected = if empty {
        0
    } else {
        product(shape.iter().map(|&d| d as u64), item_size)?
    };
    exact(bytes, expected)?;
    let c = empty || contiguous(shape.iter().zip(strides).rev(), item_size);
    let f = empty || contiguous(shape.iter().zip(strides), item_size);
    if c != c_contiguous || f != f_contiguous || !(c || f) {
        return Err(PayloadLayoutError::Invalid("array contiguity/strides"));
    }
    Ok(())
}
