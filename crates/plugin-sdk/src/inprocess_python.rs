//! In-process Python execution via PyO3 (Android, and opt-in on Linux/macOS)
//!
//! This module provides the PythonNodeHandle for executing Python plugin nodes
//! directly in the host process using PyO3, without subprocess IPC.

#[cfg(feature = "inprocess-python")]
mod inner {
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList, PyBytes, PyString};
    use remotemedia_types::{RuntimeData, AudioSamples, PixelFormat, VideoCodec, ImageFormat, ControlMessageType};
    use std::collections::HashMap;

    /// Handle to a loaded Python plugin node
    pub struct PythonNodeHandle {
        module_name: String,
        class_name: String,
        instance: Py<PyAny>,
    }

    impl PythonNodeHandle {
        /// Load a Python module and instantiate the plugin class
        pub fn load(module_path: &str, class_name: &str) -> PyResult<Self> {
            // Python is initialized lazily on first attach
            Python::attach(|py| {
                // Import the module
                let module = py.import(module_path)?;
                
                // Get the class
                let class = module.getattr(class_name)?;
                
                // Instantiate the class
                let instance = class.call0()?.unbind();
                
                Ok(Self {
                    module_name: module_path.to_string(),
                    class_name: class_name.to_string(),
                    instance,
                })
            })
        }

        /// Call initialize(config) on the Python plugin
        pub fn initialize(&self, config: &HashMap<String, serde_json::Value>) -> PyResult<()> {
            Python::attach(|py| {
                let py_config = dict_from_json(py, config)?;
                let instance = self.instance.bind(py);
                instance.call_method1("initialize", (py_config,))?;
                Ok(())
            })
        }

        /// Call process(input_data) on the Python plugin
        pub fn process(&self, input_data: &RuntimeData) -> PyResult<RuntimeData> {
            Python::attach(|py| {
                let py_input = runtime_data_to_python(py, input_data)?;
                let instance = self.instance.bind(py);
                let py_output = instance.call_method1("process", (py_input,))?;
                python_to_runtime_data(py, &py_output)
            })
        }

        /// Call process_streaming(input_data) on the Python plugin
        pub fn process_streaming(&self, input_data: &RuntimeData) -> PyResult<Vec<RuntimeData>> {
            Python::attach(|py| {
                let py_input = runtime_data_to_python(py, input_data)?;
                let instance = self.instance.bind(py);
                let py_gen = instance.call_method1("process_streaming", (py_input,))?;
                
                let mut results = Vec::new();
                let iterator = py_gen.try_iter()?;
                for item in iterator {
                    let item = item?;
                    let py_data = python_to_runtime_data(py, &item)?;
                    results.push(py_data);
                }
                Ok(results)
            })
        }

        /// Call finalize() on the Python plugin
        pub fn finalize(&self) -> PyResult<()> {
            Python::attach(|py| {
                let instance = self.instance.bind(py);
                instance.call_method0("finalize")?;
                Ok(())
            })
        }
        }

        fn dict_from_json(py: Python<'_>, map: &HashMap<String, serde_json::Value>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        for (k, v) in map {
            dict.set_item(k, json_to_python(py, v)?)?;
        }
        Ok(dict.unbind())
    }

    fn json_to_python(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
        match value {
            serde_json::Value::Null => Ok(py.None().into()),
            serde_json::Value::Bool(b) => {
                let py_bool = pyo3::types::PyBool::new(py, *b);
                let py_any = py_bool.as_any().to_owned();
                Ok(py_any.unbind())
            }
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(i.into_pyobject(py)?.into_any().unbind())
                } else if let Some(f) = n.as_f64() {
                    Ok(f.into_pyobject(py)?.into_any().unbind())
                } else {
                    Ok(n.to_string().into_pyobject(py)?.into_any().unbind())
                }
            }
            serde_json::Value::String(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
            serde_json::Value::Array(arr) => {
                let list = PyList::new(py, arr.iter().map(|v| json_to_python(py, v)).collect::<PyResult<Vec<_>>>()?)?;
                Ok(list.into_any().unbind())
            }
            serde_json::Value::Object(obj) => dict_from_json(py, &obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<HashMap<_, _>>()).map(|d| d.into()),
        }
    }

    fn runtime_data_to_python(py: Python<'_>, data: &RuntimeData) -> PyResult<Py<PyAny>> {
        match data {
            RuntimeData::Audio { samples, sample_rate, channels, stream_id, timestamp_us, arrival_ts_us, metadata } => {
                let dict = PyDict::new(py);
                let samples_list = PyList::new(py, samples.as_slice().iter().cloned())?;
                dict.set_item("data_type", "audio")?;
                dict.set_item("samples", samples_list)?;
                dict.set_item("sample_rate", *sample_rate)?;
                dict.set_item("channels", *channels)?;
                if let Some(s) = stream_id {
                    dict.set_item("stream_id", s.as_str())?;
                }
                if let Some(ts) = timestamp_us {
                    dict.set_item("timestamp_us", *ts)?;
                }
                if let Some(ats) = arrival_ts_us {
                    dict.set_item("arrival_ts_us", *ats)?;
                }
                if let Some(m) = metadata {
                    dict.set_item("metadata", json_to_python(py, m)?)?;
                }
                Ok(dict.unbind().into())
            }
            RuntimeData::Video { pixel_data, width, height, format, codec, frame_number, is_keyframe, timestamp_us, stream_id, arrival_ts_us } => {
                let dict = PyDict::new(py);
                dict.set_item("data_type", "video")?;
                dict.set_item("pixel_data", PyBytes::new(py, pixel_data).into_pyobject(py)?.into_any().unbind())?;
                dict.set_item("width", *width)?;
                dict.set_item("height", *height)?;
                dict.set_item("pixel_format", *format as u8)?;
                if let Some(c) = codec {
                    dict.set_item("codec", *c as u8)?;
                }
                dict.set_item("frame_number", *frame_number)?;
                dict.set_item("is_keyframe", *is_keyframe)?;
                dict.set_item("timestamp_us", *timestamp_us)?;
                if let Some(s) = stream_id {
                    dict.set_item("stream_id", s.as_str())?;
                }
                if let Some(ats) = arrival_ts_us {
                    dict.set_item("arrival_ts_us", *ats)?;
                }
                Ok(dict.unbind().into())
            }
            RuntimeData::Image { data, format, width, height, timestamp_us, stream_id, metadata } => {
                let dict = PyDict::new(py);
                dict.set_item("data_type", "image")?;
                dict.set_item("data", PyBytes::new(py, data).into_pyobject(py)?.into_any().unbind())?;
                // Convert ImageFormat to string
                let format_str = match format {
                    ImageFormat::Jpeg => "jpeg",
                    ImageFormat::Png => "png",
                    ImageFormat::WebP => "webp",
                    ImageFormat::Raw { pixel_format } => {
                        // For raw, use pixel format name
                        match pixel_format {
                            PixelFormat::Rgb24 => "rgb24",
                            PixelFormat::Rgba32 => "rgba32",
                            PixelFormat::Yuv420p => "yuv420p",
                            PixelFormat::I420 => "i420",
                            PixelFormat::NV12 => "nv12",
                            _ => "raw",
                        }
                    }
                };
                dict.set_item("format", format_str)?;
                dict.set_item("width", *width)?;
                dict.set_item("height", *height)?;
                if let Some(ts) = timestamp_us {
                    dict.set_item("timestamp_us", *ts)?;
                }
                if let Some(s) = stream_id {
                    dict.set_item("stream_id", s.as_str())?;
                }
                if let Some(m) = metadata {
                    dict.set_item("metadata", json_to_python(py, m)?)?;
                }
                Ok(dict.unbind().into())
            }
            RuntimeData::Tensor { data, shape, dtype, metadata } => {
                let dict = PyDict::new(py);
                dict.set_item("data_type", "tensor")?;
                dict.set_item("data", PyBytes::new(py, data).into_pyobject(py)?.into_any().unbind())?;
                dict.set_item("shape", PyList::new(py, shape.iter().cloned())?)?;
                dict.set_item("dtype", *dtype)?;
                if let Some(m) = metadata {
                    dict.set_item("metadata", json_to_python(py, m)?)?;
                }
                Ok(dict.unbind().into())
            }
            RuntimeData::Numpy { data, shape, dtype, strides, c_contiguous, f_contiguous } => {
                let dict = PyDict::new(py);
                dict.set_item("data_type", "numpy")?;
                dict.set_item("data", PyBytes::new(py, data).into_pyobject(py)?.into_any().unbind())?;
                dict.set_item("shape", PyList::new(py, shape.iter().cloned())?)?;
                dict.set_item("dtype", dtype.clone())?;
                dict.set_item("strides", PyList::new(py, strides.iter().cloned())?)?;
                dict.set_item("c_contiguous", *c_contiguous)?;
                dict.set_item("f_contiguous", *f_contiguous)?;
                Ok(dict.unbind().into())
            }
            RuntimeData::Json(v) => json_to_python(py, v),
            RuntimeData::Text(s) => Ok(s.clone().into_pyobject(py)?.into_any().unbind()),
            RuntimeData::Binary(b) => Ok(PyBytes::new(py, b).into_pyobject(py)?.into_any().unbind()),
            RuntimeData::ControlMessage { message_type, segment_id, timestamp_ms, metadata } => {
                let dict = PyDict::new(py);
                dict.set_item("data_type", "control_message")?;
                // Convert ControlMessageType to string
                let msg_type_str = match message_type {
                    ControlMessageType::CancelSpeculation { .. } => "CancelSpeculation",
                    ControlMessageType::BatchHint { .. } => "BatchHint",
                    ControlMessageType::DeadlineWarning { .. } => "DeadlineWarning",
                };
                dict.set_item("message_type", msg_type_str)?;
                if let Some(sid) = segment_id {
                    dict.set_item("segment_id", sid.as_str())?;
                }
                dict.set_item("timestamp_ms", *timestamp_ms)?;
                dict.set_item("metadata", json_to_python(py, metadata)?)?;
                Ok(dict.unbind().into())
            }
            RuntimeData::File { path, filename, mime_type, size, offset, length, stream_id } => {
                let dict = PyDict::new(py);
                dict.set_item("data_type", "file")?;
                dict.set_item("path", path.as_str())?;
                if let Some(f) = filename {
                    dict.set_item("filename", f.as_str())?;
                }
                if let Some(mt) = mime_type {
                    dict.set_item("mime_type", mt.as_str())?;
                }
                if let Some(s) = size {
                    dict.set_item("size", *s)?;
                }
                if let Some(o) = offset {
                    dict.set_item("offset", *o)?;
                }
                if let Some(l) = length {
                    dict.set_item("length", *l)?;
                }
                if let Some(s) = stream_id {
                    dict.set_item("stream_id", s.as_str())?;
                }
                Ok(dict.unbind().into())
            }
        }
    }

    fn python_to_runtime_data(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<RuntimeData> {
        // Try to extract data_type field
        let data_type: Option<String> = obj.getattr("data_type").ok().and_then(|v| v.extract().ok());
        
        match data_type.as_deref() {
            Some("audio") => {
                let samples: Vec<f32> = obj.getattr("samples")?.extract()?;
                let sample_rate: u32 = obj.getattr("sample_rate")?.extract()?;
                let channels: u32 = obj.getattr("channels")?.extract()?;
                let timestamp_us: Option<u64> = obj.getattr("timestamp_us").ok().and_then(|v| v.extract().ok());
                let stream_id: Option<String> = obj.getattr("stream_id").ok().and_then(|v| v.extract().ok());
                let arrival_ts_us: Option<u64> = obj.getattr("arrival_ts_us").ok().and_then(|v| v.extract().ok());
                let metadata: Option<serde_json::Value> = obj.getattr("metadata").ok().and_then(|v| py_any_to_json(&v).ok());
                Ok(RuntimeData::Audio {
                    samples: samples.into(),
                    sample_rate,
                    channels,
                    stream_id,
                    timestamp_us,
                    arrival_ts_us,
                    metadata,
                })
            }
            Some("video") => {
                let pixel_data: Vec<u8> = obj.getattr("pixel_data")?.extract()?;
                let width: u32 = obj.getattr("width")?.extract()?;
                let height: u32 = obj.getattr("height")?.extract()?;
                let pixel_format: u8 = obj.getattr("pixel_format")?.extract()?;
                let codec: Option<u8> = obj.getattr("codec").ok().and_then(|v| v.extract().ok());
                let timestamp_us: u64 = obj.getattr("timestamp_us")?.extract()?;
                let frame_number: u64 = obj.getattr("frame_number").ok().and_then(|v| v.extract().ok()).unwrap_or(0);
                let is_keyframe: bool = obj.getattr("is_keyframe").ok().and_then(|v| v.extract().ok()).unwrap_or(false);
                let stream_id: Option<String> = obj.getattr("stream_id").ok().and_then(|v| v.extract().ok());
                let arrival_ts_us: Option<u64> = obj.getattr("arrival_ts_us").ok().and_then(|v| v.extract().ok());
                Ok(RuntimeData::Video {
                    pixel_data,
                    width,
                    height,
                    format: pixel_format_to_pixel_format(pixel_format),
                    codec: codec.map(codec_to_video_codec),
                    frame_number,
                    is_keyframe,
                    timestamp_us,
                    stream_id,
                    arrival_ts_us,
                })
            }
            Some("image") => {
                let data: Vec<u8> = obj.getattr("data")?.extract()?;
                let format: String = obj.getattr("format")?.extract()?;
                let width: u32 = obj.getattr("width")?.extract()?;
                let height: u32 = obj.getattr("height")?.extract()?;
                let timestamp_us: Option<u64> = obj.getattr("timestamp_us").ok().and_then(|v| v.extract().ok());
                let stream_id: Option<String> = obj.getattr("stream_id").ok().and_then(|v| v.extract().ok());
                let metadata: Option<serde_json::Value> = obj.getattr("metadata").ok().and_then(|v| py_any_to_json(&v).ok());
                Ok(RuntimeData::Image {
                    data,
                    format: string_to_image_format(&format),
                    width,
                    height,
                    timestamp_us,
                    stream_id,
                    metadata,
                })
            }
            Some("tensor") => {
                let data: Vec<u8> = obj.getattr("data")?.extract()?;
                let shape: Vec<i32> = obj.getattr("shape")?.extract()?;
                let dtype: i32 = obj.getattr("dtype")?.extract()?;
                let metadata: Option<serde_json::Value> = obj.getattr("metadata").ok().and_then(|v| py_any_to_json(&v).ok());
                Ok(RuntimeData::Tensor { data, shape, dtype, metadata })
            }
            Some("numpy") => {
                let data: Vec<u8> = obj.getattr("data")?.extract()?;
                let shape: Vec<usize> = obj.getattr("shape")?.extract()?;
                let dtype: String = obj.getattr("dtype")?.extract()?;
                let strides: Vec<isize> = obj.getattr("strides")?.extract()?;
                let c_contiguous: bool = obj.getattr("c_contiguous")?.extract()?;
                let f_contiguous: bool = obj.getattr("f_contiguous")?.extract()?;
                Ok(RuntimeData::Numpy { data, shape, dtype, strides, c_contiguous, f_contiguous })
            }
            Some("control_message") => {
                let v = py_any_to_json(obj)?;
                Ok(RuntimeData::Json(v))
            }
            Some("text") => {
                let s: String = obj.extract()?;
                Ok(RuntimeData::Text(s))
            }
            Some("binary") => {
                let b: Vec<u8> = obj.extract()?;
                Ok(RuntimeData::Binary(b))
            }
            Some("control_message") => {
                let message_type_str: String = obj.getattr("message_type")?.extract()?;
                let message_type = string_to_control_message_type(&message_type_str, &obj)?;
                let segment_id: Option<String> = obj.getattr("segment_id").ok().and_then(|v| v.extract().ok());
                let timestamp_ms: u64 = obj.getattr("timestamp_ms")?.extract()?;
                let metadata = py_any_to_json(&obj.getattr("metadata")?)?;
                Ok(RuntimeData::ControlMessage { message_type, segment_id, timestamp_ms, metadata })
            }
            Some("file") => {
                let path: String = obj.getattr("path")?.extract()?;
                let filename: Option<String> = obj.getattr("filename").ok().and_then(|v| v.extract().ok());
                let mime_type: Option<String> = obj.getattr("mime_type").ok().and_then(|v| v.extract().ok());
                let size: Option<u64> = obj.getattr("size").ok().and_then(|v| v.extract().ok());
                let offset: Option<u64> = obj.getattr("offset").ok().and_then(|v| v.extract().ok());
                let length: Option<u64> = obj.getattr("length").ok().and_then(|v| v.extract().ok());
                let stream_id: Option<String> = obj.getattr("stream_id").ok().and_then(|v| v.extract().ok());
                Ok(RuntimeData::File { path, filename, mime_type, size, offset, length, stream_id })
            }
            _ => {
                // Fallback: try to determine from object type
                if obj.is_instance_of::<PyString>() {
                    Ok(RuntimeData::Text(obj.extract()?))
                } else if obj.is_instance_of::<PyBytes>() {
                    Ok(RuntimeData::Binary(obj.extract()?))
                } else if obj.is_instance_of::<PyDict>() {
                    let v = py_dict_to_json(obj)?;
                    Ok(RuntimeData::Json(v))
                } else if obj.is_instance_of::<PyList>() {
                    let v = py_list_to_json(obj)?;
                    Ok(RuntimeData::Json(v))
                } else {
                    Err(pyo3::exceptions::PyTypeError::new_err("Unknown RuntimeData type").into())
                }
            }
        }
    }
    fn py_dict_to_json(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
        let dict = obj.downcast::<PyDict>()?;
        let mut map = serde_json::Map::new();
        for (key, value) in dict.iter() {
            let key_str: String = key.extract()?;
            let value_json = py_any_to_json(&value)?;
            map.insert(key_str, value_json);
        }
        Ok(serde_json::Value::Object(map))
    }

    fn py_list_to_json(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
        let list = obj.downcast::<PyList>()?;
        let mut vec = Vec::new();
        for item in list.iter() {
            let value_json = py_any_to_json(&item)?;
            vec.push(value_json);
        }
        Ok(serde_json::Value::Array(vec))
    }

    fn py_any_to_json(obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
        if obj.is_none() {
            Ok(serde_json::Value::Null)
        } else if let Ok(b) = obj.extract::<bool>() {
            Ok(serde_json::Value::Bool(b))
        } else if let Ok(i) = obj.extract::<i64>() {
            Ok(serde_json::Value::Number(i.into()))
        } else if let Ok(f) = obj.extract::<f64>() {
            Ok(serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap_or(serde_json::Number::from(0))))
        } else if let Ok(s) = obj.extract::<String>() {
            Ok(serde_json::Value::String(s))
        } else if obj.is_instance_of::<PyList>() {
            py_list_to_json(obj)
        } else if obj.is_instance_of::<PyDict>() {
            py_dict_to_json(obj)
        } else {
            // Fallback to string representation
            let s: String = obj.str()?.to_string();
            Ok(serde_json::Value::String(s))
        }
    }

    fn pixel_format_to_pixel_format(discriminant: u8) -> PixelFormat {
        match discriminant {
            0 => PixelFormat::Unspecified,
            1 => PixelFormat::Yuv420p,
            2 => PixelFormat::I420,
            3 => PixelFormat::NV12,
            4 => PixelFormat::Rgb24,
            5 => PixelFormat::Rgba32,
            255 => PixelFormat::Encoded,
            _ => PixelFormat::Unspecified,
        }
    }

    fn codec_to_video_codec(discriminant: u8) -> VideoCodec {
        match discriminant {
            1 => VideoCodec::Vp8,
            2 => VideoCodec::H264,
            3 => VideoCodec::Av1,
            _ => VideoCodec::Vp8,
        }
    }

    fn string_to_image_format(s: &str) -> ImageFormat {
        let lower = s.to_lowercase();
        if lower == "jpeg" || lower == "jpg" {
            ImageFormat::Jpeg
        } else if lower == "png" {
            ImageFormat::Png
        } else if lower == "webp" {
            ImageFormat::WebP
        } else if lower == "rgb24" {
            ImageFormat::Raw { pixel_format: PixelFormat::Rgb24 }
        } else if lower == "rgba32" {
            ImageFormat::Raw { pixel_format: PixelFormat::Rgba32 }
        } else if lower == "yuv420p" || lower == "i420" {
            ImageFormat::Raw { pixel_format: PixelFormat::Yuv420p }
        } else if lower == "nv12" {
            ImageFormat::Raw { pixel_format: PixelFormat::NV12 }
        } else {
            ImageFormat::Raw { pixel_format: PixelFormat::Rgb24 }
        }
    }

    fn string_to_control_message_type(s: &str, obj: &Bound<'_, PyAny>) -> PyResult<ControlMessageType> {
        match s {
            "CancelSpeculation" => {
                let from_timestamp: u64 = obj.getattr("from_timestamp").ok().and_then(|v| v.extract().ok()).unwrap_or(0);
                let to_timestamp: u64 = obj.getattr("to_timestamp").ok().and_then(|v| v.extract().ok()).unwrap_or(0);
                Ok(ControlMessageType::CancelSpeculation { from_timestamp, to_timestamp })
            }
            "BatchHint" => {
                let suggested_batch_size: usize = obj.getattr("suggested_batch_size").ok().and_then(|v| v.extract().ok()).unwrap_or(0);
                Ok(ControlMessageType::BatchHint { suggested_batch_size })
            }
            "DeadlineWarning" => {
                let deadline_us: u64 = obj.getattr("deadline_us").ok().and_then(|v| v.extract().ok()).unwrap_or(0);
                Ok(ControlMessageType::DeadlineWarning { deadline_us })
            }
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!("Unknown ControlMessageType: {}", s)).into()),
        }
    }
}

#[cfg(feature = "inprocess-python")]
#[cfg(test)]
mod tests {
    use super::PythonNodeHandle;
    use remotemedia_types::{RuntimeData, ControlMessageType};
    use serde_json::json;
    use std::collections::HashMap;

    async fn get_test_handle() -> PythonNodeHandle {
        PythonNodeHandle::load("tests.inprocess_test_plugin", "EchoPlugin")
            .expect("Failed to load test plugin")
    }

    #[tokio::test]
    async fn test_python_node_handle_load() {
        let handle = PythonNodeHandle::load("tests.inprocess_test_plugin", "EchoPlugin");
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn test_python_node_roundtrip() {
        let handle = get_test_handle().await;

        let config = HashMap::new();
        handle.initialize(&config).unwrap();

        let input = RuntimeData::Text("test message".into());
        let output = handle.process(&input).unwrap();

        match output {
            RuntimeData::Text(s) => assert_eq!(s.as_str(), "test message"),
            _ => panic!("Expected Text output"),
        }

        handle.finalize().unwrap();
    }

    #[tokio::test]
    async fn test_python_node_json_roundtrip() {
        let handle = get_test_handle().await;

        let config = HashMap::new();
        handle.initialize(&config).unwrap();

        let input = RuntimeData::Json(json!({"test": "value", "number": 123}));
        let output = handle.process(&input).unwrap();

        match output {
            RuntimeData::Json(j) => assert_eq!(j, json!({"test": "value", "number": 123})),
            _ => panic!("Expected Json output"),
        }

        handle.finalize().unwrap();
    }

    #[tokio::test]
    async fn test_python_node_error_propagation() {
        let handle = PythonNodeHandle::load("tests.inprocess_error_plugin", "ErrorPlugin")
            .expect("Failed to load error plugin");

        let config = HashMap::new();
        handle.initialize(&config).unwrap();

        let input = RuntimeData::Text("trigger error".into());
        let result = handle.process(&input);

        // Should return an error
        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_str = err.to_string();
        assert!(err_str.contains("Python") || err_str.contains("error"));

        handle.finalize().unwrap();
    }

    #[tokio::test]
    async fn test_python_node_streaming() {
        let handle = PythonNodeHandle::load("tests.inprocess_streaming_plugin", "StreamingPlugin")
            .expect("Failed to load streaming plugin");

        let config = HashMap::new();
        handle.initialize(&config).unwrap();

        let input = RuntimeData::Json(json!({"count": 3}));
        let outputs = handle.process_streaming(&input).unwrap();

        assert_eq!(outputs.len(), 3);
        for (i, output) in outputs.iter().enumerate() {
            match output {
                RuntimeData::Json(j) => assert_eq!(*j, json!({"index": i as i64, "value": i as i64 * 10})),
                _ => panic!("Expected Json output at index {}", i),
            }
        }

        handle.finalize().unwrap();
    }
}

#[cfg(feature = "inprocess-python")]
pub use inner::PythonNodeHandle;

#[cfg(not(feature = "inprocess-python"))]
compile_error!("inprocess-python feature is required to use this module");
