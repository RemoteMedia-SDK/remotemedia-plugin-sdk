//! In-process Python execution via PyO3 (Android, and opt-in on Linux/macOS)
//!
//! This module provides the PythonNodeHandle for executing Python plugin nodes
//! directly in the host process using PyO3, without subprocess IPC.

#[cfg(feature = "inprocess-python")]
mod conversion;

#[cfg(feature = "inprocess-python")]
mod inner {
    use super::conversion::{require_legacy, Conversion};
    use _remotemedia_buffers::{recover_bytes, OutputAllocator as PythonAllocator};
    use pyo3::prelude::*;
    use pyo3::types::{PyBytes, PyDict, PyList, PyString};
    use remotemedia_types::{
        ControlMessageType, CopyContext, ImageFormat, PixelFormat, RuntimeData, VideoCodec,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

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
            self.process_with_copy_context(input_data, CopyContext::legacy())
        }

        /// Legacy dictionary/list/bytes contract with explicit caller accounting.
        pub fn process_with_copy_context(
            &self,
            input_data: &RuntimeData,
            copies: &CopyContext,
        ) -> PyResult<RuntimeData> {
            require_legacy(copies)?;
            Python::attach(|py| {
                let mode = Conversion::Legacy(copies);
                let py_input = runtime_data_to_python(py, input_data, mode)?;
                let instance = self.instance.bind(py);
                let py_output = instance.call_method1("process", (py_input,))?;
                python_to_runtime_data(py, &py_output, mode)
            })
        }

        /// Call process_streaming(input_data) on the Python plugin
        pub fn process_streaming(&self, input_data: &RuntimeData) -> PyResult<Vec<RuntimeData>> {
            self.process_streaming_with_copy_context(input_data, CopyContext::legacy())
        }

        /// Legacy streaming conversion retains the same ledger for every output.
        pub fn process_streaming_with_copy_context(
            &self,
            input_data: &RuntimeData,
            copies: &CopyContext,
        ) -> PyResult<Vec<RuntimeData>> {
            require_legacy(copies)?;
            Python::attach(|py| {
                let mode = Conversion::Legacy(copies);
                let py_input = runtime_data_to_python(py, input_data, mode)?;
                let instance = self.instance.bind(py);
                let py_gen = instance.call_method1("process_streaming", (py_input,))?;

                let mut results = Vec::new();
                let iterator = py_gen.try_iter()?;
                for item in iterator {
                    let item = item?;
                    let py_data = python_to_runtime_data(py, &item, mode)?;
                    results.push(py_data);
                }
                Ok(results)
            })
        }

        /// Invoke the explicit owning-buffer Python contract. The node implements
        /// process_shared(data, ctx), and publishes ctx-allocated outputs. Private
        /// arrays and writable/unpublished outputs fail instead of being copied.
        /// Tensor integers use RuntimeData's documented convention (0=f32, 1=i32),
        /// not the incompatible Python IPC or protobuf dtype numbering.
        pub fn process_shared(
            &self,
            input: RuntimeData,
            allocator: Arc<dyn remotemedia_types::OutputAllocator>,
        ) -> PyResult<RuntimeData> {
            self.process_shared_context(input, PythonAllocator::new(allocator))
        }

        fn process_shared_context(
            &self,
            input: RuntimeData,
            context: PythonAllocator,
        ) -> PyResult<RuntimeData> {
            Python::attach(|py| {
                let output = self.invoke_shared(py, input, context, "process_shared")?;
                validate_shared(python_to_runtime_data(py, &output, Conversion::Shared)?)
            })
        }

        fn invoke_shared<'py>(
            &self,
            py: Python<'py>,
            input: RuntimeData,
            context: PythonAllocator,
            method: &str,
        ) -> PyResult<Bound<'py, PyAny>> {
            let input = validate_shared(input)?;
            let data = runtime_data_to_python(py, &input, Conversion::Shared)?;
            let context = Py::new(py, context)?;
            self.instance.bind(py).call_method1(method, (data, context))
        }

        /// Runtime entrypoint: require the call-selected provider and preserve
        /// session/node identity. No implicit heap or copying fallback.
        pub fn process_shared_with_ctx(
            &self,
            input: RuntimeData,
            ctx: &dyn remotemedia_traits::runtime_context::NodeRuntimeContextRead,
        ) -> remotemedia_types::Result<RuntimeData> {
            self.process_shared_context(input, shared_context(ctx)?)
                .map_err(shared_error)
        }

        /// Deliver each yield before advancing Python again. A rejected output
        /// stops iteration and closes generators, running their finally blocks.
        /// Calls are synchronous: no detached task can outlive this invocation.
        pub fn process_streaming_shared_with_ctx<F>(
            &self,
            input: RuntimeData,
            ctx: &dyn remotemedia_traits::runtime_context::NodeRuntimeContextRead,
            callback: F,
        ) -> remotemedia_types::Result<usize>
        where
            F: FnMut(RuntimeData) -> remotemedia_types::Result<()> + Send,
        {
            let context = shared_context(ctx)?;
            Python::attach(|py| {
                let output = self
                    .invoke_shared(py, input, context, "process_streaming_shared")
                    .map_err(shared_error)?;
                deliver_shared(py, output, callback)
            })
        }

        /// Invoke a source tick with the actual clock values and session provider.
        /// Quiet ticks return an empty iterable; generator completion ends only
        /// this tick. Outputs follow the same owning contract as reactive calls.
        pub fn tick_shared_with_ctx<F>(
            &self,
            tick: remotemedia_traits::streaming::Tick,
            ctx: &dyn remotemedia_traits::runtime_context::NodeRuntimeContextRead,
            callback: F,
        ) -> remotemedia_types::Result<usize>
        where
            F: FnMut(RuntimeData) -> remotemedia_types::Result<()> + Send,
        {
            let context = shared_context(ctx)?;
            Python::attach(|py| {
                let fields = PyDict::new(py);
                fields
                    .set_item("clock_id", tick.clock_id)
                    .map_err(shared_error)?;
                fields
                    .set_item("pts_us", tick.pts_us)
                    .map_err(shared_error)?;
                fields
                    .set_item("frame_idx", tick.frame_idx)
                    .map_err(shared_error)?;
                fields
                    .set_item("deadline_us", tick.deadline_us)
                    .map_err(shared_error)?;
                let context = Py::new(py, context).map_err(shared_error)?;
                let output = self
                    .instance
                    .bind(py)
                    .call_method1("tick_shared", (fields, context))
                    .map_err(shared_error)?;
                deliver_shared(py, output, callback)
            })
        }

        /// Generator counterpart; each yielded view retains its own owner.
        pub fn process_streaming_shared(
            &self,
            input: RuntimeData,
            allocator: Arc<dyn remotemedia_types::OutputAllocator>,
        ) -> PyResult<Vec<RuntimeData>> {
            Python::attach(|py| {
                let output = self.invoke_shared(
                    py,
                    input,
                    PythonAllocator::new(allocator),
                    "process_streaming_shared",
                )?;
                output
                    .try_iter()?
                    .map(|item| {
                        validate_shared(python_to_runtime_data(py, &item?, Conversion::Shared)?)
                    })
                    .collect()
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

    fn deliver_shared<F>(
        py: Python<'_>,
        output: Bound<'_, PyAny>,
        mut callback: F,
    ) -> remotemedia_types::Result<usize>
    where
        F: FnMut(RuntimeData) -> remotemedia_types::Result<()> + Send,
    {
        let result = (|| {
            let mut count = 0;
            for item in output.try_iter().map_err(shared_error)? {
                let item = item.map_err(shared_error)?;
                let data = python_to_runtime_data(py, &item, Conversion::Shared)
                    .and_then(validate_shared)
                    .map_err(shared_error)?;
                py.detach(|| callback(data))?;
                count += 1;
            }
            Ok(count)
        })();
        if result.is_err() && output.hasattr("close").map_err(shared_error)? {
            if let Err(close) = output.call_method0("close") {
                return Err(remotemedia_types::Error::Execution(format!(
                    "{}; Python generator close failed: {close}",
                    result.unwrap_err()
                )));
            }
        }
        result
    }

    fn shared_error(error: PyErr) -> remotemedia_types::Error {
        remotemedia_types::Error::Execution(format!("Python shared-buffer call failed: {error}"))
    }

    fn shared_context(
        ctx: &dyn remotemedia_traits::runtime_context::NodeRuntimeContextRead,
    ) -> remotemedia_types::Result<PythonAllocator> {
        let allocator = ctx.output_allocator().ok_or_else(|| {
            remotemedia_types::Error::Execution(
                "Python shared-buffer call requires an output allocator".into(),
            )
        })?;
        Ok(PythonAllocator::new(allocator).with_identity(ctx.session_id(), ctx.node_id()))
    }

    fn dict_from_json(
        py: Python<'_>,
        map: &HashMap<String, serde_json::Value>,
    ) -> PyResult<Py<PyDict>> {
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
                let list = PyList::new(
                    py,
                    arr.iter()
                        .map(|v| json_to_python(py, v))
                        .collect::<PyResult<Vec<_>>>()?,
                )?;
                Ok(list.into_any().unbind())
            }
            serde_json::Value::Object(obj) => dict_from_json(
                py,
                &obj.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<HashMap<_, _>>(),
            )
            .map(|d| d.into()),
        }
    }

    fn validate_shared(data: RuntimeData) -> PyResult<RuntimeData> {
        let encoding = remotemedia_types::TensorEncoding::Runtime;
        let (metadata, storage) = data
            .into_shared_parts(encoding)
            .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
        metadata
            .attach(storage, encoding)
            .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))
    }

    fn field<'py>(obj: &Bound<'py, PyAny>, name: &str) -> PyResult<Bound<'py, PyAny>> {
        if let Ok(dict) = obj.cast::<PyDict>() {
            dict.get_item(name)?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(name.to_owned()))
        } else {
            obj.getattr(name)
        }
    }

    fn json_from_python(obj: &Bound<'_, PyAny>, shared: bool) -> PyResult<serde_json::Value> {
        if !shared {
            return py_any_to_json(obj);
        }
        // Strict metadata must actually be JSON. Do not stringify an ndarray
        // or arbitrary object (which could materialize an entire hidden payload).
        let py = obj.py();
        let options = PyDict::new(py);
        options.set_item("allow_nan", false)?;
        let encoded: String = py
            .import("json")?
            .call_method("dumps", (obj,), Some(&options))?
            .extract()?;
        serde_json::from_str(&encoded)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
    }

    fn optional_metadata(
        obj: &Bound<'_, PyAny>,
        shared: bool,
    ) -> PyResult<Option<serde_json::Value>> {
        if !shared {
            // A missing field means no metadata; a present value that is not
            // JSON is an error rather than silently dropped metadata.
            return match field(obj, "metadata") {
                Ok(value) if value.is_none() => Ok(None),
                Ok(value) => py_any_to_json(&value).map(Some),
                Err(_) => Ok(None),
            };
        }
        match field(obj, "metadata") {
            Ok(value) if value.is_none() => Ok(None),
            Ok(value) => json_from_python(&value, true).map(Some),
            Err(error)
                if error.is_instance_of::<pyo3::exceptions::PyKeyError>(obj.py())
                    || error.is_instance_of::<pyo3::exceptions::PyAttributeError>(obj.py()) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    fn runtime_data_to_python(
        py: Python<'_>,
        data: &RuntimeData,
        mode: Conversion<'_>,
    ) -> PyResult<Py<PyAny>> {
        let shared = mode.is_shared();
        match data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                stream_id,
                timestamp_us,
                arrival_ts_us,
                metadata,
            } => {
                let dict = PyDict::new(py);
                let samples_list = mode.export_audio(py, samples)?;
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
            RuntimeData::Video {
                pixel_data,
                width,
                height,
                format,
                codec,
                frame_number,
                is_keyframe,
                timestamp_us,
                stream_id,
                arrival_ts_us,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("data_type", "video")?;
                dict.set_item("pixel_data", mode.export_bytes(py, pixel_data)?)?;
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
            RuntimeData::Image {
                data,
                format,
                width,
                height,
                timestamp_us,
                stream_id,
                metadata,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("data_type", "image")?;
                dict.set_item("data", mode.export_bytes(py, data)?)?;
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
            RuntimeData::Tensor {
                data,
                shape,
                dtype,
                metadata,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("data_type", "tensor")?;
                dict.set_item("data", mode.export_bytes(py, data)?)?;
                dict.set_item("shape", PyList::new(py, shape.iter().cloned())?)?;
                dict.set_item("dtype", *dtype)?;
                if let Some(m) = metadata {
                    dict.set_item("metadata", json_to_python(py, m)?)?;
                }
                Ok(dict.unbind().into())
            }
            RuntimeData::Numpy {
                data,
                shape,
                dtype,
                strides,
                c_contiguous,
                f_contiguous,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("data_type", "numpy")?;
                dict.set_item("data", mode.export_bytes(py, data)?)?;
                dict.set_item("shape", PyList::new(py, shape.iter().cloned())?)?;
                dict.set_item("dtype", dtype.clone())?;
                dict.set_item("strides", PyList::new(py, strides.iter().cloned())?)?;
                dict.set_item("c_contiguous", *c_contiguous)?;
                dict.set_item("f_contiguous", *f_contiguous)?;
                Ok(dict.unbind().into())
            }
            RuntimeData::Json(v) if shared => {
                let dict = PyDict::new(py);
                dict.set_item("data_type", "json")?;
                dict.set_item("data", json_to_python(py, v)?)?;
                Ok(dict.into_any().unbind())
            }
            RuntimeData::Json(v) => json_to_python(py, v),
            RuntimeData::Text(s) => Ok(s.clone().into_pyobject(py)?.into_any().unbind()),
            RuntimeData::Binary(b) => mode.export_bytes(py, b),
            RuntimeData::ControlMessage {
                message_type,
                segment_id,
                timestamp_ms,
                metadata,
            } => {
                let dict = PyDict::new(py);
                dict.set_item("data_type", "control_message")?;
                // Convert ControlMessageType to string
                let msg_type_str = match message_type {
                    ControlMessageType::CancelSpeculation { .. } => "CancelSpeculation",
                    ControlMessageType::BatchHint { .. } => "BatchHint",
                    ControlMessageType::DeadlineWarning { .. } => "DeadlineWarning",
                };
                dict.set_item("message_type", msg_type_str)?;
                match message_type {
                    ControlMessageType::CancelSpeculation {
                        from_timestamp,
                        to_timestamp,
                    } => {
                        dict.set_item("from_timestamp", *from_timestamp)?;
                        dict.set_item("to_timestamp", *to_timestamp)?;
                    }
                    ControlMessageType::BatchHint {
                        suggested_batch_size,
                    } => dict.set_item("suggested_batch_size", *suggested_batch_size)?,
                    ControlMessageType::DeadlineWarning { deadline_us } => {
                        dict.set_item("deadline_us", *deadline_us)?
                    }
                }
                if let Some(sid) = segment_id {
                    dict.set_item("segment_id", sid.as_str())?;
                }
                dict.set_item("timestamp_ms", *timestamp_ms)?;
                dict.set_item("metadata", json_to_python(py, metadata)?)?;
                Ok(dict.unbind().into())
            }
            RuntimeData::File {
                path,
                filename,
                mime_type,
                size,
                offset,
                length,
                stream_id,
            } => {
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

    fn python_to_runtime_data(
        _py: Python<'_>,
        obj: &Bound<'_, PyAny>,
        mode: Conversion<'_>,
    ) -> PyResult<RuntimeData> {
        let shared = mode.is_shared();
        // Try to extract data_type field
        let data_type: Option<String> = field(obj, "data_type").ok().and_then(|v| v.extract().ok());

        match data_type.as_deref() {
            Some("json") => Ok(RuntimeData::Json(json_from_python(
                &field(obj, "data")?,
                shared,
            )?)),
            Some("audio") => {
                let samples = mode.import_audio(&field(obj, "samples")?)?;
                let sample_rate: u32 = field(obj, "sample_rate")?.extract()?;
                let channels: u32 = field(obj, "channels")?.extract()?;
                let timestamp_us: Option<u64> = field(obj, "timestamp_us")
                    .ok()
                    .and_then(|v| v.extract().ok());
                let stream_id: Option<String> =
                    field(obj, "stream_id").ok().and_then(|v| v.extract().ok());
                let arrival_ts_us: Option<u64> = field(obj, "arrival_ts_us")
                    .ok()
                    .and_then(|v| v.extract().ok());
                let metadata = optional_metadata(obj, shared)?;
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
                let pixel_data = mode.import_bytes(&field(obj, "pixel_data")?)?;
                let width: u32 = field(obj, "width")?.extract()?;
                let height: u32 = field(obj, "height")?.extract()?;
                let pixel_format: u8 = field(obj, "pixel_format")?.extract()?;
                let codec: Option<u8> = field(obj, "codec").ok().and_then(|v| v.extract().ok());
                let timestamp_us: u64 = field(obj, "timestamp_us")?.extract()?;
                let frame_number: u64 = field(obj, "frame_number")
                    .ok()
                    .and_then(|v| v.extract().ok())
                    .unwrap_or(0);
                let is_keyframe: bool = field(obj, "is_keyframe")
                    .ok()
                    .and_then(|v| v.extract().ok())
                    .unwrap_or(false);
                let stream_id: Option<String> =
                    field(obj, "stream_id").ok().and_then(|v| v.extract().ok());
                let arrival_ts_us: Option<u64> = field(obj, "arrival_ts_us")
                    .ok()
                    .and_then(|v| v.extract().ok());
                Ok(RuntimeData::Video {
                    pixel_data: pixel_data.into(),
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
                let data = mode.import_bytes(&field(obj, "data")?)?;
                let format: String = field(obj, "format")?.extract()?;
                let width: u32 = field(obj, "width")?.extract()?;
                let height: u32 = field(obj, "height")?.extract()?;
                let timestamp_us: Option<u64> = field(obj, "timestamp_us")
                    .ok()
                    .and_then(|v| v.extract().ok());
                let stream_id: Option<String> =
                    field(obj, "stream_id").ok().and_then(|v| v.extract().ok());
                let metadata = optional_metadata(obj, shared)?;
                Ok(RuntimeData::Image {
                    data: data.into(),
                    format: string_to_image_format(&format),
                    width,
                    height,
                    timestamp_us,
                    stream_id,
                    metadata,
                })
            }
            Some("tensor") => {
                let data = mode.import_bytes(&field(obj, "data")?)?;
                let shape: Vec<i32> = field(obj, "shape")?.extract()?;
                let dtype: i32 = field(obj, "dtype")?.extract()?;
                let metadata = optional_metadata(obj, shared)?;
                Ok(RuntimeData::Tensor {
                    data: data.into(),
                    shape,
                    dtype,
                    metadata,
                })
            }
            Some("numpy") => {
                let data = mode.import_bytes(&field(obj, "data")?)?;
                let shape: Vec<usize> = field(obj, "shape")?.extract()?;
                let dtype: String = field(obj, "dtype")?.extract()?;
                let strides: Vec<isize> = field(obj, "strides")?.extract()?;
                let c_contiguous: bool = field(obj, "c_contiguous")?.extract()?;
                let f_contiguous: bool = field(obj, "f_contiguous")?.extract()?;
                Ok(RuntimeData::Numpy {
                    data: data.into(),
                    shape,
                    dtype,
                    strides,
                    c_contiguous,
                    f_contiguous,
                })
            }
            Some("text") => {
                let s: String = obj.extract()?;
                Ok(RuntimeData::Text(s))
            }
            Some("binary") => {
                let b = mode.import_bytes(&field(obj, "data")?)?;
                Ok(RuntimeData::Binary(b.into()))
            }
            Some("control_message") => {
                let message_type_str: String = field(obj, "message_type")?.extract()?;
                let message_type = string_to_control_message_type(&message_type_str, &obj)?;
                let segment_id: Option<String> =
                    field(obj, "segment_id").ok().and_then(|v| v.extract().ok());
                let timestamp_ms: u64 = field(obj, "timestamp_ms")?.extract()?;
                let metadata = json_from_python(&field(obj, "metadata")?, shared)?;
                Ok(RuntimeData::ControlMessage {
                    message_type,
                    segment_id,
                    timestamp_ms,
                    metadata,
                })
            }
            Some("file") => {
                let path: String = field(obj, "path")?.extract()?;
                let filename: Option<String> =
                    field(obj, "filename").ok().and_then(|v| v.extract().ok());
                let mime_type: Option<String> =
                    field(obj, "mime_type").ok().and_then(|v| v.extract().ok());
                let size: Option<u64> = field(obj, "size").ok().and_then(|v| v.extract().ok());
                let offset: Option<u64> = field(obj, "offset").ok().and_then(|v| v.extract().ok());
                let length: Option<u64> = field(obj, "length").ok().and_then(|v| v.extract().ok());
                let stream_id: Option<String> =
                    field(obj, "stream_id").ok().and_then(|v| v.extract().ok());
                Ok(RuntimeData::File {
                    path,
                    filename,
                    mime_type,
                    size,
                    offset,
                    length,
                    stream_id,
                })
            }
            _ => {
                // Fallback: try to determine from object type
                if obj.is_instance_of::<PyString>() {
                    Ok(RuntimeData::Text(obj.extract()?))
                } else if obj.is_instance_of::<PyBytes>() {
                    Ok(RuntimeData::Binary(mode.import_bytes(obj)?))
                } else if obj.is_instance_of::<PyDict>() {
                    let v = json_from_python(obj, shared)?;
                    Ok(RuntimeData::Json(v))
                } else if obj.is_instance_of::<PyList>() {
                    let v = json_from_python(obj, shared)?;
                    Ok(RuntimeData::Json(v))
                } else {
                    if shared {
                        Ok(RuntimeData::Binary(recover_bytes(obj)?))
                    } else {
                        Err(
                            pyo3::exceptions::PyTypeError::new_err("Unknown RuntimeData type")
                                .into(),
                        )
                    }
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
            Ok(serde_json::Value::Number(
                serde_json::Number::from_f64(f).unwrap_or(serde_json::Number::from(0)),
            ))
        } else if let Ok(s) = obj.extract::<String>() {
            Ok(serde_json::Value::String(s))
        } else if obj.is_instance_of::<PyList>() {
            py_list_to_json(obj)
        } else if obj.is_instance_of::<pyo3::types::PyTuple>() {
            let tuple = obj.downcast::<pyo3::types::PyTuple>()?;
            tuple
                .iter()
                .map(|item| py_any_to_json(&item))
                .collect::<PyResult<Vec<_>>>()
                .map(serde_json::Value::Array)
        } else if obj.is_instance_of::<PyDict>() {
            py_dict_to_json(obj)
        } else {
            // Stringifying an ndarray, bytes or arbitrary object silently
            // materializes (and corrupts) a hidden payload; reject instead.
            Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "cannot convert Python {} to JSON; carry bulk data as a RuntimeData payload",
                obj.get_type().name()?
            )))
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
            ImageFormat::Raw {
                pixel_format: PixelFormat::Rgb24,
            }
        } else if lower == "rgba32" {
            ImageFormat::Raw {
                pixel_format: PixelFormat::Rgba32,
            }
        } else if lower == "yuv420p" || lower == "i420" {
            ImageFormat::Raw {
                pixel_format: PixelFormat::Yuv420p,
            }
        } else if lower == "nv12" {
            ImageFormat::Raw {
                pixel_format: PixelFormat::NV12,
            }
        } else {
            ImageFormat::Raw {
                pixel_format: PixelFormat::Rgb24,
            }
        }
    }

    fn string_to_control_message_type(
        s: &str,
        obj: &Bound<'_, PyAny>,
    ) -> PyResult<ControlMessageType> {
        match s {
            "CancelSpeculation" => {
                let from_timestamp: u64 = field(obj, "from_timestamp")
                    .ok()
                    .and_then(|v| v.extract().ok())
                    .unwrap_or(0);
                let to_timestamp: u64 = field(obj, "to_timestamp")
                    .ok()
                    .and_then(|v| v.extract().ok())
                    .unwrap_or(0);
                Ok(ControlMessageType::CancelSpeculation {
                    from_timestamp,
                    to_timestamp,
                })
            }
            "BatchHint" => {
                let suggested_batch_size: usize = field(obj, "suggested_batch_size")
                    .ok()
                    .and_then(|v| v.extract().ok())
                    .unwrap_or(0);
                Ok(ControlMessageType::BatchHint {
                    suggested_batch_size,
                })
            }
            "DeadlineWarning" => {
                let deadline_us: u64 = field(obj, "deadline_us")
                    .ok()
                    .and_then(|v| v.extract().ok())
                    .unwrap_or(0);
                Ok(ControlMessageType::DeadlineWarning { deadline_us })
            }
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Unknown ControlMessageType: {}",
                s
            ))
            .into()),
        }
    }

    #[cfg(test)]
    mod json_conversion_tests {
        use super::*;

        #[test]
        fn legacy_json_rejects_bulk_objects_instead_of_stringifying_them() {
            Python::attach(|py| {
                let value = py
                    .eval(
                        c"{'n': 1, 'pair': (2, 3), 'tags': ['a'], 'payload': b'raw'}",
                        None,
                        None,
                    )
                    .unwrap();
                let error = py_any_to_json(&value).unwrap_err().to_string();
                assert!(error.contains("bytes"), "{error}");
                assert!(error.contains("RuntimeData"), "{error}");
                let value = py
                    .eval(c"{'n': 1, 'pair': (2, 3), 'tags': ['a']}", None, None)
                    .unwrap();
                assert_eq!(
                    py_any_to_json(&value).unwrap(),
                    serde_json::json!({"n": 1, "pair": [2, 3], "tags": ["a"]}),
                    "tuples become JSON arrays, not their repr"
                );
            });
        }
    }
}

#[cfg(feature = "inprocess-python")]
#[cfg(test)]
mod tests {
    use super::PythonNodeHandle;
    use remotemedia_types::{ControlMessageType, RuntimeData};
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
                RuntimeData::Json(j) => {
                    assert_eq!(*j, json!({"index": i as i64, "value": i as i64 * 10}))
                }
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
