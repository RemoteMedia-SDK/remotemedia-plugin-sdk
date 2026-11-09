use super::{BufferDomain, BufferLayout, BufferPortContract};
use remotemedia_types::{RuntimeData, TensorEncoding};

impl BufferPortContract {
    /// Reject declarations that cannot identify an incoming RuntimeData payload.
    pub fn validate_definition(&self) -> Result<(), String> {
        if !self.layouts.is_empty() && (self.domains.is_empty() || self.max_payload_bytes == 0) {
            return Err("bulk port needs a memory domain and a nonzero payload bound".into());
        }
        if self.layouts.iter().any(|layout| {
            matches!(
                layout,
                BufferLayout::TensorRuntime
                    | BufferLayout::TensorPythonIpc
                    | BufferLayout::TensorProtobuf
            )
        }) {
            self.tensor_layout()?;
        }
        Ok(())
    }

    /// Check visible size, declared host layout and structural metadata without
    /// reading/copying payload elements. This does not prove allocation lineage,
    /// mapped-provider identity, producer output binding or internal copy freedom.
    /// No adaptation is implicit: an incompatible payload is an error even when
    /// the enclosing execution permits explicit counted copies.
    pub fn validate_payload(&self, data: &RuntimeData) -> Result<(), String> {
        self.validate_payload_in_domain(data, &BufferDomain::Host)
    }

    /// Validate layout and bounds against a domain already verified by the host.
    /// The caller must establish the concrete provider/transport identity first;
    /// passing a domain here does not establish that identity or create proof.
    pub fn validate_payload_in_domain(
        &self,
        data: &RuntimeData,
        verified_domain: &BufferDomain,
    ) -> Result<(), String> {
        if let Some(device) = data.device() {
            // A device owner identifies its own domain; no host domain applies.
            if !self.layouts.contains(&BufferLayout::TensorDevice) {
                return Err("undeclared payload layout TensorDevice".into());
            }
            let domain = device.domain();
            if !self.domains.contains(&BufferDomain::Device(domain.clone())) {
                return Err(format!(
                    "device tensor on {domain} needs a DeviceTransfer to this port's memory domain"
                ));
            }
            let bytes = data.size_bytes() as u64;
            if bytes > self.max_payload_bytes {
                return Err(format!(
                    "payload has {bytes} bytes; declared maximum is {}",
                    self.max_payload_bytes
                ));
            }
            return Ok(());
        }
        let Some((layout, encoding)) = self.payload_layout(data)? else {
            return Ok(());
        };
        if !self.layouts.contains(&layout) {
            return Err(format!("undeclared payload layout {layout:?}"));
        }
        if !self.domains.contains(verified_domain) {
            return Err("payload memory domain requires verified provider binding".into());
        }
        let bytes = data.bulk_payload_bytes() as u64;
        if bytes > self.max_payload_bytes {
            return Err(format!(
                "payload has {bytes} bytes; declared maximum is {}",
                self.max_payload_bytes
            ));
        }
        data.validate_shared_layout(encoding)
            .map_err(|e| e.to_string())
    }

    fn payload_layout(
        &self,
        data: &RuntimeData,
    ) -> Result<Option<(BufferLayout, TensorEncoding)>, String> {
        let layout = match data {
            RuntimeData::Audio { .. } => BufferLayout::AudioInterleavedF32,
            RuntimeData::Video { .. } => BufferLayout::VideoBytes,
            RuntimeData::Image { .. } => BufferLayout::ImageBytes,
            RuntimeData::Binary(_) => BufferLayout::Binary,
            RuntimeData::Tensor { .. } => return self.tensor_layout().map(Some),
            RuntimeData::Numpy {
                c_contiguous,
                f_contiguous,
                ..
            } => {
                if *c_contiguous && self.layouts.contains(&BufferLayout::NumpyCContiguous) {
                    BufferLayout::NumpyCContiguous
                } else if *f_contiguous && self.layouts.contains(&BufferLayout::NumpyFContiguous) {
                    BufferLayout::NumpyFContiguous
                } else {
                    return Err(
                        "NumPy layout requires an undeclared conversion/packing operation".into(),
                    );
                }
            }
            _ => return Ok(None),
        };
        Ok(Some((layout, TensorEncoding::Runtime)))
    }

    fn tensor_layout(&self) -> Result<(BufferLayout, TensorEncoding), String> {
        let mut selected = None;
        for layout in &self.layouts {
            let encoding = match layout {
                BufferLayout::TensorRuntime => TensorEncoding::Runtime,
                BufferLayout::TensorPythonIpc => TensorEncoding::PythonIpc,
                BufferLayout::TensorProtobuf => TensorEncoding::Protobuf,
                _ => continue,
            };
            if selected
                .as_ref()
                .is_some_and(|(_, prior)| *prior != encoding)
            {
                return Err(
                    "tensor convention is ambiguous; declare exactly one convention per port"
                        .into(),
                );
            }
            selected = Some((layout.clone(), encoding));
        }
        selected.ok_or_else(|| "undeclared tensor payload layout".into())
    }
}
