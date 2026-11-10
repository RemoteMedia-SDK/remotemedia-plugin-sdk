//! Relocatable, host-selected runtime artifacts. Hashes detect replacement;
//! they do not authorize code or replace the factory's trust contract.
use super::{provisioning::file_digest, MappedPythonWheels};
use remotemedia_types::{Error, Result};
use serde::Deserialize;
use std::path::{Component, Path, PathBuf};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Artifact {
    path: PathBuf,
    sha256: String,
}

impl Artifact {
    fn resolve(&self, root: &Path) -> Result<PathBuf> {
        if self.path.as_os_str().is_empty()
            || self
                .path
                .components()
                .any(|part| !matches!(part, Component::Normal(_)))
        {
            return Err(Error::ConfigError(
                "bundle artifact requires a relative contained path".into(),
            ));
        }
        let path = root
            .join(&self.path)
            .canonicalize()
            .map_err(|e| Error::ConfigError(format!("bundle artifact: {e}")))?;
        if !path.starts_with(root) || !path.is_file() {
            return Err(Error::ConfigError(
                "bundle artifact escapes its directory or is not a file".into(),
            ));
        }
        if file_digest(&path)? != self.sha256 {
            return Err(Error::ConfigError(format!(
                "bundle artifact hash mismatch: {}",
                self.path.display()
            )));
        }
        Ok(path)
    }

    fn wheel(&self, root: &Path, distribution: &str) -> Result<PathBuf> {
        let path = self.resolve(root)?;
        // Every later launch must install exactly these verified bytes.
        super::provisioning::pin_wheel(&path, &self.sha256);
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if !name.starts_with(distribution)
            || path.extension().and_then(|e| e.to_str()) != Some("whl")
        {
            return Err(Error::ConfigError(format!(
                "bundle requires a {distribution} wheel"
            )));
        }
        Ok(path)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: u32,
    os: String,
    arch: String,
    runner_protocol: u32,
    client: Artifact,
    buffers: Artifact,
    supervisor: Option<Artifact>,
    supervisor_protocol: Option<u32>,
}

/// Matching artifacts loaded from `mapped-python-runtime.json` inside a bundle.
/// Installed-wheel and live protocol verification still runs before node launch.
#[derive(Clone, Debug)]
pub struct MappedPythonBundle {
    pub wheels: MappedPythonWheels,
    pub supervisor: Option<PathBuf>,
}

impl MappedPythonBundle {
    /// Validate a host-selected bundle. The caller remains responsible for keeping
    /// these files trusted and unchanged throughout runtime use.
    pub fn load(directory: &Path) -> Result<Self> {
        let root = directory
            .canonicalize()
            .map_err(|e| Error::ConfigError(format!("mapped Python bundle: {e}")))?;
        let bytes = std::fs::read(root.join("mapped-python-runtime.json"))
            .map_err(|e| Error::ConfigError(format!("mapped Python bundle manifest: {e}")))?;
        let manifest: Manifest = serde_json::from_slice(&bytes)
            .map_err(|e| Error::ConfigError(format!("mapped Python bundle manifest: {e}")))?;
        if manifest.format != 1
            || manifest.runner_protocol != 3
            || manifest.os != std::env::consts::OS
            || manifest.arch != std::env::consts::ARCH
        {
            return Err(Error::ConfigError(
                "unsupported mapped Python bundle format, protocol or target".into(),
            ));
        }
        let supervisor = manifest.resolve_supervisor(&root)?;
        Ok(Self {
            wheels: MappedPythonWheels {
                client: manifest.client.wheel(&root, "remotemedia_client-")?,
                buffers: manifest.buffers.wheel(&root, "remotemedia_buffers-")?,
            },
            supervisor,
        })
    }
}

impl Manifest {
    fn resolve_supervisor(&self, root: &Path) -> Result<Option<PathBuf>> {
        if cfg!(all(
            target_os = "linux",
            feature = "linux-worker-supervisor"
        )) {
            if self.supervisor_protocol != Some(2) {
                return Err(Error::ConfigError(
                    "Linux bundle requires supervisor protocol 2".into(),
                ));
            }
            let artifact = self.supervisor.as_ref().ok_or_else(|| {
                Error::ConfigError("Linux bundle is missing its supervisor".into())
            })?;
            Ok(Some(artifact.resolve(root)?))
        } else if cfg!(windows) && self.supervisor.is_none() && self.supervisor_protocol.is_none() {
            Ok(None)
        } else {
            Err(Error::ConfigError("bundle requires Windows or Linux with linux-worker-supervisor; unexpected supervisor artifacts are rejected".into()))
        }
    }
}
