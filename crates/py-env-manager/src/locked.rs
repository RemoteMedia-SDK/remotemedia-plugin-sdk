//! Verified inputs and exclusive ownership for a locked managed environment.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;
use std::{fs, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{ProvisioningContext, VenvInfo};
use remotemedia_types::{Error, Result};

const PLAN_LIMIT: u64 = 1024 * 1024;
const WHEEL_LIMIT: usize = 2048;
const WHEEL_BYTES_LIMIT: u64 = 64 * 1024 * 1024 * 1024;

const PYTHON_TARGET: &str = r#"
import json, platform, sys, sysconfig
arch = platform.machine().lower()
if sys.platform == 'win32':
    tag = sysconfig.get_platform().lower()
    arch = 'x86' if tag == 'win32' else tag.removeprefix('win-')
arch = {'amd64': 'x86_64', 'aarch64': 'arm64'}.get(arch, arch)
print(json.dumps({'platform': sys.platform, 'arch': arch, 'version': sys.version_info[:3],
    'standard_cpython': (sys.implementation.name == 'cpython' and not getattr(sys, 'abiflags', '')
        and not sysconfig.get_config_var('Py_GIL_DISABLED') and not sysconfig.get_config_var('Py_DEBUG'))}))
"#;

/// Worker-approved wheel closure. Read from a bounded, trusted configuration
/// channel after resolving the operator policy and preparing private wheel inputs.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LockedWheelPlan {
    version: u32,
    artifact_sha256: String,
    policy_revision: String,
    target: LockedTarget,
    wheels: BTreeMap<String, WheelIdentity>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LockedTarget {
    platform: String,
    arch: String,
    python: String,
    abi: String,
    backend: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WheelIdentity {
    size: u64,
    sha256: String,
}

#[derive(Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct PythonTarget {
    platform: String,
    arch: String,
    version: [u32; 3],
    standard_cpython: bool,
}

impl LockedWheelPlan {
    pub fn read(reader: impl Read) -> Result<Self> {
        let mut bytes = Vec::new();
        reader
            .take(PLAN_LIMIT + 1)
            .read_to_end(&mut bytes)
            .map_err(crate::process::error)?;
        if bytes.len() as u64 > PLAN_LIMIT {
            return Err(invalid("Approved wheel plan exceeds its size limit"));
        }
        let plan: Self =
            serde_json::from_slice(&bytes).map_err(|_| invalid("Invalid approved wheel plan"))?;
        let mut total = 0u64;
        if plan.version != 1
            || !digest(&plan.artifact_sha256)
            || !digest(&plan.policy_revision)
            || plan.wheels.is_empty()
            || plan.wheels.len() > WHEEL_LIMIT
            || !plan.wheels.iter().all(|(name, wheel)| {
                total = total.saturating_add(wheel.size);
                wheel_name(name)
                    && digest(&wheel.sha256)
                    && wheel.size > 0
                    && total <= WHEEL_BYTES_LIMIT
            })
        {
            return Err(invalid("Invalid approved wheel identities"));
        }
        Ok(plan)
    }

    pub(crate) fn requirements(&self, root: &Path) -> Result<String> {
        if !plain_entry(
            &fs::symlink_metadata(root).map_err(crate::process::error)?,
            true,
        ) {
            return Err(invalid("Expected a private approved wheel directory"));
        }
        let mut found = 0;
        for entry in fs::read_dir(root).map_err(crate::process::error)? {
            let entry = entry.map_err(crate::process::error)?;
            let name = entry.file_name();
            let expected = name
                .to_str()
                .and_then(|name| self.wheels.get(name))
                .ok_or_else(|| invalid("Wheel directory differs from its approved inventory"))?;
            let actual = fs::symlink_metadata(entry.path()).map_err(crate::process::error)?;
            if !plain_entry(&actual, false) || actual.len() != expected.size {
                return Err(invalid("Approved wheel changed or is not a regular file"));
            }
            found += 1;
        }
        if found != self.wheels.len() {
            return Err(invalid("Approved wheel directory is incomplete"));
        }
        let mut requirements = String::new();
        for (name, wheel) in &self.wheels {
            let uri = url::Url::from_file_path(root.join(name))
                .map_err(|_| invalid("Cannot address the approved wheel as a local file"))?;
            use std::fmt::Write;
            writeln!(requirements, "{uri} --hash=sha256:{}", wheel.sha256)
                .expect("write to String");
        }
        Ok(requirements)
    }

    pub(crate) fn bind_interpreter(&self, python: &Path, actual: &PythonTarget) -> Result<String> {
        let [major, minor, patch] = actual.version;
        let target = &self.target;
        if !actual.standard_cpython
            || major != 3
            || target.platform != actual.platform
            || target.arch != actual.arch
            || target.abi != format!("cp{major}{minor}")
            || (target.python != format!("{major}.{minor}")
                && target.python != format!("{major}.{minor}.{patch}"))
        {
            return Err(invalid(
                "Trusted Python does not match the approved target, version, or ABI",
            ));
        }
        let canonical_python = python.canonicalize().map_err(crate::process::error)?;
        let identity = serde_json::to_vec(&(self, canonical_python, actual))
            .map_err(|_| invalid("Cannot encode the approved environment identity"))?;
        Ok(hex::encode(Sha256::digest(identity)))
    }
}

pub(crate) async fn probe_python(
    python: &Path,
    root: &Path,
    context: &ProvisioningContext,
) -> Result<PythonTarget> {
    let mut launch = context
        .locked_command(python, root)
        .map_err(crate::process::error)?;
    launch.command_mut().args(["-I", "-S", "-c", PYTHON_TARGET]);
    let output = tokio::time::timeout(Duration::from_secs(15), launch.output())
        .await
        .map_err(|_| invalid("Trusted Python target probe timed out"))?
        .map_err(crate::process::error)?;
    if !output.status.success() {
        return Err(invalid("Trusted Python target probe failed"));
    }
    serde_json::from_slice(&output.stdout).map_err(|_| invalid("Invalid trusted Python target"))
}

/// An installation never participates in the shared venv cache. Dropping its
/// owner removes the entire installation and private scratch after subprocesses exit.
pub struct RunPrivateVenv {
    pub(crate) directory: Arc<tempfile::TempDir>,
    pub(crate) info: VenvInfo,
}

impl RunPrivateVenv {
    pub fn info(&self) -> &VenvInfo {
        &self.info
    }

    /// Transfer cleanup to the trusted caller that owns the parent directory.
    pub fn persist(self) -> Result<VenvInfo> {
        let _ = Arc::try_unwrap(self.directory)
            .map_err(|_| invalid("Installer ownership could not be transferred"))?
            .keep();
        Ok(self.info)
    }
}

pub(crate) fn absolute_regular_program(path: &Path) -> Result<()> {
    if !path.is_absolute() || !path.is_file() {
        return Err(invalid(
            "Locked provisioning requires an absolute trusted executable",
        ));
    }
    Ok(())
}

fn plain_entry(metadata: &fs::Metadata, directory: bool) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return false;
        }
    }
    if directory {
        metadata.is_dir()
    } else {
        metadata.is_file()
    }
}

fn wheel_name(name: &str) -> bool {
    name.strip_suffix(".whl").is_some_and(|stem| {
        (1..=220).contains(&stem.len())
            && stem.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'+')
            })
    })
}

fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) fn invalid(message: &str) -> Error {
    Error::ConfigError(message.into())
}
