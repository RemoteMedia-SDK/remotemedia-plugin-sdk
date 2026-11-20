use std::fs::{self, File};
use std::io::Read;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use serde_json::{json, Value};
use tokio::process::Command;

use super::exclusive_device::canonical_gpu_uuid;
use super::{CONTROL_TIMEOUT, GUEST_UID};
use crate::SandboxError;

const VERIFIER: &str = "/opt/remotemedia/remotemedia-cuda-verify";
const NVIDIA_MAJOR: u32 = 195;
const CONTROL_MINOR: u32 = 255;

/// Operator-owned inputs for a native Linux CUDA sandbox.
#[derive(Clone, Debug)]
pub struct RunscCudaConfig {
    pub registry: PathBuf,
    pub device_uuid: String,
    pub driver_version: String,
}

#[derive(PartialEq, Eq)]
pub(super) struct CudaDevice {
    uuid: String,
    driver_version: String,
    minor: u32,
    uvm_major: u32,
}

impl CudaDevice {
    pub(super) fn discover(
        uuid: &str,
        driver_version: &str,
        rootfs: &Path,
    ) -> Result<Self, SandboxError> {
        if !canonical_gpu_uuid(uuid) || !driver_version_is_valid(driver_version) {
            return Err(SandboxError::Invalid(
                "a physical GPU UUID and exact driver version are required",
            ));
        }
        let version = read_kernel_text(Path::new("/sys/module/nvidia/version"))
            .map_err(|_| SandboxError::Invalid("native NVIDIA kernel driver is unavailable"))?;
        if version.trim() != driver_version {
            return Err(SandboxError::Invalid(
                "loaded NVIDIA driver differs from the approved version",
            ));
        }
        let inventory = fs::read_dir("/proc/driver/nvidia/gpus")
            .map_err(|_| SandboxError::Invalid("native NVIDIA device inventory is unavailable"))?;
        let mut matching_minor = None;
        for entry in inventory {
            let information = read_kernel_text(&entry?.path().join("information"))?;
            if let Some(minor) = matching_gpu_minor(&information, uuid)? {
                if matching_minor.replace(minor).is_some() {
                    return Err(SandboxError::Invalid("NVIDIA device UUID is ambiguous"));
                }
            }
        }
        let minor =
            matching_minor.ok_or(SandboxError::Invalid("assigned NVIDIA GPU is unavailable"))?;
        let devices = read_kernel_text(Path::new("/proc/devices"))?;
        let uvm_major = uvm_device_major(&devices)?;
        let device = Self {
            uuid: uuid.into(),
            driver_version: driver_version.into(),
            minor,
            uvm_major,
        };
        for (path, major, minor) in device.nodes() {
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.file_type().is_char_device()
                || libc::major(metadata.rdev()) != major
                || libc::minor(metadata.rdev()) != minor
            {
                return Err(SandboxError::Invalid(
                    "NVIDIA character device identity does not match the kernel inventory",
                ));
            }
        }
        let verifier = fs::canonicalize(rootfs.join(VERIFIER.trim_start_matches('/')))?;
        if !verifier.starts_with(rootfs)
            || !fs::metadata(verifier).is_ok_and(|m| m.is_file() && m.mode() & 0o111 != 0)
        {
            return Err(SandboxError::Invalid(
                "trusted CUDA verifier is unavailable in the guest image",
            ));
        }
        Ok(device)
    }

    pub(super) fn recheck(&self, rootfs: &Path) -> Result<(), SandboxError> {
        if Self::discover(&self.uuid, &self.driver_version, rootfs)? != *self {
            return Err(SandboxError::Invalid(
                "assigned GPU changed while the provider was open",
            ));
        }
        Ok(())
    }

    pub(super) async fn require_supported_driver(&self, runsc: &Path) -> Result<(), SandboxError> {
        let output = tokio::time::timeout(
            CONTROL_TIMEOUT,
            Command::new(runsc)
                .args(["nvproxy", "list-supported-drivers"])
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .stdin(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .output(),
        )
        .await
        .map_err(|_| SandboxError::Timeout("CUDA driver compatibility"))??;
        if !output.status.success()
            || !std::str::from_utf8(&output.stdout).is_ok_and(|versions| {
                versions
                    .lines()
                    .any(|version| version.trim() == self.driver_version)
            })
        {
            return Err(SandboxError::Invalid(
                "pinned runsc does not support the loaded NVIDIA driver version",
            ));
        }
        Ok(())
    }

    pub(super) fn launch_argv(&self, workload: &[String]) -> Vec<String> {
        let mut argv = vec![VERIFIER.into(), self.uuid.clone(), self.minor.to_string()];
        argv.extend_from_slice(workload);
        argv
    }

    pub(super) fn configure_spec(&self, linux: &mut Value) {
        let nodes = self.nodes();
        linux["devices"] = Value::Array(
            nodes
                .iter()
                .map(|(path, major, minor)| {
                    json!({
                        "path":path,"type":"c","major":major,"minor":minor,
                        "fileMode":0o600,"uid":GUEST_UID,"gid":GUEST_UID,
                    })
                })
                .collect(),
        );
        let mut access = vec![
            json!({"allow":false,"type":"c","major":NVIDIA_MAJOR,"access":"rwm"}),
            json!({"allow":false,"type":"c","major":self.uvm_major,"access":"rwm"}),
        ];
        access.extend(nodes.into_iter().map(|(_, major, minor)| {
            json!({
                "allow":true,"type":"c","major":major,"minor":minor,"access":"rwm",
            })
        }));
        linux["resources"]["devices"] = Value::Array(access);
    }

    fn nodes(&self) -> [(String, u32, u32); 3] {
        [
            (
                format!("/dev/nvidia{}", self.minor),
                NVIDIA_MAJOR,
                self.minor,
            ),
            ("/dev/nvidiactl".into(), NVIDIA_MAJOR, CONTROL_MINOR),
            ("/dev/nvidia-uvm".into(), self.uvm_major, 0),
        ]
    }
}

fn read_kernel_text(path: &Path) -> Result<String, SandboxError> {
    let mut data = String::new();
    File::open(path)?
        .take(64 * 1024 + 1)
        .read_to_string(&mut data)?;
    if data.len() > 64 * 1024 {
        return Err(SandboxError::Invalid(
            "kernel NVIDIA inventory exceeds its size limit",
        ));
    }
    Ok(data)
}

fn matching_gpu_minor(information: &str, expected: &str) -> Result<Option<u32>, SandboxError> {
    let mut fields = information.lines().filter_map(|line| line.split_once(':'));
    if !fields
        .clone()
        .any(|(key, value)| key == "GPU UUID" && value.trim() == expected)
    {
        return Ok(None);
    }
    let minor = fields
        .find(|(key, _)| *key == "Device Minor")
        .and_then(|(_, value)| value.trim().parse::<u32>().ok())
        .filter(|minor| *minor < CONTROL_MINOR)
        .ok_or(SandboxError::Invalid(
            "assigned NVIDIA GPU has an invalid device minor",
        ))?;
    Ok(Some(minor))
}

fn uvm_device_major(devices: &str) -> Result<u32, SandboxError> {
    devices
        .lines()
        .filter_map(|line| line.trim().split_once(char::is_whitespace))
        .find(|(_, name)| name.trim() == "nvidia-uvm")
        .and_then(|(major, _)| major.parse::<u32>().ok())
        .filter(|major| *major > 0 && *major != NVIDIA_MAJOR)
        .ok_or(SandboxError::Invalid(
            "native NVIDIA UVM driver is unavailable",
        ))
}

fn driver_version_is_valid(version: &str) -> bool {
    version.len() <= 16
        && version.split('.').count() == 3
        && version
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const UUID: &str = "GPU-11111111-1111-1111-1111-111111111111";

    #[test]
    fn maps_uuid_and_checks_exact_native_driver_inputs() {
        let info = format!("Model: NVIDIA Test GPU\nGPU UUID: \t{UUID}\nDevice Minor:\t7\n");
        assert_eq!(matching_gpu_minor(&info, UUID).unwrap(), Some(7));
        assert_eq!(matching_gpu_minor(&info, "GPU-other").unwrap(), None);
        assert!(matching_gpu_minor(&info.replace("\t7", "\t255"), UUID).is_err());
        assert_eq!(
            uvm_device_major("Character devices:\n 195 nvidia\n 510 nvidia-uvm\n").unwrap(),
            510
        );
        assert!(uvm_device_major("195 nvidia-uvm\n").is_err());
        assert!(driver_version_is_valid("580.126.20"));
        assert!(!driver_version_is_valid("580.126.20 --allow-unsupported"));
    }

    #[test]
    fn cuda_oci_projection_allows_only_the_assigned_device_and_required_control_nodes() {
        let cuda = CudaDevice {
            uuid: UUID.into(),
            driver_version: "580.126.20".into(),
            minor: 7,
            uvm_major: 510,
        };
        let mut linux = json!({"resources":{"pids":{"limit":32}}});
        cuda.configure_spec(&mut linux);
        assert_eq!(
            linux["devices"]
                .as_array()
                .unwrap()
                .iter()
                .map(|device| device["path"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["/dev/nvidia7", "/dev/nvidiactl", "/dev/nvidia-uvm"]
        );
        let allowed = linux["resources"]["devices"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|rule| rule["allow"] == true)
            .map(|rule| (rule["major"].as_u64(), rule["minor"].as_u64()))
            .collect::<Vec<_>>();
        assert_eq!(
            allowed,
            [
                (Some(195), Some(7)),
                (Some(195), Some(255)),
                (Some(510), Some(0))
            ]
        );
        assert_eq!(linux["resources"]["pids"]["limit"], 32);
        assert_eq!(
            cuda.launch_argv(&["/probe".into(), "serve".into()]),
            [VERIFIER, UUID, "7", "/probe", "serve"]
        );
    }
}
