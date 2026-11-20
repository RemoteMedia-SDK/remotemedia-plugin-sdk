use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use super::RunscConfig;
use crate::SandboxError;

const BINDING: &str = "binding.json";
const MAX_BINDING_BYTES: u64 = 16 * 1024;

pub(super) struct ExclusiveDevice {
    directory: PathBuf,
    lock: File,
    binding: Binding,
    pub(super) reservations: Mutex<()>,
}

#[derive(Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    version: u32,
    boot_id: String,
    state: BoundDirectory,
    cgroup: BoundDirectory,
}

#[derive(Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
struct BoundDirectory {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl BoundDirectory {
    fn identify(path: &Path) -> Result<Self, SandboxError> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_dir() {
            return Err(SandboxError::DeviceRecoveryRequired);
        }
        Ok(Self {
            path: path.to_owned(),
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

impl ExclusiveDevice {
    pub(super) fn open(
        registry: &Path,
        uuid: &str,
        config: &RunscConfig,
    ) -> Result<Self, SandboxError> {
        if !registry.is_absolute() || !canonical_gpu_uuid(uuid) {
            return Err(SandboxError::Invalid(
                "an absolute registry and physical GPU UUID are required",
            ));
        }
        let registry_file = private_directory(registry)?;
        let registry = fs::canonicalize(registry)?;
        if [&config.state, &config.rootfs]
            .iter()
            .any(|path| registry.starts_with(path) || path.starts_with(&registry))
        {
            return Err(SandboxError::Invalid(
                "device registry must be separate from provider state and guest rootfs",
            ));
        }
        let directory = registry.join(uuid);
        match fs::DirBuilder::new().mode(0o700).create(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        let lock = private_directory(&directory)?;
        if let Err(error) = fs2::FileExt::try_lock_exclusive(&lock) {
            if error.raw_os_error() == fs2::lock_contended_error().raw_os_error() {
                return Err(SandboxError::DeviceBusy);
            }
            return Err(error.into());
        }

        let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        let boot_id = boot_id.trim();
        if !canonical_uuid(boot_id) {
            return Err(SandboxError::Invalid("host boot ID is unavailable"));
        }
        let expected = Binding {
            version: 1,
            boot_id: boot_id.to_owned(),
            state: BoundDirectory::identify(&config.state)?,
            cgroup: BoundDirectory::identify(&config.delegated_cgroup)?,
        };
        let binding = directory.join(BINDING);
        let actual = read_binding(&binding)?;
        if let Some(actual) = &actual {
            // An old boot cannot still own a live process. Reconciliation can
            // refresh inode identities only at the original configured paths.
            if actual.version != expected.version
                || !canonical_uuid(&actual.boot_id)
                || actual.state.path != expected.state.path
                || actual.cgroup.path != expected.cgroup.path
                || (actual.boot_id == expected.boot_id && actual != &expected)
            {
                return Err(SandboxError::DeviceRecoveryRequired);
            }
        }
        if actual.as_ref() != Some(&expected) {
            let mut temporary = tempfile::NamedTempFile::new_in(&directory)?;
            let bytes = serde_json::to_vec(&expected)?;
            if bytes.len() as u64 > MAX_BINDING_BYTES {
                return Err(SandboxError::Invalid("device binding is too large"));
            }
            temporary.write_all(&bytes)?;
            temporary.as_file().sync_all()?;
            temporary.persist(&binding).map_err(|error| error.error)?;
        }
        lock.sync_all()?;
        registry_file.sync_all()?;
        Ok(Self {
            directory,
            lock,
            binding: expected,
            reservations: Mutex::new(()),
        })
    }

    pub(super) fn verify_scope(&self) -> Result<(), SandboxError> {
        for entry in [&self.binding.state, &self.binding.cgroup] {
            if BoundDirectory::identify(&entry.path)? != *entry {
                return Err(SandboxError::DeviceRecoveryRequired);
            }
        }
        Ok(())
    }

    pub(super) fn release(&self) -> Result<(), SandboxError> {
        self.verify_scope()?;
        // Keep the directory inode and its lock stable for all contenders.
        fs::remove_file(self.directory.join(BINDING))?;
        self.lock.sync_all()?;
        Ok(())
    }
}

fn private_directory(path: &Path) -> Result<File, SandboxError> {
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = directory.metadata()?;
    if metadata.mode() & 0o077 != 0 || metadata.uid() != unsafe { libc::geteuid() } {
        return Err(SandboxError::Invalid(
            "device registry must be private to the provider account",
        ));
    }
    Ok(directory)
}

fn read_binding(path: &Path) -> Result<Option<Binding>, SandboxError> {
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(SandboxError::DeviceRecoveryRequired);
    }
    let mut bytes = Vec::new();
    file.take(MAX_BINDING_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BINDING_BYTES {
        return Err(SandboxError::DeviceRecoveryRequired);
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| SandboxError::DeviceRecoveryRequired)
}

fn canonical_gpu_uuid(uuid: &str) -> bool {
    uuid.strip_prefix("GPU-").is_some_and(canonical_uuid)
}

fn canonical_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => matches!(byte, b'0'..=b'9' | b'a'..=b'f'),
        })
        && value != "00000000-0000-0000-0000-000000000000"
}
