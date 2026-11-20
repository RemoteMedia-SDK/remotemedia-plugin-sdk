use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use super::{validate_request, BuildOutput, RunscProvider, RunscReservation};
use crate::{SandboxError, WorkloadLimits, WorkloadPhase, WorkloadRequest, WorkloadTarget};

const LAUNCHER: &str = "/opt/remotemedia/remotemedia-space-launch";

/// Worker-owned identities for an offline source distribution and its wheel.
/// Inputs are relative to an operator-selected directory with `source` and
/// `wheels` subdirectories. A request or repository cannot supply this policy.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovedWheelBuild {
    pub version: u32,
    pub python_abi: String,
    pub source: BuildFile,
    pub build_wheels: BTreeMap<String, BuildFileIdentity>,
    pub output: BuildFile,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildFile {
    pub filename: String,
    pub identity: BuildFileIdentity,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildFileIdentity {
    pub size: u64,
    pub sha256: String,
}

impl RunscProvider {
    /// Stage the operator-approved closure and build one pinned wheel in a
    /// fresh CPU sandbox. The returned file has passed the output check and
    /// can be supplied to the trusted artifact publisher.
    pub async fn build_python_wheel(
        &self,
        inputs: &Path,
        plan: &ApprovedWheelBuild,
        limits: WorkloadLimits,
        cancelled: impl Future<Output = ()>,
    ) -> Result<File, SandboxError> {
        let request = WorkloadRequest {
            target: WorkloadTarget::LinuxCpu,
            phase: WorkloadPhase::Build,
            limits,
            argv: vec![
                LAUNCHER.into(),
                "build".into(),
                plan.source.filename.clone(),
                plan.output.filename.clone(),
                plan.output.identity.size.to_string(),
            ],
        };
        validate_request(&request, WorkloadTarget::LinuxCpu)?;
        let output = plan.validate(limits.scratch_bytes)?;
        self.check_ready(Path::new(LAUNCHER)).await?;
        let reservation = self.reserve()?;
        let inputs = inputs.to_owned();
        let plan = plan.clone();
        let mut stage = tokio::task::spawn_blocking(move || {
            plan.stage(&inputs, &reservation)?;
            Ok::<_, SandboxError>(reservation)
        });
        tokio::pin!(cancelled);
        let reservation = tokio::select! {
            biased;
            _ = &mut cancelled => {
                // Let the bounded copy finish so the reservation is removed
                // before a cancelled caller exits its worker process.
                drop(stage.await);
                return Err(SandboxError::Runtime("build cancelled"));
            }
            result = &mut stage => result.map_err(std::io::Error::other)??,
        };
        reservation.build_until(request, output, cancelled).await
    }
}

impl ApprovedWheelBuild {
    fn validate(&self, scratch_bytes: u64) -> Result<BuildOutput, SandboxError> {
        let abi = self.python_abi.strip_prefix("cp").unwrap_or_default();
        if self.version != 1
            || !(2..=3).contains(&abi.len())
            || !abi.bytes().all(|byte| byte.is_ascii_digit())
            || !basename(&self.source.filename, ".tar.gz", 213)
            || !self.source.filename.as_bytes()[0].is_ascii_alphanumeric()
            || !basename(&self.output.filename, ".whl", 220)
            || self.build_wheels.len() > 2048
            || self
                .build_wheels
                .keys()
                .any(|name| !basename(name, ".whl", 220))
        {
            return Err(SandboxError::Invalid(
                "unsupported approved Python build plan",
            ));
        }
        let inputs = std::iter::once(&self.source.identity).chain(self.build_wheels.values());
        let mut total = 0u64;
        for identity in inputs {
            identity.digest()?;
            total = total
                .checked_add(identity.size)
                .filter(|size| *size <= scratch_bytes)
                .ok_or(SandboxError::Invalid(
                    "build inputs exceed the scratch limit",
                ))?;
        }
        let output = BuildOutput {
            size: self.output.identity.size,
            sha256: self.output.identity.digest()?,
        };
        if output.size > scratch_bytes {
            return Err(SandboxError::Invalid(
                "build output exceeds the scratch limit",
            ));
        }
        Ok(output)
    }

    fn stage(&self, offered: &Path, reservation: &RunscReservation) -> Result<(), SandboxError> {
        let root = reservation.input_root();
        let source = root.join("source");
        let wheels = root.join("wheels");
        for path in [&source, &wheels] {
            fs::create_dir(path)?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
        }
        self.source.identity.copy_to(
            &offered.join("source").join(&self.source.filename),
            &source.join(&self.source.filename),
        )?;
        let mut inventory = BTreeMap::new();
        for (name, identity) in &self.build_wheels {
            identity.copy_to(&offered.join("wheels").join(name), &wheels.join(name))?;
            inventory.insert(name, &identity.sha256);
        }
        let file = File::create_new(wheels.join("approved-wheels.json"))?;
        serde_json::to_writer(
            &file,
            &json!({"version": 1, "python_abi": self.python_abi, "wheels": inventory}),
        )?;
        file.set_permissions(fs::Permissions::from_mode(0o444))?;
        Ok(())
    }
}

impl BuildFileIdentity {
    fn digest(&self) -> Result<[u8; 32], SandboxError> {
        let invalid = SandboxError::Invalid("invalid approved build file identity");
        if self.size == 0
            || self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(invalid);
        }
        let mut digest = [0; 32];
        hex::decode_to_slice(&self.sha256, &mut digest).map_err(|_| invalid)?;
        Ok(digest)
    }

    fn copy_to(&self, input: &Path, output: &Path) -> Result<(), SandboxError> {
        let expected = self.digest()?;
        let changed =
            || SandboxError::Invalid("offered build input differs from its approved identity");
        let opened = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(input)?;
        let metadata = opened.metadata()?;
        if !metadata.is_file() || metadata.len() != self.size {
            return Err(changed());
        }
        let mut input = opened.take(self.size + 1);
        let mut output = File::create_new(output)?;
        let mut digest = Sha256::new();
        let mut buffer = [0; 16 * 1024];
        let mut written = 0;
        loop {
            let size = input.read(&mut buffer)?;
            if size == 0 {
                break;
            }
            written += size as u64;
            if written > self.size {
                return Err(changed());
            }
            digest.update(&buffer[..size]);
            output.write_all(&buffer[..size])?;
        }
        let actual: [u8; 32] = digest.finalize().into();
        if written != self.size || actual != expected {
            return Err(changed());
        }
        output.set_permissions(fs::Permissions::from_mode(0o444))?;
        Ok(())
    }
}

fn basename(name: &str, suffix: &str, max_stem: usize) -> bool {
    name.strip_suffix(suffix).is_some_and(|stem| {
        !stem.is_empty()
            && stem.len() <= max_stem
            && stem.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(byte, b'_' | b'.' | b'-')
                    || (suffix == ".whl" && byte == b'+')
            })
    })
}
