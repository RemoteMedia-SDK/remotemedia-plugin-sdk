//! System-based backend for Python environment management.
//!
//! Falls back to the standard library `venv` module and `pip` when `uv` is
//! not available. This backend assumes a working `python3` (or `python` on
//! Windows) is already installed on the system.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::process::Command;

use super::{EnvBackend, VenvInfo};
use remotemedia_types::{Error, Result};

/// Backend using the system Python and standard `venv` + `pip`.
///
/// This is the fallback when `uv` is not available. It is slower but
/// requires no additional tooling beyond a Python installation.
pub struct SystemBackend;

impl SystemBackend {
    /// Create a new SystemBackend.
    pub fn new() -> Self {
        Self
    }

    /// Get the platform-appropriate Python command name.
    fn python_command() -> &'static str {
        if cfg!(windows) {
            "python"
        } else {
            "python3"
        }
    }
}

impl Default for SystemBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EnvBackend for SystemBackend {
    async fn ensure_python(&self, version: &str) -> Result<PathBuf> {
        let python_cmd = Self::python_command();

        let output = Command::new(python_cmd)
            .arg("--version")
            .output()
            .await
            .map_err(|e| {
                Error::ConfigError(format!(
                    "Python not found. Ensure '{}' is installed and on PATH: {}",
                    python_cmd, e
                ))
            })?;

        if !output.status.success() {
            return Err(Error::ConfigError(format!(
                "'{}' --version failed (exit {})",
                python_cmd, output.status
            )));
        }

        let version_output = String::from_utf8_lossy(&output.stdout);
        let installed_version = version_output.trim();

        tracing::info!(
            requested = %version,
            installed = %installed_version,
            "Using system Python"
        );

        // We don't enforce exact version matching for system backend;
        // just verify python exists and log what we found.
        // The user requested version is advisory.

        // Return the command name as the "path" - Command::new will find it on PATH.
        Ok(PathBuf::from(python_cmd))
    }

    async fn create_venv(
        &self,
        python: &Path,
        cache_dir: &Path,
        cache_key: &str,
    ) -> Result<VenvInfo> {
        let venv_path = cache_dir.join(cache_key);

        // Create an isolated venv without --system-site-packages to avoid
        // inheriting broken/conflicting system packages (e.g., numba/coverage).
        // The remotemedia package is found via sys.path in the multiprocess runner.
        let output = Command::new(python)
            .args(["-m", "venv", &venv_path.to_string_lossy()])
            .output()
            .await
            .map_err(|e| {
                Error::Execution(format!(
                    "Failed to create venv at {}: {}",
                    venv_path.display(),
                    e
                ))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Execution(format!(
                "python -m venv failed (exit {}): {}",
                output.status,
                stderr.trim()
            )));
        }

        let python_executable = self.resolve_python(&VenvInfo {
            path: venv_path.clone(),
            python_executable: PathBuf::new(),
            cache_key: cache_key.to_string(),
        });

        Ok(VenvInfo {
            path: venv_path,
            python_executable,
            cache_key: cache_key.to_string(),
        })
    }

    async fn install_deps(&self, venv: &VenvInfo, deps: &[String]) -> Result<()> {
        if deps.is_empty() {
            return Ok(());
        }

        // Write a requirements file instead of passing deps as argv. pip
        // parses each line with its own tokenizer, so `-e /path/to/src`
        // round-trips correctly — whereas handing it as a single argv
        // element makes pip treat the whole "-e /path" string as one
        // requirement name and reject it with "not a valid editable
        // requirement". Mirrors the uv_backend approach.
        let req_path = venv.path.join(format!(
            "requirements-{}-{}.txt",
            std::process::id(),
            unique_suffix()
        ));
        std::fs::write(&req_path, deps.join("\n")).map_err(|e| {
            Error::Execution(format!(
                "Failed to write requirements.txt to {}: {}",
                req_path.display(),
                e
            ))
        })?;

        let args = vec![
            "-m".to_string(),
            "pip".to_string(),
            "install".to_string(),
            "-r".to_string(),
            req_path.to_string_lossy().into_owned(),
        ];

        let output = Command::new(&venv.python_executable)
            .args(&args)
            .output()
            .await
            .map_err(|e| {
                Error::Execution(format!(
                    "Failed to run pip install in {}: {}",
                    venv.path.display(),
                    e
                ))
            })?;

        let _ = std::fs::remove_file(&req_path);

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Execution(format!(
                "pip install failed (exit {}): {}",
                output.status,
                stderr.trim()
            )));
        }

        Ok(())
    }

    fn resolve_python(&self, venv: &VenvInfo) -> PathBuf {
        if cfg!(windows) {
            venv.path.join("Scripts").join("python.exe")
        } else {
            venv.path.join("bin").join("python")
        }
    }
}

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_python_command_not_empty() {
        let cmd = SystemBackend::python_command();
        assert!(!cmd.is_empty());
    }

    #[test]
    fn test_resolve_python_unix() {
        if !cfg!(windows) {
            let backend = SystemBackend::new();
            let venv = VenvInfo {
                path: PathBuf::from("/tmp/test-venv"),
                python_executable: PathBuf::new(),
                cache_key: "test".to_string(),
            };
            let resolved = backend.resolve_python(&venv);
            assert_eq!(resolved, PathBuf::from("/tmp/test-venv/bin/python"));
        }
    }

    #[test]
    fn test_default_impl() {
        let _backend = SystemBackend::default();
    }
}
