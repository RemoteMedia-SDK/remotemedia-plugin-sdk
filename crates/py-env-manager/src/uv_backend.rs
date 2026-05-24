//! `uv`-based backend for Python environment management.
//!
//! This backend uses the `uv` tool for fast virtual environment creation
//! and dependency installation. It supports multiple strategies for finding
//! or provisioning the `uv` binary.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::process::Command;

use super::{EnvBackend, VenvInfo};
use remotemedia_types::{Error, Result};

/// Backend that uses `uv` for environment management.
///
/// `uv` is significantly faster than pip for dependency resolution and
/// installation. This backend will attempt to locate or provision the
/// uv binary through several strategies.
pub struct UvBackend {
    /// Path to the `uv` binary.
    uv_path: PathBuf,
}

impl UvBackend {
    /// Create a new UvBackend by locating the `uv` binary.
    ///
    /// Detection order:
    /// 1. `UV_BINARY_PATH` environment variable
    /// 2. `uv` on the system PATH
    /// 3. `~/.config/remotemedia/bin/uv`
    /// 4. Embedded binary (if `bundled-uv-embedded` feature is enabled)
    /// 5. Download from GitHub releases (if `bundled-uv` feature is enabled)
    pub fn new() -> Result<Self> {
        // 1. Check UV_BINARY_PATH env var
        if let Ok(path) = std::env::var("UV_BINARY_PATH") {
            let path = PathBuf::from(path);
            if path.exists() {
                tracing::info!(path = %path.display(), "Found uv via UV_BINARY_PATH");
                return Ok(Self { uv_path: path });
            }
        }

        // 2. Check PATH via which-style lookup
        if let Ok(output) = std::process::Command::new("uv").arg("--version").output() {
            if output.status.success() {
                // Find the actual path
                let uv_path = which_uv().unwrap_or_else(|| PathBuf::from("uv"));
                tracing::info!(path = %uv_path.display(), "Found uv on PATH");
                return Ok(Self { uv_path });
            }
        }

        // 3. Check ~/.config/remotemedia/bin/uv
        let config_uv = default_uv_bin_path();
        if config_uv.exists() {
            tracing::info!(path = %config_uv.display(), "Found uv in config directory");
            return Ok(Self { uv_path: config_uv });
        }

        // 4. Embedded binary (feature-gated)
        #[cfg(feature = "bundled-uv-embedded")]
        {
            let dest = default_uv_bin_path();
            if let Ok(()) = extract_embedded_uv(&dest) {
                tracing::info!(path = %dest.display(), "Extracted embedded uv binary");
                return Ok(Self { uv_path: dest });
            }
        }

        // 5. Download from GitHub releases (feature-gated)
        #[cfg(feature = "bundled-uv")]
        {
            let version = option_env!("UV_VERSION").unwrap_or("0.5.0");
            let checksum = option_env!("UV_CHECKSUM").unwrap_or("");
            let dest = default_uv_bin_path();
            if let Ok(()) = download_uv(version, checksum, &dest) {
                tracing::info!(path = %dest.display(), "Downloaded uv binary");
                return Ok(Self { uv_path: dest });
            }
        }

        Err(Error::ConfigError(
            "uv not found. Install uv (https://docs.astral.sh/uv/) or set UV_BINARY_PATH, \
             or use PythonEnvMode::System to fall back to system venv+pip."
                .to_string(),
        ))
    }

    /// Run a uv command and return its stdout on success.
    ///
    /// On success OR failure, surfaces uv's stderr via tracing so that
    /// silent "install succeeded but nothing was installed" cases
    /// become visible. Progress / resolution errors from uv's pub-grub
    /// are written to stderr even when the overall exit code is 0
    /// (for example, a `uv pip install` on a no-op resolved set), and
    /// we were previously swallowing them.
    async fn run_uv(&self, args: &[&str]) -> Result<String> {
        tracing::info!(argv = ?args, "Invoking uv");
        let output = Command::new(&self.uv_path)
            .args(args)
            .output()
            .await
            .map_err(|e| {
                Error::Execution(format!(
                    "Failed to execute uv {}: {}",
                    args.first().unwrap_or(&""),
                    e
                ))
            })?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if !output.status.success() {
            return Err(Error::Execution(format!(
                "uv {} failed (exit {}):\n--- stdout ---\n{}\n--- stderr ---\n{}",
                args.join(" "),
                output.status,
                stdout.trim(),
                stderr.trim()
            )));
        }

        // Log uv's own progress output so cached / no-op installs can
        // be distinguished from real ones. uv writes resolution and
        // install progress to stderr even on success.
        if !stderr.trim().is_empty() {
            tracing::info!(uv_stderr = %stderr.trim(), "uv finished");
        }

        Ok(stdout.to_string())
    }
}

#[async_trait]
impl EnvBackend for UvBackend {
    async fn ensure_python(&self, version: &str) -> Result<PathBuf> {
        // Use `uv python install` to ensure the version is available
        self.run_uv(&["python", "install", version]).await?;

        // Find the installed python
        let output = self.run_uv(&["python", "find", version]).await?;
        let python_path = output.trim().to_string();

        if python_path.is_empty() {
            return Err(Error::Execution(format!(
                "uv python install succeeded but could not find Python {}",
                version
            )));
        }

        Ok(PathBuf::from(python_path))
    }

    async fn create_venv(
        &self,
        python: &Path,
        cache_dir: &Path,
        cache_key: &str,
    ) -> Result<VenvInfo> {
        let venv_path = cache_dir.join(cache_key);

        self.run_uv(&[
            "venv",
            "--python",
            &python.to_string_lossy(),
            &venv_path.to_string_lossy(),
        ])
        .await?;

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

        // Write requirements to a temp file
        let req_path = venv.path.join("requirements.txt");
        std::fs::write(&req_path, deps.join("\n")).map_err(|e| {
            Error::Execution(format!(
                "Failed to write requirements.txt to {}: {}",
                req_path.display(),
                e
            ))
        })?;

        self.run_uv(&[
            "pip",
            "install",
            "-r",
            &req_path.to_string_lossy(),
            "--python",
            &venv.python_executable.to_string_lossy(),
        ])
        .await?;

        // Clean up the temp requirements file
        let _ = std::fs::remove_file(&req_path);

        Ok(())
    }

    fn resolve_python(&self, venv: &VenvInfo) -> PathBuf {
        resolve_venv_python(&venv.path)
    }
}

/// Resolve the path to the Python executable inside a virtual environment.
///
/// Platform-aware: uses `bin/python` on Unix and `Scripts/python.exe` on Windows.
pub(crate) fn resolve_venv_python(venv_path: &Path) -> PathBuf {
    if cfg!(windows) {
        venv_path.join("Scripts").join("python.exe")
    } else {
        venv_path.join("bin").join("python")
    }
}

/// Try to find `uv` on the system PATH.
fn which_uv() -> Option<PathBuf> {
    let path_var = std::env::var("PATH").ok()?;
    let separator = if cfg!(windows) { ';' } else { ':' };

    for dir in path_var.split(separator) {
        let candidate = if cfg!(windows) {
            PathBuf::from(dir).join("uv.exe")
        } else {
            PathBuf::from(dir).join("uv")
        };
        if candidate.exists() {
            return Some(candidate);
        }
    }

    None
}

/// Default path for the uv binary in the config directory.
fn default_uv_bin_path() -> PathBuf {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_else(|_| "/tmp".to_string());

    let bin_name = if cfg!(windows) { "uv.exe" } else { "uv" };

    PathBuf::from(home)
        .join(".config")
        .join("remotemedia")
        .join("bin")
        .join(bin_name)
}

/// Download the uv binary from GitHub releases.
///
/// TODO: Implement using reqwest. For now this is a placeholder that always
/// returns an error. The implementation should:
/// 1. Download from `https://github.com/astral-sh/uv/releases/download/{version}/uv-{target}.tar.gz`
/// 2. Verify the SHA256 checksum matches `expected_checksum`
/// 3. Extract the binary to `dest`
/// 4. Set executable permissions on Unix
#[allow(unused_variables)]
fn download_uv(version: &str, expected_checksum: &str, dest: &Path) -> Result<()> {
    // TODO: Implement HTTP download using reqwest
    Err(Error::ConfigError(format!(
        "uv download not yet implemented (version: {}, dest: {})",
        version,
        dest.display()
    )))
}

/// Extract an embedded uv binary from `include_bytes!()`.
///
/// This is only available when the `bundled-uv-embedded` feature is enabled.
#[cfg(feature = "bundled-uv-embedded")]
#[allow(unused_variables)]
fn extract_embedded_uv(dest: &Path) -> Result<()> {
    // TODO: Implement embedded binary extraction
    // static UV_BINARY: &[u8] = include_bytes!("path/to/uv");
    Err(Error::ConfigError(
        "Embedded uv extraction not yet implemented".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_venv_python_unix() {
        if !cfg!(windows) {
            let path = resolve_venv_python(Path::new("/tmp/test-venv"));
            assert_eq!(path, PathBuf::from("/tmp/test-venv/bin/python"));
        }
    }

    #[test]
    fn test_default_uv_bin_path() {
        let path = default_uv_bin_path();
        assert!(path.to_string_lossy().contains("remotemedia"));
        assert!(path.to_string_lossy().contains("bin"));
    }

    #[test]
    fn test_download_uv_placeholder() {
        let result = download_uv("0.5.0", "abc123", Path::new("/tmp/uv"));
        assert!(result.is_err());
    }
}
