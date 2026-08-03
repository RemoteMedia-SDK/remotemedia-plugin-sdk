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
            // `cargo:rustc-env` emitted by remotemedia-sdk-base's build script
            // is crate-local and therefore is not visible while this dependency
            // is compiled. Keep the fallback aligned with that crate's pinned
            // release; uv 0.5.0 can attempt Python-3.9-only llvmlite candidates
            // while resolving liquid-audio on Python 3.12.
            let version = option_env!("UV_VERSION").unwrap_or("0.6.14");
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

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
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

// ---------------------------------------------------------------------------
// Platform -> uv target triple mapping
// ---------------------------------------------------------------------------

/// Map the current platform to a uv release archive name.
///
/// Returns the platform-specific suffix used in uv GitHub release downloads,
/// for example `x86_64-unknown-linux-gnu` or `aarch64-apple-darwin`.
fn uv_platform_target() -> Option<&'static str> {
    let arch = std::env::consts::ARCH;
    let os = std::env::consts::OS;

    match (os, arch) {
        ("linux", "x86_64") => Some("x86_64-unknown-linux-gnu"),
        ("linux", "aarch64") => Some("aarch64-unknown-linux-gnu"),
        ("macos", "x86_64") => Some("x86_64-apple-darwin"),
        ("macos", "aarch64") => Some("aarch64-apple-darwin"),
        ("windows", "x86_64") => Some("x86_64-pc-windows-msvc"),
        ("windows", "aarch64") => Some("aarch64-pc-windows-msvc"),
        _ => None,
    }
}

/// Return `true` if the current platform is Windows (uses `.zip` archives).
fn is_windows_target() -> bool {
    std::env::consts::OS == "windows"
}

/// Download the uv binary from GitHub releases.
///
/// Downloads the uv release `.tar.gz` (Unix) or `.zip` (Windows) archive
/// from the official GitHub releases endpoint, verifies the SHA256 checksum
/// (if `expected_checksum` is non-empty), extracts the single `uv` binary,
/// and writes it to `dest`.
///
/// On Unix, sets the executable bit (`0o755`) on the extracted binary.
fn download_uv(version: &str, expected_checksum: &str, dest: &Path) -> Result<()> {
    let target = uv_platform_target().ok_or_else(|| {
        Error::ConfigError(format!(
            "Unsupported platform for uv download: {} / {}",
            std::env::consts::ARCH,
            std::env::consts::OS
        ))
    })?;

    let extension = if is_windows_target() { "zip" } else { "tar.gz" };
    let url = format!(
        "https://github.com/astral-sh/uv/releases/download/{version}/uv-{target}.{extension}",
        version = version,
        target = target,
        extension = extension
    );

    tracing::info!(%url, "Downloading uv binary");

    // --- Download the archive ---
    let response = reqwest::blocking::get(&url).map_err(|e| {
        Error::ConfigError(format!("Failed to download uv from {}: {}", url, e))
    })?;

    let status = response.status();
    if !status.is_success() {
        return Err(Error::ConfigError(format!(
            "Failed to download uv from {}: HTTP {}",
            url, status
        )));
    }

    let archive_bytes = response.bytes().map_err(|e| {
        Error::ConfigError(format!("Failed to read response body from {}: {}", url, e))
    })?;

    // --- SHA256 verification ---
    if !expected_checksum.is_empty() {
        let actual = sha2_hex(&archive_bytes);
        if actual != expected_checksum {
            return Err(Error::ConfigError(format!(
                "SHA256 mismatch for uv binary download from {}.\n  Expected: {}\n  Actual:   {}",
                url, expected_checksum, actual
            )));
        }
        tracing::info!("SHA256 checksum verified for uv binary");
    }

    // --- Extract and write the binary ---
    let uv_binary_name = if cfg!(windows) { "uv.exe" } else { "uv" };

    if is_windows_target() {
        extract_from_zip(&archive_bytes, uv_binary_name, dest)?;
    } else {
        extract_from_tar_gz(&archive_bytes, uv_binary_name, dest)?;
    }

    // Set executable permission on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dest, std::fs::Permissions::from_mode(0o755)).map_err(|e| {
            Error::ConfigError(format!(
                "Failed to set executable permission on {}: {}",
                dest.display(),
                e
            ))
        })?;
    }

    tracing::info!(
        path = %dest.display(),
        size = %human_readable_size(dest),
        "uv binary ready"
    );

    Ok(())
}

/// Compute the lowercase hex SHA256 of a byte slice.
fn sha2_hex(data: &[u8]) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Human-readable size for a Path (reads from disk).
fn human_readable_size(path: &Path) -> String {
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    human_readable_size_bytes(size as usize)
}

/// Human-readable size from a byte count.
fn human_readable_size_bytes(size: usize) -> String {
    if size < 1024 {
        format!("{} B", size)
    } else if size < 1024 * 1024 {
        format!("{:.1} KB", size as f64 / 1024.0)
    } else {
        format!("{:.1} MB", size as f64 / (1024.0 * 1024.0))
    }
}

/// Extract a single file from a `.tar.gz` archive.
fn extract_from_tar_gz(archive: &[u8], filename: &str, dest: &Path) -> Result<()> {
    let decoder = flate2::read::GzDecoder::new(archive);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries().map_err(|e| {
        Error::ConfigError(format!("Failed to read tar archive: {}", e))
    })? {
        let mut entry = entry.map_err(|e| {
            Error::ConfigError(format!("Failed to read tar entry: {}", e))
        })?;

        let entry_path = entry.path().map_err(|e| {
            Error::ConfigError(format!("Failed to read entry path: {}", e))
        })?;

        if entry_path.file_name().and_then(|n| n.to_str()) == Some(filename) {
            // Ensure parent directory exists
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    Error::ConfigError(format!(
                        "Failed to create parent directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }

            let mut out = std::fs::File::create(dest).map_err(|e| {
                Error::ConfigError(format!("Failed to create {}: {}", dest.display(), e))
            })?;

            std::io::copy(&mut entry, &mut out).map_err(|e| {
                Error::ConfigError(format!("Failed to write {}: {}", dest.display(), e))
            })?;

            tracing::info!("Extracted {} from tar archive", filename);
            return Ok(());
        }
    }

    Err(Error::ConfigError(format!(
        "Could not find '{}' in tar archive",
        filename
    )))
}

/// Extract a single file from a `.zip` archive.
fn extract_from_zip(archive: &[u8], filename: &str, dest: &Path) -> Result<()> {
    let cursor = std::io::Cursor::new(archive);
    let mut zip = zip::ZipArchive::new(cursor).map_err(|e| {
        Error::ConfigError(format!("Failed to read zip archive: {}", e))
    })?;

    for i in 0..zip.len() {
        let mut entry = zip.by_index(i).map_err(|e| {
            Error::ConfigError(format!("Failed to read zip entry {}: {}", i, e))
        })?;

        let entry_name = entry.name().to_string();
        let entry_file = std::path::Path::new(&entry_name);

        if entry_file.file_name().and_then(|n| n.to_str()) == Some(filename) {
            // Ensure parent directory exists
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    Error::ConfigError(format!(
                        "Failed to create parent directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }

            let mut out = std::fs::File::create(dest).map_err(|e| {
                Error::ConfigError(format!("Failed to create {}: {}", dest.display(), e))
            })?;

            std::io::copy(&mut entry, &mut out).map_err(|e| {
                Error::ConfigError(format!("Failed to write {}: {}", dest.display(), e))
            })?;

            tracing::info!("Extracted {} from zip archive", filename);
            return Ok(());
        }
    }

    Err(Error::ConfigError(format!(
        "Could not find '{}' in zip archive",
        filename
    )))
}

/// Extract an embedded uv binary from `include_bytes!()`.
///
/// This is only available when the `bundled-uv-embedded` feature is enabled.
/// The binary must have been placed at `OUT_DIR/uv_binary` by `build.rs`
/// during compilation.
#[cfg(feature = "bundled-uv-embedded")]
fn extract_embedded_uv(dest: &Path) -> Result<()> {
    let embedded: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/uv_binary"));

    tracing::info!(
        size = %human_readable_size_bytes(embedded.len()),
        dest = %dest.display(),
        "Extracting embedded uv binary"
    );

    // Check if destination already exists with identical content
    if dest.exists() {
        let existing = std::fs::read(dest).map_err(|e| {
            Error::ConfigError(format!(
                "Failed to read existing uv binary at {}: {}",
                dest.display(),
                e
            ))
        })?;

        if sha2_hex(&existing) == sha2_hex(embedded) {
            tracing::info!(
                path = %dest.display(),
                "Embedded uv binary already present, skipping extraction"
            );
            return Ok(());
        }

        tracing::warn!(
            path = %dest.display(),
            "Replacing stale uv binary with embedded version"
        );
    }

    // Ensure parent directory exists
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            Error::ConfigError(format!(
                "Failed to create directory {}: {}",
                parent.display(),
                e
            ))
        })?;
    }

    std::fs::write(dest, embedded).map_err(|e| {
        Error::ConfigError(format!(
            "Failed to write embedded uv binary to {}: {}",
            dest.display(),
            e
        ))
    })?;

    // Set executable permission on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dest, std::fs::Permissions::from_mode(0o755)).map_err(|e| {
            Error::ConfigError(format!(
                "Failed to set executable permission on {}: {}",
                dest.display(),
                e
            ))
        })?;
    }

    tracing::info!(
        path = %dest.display(),
        size = %human_readable_size(dest),
        "Embedded uv binary extracted"
    );

    Ok(())
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
    fn test_uv_platform_target() {
        let target = uv_platform_target();
        if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
            assert_eq!(target, Some("x86_64-unknown-linux-gnu"));
        } else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
            assert_eq!(target, Some("aarch64-unknown-linux-gnu"));
        } else if cfg!(all(target_os = "macos", target_arch = "x86_64")) {
            assert_eq!(target, Some("x86_64-apple-darwin"));
        } else if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
            assert_eq!(target, Some("aarch64-apple-darwin"));
        } else if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
            assert_eq!(target, Some("x86_64-pc-windows-msvc"));
        } else if cfg!(all(target_os = "windows", target_arch = "aarch64")) {
            assert_eq!(target, Some("aarch64-pc-windows-msvc"));
        } else {
            assert!(target.is_none());
        }
    }

    #[test]
    fn test_sha2_hex() {
        let digest = sha2_hex(b"hello");
        assert_eq!(
            digest,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_is_windows_target() {
        assert_eq!(is_windows_target(), cfg!(windows));
    }

    #[test]
    fn test_human_readable_size_bytes() {
        assert_eq!(human_readable_size_bytes(0), "0 B");
        assert_eq!(human_readable_size_bytes(512), "512 B");
        assert_eq!(human_readable_size_bytes(1024), "1.0 KB");
        assert_eq!(human_readable_size_bytes(1536), "1.5 KB");
        assert_eq!(human_readable_size_bytes(1048576), "1.0 MB");
        assert_eq!(human_readable_size_bytes(1572864), "1.5 MB");
    }

    #[test]
    fn test_extract_from_tar_gz_roundtrip() {
        // Build a tiny tar.gz containing a single "uv" file
        use std::io::Write;

        // Create the tar content in memory
        let mut tar_bytes = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut tar_bytes);
            let mut header = tar::Header::new_gnu();
            header.set_path("uv").unwrap();
            header.set_size(5);
            header.set_mode(0o755);
            header.set_cksum();
            builder.append(&header, &b"hello"[..]).unwrap();
            builder.finish().unwrap();
        }

        // Gzip compress
        let mut compressed = Vec::new();
        {
            use flate2::write::GzEncoder;
            use flate2::Compression;
            let mut encoder = GzEncoder::new(&mut compressed, Compression::fast());
            encoder.write_all(&tar_bytes).unwrap();
            encoder.finish().unwrap();
        }

        let tmp = std::env::temp_dir().join(format!("test_uv_{}", std::process::id()));
        let dest = tmp.join("uv");

        let result = extract_from_tar_gz(&compressed, "uv", &dest);
        assert!(result.is_ok(), "extract_from_tar_gz failed: {:?}", result);

        let content = std::fs::read(&dest).unwrap();
        assert_eq!(content, b"hello");

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_extract_from_zip_roundtrip() {
        // Build a tiny zip containing a single "uv.exe" file
        use std::io::Write;

        let mut zip_bytes = Vec::new();
        {
            let mut zip_writer = zip::ZipWriter::new(std::io::Cursor::new(&mut zip_bytes));
            let options: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default()
                .unix_permissions(0o755);
            zip_writer.start_file("uv.exe", options).unwrap();
            zip_writer.write_all(b"hello").unwrap();
            zip_writer.finish().unwrap();
        }

        let tmp = std::env::temp_dir().join(format!("test_uv_zip_{}", std::process::id()));
        let dest = tmp.join("uv.exe");

        let result = extract_from_zip(&zip_bytes, "uv.exe", &dest);
        assert!(result.is_ok(), "extract_from_zip failed: {:?}", result);

        let content = std::fs::read(&dest).unwrap();
        assert_eq!(content, b"hello");

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn test_download_uv_unsupported_platform_returns_error() {
        // The function uses runtime env consts — on a supported platform it would
        // attempt an actual HTTP download (which we can't test offline). The
        // platform-target function is tested separately above.
        // This test confirms the error path for unsupported platforms compiles
        // and returns the right error type.
        match uv_platform_target() {
            None => {
                let result = download_uv("0.5.0", "", Path::new("/tmp/uv"));
                assert!(result.is_err());
                let err = result.unwrap_err();
                assert!(err.to_string().contains("Unsupported platform"));
            }
            Some(_) => {
                // On supported platforms, just confirm the function compiles
                // (it would try to actually download, which is skipped in unit tests)
            }
        }
    }
}
