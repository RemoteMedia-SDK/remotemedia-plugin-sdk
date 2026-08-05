/// build.rs — Embed uv binary at compile time.
///
/// When the `bundled-uv-embedded` feature is enabled, this script provisions
/// a uv binary and copies it into `OUT_DIR/uv_binary` so the crate's source can
/// include it via `include_bytes!(concat!(env!("OUT_DIR"), "/uv_binary"))`.
///
/// Provisioning order (first match wins):
///   1. `UV_BINARY_PATH` env var — explicit override (e.g. a pinned/prebuilt uv).
///   2. `uv` discovered on the build host's PATH — copy it directly (no download).
///   3. Auto-download the uv release matching the build host's target triple
///      from GitHub releases into a temp dir, then copy it.
///
/// If none of these succeed, the build fails with a clear message. Auto-provisioning
/// means a plain `cargo build --features bundled-uv-embedded` works on any host with
/// network access (or a system `uv`), without manually exporting `UV_BINARY_PATH`.
///
/// # Usage (explicit override still supported)
///
/// ```sh
/// UV_BINARY_PATH=/path/to/uv cargo build --features bundled-uv-embedded
/// ```

use std::io::Read;
use std::path::Path;

fn main() {
    // Only run when the feature is active.
    #[cfg(feature = "bundled-uv-embedded")]
    {
        let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR must be set by Cargo");
        let dest = Path::new(&out_dir).join("uv_binary");

        if let Some(src) = provision_uv() {
            copy_with_perms(&src, &dest);
            println!(
                "cargo:info=Embedded uv binary from '{}' ({}",
                src.display(),
                human_readable_size(&dest)
            );
        } else {
            panic!(
                "Could not provision a uv binary for embedding. Set UV_BINARY_PATH to a \
                 local uv binary, install `uv` on the build host's PATH, or ensure the \
                 build host has network access to download uv from GitHub releases.\n\
                 See https://github.com/astral-sh/uv/releases"
            );
        }
    }
}

/// Resolve a uv binary to embed, honoring the provisioning order.
/// Returns the path to a usable uv binary, or `None` if none could be found.
fn provision_uv() -> Option<std::path::PathBuf> {
    // 1. Explicit override.
    if let Ok(p) = std::env::var("UV_BINARY_PATH") {
        let path = std::path::PathBuf::from(p);
        if path.exists() && path.is_file() {
            return Some(path);
        }
    }

    // 2. `uv` on the build host's PATH.
    if let Some(path) = which_uv() {
        return Some(path);
    }

    // 3. Auto-download for the build host's platform.
    if let Some(path) = download_uv_for_build_host() {
        return Some(path);
    }

    None
}

/// Locate `uv` on the build host PATH (mirrors the runtime `which_uv`).
fn which_uv() -> Option<std::path::PathBuf> {
    let path_var = std::env::var("PATH").ok()?;
    for dir in path_var.split(':') {
        if dir.is_empty() {
            continue;
        }
        let candidate = Path::new(dir).join(if cfg!(windows) { "uv.exe" } else { "uv" });
        if candidate.exists() && candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Download the uv release matching the build host's target triple into a temp
/// dir and return its path. Best-effort: returns `None` on any failure so the
/// caller can surface a clear error.
fn download_uv_for_build_host() -> Option<std::path::PathBuf> {
    let target = uv_platform_target()?;
    let version = std::option_env!("UV_VERSION").unwrap_or("0.6.14");
    let extension = if cfg!(windows) { "zip" } else { "tar.gz" };
    let url = format!(
        "https://github.com/astral-sh/uv/releases/download/{version}/uv-{target}.{extension}"
    );

    eprintln!("build.rs: downloading uv {version} from {url}");

    let bytes = match reqwest::blocking::get(&url).and_then(|r| r.bytes()) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("build.rs: uv download failed: {e}");
            return None;
        }
    };

    let tmp = match std::env::temp_dir().join(format!("uv-embed-{}-{}", target, version)) {
        p => p,
    };
    let _ = std::fs::create_dir_all(&tmp);
    let uv_name = if cfg!(windows) { "uv.exe" } else { "uv" };
    let dest = tmp.join(uv_name);

    if cfg!(windows) {
        if extract_zip(&bytes, uv_name, &dest).is_err() {
            return None;
        }
    } else if extract_tar_gz(&bytes, uv_name, &dest).is_err() {
        return None;
    }

    Some(dest)
}

/// Map the build host platform to a uv release archive suffix.
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

/// Copy `src` to `dest`, preserving the executable bit on Unix.
fn copy_with_perms(src: &Path, dest: &Path) {
    std::fs::copy(src, dest).unwrap_or_else(|e| {
        panic!("Failed to copy uv binary from '{}' to '{}': {}", src.display(), dest.display(), e)
    });
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(src)
            .map(|m| m.permissions().mode())
            .unwrap_or(0o755);
        let mut perms = std::fs::metadata(dest).unwrap().permissions();
        perms.set_mode(mode);
        let _ = std::fs::set_permissions(dest, perms);
    }
}

#[allow(dead_code)]
fn extract_tar_gz(archive: &[u8], filename: &str, dest: &Path) -> std::io::Result<()> {
    let decoder = flate2::read::GzDecoder::new(archive);
    let mut tar = tar::Archive::new(decoder);
    for entry in tar.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        if path.file_name().and_then(|n| n.to_str()) == Some(filename) {
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut out = std::fs::File::create(dest)?;
            std::io::copy(&mut entry, &mut out)?;
            return Ok(());
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("uv binary '{filename}' not found in archive"),
    ))
}

#[allow(dead_code)]
fn extract_zip(archive: &[u8], filename: &str, dest: &Path) -> std::io::Result<()> {
    let reader = std::io::Cursor::new(archive);
    let mut zip = zip::ZipArchive::new(reader)?;
    for i in 0..zip.len() {
        let mut entry = zip.by_index(i)?;
        let name = entry.name().to_string();
        if Path::new(&name).file_name().and_then(|n| n.to_str()) == Some(filename) {
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut out = std::fs::File::create(dest)?;
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf)?;
            std::io::Write::write_all(&mut out, &buf)?;
            return Ok(());
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("uv binary '{filename}' not found in archive"),
    ))
}

#[allow(dead_code)]
/// Compute a human-readable file size string for a path.
fn human_readable_size(path: &Path) -> String {
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    if size < 1024 {
        format!("{} B", size)
    } else if size < 1024 * 1024 {
        format!("{:.1} KB", size as f64 / 1024.0)
    } else {
        format!("{:.1} MB", size as f64 / (1024.0 * 1024.0))
    }
}
