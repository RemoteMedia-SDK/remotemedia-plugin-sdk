/// build.rs — Embed uv binary at compile time.
///
/// When the `bundled-uv-embedded` feature is enabled, this script reads
/// the `UV_BINARY_PATH` environment variable, validates the file exists,
/// and copies it into `OUT_DIR/uv_binary` so the crate's source can
/// include it via `include_bytes!(concat!(env!("OUT_DIR"), "/uv_binary"))`.
///
/// # Usage
///
/// ```sh
/// UV_BINARY_PATH=/path/to/uv cargo build --features bundled-uv-embedded
/// ```

use std::path::Path;
use std::fs;

fn main() {
    // Only run when the feature is active. Cargo will not re-run build.rs
    // when features change, so the emitted cfg check is OK — Cargo watches
    // Cargo.toml for feature changes and invalidates the build script.
    #[cfg(feature = "bundled-uv-embedded")]
    {
        let uv_binary_path = std::env::var("UV_BINARY_PATH").unwrap_or_else(|_| {
            panic!(
                "UV_BINARY_PATH must be set when building with bundled-uv-embedded feature. \
                 Download the uv binary for your target platform from \
                 https://github.com/astral-sh/uv/releases and set UV_BINARY_PATH to its path.\n\n\
                 Example:\n  UV_BINARY_PATH=/tmp/uv-x86_64-unknown-linux-gnu/uv cargo build"
            )
        });

        let src = Path::new(&uv_binary_path);

        assert!(
            src.exists(),
            "UV_BINARY_PATH points to a file that does not exist: {}",
            uv_binary_path
        );

        let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR must be set by Cargo");
        let dest = Path::new(&out_dir).join("uv_binary");

        fs::copy(src, &dest).unwrap_or_else(|e| {
            panic!(
                "Failed to copy uv binary from '{}' to '{}': {}",
                uv_binary_path,
                dest.display(),
                e
            )
        });

        // On Unix, preserve the executable bit
        #[cfg(unix)]
        {
            let metadata = fs::metadata(src).unwrap_or_else(|e| {
                panic!("Failed to read metadata of '{}': {}", uv_binary_path, e)
            });
            fs::set_permissions(&dest, metadata.permissions()).unwrap_or_else(|e| {
                panic!(
                    "Failed to set permissions on '{}': {}",
                    dest.display(),
                    e
                )
            });
        }

        println!("cargo:rerun-if-changed={}", uv_binary_path);
        println!("cargo:rerun-if-env-changed=UV_BINARY_PATH");

        println!(
            "cargo:info=Embedded uv binary from '{}' ({})",
            uv_binary_path,
            human_readable_size(&dest)
        );
    }
}

#[allow(dead_code)]
/// Compute a human-readable file size string for a path.
fn human_readable_size(path: &Path) -> String {
    let size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    if size < 1024 {
        format!("{} B", size)
    } else if size < 1024 * 1024 {
        format!("{:.1} KB", size as f64 / 1024.0)
    } else {
        format!("{:.1} MB", size as f64 / (1024.0 * 1024.0))
    }
}