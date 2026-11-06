use std::path::Path;

use remotemedia_types::{Error, Result};
use tokio::process::Command;

/// Probe the interpreter itself, ignoring ambient Python import paths. Check
/// cached environments too: metadata records intent, not the executable's ABI.
pub(crate) async fn validate_python_version(python: &Path, requested: &str) -> Result<()> {
    let components = requested
        .split('.')
        .map(str::parse::<u32>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| Error::ConfigError(format!("Invalid strict Python version {requested:?}")))?;
    if !(2..=3).contains(&components.len()) {
        return Err(Error::ConfigError(
            "Strict Python version must be major.minor or major.minor.patch".into(),
        ));
    }
    let output = crate::hidden(Command::new(python))
        .args([
            "-I",
            "-c",
            "import json,sys; print(json.dumps(list(sys.version_info[:3])))",
        ])
        .kill_on_drop(true)
        .output();
    let output = tokio::time::timeout(std::time::Duration::from_secs(15), output)
        .await
        .map_err(|_| Error::ConfigError("Python version probe timed out".into()))?
        .map_err(|e| Error::ConfigError(format!("Cannot probe {}: {e}", python.display())))?;
    if !output.status.success() {
        return Err(Error::ConfigError(format!(
            "Python version probe failed for {}: {}",
            python.display(),
            String::from_utf8_lossy(&output.stderr).trim(),
        )));
    }
    let installed: [u32; 3] = serde_json::from_slice(&output.stdout)
        .map_err(|e| Error::ConfigError(format!("Invalid Python version response: {e}")))?;
    if installed[..components.len()] != components {
        return Err(Error::ConfigError(format!(
            "Python version mismatch at {}: requested {requested}, installed {}.{}.{}",
            python.display(),
            installed[0],
            installed[1],
            installed[2],
        )));
    }
    Ok(())
}
