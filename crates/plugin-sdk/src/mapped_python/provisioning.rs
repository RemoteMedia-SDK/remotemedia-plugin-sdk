use super::MappedPythonConfig;
use remotemedia_py_env::{merge_deps, EnvScope, PythonEnvManager};
use remotemedia_shared_memory::peer::{CallContext, Protocols};
use remotemedia_types::{Error, Result};
use sha2::{Digest, Sha256};
use std::{io::Read, path::Path};

fn wheel_requirement(name: &str, path: &Path) -> Result<(String, String)> {
    let path = path
        .canonicalize()
        .map_err(|e| Error::ConfigError(format!("{name} wheel: {e}")))?;
    if path.extension().and_then(|ext| ext.to_str()) != Some("whl") {
        return Err(Error::ConfigError(format!(
            "{name} must be a wheel artifact"
        )));
    }
    let digest = file_digest(&path)?;
    if let Some(expected) = pinned_digest(&path) {
        if expected != digest {
            return Err(Error::ConfigError(format!(
                "{name} wheel changed after its runtime bundle was verified: {}",
                path.display()
            )));
        }
    }
    let mut url = url::Url::from_file_path(path)
        .map_err(|_| Error::ConfigError(format!("{name} wheel path cannot become a file URL")))?;
    url.set_fragment(Some(&format!("sha256={digest}")));
    Ok((format!("{name} @ {url}"), digest))
}

/// Wheels verified by a loaded runtime bundle, keyed by canonical path. A
/// later launch must install exactly the verified bytes, not whatever file
/// has since replaced them at that path.
fn pinned() -> &'static std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, String>> {
    static PINNED: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, String>>,
    > = std::sync::OnceLock::new();
    PINNED.get_or_init(Default::default)
}

pub(super) fn pin_wheel(canonical: &Path, digest: &str) {
    pinned()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(canonical.to_path_buf(), digest.to_owned());
}

fn pinned_digest(canonical: &Path) -> Option<String> {
    pinned()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(canonical)
        .cloned()
}

pub(super) fn file_digest(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path).map_err(|e| Error::ConfigError(e.to_string()))?;
    let mut hash = Sha256::new();
    let mut bytes = [0; 64 * 1024];
    loop {
        let count = file
            .read(&mut bytes)
            .map_err(|e| Error::ConfigError(e.to_string()))?;
        if count == 0 {
            break;
        }
        hash.update(&bytes[..count]);
    }
    Ok(hex::encode(hash.finalize()))
}

pub(super) async fn provision(
    config: &MappedPythonConfig,
    params: &serde_json::Value,
    identity: &CallContext,
    protocols: Protocols,
) -> Result<super::launch::PreparedPython> {
    let (client, client_hash) = wheel_requirement("remotemedia-client", &config.wheels.client)?;
    let (buffers, buffers_hash) = wheel_requirement("remotemedia-buffers", &config.wheels.buffers)?;
    let mut environment = config.environment.clone();
    environment.require_python_version = true;
    // Explicit wheels suppress editable/autodiscovered SDK installation. Keep
    // other caller base dependencies; conflicting SDK pins must fail resolution.
    environment.base_deps.extend([client, buffers]);
    if let Some(version) = params.get("__python_version__") {
        environment.python_version = version
            .as_str()
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| {
                Error::ConfigError("__python_version__ must be a nonempty string".into())
            })?
            .into();
    }
    if let Some(paths) = params.get("__python_find_links__") {
        let paths: Vec<std::path::PathBuf> = serde_json::from_value(paths.clone())
            .map_err(|e| Error::ConfigError(format!("__python_find_links__: {e}")))?;
        environment.find_links.extend(paths);
    }
    if params.get("__python_explicit_venv__").is_some() {
        return Err(Error::ConfigError(
            "frozen explicit venvs are not yet supported by mapped Python placement".into(),
        ));
    }
    let scope_policy: EnvScope = params
        .get("__python_env_scope__")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .map_err(|e| Error::ConfigError(format!("__python_env_scope__: {e}")))?
        .unwrap_or(environment.scope.clone());
    let scope = if let Some(value) = params.get("__python_env_scope_context__") {
        Some(
            value
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    Error::ConfigError(
                        "__python_env_scope_context__ must be a nonempty string".into(),
                    )
                })?
                .to_owned(),
        )
    } else {
        match scope_policy {
            EnvScope::Global => None,
            EnvScope::PerPipeline | EnvScope::PerNode => {
                let session = identity
                    .session_id
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        Error::ConfigError(
                            "scoped Python environments require a session identity".into(),
                        )
                    })?;
                Some(
                    serde_json::json!([
                        "mapped-python-scope-v1",
                        session,
                        if scope_policy == EnvScope::PerNode {
                            Some(&identity.node_id)
                        } else {
                            None
                        }
                    ])
                    .to_string(),
                )
            }
        }
    };
    let manifest_deps: Vec<String> = serde_json::from_value(
        params
            .get("__python_deps__")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
    )
    .map_err(|e| Error::ConfigError(format!("__python_deps__: {e}")))?;
    let extra_deps: Vec<String> = serde_json::from_value(
        params
            .get("__python_extra_deps__")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
    )
    .map_err(|e| Error::ConfigError(format!("__python_extra_deps__: {e}")))?;
    let requirements = merge_deps(&config.requirements, &manifest_deps, &extra_deps);
    let manager = PythonEnvManager::new(environment)?;
    let venv = manager
        .ensure_env_scoped_leased(&requirements, scope.as_deref())
        .await?;
    let context =
        remotemedia_py_env::ProvisioningContext::new(manager.config().process_namespace.clone());
    super::launch::prepare(venv, &client_hash, &buffers_hash, protocols, &context).await
}

#[cfg(test)]
mod pin_tests {
    use super::*;

    #[test]
    fn bundle_pinned_wheel_rejects_replacement_before_install() {
        let dir = std::env::temp_dir().join(format!(
            "rm-wheel-pin-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let wheel = dir.join("remotemedia_client-0.0.0-py3-none-any.whl");
        std::fs::write(&wheel, b"verified bytes").unwrap();
        let canonical = wheel.canonicalize().unwrap();
        pin_wheel(&canonical, &file_digest(&canonical).unwrap());
        let (requirement, _) = wheel_requirement("remotemedia-client", &wheel).unwrap();
        assert!(requirement.starts_with("remotemedia-client @ file:"));
        std::fs::write(&wheel, b"replaced bytes").unwrap();
        let error = wheel_requirement("remotemedia-client", &wheel)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("changed after its runtime bundle was verified"),
            "{error}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
