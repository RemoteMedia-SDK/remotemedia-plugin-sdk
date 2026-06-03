//! Python environment manager for RemoteMedia SDK.
//!
//! Manages Python virtual environments using `uv` (preferred) or falling back
//! to the system `venv` + `pip` toolchain.
//!
//! # Overview
//!
//! The environment manager:
//! - Creates and caches virtual environments keyed by dependency set
//! - Supports three modes: System (use existing python), Managed (uv manages venvs),
//!   and ManagedWithPython (uv manages both python and venvs)
//! - Provides LRU eviction of cached environments
//! - Normalizes package names per PEP 503 for deduplication

#[cfg(feature = "bundled-uv")]
pub mod uv_backend;

pub mod system_backend;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use remotemedia_types::{Error, Result};

// ---------------------------------------------------------------------------
// Public enums (always available, not feature-gated)
// ---------------------------------------------------------------------------

/// How the Python environment is managed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PythonEnvMode {
    /// Use whatever `python3` is on the system PATH.
    System,
    /// Use `uv` to create/manage virtual environments, but rely on a
    /// system-installed Python interpreter.
    Managed,
    /// Use `uv` to both install the requested Python version and manage
    /// virtual environments.
    ManagedWithPython,
}

impl Default for PythonEnvMode {
    fn default() -> Self {
        Self::Managed
    }
}

/// Scope at which virtual environments are cached / shared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvScope {
    /// One global environment shared by all pipelines and nodes.
    Global,
    /// One environment per pipeline (keyed by manifest hash).
    PerPipeline,
    /// One environment per node (keyed by node id + deps).
    PerNode,
}

impl Default for EnvScope {
    fn default() -> Self {
        Self::Global
    }
}

// ---------------------------------------------------------------------------
// Package name helpers (always available)
// ---------------------------------------------------------------------------

/// Normalize a Python package name per PEP 503.
///
/// Lowercases the name and replaces hyphens, underscores, and dots with a
/// single hyphen. This ensures `my_package`, `My-Package`, and `my.package`
/// all map to the same canonical form.
pub fn normalize_package_name(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c == '_' || c == '.' { '-' } else { c })
        .collect()
}

/// Merge dependency lists with marker-aware override semantics.
///
/// Dedup key is `(normalized_name, environment_marker)`, not just the
/// package name. Two entries for the same package collapse only when
/// their PEP 508 environment marker (the text after `;`) is identical,
/// in which case the later one wins — preserving the "manifest_deps
/// override node_deps" contract for plain version-pin overrides.
///
/// Entries with *different* markers are all kept; uv evaluates markers
/// per platform and installs whichever applies. This is the only safe
/// behaviour when nodes declare cross-platform fallbacks via
/// `@python_requires` alongside platform-specific URL pins via PEP 723
/// — naïve last-write-wins by name silently dropped the unconditional
/// entry when a marker-bearing entry for the same package was present
/// (see the moss-tts-realtime torchaudio incident).
///
/// The result is sorted lexicographically by the full dependency string.
pub fn merge_deps(
    node_deps: &[String],
    manifest_deps: &[String],
    extra_deps: &[String],
) -> Vec<String> {
    let mut seen: HashMap<(String, String), String> = HashMap::new();

    let mut record = |dep: &String| {
        let name = extract_package_name(dep);
        let norm = normalize_package_name(&name);
        let marker = extract_marker(dep);
        seen.insert((norm, marker), dep.clone());
    };

    for dep in node_deps {
        record(dep);
    }
    for dep in manifest_deps {
        record(dep);
    }
    for dep in extra_deps {
        record(dep);
    }

    let mut result: Vec<String> = seen.into_values().collect();
    result.sort();
    result
}

/// Extract the PEP 508 environment marker from a dep spec.
///
/// `"torch>=2.1"` -> `""`, `"torch ; sys_platform == 'win32'"` ->
/// `"sys_platform == 'win32'"`. Returned text is trimmed but otherwise
/// preserved verbatim so two entries with the same marker stringify
/// to the same dedup key.
fn extract_marker(dep: &str) -> String {
    match dep.split_once(';') {
        Some((_, marker)) => marker.trim().to_string(),
        None => String::new(),
    }
}

/// Validate that every requested dependency is actually present in the venv.
///
/// `pip install` / `uv pip install` returns exit 0 even when all of a
/// package's specs are filtered out by PEP 508 environment markers — for
/// example a Windows-only URL pin run on Linux. Without this check those
/// silent drops only surface later as `ModuleNotFoundError` at first import,
/// often deep inside a subprocess where the cause is hard to recover.
///
/// We probe the venv via `importlib.metadata` (stdlib, no pip required —
/// `uv venv` does NOT install pip by default, so `python -m pip list`
/// would silently no-op and defeat the validation). Then for each
/// requested dep we group by normalized package name and check whether
/// at least one landed in the venv. A miss is logged at WARN with all
/// the original specs so the operator can see exactly which markers
/// excluded the current platform. Failure of the probe itself never
/// breaks the install — it's diagnostic only.
async fn validate_installed_deps(python_executable: &Path, deps: &[String]) {
    use std::collections::{HashMap, HashSet};

    // Group requested deps by normalized package name, skipping entries
    // that don't carry a conventional name (editable installs, raw URLs,
    // `-r requirements.txt` references, etc — pip handles those itself
    // and the post-install state is uninspectable by name).
    let mut groups: HashMap<String, Vec<&String>> = HashMap::new();
    for dep in deps {
        let trimmed = dep.trim();
        if trimmed.is_empty()
            || trimmed.starts_with('-')
            || trimmed.starts_with("http://")
            || trimmed.starts_with("https://")
            || trimmed.starts_with("file://")
        {
            continue;
        }
        let name = extract_package_name(trimmed);
        if name.is_empty() || name.starts_with('-') {
            continue;
        }
        let norm = normalize_package_name(&name);
        groups.entry(norm).or_default().push(dep);
    }
    if groups.is_empty() {
        return;
    }

    // Stdlib-only probe. Lists every distribution discoverable on the
    // venv's sys.path. `Distribution.name` is the raw project name; we
    // normalize the same way we did the requested specs.
    let probe = "\
import json, sys
try:
    from importlib.metadata import distributions
    names = sorted({d.metadata['Name'] for d in distributions() if d.metadata and d.metadata['Name']})
    sys.stdout.write(json.dumps(names))
except Exception as exc:
    sys.stderr.write(f'{type(exc).__name__}: {exc}\\n')
    sys.exit(2)
";

    let output = tokio::process::Command::new(python_executable)
        .args(["-c", probe])
        .output()
        .await;

    let stdout = match output {
        Ok(out) if out.status.success() => out.stdout,
        Ok(out) => {
            tracing::debug!(
                python = %python_executable.display(),
                exit = ?out.status.code(),
                stderr = %String::from_utf8_lossy(&out.stderr).trim(),
                "post-install validation: importlib.metadata probe failed; skipping (install itself succeeded)"
            );
            return;
        }
        Err(e) => {
            tracing::debug!(
                python = %python_executable.display(),
                error = %e,
                "post-install validation: could not spawn validation probe; skipping"
            );
            return;
        }
    };

    let installed: HashSet<String> = match serde_json::from_slice::<Vec<String>>(&stdout) {
        Ok(names) => names
            .into_iter()
            .map(|s| normalize_package_name(&s))
            .collect(),
        Err(e) => {
            tracing::debug!(
                error = %e,
                "post-install validation: could not parse importlib.metadata output; skipping"
            );
            return;
        }
    };

    for (norm, specs) in &groups {
        if installed.contains(norm) {
            continue;
        }
        let all_marker_bearing = specs.iter().all(|s| s.contains(';'));
        if all_marker_bearing {
            tracing::warn!(
                package = %norm,
                requested_specs = ?specs,
                "post-install: requested package not present in venv. Every \
                 spec has a PEP 508 environment marker that excluded the \
                 current platform. If a node imports `{}` here, add an \
                 unconditional fallback in @python_requires.",
                norm
            );
        } else {
            tracing::error!(
                package = %norm,
                requested_specs = ?specs,
                "post-install: requested package not present in venv despite \
                 an unconditional spec. This suggests the install step \
                 silently skipped it — check the `uv finished` / pip stderr \
                 lines above for resolution conflicts."
            );
        }
    }
}

/// Extract the package name portion from a dependency specifier.
///
/// E.g. `"numpy>=1.21"` -> `"numpy"`, `"my-package[extra]"` -> `"my-package"`.
fn extract_package_name(dep: &str) -> String {
    let dep = dep.trim();
    // Split on version specifiers or extras
    let end = dep
        .find(|c: char| {
            c == '>' || c == '<' || c == '=' || c == '!' || c == '[' || c == ';' || c == '@'
        })
        .unwrap_or(dep.len());
    dep[..end].trim().to_string()
}

// ---------------------------------------------------------------------------
// VenvInfo
// ---------------------------------------------------------------------------

/// Information about a created virtual environment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenvInfo {
    /// Root directory of the virtual environment.
    pub path: PathBuf,
    /// Path to the Python executable inside the venv.
    pub python_executable: PathBuf,
    /// Cache key that uniquely identifies this environment's dependency set.
    pub cache_key: String,
}

// ---------------------------------------------------------------------------
// EnvBackend trait
// ---------------------------------------------------------------------------

/// Backend trait for creating and managing Python environments.
///
/// Implementations handle the differences between uv-based and system-based
/// environment management.
#[async_trait]
pub trait EnvBackend: Send + Sync {
    /// Ensure the requested Python version is available.
    ///
    /// Returns the path to the Python interpreter. For system backends this
    /// simply validates the system python; for uv it may install the version.
    async fn ensure_python(&self, version: &str) -> Result<PathBuf>;

    /// Create a new virtual environment.
    ///
    /// `python` is the interpreter path (from `ensure_python`).
    /// `cache_dir` is the parent directory for cached venvs.
    /// `cache_key` is the unique key for this dependency set.
    async fn create_venv(
        &self,
        python: &Path,
        cache_dir: &Path,
        cache_key: &str,
    ) -> Result<VenvInfo>;

    /// Install dependencies into an existing virtual environment.
    async fn install_deps(&self, venv: &VenvInfo, deps: &[String]) -> Result<()>;

    /// Resolve the path to the Python executable inside a venv.
    fn resolve_python(&self, venv: &VenvInfo) -> PathBuf;
}

// ---------------------------------------------------------------------------
// VenvCache
// ---------------------------------------------------------------------------

/// Metadata stored alongside each cached virtual environment.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct VenvMetadata {
    /// Dependencies installed in this environment.
    deps: Vec<String>,
    /// Python version used to create the environment.
    python_version: String,
    /// ISO 8601 timestamp when the environment was created.
    created_at: String,
    /// ISO 8601 timestamp when the environment was last used.
    last_used_at: String,
}

const METADATA_FILENAME: &str = "remotemedia-env.json";

/// Cache of virtual environments on disk.
///
/// Environments are stored under `~/.config/remotemedia/envs/<cache_key>/`.
struct VenvCache {
    /// Base directory for all cached environments.
    cache_dir: PathBuf,
    /// Maximum number of cached environments before LRU eviction.
    max_cached_envs: usize,
    /// Lock to prevent concurrent venv creation for the same cache key.
    lock: tokio::sync::Mutex<()>,
}

static GLOBAL_VENV_CACHE_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
    std::sync::OnceLock::new();

impl VenvCache {
    fn new(cache_dir: PathBuf, max_cached_envs: usize) -> Self {
        Self {
            cache_dir,
            max_cached_envs,
            lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Compute a cache key from the Python version and sorted dependency list.
    fn cache_key(python_version: &str, deps: &[String], scope_context: Option<&str>) -> String {
        let mut sorted_deps: Vec<String> = deps
            .iter()
            .map(|d| {
                normalize_package_name(&extract_package_name(d))
                    + &d[extract_package_name(d).len()..]
            })
            .collect();
        sorted_deps.sort();

        let mut hasher = Sha256::new();
        hasher.update(python_version.as_bytes());
        hasher.update(b"\0");
        hasher.update(sorted_deps.join("\0").as_bytes());
        if let Some(scope_context) = scope_context.filter(|s| !s.is_empty()) {
            hasher.update(b"\0scope\0");
            hasher.update(scope_context.as_bytes());
        }
        let hash = hasher.finalize();
        hex::encode(&hash[..8]) // 16 hex chars from first 8 bytes
    }

    /// Get an existing cached environment or create a new one.
    async fn get_or_create(
        &self,
        deps: &[String],
        python_version: &str,
        scope_context: Option<&str>,
        backend: &dyn EnvBackend,
    ) -> Result<VenvInfo> {
        let global_lock = GLOBAL_VENV_CACHE_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
        let _global_guard = global_lock.lock().await;
        let _guard = self.lock.lock().await;

        let key = Self::cache_key(python_version, deps, scope_context);
        let venv_dir = self.cache_dir.join(&key);
        let meta_path = venv_dir.join(METADATA_FILENAME);

        // Check if a valid cached environment exists
        if meta_path.exists() {
            if let Ok(contents) = std::fs::read_to_string(&meta_path) {
                if let Ok(mut meta) = serde_json::from_str::<VenvMetadata>(&contents) {
                    // Update last_used_at timestamp
                    meta.last_used_at = now_iso8601();
                    if let Ok(json) = serde_json::to_string_pretty(&meta) {
                        let _ = std::fs::write(&meta_path, json);
                    }

                    let python_executable = backend.resolve_python(&VenvInfo {
                        path: venv_dir.clone(),
                        python_executable: PathBuf::new(), // will be resolved
                        cache_key: key.clone(),
                    });

                    if python_executable.exists() {
                        tracing::info!(
                            cache_key = %key,
                            "Reusing cached Python environment"
                        );
                        return Ok(VenvInfo {
                            path: venv_dir,
                            python_executable,
                            cache_key: key,
                        });
                    }
                }
            }
        }

        // Create a new environment
        tracing::info!(
            cache_key = %key,
            num_deps = deps.len(),
            "Creating new Python environment"
        );

        let python = backend.ensure_python(python_version).await?;

        // Remove stale directory if it exists
        if venv_dir.exists() {
            std::fs::remove_dir_all(&venv_dir).map_err(|e| {
                Error::Execution(format!(
                    "Failed to remove stale venv directory {}: {}",
                    venv_dir.display(),
                    e
                ))
            })?;
        }

        std::fs::create_dir_all(&self.cache_dir).map_err(|e| {
            Error::Execution(format!(
                "Failed to create cache directory {}: {}",
                self.cache_dir.display(),
                e
            ))
        })?;

        let venv_info = backend.create_venv(&python, &self.cache_dir, &key).await?;

        // Install dependencies
        if !deps.is_empty() {
            backend.install_deps(&venv_info, deps).await?;
            // Post-install validation: `pip install` returning exit 0 can
            // still mean "installed nothing" when every spec for a given
            // package has a PEP 508 environment marker that excludes the
            // current platform. Surface that loudly so a future silent
            // drop doesn't reach the user as a ModuleNotFoundError at
            // first import.
            validate_installed_deps(&venv_info.python_executable, deps).await;
        }

        // Write metadata
        let now = now_iso8601();
        let meta = VenvMetadata {
            deps: deps.to_vec(),
            python_version: python_version.to_string(),
            created_at: now.clone(),
            last_used_at: now,
        };

        if let Ok(json) = serde_json::to_string_pretty(&meta) {
            let _ = std::fs::write(venv_dir.join(METADATA_FILENAME), json);
        }

        // Evict old environments if over limit
        self.evict_lru().ok();

        Ok(venv_info)
    }

    /// Evict least-recently-used environments when cache exceeds max size.
    fn evict_lru(&self) -> Result<()> {
        let entries = std::fs::read_dir(&self.cache_dir).map_err(|e| {
            Error::Execution(format!(
                "Failed to read cache directory {}: {}",
                self.cache_dir.display(),
                e
            ))
        })?;

        let mut envs: Vec<(PathBuf, String)> = Vec::new();

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let meta_path = path.join(METADATA_FILENAME);
            if let Ok(contents) = std::fs::read_to_string(&meta_path) {
                if let Ok(meta) = serde_json::from_str::<VenvMetadata>(&contents) {
                    envs.push((path, meta.last_used_at));
                }
            }
        }

        if envs.len() <= self.max_cached_envs {
            return Ok(());
        }

        // Sort by last_used_at ascending (oldest first)
        envs.sort_by(|a, b| a.1.cmp(&b.1));

        let to_remove = envs.len() - self.max_cached_envs;
        for (path, _) in envs.into_iter().take(to_remove) {
            tracing::info!(
                path = %path.display(),
                "Evicting least-recently-used Python environment"
            );
            if let Err(e) = std::fs::remove_dir_all(&path) {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "Failed to evict cached environment"
                );
            }
        }

        Ok(())
    }
}

/// Get current time as ISO 8601 string.
///
/// Uses chrono if available, otherwise falls back to a simple unix timestamp.
fn now_iso8601() -> String {
    // Use std::time for a portable timestamp without requiring chrono at runtime.
    // Format: seconds since epoch (not pretty, but monotonic and comparable).
    use std::time::SystemTime;
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => {
            // Produce a sortable ISO-ish string: "2024-01-15T12:00:00Z"
            // We do basic formatting without chrono to avoid the optional dep issue.
            let secs = d.as_secs();
            // Simple approach: store as numeric string that sorts correctly
            format!("{}", secs)
        }
        Err(_) => "0".to_string(),
    }
}

// ---------------------------------------------------------------------------
// PythonEnvConfig
// ---------------------------------------------------------------------------

/// Configuration for the Python environment manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonEnvConfig {
    /// How environments are managed.
    #[serde(default)]
    pub mode: PythonEnvMode,

    /// Scope for environment caching.
    #[serde(default)]
    pub scope: EnvScope,

    /// Python version to use (e.g. "3.11", "3.12.1").
    #[serde(default = "default_python_version")]
    pub python_version: String,

    /// Maximum number of cached environments.
    #[serde(default = "default_max_cached_envs")]
    pub max_cached_envs: usize,

    /// Override the cache directory (default: ~/.config/remotemedia/envs/).
    #[serde(default)]
    pub cache_dir: Option<PathBuf>,

    /// Dependencies always installed into every provisioned venv.
    ///
    /// Used for the `remotemedia` client itself — without it, the
    /// multiprocess runner inside the venv can't `import remotemedia.*`
    /// and every node fails with `Node type '...' not registered`.
    /// Each entry is passed verbatim to the backend's install step, so
    /// PEP 440 specs (`remotemedia-client==0.2.0`) and editable paths
    /// (`-e /path/to/clients/python`) both work.
    ///
    /// Populated automatically by [`PythonEnvManager::new`] from the
    /// `REMOTEMEDIA_PYTHON_SRC` env var when empty. Set explicitly to
    /// override.
    #[serde(default)]
    pub base_deps: Vec<String>,
}

fn default_python_version() -> String {
    "3.11".to_string()
}

fn default_max_cached_envs() -> usize {
    8
}

impl Default for PythonEnvConfig {
    fn default() -> Self {
        Self {
            mode: PythonEnvMode::default(),
            scope: EnvScope::default(),
            python_version: default_python_version(),
            max_cached_envs: default_max_cached_envs(),
            cache_dir: None,
            base_deps: Vec::new(),
        }
    }
}

impl PythonEnvConfig {
    /// Build config from defaults plus process environment overrides.
    ///
    /// Source-load and cdylib Python plugins use this so they honor the
    /// same deployment knobs as the in-tree multiprocess executor.
    pub fn from_env() -> Self {
        let mut config = Self::default();
        config.apply_env_overrides();
        config
    }

    /// Apply process environment overrides in place.
    pub fn apply_env_overrides(&mut self) {
        if let Ok(mode) = std::env::var("PYTHON_ENV_MODE") {
            self.mode = match mode.to_lowercase().as_str() {
                "system" => PythonEnvMode::System,
                "managed" | "uv" => PythonEnvMode::Managed,
                "managed_with_python" | "managed-with-python" | "uv_python" | "uv-python" => {
                    PythonEnvMode::ManagedWithPython
                }
                other => {
                    tracing::warn!(
                        value = other,
                        "Ignoring unknown PYTHON_ENV_MODE; expected system, managed, or managed_with_python"
                    );
                    self.mode.clone()
                }
            };
        }

        if let Ok(version) = std::env::var("PYTHON_VERSION") {
            if !version.trim().is_empty() {
                self.python_version = version;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PythonEnvManager
// ---------------------------------------------------------------------------

/// Manages Python virtual environments for pipeline execution.
///
/// Selects between `uv`-based (fast, recommended) and system `venv`+`pip`
/// (fallback) backends depending on configuration and availability.
pub struct PythonEnvManager {
    backend: Arc<dyn EnvBackend>,
    config: PythonEnvConfig,
    cache: VenvCache,
}

impl PythonEnvManager {
    /// Create a new environment manager with the given configuration.
    ///
    /// Selects the backend based on mode:
    /// - `System`: always uses `SystemBackend`
    /// - `Managed` / `ManagedWithPython`: tries `UvBackend`, falls back to
    ///   `SystemBackend` if uv is not available
    pub fn new(mut config: PythonEnvConfig) -> Result<Self> {
        // Ensure every provisioned venv can `import remotemedia`. Resolution
        // order when the caller didn't set `base_deps` explicitly:
        //   1. `REMOTEMEDIA_PYTHON_SRC` env var (explicit override).
        //   2. In-tree discovery: walk up from this crate's manifest dir to
        //      find a sibling `clients/python` with a `setup.py`. Lets
        //      workspace-builds (cargo run, examples, tests) work with zero
        //      extra config.
        // Without one of these, every multiprocess node crashes with
        // `Node type '...' not registered` because the runner inside the
        // venv can't import the package that defines them.
        if config.base_deps.is_empty() {
            let resolved = std::env::var("REMOTEMEDIA_PYTHON_SRC")
                .ok()
                .map(|s| (s.trim().to_string(), "REMOTEMEDIA_PYTHON_SRC"))
                .filter(|(s, _)| !s.is_empty())
                .or_else(|| {
                    discover_in_tree_python_src()
                        .map(|p| (p.to_string_lossy().into_owned(), "workspace auto-discovery"))
                });

            match resolved {
                Some((src, origin)) => {
                    // Single requirements.txt line — the `-e <path>` form
                    // must not be split across lines or `pip` rejects it.
                    config.base_deps = vec![format!("-e {}", src)];
                    tracing::info!(
                        src = %src,
                        origin = origin,
                        "Managed venvs will editable-install remotemedia"
                    );
                }
                None => {
                    config.base_deps = vec!["remotemedia-client".to_string()];
                    tracing::info!(
                        "Could not locate local remotemedia Python client. \
                         Managed venvs will install 'remotemedia-client' from PyPI by default."
                    );
                }
            }
        }

        let cache_dir = config.cache_dir.clone().unwrap_or_else(default_cache_dir);

        let backend: Arc<dyn EnvBackend> = match config.mode {
            PythonEnvMode::System => Arc::new(system_backend::SystemBackend::new()),
            PythonEnvMode::Managed | PythonEnvMode::ManagedWithPython => {
                // Try uv first, fall back to system
                #[cfg(feature = "bundled-uv")]
                {
                    match uv_backend::UvBackend::new() {
                        Ok(uv) => Arc::new(uv),
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "uv not available, falling back to system venv+pip"
                            );
                            Arc::new(system_backend::SystemBackend::new())
                        }
                    }
                }
                #[cfg(not(feature = "bundled-uv"))]
                {
                    tracing::info!("bundled-uv feature not enabled, using system venv+pip");
                    Arc::new(system_backend::SystemBackend::new())
                }
            }
        };

        let cache = VenvCache::new(cache_dir, config.max_cached_envs);

        Ok(Self {
            backend,
            config,
            cache,
        })
    }

    /// Ensure a virtual environment exists with the given dependencies.
    ///
    /// This is the main entry point. It will:
    /// 1. Compute a cache key from the python version + sorted deps
    /// 2. Return a cached environment if one matches
    /// 3. Otherwise create a new venv, install deps, and cache it
    pub async fn ensure_env(&self, deps: &[String]) -> Result<VenvInfo> {
        self.ensure_env_scoped(deps, None).await
    }

    /// Ensure a virtual environment exists with an internally-derived scope context.
    ///
    /// `scope_context` is not user-facing. Callers derive it from manifest
    /// scope policy plus runtime identity, such as session or node identity.
    pub async fn ensure_env_scoped(
        &self,
        deps: &[String],
        scope_context: Option<&str>,
    ) -> Result<VenvInfo> {
        // Prepend `base_deps` — e.g. the `remotemedia` client itself —
        // so every provisioned venv can import the package that defines
        // the multiprocess nodes. Different `base_deps` values produce
        // different cache keys naturally (via the sorted-dep hash),
        // so a dev machine that swaps `REMOTEMEDIA_PYTHON_SRC` doesn't
        // get a stale cached venv.
        let merged: Vec<String> = if self.config.base_deps.is_empty() {
            deps.to_vec()
        } else {
            let mut v = Vec::with_capacity(self.config.base_deps.len() + deps.len());
            v.extend(self.config.base_deps.iter().cloned());
            v.extend(deps.iter().cloned());
            v
        };
        self.cache
            .get_or_create(
                &merged,
                &self.config.python_version,
                scope_context,
                self.backend.as_ref(),
            )
            .await
    }

    /// Install additional dependencies into an existing virtual environment.
    ///
    /// This is used as a fallback when node-declared deps are discovered
    /// after the initial venv was created (e.g., via the DEPS control channel).
    /// The deps are installed into the given venv without recreating it.
    pub async fn install_additional_deps(&self, venv: &VenvInfo, deps: &[String]) -> Result<()> {
        if deps.is_empty() {
            return Ok(());
        }
        tracing::info!(
            cache_key = %venv.cache_key,
            deps = ?deps,
            "Installing additional dependencies into existing venv"
        );
        self.backend.install_deps(venv, deps).await?;
        validate_installed_deps(&venv.python_executable, deps).await;
        Ok(())
    }

    /// Get the current configuration.
    pub fn config(&self) -> &PythonEnvConfig {
        &self.config
    }
}

/// Discover the in-tree `clients/python/` source if this crate is still
/// sitting inside its original workspace.
///
/// `CARGO_MANIFEST_DIR` is baked in at compile time and points at the
/// absolute path of `crates/py-env-manager/` on the build host. From
/// there, the SDK's Python client lives two directories up at
/// `clients/python/`. We require `setup.py` to actually exist before
/// trusting the path — a binary shipped to a deploy host will still
/// have the baked-in build path, but the directory won't exist there.
fn discover_in_tree_python_src() -> Option<PathBuf> {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let candidate = crate_dir.parent()?.parent()?.join("clients").join("python");
    if candidate.join("setup.py").is_file() {
        return Some(candidate);
    }

    // Check sibling repo remotemedia-sdk
    if let Some(parent) = crate_dir.parent() {
        if let Some(grandparent) = parent.parent() {
            if let Some(great_grandparent) = grandparent.parent() {
                let sibling_candidate = great_grandparent
                    .join("remotemedia-sdk")
                    .join("clients")
                    .join("python");
                if sibling_candidate.join("setup.py").is_file() {
                    return Some(sibling_candidate);
                }
            }
        }
    }

    None
}

/// Default cache directory: `~/.config/remotemedia/envs/`
fn default_cache_dir() -> PathBuf {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(home)
        .join(".config")
        .join("remotemedia")
        .join("envs")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_package_name() {
        assert_eq!(normalize_package_name("My_Package"), "my-package");
        assert_eq!(normalize_package_name("my-package"), "my-package");
        assert_eq!(normalize_package_name("MY.PACKAGE"), "my-package");
        assert_eq!(normalize_package_name("numpy"), "numpy");
        assert_eq!(normalize_package_name("Scikit_Learn"), "scikit-learn");
    }

    #[test]
    fn test_extract_package_name() {
        assert_eq!(extract_package_name("numpy>=1.21"), "numpy");
        assert_eq!(extract_package_name("my-package[extra]"), "my-package");
        assert_eq!(extract_package_name("torch==2.0"), "torch");
        assert_eq!(extract_package_name("simple"), "simple");
        assert_eq!(extract_package_name("pkg ; python_version>='3.8'"), "pkg");
    }

    #[test]
    fn test_merge_deps_basic() {
        let node = vec!["numpy>=1.21".to_string(), "scipy".to_string()];
        let manifest = vec!["numpy>=1.24".to_string()]; // overrides node
        let extra = vec!["pytest".to_string()];

        let merged = merge_deps(&node, &manifest, &extra);
        assert_eq!(merged.len(), 3);
        // numpy should be the manifest version
        assert!(merged.contains(&"numpy>=1.24".to_string()));
        assert!(merged.contains(&"scipy".to_string()));
        assert!(merged.contains(&"pytest".to_string()));
    }

    #[test]
    fn test_merge_deps_normalized_override() {
        let node = vec!["My_Package>=1.0".to_string()];
        let manifest = vec!["my-package>=2.0".to_string()];
        let extra: Vec<String> = vec![];

        let merged = merge_deps(&node, &manifest, &extra);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0], "my-package>=2.0");
    }

    #[test]
    fn test_extract_marker() {
        assert_eq!(extract_marker("torch>=2.1"), "");
        assert_eq!(extract_marker("numpy"), "");
        assert_eq!(
            extract_marker("torch>=2.1 ; sys_platform == 'win32'"),
            "sys_platform == 'win32'"
        );
        assert_eq!(
            extract_marker("torch @ https://x/y.whl ; python_version == '3.12'"),
            "python_version == '3.12'"
        );
    }

    /// Regression: moss-tts-realtime declared both
    ///   - `torchaudio>=2.1` via `@python_requires` (cross-platform fallback)
    ///   - `torchaudio @ ...win_amd64.whl ; sys_platform == 'win32' ...`
    ///     via PEP 723 (Windows-only cu128 wheel)
    /// The old name-only dedup overwrote the unconditional entry with
    /// the win32-marked one, leaving Linux venvs with no torchaudio.
    /// Both must survive the merge so uv can route per-platform.
    #[test]
    fn test_merge_deps_keeps_marker_variants() {
        let node = vec![
            "torch>=2.1".to_string(),
            "torchaudio>=2.1".to_string(),
            "torch @ https://x/torch-win.whl ; sys_platform == 'win32'".to_string(),
            "torchaudio @ https://x/torchaudio-win.whl ; sys_platform == 'win32'".to_string(),
        ];
        let merged = merge_deps(&node, &[], &[]);
        assert_eq!(merged.len(), 4, "all 4 entries should survive: {merged:?}");
        assert!(merged.iter().any(|d| d == "torch>=2.1"));
        assert!(merged.iter().any(|d| d == "torchaudio>=2.1"));
        assert!(merged
            .iter()
            .any(|d| d.starts_with("torch @") && d.contains("sys_platform")));
        assert!(merged
            .iter()
            .any(|d| d.starts_with("torchaudio @") && d.contains("sys_platform")));
    }

    /// Same marker (or both unmarked) ⇒ last-write-wins, preserving
    /// the documented "manifest_deps override node_deps" contract for
    /// plain version-pin overrides.
    #[test]
    fn test_merge_deps_same_marker_still_overrides() {
        let node = vec!["torch>=2.1".to_string()];
        let manifest = vec!["torch>=2.5".to_string()];
        let merged = merge_deps(&node, &manifest, &[]);
        assert_eq!(merged, vec!["torch>=2.5".to_string()]);
    }

    /// Identical marker strings collapse, different markers don't.
    #[test]
    fn test_merge_deps_identical_markers_collapse() {
        let node = vec![
            "torch>=2.1 ; sys_platform == 'win32'".to_string(),
            "torch>=2.5 ; sys_platform == 'win32'".to_string(),
            "torch>=2.1 ; sys_platform == 'linux'".to_string(),
        ];
        let merged = merge_deps(&node, &[], &[]);
        assert_eq!(merged.len(), 2, "win32 collapsed, linux kept: {merged:?}");
        assert!(merged
            .iter()
            .any(|d| d == "torch>=2.5 ; sys_platform == 'win32'"));
        assert!(merged
            .iter()
            .any(|d| d == "torch>=2.1 ; sys_platform == 'linux'"));
    }

    #[test]
    fn test_merge_deps_sorted() {
        let node: Vec<String> = vec![];
        let manifest: Vec<String> = vec![];
        let extra = vec![
            "z-package".to_string(),
            "a-package".to_string(),
            "m-package".to_string(),
        ];

        let merged = merge_deps(&node, &manifest, &extra);
        assert_eq!(merged, vec!["a-package", "m-package", "z-package"]);
    }

    #[test]
    fn test_cache_key_deterministic() {
        let deps = vec!["numpy>=1.21".to_string(), "scipy".to_string()];
        let key1 = VenvCache::cache_key("3.11", &deps, None);
        let key2 = VenvCache::cache_key("3.11", &deps, None);
        assert_eq!(key1, key2);
        assert_eq!(key1.len(), 16); // 8 bytes = 16 hex chars
    }

    #[test]
    fn test_cache_key_order_independent() {
        let deps1 = vec!["scipy".to_string(), "numpy".to_string()];
        let deps2 = vec!["numpy".to_string(), "scipy".to_string()];
        let key1 = VenvCache::cache_key("3.11", &deps1, None);
        let key2 = VenvCache::cache_key("3.11", &deps2, None);
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_cache_key_version_matters() {
        let deps = vec!["numpy".to_string()];
        let key1 = VenvCache::cache_key("3.11", &deps, None);
        let key2 = VenvCache::cache_key("3.12", &deps, None);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_cache_key_scope_context_matters() {
        let deps = vec!["numpy".to_string()];
        let global = VenvCache::cache_key("3.11", &deps, None);
        let node_a = VenvCache::cache_key("3.11", &deps, Some("session:s1;node:a"));
        let node_b = VenvCache::cache_key("3.11", &deps, Some("session:s1;node:b"));
        assert_ne!(global, node_a);
        assert_ne!(node_a, node_b);
    }

    #[test]
    fn test_python_env_mode_serde() {
        let json = serde_json::to_string(&PythonEnvMode::ManagedWithPython).unwrap();
        assert_eq!(json, "\"managed_with_python\"");
        let mode: PythonEnvMode = serde_json::from_str(&json).unwrap();
        assert_eq!(mode, PythonEnvMode::ManagedWithPython);
    }

    #[test]
    fn test_env_scope_serde() {
        let json = serde_json::to_string(&EnvScope::PerPipeline).unwrap();
        assert_eq!(json, "\"per_pipeline\"");
        let scope: EnvScope = serde_json::from_str(&json).unwrap();
        assert_eq!(scope, EnvScope::PerPipeline);
    }

    #[test]
    fn test_default_config() {
        let config = PythonEnvConfig::default();
        assert_eq!(config.mode, PythonEnvMode::Managed);
        assert_eq!(config.scope, EnvScope::Global);
        assert_eq!(config.python_version, "3.11");
        assert_eq!(config.max_cached_envs, 8);
        assert!(config.cache_dir.is_none());
    }
}
