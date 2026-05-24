//! Python plugin factory for loadable-node cdylibs.
//!
//! Enables shipping a Python streaming node (e.g. `MossTTSRealtimeNode`)
//! as a single `.so`/`.dylib`/`.dll`. The cdylib embeds the Python
//! source via `include_dir!`, extracts it to a per-plugin cache on first
//! load, provisions a uv-managed venv from the module's PEP 723 deps via
//! [`remotemedia_py_env`], and spawns the existing
//! `clients/python/remotemedia/core/multiprocessing/runner.py` against
//! that venv. IPC reuses the same iceoryx2 channel shape as the in-host
//! multiprocess executor (`{session_id}_{node_id}_input/output`).
//!
//! See [`crate::python_plugin_export`] for the author-facing macro.
//!
//! Gated behind the `python-plugin` cargo feature so Rust-only plugins
//! don't pay for the `include_dir` / `py-env-manager` / `dirs` dep
//! weight.

use std::path::PathBuf;
use std::sync::Arc;

use abi_stable::std_types::{RErr, ROk, RResult, RString, RVec};
use async_ffi::{FfiFuture, FutureExt};
use include_dir::Dir;
use loadable_node_abi::{FfiNode, FfiNodeBox, FfiNodeFactory, FfiNodeFactory_TO};
use remotemedia_py_env::{PythonEnvConfig, PythonEnvManager, VenvInfo};
use sha2::{Digest, Sha256};

/// Argv contract version the plugin-side helper emits. Matches
/// `RUNNER_PROTOCOL_VERSION` in
/// `clients/python/remotemedia/core/multiprocessing/runner.py`.
///
/// Bumped only when the argv surface changes incompatibly. Plugins built
/// against a newer version against an older runner will still attempt to
/// spawn — argparse silently ignores unknown flags only when explicitly
/// asked to, so a mismatch surfaces immediately as a hard
/// `unrecognized arguments` error from argparse. That is the desired
/// fail-loud behaviour.
pub const RUNNER_PROTOCOL_VERSION: u32 = 1;

/// PEP 723 dependency extractor.
///
/// Parses a Python source string and returns the literal list of
/// requirement specifiers from the most recent `@python_requires([...])`
/// decorator. Returns `Vec::new()` when no decorator is present or when
/// the argument is not a syntactically obvious string-literal list.
///
/// This is a deliberately narrow parser — it handles the format the
/// in-tree nodes use (literal lists of `"requirement"` strings, possibly
/// across multiple lines, possibly with `#` comments). It does NOT
/// resolve a bare-name constant (`@python_requires(_MY_REQUIRES)`) — see
/// the precedent in the project ([`moss_tts_realtime.py`] explicitly
/// inlines the literal list at the decorator site for exactly this
/// reason).
///
/// [`moss_tts_realtime.py`]: clients/python/remotemedia/nodes/ml/moss_tts_realtime.py
///
/// Returned strings are exactly as they appeared in source (unquoted),
/// with surrounding whitespace trimmed. Order is preserved.
pub fn extract_python_requires(src: &str) -> Vec<String> {
    let needle = "@python_requires(";
    let mut last_match: Option<&str> = None;
    let mut search = src;
    while let Some(idx) = search.find(needle) {
        last_match = Some(&search[idx + needle.len()..]);
        search = &search[idx + needle.len()..];
    }
    let Some(after_open) = last_match else {
        return Vec::new();
    };
    // Find the matching close-paren, accounting for nesting depth and
    // single-quoted / double-quoted / triple-quoted string content.
    let bytes = after_open.as_bytes();
    let mut depth: i32 = 1;
    let mut i = 0usize;
    let mut quote: Option<u8> = None; // current string-quote character if inside a string
    let mut end = None;
    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = quote {
            if b == q {
                quote = None;
            } else if b == b'\\' && i + 1 < bytes.len() {
                i += 1; // skip escaped char
            }
        } else {
            match b {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(i);
                        break;
                    }
                }
                b'#' => {
                    // Skip to end of line.
                    while i < bytes.len() && bytes[i] != b'\n' {
                        i += 1;
                    }
                    continue;
                }
                b'"' | b'\'' => quote = Some(b),
                _ => {}
            }
        }
        i += 1;
    }
    let Some(end) = end else {
        return Vec::new();
    };
    let inside = &after_open[..end];
    // Inside the parens we expect `[...]` (a list literal). Strip optional
    // surrounding `[ ... ]`; tolerate raw comma-separated strings too.
    let inside = inside.trim();
    let inside = inside.strip_prefix('[').unwrap_or(inside);
    let inside = inside.strip_suffix(']').unwrap_or(inside);
    // Now extract every string literal. Each is delimited by matching
    // `"` or `'`. Tolerate `\"` and `\'` escapes inside. Trim each.
    let mut out: Vec<String> = Vec::new();
    let bytes = inside.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'#' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b == b'"' || b == b'\'' {
            let quote_char = b;
            let start = i + 1;
            i += 1;
            let mut buf = String::new();
            while i < bytes.len() {
                let c = bytes[i];
                if c == b'\\' && i + 1 < bytes.len() {
                    buf.push(bytes[i + 1] as char);
                    i += 2;
                    continue;
                }
                if c == quote_char {
                    break;
                }
                buf.push(c as char);
                i += 1;
            }
            let _ = start; // start kept for clarity; buf already holds content
            let trimmed = buf.trim().to_string();
            if !trimmed.is_empty() {
                out.push(trimmed);
            }
            i += 1; // skip closing quote
            continue;
        }
        i += 1;
    }
    out
}

/// Stable content hash of an embedded `include_dir!` tree. Used as the
/// extraction cache key under `~/.cache/remotemedia/plugins/<hash>/`.
///
/// Hashes file paths + bytes deterministically. Independent of build
/// timestamps so identical source → identical hash across rebuilds.
pub fn hash_embedded_dir(dir: &Dir<'_>) -> String {
    let mut hasher = Sha256::new();
    fn walk(dir: &Dir<'_>, hasher: &mut Sha256) {
        // Sort children by path so iteration order is stable.
        let mut files: Vec<_> = dir.files().collect();
        files.sort_by_key(|f| f.path());
        for f in files {
            hasher.update(f.path().to_string_lossy().as_bytes());
            hasher.update(b"\0");
            hasher.update(f.contents());
            hasher.update(b"\0");
        }
        let mut subdirs: Vec<_> = dir.dirs().collect();
        subdirs.sort_by_key(|d| d.path());
        for d in subdirs {
            walk(d, hasher);
        }
    }
    walk(dir, &mut hasher);
    let digest = hasher.finalize();
    hex::encode(&digest[..16]) // 32 hex chars, plenty for cache disambiguation
}

/// Resolve the on-disk extraction directory for a plugin bundle. Created
/// at `~/.cache/remotemedia/plugins/<hash>/` (override via the
/// `REMOTEMEDIA_PLUGIN_CACHE` env var when supplied).
pub fn plugin_cache_dir(hash: &str) -> std::io::Result<PathBuf> {
    let base = if let Ok(custom) = std::env::var("REMOTEMEDIA_PLUGIN_CACHE") {
        PathBuf::from(custom)
    } else {
        dirs::cache_dir()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "no user cache dir on this platform",
                )
            })?
            .join("remotemedia")
            .join("plugins")
    };
    let target = base.join(hash);
    std::fs::create_dir_all(&target)?;
    Ok(target)
}

/// Extract an embedded directory tree to disk if any file is missing or
/// stale. Cheap to re-call on subsequent loads — the hash-stable cache
/// directory means re-extraction is a no-op when the content matches.
pub fn extract_embedded_dir(dir: &Dir<'_>, target: &std::path::Path) -> std::io::Result<()> {
    dir.extract(target)
}

/// Factory configuration baked at compile time by the
/// [`crate::python_plugin_export!`] macro.
#[derive(Clone)]
pub struct PythonPluginConfig {
    /// abi_stable factory name (matches the registered `node_type` the
    /// pipeline references).
    pub node_type: &'static str,
    /// Bare Python module name to import in the runner (e.g.
    /// `"moss_tts_realtime"`). Resolved against the extracted directory
    /// via `--module-root`.
    pub module: &'static str,
    /// Python class within the module. Currently informational; the
    /// runner looks up nodes by registered `node_type`, not class name,
    /// but the class is stored for diagnostics + future lookup paths.
    pub class: &'static str,
    /// Embedded source tree (produced by `include_dir!`). The cdylib's
    /// only payload.
    pub embedded: &'static Dir<'static>,
}

/// Factory that the macro `inventory::submit!`s per plugin. Implements
/// abi_stable's [`FfiNodeFactory`] so the host's
/// `LoadableNodeBundle::load()` can register it without knowing anything
/// about Python.
pub struct PythonSubprocessFactory {
    config: Arc<PythonPluginConfig>,
}

impl PythonSubprocessFactory {
    pub fn new(config: PythonPluginConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    pub fn boxed(config: PythonPluginConfig) -> FfiNodeFactoryBoxAlias {
        FfiNodeFactory_TO::from_value(Self::new(config), abi_stable::sabi_trait::TD_Opaque)
    }

    pub fn config(&self) -> &PythonPluginConfig {
        &self.config
    }
}

/// Alias for the boxed factory type — keeps macro expansion readable.
pub type FfiNodeFactoryBoxAlias = loadable_node_abi::FfiNodeFactoryBox;

impl FfiNodeFactory for PythonSubprocessFactory {
    fn node_type(&self) -> RString {
        RString::from(self.config.node_type.to_string())
    }

    fn create(&self, params: RString) -> RResult<FfiNodeBox, RString> {
        let provisioning = match provision_plugin_env_blocking(&self.config) {
            Ok(p) => p,
            Err(e) => {
                return RErr(RString::from(format!(
                    "PythonSubprocessFactory::create — provisioning failed: {e}"
                )))
            }
        };

        // Conventions:
        // - one subprocess per `create()` call (per node instance)
        // - session_id is unique per spawn (PID + monotonic suffix)
        //   so iceoryx2 channels never collide with stale services
        //   from a crashed previous run. The host doesn't currently
        //   pass a session_id through to FFI factory create() — when
        //   it does, prefer that over the synthetic one.
        // - node_id derives from the factory's node_type — fine for
        //   the one-node-per-cdylib case (which is the common shape).
        let session_id = format!(
            "rmplug-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros())
                .unwrap_or(0),
        );
        let node_id = format!("{}-1", self.config.node_type);
        let params_json: String = params.as_str().to_string();
        let params_json = if params_json.is_empty() {
            "{}".to_string()
        } else {
            params_json
        };

        let argv = build_runner_argv(
            &provisioning.venv.python_executable,
            std::path::Path::new(""), // unused (-m form)
            &provisioning.extracted_dir,
            self.config.module,
            self.config.node_type,
            &node_id,
            &session_id,
            &params_json,
            None,
        );

        let (cmd_tx, child) =
            match crate::python_ipc::spawn_runner_and_ipc(&argv, &session_id, &node_id) {
                Ok(pair) => pair,
                Err(e) => {
                    return RErr(RString::from(format!(
                        "PythonSubprocessFactory::create — spawn/IPC failed: {e}"
                    )))
                }
            };

        let node = PythonSubprocessNode {
            node_type: self.config.node_type.to_string(),
            session_id,
            cmd_tx,
            _child: child,
        };
        ROk(loadable_node_abi::FfiNode_TO::from_value(
            node,
            abi_stable::sabi_trait::TD_Opaque,
        ))
    }
}

/// Construct the runner argv conforming to RUNNER_PROTOCOL_VERSION 1
/// (see `clients/python/remotemedia/core/multiprocessing/runner.py`).
///
/// The runner is invoked via `python -m remotemedia.core.multiprocessing.runner`
/// — matches what `process_manager.rs::spawn_node` does for the in-host
/// path. The `runner_script` parameter is kept for documentation / test
/// purposes but is NOT used in the emitted argv (the `-m` form bypasses
/// any need for a runner-script-path).
///
/// Pure function — no IO. Test-friendly and the single source of truth
/// for the argv shape the factory hands to `Command::new`.
pub fn build_runner_argv(
    python_executable: &std::path::Path,
    _runner_script: &std::path::Path,
    module_root: &std::path::Path,
    module: &str,
    node_type: &str,
    node_id: &str,
    session_id: &str,
    params_json: &str,
    ipc_config_json: Option<&str>,
) -> Vec<String> {
    let mut argv: Vec<String> = vec![
        python_executable.to_string_lossy().into_owned(),
        "-m".into(),
        "remotemedia.core.multiprocessing.runner".into(),
        "--node-type".into(),
        node_type.into(),
        "--node-id".into(),
        node_id.into(),
        "--session-id".into(),
        session_id.into(),
        "--module-root".into(),
        module_root.to_string_lossy().into_owned(),
        "--register-module".into(),
        module.into(),
        "--params".into(),
        params_json.into(),
    ];
    if let Some(ipc) = ipc_config_json {
        argv.push("--ipc-config".into());
        argv.push(ipc.into());
    }
    argv
}

/// Result of a successful plugin provisioning step. Carries everything
/// the subsequent subprocess-spawn step needs: the path to the
/// extracted Python source tree, the deps that were resolved, and the
/// venv interpreter to invoke the runner with.
#[derive(Debug, Clone)]
pub struct PluginProvisioning {
    /// Stable content-hash cache key for the embedded directory.
    pub hash: String,
    /// Directory the embedded source was extracted into.
    /// `~/.cache/remotemedia/plugins/<hash>/` by default.
    pub extracted_dir: PathBuf,
    /// PEP 723 deps parsed from the primary module's
    /// `@python_requires([...])` decorator.
    pub deps: Vec<String>,
    /// Venv info returned by [`PythonEnvManager::ensure_env`].
    /// `python_executable` is the path to invoke for the runner.
    pub venv: VenvInfo,
}

/// Async helper that performs the full plugin-side provisioning flow
/// for one [`PythonPluginConfig`]:
///
/// 1. Hash the embedded directory.
/// 2. Extract to `~/.cache/remotemedia/plugins/<hash>/` (idempotent).
/// 3. Read the primary module file from the extracted tree.
/// 4. Parse PEP 723 `@python_requires([...])` deps.
/// 5. Provision (or reuse) a uv-managed venv via [`PythonEnvManager`].
///
/// Returns a [`PluginProvisioning`] that the next phase (subprocess
/// spawn) consumes.
///
/// # Errors
///
/// Returns an error message string suitable for forwarding across the
/// abi_stable FFI boundary via [`RString`]. The factory's `create()`
/// uses this directly.
pub async fn provision_plugin_env(
    config: &PythonPluginConfig,
) -> Result<PluginProvisioning, String> {
    let hash = hash_embedded_dir(config.embedded);
    let extracted_dir =
        plugin_cache_dir(&hash).map_err(|e| format!("plugin_cache_dir({hash}): {e}"))?;
    extract_embedded_dir(config.embedded, &extracted_dir)
        .map_err(|e| format!("extract_embedded_dir into {}: {e}", extracted_dir.display()))?;

    let module_file = extracted_dir.join(format!("{}.py", config.module));
    let module_src = std::fs::read_to_string(&module_file)
        .map_err(|e| format!("read_to_string({}): {e}", module_file.display()))?;
    let deps = extract_python_requires(&module_src);

    let env_config = PythonEnvConfig::default();
    let env_mgr =
        PythonEnvManager::new(env_config).map_err(|e| format!("PythonEnvManager::new: {e}"))?;
    let venv = env_mgr
        .ensure_env(&deps)
        .await
        .map_err(|e| format!("PythonEnvManager::ensure_env({deps:?}): {e}"))?;

    Ok(PluginProvisioning {
        hash,
        extracted_dir,
        deps,
        venv,
    })
}

/// Sync wrapper around [`provision_plugin_env`] for callers that can't
/// `.await` (e.g. abi_stable's sync [`FfiNodeFactory::create`]).
///
/// Spins up a single-threaded tokio runtime per call. That's fine for
/// the one-shot provisioning case — `ensure_env` is dominated by
/// network/disk IO that an unshared runtime handles cleanly, and we
/// don't want to share global state across cdylib boundaries.
pub fn provision_plugin_env_blocking(
    config: &PythonPluginConfig,
) -> Result<PluginProvisioning, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("build provisioning runtime: {e}"))?;
    rt.block_on(provision_plugin_env(config))
}

/// Spawn the Python runner subprocess against a previously-provisioned
/// venv + extracted source directory. Returns the IPC channel + child
/// handle the caller uses to drive round-trips and (via Drop) kill the
/// subprocess.
///
/// This is the lower-level helper used by both:
///
/// - The cdylib path: [`PythonSubprocessFactory::create`] (via
///   `spawn_runner_and_ipc` directly).
/// - The OnDisk source-load path in core's `loadable::resolver` —
///   external callers construct argv via [`build_runner_argv`], call
///   this helper, and wrap the returned channel in their own
///   `StreamingNode` adapter.
///
/// `session_id` should be unique per spawn to avoid stale iceoryx2
/// service collisions. Convention: `format!("rmplug-{}-{}", pid,
/// timestamp_us)` for the cdylib path; core picks its own scheme.
///
/// Performs the READY handshake before returning — see
/// `python_ipc::spawn_runner_and_ipc` for timeout configuration.
pub fn spawn_python_subprocess(
    provisioning: &PluginProvisioning,
    module: &str,
    node_type: &str,
    node_id: &str,
    session_id: &str,
    params_json: &str,
) -> Result<
    (
        tokio::sync::mpsc::Sender<crate::python_ipc::IpcCommand>,
        std::sync::Arc<std::sync::Mutex<std::process::Child>>,
    ),
    String,
> {
    let argv = build_runner_argv(
        &provisioning.venv.python_executable,
        std::path::Path::new(""),
        &provisioning.extracted_dir,
        module,
        node_type,
        node_id,
        session_id,
        params_json,
        None,
    );
    crate::python_ipc::spawn_runner_and_ipc(&argv, session_id, node_id)
}

/// Async variant for the OnDisk source path (Python source-load plugins
/// resolved by core's `loadable::resolver`).
///
/// The cdylib-embedded path runs PEP 723 dep extraction against the
/// embedded `.py` source. The OnDisk path SKIPS that step because the
/// resolver already parsed `plugin.toml` for the dep list — pass it in
/// directly via `deps`.
///
/// `module_root` is the directory containing `<entry_module>.py`.
/// `hash` should be a stable content-derived identifier (the resolver
/// uses the tarball's SHA256) — used as the cache key for venv reuse.
///
/// Returns a [`PluginProvisioning`] in the same shape the cdylib path
/// produces, so downstream subprocess-spawn code is identical.
pub async fn provision_plugin_env_from_path(
    module_root: PathBuf,
    deps: Vec<String>,
    hash: String,
) -> Result<PluginProvisioning, String> {
    let env_config = PythonEnvConfig::default();
    let env_mgr =
        PythonEnvManager::new(env_config).map_err(|e| format!("PythonEnvManager::new: {e}"))?;
    let venv = env_mgr
        .ensure_env(&deps)
        .await
        .map_err(|e| format!("PythonEnvManager::ensure_env({deps:?}): {e}"))?;

    Ok(PluginProvisioning {
        hash,
        extracted_dir: module_root,
        deps,
        venv,
    })
}

/// FFI-side node handle returned by the factory. Owns the spawned
/// Python subprocess (killed on drop) and a `mpsc::Sender<IpcCommand>`
/// onto the dedicated IPC thread that runs the iceoryx2 publisher /
/// subscriber.
///
/// `process` translates: `RVec<u8>` (rmp-serde RuntimeData from the
/// host) → [`crate::python_ipc::WireRuntimeData`] (the runner's wire
/// format) → iceoryx2 → first emitted output → rmp-serde back.
///
/// v1 scope (per `python_ipc` module docs):
/// - Text-only round-trip.
/// - Single output per input (drops further emissions until the
///   multi-output drain lands).
/// - No READY handshake (500ms sleep at spawn).
pub struct PythonSubprocessNode {
    pub(crate) node_type: String,
    pub session_id: String,
    pub cmd_tx: tokio::sync::mpsc::Sender<crate::python_ipc::IpcCommand>,
    /// Held so the child is killed when the node is dropped.
    pub _child: std::sync::Arc<std::sync::Mutex<std::process::Child>>,
}

impl PythonSubprocessNode {
    /// Construct directly from a freshly-spawned subprocess + IPC
    /// channel. Used by core's source-load resolver to wrap the
    /// result of [`spawn_python_subprocess`] without going through
    /// the abi_stable factory path.
    pub fn new(
        node_type: String,
        session_id: String,
        cmd_tx: tokio::sync::mpsc::Sender<crate::python_ipc::IpcCommand>,
        child: std::sync::Arc<std::sync::Mutex<std::process::Child>>,
    ) -> Self {
        Self {
            node_type,
            session_id,
            cmd_tx,
            _child: child,
        }
    }
}

impl FfiNode for PythonSubprocessNode {
    fn node_type(&self) -> RString {
        RString::from(self.node_type.clone())
    }

    fn process(&self, input: RVec<u8>) -> FfiFuture<RResult<RVec<u8>, RString>> {
        let cmd_tx = self.cmd_tx.clone();
        let session_id = self.session_id.clone();
        async move {
            let req_bytes = match encode_input(&input, &session_id) {
                Ok(b) => b,
                Err(e) => return RErr(RString::from(e)),
            };

            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            if let Err(e) = cmd_tx
                .send(crate::python_ipc::IpcCommand::Round {
                    req_bytes,
                    reply: reply_tx,
                })
                .await
            {
                return RErr(RString::from(format!("ipc cmd send: {e}")));
            }
            let resp_bytes = match reply_rx.await {
                Ok(Ok(b)) => b,
                Ok(Err(e)) => return RErr(RString::from(format!("ipc reply: {e}"))),
                Err(e) => return RErr(RString::from(format!("ipc reply recv: {e}"))),
            };
            match decode_output_to_ffi(&resp_bytes) {
                Ok(bytes) => ROk(RVec::from(bytes)),
                Err(e) => RErr(RString::from(e)),
            }
        }
        .into_ffi()
    }

    /// Multi-output drain: collects every frame the runner emits for
    /// this input until the `EndOfInput` sentinel arrives. Returns each
    /// emission as a separately-encoded rmp-serde RuntimeData blob.
    ///
    /// Streaming Python nodes (TTS audio chunks, STT segments, …) need
    /// this — `process` would return only their first emission.
    fn process_multi(&self, input: RVec<u8>) -> FfiFuture<RResult<RVec<RVec<u8>>, RString>> {
        let cmd_tx = self.cmd_tx.clone();
        let session_id = self.session_id.clone();
        async move {
            let req_bytes = match encode_input(&input, &session_id) {
                Ok(b) => b,
                Err(e) => return RErr(RString::from(e)),
            };

            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            if let Err(e) = cmd_tx
                .send(crate::python_ipc::IpcCommand::RoundMulti {
                    req_bytes,
                    reply: reply_tx,
                })
                .await
            {
                return RErr(RString::from(format!("ipc cmd send: {e}")));
            }
            let frames = match reply_rx.await {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => return RErr(RString::from(format!("ipc reply: {e}"))),
                Err(e) => return RErr(RString::from(format!("ipc reply recv: {e}"))),
            };

            let mut out: Vec<RVec<u8>> = Vec::with_capacity(frames.len());
            for frame in frames {
                match decode_output_to_ffi(&frame) {
                    Ok(bytes) => out.push(RVec::from(bytes)),
                    Err(e) => return RErr(RString::from(e)),
                }
            }
            ROk(RVec::from(out))
        }
        .into_ffi()
    }
}

/// Decode the FFI-side rmp-serde RuntimeData → encode into the runner's
/// wire format. Variants supported: Text, Audio, Tensor, Video,
/// ControlMessage (matches the Python data.py wire types). Other
/// variants surface a precise error so the failure mode is obvious
/// from the host log.
fn encode_input(input: &RVec<u8>, session_id: &str) -> Result<Vec<u8>, String> {
    let public: remotemedia_types::RuntimeData =
        rmp_serde::from_slice(input.as_slice()).map_err(|e| format!("rmp decode input: {e}"))?;
    let wire = match public {
        remotemedia_types::RuntimeData::Text(t) => {
            crate::python_ipc::WireRuntimeData::now_text(&t, session_id)
        }
        remotemedia_types::RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            ..
        } => {
            // `samples` Deref<Target=[f32]>; channels narrows from u32→u16 (Python wire format).
            let ch = u16::try_from(channels).map_err(|_| {
                format!("audio channels {channels} doesn't fit u16 (wire format limit)")
            })?;
            crate::python_ipc::WireRuntimeData::now_audio(&samples, sample_rate, ch, session_id)
        }
        remotemedia_types::RuntimeData::Tensor {
            data,
            shape,
            dtype,
            metadata,
        } => {
            // Public `shape: Vec<i32>` widens to wire `Vec<u32>` (negative
            // dims would be a logic bug in the producing node).
            let shape_u32: Vec<u32> = shape
                .into_iter()
                .map(|d| u32::try_from(d).map_err(|_| format!("negative tensor dim: {d}")))
                .collect::<Result<_, _>>()?;
            let dtype_u8 =
                u8::try_from(dtype).map_err(|_| format!("tensor dtype {dtype} doesn't fit u8"))?;
            crate::python_ipc::WireRuntimeData::now_tensor(
                &data,
                &shape_u32,
                dtype_u8,
                metadata.as_ref(),
                session_id,
            )
        }
        remotemedia_types::RuntimeData::Video {
            pixel_data,
            width,
            height,
            format,
            codec,
            frame_number,
            is_keyframe,
            ..
        } => {
            // PixelFormat / VideoCodec → u8 discriminant matching the
            // Python data.py layout. Cast via `as u8` since both enums
            // are `#[repr(u8)]`.
            let format_u8 = format as u8;
            let codec_u8 = codec.map(|c| c as u8).unwrap_or(0);
            crate::python_ipc::WireRuntimeData::now_video(
                &pixel_data,
                width,
                height,
                format_u8,
                codec_u8,
                frame_number,
                is_keyframe,
                session_id,
            )
        }
        remotemedia_types::RuntimeData::ControlMessage {
            message_type,
            segment_id,
            timestamp_ms,
            metadata,
        } => {
            // Same JSON envelope shape as
            // `data_transfer::RuntimeData::control_message`.
            let payload = serde_json::json!({
                "message_type": message_type,
                "segment_id": segment_id,
                "timestamp_ms": timestamp_ms,
                "metadata": metadata,
            });
            crate::python_ipc::WireRuntimeData::now_control_message(&payload, session_id)
        }
        remotemedia_types::RuntimeData::Json(value) => {
            // Json → Text(JSON string) — matches the in-host pattern in
            // `multiprocess_executor::convert_to_ipc_runtime_data`. This
            // is what carries aux-port envelopes (`{ "__aux_port__":
            // "port_name", "payload": ... }` produced by
            // `transport::session_control::wrap_aux_port`) into Python
            // plugins. Python can recover the structured value with
            // `json.loads(data.as_text())`.
            let text =
                serde_json::to_string(&value).map_err(|e| format!("encode Json input: {e}"))?;
            crate::python_ipc::WireRuntimeData::now_text(&text, session_id)
        }
        other => {
            return Err(format!(
                "PythonSubprocessNode encodes only Text/Audio/Tensor/Video/ControlMessage/Json \
                 input (got variant {:?}). Numpy/Image/Binary/File have no Python wire type — \
                 route via Tensor or ControlMessage instead.",
                std::mem::discriminant(&other)
            ));
        }
    };
    Ok(wire.to_bytes())
}

/// Decode the runner's wire-format bytes → encode as rmp-serde
/// RuntimeData ready to hand back across the FFI boundary. Variants
/// supported: Text, Audio, Tensor, Video, ControlMessage.
fn decode_output_to_ffi(resp_bytes: &[u8]) -> Result<Vec<u8>, String> {
    let wire = crate::python_ipc::WireRuntimeData::from_bytes(resp_bytes)
        .map_err(|e| format!("wire decode: {e}"))?;
    let public = match wire.data_type {
        crate::python_ipc::WireDataType::Text => {
            let text = String::from_utf8_lossy(&wire.payload).into_owned();
            remotemedia_types::RuntimeData::Text(text)
        }
        crate::python_ipc::WireDataType::Audio => {
            let (samples, sample_rate, channels) = wire.decode_audio()?;
            remotemedia_types::RuntimeData::Audio {
                samples: remotemedia_types::AudioSamples::from(samples),
                sample_rate,
                channels: channels as u32,
                stream_id: None,
                timestamp_us: Some(wire.timestamp_us),
                arrival_ts_us: None,
                metadata: None,
            }
        }
        crate::python_ipc::WireDataType::Tensor => {
            let (data, shape, dtype, extras) = wire.decode_tensor()?;
            let metadata = match extras {
                serde_json::Value::Null => None,
                other => Some(other),
            };
            remotemedia_types::RuntimeData::Tensor {
                data,
                shape: shape.into_iter().map(|d| d as i32).collect(),
                dtype: dtype as i32,
                metadata,
            }
        }
        crate::python_ipc::WireDataType::Video => {
            let (width, height, format_u8, codec_u8, frame_number, is_keyframe, pixel_data) =
                wire.decode_video()?;
            // Public `PixelFormat` / `VideoCodec` are `#[repr(u8)]` but
            // we can't `unsafe { transmute }` u8 → enum without
            // assurance the discriminant is valid. Serde via JSON
            // discriminant byte → enum: round-trip through the JSON
            // wire shape Python emits. Cheaper alternative: a small
            // match table for the enum variants the Python runner
            // actually emits.
            let format = pixel_format_from_u8(format_u8)
                .ok_or_else(|| format!("unknown PixelFormat discriminant {format_u8}"))?;
            let codec = if codec_u8 == 0 {
                None
            } else {
                Some(
                    video_codec_from_u8(codec_u8)
                        .ok_or_else(|| format!("unknown VideoCodec discriminant {codec_u8}"))?,
                )
            };
            remotemedia_types::RuntimeData::Video {
                pixel_data,
                width,
                height,
                format,
                codec,
                frame_number,
                timestamp_us: wire.timestamp_us,
                is_keyframe,
                stream_id: None,
                arrival_ts_us: None,
            }
        }
        crate::python_ipc::WireDataType::ControlMessage => {
            let v = wire.decode_control_message()?;
            // Mirror the shape Python's `control_message()` ctor emits.
            let message_type: remotemedia_types::ControlMessageType =
                serde_json::from_value(v.get("message_type").cloned().unwrap_or_default())
                    .map_err(|e| format!("control message_type decode: {e}"))?;
            let segment_id = v
                .get("segment_id")
                .and_then(|x| x.as_str())
                .map(String::from);
            let timestamp_ms = v.get("timestamp_ms").and_then(|x| x.as_u64()).unwrap_or(0);
            let metadata = v
                .get("metadata")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            remotemedia_types::RuntimeData::ControlMessage {
                message_type,
                segment_id,
                timestamp_ms,
                metadata,
            }
        }
        other => {
            return Err(format!(
                "PythonSubprocessNode decodes only Text/Audio/Tensor/Video/ControlMessage \
                 output (got {other:?})"
            ));
        }
    };
    rmp_serde::to_vec_named(&public).map_err(|e| format!("rmp encode output: {e}"))
}

/// PixelFormat enum discriminant table. Matches
/// `remotemedia_types::PixelFormat`'s `#[repr(u8)]` discriminants
/// (defined in `crates/types/src/lib.rs`). Returns `None` on unknown
/// discriminants so callers surface a precise error rather than
/// transmuting into an invalid variant.
fn pixel_format_from_u8(b: u8) -> Option<remotemedia_types::PixelFormat> {
    use remotemedia_types::PixelFormat::*;
    Some(match b {
        1 => Yuv420p,
        2 => I420,
        3 => NV12,
        4 => Rgb24,
        5 => Rgba32,
        255 => Encoded,
        _ => return None,
    })
}

/// VideoCodec enum discriminant table — `0` indicates "no codec / raw"
/// in the wire format and is mapped to `None` at the call site (so the
/// returned `Option<VideoCodec>` matches the public type).
fn video_codec_from_u8(b: u8) -> Option<remotemedia_types::VideoCodec> {
    use remotemedia_types::VideoCodec::*;
    Some(match b {
        1 => Vp8,
        2 => H264,
        3 => Av1,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_basic_literal_list() {
        let src = r#"
@python_requires(["torch>=2.1", "transformers>=5.0,<6.0"])
class Foo: pass
"#;
        assert_eq!(
            extract_python_requires(src),
            vec!["torch>=2.1", "transformers>=5.0,<6.0"]
        );
    }

    #[test]
    fn extract_handles_multiline_with_comments() {
        let src = r#"
@python_requires([
    # cross-platform fallback
    "torch>=2.1",
    "torchaudio>=2.1",
    # comment between entries
    "accelerate>=0.33",
])
class Foo: pass
"#;
        assert_eq!(
            extract_python_requires(src),
            vec!["torch>=2.1", "torchaudio>=2.1", "accelerate>=0.33"]
        );
    }

    #[test]
    fn extract_handles_pep_508_markers_with_quotes() {
        // Real-world case from moss_tts_realtime.py — strings contain
        // ; and ' inside a marker. The closing-paren scanner must respect
        // string quoting.
        let src = r#"
@python_requires([
    "torch>=2.1",
    "torchaudio @ https://example.com/x.whl ; sys_platform == 'win32'",
])
"#;
        let deps = extract_python_requires(src);
        assert_eq!(deps.len(), 2, "got {:?}", deps);
        assert_eq!(deps[0], "torch>=2.1");
        assert!(deps[1].contains("torchaudio @"));
        assert!(deps[1].contains("sys_platform == 'win32'"));
    }

    #[test]
    fn extract_missing_decorator_returns_empty() {
        assert_eq!(
            extract_python_requires("class Foo: pass"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn extract_uses_last_decorator_when_multiple() {
        // Two @python_requires in one file (only legal if the latter
        // replaces the former — but we want the closer-to-class
        // decorator to win).
        let src = r#"
@python_requires(["wrong"])
@python_requires(["right"])
class Foo: pass
"#;
        assert_eq!(extract_python_requires(src), vec!["right"]);
    }

    /// End-to-end provisioning smoke against a real uv venv. Marked
    /// `#[ignore]` because it provisions a real venv (slow, network IO).
    /// Run with `cargo test -p remotemedia-plugin-sdk --features python-plugin -- --ignored`.
    ///
    /// What it validates:
    /// - `hash_embedded_dir` → stable cache key
    /// - `plugin_cache_dir` + `extract_embedded_dir` → on-disk source
    /// - `extract_python_requires` from the extracted module
    /// - `PythonEnvManager::ensure_env` against the parsed deps
    /// - Resulting `python_executable` exists and runs `import sys`.
    #[test]
    #[ignore = "provisions a real uv venv — run with --ignored"]
    fn provision_real_venv_smoke() {
        use include_dir::DirEntry;
        // Build an in-memory Dir tree for a trivial module declaring
        // zero deps so the provisioned venv stays small.
        // include_dir! at runtime isn't possible, so we use a const Dir
        // built from a real on-disk fixture.
        const EMBED: include_dir::Dir<'static> =
            include_dir::include_dir!("$CARGO_MANIFEST_DIR/tests/fixtures/echo_plugin_minimal");

        // Sanity-check the fixture exists.
        assert!(
            EMBED.get_file("echo_minimal.py").is_some(),
            "test fixture missing: tests/fixtures/echo_plugin_minimal/echo_minimal.py"
        );

        let config = PythonPluginConfig {
            node_type: "EchoMinimal",
            module: "echo_minimal",
            class: "EchoMinimal",
            embedded: &EMBED,
        };
        let provisioning = provision_plugin_env_blocking(&config)
            .expect("provisioning should succeed for a no-deps module");

        assert_eq!(provisioning.hash.len(), 32, "hash should be 32 hex chars");
        assert!(
            provisioning.extracted_dir.exists(),
            "extracted dir should exist"
        );
        assert!(
            provisioning.extracted_dir.join("echo_minimal.py").exists(),
            "primary module file should be extracted",
        );
        assert!(
            provisioning.venv.python_executable.exists(),
            "venv python should be a real file at {}",
            provisioning.venv.python_executable.display(),
        );

        // Smoke: the venv interpreter actually runs.
        let out = std::process::Command::new(&provisioning.venv.python_executable)
            .args(["-c", "import sys; print(sys.version_info[0])"])
            .output()
            .expect("venv python should execute");
        assert!(out.status.success(), "venv python -c failed: {:?}", out);
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "3");

        // include_dir API uses `DirEntry` — referenced only to silence
        // unused-import lint when the test is disabled.
        let _: Option<DirEntry> = None;
    }

    #[test]
    fn encode_input_json_becomes_text_for_aux_port() {
        // Aux-port envelope produced by
        // `transport::session_control::wrap_aux_port(port, ...)` — a
        // RuntimeData::Json shaped `{ "__aux_port__": ..., "payload": ... }`.
        // The plugin-sdk encoder turns this into a Text wire frame
        // carrying the JSON string, matching the in-host pattern. The
        // Python plugin recovers the structured value with json.loads.
        let envelope = remotemedia_types::RuntimeData::Json(serde_json::json!({
            "__aux_port__": "barge_in",
            "payload": {"reason": "user_interrupt", "ts_ms": 1_700_000_000_000u64},
        }));
        let rmp_bytes = rmp_serde::to_vec_named(&envelope).expect("rmp encode envelope");
        let input = RVec::from(rmp_bytes);
        let wire_bytes = encode_input(&input, "sess-aux").expect("encode_input");
        let wire =
            crate::python_ipc::WireRuntimeData::from_bytes(&wire_bytes).expect("decode wire bytes");
        assert_eq!(wire.data_type, crate::python_ipc::WireDataType::Text);
        assert_eq!(wire.session_id, "sess-aux");
        // The Text payload IS the JSON-stringified envelope. Re-parse to
        // confirm the round-trip preserves both the aux-port discriminant
        // and the inner payload.
        let parsed: serde_json::Value =
            serde_json::from_slice(&wire.payload).expect("text payload should be JSON");
        assert_eq!(parsed["__aux_port__"], "barge_in");
        assert_eq!(parsed["payload"]["reason"], "user_interrupt");
        assert_eq!(parsed["payload"]["ts_ms"], 1_700_000_000_000u64);
    }

    #[test]
    fn build_runner_argv_shape() {
        let argv = build_runner_argv(
            std::path::Path::new("/venv/bin/python"),
            std::path::Path::new("/site/runner.py"), // unused — `-m` form
            std::path::Path::new("/cache/abc123"),
            "my_node",
            "MyNodeType",
            "node-1",
            "sess-xyz",
            "{}",
            Some(r#"{"channel":"foo"}"#),
        );
        // Spot-check key shape constraints. Each --flag is followed by its value.
        let joined = argv.join(" ");
        assert!(joined.contains("--node-type MyNodeType"));
        assert!(joined.contains("--node-id node-1"));
        assert!(joined.contains("--session-id sess-xyz"));
        assert!(joined.contains("--module-root /cache/abc123"));
        assert!(joined.contains("--register-module my_node"));
        assert!(joined.contains("--params {}"));
        assert!(joined.contains(r#"--ipc-config {"channel":"foo"}"#));
        assert_eq!(argv[0], "/venv/bin/python");
        assert_eq!(argv[1], "-m");
        assert_eq!(argv[2], "remotemedia.core.multiprocessing.runner");
        // No script path baked into argv — runner is invoked as a module.
        assert!(
            !joined.contains("/site/runner.py"),
            "argv should not embed the script path"
        );
    }
}
