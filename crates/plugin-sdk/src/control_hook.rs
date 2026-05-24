//! Process-wide hook that lets a host (e.g. `remotemedia-core`) drain
//! runtime control-channel messages emitted by a Python source plugin.
//!
//! ## Why this exists
//!
//! [`python_ipc::spawn_runner_and_ipc`] owns a dedicated OS thread for
//! each Python plugin subprocess. The thread holds an iceoryx2
//! subscriber on `control/<session>_<node>` and drains `b"READY"` on
//! startup, but it does NOT route the `PROGRESS:` / `PUBLISH:`
//! payloads Python publishes at runtime (e.g. `publish_progress` or
//! `publish_to_node_port` calls from inside a `process()` body).
//!
//! Those payloads need to reach the host's
//! `SessionControl::publish_tap` (`PROGRESS:` → `__system__.out`
//! tap) and `SessionControl::publish` (`PUBLISH:` → sibling node's
//! aux port). `plugin-sdk` intentionally does not depend on
//! `remotemedia-core` (the path-dep cycle is documented in
//! `plugin-sdk/Cargo.toml`), so it cannot reach the bus directly.
//!
//! Instead, the host registers a hook via
//! [`install_control_message_hook`] at startup; the IPC thread invokes
//! it via [`invoke_control_hook`] for every runtime control message it
//! receives. If no hook is installed (Rust-only test paths, plugins
//! used outside a `remotemedia-core` host) the messages are dropped
//! silently — behaviour identical to before this hook existed.
//!
//! The hook is fire-and-forget: it must not panic, must complete
//! quickly, and must be `Send + Sync` (the IPC thread is a regular
//! `std::thread::spawn` worker; the host's handler is free to spawn
//! work onto its own runtime if needed).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

/// Hook signature. Called once per runtime control message received on
/// `control/<session>_<node>`.
///
/// Arguments:
/// - `session_id`: session that owns the Python subprocess emitting
///   the message — must match the `session_id` passed to
///   [`crate::python_ipc::spawn_runner_and_ipc`].
/// - `node_id`: node id of the source plugin (origin of the message).
/// - `bytes`: raw control-channel payload from iceoryx2. The host is
///   expected to recognize `PROGRESS:<json>` and `PUBLISH:<json>`
///   prefixes (defined alongside the Python `publish_progress` /
///   `publish_to_node_port` methods in
///   `clients/python/remotemedia/core/multiprocessing/node.py`) and
///   drop anything else.
pub type ControlMessageHook = Box<dyn Fn(&str, &str, &[u8]) + Send + Sync + 'static>;

static CONTROL_HOOK: OnceLock<ControlMessageHook> = OnceLock::new();

/// Per-(node) counter of control messages observed by the IPC thread.
/// Diagnostic only — used by the eprintln!-based traces below so we
/// can confirm at runtime that the plugin-sdk side is actually
/// draining the iceoryx2 control channel when blip events fire.
static OBSERVED_COUNT: AtomicU64 = AtomicU64::new(0);

/// Install the process-wide control-message hook. Idempotent — only
/// the first call sticks; subsequent calls return `Err(hook)` so the
/// caller can recover the boxed hook if needed.
pub fn install_control_message_hook(hook: ControlMessageHook) -> Result<(), ControlMessageHook> {
    let res = CONTROL_HOOK.set(hook);
    // Use eprintln! so this surfaces even when the host process hasn't
    // wired up the `tracing` subscriber yet (install can run very
    // early — before WebRTC's tracing_subscriber::fmt init). One-shot
    // line so the log isn't noisy.
    match &res {
        Ok(()) => eprintln!(
            "[plugin-sdk control_hook] hook installed — runtime PROGRESS/PUBLISH from Python source plugins will be forwarded"
        ),
        Err(_) => eprintln!(
            "[plugin-sdk control_hook] install_control_message_hook called twice — keeping the first hook"
        ),
    }
    res
}

/// Invoke the installed hook, if any. Silently drops the message if
/// no hook is installed. Intended for use by [`crate::python_ipc`]'s
/// IPC thread.
pub(crate) fn invoke_control_hook(session_id: &str, node_id: &str, bytes: &[u8]) {
    let n = OBSERVED_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    // Cap the diagnostic trace at the first few invocations per node
    // — after that the host's own tracing! logs in
    // `handle_plugin_control_message` cover it. Use eprintln! since
    // this runs on the plugin-sdk's IPC thread which has no tracing
    // subscriber installed by plugin-sdk itself.
    if n <= 20 {
        let preview = std::str::from_utf8(bytes)
            .map(|s| s.chars().take(120).collect::<String>())
            .unwrap_or_else(|_| format!("<{} non-utf8 bytes>", bytes.len()));
        let installed = CONTROL_HOOK.get().is_some();
        eprintln!(
            "[plugin-sdk control_hook] invoke #{n} node={node_id} session={session_id} \
             installed={installed} payload={preview:?}"
        );
    }
    if let Some(hook) = CONTROL_HOOK.get() {
        hook(session_id, node_id, bytes);
    }
}

/// Returns `true` iff a hook has been installed. Diagnostic helper
/// for the IPC thread so it can choose to skip the
/// `control_subscriber.receive()` poll entirely when no consumer is
/// attached (saves the syscall per loop iteration on Rust-only test
/// runs).
pub(crate) fn hook_installed() -> bool {
    CONTROL_HOOK.get().is_some()
}
