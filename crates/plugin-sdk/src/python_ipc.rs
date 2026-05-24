//! Subprocess + iceoryx2 IPC for Python loadable plugins.
//!
//! Mirrors the shape of [`remotemedia_core::multiprocess::factory`] but
//! talks to the existing Python runner
//! (`clients/python/remotemedia/core/multiprocessing/runner.py`)
//! instead of a native Rust binary. The wire format is the
//! `data_transfer::RuntimeData::{to_bytes, from_bytes}` binary layout
//! the runner already speaks.
//!
//! ## Status
//!
//! End-to-end wiring for [`examples/echo-python-loadable/`]. Matches
//! the in-host `multiprocess_executor.rs` byte-for-byte on:
//!
//! - **READY handshake**: subscribes to `control/{session_id}_{node_id}`
//!   Rust-side and blocks until Python publishes `b"READY"` (the runner
//!   emits this from `Node._publish_ready_signal` after `initialize()`
//!   completes). Timeout configurable via
//!   `REMOTEMEDIA_PLUGIN_READY_TIMEOUT_SECS` (default 300s — same as the
//!   in-host `init_timeout_secs` so heavy ML weight loads have headroom).
//!   `DEPS:` and `PROGRESS:` control messages flow past silently.
//! - **Multi-output**: `round_trip_multi` drains every emission until
//!   the runner publishes `EndOfInput` (data_type=8). `round_trip`
//!   returns the first emission then continues draining so the next
//!   call doesn't see stale frames.
//! - **Aux ports**: data crosses as a single iceoryx2 channel pair.
//!   The runner's aux-port envelope flows through unchanged (it's
//!   embedded in the data payload, not a separate channel).
//!
//! The wire format and channel naming match the in-host executor
//! exactly, so the runner needs zero changes.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use iceoryx2::prelude::*;
use tokio::sync::{mpsc, oneshot};

const MAX_SLICE_LEN: usize = 1024 * 1024;
const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(300);
const READY_TIMEOUT_ENV: &str = "REMOTEMEDIA_PLUGIN_READY_TIMEOUT_SECS";
const READY_POLL_INTERVAL: Duration = Duration::from_millis(10);

fn ready_timeout() -> Duration {
    std::env::var(READY_TIMEOUT_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_READY_TIMEOUT)
}

/// Data-type discriminant — matches the byte values the Python runner
/// emits / accepts (see
/// `clients/python/remotemedia/core/multiprocessing/data.py`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WireDataType {
    Audio = 1,
    Video = 2,
    Text = 3,
    Tensor = 4,
    ControlMessage = 5,
    Numpy = 6,
    File = 7,
    EndOfInput = 8,
}

impl WireDataType {
    fn from_u8(b: u8) -> Option<Self> {
        Some(match b {
            1 => Self::Audio,
            2 => Self::Video,
            3 => Self::Text,
            4 => Self::Tensor,
            5 => Self::ControlMessage,
            6 => Self::Numpy,
            7 => Self::File,
            8 => Self::EndOfInput,
            _ => return None,
        })
    }
}

/// IPC-flavored RuntimeData matching the runner's wire format byte-for-byte.
///
/// `payload` is the variant-specific bytes (e.g. UTF-8 for Text). The
/// payload encoding is variant-specific and must match what the runner
/// expects — see `clients/python/remotemedia/core/multiprocessing/data.py`
/// for the canonical layout per variant.
#[derive(Debug, Clone)]
pub struct WireRuntimeData {
    pub data_type: WireDataType,
    pub session_id: String,
    pub timestamp_us: u64,
    pub payload: Vec<u8>,
}

impl WireRuntimeData {
    fn now_us() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0)
    }

    pub fn now_text(text: &str, session_id: &str) -> Self {
        Self {
            data_type: WireDataType::Text,
            session_id: session_id.to_string(),
            timestamp_us: Self::now_us(),
            payload: text.as_bytes().to_vec(),
        }
    }

    /// Audio payload layout matches `data_transfer::RuntimeData::audio`:
    ///   sample_rate(4 LE u32) | channels(2 LE u16) | metadata_len(4 LE u32) | metadata_bytes | f32 samples LE
    pub fn now_audio(samples: &[f32], sample_rate: u32, channels: u16, session_id: &str) -> Self {
        let samples_bytes = unsafe {
            std::slice::from_raw_parts(
                samples.as_ptr() as *const u8,
                samples.len() * std::mem::size_of::<f32>(),
            )
        };
        let mut payload = Vec::with_capacity(10 + samples_bytes.len());
        payload.extend_from_slice(&sample_rate.to_le_bytes());
        payload.extend_from_slice(&channels.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes()); // metadata_len = 0
        payload.extend_from_slice(samples_bytes);
        Self {
            data_type: WireDataType::Audio,
            session_id: session_id.to_string(),
            timestamp_us: Self::now_us(),
            payload,
        }
    }

    /// Tensor payload layout matches `data_transfer::RuntimeData::tensor`:
    ///   n_dims(1) | dims(4*n_dims LE u32) | dtype(1) | metadata_len(4 LE u32) | metadata_bytes | data
    pub fn now_tensor(
        data: &[u8],
        shape: &[u32],
        dtype_code: u8,
        extras: Option<&serde_json::Value>,
        session_id: &str,
    ) -> Self {
        let metadata_bytes = extras
            .map(|m| serde_json::to_vec(m).unwrap_or_default())
            .unwrap_or_default();
        let n_dims = shape.len().min(255) as u8;
        let mut payload = Vec::with_capacity(
            1 + (n_dims as usize) * 4 + 1 + 4 + metadata_bytes.len() + data.len(),
        );
        payload.push(n_dims);
        for &d in shape.iter().take(n_dims as usize) {
            payload.extend_from_slice(&d.to_le_bytes());
        }
        payload.push(dtype_code);
        payload.extend_from_slice(&(metadata_bytes.len() as u32).to_le_bytes());
        payload.extend_from_slice(&metadata_bytes);
        payload.extend_from_slice(data);
        Self {
            data_type: WireDataType::Tensor,
            session_id: session_id.to_string(),
            timestamp_us: Self::now_us(),
            payload,
        }
    }

    /// Decode tensor payload → `(data, shape, dtype_code, extras_json)`.
    /// `extras_json` is `Value::Null` when no extras were attached.
    pub fn decode_tensor(&self) -> Result<(Vec<u8>, Vec<u32>, u8, serde_json::Value), String> {
        if self.data_type != WireDataType::Tensor {
            return Err(format!("decode_tensor on non-tensor: {:?}", self.data_type));
        }
        let p = &self.payload;
        if p.is_empty() {
            return Err("tensor payload empty".into());
        }
        let n_dims = p[0] as usize;
        let mut pos = 1usize;
        if pos + n_dims * 4 > p.len() {
            return Err(format!("tensor shape truncated (n_dims={n_dims})"));
        }
        let mut shape = Vec::with_capacity(n_dims);
        for _ in 0..n_dims {
            shape.push(u32::from_le_bytes([
                p[pos],
                p[pos + 1],
                p[pos + 2],
                p[pos + 3],
            ]));
            pos += 4;
        }
        if pos + 1 > p.len() {
            return Err("tensor dtype byte missing".into());
        }
        let dtype_code = p[pos];
        pos += 1;
        if pos + 4 > p.len() {
            return Err("tensor metadata_len missing".into());
        }
        let meta_len = u32::from_le_bytes([p[pos], p[pos + 1], p[pos + 2], p[pos + 3]]) as usize;
        pos += 4;
        if pos + meta_len > p.len() {
            return Err(format!("tensor metadata truncated (need {meta_len})"));
        }
        let extras: serde_json::Value = if meta_len == 0 {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&p[pos..pos + meta_len])
                .map_err(|e| format!("tensor metadata json: {e}"))?
        };
        pos += meta_len;
        Ok((p[pos..].to_vec(), shape, dtype_code, extras))
    }

    /// Video payload layout matches `data_transfer::RuntimeData::video`:
    ///   width(4 LE u32) | height(4 LE u32) | format(1) | codec(1) | frame_number(8 LE u64) | is_keyframe(1) | pixel_data
    pub fn now_video(
        pixel_data: &[u8],
        width: u32,
        height: u32,
        format: u8,
        codec: u8,
        frame_number: u64,
        is_keyframe: bool,
        session_id: &str,
    ) -> Self {
        let mut payload = Vec::with_capacity(19 + pixel_data.len());
        payload.extend_from_slice(&width.to_le_bytes());
        payload.extend_from_slice(&height.to_le_bytes());
        payload.push(format);
        payload.push(codec);
        payload.extend_from_slice(&frame_number.to_le_bytes());
        payload.push(if is_keyframe { 1 } else { 0 });
        payload.extend_from_slice(pixel_data);
        Self {
            data_type: WireDataType::Video,
            session_id: session_id.to_string(),
            timestamp_us: Self::now_us(),
            payload,
        }
    }

    /// Decode video payload → `(width, height, format, codec, frame_number, is_keyframe, pixel_data)`.
    pub fn decode_video(&self) -> Result<(u32, u32, u8, u8, u64, bool, Vec<u8>), String> {
        if self.data_type != WireDataType::Video {
            return Err(format!("decode_video on non-video: {:?}", self.data_type));
        }
        if self.payload.len() < 19 {
            return Err(format!("video payload too short: {}", self.payload.len()));
        }
        let p = &self.payload;
        let width = u32::from_le_bytes([p[0], p[1], p[2], p[3]]);
        let height = u32::from_le_bytes([p[4], p[5], p[6], p[7]]);
        let format = p[8];
        let codec = p[9];
        let frame_number =
            u64::from_le_bytes([p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17]]);
        let is_keyframe = p[18] != 0;
        Ok((
            width,
            height,
            format,
            codec,
            frame_number,
            is_keyframe,
            p[19..].to_vec(),
        ))
    }

    /// ControlMessage payload is a JSON-serialized object — matches
    /// `data_transfer::RuntimeData::control_message`. Layout:
    ///   `{"message_type": ..., "segment_id": ..., "timestamp_ms": ..., "metadata": {...}}` as UTF-8 JSON.
    pub fn now_control_message(payload_json: &serde_json::Value, session_id: &str) -> Self {
        let payload = serde_json::to_vec(payload_json).unwrap_or_default();
        Self {
            data_type: WireDataType::ControlMessage,
            session_id: session_id.to_string(),
            timestamp_us: Self::now_us(),
            payload,
        }
    }

    /// Decode ControlMessage payload as a JSON object.
    pub fn decode_control_message(&self) -> Result<serde_json::Value, String> {
        if self.data_type != WireDataType::ControlMessage {
            return Err(format!(
                "decode_control_message on non-control: {:?}",
                self.data_type
            ));
        }
        serde_json::from_slice(&self.payload)
            .map_err(|e| format!("control message json decode: {e}"))
    }

    /// Decode audio payload back into `(samples, sample_rate, channels)`.
    /// Returns an error if the payload doesn't have the audio layout.
    pub fn decode_audio(&self) -> Result<(Vec<f32>, u32, u16), String> {
        if self.data_type != WireDataType::Audio {
            return Err(format!(
                "decode_audio on non-audio frame: {:?}",
                self.data_type
            ));
        }
        if self.payload.len() < 10 {
            return Err(format!("audio payload too short: {}", self.payload.len()));
        }
        let sr = u32::from_le_bytes([
            self.payload[0],
            self.payload[1],
            self.payload[2],
            self.payload[3],
        ]);
        let ch = u16::from_le_bytes([self.payload[4], self.payload[5]]);
        let md_len = u32::from_le_bytes([
            self.payload[6],
            self.payload[7],
            self.payload[8],
            self.payload[9],
        ]) as usize;
        let samples_start = 10 + md_len;
        if samples_start > self.payload.len() {
            return Err(format!("audio metadata_len {md_len} overruns payload"));
        }
        let raw = &self.payload[samples_start..];
        if raw.len() % 4 != 0 {
            return Err(format!(
                "audio samples length {} not f32-aligned",
                raw.len()
            ));
        }
        let n = raw.len() / 4;
        let mut samples = Vec::with_capacity(n);
        for i in 0..n {
            let off = i * 4;
            samples.push(f32::from_le_bytes([
                raw[off],
                raw[off + 1],
                raw[off + 2],
                raw[off + 3],
            ]));
        }
        Ok((samples, sr, ch))
    }

    /// Serialize: type(1) | session_len(2 LE) | session | timestamp(8 LE) | payload_len(4 LE) | payload.
    pub fn to_bytes(&self) -> Vec<u8> {
        let session_bytes = self.session_id.as_bytes();
        let mut bytes =
            Vec::with_capacity(1 + 2 + session_bytes.len() + 8 + 4 + self.payload.len());
        bytes.push(self.data_type as u8);
        bytes.extend_from_slice(&(session_bytes.len() as u16).to_le_bytes());
        bytes.extend_from_slice(session_bytes);
        bytes.extend_from_slice(&self.timestamp_us.to_le_bytes());
        bytes.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    /// Deserialize the wire format.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 1 + 2 + 8 + 4 {
            return Err(format!("wire blob too short: {} bytes", bytes.len()));
        }
        let mut p = 0;
        let data_type = WireDataType::from_u8(bytes[p])
            .ok_or_else(|| format!("unknown data type {}", bytes[p]))?;
        p += 1;
        let session_len = u16::from_le_bytes([bytes[p], bytes[p + 1]]) as usize;
        p += 2;
        if p + session_len > bytes.len() {
            return Err("session length overruns buffer".into());
        }
        let session_id = String::from_utf8_lossy(&bytes[p..p + session_len]).into_owned();
        p += session_len;
        if p + 8 > bytes.len() {
            return Err("timestamp overruns buffer".into());
        }
        let mut ts = [0u8; 8];
        ts.copy_from_slice(&bytes[p..p + 8]);
        let timestamp_us = u64::from_le_bytes(ts);
        p += 8;
        if p + 4 > bytes.len() {
            return Err("payload length overruns buffer".into());
        }
        let mut pl = [0u8; 4];
        pl.copy_from_slice(&bytes[p..p + 4]);
        let payload_len = u32::from_le_bytes(pl) as usize;
        p += 4;
        if p + payload_len > bytes.len() {
            return Err(format!(
                "payload length {payload_len} overruns buffer ({} remaining)",
                bytes.len() - p
            ));
        }
        let payload = bytes[p..p + payload_len].to_vec();
        Ok(Self {
            data_type,
            session_id,
            timestamp_us,
            payload,
        })
    }
}

/// IPC command driving round-trips on the dedicated OS thread that
/// owns the iceoryx2 publisher + subscriber.
pub enum IpcCommand {
    /// Block until Python publishes `b"READY"` on the control channel
    /// or `timeout` elapses. Driven once per node, AFTER the subprocess
    /// has been spawned (so the control subscriber can observe the
    /// `Node._publish_ready_signal` emission). `DEPS:` / `PROGRESS:`
    /// control messages flow past without satisfying the wait.
    WaitForReady {
        timeout: Duration,
        // sync mpsc so the sync `spawn_runner_and_ipc` caller can
        // .recv() without a tokio runtime in scope.
        reply: std::sync::mpsc::Sender<Result<(), String>>,
    },
    /// Single-output: publish `req_bytes` and return the FIRST output
    /// frame the runner emits. Drops any subsequent emissions for the
    /// same input (until the next `Round` / `RoundMulti`).
    Round {
        req_bytes: Vec<u8>,
        reply: oneshot::Sender<Result<Vec<u8>, String>>,
    },
    /// Multi-output: publish `req_bytes` and drain the output channel
    /// until the runner emits an `EndOfInput` sentinel (data_type=8).
    /// Returns every output frame in emission order, EXCLUDING the
    /// sentinel itself.
    RoundMulti {
        req_bytes: Vec<u8>,
        reply: oneshot::Sender<Result<Vec<Vec<u8>>, String>>,
    },
    /// Streaming multi-output: publish `req_bytes` and forward each
    /// output frame on `frame_tx` *as it arrives*, then send
    /// `Ok(())` on `reply` once the `EndOfInput` sentinel is observed.
    /// `frame_tx` is dropped before `reply` fires so consumers can
    /// detect end-of-stream via a closed channel.
    ///
    /// Use this instead of `RoundMulti` for any streaming source
    /// (TTS chunks, STT segments, …). `RoundMulti` collects every
    /// frame into a `Vec` before returning, which serializes
    /// real-time generators behind their own completion — for an
    /// LFM2-Audio turn that yields 13 s of audio over 13 s of wall
    /// time, no chunk leaves the IPC thread until the very end.
    RoundStreaming {
        req_bytes: Vec<u8>,
        frame_tx: mpsc::Sender<Vec<u8>>,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

/// Spawn the runner subprocess + dedicated IPC thread for one
/// `(session_id, node_id)` pair. Returns the IPC sender + the child
/// process handle (so the caller can kill it on drop).
///
/// Setup order matches the in-host executor:
/// 1. IPC thread creates iceoryx2 services (input/output/control) —
///    `open_or_create` so we're tolerant of Python attaching first.
///    The control-channel subscriber is also created here so it can
///    observe `b"READY"` once Python attaches and publishes.
/// 2. Spawn the runner subprocess. It attaches to the existing services
///    via `.open()` — Python's `connect_channels` retries 50× × 100ms.
/// 3. Send `WaitForReady` to the IPC thread. It polls the control
///    subscriber until `b"READY"` arrives (or the timeout elapses),
///    silently dropping `DEPS:` / `PROGRESS:` messages along the way.
///    Timeout: `REMOTEMEDIA_PLUGIN_READY_TIMEOUT_SECS` (default 300s).
pub fn spawn_runner_and_ipc(
    argv: &[String],
    session_id: &str,
    node_id: &str,
) -> Result<(mpsc::Sender<IpcCommand>, Arc<Mutex<Child>>), String> {
    if argv.is_empty() {
        return Err("argv must not be empty".into());
    }
    let in_channel = format!("{session_id}_{node_id}_input");
    let out_channel = format!("{session_id}_{node_id}_output");
    let control_channel = format!("control/{session_id}_{node_id}");

    // Start the IPC thread FIRST so the iceoryx2 services (input,
    // output, control) exist by the time Python attaches.
    let cmd_tx = spawn_ipc_thread(
        in_channel,
        out_channel,
        control_channel,
        session_id.to_string(),
        node_id.to_string(),
    )?;

    let mut command = Command::new(&argv[0]);
    command.args(&argv[1..]);
    // Stderr handling: by default inherit (so a host with a real terminal
    // sees the runner's logs). When the parent's stderr inheritance is
    // unreliable (e.g. NAPI / Node hides it from grandchild processes),
    // set REMOTEMEDIA_PLUGIN_STDERR_FILE=<path> to append the runner's
    // stderr to that file instead. Diagnostic-only — production callers
    // leave it unset.
    match std::env::var("REMOTEMEDIA_PLUGIN_STDERR_FILE") {
        Ok(path) if !path.is_empty() => match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            Ok(f) => {
                command.stderr(Stdio::from(f));
            }
            Err(_) => {
                command.stderr(Stdio::inherit());
            }
        },
        _ => {
            command.stderr(Stdio::inherit());
        }
    }

    // Linux-only safety net: ask the kernel to deliver SIGTERM to this
    // child if the host process dies before Drop runs (segfault, SIGKILL,
    // etc.). Normal teardown still relies on the child handle's `Drop`
    // killing the runner explicitly — this is purely the adversarial
    // parent-death path. Matches the pattern in
    // `crates/core/src/python/multiprocess/process_manager.rs`.
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: prctl(PR_SET_PDEATHSIG) is async-signal-safe — fine to
        // call from `pre_exec` which runs in the forked child between
        // fork() and exec(). The libc::SIGTERM arg is a constant; no
        // allocations.
        unsafe {
            command.pre_exec(|| {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

    let child = command
        .spawn()
        .map_err(|e| format!("spawn runner {}: {e}", argv[0]))?;
    let child = Arc::new(Mutex::new(child));

    // Wait for the runner to publish `b"READY"` on the control
    // channel. Python's `Node._publish_ready_signal` emits this after
    // `initialize()` completes — i.e. after the model is loaded.
    // Sending data before READY can be silently dropped (Python's
    // input subscriber races with the model load) so we block here.
    //
    // `try_send` (vs `blocking_send`) because we may be called from
    // inside a tokio runtime worker (core's executor path) where
    // `blocking_send` panics. The channel has capacity 64 and the IPC
    // thread has just been started with an empty queue, so the send
    // never actually blocks — `try_send` always succeeds in practice.
    // Reply is via std::sync::mpsc which is safe to `recv()` from
    // any thread (including a tokio worker — std::sync::mpsc isn't
    // a tokio primitive).
    let (rdy_tx, rdy_rx) = std::sync::mpsc::channel::<Result<(), String>>();
    cmd_tx
        .try_send(IpcCommand::WaitForReady {
            timeout: ready_timeout(),
            reply: rdy_tx,
        })
        .map_err(|e| format!("dispatch WaitForReady: {e}"))?;

    // Poll for READY OR subprocess death, whichever lands first.
    // Without the death check, a Python crash (ModuleNotFoundError,
    // venv missing `remotemedia`, iceoryx2 version skew, segfault, …)
    // would silently wait the full READY timeout (default 300s) before
    // surfacing — long enough to look like a hang in test harnesses
    // with shorter deadlines (jest beforeAll @ 90s). Polling
    // `try_wait()` makes the failure visible quickly; the user can then
    // check the runner's stderr (or `REMOTEMEDIA_PLUGIN_STDERR_FILE`)
    // for the real cause.
    //
    // Two safeguards against false positives:
    //   1. After observing an exit status, drain the READY channel one
    //      more time (with a short timeout) — if Python published READY
    //      and *then* exited (close-on-success scenarios), the READY
    //      message is still in the queue and we should honor it.
    //   2. Use a 250ms poll interval so a Python that pegs the CPU
    //      during startup doesn't get racy `waitpid(WNOHANG)` reads
    //      between SIGCHLD bookkeeping and actual reap.
    let death_poll = Duration::from_millis(250);
    loop {
        match rdy_rx.recv_timeout(death_poll) {
            Ok(result) => {
                result?;
                break;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                let exit = child
                    .lock()
                    .ok()
                    .and_then(|mut g| g.try_wait().ok().flatten());
                if let Some(status) = exit {
                    // Last-chance drain: READY may have been published
                    // in the same window the child exited. Give the IPC
                    // thread up to 200ms more to deliver it before
                    // declaring failure.
                    if let Ok(result) = rdy_rx.recv_timeout(Duration::from_millis(200)) {
                        result?;
                        break;
                    }
                    return Err(format!(
                        "runner process exited before publishing READY (status: {status}). \
                         Common causes: managed venv missing the `remotemedia` package \
                         (set REMOTEMEDIA_PYTHON_SRC=/path/to/clients/python), \
                         iceoryx2 version skew between Rust workspace and the plugin's \
                         @python_requires pin, or an unhandled ImportError. \
                         Set REMOTEMEDIA_PLUGIN_STDERR_FILE=<path> to capture the \
                         runner's stderr for diagnosis."
                    ));
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err("WaitForReady reply channel disconnected".into());
            }
        }
    }

    Ok((cmd_tx, child))
}

/// Owns iceoryx2 publisher + subscriber on a dedicated OS thread
/// (`Publisher`/`Subscriber` are `!Send`). Async callers use the
/// returned `mpsc::Sender<IpcCommand>` to round-trip data.
fn spawn_ipc_thread(
    in_channel: String,
    out_channel: String,
    control_channel: String,
    session_id: String,
    node_id: String,
) -> Result<mpsc::Sender<IpcCommand>, String> {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<IpcCommand>(64);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();

    std::thread::Builder::new()
        .name(format!("rm-py-plugin-ipc-{in_channel}"))
        .spawn(move || {
            let setup = || -> Result<_, String> {
                let node = NodeBuilder::new()
                    .create::<ipc::Service>()
                    .map_err(|e| format!("iceoryx2 node create: {e:?}"))?;

                let in_service = node
                    .service_builder(
                        &ServiceName::new(&in_channel)
                            .map_err(|e| format!("input service name: {e:?}"))?,
                    )
                    .publish_subscribe::<[u8]>()
                    // Allow a few stale publishers/subscribers from
                    // crashed prior runs to linger without blocking
                    // fresh services. iceoryx2 reclaims slots on
                    // attach; the dev iteration loop frequently leaves
                    // services behind when a Python crash skips Drop.
                    .max_publishers(8)
                    .max_subscribers(8)
                    .subscriber_max_buffer_size(128)
                    .open_or_create()
                    .map_err(|e| format!("input service: {e:?}"))?;

                let out_service = node
                    .service_builder(
                        &ServiceName::new(&out_channel)
                            .map_err(|e| format!("output service name: {e:?}"))?,
                    )
                    .publish_subscribe::<[u8]>()
                    .max_publishers(8)
                    .max_subscribers(8)
                    .subscriber_max_buffer_size(128)
                    .open_or_create()
                    .map_err(|e| format!("output service: {e:?}"))?;

                // Control channel — Python publishes `b"READY"` after
                // it finishes initialize(). The runner's
                // `connect_channels` opens via `.open()` (not
                // open_or_create), so the service MUST exist before
                // Python attaches. The service handle has to be kept
                // alive — drop it and iceoryx2 reaps the service when
                // no port-holders remain, defeating Python's open.
                let control_service = node
                    .service_builder(
                        &ServiceName::new(&control_channel)
                            .map_err(|e| format!("control service name: {e:?}"))?,
                    )
                    .publish_subscribe::<[u8]>()
                    .max_publishers(8)
                    .max_subscribers(8)
                    .subscriber_max_buffer_size(128)
                    .open_or_create()
                    .map_err(|e| format!("control service: {e:?}"))?;
                // Control subscriber drives the READY handshake. Held
                // here so it lives on the dedicated thread (it's !Send
                // like the data publisher/subscriber).
                let control_subscriber = control_service
                    .subscriber_builder()
                    .create()
                    .map_err(|e| format!("control subscriber create: {e:?}"))?;

                let publisher = in_service
                    .publisher_builder()
                    .initial_max_slice_len(MAX_SLICE_LEN)
                    .create()
                    .map_err(|e| format!("publisher create: {e:?}"))?;
                let subscriber = out_service
                    .subscriber_builder()
                    .create()
                    .map_err(|e| format!("subscriber create: {e:?}"))?;
                Ok((publisher, subscriber, control_subscriber, control_service))
            };

            let (publisher, subscriber, control_subscriber, _control_service) = match setup() {
                Ok(ps) => {
                    let _ = ready_tx.send(Ok(()));
                    ps
                }
                Err(e) => {
                    let _ = ready_tx.send(Err(e));
                    return;
                }
            };

            // `control_ctx` is the (session_id, node_id) pair the IPC
            // thread tags every runtime control message with before
            // handing it to `control_hook::invoke_control_hook`. Held
            // by reference so the per-command closures don't have to
            // clone the strings.
            let control_ctx = (session_id.as_str(), node_id.as_str());
            while let Some(cmd) = cmd_rx.blocking_recv() {
                match cmd {
                    IpcCommand::WaitForReady { timeout, reply } => {
                        // During READY handshake we still drain
                        // PROGRESS:/PUBLISH: via the hook so a Python
                        // plugin that calls publish_progress *inside*
                        // its `initialize()` (model-load progress
                        // updates, common in the LFM2 / Voxtral
                        // plugins) actually reaches the orb.
                        let result = wait_for_ready(&control_subscriber, timeout, control_ctx);
                        let _ = reply.send(result);
                    }
                    IpcCommand::Round { req_bytes, reply } => {
                        let result = round_trip(
                            &publisher,
                            &subscriber,
                            &control_subscriber,
                            control_ctx,
                            &req_bytes,
                        );
                        let _ = reply.send(result);
                    }
                    IpcCommand::RoundMulti { req_bytes, reply } => {
                        let result = round_trip_multi(
                            &publisher,
                            &subscriber,
                            &control_subscriber,
                            control_ctx,
                            &req_bytes,
                        );
                        let _ = reply.send(result);
                    }
                    IpcCommand::RoundStreaming {
                        req_bytes,
                        frame_tx,
                        reply,
                    } => {
                        let result = round_trip_streaming(
                            &publisher,
                            &subscriber,
                            &control_subscriber,
                            control_ctx,
                            &req_bytes,
                            &frame_tx,
                        );
                        // Drop frame_tx BEFORE replying so the consumer's
                        // `while let Some(...) = frame_rx.recv().await`
                        // loop unblocks before it awaits `reply`.
                        drop(frame_tx);
                        let _ = reply.send(result);
                    }
                }
            }
        })
        .map_err(|e| format!("spawn ipc thread: {e}"))?;

    ready_rx
        .recv()
        .map_err(|e| format!("ipc thread ready signal lost: {e}"))??;
    Ok(cmd_tx)
}

/// Drain pending runtime control messages (non-blocking).
///
/// Reads every immediately-available sample from `control_subscriber`
/// and forwards each to [`crate::control_hook::invoke_control_hook`]
/// with the IPC thread's `(session_id, node_id)`. Stops at the first
/// `None` from `receive()` — i.e. drains what's queued, does not wait.
///
/// `b"READY"` is filtered out: that signal is consumed by
/// [`wait_for_ready`] during startup, but if a late READY publish
/// races against the IPC thread switching into runtime polling we'd
/// still see one here. It carries no payload for the hook to act on
/// so we drop it.
///
/// Cheap no-op when no hook is installed (saves the syscall per
/// outer-loop iteration on Rust-only test runs that never register a
/// control-bus handler).
fn drain_control_messages(
    control_subscriber: &iceoryx2::port::subscriber::Subscriber<ipc::Service, [u8], ()>,
    ctx: (&str, &str),
) {
    if !crate::control_hook::hook_installed() {
        return;
    }
    let (session_id, node_id) = ctx;
    let mut drained: u32 = 0;
    loop {
        match control_subscriber.receive() {
            Ok(Some(sample)) => {
                drained += 1;
                let bytes = sample.payload();
                if bytes == b"READY" {
                    continue;
                }
                crate::control_hook::invoke_control_hook(session_id, node_id, bytes);
            }
            Ok(None) => {
                if drained > 0 {
                    eprintln!(
                        "[plugin-sdk python_ipc] drain_control_messages node={node_id} \
                         session={session_id} drained={drained}"
                    );
                }
                return;
            }
            Err(e) => {
                eprintln!(
                    "[plugin-sdk python_ipc] drain_control_messages node={node_id} \
                     session={session_id} receive error after {drained} msgs: {e:?}"
                );
                return;
            }
        }
    }
}

/// Publish `req` on the input channel, return the FIRST output sample,
/// then drain any remaining frames (including the `EndOfInput`
/// sentinel) so they don't leak into the next call. Drops extra
/// outputs silently — use `round_trip_multi` for streaming nodes.
fn round_trip(
    publisher: &iceoryx2::port::publisher::Publisher<ipc::Service, [u8], ()>,
    subscriber: &iceoryx2::port::subscriber::Subscriber<ipc::Service, [u8], ()>,
    control_subscriber: &iceoryx2::port::subscriber::Subscriber<ipc::Service, [u8], ()>,
    ctx: (&str, &str),
    req: &[u8],
) -> Result<Vec<u8>, String> {
    let sample = publisher
        .loan_slice_uninit(req.len())
        .map_err(|e| format!("loan: {e:?}"))?;
    let sample = sample.write_from_slice(req);
    sample.send().map_err(|e| format!("send: {e:?}"))?;

    let mut first: Option<Vec<u8>> = None;
    loop {
        // Drain runtime control messages first so `publish_progress`
        // / `publish_to_node_port` calls from inside `process()` are
        // surfaced promptly rather than after the turn ends.
        drain_control_messages(control_subscriber, ctx);
        match subscriber
            .receive()
            .map_err(|e| format!("receive: {e:?}"))?
        {
            Some(sample) => {
                let bytes = sample.payload().to_vec();
                // EndOfInput sentinel — drain complete, return the
                // first real output (or error if nothing came).
                if bytes.is_empty() || bytes[0] == WireDataType::EndOfInput as u8 {
                    // Final drain so any straggler control frames
                    // emitted alongside the EndOfInput aren't left on
                    // the channel for the next round.
                    drain_control_messages(control_subscriber, ctx);
                    return first.ok_or_else(|| {
                        "runner emitted EndOfInput with no real output frames".to_string()
                    });
                }
                if first.is_none() {
                    first = Some(bytes);
                }
                // Extra outputs ARE silently dropped — single-output
                // contract. Keep looping to consume EndOfInput so the
                // next call doesn't see stale frames.
            }
            None => std::thread::yield_now(),
        }
    }
}

/// Multi-output drain: publish `req`, then collect every emission until
/// we see an `EndOfInput` sentinel (data_type byte = 8). Returned vec
/// excludes the sentinel — only carries the real outputs.
///
/// The runner emits exactly one `EndOfInput` per input it receives,
/// AFTER all `async generator` yields complete. See
/// `clients/python/remotemedia/core/multiprocessing/node.py::_send_end_of_input`.
fn round_trip_multi(
    publisher: &iceoryx2::port::publisher::Publisher<ipc::Service, [u8], ()>,
    subscriber: &iceoryx2::port::subscriber::Subscriber<ipc::Service, [u8], ()>,
    control_subscriber: &iceoryx2::port::subscriber::Subscriber<ipc::Service, [u8], ()>,
    ctx: (&str, &str),
    req: &[u8],
) -> Result<Vec<Vec<u8>>, String> {
    let sample = publisher
        .loan_slice_uninit(req.len())
        .map_err(|e| format!("loan: {e:?}"))?;
    let sample = sample.write_from_slice(req);
    sample.send().map_err(|e| format!("send: {e:?}"))?;

    let mut outputs: Vec<Vec<u8>> = Vec::new();
    loop {
        drain_control_messages(control_subscriber, ctx);
        match subscriber
            .receive()
            .map_err(|e| format!("receive: {e:?}"))?
        {
            Some(sample) => {
                let bytes = sample.payload().to_vec();
                // Peek at type byte — if it's EndOfInput we're done.
                // Empty payloads also count as "no more output" but the
                // runner never sends those; treat them as EndOfInput
                // for safety.
                if bytes.is_empty() || bytes[0] == WireDataType::EndOfInput as u8 {
                    drain_control_messages(control_subscriber, ctx);
                    return Ok(outputs);
                }
                outputs.push(bytes);
            }
            None => std::thread::yield_now(),
        }
    }
}

/// Streaming drain: publish `req`, then forward every emission on
/// `frame_tx` *immediately* (no batching). Returns `Ok(())` after the
/// `EndOfInput` sentinel arrives. Identical wire contract to
/// `round_trip_multi`; the only difference is per-frame delivery.
///
/// If the consumer drops the receiver mid-stream (e.g. the session
/// shut down while audio was still generating) this drains remaining
/// frames silently to consume the `EndOfInput` sentinel — otherwise
/// the next `Round` / `RoundMulti` / `RoundStreaming` call would
/// observe leftover frames from this turn.
///
/// The first `subscriber.receive()` per iteration is preceded by a
/// non-blocking drain of the control channel — this is where the
/// LFM2-Audio plugin's v4.1 blip detection actually surfaces to the
/// host (the blip fires within the first ~300 ms of generation, well
/// before this function returns `EndOfInput` ~5 s later).
fn round_trip_streaming(
    publisher: &iceoryx2::port::publisher::Publisher<ipc::Service, [u8], ()>,
    subscriber: &iceoryx2::port::subscriber::Subscriber<ipc::Service, [u8], ()>,
    control_subscriber: &iceoryx2::port::subscriber::Subscriber<ipc::Service, [u8], ()>,
    ctx: (&str, &str),
    req: &[u8],
    frame_tx: &mpsc::Sender<Vec<u8>>,
) -> Result<(), String> {
    let sample = publisher
        .loan_slice_uninit(req.len())
        .map_err(|e| format!("loan: {e:?}"))?;
    let sample = sample.write_from_slice(req);
    sample.send().map_err(|e| format!("send: {e:?}"))?;

    let mut consumer_alive = true;
    loop {
        drain_control_messages(control_subscriber, ctx);
        match subscriber
            .receive()
            .map_err(|e| format!("receive: {e:?}"))?
        {
            Some(sample) => {
                let bytes = sample.payload().to_vec();
                if bytes.is_empty() || bytes[0] == WireDataType::EndOfInput as u8 {
                    drain_control_messages(control_subscriber, ctx);
                    return Ok(());
                }
                if consumer_alive {
                    // `blocking_send` parks this OS thread when the
                    // consumer is slow — that's the desired backpressure
                    // (matches `round_trip_multi`'s implicit memory
                    // backpressure via the growing Vec, except here it
                    // propagates to the producer in real time).
                    if frame_tx.blocking_send(bytes).is_err() {
                        // Receiver dropped — silently swallow the rest
                        // of this turn so we don't leak frames into the
                        // next call.
                        consumer_alive = false;
                    }
                }
                // else: drain to EndOfInput without forwarding.
            }
            None => std::thread::yield_now(),
        }
    }
}

/// Poll `control_subscriber` for `b"READY"` from the Python runner.
/// `DEPS:` and `PROGRESS:` control messages are forwarded to the host
/// hook (so a plugin's in-`initialize()` progress events flow to the
/// orb) but they don't satisfy the wait — only the literal `READY`
/// bytes do. Returns `Err` if `timeout` elapses or if the subscriber
/// fails.
fn wait_for_ready(
    control_subscriber: &iceoryx2::port::subscriber::Subscriber<ipc::Service, [u8], ()>,
    timeout: Duration,
    ctx: (&str, &str),
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + timeout;
    let mut polls = 0u64;
    loop {
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "timed out after {:?} waiting for runner READY (polled {polls} times)",
                timeout
            ));
        }
        match control_subscriber
            .receive()
            .map_err(|e| format!("control receive: {e:?}"))?
        {
            Some(sample) => {
                let bytes = sample.payload();
                if bytes == b"READY" {
                    return Ok(());
                }
                // DEPS: / PROGRESS: / PUBLISH: / unknown — forward to
                // the host control hook so in-initialize() taps reach
                // the SessionControlBus before READY lands. DEPS: in
                // particular is informational; PROGRESS: is the orb
                // / observer's primary feed during heavy model loads.
                let (session_id, node_id) = ctx;
                crate::control_hook::invoke_control_hook(session_id, node_id, bytes);
            }
            None => std::thread::sleep(READY_POLL_INTERVAL),
        }
        polls += 1;
    }
}

/// Convenience helper used by tests: take the runner script path,
/// venv python, module-root, module name, and node ids and assemble
/// the argv the runner expects.
///
/// Production callers should use [`crate::python_subprocess::build_runner_argv`]
/// directly — this is a thin shim re-exposing it for tests + docs.
pub fn make_runner_argv(
    python_exe: &Path,
    runner_script: &Path,
    module_root: &Path,
    module: &str,
    node_type: &str,
    node_id: &str,
    session_id: &str,
) -> Vec<String> {
    crate::python_subprocess::build_runner_argv(
        python_exe,
        runner_script,
        module_root,
        module,
        node_type,
        node_id,
        session_id,
        "{}",
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_roundtrip_text() {
        let original = WireRuntimeData::now_text("hello", "sess-1");
        let bytes = original.to_bytes();
        let decoded = WireRuntimeData::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded.data_type, WireDataType::Text);
        assert_eq!(decoded.session_id, "sess-1");
        assert_eq!(decoded.payload, b"hello");
    }

    #[test]
    fn wire_decode_rejects_short_buffer() {
        assert!(WireRuntimeData::from_bytes(&[0u8; 3]).is_err());
    }

    #[test]
    fn wire_decode_rejects_unknown_type() {
        // type byte = 99 → invalid
        let mut bytes = vec![99u8];
        bytes.extend_from_slice(&0u16.to_le_bytes()); // session_len = 0
        bytes.extend_from_slice(&0u64.to_le_bytes()); // timestamp
        bytes.extend_from_slice(&0u32.to_le_bytes()); // payload_len = 0
        assert!(WireRuntimeData::from_bytes(&bytes).is_err());
    }

    #[test]
    fn wire_roundtrip_audio() {
        let samples: Vec<f32> = vec![0.1, -0.5, 0.0, 1.0];
        let original = WireRuntimeData::now_audio(&samples, 16_000, 1, "sess-a");
        let bytes = original.to_bytes();
        let decoded = WireRuntimeData::from_bytes(&bytes).expect("decode");
        let (out_samples, sr, ch) = decoded.decode_audio().expect("decode audio");
        assert_eq!(sr, 16_000);
        assert_eq!(ch, 1);
        assert_eq!(out_samples, samples);
    }

    #[test]
    fn wire_roundtrip_tensor_with_extras() {
        let data = [0xDEu8, 0xAD, 0xBE, 0xEF, 0x00, 0x11, 0x22, 0x33];
        let shape = [2u32, 4];
        let extras = serde_json::json!({"kind": "activation_tap", "layer": 7});
        let original = WireRuntimeData::now_tensor(
            &data,
            &shape,
            /*dtype f32=*/ 0,
            Some(&extras),
            "sess-t",
        );
        let bytes = original.to_bytes();
        let decoded = WireRuntimeData::from_bytes(&bytes).expect("decode");
        let (out_data, out_shape, dtype, out_extras) =
            decoded.decode_tensor().expect("decode tensor");
        assert_eq!(out_data, &data[..]);
        assert_eq!(out_shape, vec![2, 4]);
        assert_eq!(dtype, 0);
        assert_eq!(out_extras, extras);
    }

    #[test]
    fn wire_roundtrip_tensor_without_extras() {
        let data = vec![1u8, 2, 3, 4];
        let original = WireRuntimeData::now_tensor(&data, &[4u32], 1, None, "sess-t");
        let bytes = original.to_bytes();
        let decoded = WireRuntimeData::from_bytes(&bytes).expect("decode");
        let (out_data, out_shape, dtype, extras) = decoded.decode_tensor().expect("decode tensor");
        assert_eq!(out_data, data);
        assert_eq!(out_shape, vec![4]);
        assert_eq!(dtype, 1);
        assert_eq!(extras, serde_json::Value::Null);
    }

    #[test]
    fn wire_roundtrip_video_keyframe() {
        let pixels = vec![0u8; 64];
        let original = WireRuntimeData::now_video(
            &pixels, 320, 240, /*format=*/ 1, /*codec=*/ 0, /*frame_number=*/ 42,
            /*is_keyframe=*/ true, "sess-v",
        );
        let bytes = original.to_bytes();
        let decoded = WireRuntimeData::from_bytes(&bytes).expect("decode");
        let (w, h, fmt, codec, frame, kf, px) = decoded.decode_video().expect("decode video");
        assert_eq!(w, 320);
        assert_eq!(h, 240);
        assert_eq!(fmt, 1);
        assert_eq!(codec, 0);
        assert_eq!(frame, 42);
        assert!(kf);
        assert_eq!(px, pixels);
    }

    #[test]
    fn wire_roundtrip_control_message() {
        let payload = serde_json::json!({
            "message_type": "CancelSpeculation",
            "segment_id": "seg-001",
            "timestamp_ms": 1_700_000_000_000u64,
            "metadata": { "reason": "user_interrupt" }
        });
        let original = WireRuntimeData::now_control_message(&payload, "sess-c");
        let bytes = original.to_bytes();
        let decoded = WireRuntimeData::from_bytes(&bytes).expect("decode");
        let out = decoded.decode_control_message().expect("decode ctrl");
        assert_eq!(out, payload);
    }
}
