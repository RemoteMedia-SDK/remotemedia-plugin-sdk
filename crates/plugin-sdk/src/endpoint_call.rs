//! Node-initiated calls to client-hosted endpoints, for plugin nodes
//! (openspec `add-agent-harness-integration`, design Decision 12).
//!
//! The call travels over the aux-envelope convention, so it needs no ABI
//! change and works for loadable cdylib plugins and in-tree nodes alike:
//!
//! 1. the node lists [`call_reply_method`] in `rpc_methods()`;
//! 2. inside `process_streaming*` it calls [`EndpointCaller::call`], which
//!    emits a `__call__` aux envelope through the node's output callback —
//!    the host router turns it into an endpoint call and never forwards it
//!    downstream;
//! 3. the host delivers each reply chunk by invoking `__call_reply__` on the
//!    node's RPC lane, which runs concurrently with the in-flight
//!    processing call; the node routes it with
//!    [`EndpointCaller::handle_rpc`] from its `call_rpc`.
//!
//! ```ignore
//! struct Brain { calls: EndpointCaller }
//!
//! #[async_trait]
//! impl AsyncStreamingNode for Brain {
//!     fn rpc_methods(&self) -> Vec<RpcMethod> { vec![call_reply_method()] }
//!     async fn call_rpc(&self, req: RpcRequest, _ctx: &dyn NodeRuntimeContextRead,
//!                       _r: &mut RpcResponder) -> Result<(), Error> {
//!         self.calls.handle_rpc(&req).unwrap_or_else(|| Err(Error::Execution("unknown".into())))
//!     }
//!     async fn process_streaming_with_ctx<F>(&self, data: RuntimeData,
//!         _ctx: &dyn NodeRuntimeContextRead, callback: F) -> Result<usize, Error>
//!     where F: FnMut(RuntimeData) -> Result<(), Error> + Send {
//!         let emit = Emitter::new(callback);
//!         let decision = self.calls
//!             .call(&emit, "harness", "decide", vec![], kwargs, CallOptions::default())?
//!             .value().await;
//!         emit.emit(RuntimeData::Text(format!("{decision:?}")))?;
//!         Ok(1)
//!     }
//! }
//! ```
//!
//! **Batched paths are unsupported.** The FFI `process` / `process_multi`
//! entry points return outputs only after processing completes, so a
//! `__call__` emitted there could not leave the node before the reply is
//! awaited. [`EndpointCaller::call`] fails fast with `unsupported` on those
//! paths instead of deadlocking.

pub use remotemedia_traits::call::{
    call_reply_method, CallErrorKind, CallReplyEnvelope, CallReplyEvent, CallRequestEnvelope,
    CALL_AUX_PORT, CALL_REPLY_METHOD, DEFAULT_CALL_DEADLINE_MS,
};
use remotemedia_traits::rpc::{RpcMode, RpcRequest};
use remotemedia_types::{Error, RuntimeData};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// Extra time the helper waits past the call deadline for the host's own
/// `timeout` reply before giving up locally (e.g. an older host that does
/// not route `__call__` envelopes).
const LOCAL_DEADLINE_GRACE: Duration = Duration::from_millis(250);

tokio::task_local! {
    static BATCHED_PATH: ();
}

/// Run `fut` marked as a batched process path: endpoint calls made inside
/// fail fast with `unsupported`. The FFI adapter wraps `process` and
/// `process_multi` with this.
pub async fn on_batched_path<F: Future>(fut: F) -> F::Output {
    BATCHED_PATH.scope((), fut).await
}

/// Whether the current task runs a batched process path.
pub fn in_batched_path() -> bool {
    BATCHED_PATH.try_with(|_| ()).is_ok()
}

/// Options of one endpoint call.
#[derive(Debug, Clone)]
pub struct CallOptions {
    pub mode: RpcMode,
    pub deadline_ms: u64,
    pub hold_ms: Option<u64>,
    pub route_hint: Option<String>,
    pub adapter_hint: Option<String>,
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            mode: RpcMode::Reply,
            deadline_ms: DEFAULT_CALL_DEADLINE_MS,
            hold_ms: None,
            route_hint: None,
            adapter_hint: None,
        }
    }
}

impl CallOptions {
    pub fn mode(mut self, mode: RpcMode) -> Self {
        self.mode = mode;
        self
    }
    pub fn deadline_ms(mut self, ms: u64) -> Self {
        self.deadline_ms = ms;
        self
    }
    pub fn hold_ms(mut self, ms: u64) -> Self {
        self.hold_ms = Some(ms);
        self
    }
    pub fn route_hint(mut self, hint: impl Into<String>) -> Self {
        self.route_hint = Some(hint.into());
        self
    }
    pub fn adapter_hint(mut self, hint: impl Into<String>) -> Self {
        self.adapter_hint = Some(hint.into());
        self
    }
}

/// Terminal failure of an endpoint call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallError {
    pub kind: CallErrorKind,
    pub message: String,
}

impl CallError {
    fn new(kind: CallErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.message.is_empty() {
            write!(f, "{}", self.kind)
        } else {
            write!(f, "{}: {}", self.kind, self.message)
        }
    }
}

impl std::error::Error for CallError {}

impl From<CallError> for Error {
    fn from(e: CallError) -> Self {
        Error::Execution(format!("endpoint call failed: {e}"))
    }
}

type EmitFn<'a> = dyn FnMut(RuntimeData) -> Result<(), Error> + Send + 'a;

/// Shareable wrapper around a node's output callback, so pending calls can
/// emit their cancellation envelope when dropped.
pub struct Emitter<'a> {
    inner: Arc<Mutex<Box<EmitFn<'a>>>>,
}

impl<'a> Clone for Emitter<'a> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<'a> Emitter<'a> {
    pub fn new<F>(callback: F) -> Self
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send + 'a,
    {
        Self {
            inner: Arc::new(Mutex::new(Box::new(callback))),
        }
    }

    /// Emit a normal output.
    pub fn emit(&self, data: RuntimeData) -> Result<(), Error> {
        let mut cb = self
            .inner
            .lock()
            .map_err(|_| Error::Execution("output callback poisoned".into()))?;
        (cb)(data)
    }
}

/// Per-node-instance correlation table for endpoint calls. Cheap to clone.
#[derive(Clone)]
pub struct EndpointCaller {
    pending: Arc<Mutex<HashMap<String, (mpsc::UnboundedSender<CallReplyEnvelope>, RpcMode)>>>,
    next: Arc<AtomicU64>,
    prefix: Arc<str>,
}

impl Default for EndpointCaller {
    fn default() -> Self {
        Self::new()
    }
}

impl EndpointCaller {
    pub fn new() -> Self {
        static INSTANCE: AtomicU64 = AtomicU64::new(1);
        let n = INSTANCE.fetch_add(1, Ordering::Relaxed);
        Self {
            pending: Arc::default(),
            next: Arc::new(AtomicU64::new(1)),
            prefix: Arc::from(format!("p{n}")),
        }
    }

    /// Route a `__call_reply__` invocation. Returns `None` for any other
    /// method so `call_rpc` can fall through to the node's own methods.
    pub fn handle_rpc(&self, request: &RpcRequest) -> Option<Result<(), Error>> {
        if request.method != CALL_REPLY_METHOD {
            return None;
        }
        let Some(arg) = request.args.first() else {
            return Some(Err(Error::Execution(
                "__call_reply__ requires one CallReplyEnvelope argument".into(),
            )));
        };
        Some(
            serde_json::from_value::<CallReplyEnvelope>(arg.clone())
                .map_err(|e| Error::Execution(format!("invalid __call_reply__ envelope: {e}")))
                .map(|env| {
                    self.deliver(env);
                }),
        )
    }

    /// Hand one reply chunk to its waiting call. `false` when no call with
    /// that `corr_id` is waiting (late or unknown — discarded).
    pub fn deliver(&self, env: CallReplyEnvelope) -> bool {
        let mut pending = self.pending.lock().expect("pending poisoned");
        let (delivered, terminal) = match pending.get(&env.corr_id) {
            Some((tx, mode)) => (tx.send(env.clone()).is_ok(), is_terminal_for(&env, *mode)),
            None => (false, true),
        };
        if terminal || !delivered {
            pending.remove(&env.corr_id);
        }
        delivered
    }

    /// Number of calls waiting for replies.
    pub fn in_flight(&self) -> usize {
        self.pending.lock().map(|p| p.len()).unwrap_or(0)
    }

    /// Issue a call by emitting a `__call__` envelope through `emit`.
    ///
    /// Must be used from a streaming process path (`process_streaming*`,
    /// `tick`); batched paths fail fast with `unsupported`.
    pub fn call<'a>(
        &self,
        emit: &Emitter<'a>,
        endpoint: &str,
        method: &str,
        args: Vec<Value>,
        kwargs: Map<String, Value>,
        opts: CallOptions,
    ) -> Result<PendingCall<'a>, CallError> {
        if in_batched_path() {
            return Err(CallError::new(
                CallErrorKind::Unsupported,
                "endpoint calls need a streaming process path (process_streaming*); \
                 batched process/process_multi cannot receive replies mid-call",
            ));
        }
        let corr_id = format!(
            "{}-{}",
            self.prefix,
            self.next.fetch_add(1, Ordering::Relaxed)
        );
        let mut req = CallRequestEnvelope::new(endpoint, method, corr_id.clone());
        req.mode = opts.mode;
        req.args = args;
        req.kwargs = kwargs;
        req.deadline_ms = opts.deadline_ms;
        req.hold_ms = opts.hold_ms;
        req.route_hint = opts.route_hint;
        req.adapter_hint = opts.adapter_hint;
        let (tx, rx) = mpsc::unbounded_channel();
        self.pending
            .lock()
            .expect("pending poisoned")
            .insert(corr_id.clone(), (tx, opts.mode));
        if let Err(e) = emit.emit(RuntimeData::Json(req.to_aux_json())) {
            self.pending
                .lock()
                .expect("pending poisoned")
                .remove(&corr_id);
            return Err(CallError::new(
                CallErrorKind::Unavailable,
                format!("could not emit __call__ envelope: {e}"),
            ));
        }
        let local_deadline = Duration::from_millis(opts.deadline_ms.max(1)) + LOCAL_DEADLINE_GRACE;
        Ok(PendingCall {
            mode: opts.mode,
            caller: self.clone(),
            emit: emit.clone(),
            endpoint: endpoint.to_string(),
            method: method.to_string(),
            corr_id,
            rx,
            deadline: tokio::time::Instant::now() + local_deadline,
            done: false,
        })
    }
}

/// A `value` completes a `reply` call; `end` / `error` complete any call.
fn is_terminal_for(env: &CallReplyEnvelope, mode: RpcMode) -> bool {
    env.is_terminal() || mode == RpcMode::Reply
}

/// One in-flight call. Dropping it before completion emits a
/// `__call__ {cancel: true}` envelope for the same `corr_id`.
pub struct PendingCall<'a> {
    mode: RpcMode,
    caller: EndpointCaller,
    emit: Emitter<'a>,
    endpoint: String,
    method: String,
    corr_id: String,
    rx: mpsc::UnboundedReceiver<CallReplyEnvelope>,
    deadline: tokio::time::Instant,
    done: bool,
}

impl PendingCall<'_> {
    pub fn corr_id(&self) -> &str {
        &self.corr_id
    }

    /// Next reply chunk (`value`, then the terminal `end` / `error`); `None`
    /// after the terminal chunk.
    pub async fn recv(&mut self) -> Option<CallReplyEnvelope> {
        if self.done {
            return None;
        }
        match tokio::time::timeout_at(self.deadline, self.rx.recv()).await {
            Ok(Some(env)) => {
                if is_terminal_for(&env, self.mode) {
                    self.done = true;
                }
                Some(env)
            }
            Ok(None) => {
                self.done = true;
                Some(CallReplyEnvelope::error(
                    self.corr_id.clone(),
                    CallErrorKind::Cancelled,
                    "reply channel closed",
                ))
            }
            Err(_) => {
                // The host never answered (not even its own timeout):
                // cancel and report a local timeout.
                self.cancel_now();
                self.done = true;
                Some(CallReplyEnvelope::error(
                    self.corr_id.clone(),
                    CallErrorKind::Timeout,
                    "no reply before the call deadline",
                ))
            }
        }
    }

    /// Resolve a `reply` call to its value (`end` without a value ⇒ `Null`).
    pub async fn value(mut self) -> Result<Value, CallError> {
        match self.recv().await {
            Some(CallReplyEnvelope {
                event: CallReplyEvent::Value { data },
                ..
            }) => Ok(data),
            Some(CallReplyEnvelope {
                event: CallReplyEvent::End,
                ..
            }) => Ok(Value::Null),
            Some(CallReplyEnvelope {
                event: CallReplyEvent::Error { error, message },
                ..
            }) => Err(CallError::new(error, message)),
            None => Err(CallError::new(
                CallErrorKind::Cancelled,
                "call already finished",
            )),
        }
    }

    /// Collect a `stream` call's values until `end`.
    pub async fn collect(mut self) -> Result<Vec<Value>, CallError> {
        let mut out = Vec::new();
        while let Some(env) = self.recv().await {
            match env.event {
                CallReplyEvent::Value { data } => out.push(data),
                CallReplyEvent::End => return Ok(out),
                CallReplyEvent::Error { error, message } => {
                    return Err(CallError::new(error, message))
                }
            }
        }
        Ok(out)
    }

    /// Cancel the call now (same as dropping it).
    pub fn cancel(mut self) {
        self.cancel_now();
        self.done = true;
    }

    fn cancel_now(&mut self) {
        self.caller
            .pending
            .lock()
            .map(|mut p| p.remove(&self.corr_id))
            .ok();
        let env = CallRequestEnvelope::cancellation(&self.endpoint, &self.method, &self.corr_id);
        // Best effort: the output path may already be closed (e.g. the
        // processing call was aborted); the host also cancels calls of an
        // interrupted node on its own.
        let _ = self.emit.emit(RuntimeData::Json(env.to_aux_json()));
    }
}

impl Drop for PendingCall<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.cancel_now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collecting() -> (Emitter<'static>, Arc<Mutex<Vec<RuntimeData>>>) {
        let out = Arc::new(Mutex::new(Vec::new()));
        let sink = out.clone();
        (
            Emitter::new(move |d| {
                sink.lock().unwrap().push(d);
                Ok(())
            }),
            out,
        )
    }

    fn reply_request(env: &CallReplyEnvelope) -> RpcRequest {
        RpcRequest {
            method: CALL_REPLY_METHOD.into(),
            corr_id: remotemedia_traits::rpc::RpcId::String(env.corr_id.clone()),
            args: vec![serde_json::to_value(env).unwrap()],
            kwargs: Map::new(),
        }
    }

    #[tokio::test]
    async fn emits_call_envelope_and_resolves_reply() {
        let caller = EndpointCaller::new();
        let (emit, out) = collecting();
        let call = caller
            .call(
                &emit,
                "harness",
                "decide",
                vec![],
                Map::new(),
                CallOptions::default(),
            )
            .unwrap();
        let emitted = out.lock().unwrap()[0].clone();
        let RuntimeData::Json(aux) = emitted else {
            panic!("json")
        };
        let req = CallRequestEnvelope::from_aux_json(&aux).unwrap().unwrap();
        assert_eq!(req.endpoint, "harness");
        assert_eq!(req.corr_id, call.corr_id());
        let env = CallReplyEnvelope::value(req.corr_id.clone(), serde_json::json!({"tool": "x"}));
        caller.handle_rpc(&reply_request(&env)).unwrap().unwrap();
        assert_eq!(
            call.value().await.unwrap(),
            serde_json::json!({"tool": "x"})
        );
        assert_eq!(caller.in_flight(), 0);
        // No cancel envelope after completion.
        assert_eq!(out.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn stream_values_in_order_then_end() {
        let caller = EndpointCaller::new();
        let (emit, _out) = collecting();
        let call = caller
            .call(
                &emit,
                "harness",
                "reply_text",
                vec![],
                Map::new(),
                CallOptions::default().mode(RpcMode::Stream),
            )
            .unwrap();
        let corr = call.corr_id().to_string();
        for i in 0..3 {
            caller.deliver(CallReplyEnvelope::value(corr.clone(), i.into()));
        }
        caller.deliver(CallReplyEnvelope::end(corr));
        assert_eq!(
            call.collect().await.unwrap(),
            vec![Value::from(0), 1.into(), 2.into()]
        );
    }

    #[tokio::test]
    async fn drop_emits_cancellation() {
        let caller = EndpointCaller::new();
        let (emit, out) = collecting();
        let call = caller
            .call(
                &emit,
                "harness",
                "decide",
                vec![],
                Map::new(),
                CallOptions::default(),
            )
            .unwrap();
        let corr = call.corr_id().to_string();
        drop(call);
        let RuntimeData::Json(aux) = out.lock().unwrap()[1].clone() else {
            panic!()
        };
        let req = CallRequestEnvelope::from_aux_json(&aux).unwrap().unwrap();
        assert!(req.cancel);
        assert_eq!(req.corr_id, corr);
        // A late reply is discarded.
        assert!(!caller.deliver(CallReplyEnvelope::end(corr)));
    }

    #[tokio::test]
    async fn batched_path_fails_fast_unsupported() {
        let caller = EndpointCaller::new();
        let (emit, out) = collecting();
        let err = on_batched_path(async {
            caller
                .call(
                    &emit,
                    "harness",
                    "decide",
                    vec![],
                    Map::new(),
                    CallOptions::default(),
                )
                .err()
                .unwrap()
        })
        .await;
        assert_eq!(err.kind, CallErrorKind::Unsupported);
        assert!(out.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn local_deadline_when_host_never_answers() {
        let caller = EndpointCaller::new();
        let (emit, out) = collecting();
        let call = caller
            .call(
                &emit,
                "harness",
                "decide",
                vec![],
                Map::new(),
                CallOptions::default().deadline_ms(10),
            )
            .unwrap();
        let err = call.value().await.unwrap_err();
        assert_eq!(err.kind, CallErrorKind::Timeout);
        // request + cancellation
        assert_eq!(out.lock().unwrap().len(), 2);
    }
}
