//! Per-call cancellation (openspec: add-generator-cancellation).
//!
//! The runtime cancels a call for several reasons (a barge-in, a newer input,
//! a caller's cancel, a closed session). It fires the call's [`CancelToken`]
//! with a [`Cancellation`] *before* it drops the call's future, so work that
//! lives outside the future (a model thread, a worker process, a GPU stream,
//! a remote request) can stop, and so the node knows what to do with data.
//!
//! A [`Cancellation`] carries a [`Disposition`]: whether the node keeps what it
//! generated before the cancel in its own state (`partial_output`) and whether
//! input already received or queued is kept for the next call
//! (`pending_input`). An interrupt ([`Disposition::INTERRUPT`]) keeps both; an
//! abort ([`Disposition::ABORT`]) discards both.
//!
//! The token is std-only: it works from async code (`cancelled().await`), from
//! blocking threads (`is_cancelled`, `wait`) and from callbacks (`on_cancel`),
//! with no async runtime dependency.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Why a call was cancelled.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CancelReason {
    /// A `barge_in` (speech over the reply, an interrupt control).
    BargeIn,
    /// A newer input replaced the call on a latest-input-wins node.
    Superseded,
    /// The caller cancelled the call (an RPC or endpoint cancel, a dropped
    /// plugin call with no reason given).
    CallerCancelled,
    /// The input that started the call was retracted (`CancelSpeculation`).
    Retracted,
    /// The session closed while the call ran.
    SessionClosed,
    /// The runtime is shutting down.
    Shutdown,
    /// The call missed a deadline.
    Deadline,
}

impl CancelReason {
    /// Every reason, for exhaustive tests and schema parity.
    pub const ALL: [CancelReason; 7] = [
        CancelReason::BargeIn,
        CancelReason::Superseded,
        CancelReason::CallerCancelled,
        CancelReason::Retracted,
        CancelReason::SessionClosed,
        CancelReason::Shutdown,
        CancelReason::Deadline,
    ];

    /// The disposition a cancel with this reason has when nobody chose one:
    /// a barge-in or a supersession interrupts, everything else aborts.
    pub const fn default_disposition(self) -> Disposition {
        match self {
            CancelReason::BargeIn | CancelReason::Superseded => Disposition::INTERRUPT,
            CancelReason::CallerCancelled
            | CancelReason::Retracted
            | CancelReason::SessionClosed
            | CancelReason::Shutdown
            | CancelReason::Deadline => Disposition::ABORT,
        }
    }
}

/// Keep or drop one kind of data on a cancel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Retention {
    Retain,
    Discard,
}

/// What a cancel does with data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct Disposition {
    /// What the node does with what it generated before the cancel: commit it
    /// to its own state (an LLM keeps the cut-off reply in history, marked
    /// interrupted) or roll it back. Output already emitted downstream is
    /// never recalled.
    pub partial_output: Retention,
    /// Input already received or queued for the node, including input the
    /// node buffered internally: kept for the next call, or dropped.
    pub pending_input: Retention,
}

impl Disposition {
    /// Stop generating; keep the partial output and the pending input.
    pub const INTERRUPT: Disposition = Disposition {
        partial_output: Retention::Retain,
        pending_input: Retention::Retain,
    };
    /// The turn is void: roll back the partial output, drop pending input.
    pub const ABORT: Disposition = Disposition {
        partial_output: Retention::Discard,
        pending_input: Retention::Discard,
    };

    /// Apply an override's fields on top of this disposition.
    pub fn with(self, over: DispositionOverride) -> Disposition {
        Disposition {
            partial_output: over.partial_output.unwrap_or(self.partial_output),
            pending_input: over.pending_input.unwrap_or(self.pending_input),
        }
    }
}

/// A partial disposition, as a canceller (`barge_in` envelope, RPC cancel) or
/// a manifest node's `on_cancel` gives it. Absent fields fall through.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DispositionOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partial_output: Option<Retention>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_input: Option<Retention>,
}

impl DispositionOverride {
    pub fn is_empty(&self) -> bool {
        self.partial_output.is_none() && self.pending_input.is_none()
    }

    /// Fields of `self` win; the rest come from `lower`.
    pub fn or(self, lower: DispositionOverride) -> DispositionOverride {
        DispositionOverride {
            partial_output: self.partial_output.or(lower.partial_output),
            pending_input: self.pending_input.or(lower.pending_input),
        }
    }
}

impl From<Disposition> for DispositionOverride {
    fn from(d: Disposition) -> Self {
        DispositionOverride {
            partial_output: Some(d.partial_output),
            pending_input: Some(d.pending_input),
        }
    }
}

/// What a node's `default_disposition` is told about a cancel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelContext {
    pub reason: CancelReason,
    /// Time since the call started, when the host knows it.
    pub elapsed: Option<Duration>,
}

impl CancelContext {
    pub fn new(reason: CancelReason) -> Self {
        CancelContext {
            reason,
            elapsed: None,
        }
    }
}

/// Resolve a cancel's disposition, per field, in order of precedence: the
/// canceller, the manifest node's `on_cancel`, the node's default (which
/// itself starts from the reason's default).
pub fn resolve_disposition(
    canceller: Option<DispositionOverride>,
    manifest: Option<DispositionOverride>,
    node_default: Disposition,
) -> Disposition {
    let over = canceller
        .unwrap_or_default()
        .or(manifest.unwrap_or_default());
    node_default.with(over)
}

/// A call's cancellation: why, and what to do with data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Cancellation {
    pub reason: CancelReason,
    pub disposition: Disposition,
}

impl Cancellation {
    /// A cancellation with the reason's default disposition.
    pub const fn new(reason: CancelReason) -> Self {
        Cancellation {
            reason,
            disposition: reason.default_disposition(),
        }
    }

    pub const fn with_disposition(reason: CancelReason, disposition: Disposition) -> Self {
        Cancellation {
            reason,
            disposition,
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("Cancellation serializes")
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// A cancellation as it crosses a boundary to a node in another library or
/// process (`FfiNode::cancel_call`, a worker's cancel frame). `cancellation`
/// is the host's resolution; `explicit` is what the canceller and manifest
/// chose, so the node can re-resolve with its own default for this cancel
/// (which may depend on `elapsed_ms`, e.g. an early-interrupt rollback).
///
/// Wire rule, shared with Python: a request that carries `explicit` or
/// `elapsed_ms` is re-resolved by the node; one with only `cancellation`
/// (or a bare `Cancellation`) is final. `explicit` is therefore always
/// serialized, also when empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CancelRequest {
    pub cancellation: Cancellation,
    #[serde(default)]
    pub explicit: DispositionOverride,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<u64>,
}

impl CancelRequest {
    pub fn new(cancellation: Cancellation) -> Self {
        CancelRequest {
            cancellation,
            explicit: DispositionOverride::default(),
            elapsed_ms: None,
        }
    }

    pub fn context(&self) -> CancelContext {
        CancelContext {
            reason: self.cancellation.reason,
            elapsed: self.elapsed_ms.map(Duration::from_millis),
        }
    }

    /// The cancellation with the node's own default under the explicit
    /// choices.
    pub fn resolve_with(&self, node_default: Disposition) -> Cancellation {
        Cancellation::with_disposition(self.cancellation.reason, node_default.with(self.explicit))
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("CancelRequest serializes")
    }

    /// A request whose disposition is final: the node's default does not
    /// apply.
    pub fn exact(cancellation: Cancellation) -> Self {
        CancelRequest {
            cancellation,
            explicit: cancellation.disposition.into(),
            elapsed_ms: None,
        }
    }

    /// Accepts a `CancelRequest`, or a bare `Cancellation`. One that carries
    /// neither `explicit` nor `elapsed_ms`, and a bare `Cancellation`, are
    /// taken as [`CancelRequest::exact`].
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        let value: serde_json::Value = serde_json::from_str(json)?;
        let reresolve = value.get("explicit").is_some() || value.get("elapsed_ms").is_some();
        match serde_json::from_value::<CancelRequest>(value.clone()) {
            Ok(request) if reresolve => Ok(request),
            Ok(request) => Ok(CancelRequest::exact(request.cancellation)),
            Err(_) => serde_json::from_value::<Cancellation>(value).map(CancelRequest::exact),
        }
    }
}

/// A node's default disposition for each reason, declared up front so a host
/// can apply `pending_input` to its own queues for a node in another library
/// or process (plugin `pacing()` JSON, a Python node's discovery frame).
pub type DefaultDispositions = std::collections::BTreeMap<CancelReason, Disposition>;

/// Evaluate a node's `default_disposition` for every reason (no elapsed time).
pub fn default_dispositions(
    default_for: impl Fn(&CancelContext) -> Disposition,
) -> DefaultDispositions {
    CancelReason::ALL
        .into_iter()
        .map(|reason| (reason, default_for(&CancelContext::new(reason))))
        .collect()
}

// ─── Token ──────────────────────────────────────────────────────────────

type Callback = Box<dyn FnOnce(&Cancellation) + Send + 'static>;

#[derive(Default)]
struct State {
    cancellation: Option<Cancellation>,
    callbacks: Vec<(u64, Callback)>,
    wakers: Vec<(u64, Waker)>,
    children: Vec<Weak<Inner>>,
    next_id: u64,
}

#[derive(Default)]
struct Inner {
    fired: AtomicBool,
    state: Mutex<State>,
    condvar: Condvar,
}

impl Inner {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // A panicking callback never runs under the lock, so poisoning only
        // follows a panic elsewhere; the state stays consistent.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn fire(&self, cancellation: Cancellation) -> bool {
        let (callbacks, wakers, children) = {
            let mut state = self.lock();
            if state.cancellation.is_some() {
                return false;
            }
            state.cancellation = Some(cancellation);
            self.fired.store(true, Ordering::Release);
            (
                std::mem::take(&mut state.callbacks),
                std::mem::take(&mut state.wakers),
                std::mem::take(&mut state.children),
            )
        };
        self.condvar.notify_all();
        for (_, callback) in callbacks {
            // A node's callback must not take down the canceller.
            let _ =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(&cancellation)));
        }
        for (_, waker) in wakers {
            waker.wake();
        }
        for child in children {
            if let Some(child) = child.upgrade() {
                child.fire(cancellation);
            }
        }
        true
    }

    fn cancellation(&self) -> Option<Cancellation> {
        if !self.fired.load(Ordering::Acquire) {
            return None;
        }
        self.lock().cancellation
    }
}

/// Fires a call's cancellation. Held by whoever may cancel the call (the
/// host, or a node cancelling its own sub-work through [`CancelToken::child`]).
#[derive(Clone, Default)]
pub struct CancelSource {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for CancelSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancelSource")
            .field("cancellation", &self.inner.cancellation())
            .finish()
    }
}

impl CancelSource {
    pub fn new() -> Self {
        Self::default()
    }

    /// The observing side of this source.
    pub fn token(&self) -> CancelToken {
        CancelToken {
            inner: Some(self.inner.clone()),
        }
    }

    /// Fire the cancellation. Only the first call has an effect; returns
    /// whether this one did. Callbacks run on the calling thread.
    pub fn cancel(&self, cancellation: Cancellation) -> bool {
        self.inner.fire(cancellation)
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.fired.load(Ordering::Acquire)
    }
}

/// Observes a call's cancellation. Cheap to clone and `Send + Sync`.
#[derive(Clone, Default)]
pub struct CancelToken {
    /// `None` is a token that never fires.
    inner: Option<Arc<Inner>>,
}

impl std::fmt::Debug for CancelToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancelToken")
            .field("cancellation", &self.cancellation())
            .finish()
    }
}

impl CancelToken {
    /// A token that never fires: the token of a context that has no
    /// cancellation (a transform's call in a host that predates this API).
    pub const fn never() -> Self {
        CancelToken { inner: None }
    }

    /// Whether the call has been cancelled. One atomic load.
    pub fn is_cancelled(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| inner.fired.load(Ordering::Acquire))
    }

    /// The cancellation, once fired.
    pub fn cancellation(&self) -> Option<Cancellation> {
        self.inner.as_ref().and_then(|inner| inner.cancellation())
    }

    /// Resolves when the call is cancelled. `select!`-able; never resolves
    /// for [`CancelToken::never`].
    pub fn cancelled(&self) -> Cancelled {
        Cancelled {
            inner: self.inner.clone(),
            key: None,
        }
    }

    /// Block the current thread until the call is cancelled or `timeout`
    /// passes (`None` waits indefinitely). For model loops on plain threads.
    pub fn wait(&self, timeout: Option<Duration>) -> Option<Cancellation> {
        let inner = self.inner.as_ref()?;
        let mut state = inner.lock();
        match timeout {
            None => {
                while state.cancellation.is_none() {
                    state = inner.condvar.wait(state).unwrap_or_else(|e| e.into_inner());
                }
            }
            Some(timeout) => {
                let deadline = std::time::Instant::now() + timeout;
                while state.cancellation.is_none() {
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        break;
                    }
                    state = inner
                        .condvar
                        .wait_timeout(state, deadline - now)
                        .unwrap_or_else(|e| e.into_inner())
                        .0;
                }
            }
        }
        state.cancellation
    }

    /// Run `f` once when the call is cancelled, on the canceller's thread. If
    /// it already was, `f` runs now on this thread. Dropping the returned
    /// registration before the cancel unregisters `f`.
    #[must_use = "dropping the registration unregisters the callback"]
    pub fn on_cancel(&self, f: impl FnOnce(&Cancellation) + Send + 'static) -> CancelRegistration {
        let Some(inner) = &self.inner else {
            return CancelRegistration::inert();
        };
        let mut state = inner.lock();
        if let Some(cancellation) = state.cancellation {
            drop(state);
            f(&cancellation);
            return CancelRegistration::inert();
        }
        let id = state.next_id;
        state.next_id += 1;
        state.callbacks.push((id, Box::new(f)));
        CancelRegistration {
            inner: Arc::downgrade(inner),
            id: Some(id),
        }
    }

    /// A source for sub-work: it fires when this token fires (with the same
    /// cancellation) or when it is cancelled itself.
    pub fn child(&self) -> CancelSource {
        let child = CancelSource::new();
        if let Some(inner) = &self.inner {
            let mut state = inner.lock();
            match state.cancellation {
                Some(cancellation) => {
                    drop(state);
                    child.cancel(cancellation);
                }
                None => {
                    state.children.retain(|c| c.strong_count() > 0);
                    state.children.push(Arc::downgrade(&child.inner));
                }
            }
        }
        child
    }
}

/// Keeps an [`CancelToken::on_cancel`] callback registered.
#[derive(Debug)]
pub struct CancelRegistration {
    inner: Weak<Inner>,
    id: Option<u64>,
}

impl CancelRegistration {
    fn inert() -> Self {
        CancelRegistration {
            inner: Weak::new(),
            id: None,
        }
    }

    /// Keep the callback registered for the token's lifetime.
    pub fn forget(mut self) {
        self.id = None;
    }
}

impl Drop for CancelRegistration {
    fn drop(&mut self) {
        let (Some(id), Some(inner)) = (self.id, self.inner.upgrade()) else {
            return;
        };
        inner.lock().callbacks.retain(|(cb, _)| *cb != id);
    }
}

/// Future returned by [`CancelToken::cancelled`].
pub struct Cancelled {
    inner: Option<Arc<Inner>>,
    key: Option<u64>,
}

impl Future for Cancelled {
    type Output = Cancellation;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Cancellation> {
        let Some(inner) = self.inner.clone() else {
            return Poll::Pending;
        };
        let mut state = inner.lock();
        if let Some(cancellation) = state.cancellation {
            return Poll::Ready(cancellation);
        }
        match self.key {
            Some(key) => {
                if let Some(slot) = state.wakers.iter_mut().find(|(k, _)| *k == key) {
                    if !slot.1.will_wake(cx.waker()) {
                        slot.1 = cx.waker().clone();
                    }
                }
            }
            None => {
                let key = state.next_id;
                state.next_id += 1;
                state.wakers.push((key, cx.waker().clone()));
                drop(state);
                self.key = Some(key);
            }
        }
        Poll::Pending
    }
}

impl Drop for Cancelled {
    fn drop(&mut self) {
        if let (Some(inner), Some(key)) = (&self.inner, self.key) {
            inner.lock().wakers.retain(|(k, _)| *k != key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn reason_defaults() {
        assert_eq!(
            CancelReason::BargeIn.default_disposition(),
            Disposition::INTERRUPT
        );
        assert_eq!(
            CancelReason::Superseded.default_disposition(),
            Disposition::INTERRUPT
        );
        for reason in [
            CancelReason::CallerCancelled,
            CancelReason::Retracted,
            CancelReason::SessionClosed,
            CancelReason::Shutdown,
            CancelReason::Deadline,
        ] {
            assert_eq!(
                reason.default_disposition(),
                Disposition::ABORT,
                "{reason:?}"
            );
        }
    }

    #[test]
    fn precedence_is_per_field() {
        let canceller = DispositionOverride {
            pending_input: Some(Retention::Discard),
            ..Default::default()
        };
        let manifest = DispositionOverride {
            partial_output: Some(Retention::Discard),
            pending_input: Some(Retention::Retain),
        };
        let resolved = resolve_disposition(Some(canceller), Some(manifest), Disposition::INTERRUPT);
        assert_eq!(resolved, Disposition::ABORT);
        assert_eq!(
            resolve_disposition(None, None, Disposition::INTERRUPT),
            Disposition::INTERRUPT
        );
    }

    #[test]
    fn json_shape() {
        let c = Cancellation::new(CancelReason::BargeIn);
        assert_eq!(
            c.to_json(),
            r#"{"reason":"barge_in","disposition":{"partial_output":"retain","pending_input":"retain"}}"#
        );
        assert_eq!(Cancellation::from_json(&c.to_json()).unwrap(), c);
        let over: DispositionOverride =
            serde_json::from_str(r#"{"pending_input":"discard"}"#).unwrap();
        assert_eq!(over.pending_input, Some(Retention::Discard));
        assert_eq!(over.partial_output, None);
    }

    #[test]
    fn cancel_request_reresolves_with_node_default() {
        let req = CancelRequest {
            cancellation: Cancellation::new(CancelReason::BargeIn),
            explicit: DispositionOverride {
                partial_output: Some(Retention::Retain),
                ..Default::default()
            },
            elapsed_ms: Some(120),
        };
        let json = req.to_json();
        assert_eq!(CancelRequest::from_json(&json).unwrap(), req);
        // A node whose default discards both keeps the explicit partial_output.
        let c = req.resolve_with(Disposition::ABORT);
        assert_eq!(c.disposition.partial_output, Retention::Retain);
        assert_eq!(c.disposition.pending_input, Retention::Discard);
        assert_eq!(req.context().elapsed, Some(Duration::from_millis(120)));
        // A bare Cancellation is accepted, and is final.
        let bare = Cancellation::new(CancelReason::Shutdown);
        let parsed = CancelRequest::from_json(&bare.to_json()).unwrap();
        assert_eq!(parsed, CancelRequest::exact(bare));
        assert_eq!(parsed.resolve_with(Disposition::INTERRUPT), bare);
        // So is a request with only `cancellation` (the rule Python shares).
        let only = format!(r#"{{"cancellation":{}}}"#, bare.to_json());
        assert_eq!(CancelRequest::from_json(&only).unwrap(), CancelRequest::exact(bare));
        // An empty `explicit` is serialized, so the node re-resolves.
        let open = CancelRequest::new(bare);
        assert!(open.to_json().contains(r#""explicit":{}"#), "{}", open.to_json());
        assert_eq!(CancelRequest::from_json(&open.to_json()).unwrap(), open);
    }

    #[test]
    fn default_dispositions_cover_every_reason() {
        let map = default_dispositions(|ctx| ctx.reason.default_disposition());
        assert_eq!(map.len(), CancelReason::ALL.len());
        let json = serde_json::to_string(&map).unwrap();
        assert!(
            json.contains(r#""barge_in":{"partial_output":"retain","pending_input":"retain"}"#),
            "{json}"
        );
    }

    #[test]
    fn fires_once_first_wins() {
        let source = CancelSource::new();
        let token = source.token();
        assert!(!token.is_cancelled());
        assert!(source.cancel(Cancellation::new(CancelReason::BargeIn)));
        assert!(!source.cancel(Cancellation::new(CancelReason::Shutdown)));
        assert_eq!(token.cancellation().unwrap().reason, CancelReason::BargeIn);
    }

    #[test]
    fn never_token_never_fires() {
        let token = CancelToken::never();
        assert!(!token.is_cancelled());
        assert!(token.wait(Some(Duration::from_millis(5))).is_none());
        let ran = Arc::new(AtomicUsize::new(0));
        let r = ran.clone();
        let _reg = token.on_cancel(move |_| {
            r.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn on_cancel_runs_once_from_another_thread() {
        let source = CancelSource::new();
        let token = source.token();
        let ran = Arc::new(AtomicUsize::new(0));
        let r = ran.clone();
        let reg = token.on_cancel(move |c| {
            assert_eq!(c.reason, CancelReason::Superseded);
            r.fetch_add(1, Ordering::SeqCst);
        });
        std::thread::spawn(move || {
            source.cancel(Cancellation::new(CancelReason::Superseded));
            source.cancel(Cancellation::new(CancelReason::Superseded));
        })
        .join()
        .unwrap();
        assert_eq!(ran.load(Ordering::SeqCst), 1);
        drop(reg);
    }

    #[test]
    fn on_cancel_after_fire_runs_immediately() {
        let source = CancelSource::new();
        source.cancel(Cancellation::new(CancelReason::Deadline));
        let ran = Arc::new(AtomicUsize::new(0));
        let r = ran.clone();
        let _reg = source.token().on_cancel(move |_| {
            r.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(ran.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn dropped_registration_does_not_run() {
        let source = CancelSource::new();
        let ran = Arc::new(AtomicUsize::new(0));
        let r = ran.clone();
        drop(source.token().on_cancel(move |_| {
            r.fetch_add(1, Ordering::SeqCst);
        }));
        source.cancel(Cancellation::new(CancelReason::BargeIn));
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn panicking_callback_does_not_stop_others() {
        let source = CancelSource::new();
        let token = source.token();
        let ran = Arc::new(AtomicUsize::new(0));
        let _a = token.on_cancel(|_| panic!("node bug"));
        let r = ran.clone();
        let _b = token.on_cancel(move |_| {
            r.fetch_add(1, Ordering::SeqCst);
        });
        assert!(source.cancel(Cancellation::new(CancelReason::BargeIn)));
        assert_eq!(ran.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn wait_wakes_a_blocked_thread() {
        let source = CancelSource::new();
        let token = source.token();
        let waiter = std::thread::spawn(move || token.wait(None));
        std::thread::sleep(Duration::from_millis(20));
        source.cancel(Cancellation::new(CancelReason::SessionClosed));
        assert_eq!(
            waiter.join().unwrap().unwrap().reason,
            CancelReason::SessionClosed
        );
    }

    #[test]
    fn child_follows_parent_and_cancels_alone() {
        let parent = CancelSource::new();
        let child = parent.token().child();
        let lone = parent.token().child();
        assert!(lone.cancel(Cancellation::new(CancelReason::CallerCancelled)));
        assert!(!parent.is_cancelled());
        parent.cancel(Cancellation::new(CancelReason::BargeIn));
        assert_eq!(
            child.token().cancellation().unwrap().reason,
            CancelReason::BargeIn
        );
        assert_eq!(
            lone.token().cancellation().unwrap().reason,
            CancelReason::CallerCancelled
        );
        // A child made after the parent fired is already cancelled.
        assert!(parent.token().child().is_cancelled());
    }

    #[test]
    fn cancelled_future_resolves() {
        let source = CancelSource::new();
        let fut = source.token().cancelled();
        let handle = std::thread::spawn(move || block_on(fut));
        std::thread::sleep(Duration::from_millis(20));
        source.cancel(Cancellation::new(CancelReason::BargeIn));
        assert_eq!(handle.join().unwrap().reason, CancelReason::BargeIn);
    }

    /// Minimal executor: park until woken.
    fn block_on<F: Future>(fut: F) -> F::Output {
        struct ThreadWaker(std::thread::Thread);
        impl std::task::Wake for ThreadWaker {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
        let mut cx = Context::from_waker(&waker);
        let mut fut = std::pin::pin!(fut);
        loop {
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                return v;
            }
            std::thread::park();
        }
    }
}
