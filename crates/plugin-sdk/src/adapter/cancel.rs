//! Plugin-side delivery of a call's cancellation (openspec:
//! add-generator-cancellation).
//!
//! Every media call registers a per-call [`CancelSource`] in the adapter's
//! in-flight slot before its `FfiFuture` is returned. The host's
//! `FfiNode::cancel_call` fires it with the host's cancellation; a call whose
//! future is dropped without one (an older host, an error path) or whose
//! output sink reports the host receiver gone fires it with
//! `CallerCancelled` (abort). The first firing runs the node's
//! `Cancellable::on_cancel` on the plugin runtime.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use remotemedia_traits::rpc::CallContext;
use remotemedia_traits::streaming::AsyncStreamingNode;
use remotemedia_traits::{CancelReason, CancelSource, CancelToken, Cancellation};

use super::plugin_runtime;

/// One call's cancellation: its source, and what to run when it first fires.
pub(crate) struct CallCancel {
    source: CancelSource,
    on_first: Box<dyn Fn(Cancellation) + Send + Sync>,
}

impl CallCancel {
    pub(crate) fn token(&self) -> CancelToken {
        self.source.token()
    }

    /// Fire the cancellation; the first firing runs the node's `on_cancel`.
    pub(crate) fn fire(&self, cancellation: Cancellation) {
        if self.source.cancel(cancellation) {
            (self.on_first)(cancellation);
        }
    }
}

/// The media call in flight on one adapter instance.
#[derive(Default)]
pub(crate) struct InFlight {
    slot: Mutex<Option<(u64, Arc<CallCancel>)>>,
    next: AtomicU64,
}

impl InFlight {
    /// Register a new call. The guard clears the slot when the call ends and
    /// fires the fallback cancellation if the call did not complete.
    pub(crate) fn begin<N>(self: &Arc<Self>, node: Arc<N>, identity: CallContext) -> CallGuard
    where
        N: AsyncStreamingNode + Send + Sync + 'static,
    {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let call = Arc::new(CallCancel {
            source: CancelSource::new(),
            on_first: Box::new(move |cancellation| run_on_cancel(&node, &identity, cancellation)),
        });
        *self.slot.lock().unwrap_or_else(|e| e.into_inner()) = Some((id, call.clone()));
        CallGuard {
            inflight: self.clone(),
            id,
            call,
            completed: false,
        }
    }

    /// `FfiNode::cancel_call`: fire the in-flight call, if any.
    pub(crate) fn cancel(&self, cancellation_json: &str) {
        let cancellation = Cancellation::from_json(cancellation_json)
            .unwrap_or_else(|_| Cancellation::new(CancelReason::CallerCancelled));
        let call = self
            .slot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|(_, call)| call.clone());
        if let Some(call) = call {
            call.fire(cancellation);
        }
    }
}

/// Spawn the node's `on_cancel` on the plugin runtime, if it is a generator.
fn run_on_cancel<N>(node: &Arc<N>, identity: &CallContext, cancellation: Cancellation)
where
    N: AsyncStreamingNode + Send + Sync + 'static,
{
    if node.generator().is_none() {
        return;
    }
    let node = node.clone();
    let identity = identity.clone();
    plugin_runtime().spawn(async move {
        if let Some(role) = node.generator() {
            role.cancellable().on_cancel(&identity, &cancellation).await;
        }
    });
}

/// Held by one media call's future.
pub(crate) struct CallGuard {
    inflight: Arc<InFlight>,
    id: u64,
    call: Arc<CallCancel>,
    completed: bool,
}

impl CallGuard {
    pub(crate) fn token(&self) -> CancelToken {
        self.call.token()
    }

    /// The call's cancel handle, for an output sink that sees the host
    /// receiver gone.
    pub(crate) fn handle(&self) -> Arc<CallCancel> {
        self.call.clone()
    }

    /// The call ran to its end (success or error): not a cancel.
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        {
            let mut slot = self.inflight.slot.lock().unwrap_or_else(|e| e.into_inner());
            if slot.as_ref().is_some_and(|(id, _)| *id == self.id) {
                *slot = None;
            }
        }
        if !self.completed {
            self.call
                .fire(Cancellation::new(CancelReason::CallerCancelled));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use remotemedia_traits::{
        CancelStrategy, Cancellable, Disposition, GeneratorRole, Retention, TextGenerator,
    };
    use remotemedia_types::{Error, RuntimeData};
    use std::time::Duration;

    #[derive(Default)]
    struct Gen {
        seen: Mutex<Vec<Cancellation>>,
    }
    #[async_trait::async_trait]
    impl Cancellable for Gen {
        fn cancel_strategy(&self) -> CancelStrategy {
            CancelStrategy::OnCancel
        }
        async fn on_cancel(&self, _call: &CallContext, cancellation: &Cancellation) {
            self.seen.lock().unwrap().push(*cancellation);
        }
    }
    impl TextGenerator for Gen {}
    #[async_trait::async_trait]
    impl AsyncStreamingNode for Gen {
        fn node_type(&self) -> &str {
            "Gen"
        }
        fn generator(&self) -> Option<GeneratorRole<'_>> {
            GeneratorRole::text(self)
        }
        async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
            Ok(data)
        }
    }

    fn seen(node: &Gen) -> Vec<Cancellation> {
        for _ in 0..200 {
            let seen = node.seen.lock().unwrap().clone();
            if !seen.is_empty() {
                std::thread::sleep(Duration::from_millis(20));
                return node.seen.lock().unwrap().clone();
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        Vec::new()
    }

    #[test]
    fn host_cancel_then_drop_runs_on_cancel_once_with_host_disposition() {
        let node = Arc::new(Gen::default());
        let inflight = Arc::new(InFlight::default());
        let guard = inflight.begin(node.clone(), CallContext::default());
        let token = guard.token();
        let host = Cancellation::with_disposition(
            CancelReason::BargeIn,
            Disposition {
                pending_input: Retention::Discard,
                ..Disposition::INTERRUPT
            },
        );
        inflight.cancel(&host.to_json());
        assert_eq!(token.cancellation(), Some(host));
        drop(guard);
        assert_eq!(seen(&node), vec![host]);
    }

    #[test]
    fn drop_without_cancel_call_fires_caller_cancelled_abort() {
        let node = Arc::new(Gen::default());
        let inflight = Arc::new(InFlight::default());
        let guard = inflight.begin(node.clone(), CallContext::default());
        let token = guard.token();
        drop(guard);
        let c = token.cancellation().expect("fallback fired");
        assert_eq!(c.reason, CancelReason::CallerCancelled);
        assert_eq!(c.disposition, Disposition::ABORT);
        assert_eq!(seen(&node), vec![c]);
    }

    #[test]
    fn cancel_after_completion_is_a_no_op() {
        let node = Arc::new(Gen::default());
        let inflight = Arc::new(InFlight::default());
        let guard = inflight.begin(node.clone(), CallContext::default());
        let token = guard.token();
        guard.complete();
        inflight.cancel(&Cancellation::new(CancelReason::BargeIn).to_json());
        assert!(!token.is_cancelled());
        // The next call is not touched by a stale cancel either.
        let next = inflight.begin(node.clone(), CallContext::default());
        assert!(!next.token().is_cancelled());
        next.complete();
        std::thread::sleep(Duration::from_millis(30));
        assert!(node.seen.lock().unwrap().is_empty());
    }

    #[test]
    fn transform_gets_token_but_no_on_cancel() {
        struct Transform;
        #[async_trait::async_trait]
        impl AsyncStreamingNode for Transform {
            fn node_type(&self) -> &str {
                "T"
            }
            async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
                Ok(data)
            }
        }
        let inflight = Arc::new(InFlight::default());
        let guard = inflight.begin(Arc::new(Transform), CallContext::default());
        let token = guard.token();
        inflight.cancel(&Cancellation::new(CancelReason::Superseded).to_json());
        assert_eq!(
            token.cancellation().unwrap().reason,
            CancelReason::Superseded
        );
    }
}
