//! Generator conformance (openspec: add-generator-cancellation).
//!
//! [`assert_generator_cancels`] drives a generator the way the runtime does:
//! it starts a call, waits for its first output, fires the call's token with
//! a cancellation, runs the node's `on_cancel`, and drops the call. It then
//! checks that the abandoned call does not hold up the next one: the next
//! call's time to first output stays within `next_call_slack` of a baseline.
//! A generator whose work keeps running after the drop (on a thread, in a
//! worker) fails here — the regression the Gemma assistant had.
//!
//! What a node does with each disposition (keeping a cut-off reply, dropping
//! queued text) is its own state; check it in the node's tests, using the
//! cancellations this report lists.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use remotemedia_traits::rpc::CallContext;
use remotemedia_traits::{
    CancelReason, CancelSource, CancelToken, Cancellation, Disposition, MediaCapabilities,
    NodeRuntimeContextRead,
};
use remotemedia_types::RuntimeData;

use crate::traits::streaming::AsyncStreamingNode;

/// How [`assert_generator_cancels`] runs.
#[derive(Debug, Clone)]
pub struct ConformanceOptions {
    /// The cancel's reason.
    pub reason: CancelReason,
    /// One cancelled call per disposition.
    pub dispositions: Vec<Disposition>,
    /// How much later than the baseline the next call may reach its first
    /// output.
    pub next_call_slack: Duration,
    /// How long a call may take to its first output at all.
    pub first_output_timeout: Duration,
    /// Which outputs count as generated content (a node may emit status
    /// frames first). Default: any output.
    pub counts: fn(&RuntimeData) -> bool,
}

impl Default for ConformanceOptions {
    fn default() -> Self {
        ConformanceOptions {
            reason: CancelReason::BargeIn,
            dispositions: vec![Disposition::INTERRUPT, Disposition::ABORT],
            next_call_slack: Duration::from_millis(500),
            first_output_timeout: Duration::from_secs(30),
            counts: |_| true,
        }
    }
}

/// What the check measured.
#[derive(Debug, Clone)]
pub struct ConformanceReport {
    /// Time to first output of a call on the fresh node.
    pub baseline: Duration,
    /// For each cancelled call: the cancellation delivered, and the time to
    /// first output of the call after it.
    pub cancelled: Vec<(Cancellation, Duration)>,
}

/// The context a call runs with: identity and its cancel token.
struct Ctx {
    call: CallContext,
    cancel: CancelToken,
}

impl NodeRuntimeContextRead for Ctx {
    fn cancel_token(&self) -> CancelToken {
        self.cancel.clone()
    }
    fn session_id(&self) -> &str {
        self.call.session_id.as_deref().unwrap_or("")
    }
    fn node_id(&self) -> &str {
        &self.call.node_id
    }
    fn capability(&self, _: &str) -> Option<MediaCapabilities> {
        None
    }
    fn insert_capability(&self, _: &str, _: MediaCapabilities) {}
    fn snapshot_any(&self, _: &str) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
    fn state_any(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
    fn try_state_any(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
}

struct Running {
    source: CancelSource,
    task: tokio::task::JoinHandle<()>,
    first: tokio::sync::oneshot::Receiver<Instant>,
    started: Instant,
}

fn start<N>(
    node: &Arc<N>,
    call: &CallContext,
    input: RuntimeData,
    counts: fn(&RuntimeData) -> bool,
) -> Running
where
    N: AsyncStreamingNode + Send + Sync + 'static,
{
    let source = CancelSource::new();
    let ctx = Ctx {
        call: call.clone(),
        cancel: source.token(),
    };
    let (first_tx, first) = tokio::sync::oneshot::channel();
    let first_tx = Mutex::new(Some(first_tx));
    let node = node.clone();
    let task = tokio::spawn(async move {
        let _ = node
            .process_streaming_with_ctx(input, &ctx, move |out| {
                if !counts(&out) {
                    return Ok(());
                }
                if let Some(tx) = first_tx.lock().unwrap().take() {
                    let _ = tx.send(Instant::now());
                }
                Ok(())
            })
            .await;
    });
    Running {
        source,
        task,
        first,
        started: Instant::now(),
    }
}

async fn first_output(running: &mut Running, timeout: Duration) -> Result<Duration, String> {
    match tokio::time::timeout(timeout, &mut running.first).await {
        Ok(Ok(at)) => Ok(at.duration_since(running.started)),
        Ok(Err(_)) => Err("the call ended without any output".into()),
        Err(_) => Err(format!("no output within {timeout:?}")),
    }
}

/// Cancel a running call as the runtime does: fire the token, run
/// `on_cancel`, drop the call.
async fn cancel<N>(node: &Arc<N>, call: &CallContext, running: Running, cancellation: Cancellation)
where
    N: AsyncStreamingNode + Send + Sync + 'static,
{
    running.source.cancel(cancellation);
    if let Some(role) = node.generator() {
        role.cancellable().on_cancel(call, &cancellation).await;
    }
    running.task.abort();
    let _ = running.task.await;
}

/// Check that `node` stops a cancelled call without holding up the next one.
/// `input` builds the input of each call (every call gets a fresh one).
pub async fn assert_generator_cancels<N>(
    node: Arc<N>,
    input: impl Fn() -> RuntimeData,
    opts: ConformanceOptions,
) -> Result<ConformanceReport, String>
where
    N: AsyncStreamingNode + Send + Sync + 'static,
{
    let node_type = node.node_type().to_string();
    if node.generator().is_none() {
        return Err(format!(
            "{node_type}: not a generator (generator() returns None)"
        ));
    }
    let call = CallContext {
        session_id: Some("conformance".into()),
        node_id: "node".into(),
    };
    // Baseline: the first call's time to first output; then it is cancelled
    // like any other.
    let mut running = start(&node, &call, input(), opts.counts);
    let baseline = first_output(&mut running, opts.first_output_timeout)
        .await
        .map_err(|e| format!("{node_type}: baseline call: {e}"))?;
    cancel(&node, &call, running, Cancellation::new(opts.reason)).await;

    let mut cancelled = Vec::new();
    let mut running = start(&node, &call, input(), opts.counts);
    for disposition in &opts.dispositions {
        let elapsed = first_output(&mut running, opts.first_output_timeout)
            .await
            .map_err(|e| format!("{node_type}: call before a {disposition:?} cancel: {e}"))?;
        if elapsed > baseline + opts.next_call_slack {
            return Err(format!(
                "{node_type}: a call after a cancel took {elapsed:?} to its first output \
                 (baseline {baseline:?}, slack {:?}): the cancelled call kept running",
                opts.next_call_slack
            ));
        }
        let cancellation = Cancellation::with_disposition(opts.reason, *disposition);
        cancel(&node, &call, running, cancellation).await;
        cancelled.push((cancellation, elapsed));
        running = start(&node, &call, input(), opts.counts);
    }
    // The call after the last cancel.
    let elapsed = first_output(&mut running, opts.first_output_timeout)
        .await
        .map_err(|e| format!("{node_type}: call after the last cancel: {e}"))?;
    running.task.abort();
    if elapsed > baseline + opts.next_call_slack {
        return Err(format!(
            "{node_type}: the call after the last cancel took {elapsed:?} to its first output \
             (baseline {baseline:?}, slack {:?}): the cancelled call kept running",
            opts.next_call_slack
        ));
    }
    if let Some(last) = cancelled.last_mut() {
        last.1 = elapsed;
    }
    Ok(ConformanceReport {
        baseline,
        cancelled,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use remotemedia_traits::{CancelStrategy, Cancellable, GeneratorRole, TextGenerator};
    use remotemedia_types::Error;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Generates on a thread that holds one engine; each piece takes 20 ms
    /// and a reply has 100 pieces. With `honest`, the thread stops when the
    /// call's token fires.
    struct ThreadGen {
        engine: Arc<Mutex<()>>,
        honest: bool,
        on_cancel_ran: AtomicBool,
    }

    #[async_trait::async_trait]
    impl Cancellable for ThreadGen {
        fn cancel_strategy(&self) -> CancelStrategy {
            CancelStrategy::ObservesToken
        }
        async fn on_cancel(&self, _: &CallContext, _: &Cancellation) {
            self.on_cancel_ran.store(true, Ordering::SeqCst);
        }
    }
    impl TextGenerator for ThreadGen {}

    #[async_trait::async_trait]
    impl AsyncStreamingNode for ThreadGen {
        fn node_type(&self) -> &str {
            if self.honest {
                "HonestGen"
            } else {
                "RunawayGen"
            }
        }
        fn generator(&self) -> Option<GeneratorRole<'_>> {
            GeneratorRole::text(self)
        }
        async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
            Ok(data)
        }
        async fn process_streaming_with_ctx<F>(
            &self,
            _data: RuntimeData,
            ctx: &dyn NodeRuntimeContextRead,
            mut callback: F,
        ) -> Result<usize, Error>
        where
            F: FnMut(RuntimeData) -> Result<(), Error> + Send,
        {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let engine = self.engine.clone();
            let cancel = if self.honest {
                ctx.cancel_token()
            } else {
                CancelToken::never()
            };
            std::thread::spawn(move || {
                let _engine = engine.lock().unwrap();
                for i in 0..100 {
                    if cancel.is_cancelled() {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(20));
                    // A runaway keeps generating after nobody reads.
                    let _ = tx.send(i);
                }
            });
            let mut n = 0;
            while let Some(i) = rx.recv().await {
                callback(RuntimeData::Text(format!("piece {i}")))?;
                n += 1;
            }
            Ok(n)
        }
    }

    fn gen(honest: bool) -> Arc<ThreadGen> {
        Arc::new(ThreadGen {
            engine: Arc::default(),
            honest,
            on_cancel_ran: AtomicBool::new(false),
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_generator_that_stops_passes() {
        let node = gen(true);
        let report = assert_generator_cancels(
            node.clone(),
            || RuntimeData::Text("go".into()),
            ConformanceOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(report.cancelled.len(), 2);
        assert_eq!(report.cancelled[0].0.disposition, Disposition::INTERRUPT);
        assert!(node.on_cancel_ran.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_generator_that_keeps_running_fails() {
        let error = assert_generator_cancels(
            gen(false),
            || RuntimeData::Text("go".into()),
            ConformanceOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(
            error.contains("RunawayGen") && error.contains("kept running"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_transform_is_not_checked() {
        struct T;
        #[async_trait::async_trait]
        impl AsyncStreamingNode for T {
            fn node_type(&self) -> &str {
                "T"
            }
            async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
                Ok(data)
            }
        }
        let error = assert_generator_cancels(
            Arc::new(T),
            || RuntimeData::Text("x".into()),
            ConformanceOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(error.contains("not a generator"), "{error}");
    }
}
