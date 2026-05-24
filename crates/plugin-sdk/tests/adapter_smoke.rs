//! Smoke test for StreamingNodeFfiAdapter.
//!
//! Wraps a trivial Echo node, encodes a RuntimeData::Text via rmp-serde,
//! calls FfiNode::process, decodes the output, asserts equality.

use async_trait::async_trait;
use remotemedia_plugin_sdk::adapter::StreamingNodeFfiAdapter;
use remotemedia_plugin_sdk::traits::streaming::AsyncStreamingNode;
use remotemedia_plugin_sdk::types::{Error, RuntimeData};
use remotemedia_plugin_sdk::FfiNode;

struct Echo;

#[async_trait]
impl AsyncStreamingNode for Echo {
    fn node_type(&self) -> &str {
        "Echo"
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        Ok(data)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn adapter_round_trips_text() {
    let node = StreamingNodeFfiAdapter::new(Echo);
    let input = RuntimeData::Text("hello".to_string());
    let bytes = rmp_serde::to_vec_named(&input).unwrap();

    let result = node.process(abi_stable::std_types::RVec::from(bytes)).await;

    let out_bytes = match result {
        abi_stable::std_types::RResult::ROk(b) => b,
        abi_stable::std_types::RResult::RErr(e) => panic!("adapter returned error: {}", e),
    };

    let decoded: RuntimeData = rmp_serde::from_slice(out_bytes.as_slice()).unwrap();
    assert_eq!(input, decoded);
}

/// Node that exercises a Tokio-runtime-only API inside `process()`.
/// Without the adapter's runtime injection, `tokio::time::sleep`
/// panics with "there is no reactor running" because the host polls
/// our FfiFuture from a thread that has no Tokio handle in TLS.
struct SleepyEcho;

#[async_trait]
impl AsyncStreamingNode for SleepyEcho {
    fn node_type(&self) -> &str {
        "SleepyEcho"
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(data)
    }
}

/// Drive the FfiFuture from a thread that has NO Tokio runtime in TLS,
/// using `futures::executor::block_on`. This is the exact condition
/// the host's polling thread is in (the panic-trigger we are fixing).
///
/// Without the adapter's `plugin_runtime().spawn(...)` injection, the
/// inner `tokio::time::sleep` would panic with `there is no reactor
/// running`. With the fix, the inner work runs on the plugin's
/// dedicated multi-thread runtime and completes successfully.
///
/// We use a plain `#[test]` (not `#[tokio::test]`) — `#[tokio::test]`
/// installs a runtime in TLS, masking the bug. The work happens on a
/// fresh `std::thread::spawn` so even ambient test-harness state
/// can't leak in.
#[test]
fn adapter_provides_tokio_runtime_to_plugin() {
    let join = std::thread::spawn(|| {
        let node = StreamingNodeFfiAdapter::new(SleepyEcho);
        let input = RuntimeData::Text("sleepy".to_string());
        let bytes = rmp_serde::to_vec_named(&input).unwrap();

        // No tokio runtime in TLS on this thread — we drive the
        // FfiFuture via the futures crate's plain executor, exactly
        // like the FFI host would.
        let fut = node.process(abi_stable::std_types::RVec::from(bytes));
        let result = futures::executor::block_on(fut);

        let out_bytes = match result {
            abi_stable::std_types::RResult::ROk(b) => b,
            abi_stable::std_types::RResult::RErr(e) => panic!("adapter returned error: {}", e),
        };

        let decoded: RuntimeData = rmp_serde::from_slice(out_bytes.as_slice()).unwrap();
        assert_eq!(input, decoded);
    });
    join.join()
        .expect("plugin adapter panicked on no-runtime thread");
}

/// Multi-output node — fires the streaming callback N times per input.
/// Used to verify [`FfiNode::process_multi`] collects ALL emissions
/// into the returned vec (vs. silently dropping all but the first
/// like single-output `process` would).
struct MultiEcho;

#[async_trait]
impl AsyncStreamingNode for MultiEcho {
    fn node_type(&self) -> &str {
        "MultiEcho"
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        // Single-output entry stays correct (echoes once).
        Ok(data)
    }

    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        _session_id: Option<String>,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        // Emit the input THREE times — exactly the multi-output
        // shape (Json + audio + ...) that the FFI surface previously
        // truncated.
        callback(data.clone())?;
        callback(data.clone())?;
        callback(data)?;
        Ok(3)
    }
}

/// Drive `FfiNode::process_multi` from a thread without a Tokio
/// runtime in TLS — same conditions as the host's polling thread —
/// and assert that all 3 emissions come back through the FFI vec.
#[test]
fn adapter_process_multi_collects_streaming_outputs() {
    let join = std::thread::spawn(|| {
        let node = StreamingNodeFfiAdapter::new(MultiEcho);
        let input = RuntimeData::Text("multi".to_string());
        let bytes = rmp_serde::to_vec_named(&input).unwrap();

        let fut = node.process_multi(abi_stable::std_types::RVec::from(bytes));
        let result = futures::executor::block_on(fut);

        let outputs = match result {
            abi_stable::std_types::RResult::ROk(v) => v,
            abi_stable::std_types::RResult::RErr(e) => panic!("adapter returned error: {}", e),
        };

        assert_eq!(
            outputs.len(),
            3,
            "expected 3 outputs from MultiEcho, got {}",
            outputs.len()
        );
        for out_bytes in outputs.iter() {
            let decoded: RuntimeData = rmp_serde::from_slice(out_bytes.as_slice()).unwrap();
            assert_eq!(input, decoded);
        }
    });
    join.join()
        .expect("plugin adapter panicked while collecting multi-output stream");
}

/// Sink that records each `push` into a shared `Vec`. Plain Mutex
/// — no Tokio runtime needed; matches what the test thread has.
struct VecSink {
    store: std::sync::Arc<std::sync::Mutex<Vec<RuntimeData>>>,
}

impl loadable_node_abi::OutputSink for VecSink {
    fn push(
        &self,
        bytes: abi_stable::std_types::RVec<u8>,
    ) -> abi_stable::std_types::RResult<(), abi_stable::std_types::RString> {
        let decoded: RuntimeData = rmp_serde::from_slice(bytes.as_slice()).unwrap();
        self.store.lock().unwrap().push(decoded);
        abi_stable::std_types::ROk(())
    }
}

fn build_vec_sink() -> (
    std::sync::Arc<std::sync::Mutex<Vec<RuntimeData>>>,
    loadable_node_abi::OutputSinkBox,
) {
    let store = std::sync::Arc::new(std::sync::Mutex::new(Vec::<RuntimeData>::new()));
    let sink = loadable_node_abi::OutputSink_TO::from_value(
        VecSink {
            store: std::sync::Arc::clone(&store),
        },
        abi_stable::sabi_trait::TD_Opaque,
    );
    (store, sink)
}

/// `process_streaming` is the per-frame replacement for `process_multi`.
/// The adapter override should forward each callback emission to the
/// sink as it arrives. Verify the MultiEcho (3 emissions) actually
/// pushes 3 frames through the sink and returns count=3.
#[test]
fn adapter_process_streaming_delivers_all_emissions() {
    let join = std::thread::spawn(|| {
        let node = StreamingNodeFfiAdapter::new(MultiEcho);
        let input = RuntimeData::Text("stream".to_string());
        let bytes = rmp_serde::to_vec_named(&input).unwrap();

        let (received, sink) = build_vec_sink();

        let fut = node.process_streaming(abi_stable::std_types::RVec::from(bytes), sink);
        let result = futures::executor::block_on(fut);

        let count = match result {
            abi_stable::std_types::RResult::ROk(c) => c,
            abi_stable::std_types::RResult::RErr(e) => panic!("adapter returned error: {}", e),
        };

        assert_eq!(count, 3, "expected count=3, got {}", count);
        let frames = received.lock().unwrap();
        assert_eq!(
            frames.len(),
            3,
            "expected 3 frames pushed to sink, got {}",
            frames.len()
        );
        for f in frames.iter() {
            assert_eq!(*f, input);
        }
    });
    join.join().expect("streaming adapter test panicked");
}

/// Single-output `Echo` doesn't override `process_streaming` on the
/// `AsyncStreamingNode` side — the trait's default impl delegates to
/// `process` and invokes the callback once. The adapter's
/// `process_streaming` should still forward that single emission to
/// the sink and return count=1.
#[test]
fn adapter_process_streaming_back_compat_for_single_output() {
    let join = std::thread::spawn(|| {
        let node = StreamingNodeFfiAdapter::new(Echo);
        let input = RuntimeData::Text("default-streaming".to_string());
        let bytes = rmp_serde::to_vec_named(&input).unwrap();

        let (received, sink) = build_vec_sink();

        let fut = node.process_streaming(abi_stable::std_types::RVec::from(bytes), sink);
        let result = futures::executor::block_on(fut);

        let count = match result {
            abi_stable::std_types::RResult::ROk(c) => c,
            abi_stable::std_types::RResult::RErr(e) => panic!("adapter returned error: {}", e),
        };

        assert_eq!(count, 1);
        let frames = received.lock().unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0], input);
    });
    join.join()
        .expect("streaming adapter back-compat test panicked");
}

/// Default impl on `FfiNode::process_multi` wraps `process` as a
/// 1-element vec — verify back-compat with single-output plugins
/// that don't override `process_streaming`.
#[test]
fn adapter_process_multi_back_compat_for_single_output() {
    let join = std::thread::spawn(|| {
        // `Echo` only implements `process` — no `process_streaming`
        // override. The default `AsyncStreamingNode::process_streaming`
        // delegates to `process`, so we expect exactly 1 output.
        let node = StreamingNodeFfiAdapter::new(Echo);
        let input = RuntimeData::Text("echo-once".to_string());
        let bytes = rmp_serde::to_vec_named(&input).unwrap();

        let fut = node.process_multi(abi_stable::std_types::RVec::from(bytes));
        let result = futures::executor::block_on(fut);

        let outputs = match result {
            abi_stable::std_types::RResult::ROk(v) => v,
            abi_stable::std_types::RResult::RErr(e) => panic!("adapter returned error: {}", e),
        };

        assert_eq!(
            outputs.len(),
            1,
            "expected 1 output from single-output Echo via process_multi, got {}",
            outputs.len()
        );
        let decoded: RuntimeData = rmp_serde::from_slice(outputs[0].as_slice()).unwrap();
        assert_eq!(input, decoded);
    });
    join.join()
        .expect("plugin adapter panicked on single-output back-compat");
}
