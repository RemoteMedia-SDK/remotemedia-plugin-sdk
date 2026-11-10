//! Actual ONNX output binding through the production Python runner and host.
use super::*;
use remotemedia_shared_memory::PeerId;
use remotemedia_types::CopyContext;
use std::path::{Path, PathBuf};

struct Markers(PathBuf);
impl Markers {
    fn new() -> Self {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self(std::env::temp_dir().join(format!("rm-onnx-{}-{unique}", std::process::id())))
    }
    fn path(&self, suffix: &str) -> PathBuf {
        self.0.with_extension(suffix)
    }
}
impl Drop for Markers {
    fn drop(&mut self) {
        // Also release the gated thread if an assertion panics.
        let _ = std::fs::write(self.path("release"), "release");
    }
}

async fn wait_for(path: &Path) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

fn model_input(pool: &SharedPool) -> RuntimeData {
    // Three model inputs share one owner; no Python packing/casting is needed.
    let mut loan = pool.allocate::<u8>(3336).unwrap();
    for (index, chunk) in loan[..2304].chunks_exact_mut(4).enumerate() {
        chunk.copy_from_slice(&((index as f32 * 0.1).sin() * 0.2).to_le_bytes());
    }
    loan[3328..].copy_from_slice(&16000_i64.to_le_bytes());
    RuntimeData::Binary(loan.freeze())
}

async fn model_node(
    pool: &SharedPool,
    markers: &Markers,
    gated: bool,
) -> (Arc<dyn StreamingNode>, CopyContext) {
    let root = PathBuf::from(std::env::var("MAPPED_SDK_PYTHON").unwrap());
    let params = serde_json::json!({
        "model": root.join("../../crates/core/silero_vad.onnx"),
        "cleanup": markers.path("cleanup"),
        "gate": gated.then(|| markers.path("release")),
    });
    // Load the exact built extension rather than an unrelated installed wheel.
    // The bootstrap still claims the private channel before importing models.
    let mut command = Command::new(std::env::var("MAPPED_WORKER_PYTHON").unwrap());
    command.env("PYTHONPATH", &root).args([
        "-c", "import importlib.util as u, importlib.machinery as m, os, runpy, sys; p=os.environ['NATIVE_BUFFER_EXTENSION']; s=u.spec_from_file_location('_remotemedia_buffers',p,loader=m.ExtensionFileLoader('_remotemedia_buffers',p)); module=u.module_from_spec(s); sys.modules[s.name]=module; s.loader.exec_module(module); runpy.run_module('remotemedia_mapped_worker',run_name='__main__')",
        "--node-type", "MappedOnnxFixture", "--node-id", "native-node",
        "--session-id", "native-session", "--params", &params.to_string(),
        "--module-root", root.join("tests/support").to_str().unwrap(),
        "--register-module", "mapped_onnx_fixture",
    ]);
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "onnx-worker-input");
    // SAFETY: checked-in fixture/runner and matching extension retain owners,
    // join all native work and never fork or delegate mapping handles.
    let worker = unsafe {
        NativeWorkerNode::spawn_with_input_copy_context(
            &mut command,
            pool.clone(),
            remotemedia_shared_memory::peer::CallContext {
                session_id: Some("native-session".into()),
                node_id: "native-node".into(),
            },
            Limits::default(),
            Duration::from_secs(30),
            copies.clone(),
        )
    }
    .unwrap();
    let node: Arc<dyn StreamingNode> = Arc::new(traits::AsyncNodeWrapper(Arc::new(worker)));
    tokio::time::timeout(Duration::from_secs(30), node.initialize(&Init::default()))
        .await
        .unwrap()
        .unwrap();
    (node, copies)
}

async fn shutdown(node: &dyn StreamingNode) {
    assert!(tokio::time::timeout(
        Duration::from_secs(5),
        node.process_control_message(shutdown_message(), &context())
    )
    .await
    .unwrap()
    .unwrap());
}

fn assert_model_outputs(pool: &SharedPool, outputs: &[RuntimeData]) {
    assert!(outputs.iter().all(|data| pool.owns_payload(data)));
    assert!(matches!(&outputs[0], RuntimeData::Audio { samples, .. }
        if samples.len() == 1 && (0.0..=1.0).contains(&samples[0])));
    let (
        RuntimeData::Tensor {
            data: state,
            shape,
            dtype: 0,
            ..
        },
        RuntimeData::Tensor {
            data: slice,
            shape: slice_shape,
            dtype: 0,
            ..
        },
    ) = (&outputs[1], &outputs[2])
    else {
        panic!("expected f32 state tensors")
    };
    assert_eq!(shape, &[2, 1, 128]);
    assert_eq!(slice_shape, &[10]);
    let full = pool.export(state, PeerId(992)).unwrap();
    let sliced = pool.export(slice, PeerId(992)).unwrap();
    assert_eq!(full.descriptor().allocation, sliced.descriptor().allocation);
    assert_eq!(sliced.descriptor().offset - full.descriptor().offset, 28);
    assert_eq!(sliced.descriptor().byte_len, 40);
    assert_eq!(&state[28..68], slice.as_ref());
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires MAPPED_WORKER_PYTHON, MAPPED_SDK_PYTHON, NATIVE_BUFFER_EXTENSION and CPU ONNX Runtime"]
async fn real_onnx_outputs_share_allocations_and_survive_worker_shutdown() {
    let pool = pool();
    // Reuse the host fixture pool, leaving precisely three slots for this model.
    let _reserved: Vec<_> = (0..5).map(|_| pool.allocate::<u8>(1).unwrap()).collect();
    let markers = Markers::new();
    let (node, copies) = model_node(&pool, &markers, false).await;
    // Leave room for only the first ONNX output. Failure to bind the second
    // must release the partial reservation and leave this same worker usable.
    let occupied = pool.allocate::<u8>(1).unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(15),
        node.process_async(model_input(&pool), &context()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("output allocator has insufficient capacity"),
        "{error}"
    );
    drop(occupied);
    let outputs = Arc::new(Mutex::new(Vec::new()));
    let sink = outputs.clone();
    let count = tokio::time::timeout(
        Duration::from_secs(15),
        node.process_streaming_async(
            model_input(&pool),
            &context(),
            Box::new(move |data| {
                sink.lock().unwrap().push(data);
                Ok(())
            }),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(count, 3);
    shutdown(node.as_ref()).await;
    assert!(markers.path("cleanup").exists());
    drop(node);
    let mut outputs = std::mem::take(&mut *outputs.lock().unwrap());
    assert_model_outputs(&pool, &outputs);
    // A retained slice consumes the complete allocation until its last owner.
    outputs.remove(1);
    let free = pool.allocate::<u8>(1).unwrap();
    assert!(pool.allocate::<u8>(1).is_err());
    outputs.clear();
    let reclaimed: Vec<_> = (0..2).map(|_| pool.allocate::<u8>(1).unwrap()).collect();
    assert!(pool.allocate::<u8>(1).is_err());
    drop((free, reclaimed));
    assert!(copies
        .report()
        .entries
        .iter()
        .all(|(_, c)| c.copied_bytes == 0));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires MAPPED_WORKER_PYTHON, MAPPED_SDK_PYTHON, NATIVE_BUFFER_EXTENSION and CPU ONNX Runtime"]
async fn cancelled_onnx_worker_joins_bound_native_writes_before_cleanup() {
    let pool = pool();
    let _reserved: Vec<_> = (0..5).map(|_| pool.allocate::<u8>(1).unwrap()).collect();
    let markers = Markers::new();
    let (node, copies) = model_node(&pool, &markers, true).await;
    let worker = node.clone();
    let input = model_input(&pool);
    let task = tokio::spawn(async move {
        worker
            .process_streaming_async(
                input,
                &context(),
                Box::new(|_| panic!("cancelled call must not emit outputs")),
            )
            .await
    });
    wait_for(&markers.path("entered")).await;
    assert!(pool.allocate::<u8>(1).is_err());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    wait_for(&markers.path("cancelled")).await;
    let worker = node.clone();
    let closing = tokio::spawn(async move { shutdown(worker.as_ref()).await });
    // Observe the worker's closing state, rather than relying on a timed sleep
    // to assume that the shutdown request has arrived.
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Err(error) = node
                .process_control_message(RuntimeData::Text("shutdown probe".into()), &context())
                .await
            {
                if error.to_string().contains("shutting down") {
                    break;
                }
                // A probe accepted just before shutdown is cancelled alongside
                // the inference. Retry to observe the closed admission gate.
                assert!(error.to_string().contains("cancelled"), "{error}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(!closing.is_finished());
    assert!(!markers.path("cleanup").exists());
    assert!(pool.allocate::<u8>(1).is_err());
    std::fs::write(markers.path("release"), "release").unwrap();
    closing.await.unwrap();
    assert!(markers.path("finished").exists());
    assert!(markers.path("cleanup").exists());
    drop(node);
    let reclaimed: Vec<_> = (0..3).map(|_| pool.allocate::<u8>(1).unwrap()).collect();
    assert!(pool.allocate::<u8>(1).is_err());
    drop(reclaimed);
    assert!(copies
        .report()
        .entries
        .iter()
        .all(|(_, c)| c.copied_bytes == 0));
}
