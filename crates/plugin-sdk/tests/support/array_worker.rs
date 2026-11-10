//! Real managed Python array adaptation with the host node's output authority.
use super::*;

#[path = "kokoro_worker.rs"]
mod kokoro;
use remotemedia_plugin_sdk::mapped_python::{
    MappedPythonConfig, MappedPythonFactory, MappedPythonWheels,
};
use remotemedia_py_env::{PythonEnvConfig, PythonEnvMode};
use remotemedia_types::{BoundOutputAllocator, CopyContext, CopyCounts, OutputAllocator};
use std::any::Any;

struct OutputContext {
    copies: Option<CopyContext>,
    allocator: Arc<BoundOutputAllocator>,
}
impl traits::NodeRuntimeContextRead for OutputContext {
    fn session_id(&self) -> &str {
        "native-session"
    }
    fn node_id(&self) -> &str {
        "native-node"
    }
    fn capability(&self, _: &str) -> Option<traits::MediaCapabilities> {
        None
    }
    fn insert_capability(&self, _: &str, _: traits::MediaCapabilities) {}
    fn snapshot_any(&self, _: &str) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }
    fn state_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }
    fn try_state_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        None
    }
    fn copy_context(&self) -> Option<CopyContext> {
        self.copies.clone()
    }
    fn output_allocator(&self) -> Option<Arc<dyn OutputAllocator>> {
        Some(self.allocator.clone())
    }
}

fn counts(context: &CopyContext) -> CopyCounts {
    context
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::BorrowedInput)
        .unwrap()
        .1
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires matching MAPPED_CLIENT_WHEEL, MAPPED_BUFFER_WHEEL, MAPPED_WHEELHOUSE, MAPPED_ENV_CACHE and MAPPED_SDK_PYTHON"]
async fn managed_array_outputs_use_call_policy_and_selected_storage() {
    verify(false).await;
}

#[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires matching mapped wheels and NATIVE_WORKER_SUPERVISOR"]
async fn namespaced_array_outputs_use_call_policy_and_selected_storage() {
    verify(true).await;
}

fn factory(
    pool: SharedPool,
    namespaced: bool,
    node_type: &str,
    module: &str,
) -> MappedPythonFactory {
    let config = MappedPythonConfig {
        node_type: node_type.into(),
        module_root: std::path::PathBuf::from(std::env::var("MAPPED_SDK_PYTHON").unwrap())
            .join("tests/support"),
        entry_module: module.into(),
        requirements: vec![],
        wheels: MappedPythonWheels {
            client: std::env::var("MAPPED_CLIENT_WHEEL").unwrap().into(),
            buffers: std::env::var("MAPPED_BUFFER_WHEEL").unwrap().into(),
        },
        environment: PythonEnvConfig {
            mode: PythonEnvMode::System,
            python_version: "3.12".into(),
            cache_dir: Some(std::env::var("MAPPED_ENV_CACHE").unwrap().into()),
            no_index: true,
            ..Default::default()
        },
    };
    // SAFETY: exact local wheels and the checked-in fixture obey owning mapped
    // publication, joined work and the no-delegation contract.
    let factory = unsafe { MappedPythonFactory::new(config, pool.clone()) }.with_output_copies();
    if namespaced {
        // SAFETY: matching supervisor and fixture, no mapping delegation.
        unsafe { factory.with_linux_namespace(namespace::supervisor(), "/usr/bin/unshare".into()) }
            .unwrap()
    } else {
        factory
    }
}

async fn verify(namespaced: bool) {
    let pool = pool();
    let _reserved: Vec<_> = (0..7).map(|_| pool.allocate::<u8>(1).unwrap()).collect();
    let allocator = Arc::new(BoundOutputAllocator::new(Arc::new(pool.clone())));
    let factory = factory(
        pool.clone(),
        namespaced,
        "MappedFixtureNode",
        "mapped_node_fixture",
    );
    let node = factory.create("native-node".into(), &serde_json::json!({
        "gain":1, "adapt_audio":true,
        "__python_version__":std::env::var("MAPPED_PYTHON_VERSION").unwrap_or("3.12".into()),
        "__python_find_links__":[std::env::var("MAPPED_WHEELHOUSE").unwrap()],
        "__python_env_scope_context__":"array-output-tests",
    }), Some("native-session".into())).unwrap();
    node.initialize(&Init::default()).await.unwrap();
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "node/output");
    let allow = CopyContext::new(CopyPolicy::AllowCopies, "node/output");
    let mut ctx = OutputContext {
        copies: Some(strict.clone()),
        allocator: allocator.clone(),
    };
    let input = || RuntimeData::Text("adapt_audio".into());
    let error = node.process_async(input(), &ctx).await.unwrap_err();
    assert!(error.to_string().contains("copy policy denied"), "{error}");
    ctx.copies = None;
    assert!(node
        .process_async(input(), &ctx)
        .await
        .unwrap_err()
        .to_string()
        .contains("copy policy denied"));
    ctx.copies = Some(allow.clone());
    let outputs = Arc::new(Mutex::new(Vec::new()));
    let sink = outputs.clone();
    let count = node
        .process_streaming_async(
            input(),
            &ctx,
            Box::new(move |value| {
                sink.lock().unwrap().push(value);
                Ok(())
            }),
        )
        .await
        .unwrap();
    assert_eq!(count, 1);
    let held = outputs.lock().unwrap().pop().unwrap();
    assert!(allocator.owns_payload(&held));
    assert!(node
        .process_async(input(), &ctx)
        .await
        .unwrap_err()
        .to_string()
        .contains("insufficient capacity"));
    drop(held);
    let output = node.process_async(input(), &ctx).await.unwrap();
    let RuntimeData::Audio { samples, .. } = &output else {
        panic!("audio")
    };
    let pointer = samples.as_ptr() as usize;
    ctx.copies = Some(strict.clone());
    let retained = node.process_async(output, &ctx).await.unwrap();
    assert!(allocator.owns_payload(&retained));
    let RuntimeData::Audio { samples, .. } = &retained else {
        panic!("audio")
    };
    assert_eq!(samples.as_ptr() as usize, pointer);
    assert_eq!(&samples[..], &[0., 1., 2., 3., 4., 5., 6., 7.]);
    assert_eq!(
        (counts(&strict).rejected, counts(&strict).rejected_bytes),
        (1, 32)
    );
    assert_eq!(
        (
            counts(&allow).completed,
            counts(&allow).copied_bytes,
            counts(&allow).failed,
            counts(&allow).failed_requested_bytes,
            counts(&allow).in_flight
        ),
        (2, 64, 1, 32, 0)
    );
    node.process_control_message(shutdown_message(), &context())
        .await
        .unwrap();
    drop(node);
    assert!(pool.allocate::<u8>(1).is_err());
    drop(retained);
    assert!(pool.allocate::<u8>(1).is_ok());
}
