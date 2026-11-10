//! Real Kokoro adapter, with deterministic NumPy model output in a managed worker.
use super::*;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires matching mapped wheels, wheelhouse, SDK fixture and environment cache"]
async fn managed_kokoro_outputs_obey_host_policy_and_keep_speech_timeline() {
    verify(false).await;
}

#[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires matching mapped wheels and NATIVE_WORKER_SUPERVISOR"]
async fn namespaced_kokoro_outputs_obey_host_policy_and_keep_speech_timeline() {
    verify(true).await;
}

async fn verify(namespaced: bool) {
    let pool = pool();
    let _reserved: Vec<_> = (0..6).map(|_| pool.allocate::<u8>(1).unwrap()).collect();
    let allocator = Arc::new(BoundOutputAllocator::new(Arc::new(pool.clone())));
    let factory = factory(
        pool.clone(),
        namespaced,
        "MappedKokoroFixture",
        "mapped_kokoro_fixture",
    );
    let node = factory.create("native-node".into(), &serde_json::json!({
        "__python_version__":std::env::var("MAPPED_PYTHON_VERSION").unwrap_or("3.12".into()),
        "__python_find_links__":[std::env::var("MAPPED_WHEELHOUSE").unwrap()],
        "__python_env_scope_context__":"kokoro-output-tests",
    }), Some("native-session".into())).unwrap();
    node.initialize(&Init::default()).await.unwrap();
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "kokoro/output");
    let allow = CopyContext::new(CopyPolicy::AllowCopies, "kokoro/output");
    let mut ctx = OutputContext {
        copies: Some(strict.clone()),
        allocator: allocator.clone(),
    };
    let error = node
        .process_streaming_async(
            RuntimeData::Text("Hello.".into()),
            &ctx,
            Box::new(|_| panic!("strict private output")),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("copy policy denied"), "{error}");
    assert_eq!(
        (counts(&strict).rejected, counts(&strict).rejected_bytes),
        (1, 32)
    );

    ctx.copies = Some(allow.clone());
    let outputs = Arc::new(Mutex::new(Vec::new()));
    let sink = outputs.clone();
    let count = node
        .process_streaming_async(
            RuntimeData::Text("Hello.".into()),
            &ctx,
            Box::new(move |data| {
                sink.lock().unwrap().push(data);
                Ok(())
            }),
        )
        .await
        .unwrap();
    assert_eq!(count, 3);
    let mut outputs = std::mem::take(&mut *outputs.lock().unwrap()).into_iter();
    let first = outputs.next().unwrap();
    let second = outputs.next().unwrap();
    let end = outputs.next().unwrap();
    let pointers: Vec<_> = [&first, &second]
        .into_iter()
        .enumerate()
        .map(|(index, output)| {
            assert!(allocator.owns_payload(output));
            let RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                metadata,
                ..
            } = output
            else {
                panic!("audio")
            };
            assert_eq!((*sample_rate, *channels), (24000, 1));
            assert_eq!(
                &samples[..],
                &(0..8)
                    .map(|i| (index * 100 + i * 2) as f32)
                    .collect::<Vec<_>>()
            );
            assert_eq!(metadata.as_ref().unwrap()["pts_us"], (index * 333) as u64);
            assert_eq!(metadata.as_ref().unwrap()["speech_epoch"], 0);
            samples.as_ptr() as usize
        })
        .collect();
    assert_ne!(pointers[0], pointers[1]);
    let RuntimeData::Text(end) = end else {
        panic!("speech end")
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&end).unwrap(),
        serde_json::json!({"kind":"speech_end","speech_epoch":0,"pts_us":666})
    );
    assert_eq!(
        (counts(&allow).completed, counts(&allow).copied_bytes),
        (2, 64)
    );
    assert!(pool.allocate::<u8>(1).is_err());

    ctx.copies = Some(strict.clone());
    let first = node.process_async(first, &ctx).await.unwrap(); // Audio pass-through.
    let RuntimeData::Audio { samples, .. } = &first else {
        panic!("audio")
    };
    assert_eq!(samples.as_ptr() as usize, pointers[0]);
    node.process_control_message(shutdown_message(), &context())
        .await
        .unwrap();
    drop(node);
    assert!(pool.allocate::<u8>(1).is_err());
    drop(second);
    let available = pool.allocate::<u8>(1).unwrap();
    assert!(pool.allocate::<u8>(1).is_err());
    drop(first);
    assert!(pool.allocate::<u8>(1).is_ok());
    drop(available);
}
