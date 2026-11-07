//! Real CPU model -> production mapped Python runner -> host owning buffers.
use super::*;
use remotemedia_shared_memory::peer::{
    CallKind, CallResources, HostHandle, PeerConnection, PeerEvent,
};
use remotemedia_types::{AudioSamples, BoundOutputAllocator, CopyContext};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

fn call(
    peer: &mut PeerConnection,
    handle: &HostHandle,
    kind: CallKind,
    input: Option<RuntimeData>,
    allocator: Arc<BoundOutputAllocator>,
    copies: Option<CopyContext>,
) -> (Vec<RuntimeData>, Result<(), String>) {
    let id = handle
        .start_call_with_resources(
            kind,
            remotemedia_shared_memory::peer::CallContext {
                session_id: Some("native-session".into()),
                node_id: "native-node".into(),
            },
            serde_json::json!({}),
            input
                .into_iter()
                .map(|data| ("input".into(), data))
                .collect(),
            CallResources {
                output_allocator: Some(allocator),
                output_copies: copies,
                ..Default::default()
            },
        )
        .unwrap();
    let mut outputs = Vec::new();
    loop {
        match peer.next_event().unwrap() {
            PeerEvent::Output(output) => {
                assert_eq!(output.call(), id);
                outputs.push(output.into_data());
            }
            PeerEvent::Complete { call, result, .. } => {
                assert_eq!(call, id);
                return (outputs, result);
            }
            _ => panic!("unexpected handshake during call"),
        }
    }
}

fn audio(data: &RuntimeData) -> &AudioSamples {
    match data {
        RuntimeData::Audio {
            samples,
            sample_rate: 24000,
            channels: 1,
            ..
        } => samples,
        _ => panic!("24 kHz mono audio required"),
    }
}

fn checksum(samples: &[f32]) -> String {
    let mut digest = Sha256::new();
    for sample in samples {
        digest.update(sample.to_le_bytes());
    }
    hex::encode(digest.finalize())
}

#[test]
#[ignore = "requires real Kokoro CPU dependencies, exact model snapshot and matching native extension"]
fn real_kokoro_mapped_copy_policy_forwarding_and_shutdown() {
    let root = PathBuf::from(std::env::var("MAPPED_SDK_PYTHON").unwrap());
    let evidence = PathBuf::from(std::env::var("KOKORO_PROBE_OUTPUT").unwrap());
    std::fs::create_dir_all(&evidence).unwrap();
    let trace = evidence.join("model-outputs.jsonl");
    assert!(
        !trace.exists(),
        "use a fresh evidence directory for each run"
    );
    let params = serde_json::json!({
        "snapshot": std::env::var("KOKORO_MODEL_SNAPSHOT").unwrap(),
        "trace": trace, "voice": "af_heart", "speed": 1.0, "sample_rate": 24000,
    });
    let mut command = Command::new(std::env::var("MAPPED_WORKER_PYTHON").unwrap());
    command.env("PYTHONPATH", &root).env("HF_HUB_OFFLINE", "1").args([
        "-c", "import importlib.util as u, importlib.machinery as m, os, runpy, sys; p=os.environ['NATIVE_BUFFER_EXTENSION']; s=u.spec_from_file_location('_remotemedia_buffers',p,loader=m.ExtensionFileLoader('_remotemedia_buffers',p)); module=u.module_from_spec(s); sys.modules[s.name]=module; s.loader.exec_module(module); runpy.run_module('remotemedia_mapped_worker',run_name='__main__')",
        "--node-type", "MappedRealKokoro", "--node-id", "native-node",
        "--session-id", "native-session", "--params", &params.to_string(),
        "--module-root", root.join("tests/support").to_str().unwrap(),
        "--register-module", "mapped_real_kokoro",
    ]);
    let pool = SharedPool::new(
        SessionId([126; 16]),
        PoolConfig {
            classes: vec![SizeClass {
                bytes: 1024 * 1024,
                slots: 2,
            }],
            max_bytes: 2 * 1024 * 1024,
            max_leases: 32,
        },
    )
    .unwrap();
    let allocator = Arc::new(BoundOutputAllocator::new(Arc::new(pool.clone())));
    // SAFETY: exact native extension and checked-in runner/fixture retain owning
    // buffers, join model work and do not delegate mappings or launch children.
    let (mut peer, handle) = unsafe {
        PeerConnection::spawn_with_output_copies(&mut command, pool.clone(), Limits::default(), 0)
    }
    .unwrap();
    let (done, wait) = std::sync::mpsc::channel();
    let timeout = handle.clone();
    let watchdog = std::thread::spawn(move || {
        if wait.recv_timeout(Duration::from_secs(180)).is_err() {
            timeout.close();
        }
    });
    assert!(matches!(
        peer.next_event().unwrap(),
        PeerEvent::Ready { .. }
    ));
    let (_, initialized) = call(
        &mut peer,
        &handle,
        CallKind::Initialize,
        None,
        allocator.clone(),
        None,
    );
    initialized.unwrap();
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "real-kokoro/output");
    let allow = CopyContext::new(CopyPolicy::AllowCopies, "real-kokoro/output");
    let text = || Some(RuntimeData::Text("Hello, shared buffers.".into()));
    let (rejected, error) = call(
        &mut peer,
        &handle,
        CallKind::Stream,
        text(),
        allocator.clone(),
        Some(strict.clone()),
    );
    assert!(error.unwrap_err().contains("copy policy denied"));
    assert!(
        rejected.is_empty(),
        "denial must not emit audio or speech end"
    );
    let (outputs, result) = call(
        &mut peer,
        &handle,
        CallKind::Stream,
        text(),
        allocator.clone(),
        Some(allow.clone()),
    );
    result.unwrap();
    assert_eq!(outputs.len(), 3);
    let rows: Vec<serde_json::Value> = std::fs::read_to_string(&trace)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), 3); // One rejected model chunk, two accepted chunks.
    let mut samples = 0_u64;
    for (index, data) in outputs[..2].iter().enumerate() {
        assert!(pool.owns_payload(data));
        let values = audio(data);
        assert!(values.iter().all(|v| v.is_finite()));
        assert!(values.iter().any(|v| *v != 0.0));
        assert_eq!(
            values.len() as u64,
            rows[index + 1]["samples"].as_u64().unwrap()
        );
        assert_eq!(
            checksum(values),
            rows[index + 1]["sha256"].as_str().unwrap()
        );
        let RuntimeData::Audio {
            metadata: Some(metadata),
            ..
        } = data
        else {
            panic!("metadata")
        };
        assert_eq!(metadata["pts_us"], samples * 1_000_000 / 24000);
        samples += values.len() as u64;
    }
    let RuntimeData::Text(end) = &outputs[2] else {
        panic!("speech end")
    };
    let end: serde_json::Value = serde_json::from_str(end).unwrap();
    assert_eq!(end["kind"], "speech_end");
    assert_eq!(end["pts_us"], samples * 1_000_000 / 24000);
    let count = &allow
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::BorrowedInput)
        .unwrap()
        .1;
    assert_eq!(
        (count.completed, count.copied_bytes, count.failed),
        (2, samples * 4, 0)
    );
    let denied = &strict
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::BorrowedInput)
        .unwrap()
        .1;
    assert_eq!(
        (denied.rejected, denied.rejected_bytes, denied.copied_bytes),
        (1, rows[0]["samples"].as_u64().unwrap() * 4, 0)
    );
    assert!(
        pool.allocate::<u8>(1).is_err(),
        "retained audio must pin both slots"
    );
    let pointer = audio(&outputs[0]).as_ptr();
    let (forwarded, result) = call(
        &mut peer,
        &handle,
        CallKind::Process,
        Some(outputs[0].clone()),
        allocator.clone(),
        Some(strict.clone()),
    );
    result.unwrap();
    assert_eq!(forwarded.len(), 1);
    assert_eq!(audio(&forwarded[0]).as_ptr(), pointer);
    let (_, shutdown) = call(
        &mut peer,
        &handle,
        CallKind::Control,
        Some(RuntimeData::Json(
            serde_json::json!({"__aux_port__":"shutdown"}),
        )),
        allocator,
        None,
    );
    shutdown.unwrap();
    assert!(trace.with_extension("closed").exists());
    handle.close();
    drop(peer);
    done.send(()).unwrap();
    watchdog.join().unwrap();
    assert!(pool.allocate::<u8>(1).is_err());
    assert_eq!(
        checksum(audio(&outputs[0])),
        rows[1]["sha256"].as_str().unwrap()
    );
    let AudioSamples::Shared(owner) = audio(&outputs[0]) else {
        panic!("shared owner")
    };
    let retained = owner.slice(7..owner.len() - 7).unwrap();
    assert_eq!(retained.as_ptr(), pointer.wrapping_add(7));
    let retained_hash = checksum(retained.as_slice());
    drop(forwarded);
    drop(outputs);
    let free = pool.allocate::<u8>(1).unwrap();
    assert!(pool.allocate::<u8>(1).is_err());
    assert_eq!(checksum(retained.as_slice()), retained_hash);
    drop(retained);
    assert!(pool.allocate::<u8>(1).is_ok());
    drop(free);
    let report = serde_json::json!({"samples":samples, "strict":strict.report(),
        "compatible":allow.report(), "retained_slice_sha256":retained_hash,
        "joined_cleanup":true, "all_slots_reclaimed":true});
    std::fs::write(
        evidence.join("host-result.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
}
