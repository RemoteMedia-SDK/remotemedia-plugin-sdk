#![cfg(all(target_os = "linux", target_arch = "x86_64", feature = "test-runsc"))]

use std::fs;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use remotemedia_workload_sandbox::runsc::{
    RunscConfig, RunscCudaConfig, RunscProvider, RunscSandbox,
};
use remotemedia_workload_sandbox::{
    SandboxError, WorkloadExit, WorkloadLimits, WorkloadPhase, WorkloadRequest, WorkloadTarget,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PORT: u16 = 25255;
const SERVE: &str = r#"
import socket, sys
with socket.socket(socket.AF_INET) as listener:
    listener.bind(('127.0.0.1', int(sys.argv[1])))
    listener.listen()
    while True:
        stream, _ = listener.accept()
        with stream:
            if stream.recv(1) == b'?':
                stream.sendall(sys.argv[2].encode('ascii'))
"#;

#[tokio::test]
#[ignore = "requires two physical NVIDIA GPUs, the pinned runsc, CUDA guest image and a dedicated cgroup"]
async fn two_physical_cuda_devices_stay_exclusive_and_serve_independently() {
    let executable = PathBuf::from(setting("RM_SPACE_TEST_RUNSC"));
    let executable_sha256 = hex::encode(Sha256::digest(fs::read(&executable).unwrap()));
    let cgroup = PathBuf::from(setting("RM_SPACE_TEST_CGROUP"));
    let rootfs = PathBuf::from(setting("RM_SPACE_TEST_CUDA_ROOTFS"));
    let root = tempfile::tempdir().unwrap();
    let registry = root.path().join("devices");
    fs::DirBuilder::new().mode(0o700).create(&registry).unwrap();
    let configure = |name: &str| {
        let delegated_cgroup = group(&cgroup, name);
        RunscConfig {
            executable: executable.clone(),
            executable_sha256: executable_sha256.clone(),
            rootfs: rootfs.clone(),
            state: root.path().join(name),
            delegated_cgroup,
            relay_program: "/opt/remotemedia/relay".into(),
        }
    };
    let first = configure("first-device");
    let second = configure("second-device");
    let cuda = |environment| RunscCudaConfig {
        registry: registry.clone(),
        device_uuid: setting(environment),
        driver_version: setting("RM_SPACE_TEST_CUDA_DRIVER"),
    };
    let first_cuda = cuda("RM_SPACE_TEST_CUDA_DEVICE");
    let second_cuda = cuda("RM_SPACE_TEST_CUDA_OTHER_DEVICE");
    assert_ne!(first_cuda.device_uuid, second_cuda.device_uuid);
    let a = RunscProvider::open_cuda(first.clone(), &first_cuda)
        .await
        .unwrap();
    assert!(matches!(
        RunscProvider::open_cuda(second.clone(), &first_cuda).await,
        Err(SandboxError::DeviceBusy)
    ));
    let b = RunscProvider::open_cuda(second.clone(), &second_cuda)
        .await
        .unwrap();
    let (a_run, b_run) = tokio::try_join!(
        a.reserve().unwrap().start(request(&first_cuda.device_uuid)),
        b.reserve()
            .unwrap()
            .start(request(&second_cuda.device_uuid)),
    )
    .unwrap();
    let a_id = a_run.id().to_owned();
    let b_id = b_run.id().to_owned();
    assert!(matches!(a.reserve(), Err(SandboxError::DeviceBusy)));
    assert!(matches!(b.reserve(), Err(SandboxError::DeviceBusy)));
    verify_reply(&a_run, &first_cuda.device_uuid).await;
    verify_reply(&b_run, &second_cuda.device_uuid).await;
    assert_eq!(a_run.stop().await.unwrap(), WorkloadExit::Stopped);
    verify_reply(&b_run, &second_cuda.device_uuid).await;
    assert_eq!(b_run.stop().await.unwrap(), WorkloadExit::Stopped);
    for (config, id) in [(&first, a_id), (&second, b_id)] {
        assert!(!config.state.join("bundles").join(&id).exists());
        assert!(!config.delegated_cgroup.join(id).exists());
    }
    a.release_exclusive_device().await.unwrap();
    b.release_exclusive_device().await.unwrap();
    for config in [first, second] {
        fs::remove_dir(config.delegated_cgroup).unwrap();
    }
}

fn setting(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name} for the physical CUDA fixture"))
}

fn group(parent: &Path, name: &str) -> PathBuf {
    let path = parent.join(name);
    fs::create_dir(&path).unwrap();
    fs::write(path.join("cgroup.subtree_control"), "+cpu +memory +pids").unwrap();
    path
}

fn request(uuid: &str) -> WorkloadRequest {
    WorkloadRequest {
        target: WorkloadTarget::LinuxCuda,
        phase: WorkloadPhase::Serve {
            loopback_port: PORT,
        },
        limits: WorkloadLimits {
            cpu_millis: 1000,
            memory_bytes: 2 * 1024 * 1024 * 1024,
            scratch_bytes: 64 * 1024 * 1024,
            processes: 128,
            lifetime: Duration::from_secs(90),
        },
        argv: vec![
            "/usr/bin/python3".into(),
            "-I".into(),
            "-c".into(),
            SERVE.into(),
            PORT.to_string(),
            uuid.into(),
        ],
    }
}

async fn verify_reply(run: &RunscSandbox, uuid: &str) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(mut stream) = run.open_stream() {
                let mut reply = vec![0; uuid.len()];
                if stream.input.write_all(b"?").await.is_ok()
                    && stream.output.read_exact(&mut reply).await.is_ok()
                {
                    assert_eq!(reply, uuid.as_bytes());
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("CUDA verifier and guest service must finish startup");
}
