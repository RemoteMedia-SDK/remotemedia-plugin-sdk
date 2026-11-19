#![cfg(all(target_os = "linux", feature = "test-runsc"))]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use remotemedia_workload_sandbox::runsc::{RunscConfig, RunscProvider};
use remotemedia_workload_sandbox::{
    SandboxError, WorkloadExit, WorkloadInputs, WorkloadLimits, WorkloadPhase, WorkloadRequest,
    WorkloadTarget,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn fixture() -> (tempfile::TempDir, RunscConfig, PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let rootfs = root.path().join("rootfs");
    let source = root.path().join("inputs");
    fs::create_dir(&rootfs).unwrap();
    fs::create_dir(&source).unwrap();
    fs::set_permissions(&source, fs::Permissions::from_mode(0o755)).unwrap();
    fs::write(source.join("evidence"), b"trusted test bytes").unwrap();
    let program = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cpu.c");
    assert!(Command::new("cc")
        .arg("-static")
        .arg(program)
        .arg("-o")
        .arg(rootfs.join("probe"))
        .status()
        .unwrap()
        .success());
    let config = fixture_config(root.path());
    (root, config, source)
}

fn fixture_config(root: &Path) -> RunscConfig {
    let executable =
        PathBuf::from(std::env::var_os("RM_SPACE_TEST_RUNSC").expect("set the pinned runsc path"));
    let delegated_cgroup = PathBuf::from(
        std::env::var_os("RM_SPACE_TEST_CGROUP")
            .expect("set a dedicated writable cgroup v2 subtree"),
    );
    let executable_sha256 = format!("{:x}", Sha256::digest(fs::read(&executable).unwrap()));
    RunscConfig {
        executable,
        executable_sha256,
        rootfs: root.join("rootfs"),
        state: root.join("state"),
        delegated_cgroup,
        relay_program: "/probe".into(),
    }
}

fn request(source: &Path, phase: WorkloadPhase, mode: &str, lifetime: Duration) -> WorkloadRequest {
    WorkloadRequest {
        target: WorkloadTarget::LinuxCpu,
        phase,
        limits: WorkloadLimits {
            cpu_millis: 500,
            memory_bytes: 128 * 1024 * 1024,
            scratch_bytes: 16 * 1024 * 1024,
            processes: 64,
            lifetime,
        },
        inputs: WorkloadInputs {
            source: Some(source.to_owned()),
            ..Default::default()
        },
        argv: vec!["/probe".into(), mode.into()],
    }
}

#[tokio::test]
async fn real_cpu_provider_checks_isolation_and_owns_relay_and_cleanup() {
    const CRASH_ROOT: &str = "RM_SPACE_TEST_CRASH_ROOT";
    if let Some(root) = std::env::var_os(CRASH_ROOT).map(PathBuf::from) {
        let provider = RunscProvider::open(fixture_config(&root)).await.unwrap();
        let running = provider
            .start(request(
                &root.join("inputs"),
                WorkloadPhase::Serve {
                    loopback_port: 24566,
                },
                "serve",
                Duration::from_secs(15),
            ))
            .await
            .unwrap();
        fs::write(root.join("interrupted-run"), running.id()).unwrap();
        // Exit without dropping the active run or executing the provider's actor.
        std::process::exit(0);
    }
    let (_root, config, source) = fixture();
    let provider = RunscProvider::open(config.clone()).await.unwrap();
    assert!(provider
        .probe(WorkloadTarget::LinuxCpu)
        .await
        .unwrap()
        .starts_with("runsc version "));
    assert!(matches!(
        provider.probe(WorkloadTarget::LinuxCuda).await,
        Err(SandboxError::UnsupportedTarget(_))
    ));
    assert!(RunscProvider::open(config.clone()).await.is_err());
    let mut conflicting = config.clone();
    conflicting.state = _root.path().join("conflicting-state");
    assert!(matches!(
        RunscProvider::open(conflicting).await,
        Err(SandboxError::Runtime("exclusive provider cgroup lock"))
    ));
    let install = provider
        .start(request(
            &source,
            WorkloadPhase::Install,
            "check",
            Duration::from_secs(15),
        ))
        .await
        .unwrap();
    assert!(install.open_stream().is_err());
    assert_eq!(install.wait().await.unwrap(), WorkloadExit::Exited(0));
    assert!(!source.join("write").exists());

    let serve = provider
        .start(request(
            &source,
            WorkloadPhase::Serve {
                loopback_port: 24566,
            },
            "serve",
            Duration::from_secs(15),
        ))
        .await
        .unwrap();
    let id = serve.id().to_owned();
    let cgroup = config.delegated_cgroup.join(&id);
    for (control, expected) in [
        ("cpu.max", "50000 100000"),
        ("memory.max", "134217728"),
        ("memory.swap.max", "0"),
        ("pids.max", "64"),
    ] {
        assert_eq!(
            fs::read_to_string(cgroup.join(control)).unwrap().trim(),
            expected
        );
    }
    assert!(std::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, 24566)).is_err());
    let mut stream = serve.open_stream().unwrap();
    stream.input.write_all(b"private HTTP bytes").await.unwrap();
    let mut received = [0; 18];
    tokio::time::timeout(
        Duration::from_secs(5),
        stream.output.read_exact(&mut received),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(&received, b"private HTTP bytes");
    let mut after = [0];
    assert!(
        tokio::time::timeout(Duration::from_millis(150), stream.output.read(&mut after))
            .await
            .is_err()
    );
    assert_eq!(serve.stop().await.unwrap(), WorkloadExit::Stopped);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), stream.output.read(&mut after))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert!(!config.delegated_cgroup.join(&id).exists());
    assert!(!config.state.join("bundles").join(&id).exists());

    let expired = provider
        .start(request(
            &source,
            WorkloadPhase::Build,
            "block",
            Duration::from_millis(400),
        ))
        .await
        .unwrap();
    assert_eq!(expired.wait().await.unwrap(), WorkloadExit::Deadline);
    let dropped = provider
        .start(request(
            &source,
            WorkloadPhase::Build,
            "block",
            Duration::from_secs(15),
        ))
        .await
        .unwrap();
    let id = dropped.id().to_owned();
    drop(dropped);
    tokio::time::timeout(Duration::from_secs(15), async {
        while config.state.join("bundles").join(&id).exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(!config.delegated_cgroup.join(id).exists());
    drop(provider);

    let mut worker = tokio::process::Command::new(std::env::current_exe().unwrap());
    worker
        .args([
            "--exact",
            "real_cpu_provider_checks_isolation_and_owns_relay_and_cleanup",
        ])
        .env(CRASH_ROOT, _root.path())
        .kill_on_drop(true);
    assert!(
        tokio::time::timeout(Duration::from_secs(20), worker.status())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let interrupted = fs::read_to_string(_root.path().join("interrupted-run")).unwrap();
    let bundle = config.state.join("bundles").join(&interrupted);
    let cgroup = config.delegated_cgroup.join(interrupted);
    assert!(bundle.is_dir());
    assert!(cgroup.is_dir());
    let _reopened = RunscProvider::open(config).await.unwrap();
    assert!(!bundle.exists());
    assert!(!cgroup.exists());
}
