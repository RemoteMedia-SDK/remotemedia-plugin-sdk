#![cfg(all(target_os = "linux", feature = "test-runsc"))]

use std::fs;
use std::io::Read;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use remotemedia_workload_sandbox::runsc::{
    BuildOutput, RunscConfig, RunscProvider, RunscReservation,
};
use remotemedia_workload_sandbox::{
    SandboxError, WorkloadExit, WorkloadLimits, WorkloadPhase, WorkloadRequest, WorkloadTarget,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn fixture() -> (tempfile::TempDir, RunscConfig) {
    let root = tempfile::tempdir().unwrap();
    let rootfs = root.path().join("rootfs");
    fs::create_dir(&rootfs).unwrap();
    fs::set_permissions(&rootfs, fs::Permissions::from_mode(0o755)).unwrap();
    let program = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cpu.c");
    assert!(Command::new("cc")
        .arg("-static")
        .arg(program)
        .arg("-o")
        .arg(rootfs.join("probe"))
        .status()
        .unwrap()
        .success());
    fs::set_permissions(rootfs.join("probe"), fs::Permissions::from_mode(0o755)).unwrap();
    if let Some(relay) = std::env::var_os("RM_SPACE_TEST_RELAY") {
        let destination = rootfs.join("relay");
        fs::copy(relay, &destination).unwrap();
        fs::set_permissions(destination, fs::Permissions::from_mode(0o755)).unwrap();
    }
    let config = fixture_config(root.path());
    (root, config)
}

fn fixture_config(root: &Path) -> RunscConfig {
    let rootfs = root.join("rootfs");
    let relay = if rootfs.join("relay").is_file() {
        "/relay"
    } else {
        "/probe"
    };
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
        rootfs,
        state: root.join("state"),
        delegated_cgroup,
        relay_program: relay.into(),
    }
}

const SOURCE_BYTES: &[u8] = b"trusted test bytes";
const FIRST_DEVICE: &str = "GPU-11111111-1111-1111-1111-111111111111";
const SECOND_DEVICE: &str = "GPU-22222222-2222-2222-2222-222222222222";
const CRASH_ROOT: &str = "RM_SPACE_TEST_CRASH_ROOT";
const CRASH_DEVICE: &str = "RM_SPACE_TEST_CRASH_DEVICE";

fn staged(provider: &RunscProvider) -> RunscReservation {
    let reservation = provider.reserve().unwrap();
    let source = reservation.input_root().join("source");
    fs::create_dir(&source).unwrap();
    fs::set_permissions(&source, fs::Permissions::from_mode(0o755)).unwrap();
    fs::write(source.join("evidence"), SOURCE_BYTES).unwrap();
    fs::set_permissions(source.join("evidence"), fs::Permissions::from_mode(0o444)).unwrap();
    reservation
}

fn request(phase: WorkloadPhase, mode: &str, lifetime: Duration) -> WorkloadRequest {
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
        argv: vec!["/probe".into(), mode.into()],
    }
}

#[tokio::test]
async fn real_cpu_provider_checks_isolation_and_owns_relay_and_cleanup() {
    if let Some(root) = std::env::var_os(CRASH_ROOT).map(PathBuf::from) {
        let config = fixture_config(&root);
        let provider = if std::env::var_os(CRASH_DEVICE).is_some() {
            RunscProvider::open_for_exclusive_device(config, &root.join("devices"), FIRST_DEVICE)
                .await
                .unwrap()
        } else {
            RunscProvider::open(config).await.unwrap()
        };
        let running = staged(&provider)
            .start(request(
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
    let (_root, config) = fixture();
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
    let abandoned = staged(&provider);
    let abandoned_inputs = abandoned.input_root();
    drop(abandoned);
    assert!(!abandoned_inputs.exists());

    let install = staged(&provider)
        .start(request(
            WorkloadPhase::Install,
            "check",
            Duration::from_secs(15),
        ))
        .await
        .unwrap();
    assert!(install.open_stream().is_err());
    assert_eq!(install.wait().await.unwrap(), WorkloadExit::Exited(0));

    let payload = vec![b'w'; 12 * 8192];
    let build = |mode| request(WorkloadPhase::Build, mode, Duration::from_secs(10));
    let identity = |bytes: &[u8]| BuildOutput {
        size: bytes.len() as u64,
        sha256: Sha256::digest(bytes).into(),
    };
    let mut output = staged(&provider)
        .build(build("build"), identity(&payload))
        .await
        .unwrap();
    let mut observed = Vec::new();
    output.read_to_end(&mut observed).unwrap();
    assert_eq!(observed, payload);
    for (mode, expected) in [
        ("build", identity(&payload[..1024])),
        ("build", identity(&vec![b'X'; payload.len()])),
        ("build-fail", identity(&payload)),
    ] {
        assert!(staged(&provider)
            .build(build(mode), expected)
            .await
            .is_err());
        assert_eq!(
            fs::read_dir(config.state.join("bundles")).unwrap().count(),
            0
        );
    }
    assert!(staged(&provider)
        .build(
            request(WorkloadPhase::Build, "block", Duration::from_secs(1)),
            identity(&payload),
        )
        .await
        .is_err());
    assert_eq!(
        fs::read_dir(config.state.join("bundles")).unwrap().count(),
        0
    );

    let serve = staged(&provider)
        .start(request(
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

    let expired = staged(&provider)
        .start(request(
            WorkloadPhase::Build,
            "block",
            Duration::from_secs(1),
        ))
        .await
        .unwrap();
    assert_eq!(expired.wait().await.unwrap(), WorkloadExit::Deadline);
    let dropped = staged(&provider)
        .start(request(
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
    assert_eq!(
        fs::read(bundle.join("inputs/source/evidence")).unwrap(),
        SOURCE_BYTES
    );
    assert!(cgroup.is_dir());
    let reopened = RunscProvider::open(config.clone()).await.unwrap();
    assert!(!bundle.exists());
    assert!(!cgroup.exists());
    drop(reopened);
    verify_exclusive_device_lifecycle(_root.path(), config).await;
}

async fn verify_exclusive_device_lifecycle(root: &Path, mut primary: RunscConfig) {
    let registry = root.join("devices");
    fs::DirBuilder::new().mode(0o700).create(&registry).unwrap();
    let mut alternate = primary.clone();
    alternate.state = root.join("alternate-state");
    alternate.delegated_cgroup = primary.delegated_cgroup.join("alternate-device");
    primary.delegated_cgroup = primary.delegated_cgroup.join("primary-device");
    for group in [&primary.delegated_cgroup, &alternate.delegated_cgroup] {
        fs::create_dir(group).unwrap();
        fs::write(group.join("cgroup.subtree_control"), "+cpu +memory +pids").unwrap();
    }

    let provider =
        RunscProvider::open_for_exclusive_device(primary.clone(), &registry, FIRST_DEVICE)
            .await
            .unwrap();
    let reserved = staged(&provider);
    assert!(matches!(provider.reserve(), Err(SandboxError::DeviceBusy)));
    assert!(matches!(
        provider.clone().release_exclusive_device().await,
        Err(SandboxError::DeviceBusy)
    ));
    assert!(matches!(
        RunscProvider::open_for_exclusive_device(alternate.clone(), &registry, FIRST_DEVICE).await,
        Err(SandboxError::DeviceBusy)
    ));
    let independent =
        RunscProvider::open_for_exclusive_device(alternate.clone(), &registry, SECOND_DEVICE)
            .await
            .unwrap();
    drop(independent);
    let original_state = root.join("bound-alternate-state");
    fs::rename(&alternate.state, &original_state).unwrap();
    assert!(matches!(
        RunscProvider::open_for_exclusive_device(alternate.clone(), &registry, SECOND_DEVICE).await,
        Err(SandboxError::DeviceRecoveryRequired)
    ));
    fs::rename(&alternate.state, root.join("replacement-alternate-state")).unwrap();
    fs::rename(original_state, &alternate.state).unwrap();
    RunscProvider::open_for_exclusive_device(alternate.clone(), &registry, SECOND_DEVICE)
        .await
        .unwrap()
        .release_exclusive_device()
        .await
        .unwrap();
    let running = reserved
        .start(request(
            WorkloadPhase::Serve {
                loopback_port: 24566,
            },
            "serve",
            Duration::from_secs(15),
        ))
        .await
        .unwrap();
    assert!(matches!(provider.reserve(), Err(SandboxError::DeviceBusy)));
    assert_eq!(running.stop().await.unwrap(), WorkloadExit::Stopped);
    assert!(provider.reserve().is_ok());
    drop(provider);

    let mut worker = tokio::process::Command::new(std::env::current_exe().unwrap());
    worker
        .args([
            "--exact",
            "real_cpu_provider_checks_isolation_and_owns_relay_and_cleanup",
        ])
        .env(CRASH_ROOT, root)
        .env(CRASH_DEVICE, FIRST_DEVICE)
        .env("RM_SPACE_TEST_CGROUP", &primary.delegated_cgroup)
        .kill_on_drop(true);
    assert!(
        tokio::time::timeout(Duration::from_secs(20), worker.status())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let interrupted = fs::read_to_string(root.join("interrupted-run")).unwrap();
    let bundle = primary.state.join("bundles").join(&interrupted);
    let cgroup = primary.delegated_cgroup.join(interrupted);
    assert!(bundle.is_dir() && cgroup.is_dir());
    assert!(matches!(
        RunscProvider::open_for_exclusive_device(alternate.clone(), &registry, FIRST_DEVICE).await,
        Err(SandboxError::DeviceRecoveryRequired)
    ));
    let recovered =
        RunscProvider::open_for_exclusive_device(primary.clone(), &registry, FIRST_DEVICE)
            .await
            .unwrap();
    assert!(!bundle.exists() && !cgroup.exists());
    recovered.release_exclusive_device().await.unwrap();
    let transferred =
        RunscProvider::open_for_exclusive_device(alternate.clone(), &registry, FIRST_DEVICE)
            .await
            .unwrap();
    transferred.release_exclusive_device().await.unwrap();
    for group in [&primary.delegated_cgroup, &alternate.delegated_cgroup] {
        fs::remove_dir(group).unwrap();
    }
}
