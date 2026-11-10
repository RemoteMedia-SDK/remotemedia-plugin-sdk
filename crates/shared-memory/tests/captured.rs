#![cfg(all(
    feature = "test-peer",
    any(windows, all(target_os = "linux", feature = "test-linux-namespace"))
))]
use remotemedia_shared_memory::channel::{ProcessResources, WorkerProcess};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

fn launch(root: &Path, linger: bool, owner: Arc<()>) -> WorkerProcess {
    let worker = env!("CARGO_BIN_EXE_shared-memory-test-peer");
    #[cfg(windows)]
    let mut launch = std::process::Command::new(worker);
    #[cfg(all(target_os = "linux", feature = "test-linux-namespace"))]
    let mut launch = remotemedia_shared_memory::channel::NamespaceCommand::new(
        Path::new(env!("CARGO_BIN_EXE_remotemedia-worker-supervisor")),
        Path::new("/usr/bin/unshare"),
        worker,
    )
    .unwrap();
    #[cfg(windows)]
    let command = &mut launch;
    #[cfg(all(target_os = "linux", feature = "test-linux-namespace"))]
    let command = launch.command_mut();
    command
        .current_dir(root)
        .env("RM_CAPTURE_TAG", "unchanged environment")
        .args([
            "captured-output",
            "ready with spaces.txt",
            if linger { "linger" } else { "complete" },
        ]);
    #[cfg(windows)]
    return WorkerProcess::spawn_captured(&mut launch, ProcessResources::new(owner)).unwrap();
    #[cfg(all(target_os = "linux", feature = "test-linux-namespace"))]
    return WorkerProcess::spawn_namespaced_captured(&mut launch, ProcessResources::new(owner))
        .unwrap();
}

#[test]
fn captured_output_drains_both_pipes_and_preserves_configuration() {
    let scratch = tempfile::tempdir().unwrap();
    let owner = Arc::new(());
    let process = launch(scratch.path(), false, owner.clone());
    let output = futures::executor::block_on(process.capture_output().unwrap()).unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, vec![17; 256 * 1024]);
    assert_eq!(output.stderr, vec![29; 256 * 1024]);
    assert_eq!(
        std::fs::read_to_string(scratch.path().join("ready with spaces.txt")).unwrap(),
        "unchanged environment"
    );
    assert_eq!(Arc::strong_count(&owner), 1);
}

#[test]
fn dropping_unpolled_capture_joins_process_shutdown_before_releasing_resources() {
    let scratch = tempfile::tempdir().unwrap();
    let owner = Arc::new(());
    let process = launch(scratch.path(), true, owner.clone());
    let capture = process.capture_output().unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while !scratch.path().join("ready with spaces.txt").exists() {
        assert!(
            Instant::now() < deadline,
            "capture did not drain startup output"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(Arc::strong_count(&owner), 2);
    drop(capture);
    assert_eq!(Arc::strong_count(&owner), 1);
}

#[test]
fn missing_captured_executable_releases_launch_resources() {
    let scratch = tempfile::tempdir().unwrap();
    let missing = scratch.path().join("missing-executable");
    let owner = Arc::new(());
    #[cfg(windows)]
    let result = WorkerProcess::spawn_captured(
        &mut std::process::Command::new(&missing),
        ProcessResources::new(owner.clone()),
    );
    #[cfg(all(target_os = "linux", feature = "test-linux-namespace"))]
    let result = {
        let mut command = remotemedia_shared_memory::channel::NamespaceCommand::new(
            Path::new(env!("CARGO_BIN_EXE_remotemedia-worker-supervisor")),
            Path::new("/usr/bin/unshare"),
            &missing,
        )
        .unwrap();
        WorkerProcess::spawn_namespaced_captured(&mut command, ProcessResources::new(owner.clone()))
    };
    assert!(result.is_err());
    assert_eq!(Arc::strong_count(&owner), 1);
}
