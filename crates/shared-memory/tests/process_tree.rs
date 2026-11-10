#![cfg(all(windows, feature = "test-peer"))]
use remotemedia_shared_memory::channel::WorkerProcess;
use std::{
    io::{BufRead, Write},
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    process::{Command, Stdio},
};
use windows_sys::Win32::{
    Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT},
    System::Threading::{OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE},
};

fn handles(ids: &[u32]) -> Vec<OwnedHandle> {
    ids.iter()
        .map(|id| {
            // SAFETY: known live fixture ids; retain handles before shutdown so no
            // later PID reuse can masquerade as this process's death.
            let raw = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, *id) };
            assert!(!raw.is_null(), "{}", std::io::Error::last_os_error());
            let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
            assert_eq!(
                unsafe { WaitForSingleObject(handle.as_raw_handle(), 0) },
                WAIT_TIMEOUT
            );
            handle
        })
        .collect()
}

#[test]
fn captured_commands_own_immediate_descendants_before_any_host_message() {
    use remotemedia_shared_memory::channel::ProcessResources;
    use std::sync::Arc;
    for mode in ["terminate", "drop", "already-exited"] {
        let owner = Arc::new(());
        let mut command = Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
        command.args(["tree-leaf", "2"]);
        if mode == "already-exited" {
            command.arg("exit-parent");
        }
        let mut process =
            WorkerProcess::spawn_captured(&mut command, ProcessResources::new(owner.clone()))
                .unwrap();
        assert!(process.take_channel().is_none());
        let mut line = String::new();
        std::io::BufReader::new(process.take_stdout().unwrap())
            .read_line(&mut line)
            .unwrap();
        let ids: Vec<u32> = serde_json::from_str(&line).unwrap();
        assert_eq!(ids[0], process.id());
        assert_eq!(ids.len(), 3);
        let descendants = handles(&ids[1..]);
        assert_eq!(Arc::strong_count(&owner), 2);
        if mode == "already-exited" {
            assert!(process.wait().unwrap().success());
        }
        if mode != "drop" {
            process.terminate().unwrap();
        }
        drop(process);
        for descendant in descendants {
            assert_eq!(
                unsafe { WaitForSingleObject(descendant.as_raw_handle(), 0) },
                WAIT_OBJECT_0,
                "{mode}"
            );
        }
        assert_eq!(Arc::strong_count(&owner), 1);
    }
}

#[test]
fn dropping_output_future_joins_child_and_grandchild_shutdown() {
    captured_tree_lifetime(false);
}

#[test]
fn successful_output_future_joins_descendants_after_main_process_exits() {
    captured_tree_lifetime(true);
}

fn captured_tree_lifetime(normal_exit: bool) {
    use remotemedia_shared_memory::channel::ProcessResources;
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };
    let scratch = tempfile::tempdir().unwrap();
    let ready = scratch.path().join("tree.json");
    let exit_gate = scratch.path().join("exit");
    let owner = Arc::new(());
    let mut command = Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
    command
        .args(["tree-leaf", "2"])
        .env("RM_CAPTURE_TREE_READY", &ready);
    if normal_exit {
        command
            .arg("exit-parent")
            .env("RM_CAPTURE_TREE_EXIT_GATE", &exit_gate);
    }
    let capture = WorkerProcess::spawn_captured(&mut command, ProcessResources::new(owner.clone()))
        .unwrap()
        .capture_output()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    let ids: Vec<u32> = loop {
        if let Ok(bytes) = std::fs::read(&ready) {
            if let Ok(ids) = serde_json::from_slice(&bytes) {
                break ids;
            }
        }
        assert!(Instant::now() < deadline, "tree startup timed out");
        std::thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(ids.len(), 3);
    let original_handles = handles(&ids);
    if normal_exit {
        std::fs::write(exit_gate, b"exit").unwrap();
        assert!(futures::executor::block_on(capture)
            .unwrap()
            .status
            .success());
    } else {
        drop(capture);
    }
    for process in original_handles {
        assert_eq!(
            unsafe { WaitForSingleObject(process.as_raw_handle(), 0) },
            WAIT_OBJECT_0
        );
    }
    assert_eq!(Arc::strong_count(&owner), 1);
}

#[test]
fn abrupt_broker_exit_closes_the_only_job_handle_and_kills_the_entire_tree() {
    let mut broker = Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"))
        .arg("tree-broker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    std::io::BufReader::new(broker.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let ids: Vec<u32> = serde_json::from_str(&line).unwrap();
    assert_eq!(ids.len(), 3);
    let processes = handles(&ids);
    broker.stdin.take().unwrap().write_all(b"exit\n").unwrap();
    assert_eq!(broker.wait().unwrap().code(), Some(37));
    for process in processes {
        assert_eq!(
            unsafe { WaitForSingleObject(process.as_raw_handle(), 5000) },
            WAIT_OBJECT_0
        );
    }
}

#[test]
fn terminate_and_drop_reap_worker_children_and_grandchildren_even_after_parent_exit() {
    for mode in ["terminate", "drop", "already-exited"] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
        command.arg("process-tree");
        let mut process = WorkerProcess::spawn(&mut command).unwrap();
        let (mut sender, mut receiver) = process.take_channel().unwrap().split().unwrap();
        sender.send(b"spawn", None).unwrap();
        let ids: Vec<u32> = serde_json::from_slice(&receiver.receive().unwrap().metadata).unwrap();
        assert_eq!(ids.len(), 3);
        let processes = handles(&ids);
        if mode == "already-exited" {
            sender.send(b"exit", None).unwrap();
            assert_eq!(process.wait().unwrap().code(), Some(23));
            for descendant in &processes[1..] {
                assert_eq!(
                    unsafe { WaitForSingleObject(descendant.as_raw_handle(), 0) },
                    WAIT_TIMEOUT
                );
            }
        }
        if mode != "drop" {
            process.terminate().unwrap();
        }
        drop(process);
        for process in processes {
            assert_eq!(
                unsafe { WaitForSingleObject(process.as_raw_handle(), 0) },
                WAIT_OBJECT_0,
                "{mode}: tracked descendant survived teardown"
            );
        }
    }
}
