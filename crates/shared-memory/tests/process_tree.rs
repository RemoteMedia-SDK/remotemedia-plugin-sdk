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
                unsafe { WaitForSingleObject(process.as_raw_handle(), 5000) },
                WAIT_OBJECT_0,
                "{mode}: tracked descendant survived teardown"
            );
        }
    }
}
