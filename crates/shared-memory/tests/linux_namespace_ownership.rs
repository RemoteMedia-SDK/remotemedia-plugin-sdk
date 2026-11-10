#![cfg(all(target_os = "linux", feature = "test-linux-namespace"))]
//! Probe the proposed guardian/namespace boundary using original process handles.
use std::{
    io::{BufRead, BufReader, Write},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    process::{Child, Command, Stdio},
};

struct Probe {
    child: Child,
    output: BufReader<std::process::ChildStdout>,
    processes: Vec<OwnedFd>,
    ids: Vec<u32>,
}
impl Probe {
    fn start() -> Self {
        Self::start_mode("guardian", 5)
    }

    fn start_mode(mode: &str, process_count: usize) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_shared-memory-linux-supervisor-probe"))
            .arg(mode)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        output.read_line(&mut line).unwrap();
        let ids: Vec<u32> = serde_json::from_str(&line).expect("namespace launch must succeed");
        assert_eq!(ids.len(), process_count);
        assert_eq!(ids[0], child.id());
        let processes = ids
            .iter()
            .copied()
            .map(|pid| {
                // Retain original handles while every process is alive. No later
                // numeric PID lookup can confuse PID reuse with successful cleanup.
                let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
                assert!(raw >= 0, "{}", std::io::Error::last_os_error());
                let fd = unsafe { OwnedFd::from_raw_fd(raw as i32) };
                assert!(!exited(&fd, 0));
                fd
            })
            .collect();
        Self {
            child,
            output,
            processes,
            ids,
        }
    }

    fn command(&mut self, command: &[u8]) {
        self.child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(command)
            .unwrap();
    }

    fn assert_all_exited(&self) {
        for process in &self.processes {
            assert!(
                exited(process, 5000),
                "owned process survived namespace teardown"
            );
        }
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn exited(process: &OwnedFd, timeout_ms: i32) -> bool {
    let mut poll = libc::pollfd {
        fd: process.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    let result = unsafe { libc::poll(&mut poll, 1, timeout_ms) };
    assert!(result >= 0, "{}", std::io::Error::last_os_error());
    assert_eq!(poll.revents & (libc::POLLNVAL | libc::POLLERR), 0);
    poll.revents & libc::POLLIN != 0
}

#[test]
fn captured_namespace_command_owns_detached_children_and_grandchildren() {
    use remotemedia_shared_memory::channel::{NamespaceCommand, ProcessResources, WorkerProcess};
    use std::{path::Path, sync::Arc};
    for explicit in [false, true] {
        let owner = Arc::new(());
        let mut command = NamespaceCommand::new(
            Path::new(env!("CARGO_BIN_EXE_remotemedia-worker-supervisor")),
            Path::new("/usr/bin/unshare"),
            env!("CARGO_BIN_EXE_shared-memory-linux-supervisor-probe"),
        )
        .unwrap();
        command.command_mut().args(["leaf", "2"]);
        let mut process = WorkerProcess::spawn_namespaced_captured(
            &mut command,
            ProcessResources::new(owner.clone()),
        )
        .unwrap();
        assert!(process.take_channel().is_none());
        let mut line = String::new();
        BufReader::new(process.take_stdout().unwrap())
            .read_line(&mut line)
            .unwrap();
        let ids: Vec<u32> = serde_json::from_str(&line).unwrap();
        assert_eq!(ids.len(), 3);
        let handles: Vec<_> = ids
            .into_iter()
            .map(|pid| {
                let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
                assert!(raw >= 0, "{}", std::io::Error::last_os_error());
                let handle = unsafe { OwnedFd::from_raw_fd(raw as i32) };
                assert!(!exited(&handle, 0));
                handle
            })
            .collect();
        assert_eq!(Arc::strong_count(&owner), 2);
        if explicit {
            process.terminate().unwrap();
        }
        drop(process);
        for handle in handles {
            assert!(exited(&handle, 5000));
        }
        assert_eq!(Arc::strong_count(&owner), 1);
    }
}

#[test]
fn captured_command_rejects_missing_namespace_selection() {
    use remotemedia_shared_memory::channel::{ProcessResources, WorkerProcess};
    let mut command = Command::new("/bin/true");
    let error = WorkerProcess::spawn_captured(&mut command, ProcessResources::default())
        .err()
        .unwrap();
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
}

#[test]
fn dropping_output_future_joins_detached_child_and_grandchild_shutdown() {
    captured_tree_lifetime(false);
}

#[test]
fn successful_output_future_joins_detached_descendants_after_main_exit() {
    captured_tree_lifetime(true);
}

fn captured_tree_lifetime(normal_exit: bool) {
    use remotemedia_shared_memory::channel::{NamespaceCommand, ProcessResources, WorkerProcess};
    use std::{
        path::Path,
        sync::Arc,
        time::{Duration, Instant},
    };
    let scratch = tempfile::tempdir().unwrap();
    let ready = scratch.path().join("tree.json");
    let exit_gate = scratch.path().join("exit");
    let owner = Arc::new(());
    let mut command = NamespaceCommand::new(
        Path::new(env!("CARGO_BIN_EXE_remotemedia-worker-supervisor")),
        Path::new("/usr/bin/unshare"),
        env!("CARGO_BIN_EXE_shared-memory-linux-supervisor-probe"),
    )
    .unwrap();
    command
        .command_mut()
        .args(["leaf", "2"])
        .env("RM_CAPTURE_TREE_READY", &ready);
    if normal_exit {
        command
            .command_mut()
            .arg("exit-parent")
            .env("RM_CAPTURE_TREE_EXIT_GATE", &exit_gate);
    }
    let capture = WorkerProcess::spawn_namespaced_captured(
        &mut command,
        ProcessResources::new(owner.clone()),
    )
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
    let original_handles: Vec<_> = ids
        .into_iter()
        .map(|pid| {
            let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
            assert!(raw >= 0, "{}", std::io::Error::last_os_error());
            let handle = unsafe { OwnedFd::from_raw_fd(raw as i32) };
            assert!(!exited(&handle, 0));
            handle
        })
        .collect();
    if normal_exit {
        std::fs::write(exit_gate, b"exit").unwrap();
        assert!(futures::executor::block_on(capture)
            .unwrap()
            .status
            .success());
    } else {
        drop(capture);
    }
    for handle in original_handles {
        assert!(exited(&handle, 5000));
    }
    assert_eq!(Arc::strong_count(&owner), 1);
}

#[test]
fn closing_broker_lifeline_terminates_detached_children_and_grandchildren() {
    let mut probe = Probe::start();
    drop(probe.child.stdin.take());
    assert!(probe.child.wait().unwrap().success());
    probe.assert_all_exited();
}

#[test]
fn abrupt_guardian_death_terminates_namespace_without_rust_drop() {
    let mut probe = Probe::start();
    // SIGKILL does not execute cleanup handlers in either guardian or launcher.
    probe.child.kill().unwrap();
    assert!(!probe.child.wait().unwrap().success());
    probe.assert_all_exited();
}

#[test]
fn namespace_init_exit_terminates_descendants_before_guardian_shutdown() {
    let mut probe = Probe::start();
    probe.command(b"exit-root\n");
    let mut line = String::new();
    probe.output.read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "root-exited");
    assert!(!exited(&probe.processes[0], 0));
    for process in &probe.processes[1..] {
        assert!(exited(process, 5000));
    }
    probe.command(b"close\n");
    assert!(probe.child.wait().unwrap().success());
    probe.assert_all_exited();
}

#[test]
fn abrupt_broker_exit_closes_lifeline_and_kills_the_namespace_tree() {
    let mut probe = Probe::start_mode("broker", 6);
    probe.command(b"exit\n");
    assert_eq!(probe.child.wait().unwrap().code(), Some(37));
    probe.assert_all_exited();
}

#[test]
fn broker_eof_cancels_namespace_startup_before_worker_ready() {
    let mut probe = Probe::start_mode("guardian-stall", 2);
    let launcher_pid = probe.ids[1];
    let children = format!("/proc/{launcher_pid}/task/{launcher_pid}/children");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let init_pid: u32 = loop {
        let children = std::fs::read_to_string(&children).unwrap();
        if let Some(pid) = children.split_whitespace().next() {
            break pid.parse().unwrap();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "namespace init never started"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    };
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, init_pid, 0) };
    assert!(raw >= 0, "{}", std::io::Error::last_os_error());
    let init = unsafe { OwnedFd::from_raw_fd(raw as i32) };
    assert!(!exited(&init, 0));
    probe.processes.push(init);
    drop(probe.child.stdin.take());
    assert!(probe.child.wait().unwrap().success());
    probe.assert_all_exited();
}
