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
