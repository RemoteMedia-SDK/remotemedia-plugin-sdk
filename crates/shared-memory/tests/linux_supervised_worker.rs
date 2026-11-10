#![cfg(all(target_os = "linux", feature = "test-linux-namespace"))]
use remotemedia_shared_memory::channel::{NamespaceCommand, WorkerProcess};
use std::{
    fs::File,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    path::Path,
    process::Stdio,
    time::{Duration, Instant},
};

fn command(worker: &str) -> NamespaceCommand {
    NamespaceCommand::new(
        Path::new(env!("CARGO_BIN_EXE_remotemedia-worker-supervisor")),
        Path::new("/usr/bin/unshare"),
        worker,
    )
    .unwrap()
}

#[test]
fn environment_arguments_cwd_stdio_and_reusable_command_survive_supervision() {
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("output");
    let errors = directory.path().join("errors");
    let mut command = command("/usr/bin/python3");
    command
        .command_mut()
        .env_clear()
        .env("WORKER_TEST_VALUE", "value with spaces; literal")
        .current_dir(directory.path())
        .stdout(Stdio::from(File::create(&output).unwrap()))
        .stderr(Stdio::from(File::create(&errors).unwrap()))
        .args([
            "-c",
            r#"
import json, os, sys, stat
assert os.getpid() != 1 and os.getppid() == 1
assert 'PATH' not in os.environ
assert not any(k.startswith('REMOTEMEDIA_NAMESPACE_') for k in os.environ)
assert stat.S_ISSOCK(os.fstat(0).st_mode)
print(json.dumps([os.getcwd(), os.environ['WORKER_TEST_VALUE'], sys.argv[1:]]))
print('worker stderr', file=sys.stderr)
"#,
            "space argument; literal",
            "λ",
        ]);
    for _ in 0..2 {
        let mut process = WorkerProcess::spawn_namespaced(&mut command).unwrap();
        assert!(process.wait().unwrap().success());
        process.terminate().unwrap();
    }
    let text = std::fs::read_to_string(&output).unwrap();
    assert_eq!(text.lines().count(), 2);
    for line in text.lines() {
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(line).unwrap(),
            serde_json::json!([
                directory.path().to_str().unwrap(),
                "value with spaces; literal",
                ["space argument; literal", "λ"]
            ])
        );
    }
    assert_eq!(
        std::fs::read_to_string(errors).unwrap(),
        "worker stderr\nworker stderr\n"
    );
}

#[test]
fn unavailable_namespace_launcher_fails_without_executing_worker() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("must-not-exist");
    let mut command = NamespaceCommand::new(
        Path::new(env!("CARGO_BIN_EXE_remotemedia-worker-supervisor")),
        Path::new("/bin/false"),
        "/usr/bin/touch",
    )
    .unwrap();
    command.command_mut().arg(&marker);
    command
        .set_startup_timeout(Duration::from_millis(200))
        .unwrap();
    assert!(WorkerProcess::spawn_namespaced(&mut command).is_err());
    assert!(!marker.exists());
}

fn live_process(pid: u32) -> OwnedFd {
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    assert!(raw >= 0, "{}", std::io::Error::last_os_error());
    let process = unsafe { OwnedFd::from_raw_fd(raw as i32) };
    assert!(!exited(&process, 0));
    process
}
fn exited(process: &OwnedFd, timeout: i32) -> bool {
    let mut ready = libc::pollfd {
        fd: process.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    assert!(unsafe { libc::poll(&mut ready, 1, timeout) } >= 0);
    assert_eq!(ready.revents & (libc::POLLNVAL | libc::POLLERR), 0);
    ready.revents & libc::POLLIN != 0
}

#[test]
fn abrupt_broker_exit_closes_production_lifeline_and_kills_descendants() {
    use std::io::{BufRead, Write};
    let mut broker =
        std::process::Command::new(env!("CARGO_BIN_EXE_shared-memory-linux-supervisor-probe"))
            .arg("production-broker")
            .arg(env!("CARGO_BIN_EXE_remotemedia-worker-supervisor"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
    let mut line = String::new();
    std::io::BufReader::new(broker.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let ids: Vec<u32> = serde_json::from_str(&line).unwrap();
    assert_eq!(ids.len(), 5);
    let processes: Vec<_> = ids.into_iter().map(live_process).collect();
    broker.stdin.take().unwrap().write_all(b"exit\n").unwrap();
    assert_eq!(broker.wait().unwrap().code(), Some(37));
    for process in processes {
        assert!(exited(&process, 5000));
    }
}

#[test]
fn abrupt_broker_death_cancels_production_startup_before_init_connects() {
    let directory = tempfile::tempdir().unwrap();
    let marker = directory.path().join("stalled.json");
    let mut broker =
        std::process::Command::new(env!("CARGO_BIN_EXE_shared-memory-linux-supervisor-probe"))
            .arg("production-stall-broker")
            .arg(env!("CARGO_BIN_EXE_remotemedia-worker-supervisor"))
            .arg(&marker)
            .spawn()
            .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let ids: Vec<u32> = loop {
        if let Ok(bytes) = std::fs::read(&marker) {
            if let Ok(ids) = serde_json::from_slice(&bytes) {
                break ids;
            }
        }
        assert!(Instant::now() < deadline, "stalled launcher did not start");
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(ids.len(), 2);
    let processes: Vec<_> = ids.into_iter().map(live_process).collect();
    broker.kill().unwrap();
    broker.wait().unwrap();
    for process in processes {
        assert!(exited(&process, 5000));
    }
}

#[test]
fn terminate_drop_and_guardian_death_confirm_namespace_death_with_detached_descendants() {
    for mode in ["terminate", "drop", "guardian-killed"] {
        let directory = tempfile::tempdir().unwrap();
        let ids_path = directory.path().join("ids.json");
        let mut command = command("/usr/bin/python3");
        command
            .command_mut()
            .args([
                "-c",
                r#"
import json, os, subprocess, sys, time
child = subprocess.Popen([sys.argv[1], 'leaf', '1'], stdin=subprocess.DEVNULL,
                         stdout=subprocess.PIPE, start_new_session=True, text=True)
ids = json.loads(child.stdout.readline())
status = open('/proc/self/status').read().splitlines()
outer = int(next(s for s in status if s.startswith('NSpid:')).split()[1])
with open(sys.argv[2], 'w') as f:
    json.dump([outer] + ids, f)
while True:
    time.sleep(1)
"#,
                env!("CARGO_BIN_EXE_shared-memory-linux-supervisor-probe"),
            ])
            .arg(&ids_path);
        let mut process = WorkerProcess::spawn_namespaced(&mut command).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let ids: Vec<u32> = loop {
            if let Ok(bytes) = std::fs::read(&ids_path) {
                if let Ok(ids) = serde_json::from_slice(&bytes) {
                    break ids;
                }
            }
            assert!(
                Instant::now() < deadline,
                "worker did not publish descendant ids"
            );
            std::thread::sleep(Duration::from_millis(1));
        };
        assert_eq!(ids.len(), 3);
        let processes: Vec<_> = ids.into_iter().map(live_process).collect();
        if mode == "guardian-killed" {
            let guardian = live_process(process.id());
            assert_eq!(
                unsafe {
                    libc::syscall(
                        libc::SYS_pidfd_send_signal,
                        guardian.as_raw_fd(),
                        libc::SIGKILL,
                        std::ptr::null::<libc::siginfo_t>(),
                        0,
                    )
                },
                0
            );
            assert!(!process.wait().unwrap().success());
        }
        if mode != "drop" {
            process.terminate().unwrap();
        }
        drop(process);
        for process in processes {
            assert!(exited(&process, 5000));
        }
    }
}
