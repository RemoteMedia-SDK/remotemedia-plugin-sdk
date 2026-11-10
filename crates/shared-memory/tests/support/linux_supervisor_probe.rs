//! Executable feasibility probe, not the production worker launch path.
//! The guardian is single-threaded and owns the util-linux namespace launcher.
#[cfg(not(target_os = "linux"))]
fn main() {
    panic!("Linux-only containment probe");
}

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    linux::run()
}

#[cfg(target_os = "linux")]
mod linux {
    use std::{
        io::{self, BufRead, Write},
        os::{fd::AsRawFd, unix::process::CommandExt},
        process::{Child, Command, Stdio},
        time::Duration,
    };

    type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    // A pidfd in the test observes this outer PID even while getpid() reports a
    // namespace-local PID. This probe deliberately retains the outer /proc mount.
    fn outer_pid() -> Result<u32> {
        let status = std::fs::read_to_string("/proc/self/status")?;
        Ok(status
            .lines()
            .find(|line| line.starts_with("NSpid:"))
            .ok_or("missing NSpid")?
            .split_whitespace()
            .nth(1)
            .ok_or("missing outer PID")?
            .parse()?)
    }

    fn report(ids: &[u32]) -> Result<()> {
        println!("{}", serde_json::to_string(ids)?);
        io::stdout().flush()?;
        Ok(())
    }

    fn read_ids(child: &mut Child) -> Result<Vec<u32>> {
        let mut line = String::new();
        io::BufReader::new(child.stdout.take().ok_or("missing child output")?)
            .read_line(&mut line)?;
        Ok(serde_json::from_str(&line)?)
    }

    fn spawn_leaf(depth: u32) -> Result<Vec<u32>> {
        let mut command = Command::new(std::env::current_exe()?);
        command
            .arg("leaf")
            .arg(depth.to_string())
            .env_remove("RM_CAPTURE_TREE_READY")
            .stdin(Stdio::null())
            .stdout(Stdio::piped());
        // Deliberately detach each descendant from its parent's process group:
        // killing a process group would not pass these tests.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn()?;
        read_ids(&mut child)
    }

    fn tree() -> Result<()> {
        assert_eq!(std::process::id(), 1, "worker must be namespace init");
        let mut ids = vec![outer_pid()?];
        ids.extend(spawn_leaf(1)?);
        report(&ids)?;
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        if line.is_empty() {
            // EOF must not make the test pass by voluntarily exiting init. Stay
            // alive until the kernel containment path kills us (or fail-safe).
            std::thread::sleep(Duration::from_secs(30));
            return Err("namespace init survived control EOF".into());
        }
        assert_eq!(line.trim(), "exit");
        std::process::exit(23);
    }

    fn leaf() -> Result<()> {
        let depth: u32 = std::env::args().nth(2).ok_or("missing depth")?.parse()?;
        let mut ids = vec![outer_pid()?];
        if depth > 0 {
            ids.extend(spawn_leaf(depth - 1)?);
        }
        report(&ids)?;
        if let Some(path) = std::env::var_os("RM_CAPTURE_TREE_READY") {
            std::fs::write(path, serde_json::to_vec(&ids)?)?;
        }
        if std::env::args().nth(3).as_deref() == Some("exit-parent") {
            let gate = std::path::PathBuf::from(
                std::env::var_os("RM_CAPTURE_TREE_EXIT_GATE").ok_or("missing exit gate")?,
            );
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !gate.exists() {
                if std::time::Instant::now() >= deadline {
                    return Err("exit gate timed out".into());
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            return Ok(());
        }
        // Fail-safe only; assertions require death within five seconds.
        std::thread::sleep(Duration::from_secs(30));
        Ok(())
    }

    struct Launcher(Child);
    impl Drop for Launcher {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn startup_ready(child: &Child) -> io::Result<bool> {
        let mut ready = [
            libc::pollfd {
                fd: 0,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: child.stdout.as_ref().unwrap().as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        loop {
            if unsafe { libc::poll(ready.as_mut_ptr(), ready.len() as _, -1) } < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            // No command is accepted before readiness. Most importantly, EOF
            // cancels startup even if the namespace root has emitted nothing.
            if ready[0].revents != 0 {
                return Ok(false);
            }
            if ready[1].revents != 0 {
                return Ok(true);
            }
        }
    }

    fn guardian(stall: bool) -> Result<()> {
        let mut command = Command::new("unshare");
        command
            .args([
                "--user",
                "--map-current-user",
                "--pid",
                "--fork",
                "--kill-child=SIGKILL",
                "--",
            ])
            .arg(std::env::current_exe()?)
            .arg(if stall { "stall" } else { "tree" })
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        let guardian_pid = std::process::id() as libc::pid_t;
        // SAFETY: only async-signal-safe operations between fork and exec.
        // This *executable's main thread* remains alive throughout supervision;
        // a library caller's transient spawning thread must not be used here.
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() != guardian_pid {
                    return Err(io::Error::from_raw_os_error(libc::ECHILD));
                }
                Ok(())
            });
        }
        let mut launcher = Launcher(command.spawn()?);
        let mut ids = vec![std::process::id(), launcher.0.id()];
        if stall {
            report(&ids)?;
        }
        if !startup_ready(&launcher.0)? {
            return Ok(());
        }
        ids.extend(read_ids(&mut launcher.0)?);
        report(&ids)?;
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        match line.trim() {
            "exit-root" => {
                launcher
                    .0
                    .stdin
                    .take()
                    .ok_or("missing root input")?
                    .write_all(b"exit\n")?;
                assert_eq!(launcher.0.wait()?.code(), Some(23));
                println!("root-exited");
                io::stdout().flush()?;
                line.clear();
                io::stdin().read_line(&mut line)?;
            }
            "abort" => std::process::abort(), // No Rust Drop or normal cleanup.
            "" | "close" => {}
            _ => return Err("unknown guardian command".into()),
        }
        Ok(())
    }

    fn broker() -> Result<()> {
        let mut guardian = Command::new(std::env::current_exe()?)
            .arg("guardian")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let mut ids = vec![std::process::id()];
        ids.extend(read_ids(&mut guardian)?);
        report(&ids)?;
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        assert_eq!(line.trim(), "exit");
        // Abrupt broker exit closes the lifeline without dropping Child.
        std::process::exit(37);
    }

    fn production_channel_tree() -> Result<()> {
        use remotemedia_shared_memory::channel::Channel;
        assert_ne!(std::process::id(), 1);
        assert_eq!(unsafe { libc::getppid() }, 1);
        // SAFETY: trusted fixture claims its private broker socket once.
        let mut channel = unsafe { Channel::from_inherited_stdio()? };
        assert_eq!(channel.receive()?.metadata, b"spawn");
        let mut ids = vec![outer_pid()?];
        ids.extend(spawn_leaf(1)?);
        channel.send(&serde_json::to_vec(&ids)?, None)?;
        // No voluntary EOF exit can conceal broken namespace cleanup.
        std::thread::sleep(Duration::from_secs(30));
        Err("production namespace survived broker death".into())
    }

    fn production_broker() -> Result<()> {
        use remotemedia_shared_memory::channel::{NamespaceCommand, WorkerProcess};
        let supervisor = std::env::args_os()
            .nth(2)
            .ok_or("missing supervisor path")?;
        let mut command = NamespaceCommand::new(
            std::path::Path::new(&supervisor),
            std::path::Path::new("/usr/bin/unshare"),
            std::env::current_exe()?,
        )?;
        command.command_mut().arg("production-channel-tree");
        let mut process = WorkerProcess::spawn_namespaced(&mut command)?;
        process.channel().send(b"spawn", None)?;
        let mut ids = vec![std::process::id(), process.id()];
        ids.extend(serde_json::from_slice::<Vec<u32>>(
            &process.channel().receive()?.metadata,
        )?);
        report(&ids)?;
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        assert_eq!(line.trim(), "exit");
        std::process::exit(37);
    }

    fn production_stall_broker() -> Result<()> {
        use remotemedia_shared_memory::channel::{NamespaceCommand, WorkerProcess};
        let supervisor = std::env::args_os()
            .nth(2)
            .ok_or("missing supervisor path")?;
        let marker = std::env::args_os().nth(3).ok_or("missing marker path")?;
        let mut command = NamespaceCommand::new(
            std::path::Path::new(&supervisor),
            &std::env::current_exe()?,
            "/bin/false",
        )?;
        command.command_mut().env("NAMESPACE_TEST_STALL", marker);
        let _process = WorkerProcess::spawn_namespaced(&mut command)?;
        Err("stalled launcher unexpectedly became ready".into())
    }

    fn fake_stalled_launcher() -> Result<()> {
        let marker = std::env::var_os("NAMESPACE_TEST_STALL").ok_or("not a test launcher")?;
        std::fs::write(
            marker,
            serde_json::to_vec(&[std::process::id(), unsafe { libc::getppid() } as u32])?,
        )?;
        std::thread::sleep(Duration::from_secs(30));
        Err("startup supervisor failed to kill stalled launcher".into())
    }

    pub(super) fn run() -> Result<()> {
        match std::env::args().nth(1).as_deref() {
            Some("guardian") => guardian(false),
            Some("guardian-stall") => guardian(true),
            Some("broker") => broker(),
            Some("production-broker") => production_broker(),
            Some("production-stall-broker") => production_stall_broker(),
            Some("production-channel-tree") => production_channel_tree(),
            Some("--user") => fake_stalled_launcher(),
            Some("tree") => tree(),
            Some("leaf") => leaf(),
            Some("stall") => {
                assert_eq!(std::process::id(), 1);
                std::thread::sleep(Duration::from_secs(30));
                Err("startup cancellation failed".into())
            }
            _ => Err("unknown probe mode".into()),
        }
    }
}
