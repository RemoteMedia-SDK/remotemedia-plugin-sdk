//! Dedicated Linux CPU sandboxes managed through standalone gVisor.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::os::unix::process::ExitStatusExt;
use std::path::{Component, Path, PathBuf};
use std::process::{ExitStatus, Output, Stdio};
use std::sync::Arc;
use std::time::Duration;

use fs2::FileExt;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::oneshot;

use crate::{
    SandboxError, WorkloadExit, WorkloadLimits, WorkloadPhase, WorkloadRequest, WorkloadTarget,
};

const CONTROL_TIMEOUT: Duration = Duration::from_secs(15);
const PREFIX: &str = "rm-space-";
const GUEST_UID: u32 = 65532;
const CGROUP_FS: &str = "/sys/fs/cgroup";

/// These paths come from the host operator. Install the pinned runtime and its
/// sidecars from the same verified release, and use a trusted, immutable rootfs.
/// Reserve the state directory and delegated cgroup subtree for this provider.
#[derive(Clone, Debug)]
pub struct RunscConfig {
    pub executable: PathBuf,
    pub executable_sha256: String,
    pub rootfs: PathBuf,
    pub state: PathBuf,
    pub delegated_cgroup: PathBuf,
    /// Trusted program in the rootfs that connects stdin/stdout to guest loopback.
    pub relay_program: PathBuf,
}

#[derive(Clone)]
pub struct RunscProvider(Arc<HostState>);

/// Host-only staging for one launch. Dropping an unstarted reservation
/// discards it. Starting transfers its inputs to the provider's recovery state.
pub struct RunscReservation {
    provider: RunscProvider,
    bundle: tempfile::TempDir,
}

struct HostState {
    config: RunscConfig,
    _state_lock: File,
    _cgroup_lock: File,
}

/// Owns one workload. Dropping it requests termination. Explicitly wait or
/// stop before releasing the artifact and capacity leases owned by the host.
pub struct RunscSandbox {
    provider: RunscProvider,
    id: String,
    phase: WorkloadPhase,
    cancel: Option<oneshot::Sender<()>>,
    result: oneshot::Receiver<Result<WorkloadExit, SandboxError>>,
}

/// A private byte stream to one run's fixed loopback port. The gateway holds
/// this along with the run's authorization. The workload gets no host listener.
pub struct RunscStream {
    pub input: ChildStdin,
    pub output: ChildStdout,
    _relay: Child,
}

#[derive(Deserialize)]
struct ContainerId {
    id: String,
}

#[derive(Deserialize)]
struct ContainerState {
    id: String,
    status: String,
    pid: i32,
}

impl RunscProvider {
    pub async fn open(mut config: RunscConfig) -> Result<Self, SandboxError> {
        if !config.executable.is_absolute() || !absolute_guest_program(&config.relay_program) {
            return Err(SandboxError::Invalid(
                "absolute runtime and relay paths are required",
            ));
        }
        config.executable = fs::canonicalize(&config.executable)?;
        config.rootfs = fs::canonicalize(&config.rootfs)?;
        config.delegated_cgroup = fs::canonicalize(&config.delegated_cgroup)?;
        if !config.executable.is_file() || !config.rootfs.is_dir() || !config.state.is_absolute() {
            return Err(SandboxError::Invalid(
                "expected a runtime file and dedicated host directories",
            ));
        }
        if config.delegated_cgroup == Path::new(CGROUP_FS)
            || !config.delegated_cgroup.starts_with(CGROUP_FS)
            || !["memory", "cpu", "pids"].iter().all(|controller| {
                fs::read_to_string(config.delegated_cgroup.join("cgroup.subtree_control"))
                    .is_ok_and(|enabled| enabled.split_whitespace().any(|name| name == *controller))
            })
        {
            return Err(SandboxError::Invalid(
                "a dedicated cgroup v2 subtree with memory, cpu and pids is required",
            ));
        }
        fs::DirBuilder::new()
            .mode(0o700)
            .recursive(true)
            .create(&config.state)?;
        config.state = fs::canonicalize(&config.state)?;
        if fs::metadata(&config.state)?.permissions().mode() & 0o077 != 0 {
            return Err(SandboxError::Invalid(
                "provider state directory must be private",
            ));
        }
        fs::create_dir_all(config.state.join("bundles"))?;
        fs::create_dir_all(config.state.join("runtime"))?;
        verify_runtime(&config.executable, &config.executable_sha256)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(config.state.join("provider.lock"))?;
        lock.try_lock_exclusive()
            .map_err(|_| SandboxError::Runtime("exclusive provider state lock"))?;
        let cgroup_lock = File::open(&config.delegated_cgroup)?;
        cgroup_lock
            .try_lock_exclusive()
            .map_err(|_| SandboxError::Runtime("exclusive provider cgroup lock"))?;
        let provider = Self(Arc::new(HostState {
            config,
            _state_lock: lock,
            _cgroup_lock: cgroup_lock,
        }));
        provider.reconcile().await?;
        Ok(provider)
    }

    /// Report the pinned runtime version for the requested profile after local
    /// prerequisites pass. A worker still needs deployment acceptance evidence.
    pub async fn probe(&self, target: WorkloadTarget) -> Result<String, SandboxError> {
        if target != WorkloadTarget::LinuxCpu {
            return Err(SandboxError::UnsupportedTarget(target));
        }
        let output = self.output("version", &["--version"]).await?;
        std::str::from_utf8(&output.stdout)
            .ok()
            .and_then(|output| output.lines().next())
            .filter(|line| line.starts_with("runsc version ") && line.len() <= 128)
            .map(str::to_owned)
            .ok_or(SandboxError::Runtime("version reporting"))
    }

    pub fn reserve(&self) -> Result<RunscReservation, SandboxError> {
        let bundle = tempfile::Builder::new()
            .prefix(PREFIX)
            .tempdir_in(self.0.config.state.join("bundles"))?;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(bundle.path().join("inputs"))?;
        Ok(RunscReservation {
            provider: self.clone(),
            bundle,
        })
    }

    async fn start_reserved(
        &self,
        pending: tempfile::TempDir,
        request: WorkloadRequest,
    ) -> Result<RunscSandbox, SandboxError> {
        validate_request(&request)?;
        let id = pending
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|id| valid_id(id))
            .ok_or(SandboxError::Invalid("could not assign sandbox identity"))?
            .to_owned();
        let spec = self.spec(&id, &pending.path().join("inputs"), &request)?;
        fs::write(
            pending.path().join("config.json"),
            serde_json::to_vec(&spec)?,
        )?;
        fs::File::open(pending.path().join("config.json"))?.sync_all()?;
        if let WorkloadPhase::Serve { loopback_port } = request.phase {
            let relay = process_spec(&[
                self.0.config.relay_program.to_string_lossy().into_owned(),
                format!("127.0.0.1:{loopback_port}"),
            ]);
            fs::write(
                pending.path().join("relay.json"),
                serde_json::to_vec(&relay)?,
            )?;
        }
        File::open(pending.path().parent().unwrap())?.sync_all()?;
        let bundle = pending.keep();
        let (cancel, cancellation) = oneshot::channel();
        let (started, ready) = oneshot::channel();
        let (done, result) = oneshot::channel();
        let handle = RunscSandbox {
            provider: self.clone(),
            id: id.clone(),
            phase: request.phase,
            cancel: Some(cancel),
            result,
        };
        let provider = self.clone();
        tokio::spawn(async move {
            let terminal = provider
                .own_run(&id, &bundle, &request, cancellation, started)
                .await;
            drop(provider);
            let _ = done.send(terminal);
        });
        match ready.await {
            Ok(Ok(())) => Ok(handle),
            Ok(Err(error)) => {
                // Cleanup failure takes priority so the host retains its leases.
                handle.wait().await?;
                Err(error)
            }
            Err(_) => Err(SandboxError::CleanupPending(handle.id.clone())),
        }
    }

    /// Run on service startup before admitting workloads. The dedicated root
    /// contains only this provider's workloads, including interrupted starts.
    async fn reconcile(&self) -> Result<(), SandboxError> {
        let mut ids = self.list().await?;
        for directory in [
            self.0.config.state.join("bundles"),
            self.0.config.delegated_cgroup.clone(),
        ] {
            for entry in fs::read_dir(directory)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    if let Some(id) = entry.file_name().to_str().filter(|id| valid_id(id)) {
                        ids.insert(id.to_owned());
                    }
                }
            }
        }
        for id in ids {
            self.cleanup(&id).await?;
        }
        Ok(())
    }

    async fn own_run(
        &self,
        id: &str,
        bundle: &Path,
        request: &WorkloadRequest,
        mut cancel: oneshot::Receiver<()>,
        started: oneshot::Sender<Result<(), SandboxError>>,
    ) -> Result<WorkloadExit, SandboxError> {
        let pid_file = bundle.join("sandbox.pid");
        let deadline = tokio::time::Instant::now() + request.limits.lifetime;
        let launch = (|| {
            self.configure_cgroup(id, request.limits)?;
            // Owning the attached process preserves the exit status even when
            // the guest finishes before a separate wait command could attach.
            self.command()
                .args(["run", "--bundle"])
                .arg(bundle)
                .arg("--pid-file")
                .arg(&pid_file)
                .arg(id)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .map_err(SandboxError::from)
        })();
        let mut child = match launch {
            Ok(child) => child,
            Err(error) => {
                let _ = started.send(Err(error));
                self.cleanup(id).await?;
                return Ok(WorkloadExit::Stopped);
            }
        };
        let ready = async {
            wait_for_creation(&mut child, &pid_file).await?;
            if matches!(request.phase, WorkloadPhase::Serve { .. }) {
                self.wait_for_serving(id, &mut child).await?;
            }
            Ok(())
        };
        let start = tokio::select! {
            _ = &mut cancel => Err(SandboxError::Runtime("cancelled startup")),
            () = tokio::time::sleep_until(deadline) => Err(SandboxError::Timeout("workload lifetime")),
            result = tokio::time::timeout(CONTROL_TIMEOUT, ready) => {
                result.unwrap_or(Err(SandboxError::Timeout("create")))
            }
        };
        let active = start.is_ok();
        let observer = started.send(start).is_ok();
        let result = if active && observer {
            tokio::select! {
                () = tokio::time::sleep_until(deadline) => Ok(WorkloadExit::Deadline),
                _ = &mut cancel => Ok(WorkloadExit::Stopped),
                status = child.wait() => status.map(workload_exit).map_err(SandboxError::from),
            }
        } else {
            Ok(WorkloadExit::Stopped)
        };
        // Stop and reap the foreground process before deleting its state. Its
        // exit may also have reaped the guest and removed the runtime state.
        if !matches!(
            tokio::time::timeout(CONTROL_TIMEOUT, child.kill()).await,
            Ok(Ok(()))
        ) {
            return Err(SandboxError::CleanupPending(id.to_owned()));
        }
        self.cleanup(id).await?;
        result
    }

    async fn wait_for_serving(&self, id: &str, child: &mut Child) -> Result<(), SandboxError> {
        while child.try_wait()?.is_none() {
            let output = self.output("state", &["state", id]).await?;
            let state: ContainerState = serde_json::from_slice(&output.stdout)?;
            if state.id != id {
                return Err(SandboxError::Runtime("state identity"));
            }
            if state.status == "running" {
                let members = fs::read_to_string(
                    self.0.config.delegated_cgroup.join(id).join("cgroup.procs"),
                )?;
                if state.pid <= 0 || !members.lines().any(|pid| pid.parse() == Ok(state.pid)) {
                    return Err(SandboxError::Runtime("sandbox cgroup membership"));
                }
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(SandboxError::Runtime("serve startup"))
    }

    fn configure_cgroup(&self, id: &str, limits: WorkloadLimits) -> Result<(), SandboxError> {
        let scope = self.0.config.delegated_cgroup.join(id);
        fs::create_dir(&scope)?;
        let expected = [
            ("memory.max", limits.memory_bytes.to_string()),
            ("memory.swap.max", "0".into()),
            ("pids.max", limits.processes.to_string()),
            (
                "cpu.max",
                format!("{} 100000", u64::from(limits.cpu_millis) * 100),
            ),
        ];
        for (controller, expected) in expected {
            let path = scope.join(controller);
            fs::write(&path, &expected)?;
            let observed = match fs::read_to_string(path) {
                Ok(value) => value.trim().to_owned(),
                Err(error) => error.to_string(),
            };
            if observed != expected {
                return Err(SandboxError::ResourceLimit {
                    controller,
                    expected,
                    observed,
                });
            }
        }
        Ok(())
    }

    async fn cleanup(&self, id: &str) -> Result<(), SandboxError> {
        // A run may already have disappeared during a failed create. Prove
        // absence independently of the delete command's exit status.
        let _ = self.run("delete", &["delete", "--force", id]).await;
        let pending = || SandboxError::CleanupPending(id.to_owned());
        if self.list().await.map_err(|_| pending())?.contains(id) {
            return Err(pending());
        }
        let cgroup = self.0.config.delegated_cgroup.join(id);
        tokio::time::timeout(CONTROL_TIMEOUT, remove_cgroup(&cgroup))
            .await
            .map_err(|_| pending())?
            .map_err(|_| pending())?;
        let bundle = self.0.config.state.join("bundles").join(id);
        remove_if_missing_ok(tokio::fs::remove_dir_all(bundle).await).map_err(|_| pending())
    }

    async fn list(&self) -> Result<BTreeSet<String>, SandboxError> {
        let response = self.output("list", &["list", "--format=json"]).await?;
        let ids: Option<Vec<ContainerId>> = serde_json::from_slice(&response.stdout)?;
        ids.unwrap_or_default()
            .into_iter()
            .map(|state| {
                if !valid_id(&state.id) {
                    return Err(SandboxError::Invalid(
                        "unexpected container in dedicated runtime root",
                    ));
                }
                Ok(state.id)
            })
            .collect()
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.0.config.executable);
        // gVisor creates guest mount points during startup. Give the runtime
        // a stable umask so the non-root guest can traverse those directories.
        // The host's state and bundle roots are created separately as 0700.
        // SAFETY: umask is async-signal-safe and runs only in the forked child.
        unsafe {
            command.pre_exec(|| {
                libc::umask(0o022);
                Ok(())
            });
        }
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .kill_on_drop(true)
            .arg("--root")
            .arg(self.0.config.state.join("runtime"))
            .args([
                "--network=none",
                "--platform=systrap",
                "--directfs=false",
                "--host-uds=none",
                "--overlay2=none",
                "--sidecar-release-enforcement-policy=ALWAYS",
            ]);
        command
    }

    async fn run(&self, phase: &'static str, args: &[&str]) -> Result<(), SandboxError> {
        let mut command = self.command();
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let status = tokio::time::timeout(CONTROL_TIMEOUT, command.status())
            .await
            .map_err(|_| SandboxError::Timeout(phase))??;
        if !status.success() {
            return Err(SandboxError::Runtime(phase));
        }
        Ok(())
    }

    async fn output(&self, phase: &'static str, args: &[&str]) -> Result<Output, SandboxError> {
        let output = tokio::time::timeout(CONTROL_TIMEOUT, self.command().args(args).output())
            .await
            .map_err(|_| SandboxError::Timeout(phase))??;
        if !output.status.success() {
            return Err(SandboxError::Runtime(phase));
        }
        Ok(output)
    }

    fn spec(
        &self,
        id: &str,
        input_root: &Path,
        request: &WorkloadRequest,
    ) -> Result<Value, SandboxError> {
        let mut mounts = vec![
            json!({"destination":"/proc", "type":"proc", "source":"proc", "options":["nosuid","nodev","noexec"]}),
            json!({"destination":"/work", "type":"tmpfs", "source":"tmpfs",
                "options":["nosuid","nodev",format!("size={}",request.limits.scratch_bytes),"mode=1777".to_owned()]}),
        ];
        for name in ["source", "wheels", "models"] {
            let mount = input_root.join(name);
            match fs::symlink_metadata(&mount) {
                Ok(metadata) if metadata.is_dir() => mounts.push(
                    json!({"destination":format!("/inputs/{name}"),"type":"bind","source":mount,
                    "options":["bind","ro","nosuid","nodev","noexec"]}),
                ),
                Ok(_) => {
                    return Err(SandboxError::Invalid(
                        "expected a staged artifact directory",
                    ))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => return Err(error.into()),
            }
        }
        let scope = self
            .0
            .config
            .delegated_cgroup
            .strip_prefix(CGROUP_FS)
            .expect("validated cgroup subtree");
        Ok(
            json!({"ociVersion":"1.1.0","root":{"path":self.0.config.rootfs,"readonly":true},
            "process":process_spec(&request.argv),"mounts":mounts,
            "linux":{"cgroupsPath":Path::new("/").join(scope).join(id),
                "namespaces":[{"type":"pid"},{"type":"ipc"},{"type":"uts"},{"type":"mount"},{"type":"network"},{"type":"cgroup"}],
                "resources":{"memory":{"limit":request.limits.memory_bytes,"swap":request.limits.memory_bytes},
                    "pids":{"limit":request.limits.processes},"cpu":{"period":100000,"quota":u64::from(request.limits.cpu_millis)*100}}}}),
        )
    }
}

impl RunscReservation {
    /// An empty private directory for the host's verified input trees.
    pub fn input_root(&self) -> PathBuf {
        self.bundle.path().join("inputs")
    }

    pub async fn start(self, request: WorkloadRequest) -> Result<RunscSandbox, SandboxError> {
        self.provider.start_reserved(self.bundle, request).await
    }
}

impl RunscSandbox {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub async fn wait(mut self) -> Result<WorkloadExit, SandboxError> {
        self.wait_for_exit().await
    }

    /// Wait without consuming the owner. The caller may cancel this wait and
    /// still request a stop. Call only until the terminal result is observed.
    pub async fn wait_for_exit(&mut self) -> Result<WorkloadExit, SandboxError> {
        (&mut self.result)
            .await
            .map_err(|_| SandboxError::CleanupPending(self.id.clone()))?
    }

    pub async fn stop(mut self) -> Result<WorkloadExit, SandboxError> {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        self.wait().await
    }

    pub fn open_stream(&self) -> Result<RunscStream, SandboxError> {
        if !matches!(self.phase, WorkloadPhase::Serve { .. }) {
            return Err(SandboxError::Invalid(
                "only a serve workload exposes a private stream",
            ));
        }
        let relay = self
            .provider
            .0
            .config
            .state
            .join("bundles")
            .join(&self.id)
            .join("relay.json");
        let mut child = self
            .provider
            .command()
            .arg("exec")
            .arg("--process")
            .arg(relay)
            .arg(&self.id)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let input = child.stdin.take().expect("piped child stdin");
        let output = child.stdout.take().expect("piped child stdout");
        Ok(RunscStream {
            input,
            output,
            _relay: child,
        })
    }
}

fn process_spec(argv: &[String]) -> Value {
    json!({"cwd":"/work", "terminal":false, "user":{"uid":GUEST_UID,"gid":GUEST_UID}, "args":argv,
        "env":["HOME=/work","TMPDIR=/work","PATH=/usr/local/bin:/usr/bin:/bin","PYTHONNOUSERSITE=1","PYTHONUNBUFFERED=1"],
        "noNewPrivileges":true, "capabilities":{"bounding":[],"effective":[],"inheritable":[],"permitted":[],"ambient":[]},
        "rlimits":[{"type":"RLIMIT_CORE","hard":0,"soft":0}]})
}

fn absolute_guest_program(path: &Path) -> bool {
    path.is_absolute()
        && path.components().count() > 1
        && path
            .components()
            .all(|part| matches!(part, Component::RootDir | Component::Normal(_)))
}

async fn wait_for_creation(child: &mut Child, pid_file: &Path) -> Result<(), SandboxError> {
    loop {
        let exited = child.try_wait()?.is_some();
        if pid_file.try_exists()? {
            return Ok(());
        }
        if exited {
            return Err(SandboxError::Runtime("create"));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn remove_cgroup(cgroup: &Path) -> std::io::Result<()> {
    if !cgroup.try_exists()? {
        return Ok(());
    }
    let events = cgroup.join("cgroup.events");
    if fs::read_to_string(&events)?
        .lines()
        .any(|line| line == "populated 1")
    {
        // The delegated subtree is exclusive to this provider. A crash may
        // leave a process here without a recoverable runsc state record.
        fs::write(cgroup.join("cgroup.kill"), "1")?;
        while !fs::read_to_string(&events)?
            .lines()
            .any(|line| line == "populated 0")
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    fs::remove_dir(cgroup)
}

fn workload_exit(status: ExitStatus) -> WorkloadExit {
    let code = status
        .code()
        .unwrap_or_else(|| 128 + status.signal().unwrap_or(0));
    WorkloadExit::Exited(code)
}

fn validate_request(request: &WorkloadRequest) -> Result<(), SandboxError> {
    if request.target != WorkloadTarget::LinuxCpu {
        return Err(SandboxError::UnsupportedTarget(request.target));
    }
    let limits = request.limits;
    if limits.cpu_millis == 0
        || limits.memory_bytes == 0
        || limits.memory_bytes > i64::MAX as u64
        || limits.scratch_bytes == 0
        || limits.scratch_bytes > limits.memory_bytes
        || limits.processes == 0
        || limits.lifetime.is_zero()
        || tokio::time::Instant::now()
            .checked_add(limits.lifetime)
            .is_none()
    {
        return Err(SandboxError::Invalid(
            "finite CPU, memory, scratch, process and lifetime limits are required",
        ));
    }
    if !request
        .argv
        .first()
        .is_some_and(|program| absolute_guest_program(Path::new(program)))
        || request.argv.iter().any(|arg| arg.contains('\0'))
        || request.phase == (WorkloadPhase::Serve { loopback_port: 0 })
    {
        return Err(SandboxError::Invalid(
            "invalid guest program or loopback port",
        ));
    }
    Ok(())
}

fn verify_runtime(path: &Path, expected: &str) -> Result<(), SandboxError> {
    let mut input = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    if hex::encode(digest.finalize()) != expected {
        return Err(SandboxError::Invalid(
            "runtime binary digest does not match its approved release",
        ));
    }
    Ok(())
}

fn valid_id(id: &str) -> bool {
    id.strip_prefix(PREFIX).is_some_and(|name| {
        !name.is_empty()
            && name.len() <= 32
            && name.bytes().all(|byte| byte.is_ascii_alphanumeric())
    })
}

fn remove_if_missing_ok(result: std::io::Result<()>) -> std::io::Result<()> {
    match result {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}
