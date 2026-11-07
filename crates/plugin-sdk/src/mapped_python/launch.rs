use remotemedia_py_env::VenvInfo;
use remotemedia_types::{Error, Result};
use std::{
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};

pub(super) struct PreparedPython {
    program: PathBuf,
    venv: VenvInfo,
}

impl PreparedPython {
    pub(super) fn command(&self) -> Command {
        let mut command = Command::new(&self.program);
        self.configure(&mut command);
        command
    }

    fn configure(&self, command: &mut Command) {
        command
            .env_remove("PYTHONPATH")
            .env_remove("PYTHONHOME")
            .env_remove("PYTHONEXECUTABLE")
            .env_remove("__PYVENV_LAUNCHER__");
        #[cfg(windows)]
        {
            // CPython's venv executable is a redirector which spawns a second
            // process. DuplicateHandle must target the actual interpreter. Use
            // the same venv-launcher context on the base interpreter directly.
            // Its package prefix and original-child PID are verified below.
            command.env("__PYVENV_LAUNCHER__", &self.venv.python_executable);
        }
    }

    #[cfg(all(target_os = "linux", feature = "linux-worker-supervisor"))]
    pub(super) fn namespaced_command(
        &self,
        namespace: &crate::native_worker::NamespaceLaunch,
        timeout: Duration,
    ) -> Result<crate::native_worker::WorkerCommand> {
        // Preserve the verified venv executable spelling: canonicalizing a Unix
        // venv's python symlink to its base binary would lose the venv prefix.
        let mut command = remotemedia_shared_memory::channel::NamespaceCommand::new(
            &namespace.supervisor,
            &namespace.unshare,
            &self.program,
        )
        .map_err(|e| Error::Execution(format!("mapped Python namespace launch: {e}")))?;
        command
            .set_startup_timeout(timeout)
            .map_err(|e| Error::ConfigError(e.to_string()))?;
        self.configure(command.command_mut());
        Ok(crate::native_worker::WorkerCommand::Namespace(command))
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Probe {
    base_executable: PathBuf,
    pid: u32,
}

pub(super) async fn prepare(
    venv: VenvInfo,
    client_hash: &str,
    buffers_hash: &str,
    snapshots: u16,
    output_copies: bool,
) -> Result<PreparedPython> {
    let prepared = PreparedPython {
        program: venv.python_executable.clone(),
        venv,
    };
    let (probe, child_id) = verify(
        &prepared,
        client_hash,
        buffers_hash,
        snapshots,
        output_copies,
    )
    .await?;
    if !probe.base_executable.is_absolute() || !probe.base_executable.is_file() {
        return Err(Error::ConfigError(
            "invalid base Python interpreter path".into(),
        ));
    }
    #[cfg(windows)]
    let (prepared, probe, child_id) = {
        let _ = child_id;
        let prepared = PreparedPython {
            program: probe.base_executable,
            ..prepared
        };
        let (probe, child_id) = verify(
            &prepared,
            client_hash,
            buffers_hash,
            snapshots,
            output_copies,
        )
        .await?;
        (prepared, probe, child_id)
    };
    if probe.pid != child_id {
        return Err(Error::Execution(
            "Python launcher delegated to another process".into(),
        ));
    }
    Ok(prepared)
}

async fn verify(
    prepared: &PreparedPython,
    client_hash: &str,
    buffers_hash: &str,
    snapshots: u16,
    output_copies: bool,
) -> Result<(Probe, u32)> {
    let mut command = tokio::process::Command::from(prepared.command());
    #[cfg(windows)]
    {
        command.creation_flags(0x08000000); // CREATE_NO_WINDOW
    }
    command
        .args([
            "-I",
            "-c",
            include_str!("verify.py"),
            client_hash,
            buffers_hash,
        ])
        .arg(&prepared.venv.path)
        .arg(snapshots.to_string())
        .arg(u8::from(output_copies).to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let child = command
        .spawn()
        .map_err(|e| Error::Execution(format!("mapped Python verification: {e}")))?;
    let child_id = child
        .id()
        .ok_or_else(|| Error::Execution("missing Python probe process ID".into()))?;
    let output = tokio::time::timeout(Duration::from_secs(30), child.wait_with_output())
        .await
        .map_err(|_| Error::Execution("mapped Python package verification timed out".into()))?
        .map_err(|e| Error::Execution(format!("mapped Python package verification: {e}")))?;
    if !output.status.success() {
        return Err(Error::Execution(format!(
            "mapped Python package verification failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let probe = serde_json::from_slice(&output.stdout).map_err(|e| {
        Error::Execution(format!("invalid mapped Python verification response: {e}"))
    })?;
    Ok((probe, child_id))
}
