use remotemedia_py_env::{LeasedVenvInfo, ProvisioningCommand, ProvisioningContext};
use remotemedia_shared_memory::peer::Protocols;
use remotemedia_types::{Error, Result};
use std::{path::PathBuf, process::Command, time::Duration};

pub(super) struct PreparedPython {
    program: PathBuf,
    venv: LeasedVenvInfo,
}

impl PreparedPython {
    pub(super) fn resources(&self) -> remotemedia_shared_memory::channel::ProcessResources {
        remotemedia_shared_memory::channel::ProcessResources::new(self.venv.clone())
    }
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

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Probe {
    base_executable: PathBuf,
    pid: u32,
}

impl Probe {
    fn ensure_process(&self, expected: u32) -> Result<()> {
        if self.pid != expected {
            return Err(Error::Execution(
                "Python launcher delegated to another process".into(),
            ));
        }
        Ok(())
    }
}

pub(super) async fn prepare(
    venv: LeasedVenvInfo,
    client_hash: &str,
    buffers_hash: &str,
    protocols: Protocols,
    context: &ProvisioningContext,
) -> Result<PreparedPython> {
    let prepared = PreparedPython {
        program: venv.python_executable.clone(),
        venv,
    };
    let (probe, child_id) =
        verify(&prepared, client_hash, buffers_hash, protocols, context).await?;
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
        let (probe, child_id) =
            verify(&prepared, client_hash, buffers_hash, protocols, context).await?;
        (prepared, probe, child_id)
    };
    probe.ensure_process(child_id)?;
    Ok(prepared)
}

async fn verify(
    prepared: &PreparedPython,
    client_hash: &str,
    buffers_hash: &str,
    protocols: Protocols,
    context: &ProvisioningContext,
) -> Result<(Probe, u32)> {
    let mut command = context
        .clone()
        .with_resource(prepared.venv.clone())
        .command(&prepared.program)
        .map_err(|e| Error::Execution(format!("mapped Python verification: {e}")))?;
    prepared.configure(command.command_mut());
    command
        .command_mut()
        .args([
            "-I",
            "-c",
            include_str!("verify.py"),
            client_hash,
            buffers_hash,
        ])
        .arg(&prepared.venv.path)
        .arg(protocols.snapshots.to_string())
        .arg(u8::from(protocols.output_copies).to_string())
        .arg(u8::from(protocols.source_completion).to_string())
        .arg(u8::from(protocols.background).to_string());
    verification_output(command, Duration::from_secs(30)).await
}

async fn verification_output(
    command: ProvisioningCommand,
    timeout: Duration,
) -> Result<(Probe, u32)> {
    let child = command
        .spawn()
        .map_err(|e| Error::Execution(format!("mapped Python verification: {e}")))?;
    let child_id = child.execution_pid();
    let output = child
        .capture_output()
        .map_err(|e| Error::Execution(format!("mapped Python verification capture: {e}")))?;
    let output = tokio::time::timeout(timeout, output)
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

#[cfg(all(test, feature = "test-mapped-probe"))]
#[path = "probe_tests.rs"]
mod tests;
