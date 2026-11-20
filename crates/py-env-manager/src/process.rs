//! Explicit process containment and retained resources for environment operations.
use serde::{Deserialize, Serialize};
use std::{ffi::OsStr, io, path::PathBuf, process::Command, sync::Arc};

pub(crate) fn error(error: io::Error) -> remotemedia_types::Error {
    remotemedia_types::Error::Execution(format!("Python environment process: {error}"))
}

#[cfg(any(windows, target_os = "linux"))]
use remotemedia_shared_memory::channel::{ProcessResources, WorkerProcess};

/// Trusted, matching Linux supervisor and unshare executables. These paths must
/// remain trusted throughout provisioning. No process-group fallback is used.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessNamespace {
    pub supervisor: PathBuf,
    pub unshare: PathBuf,
}

/// Ownership passed to every backend operation. Custom backends must use
/// `command` for subprocesses, or provide equivalent joined containment and
/// retain a clone of this context until all environment access has ended.
/// Spawned async tasks do not inherit this context automatically.
#[derive(Clone, Default)]
pub struct ProvisioningContext {
    namespace: Option<ProcessNamespace>,
    #[cfg(target_os = "macos")]
    darwin_trusted: bool,
    owners: Vec<Arc<dyn Send + Sync>>,
}

impl ProvisioningContext {
    pub fn new(namespace: Option<ProcessNamespace>) -> Self {
        Self {
            namespace,
            #[cfg(target_os = "macos")]
            darwin_trusted: std::env::var_os("REMOTEMEDIA_PYTHON_DARWIN_TRUSTED_PROVISIONING")
                .is_some_and(|value| value == "1"),
            owners: Vec::new(),
        }
    }

    /// Retain cache locks or leases through confirmed command termination.
    /// Unknown termination quarantines these resources until host exit.
    pub fn with_resource(mut self, resource: impl Send + Sync + 'static) -> Self {
        self.owners.push(Arc::new(resource));
        self
    }

    /// Locked installers use only standard system tools and run-owned state.
    #[cfg(feature = "bundled-uv")]
    pub(crate) fn locked_command(
        &self,
        program: &std::path::Path,
        root: &std::path::Path,
    ) -> io::Result<ProvisioningCommand> {
        let mut launch = self.command(program)?;
        let home = root.join("home");
        let temp = root.join("tmp");
        let command = launch.command_mut();
        command
            .env_clear()
            .current_dir(root)
            .env("HOME", &home)
            .env("TMPDIR", &temp);
        #[cfg(windows)]
        {
            let windows = std::env::var_os("SystemRoot")
                .map(PathBuf::from)
                .filter(|path| path.is_absolute())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        "Windows system directory is unavailable",
                    )
                })?;
            command
                .env("SystemRoot", &windows)
                .env("PATH", windows.join("System32"))
                .env("USERPROFILE", &home)
                .env("APPDATA", &home)
                .env("LOCALAPPDATA", &home)
                .env("TEMP", &temp)
                .env("TMP", &temp);
        }
        #[cfg(not(windows))]
        command
            .env("PATH", "/usr/bin:/bin")
            .env("XDG_CONFIG_HOME", &home)
            .env("XDG_CACHE_HOME", &home);
        Ok(launch)
    }

    pub fn command(&self, program: impl AsRef<OsStr>) -> io::Result<ProvisioningCommand> {
        #[cfg(windows)]
        let command = {
            if self.namespace.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "Linux provisioning namespace selected on Windows",
                ));
            }
            Command::new(program)
        };
        #[cfg(target_os = "linux")]
        let command = {
            let namespace = self.namespace.as_ref().ok_or_else(|| io::Error::new(
                io::ErrorKind::Unsupported,
                "Linux provisioning requires explicit process_namespace supervisor and unshare paths",
            ))?;
            remotemedia_shared_memory::channel::NamespaceCommand::new(
                &namespace.supervisor,
                &namespace.unshare,
                program,
            )?
        };
        #[cfg(any(windows, target_os = "linux"))]
        return Ok(ProvisioningCommand {
            command,
            resources: ProcessResources::new(self.clone()),
        });
        #[cfg(target_os = "macos")]
        {
            if self.namespace.is_some() || !self.darwin_trusted {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "macOS provisioning requires explicit trusted process-group provisioning and no Linux namespace",
                ));
            }
            Ok(ProvisioningCommand {
                command: Command::new(program),
                resources: remotemedia_shared_memory::macos_process::ProcessResources::new(
                    self.clone(),
                ),
            })
        }
        #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
        {
            let _ = program;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "contained provisioning is unsupported on this platform",
            ))
        }
    }
}

/// Configure the actual launch command from the outset, including env_clear and
/// current_dir. Arguments apply to the installer; do not replace supervisor argv.
pub struct ProvisioningCommand {
    #[cfg(any(windows, target_os = "macos"))]
    command: Command,
    #[cfg(target_os = "linux")]
    command: remotemedia_shared_memory::channel::NamespaceCommand,
    #[cfg(any(windows, target_os = "linux"))]
    resources: ProcessResources,
    #[cfg(target_os = "macos")]
    resources: remotemedia_shared_memory::macos_process::ProcessResources,
}

impl ProvisioningCommand {
    pub fn command_mut(&mut self) -> &mut Command {
        #[cfg(any(windows, target_os = "macos"))]
        return &mut self.command;
        #[cfg(target_os = "linux")]
        return self.command.command_mut();
        #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
        unreachable!("unsupported platforms cannot construct a provisioning command")
    }

    #[cfg(any(windows, target_os = "linux"))]
    pub fn spawn(mut self) -> io::Result<WorkerProcess> {
        #[cfg(windows)]
        return WorkerProcess::spawn_captured(&mut self.command, self.resources);
        #[cfg(target_os = "linux")]
        return WorkerProcess::spawn_namespaced_captured(&mut self.command, self.resources);
    }

    /// Cancellation drops and joins the contained owner before returning control
    /// to the caller. This may block during shutdown; use on the control plane.
    pub async fn output(self) -> io::Result<std::process::Output> {
        #[cfg(any(windows, target_os = "linux"))]
        return self.spawn()?.capture_output()?.await;
        #[cfg(target_os = "macos")]
        {
            let mut command = self.command;
            remotemedia_shared_memory::macos_process::capture_trusted(&mut command, self.resources)?
                .await
        }
        #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "contained provisioning is unsupported on this platform",
        ))
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn darwin_requires_explicit_trusted_provisioning() {
        let denied = ProvisioningContext::default().command("/bin/echo");
        assert!(matches!(denied, Err(error) if error.kind() == io::ErrorKind::Unsupported));
        let trusted = ProvisioningContext {
            darwin_trusted: true,
            ..ProvisioningContext::default()
        };
        let mut command = trusted.command("/bin/echo").unwrap();
        command.command_mut().arg("trusted Darwin command");
        let output = command.output().await.unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"trusted Darwin command\n");
    }
}
